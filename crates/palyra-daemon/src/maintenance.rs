use std::sync::Arc;

use palyra_common::runtime_contracts::{
    RealtimeEventEnvelope, RealtimeEventSensitivity, RealtimeEventTopic,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    gateway::{self, current_unix_ms},
    journal,
};

const SEVERITY_INFO: &str = "info";
const SEVERITY_WARNING: &str = "warning";
const SEVERITY_BLOCKING: &str = "blocking";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MaintenanceSweeperDefinition {
    pub(crate) task_id: String,
    pub(crate) component: String,
    pub(crate) description: String,
    pub(crate) safety_level: String,
    pub(crate) dry_run_supported: bool,
    pub(crate) interval_ms: i64,
    pub(crate) max_work_per_run: u32,
    pub(crate) retention_policy: String,
    pub(crate) policy_gate: String,
    pub(crate) delete_scope: String,
    pub(crate) event_names: MaintenanceEventNames,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MaintenanceEventNames {
    pub(crate) started: String,
    pub(crate) completed: String,
    pub(crate) failed: String,
}

#[derive(Debug, Clone)]
pub(crate) struct MaintenanceRegistry {
    sweepers: Arc<[MaintenanceSweeperDefinition]>,
}

impl Default for MaintenanceRegistry {
    fn default() -> Self {
        Self {
            sweepers: Arc::from([
                sweeper(
                    "stale_runs",
                    "runs",
                    "Detects run records that have stayed active past their runtime lease window.",
                    "repairable",
                    300_000,
                    32,
                    "run metadata retention",
                    "runtime.policy.run_retention",
                    "run_state_only",
                ),
                sweeper(
                    "orphan_artifacts",
                    "artifacts",
                    "Finds content-addressed artifacts that no longer have a journal or run reference.",
                    "retention_bound_delete",
                    900_000,
                    64,
                    "artifact retention and replay reference policy",
                    "runtime.policy.artifact_retention",
                    "unreferenced_artifact_records",
                ),
                sweeper(
                    "expired_leases",
                    "leases",
                    "Reaps expired provider and worker leases after fail-closed ownership checks.",
                    "repairable",
                    120_000,
                    64,
                    "lease TTL and active worker ownership",
                    "runtime.policy.lease_ttl",
                    "expired_lease_records",
                ),
                sweeper(
                    "stale_bindings",
                    "bindings",
                    "Flags stale plugin, hook, and channel bindings that need operator review.",
                    "review_required",
                    600_000,
                    32,
                    "binding registry update policy",
                    "runtime.policy.binding_integrity",
                    "binding_metadata",
                ),
                sweeper(
                    "old_delivery_attempts",
                    "delivery",
                    "Compacts old delivery attempts after retry, quarantine, and dead-letter retention gates.",
                    "retention_bound_delete",
                    600_000,
                    128,
                    "delivery attempt retention and dead-letter policy",
                    "runtime.policy.delivery_retention",
                    "delivery_attempt_records",
                ),
                sweeper(
                    "memory_retention",
                    "memory",
                    "Applies memory TTL, capacity, and vacuum maintenance using the journal retention policy.",
                    "retention_bound_delete",
                    300_000,
                    512,
                    "memory retention config",
                    "runtime.policy.memory_retention",
                    "memory_items_and_fts_rows",
                ),
            ]),
        }
    }
}

impl MaintenanceRegistry {
    pub(crate) fn definitions(&self) -> &[MaintenanceSweeperDefinition] {
        &self.sweepers
    }

    pub(crate) fn filtered_definitions(
        &self,
        filter: &MaintenanceStatusFilter,
    ) -> Vec<MaintenanceSweeperDefinition> {
        self.definitions()
            .iter()
            .filter(|definition| {
                filter
                    .component
                    .as_deref()
                    .is_none_or(|component| definition.component.eq_ignore_ascii_case(component))
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MaintenanceStatusFilter {
    pub(crate) component: Option<String>,
    pub(crate) severity: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MaintenanceStatusSnapshot {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) registry_version: u32,
    pub(crate) summary: MaintenanceStatusSummary,
    pub(crate) tasks: Vec<MaintenanceTaskStatus>,
    pub(crate) event_contract: MaintenanceRealtimeEventContract,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MaintenanceStatusSummary {
    pub(crate) overall_state: String,
    pub(crate) highest_severity: String,
    pub(crate) total_tasks: usize,
    pub(crate) due_tasks: usize,
    pub(crate) degraded_tasks: usize,
    pub(crate) blocking_tasks: usize,
    pub(crate) fix_count: u64,
    pub(crate) error_count: u64,
    pub(crate) next_run_at_unix_ms: Option<i64>,
    pub(crate) human_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MaintenanceTaskStatus {
    pub(crate) task_id: String,
    pub(crate) component: String,
    pub(crate) description: String,
    pub(crate) state: String,
    pub(crate) severity: String,
    pub(crate) safety_level: String,
    pub(crate) dry_run_supported: bool,
    pub(crate) interval_ms: i64,
    pub(crate) max_work_per_run: u32,
    pub(crate) retention_policy: String,
    pub(crate) policy_gate: String,
    pub(crate) delete_scope: String,
    pub(crate) last_started_at_unix_ms: Option<i64>,
    pub(crate) last_completed_at_unix_ms: Option<i64>,
    pub(crate) last_result: String,
    pub(crate) fix_count: u64,
    pub(crate) error_count: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) next_run_at_unix_ms: Option<i64>,
    pub(crate) evidence: Value,
    pub(crate) remediation: String,
    pub(crate) event_names: MaintenanceEventNames,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MaintenanceRealtimeEventContract {
    pub(crate) topic: String,
    pub(crate) sensitivity: String,
    pub(crate) payload_schema: String,
    pub(crate) phases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorHealthGraphSnapshot {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) state: String,
    pub(crate) severity: String,
    pub(crate) human_summary: String,
    pub(crate) critical_path: Vec<DoctorHealthGraphCriticalPathEntry>,
    pub(crate) nodes: Vec<DoctorHealthGraphNode>,
    pub(crate) edges: Vec<DoctorHealthGraphEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorHealthGraphNode {
    pub(crate) node_id: String,
    pub(crate) component: String,
    pub(crate) status: String,
    pub(crate) severity: String,
    pub(crate) evidence: Value,
    pub(crate) last_checked_at_unix_ms: i64,
    pub(crate) impact: String,
    pub(crate) remediation: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorHealthGraphEdge {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) dependency: String,
    pub(crate) required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorHealthGraphCriticalPathEntry {
    pub(crate) node_id: String,
    pub(crate) status: String,
    pub(crate) severity: String,
    pub(crate) impact: String,
    pub(crate) remediation: String,
}

#[allow(clippy::result_large_err)]
pub(crate) async fn collect_maintenance_status(
    state: &AppState,
    context: &gateway::RequestContext,
    filter: MaintenanceStatusFilter,
) -> Result<MaintenanceStatusSnapshot, tonic::Status> {
    let _ = context;
    let now_unix_ms = current_unix_ms();
    let memory_status = state.runtime.memory_maintenance_status().await?;
    let runtime_decision = state.observability.runtime_decision_snapshot();
    let worker_snapshot = state.runtime.worker_fleet_snapshot();
    let lease_snapshot = state.runtime.provider_lease_snapshot();
    let counters = state.runtime.counters.snapshot();
    let plugin_health = load_plugin_health();
    let failed_support_jobs = count_failed_support_jobs(state);
    let failed_doctor_jobs = count_failed_doctor_jobs(state);

    let mut tasks = state
        .maintenance_registry
        .filtered_definitions(&filter)
        .into_iter()
        .map(|definition| match definition.task_id.as_str() {
            "stale_runs" => stale_runs_status(
                definition,
                &counters,
                runtime_decision.metrics.queue_depth,
                now_unix_ms,
            ),
            "orphan_artifacts" => orphan_artifacts_status(
                definition,
                failed_support_jobs,
                failed_doctor_jobs,
                now_unix_ms,
            ),
            "expired_leases" => expired_leases_status(
                definition,
                &lease_snapshot,
                &worker_snapshot,
                runtime_decision.metrics.worker_orphaned_events,
                now_unix_ms,
            ),
            "stale_bindings" => stale_bindings_status(definition, &plugin_health, now_unix_ms),
            "old_delivery_attempts" => old_delivery_attempts_status(
                definition,
                &counters,
                runtime_decision.metrics.queue_delivery_failures,
                now_unix_ms,
            ),
            "memory_retention" => memory_retention_status(definition, &memory_status, now_unix_ms),
            _ => default_registered_status(definition, now_unix_ms),
        })
        .filter(|task| {
            filter
                .severity
                .as_deref()
                .is_none_or(|severity| task.severity.eq_ignore_ascii_case(severity))
        })
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| {
        severity_rank(right.severity.as_str())
            .cmp(&severity_rank(left.severity.as_str()))
            .then_with(|| left.component.cmp(&right.component))
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    let summary = build_maintenance_summary(&tasks);
    let event_contract = MaintenanceRealtimeEventContract {
        topic: RealtimeEventTopic::System.as_str().to_owned(),
        sensitivity: RealtimeEventSensitivity::Internal.as_str().to_owned(),
        payload_schema:
            "maintenance task events include task_id, component, phase, result, fix_count, error_count, evidence_summary"
                .to_owned(),
        phases: vec!["started".to_owned(), "completed".to_owned(), "failed".to_owned()],
    };
    Ok(MaintenanceStatusSnapshot {
        generated_at_unix_ms: now_unix_ms,
        registry_version: 1,
        summary,
        tasks,
        event_contract,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn collect_doctor_health_graph(
    state: &AppState,
    context: &gateway::RequestContext,
) -> Result<DoctorHealthGraphSnapshot, tonic::Status> {
    let now_unix_ms = current_unix_ms();
    let status_snapshot =
        state.runtime.status_snapshot_async(context.clone(), state.auth.clone()).await?;
    let memory_status = state.runtime.memory_maintenance_status().await?;
    let worker_snapshot = state.runtime.worker_fleet_snapshot();
    let lease_snapshot = state.runtime.provider_lease_snapshot();
    let counters = state.runtime.counters.snapshot();
    let runtime_decision = state.observability.runtime_decision_snapshot();
    let maintenance =
        collect_maintenance_status(state, context, MaintenanceStatusFilter::default()).await?;

    let provider_node = provider_health_node(&status_snapshot.model_provider, now_unix_ms);
    let vault_node = vault_health_node(&counters, now_unix_ms);
    let queue_node = queue_health_node(
        runtime_decision.metrics.queue_depth,
        runtime_decision.metrics.queue_delivery_failures,
        now_unix_ms,
    );
    let scheduler_node = scheduler_health_node(&counters, now_unix_ms);
    let plugin_node = plugin_health_node(&load_plugin_health(), now_unix_ms);
    let worker_node = worker_health_node(&worker_snapshot, now_unix_ms);
    let storage_node = storage_health_node(
        status_snapshot.storage.journal_hash_chain_enabled,
        counters.journal_persist_failures,
        now_unix_ms,
    );
    let security_node = security_health_node(&status_snapshot.security, &counters, now_unix_ms);
    let memory_node = memory_health_node(&memory_status, now_unix_ms);
    let maintenance_node = maintenance_health_node(&maintenance, now_unix_ms);
    let lease_node = lease_health_node(&lease_snapshot, now_unix_ms);

    let nodes = vec![
        provider_node,
        vault_node,
        queue_node,
        scheduler_node,
        plugin_node,
        worker_node,
        storage_node,
        security_node,
        memory_node,
        maintenance_node,
        lease_node,
    ];
    let mut critical_path = nodes
        .iter()
        .filter(|node| node.severity != SEVERITY_INFO)
        .map(|node| DoctorHealthGraphCriticalPathEntry {
            node_id: node.node_id.clone(),
            status: node.status.clone(),
            severity: node.severity.clone(),
            impact: node.impact.clone(),
            remediation: node.remediation.clone(),
        })
        .collect::<Vec<_>>();
    critical_path.sort_by(|left, right| {
        severity_rank(right.severity.as_str())
            .cmp(&severity_rank(left.severity.as_str()))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });

    let severity = nodes
        .iter()
        .map(|node| node.severity.as_str())
        .max_by_key(|severity| severity_rank(severity))
        .unwrap_or(SEVERITY_INFO)
        .to_owned();
    let state_label = if severity == SEVERITY_BLOCKING {
        "blocking"
    } else if severity == SEVERITY_WARNING {
        "degraded"
    } else {
        "ok"
    };

    Ok(DoctorHealthGraphSnapshot {
        generated_at_unix_ms: now_unix_ms,
        state: state_label.to_owned(),
        severity: severity.clone(),
        human_summary: if critical_path.is_empty() {
            "Doctor health graph found no degraded critical-path components.".to_owned()
        } else {
            format!(
                "Doctor health graph found {} degraded critical-path component(s); first remediation: {}",
                critical_path.len(),
                critical_path[0].remediation
            )
        },
        critical_path,
        nodes,
        edges: doctor_health_edges(),
    })
}

pub(crate) fn publish_maintenance_realtime_event(
    state: &AppState,
    owner_principal: Option<String>,
    payload: Value,
) {
    let mut router = state.realtime_events.lock().unwrap_or_else(|error| error.into_inner());
    let _ = router.publish(RealtimeEventEnvelope {
        schema_version: 1,
        sequence: 0,
        event_id: ulid::Ulid::new().to_string(),
        topic: RealtimeEventTopic::System,
        sensitivity: RealtimeEventSensitivity::Internal,
        owner_principal,
        owner_session_id: None,
        occurred_at_unix_ms: current_unix_ms(),
        payload,
    });
}

fn sweeper(
    task_id: &str,
    component: &str,
    description: &str,
    safety_level: &str,
    interval_ms: i64,
    max_work_per_run: u32,
    retention_policy: &str,
    policy_gate: &str,
    delete_scope: &str,
) -> MaintenanceSweeperDefinition {
    MaintenanceSweeperDefinition {
        task_id: task_id.to_owned(),
        component: component.to_owned(),
        description: description.to_owned(),
        safety_level: safety_level.to_owned(),
        dry_run_supported: true,
        interval_ms,
        max_work_per_run,
        retention_policy: retention_policy.to_owned(),
        policy_gate: policy_gate.to_owned(),
        delete_scope: delete_scope.to_owned(),
        event_names: MaintenanceEventNames {
            started: format!("maintenance.{task_id}.started"),
            completed: format!("maintenance.{task_id}.completed"),
            failed: format!("maintenance.{task_id}.failed"),
        },
    }
}

fn stale_runs_status(
    definition: MaintenanceSweeperDefinition,
    counters: &crate::gateway::CountersSnapshot,
    queue_depth: u64,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let active_runs = counters
        .orchestrator_runs_started
        .saturating_sub(counters.orchestrator_runs_completed)
        .saturating_sub(counters.orchestrator_runs_cancelled);
    let (state, severity, last_result, remediation) = if active_runs > 0 && queue_depth > 0 {
        (
            "watching",
            SEVERITY_WARNING,
            "active run and queue pressure detected",
            "Inspect active run metadata before applying stale-run cleanup.",
        )
    } else {
        (
            "ok",
            SEVERITY_INFO,
            "no stale-run pressure detected",
            "No stale-run maintenance action is required.",
        )
    };
    task_status(
        definition,
        state,
        severity,
        None,
        None,
        last_result,
        0,
        0,
        None,
        Some(now_unix_ms.saturating_add(300_000)),
        json!({
            "active_runs": active_runs,
            "queue_depth": queue_depth,
            "runs_started": counters.orchestrator_runs_started,
            "runs_completed": counters.orchestrator_runs_completed,
            "runs_cancelled": counters.orchestrator_runs_cancelled,
        }),
        remediation,
    )
}

fn orphan_artifacts_status(
    definition: MaintenanceSweeperDefinition,
    failed_support_jobs: u64,
    failed_doctor_jobs: u64,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let error_count = failed_support_jobs.saturating_add(failed_doctor_jobs);
    let (state, severity, last_result, remediation) = if error_count > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "artifact-producing support jobs have recent failures",
            "Review support bundle and doctor recovery job outputs before deleting unreferenced artifacts.",
        )
    } else {
        (
            "ok",
            SEVERITY_INFO,
            "artifact references are not reporting cleanup blockers",
            "No orphan artifact cleanup is required.",
        )
    };
    task_status(
        definition,
        state,
        severity,
        None,
        None,
        last_result,
        0,
        error_count,
        (error_count > 0).then(|| "support or doctor job failure present".to_owned()),
        Some(now_unix_ms.saturating_add(900_000)),
        json!({
            "failed_support_bundle_jobs": failed_support_jobs,
            "failed_doctor_recovery_jobs": failed_doctor_jobs,
        }),
        remediation,
    )
}

fn expired_leases_status(
    definition: MaintenanceSweeperDefinition,
    lease_snapshot: &crate::provider_leases::ProviderLeaseManagerSnapshot,
    worker_snapshot: &palyra_workerd::WorkerFleetSnapshot,
    worker_orphaned_events: u64,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let worker_failures = usize_to_u64(worker_snapshot.orphaned_workers)
        .saturating_add(usize_to_u64(worker_snapshot.failed_closed_workers));
    let lease_errors = lease_snapshot
        .timed_out_total
        .saturating_add(lease_snapshot.deferred_total)
        .saturating_add(worker_failures)
        .saturating_add(worker_orphaned_events);
    let (state, severity, last_result, remediation) = if worker_snapshot.failed_closed_workers > 0 {
        (
            "blocked",
            SEVERITY_BLOCKING,
            "worker cleanup is fail-closed",
            "Quarantine or force-clean failed-closed workers before accepting new remote work.",
        )
    } else if lease_errors > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "lease pressure or orphaned worker telemetry detected",
            "Run expired-lease cleanup in dry-run mode and inspect provider cooldowns.",
        )
    } else {
        (
            "ok",
            SEVERITY_INFO,
            "leases are within runtime guardrails",
            "No expired-lease maintenance action is required.",
        )
    };
    task_status(
        definition,
        state,
        severity,
        None,
        None,
        last_result,
        usize_to_u64(worker_snapshot.orphaned_workers),
        lease_errors,
        (lease_errors > 0).then(|| "lease pressure or worker orphan telemetry present".to_owned()),
        Some(now_unix_ms.saturating_add(120_000)),
        json!({
            "active_provider_leases": lease_snapshot.active_leases,
            "foreground_waiters": lease_snapshot.foreground_waiters,
            "background_waiters": lease_snapshot.background_waiters,
            "deferred_total": lease_snapshot.deferred_total,
            "timed_out_total": lease_snapshot.timed_out_total,
            "registered_workers": worker_snapshot.registered_workers,
            "orphaned_workers": worker_snapshot.orphaned_workers,
            "failed_closed_workers": worker_snapshot.failed_closed_workers,
            "worker_orphaned_events": worker_orphaned_events,
        }),
        remediation,
    )
}

fn stale_bindings_status(
    definition: MaintenanceSweeperDefinition,
    plugin_health: &PluginHealthSnapshot,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let (state, severity, last_result, remediation) =
        if plugin_health.load_error.is_some() || plugin_health.unhealthy_bindings > 0 {
            (
                "needs_review",
                SEVERITY_WARNING,
                "binding index reports unhealthy or unavailable plugin bindings",
                "Inspect plugin and hook bindings before enabling additional automation.",
            )
        } else {
            (
                "ok",
                SEVERITY_INFO,
                "binding registries are available",
                "No stale binding review is required.",
            )
        };
    task_status(
        definition,
        state,
        severity,
        None,
        None,
        last_result,
        0,
        u64::try_from(plugin_health.unhealthy_bindings).unwrap_or(u64::MAX),
        plugin_health.load_error.clone(),
        Some(now_unix_ms.saturating_add(600_000)),
        json!({
            "total_plugin_bindings": plugin_health.total_bindings,
            "unhealthy_plugin_bindings": plugin_health.unhealthy_bindings,
            "load_error": plugin_health.load_error,
        }),
        remediation,
    )
}

fn old_delivery_attempts_status(
    definition: MaintenanceSweeperDefinition,
    counters: &crate::gateway::CountersSnapshot,
    queue_delivery_failures: u64,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let delivery_errors = counters
        .channel_messages_quarantined
        .saturating_add(counters.channel_reply_failures)
        .saturating_add(queue_delivery_failures);
    let (state, severity, last_result, remediation) =
        if counters.channel_router_queue_depth > 0 || delivery_errors > 0 {
            (
            "degraded",
            SEVERITY_WARNING,
            "delivery backlog or retry failures detected",
            "Inspect channel queue and dead-letter policy before compacting old delivery attempts.",
        )
        } else {
            (
                "ok",
                SEVERITY_INFO,
                "delivery attempts are within queue guardrails",
                "No delivery attempt compaction is required.",
            )
        };
    task_status(
        definition,
        state,
        severity,
        None,
        None,
        last_result,
        0,
        delivery_errors,
        (delivery_errors > 0)
            .then(|| "delivery failures or quarantined messages present".to_owned()),
        Some(now_unix_ms.saturating_add(600_000)),
        json!({
            "channel_router_queue_depth": counters.channel_router_queue_depth,
            "channel_messages_queued": counters.channel_messages_queued,
            "channel_messages_quarantined": counters.channel_messages_quarantined,
            "channel_reply_failures": counters.channel_reply_failures,
            "queue_delivery_failures": queue_delivery_failures,
        }),
        remediation,
    )
}

fn memory_retention_status(
    definition: MaintenanceSweeperDefinition,
    status: &journal::MemoryMaintenanceStatus,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    let due =
        status.next_maintenance_run_at_unix_ms.is_some_and(|next_run| next_run <= now_unix_ms);
    let last_run = status.last_run.as_ref();
    let fix_count = last_run.map(|run| run.deleted_total_count).unwrap_or(0);
    let (state, severity, last_result, remediation) = if due {
        (
            "due",
            SEVERITY_WARNING,
            "memory maintenance is due",
            "Run memory maintenance from the memory console or wait for the scheduled tick.",
        )
    } else {
        (
            "ok",
            SEVERITY_INFO,
            "memory retention is scheduled",
            "No memory retention action is required.",
        )
    };
    task_status(
        definition,
        state,
        severity,
        last_run.map(|run| run.ran_at_unix_ms),
        last_run.map(|run| run.ran_at_unix_ms),
        last_result,
        fix_count,
        0,
        None,
        status.next_maintenance_run_at_unix_ms,
        json!({
            "entries": status.usage.entries,
            "approx_bytes": status.usage.approx_bytes,
            "last_run": status.last_run,
            "last_vacuum_at_unix_ms": status.last_vacuum_at_unix_ms,
            "next_vacuum_due_at_unix_ms": status.next_vacuum_due_at_unix_ms,
        }),
        remediation,
    )
}

fn default_registered_status(
    definition: MaintenanceSweeperDefinition,
    now_unix_ms: i64,
) -> MaintenanceTaskStatus {
    task_status(
        definition,
        "ok",
        SEVERITY_INFO,
        None,
        None,
        "registered",
        0,
        0,
        None,
        Some(now_unix_ms.saturating_add(300_000)),
        json!({}),
        "No maintenance action is required.",
    )
}

#[allow(clippy::too_many_arguments)]
fn task_status(
    definition: MaintenanceSweeperDefinition,
    state: &str,
    severity: &str,
    last_started_at_unix_ms: Option<i64>,
    last_completed_at_unix_ms: Option<i64>,
    last_result: &str,
    fix_count: u64,
    error_count: u64,
    last_error: Option<String>,
    next_run_at_unix_ms: Option<i64>,
    evidence: Value,
    remediation: &str,
) -> MaintenanceTaskStatus {
    MaintenanceTaskStatus {
        task_id: definition.task_id,
        component: definition.component,
        description: definition.description,
        state: state.to_owned(),
        severity: severity.to_owned(),
        safety_level: definition.safety_level,
        dry_run_supported: definition.dry_run_supported,
        interval_ms: definition.interval_ms,
        max_work_per_run: definition.max_work_per_run,
        retention_policy: definition.retention_policy,
        policy_gate: definition.policy_gate,
        delete_scope: definition.delete_scope,
        last_started_at_unix_ms,
        last_completed_at_unix_ms,
        last_result: last_result.to_owned(),
        fix_count,
        error_count,
        last_error,
        next_run_at_unix_ms,
        evidence,
        remediation: remediation.to_owned(),
        event_names: definition.event_names,
    }
}

fn build_maintenance_summary(tasks: &[MaintenanceTaskStatus]) -> MaintenanceStatusSummary {
    let due_tasks = tasks.iter().filter(|task| task.state == "due").count();
    let degraded_tasks = tasks.iter().filter(|task| task.severity == SEVERITY_WARNING).count();
    let blocking_tasks = tasks.iter().filter(|task| task.severity == SEVERITY_BLOCKING).count();
    let fix_count = tasks.iter().map(|task| task.fix_count).sum();
    let error_count = tasks.iter().map(|task| task.error_count).sum();
    let highest_severity = tasks
        .iter()
        .map(|task| task.severity.as_str())
        .max_by_key(|severity| severity_rank(severity))
        .unwrap_or(SEVERITY_INFO);
    let overall_state = if blocking_tasks > 0 {
        "blocking"
    } else if degraded_tasks > 0 || due_tasks > 0 {
        "degraded"
    } else {
        "ok"
    };
    let next_run_at_unix_ms = tasks.iter().filter_map(|task| task.next_run_at_unix_ms).min();
    let human_summary = if tasks.is_empty() {
        "No maintenance tasks matched the requested filters.".to_owned()
    } else if blocking_tasks > 0 {
        format!(
            "{blocking_tasks} maintenance task(s) are blocking; review the first blocking remediation before applying cleanup."
        )
    } else if degraded_tasks > 0 || due_tasks > 0 {
        format!(
            "{} maintenance task(s) need attention; {} total fix(es) are visible in the latest sweep evidence.",
            degraded_tasks.max(due_tasks),
            fix_count
        )
    } else {
        format!("All {} registered maintenance task(s) are within policy.", tasks.len())
    };
    MaintenanceStatusSummary {
        overall_state: overall_state.to_owned(),
        highest_severity: highest_severity.to_owned(),
        total_tasks: tasks.len(),
        due_tasks,
        degraded_tasks,
        blocking_tasks,
        fix_count,
        error_count,
        next_run_at_unix_ms,
        human_summary,
    }
}

#[derive(Debug, Clone)]
struct PluginHealthSnapshot {
    total_bindings: usize,
    unhealthy_bindings: usize,
    load_error: Option<String>,
}

fn load_plugin_health() -> PluginHealthSnapshot {
    let root = match crate::plugins::resolve_plugins_root() {
        Ok(path) => path,
        Err(error) => {
            return PluginHealthSnapshot {
                total_bindings: 0,
                unhealthy_bindings: 0,
                load_error: Some(crate::sanitize_http_error_message(error.to_string().as_str())),
            }
        }
    };
    let index = match crate::plugins::load_plugin_bindings_index(root.as_path()) {
        Ok(index) => index,
        Err(error) => {
            return PluginHealthSnapshot {
                total_bindings: 0,
                unhealthy_bindings: 0,
                load_error: Some(crate::sanitize_http_error_message(error.to_string().as_str())),
            }
        }
    };
    let unhealthy_bindings = index
        .entries
        .iter()
        .filter(|entry| {
            let discovery_state = serde_json::to_value(entry.discovery.state)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned());
            let config_state = entry.config.as_ref().and_then(|config| {
                serde_json::to_value(config.validation.state)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
            });
            let typed_failed = entry.typed_contracts.mode
                == palyra_plugins_runtime::TypedPluginContractMode::Typed
                && (!entry.typed_contracts.ready
                    || entry.typed_contracts.entries.iter().any(|contract| {
                        contract.status
                            == palyra_plugins_runtime::TypedPluginContractStatus::Rejected
                    }));
            let config_failed = !matches!(config_state.as_deref(), Some("valid") | Some("unknown"))
                && entry.config.is_some();
            let discovery_failed =
                entry.enabled && !matches!(discovery_state.as_str(), "installed" | "unknown");
            let capability_drift = !entry.capability_diff.valid;
            !(entry.enabled
                && !typed_failed
                && !config_failed
                && !discovery_failed
                && !capability_drift)
        })
        .count();
    PluginHealthSnapshot {
        total_bindings: index.entries.len(),
        unhealthy_bindings,
        load_error: None,
    }
}

const fn usize_to_u64(value: usize) -> u64 {
    if value > u64::MAX as usize {
        u64::MAX
    } else {
        value as u64
    }
}

fn count_failed_support_jobs(state: &AppState) -> u64 {
    let jobs = state.support_bundle_jobs.lock().unwrap_or_else(|error| error.into_inner());
    jobs.values()
        .filter(|job| matches!(job.state, palyra_control_plane::SupportBundleJobState::Failed))
        .count()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn count_failed_doctor_jobs(state: &AppState) -> u64 {
    let jobs = state.doctor_jobs.lock().unwrap_or_else(|error| error.into_inner());
    jobs.values()
        .filter(|job| matches!(job.state, palyra_control_plane::DoctorRecoveryJobState::Failed))
        .count()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn provider_health_node(
    provider: &crate::model_provider::ProviderStatusSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let missing_auth = !provider.api_key_configured && provider.auth_profile_id.is_none();
    let (status, severity, remediation) = if missing_auth {
        (
            "missing_auth",
            SEVERITY_BLOCKING,
            "Configure provider credentials or an auth profile before starting model-backed runs.",
        )
    } else if provider.circuit_breaker.open || provider.runtime_metrics.error_rate_bps > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Inspect provider error rate, cooldowns, and auth refresh status.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No provider remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "provider".to_owned(),
        component: "model_provider".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "kind": provider.kind,
            "provider_id": provider.provider_id,
            "auth_profile_id": provider.auth_profile_id,
            "api_key_configured": provider.api_key_configured,
            "error_rate_bps": provider.runtime_metrics.error_rate_bps,
            "avg_latency_ms": provider.runtime_metrics.avg_latency_ms,
            "circuit_open": provider.circuit_breaker.open,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact:
            "Model calls, embeddings, learning, and routine dispatch depend on provider health."
                .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn vault_health_node(
    counters: &crate::gateway::CountersSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) = if counters.vault_rate_limited_requests > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Reduce secret lookup pressure or inspect vault backend latency.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No vault remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "vault".to_owned(),
        component: "vault".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "put_requests": counters.vault_put_requests,
            "get_requests": counters.vault_get_requests,
            "delete_requests": counters.vault_delete_requests,
            "list_requests": counters.vault_list_requests,
            "rate_limited_requests": counters.vault_rate_limited_requests,
            "access_audit_events": counters.vault_access_audit_events,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Provider auth, connector auth, and admin token references depend on vault access."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn queue_health_node(
    queue_depth: u64,
    queue_delivery_failures: u64,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) = if queue_delivery_failures > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Inspect queue delivery failures and channel backpressure before increasing dispatch.",
        )
    } else if queue_depth > 0 {
        ("watching", SEVERITY_INFO, "Queue has pending work but no delivery failures.")
    } else {
        ("ok", SEVERITY_INFO, "No queue remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "queue".to_owned(),
        component: "queue".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "queue_depth": queue_depth,
            "queue_delivery_failures": queue_delivery_failures,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Runs, connector delivery, and background dispatch share queue health.".to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn scheduler_health_node(
    counters: &crate::gateway::CountersSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) = if counters.cron_runs_failed > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Inspect routine run failures and approval policy denies before widening schedules.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No scheduler remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "scheduler".to_owned(),
        component: "scheduler".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "cron_triggers_fired": counters.cron_triggers_fired,
            "cron_runs_started": counters.cron_runs_started,
            "cron_runs_completed": counters.cron_runs_completed,
            "cron_runs_failed": counters.cron_runs_failed,
            "cron_runs_skipped": counters.cron_runs_skipped,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact:
            "Routines, maintenance ticks, and unattended automation depend on scheduler health."
                .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn plugin_health_node(
    plugin_health: &PluginHealthSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) =
        if plugin_health.load_error.is_some() || plugin_health.unhealthy_bindings > 0 {
            (
                "degraded",
                SEVERITY_WARNING,
                "Inspect plugin bindings, discovery state, and typed contract drift.",
            )
        } else {
            ("ok", SEVERITY_INFO, "No plugin remediation is required.")
        };
    DoctorHealthGraphNode {
        node_id: "plugins".to_owned(),
        component: "plugins".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "total_bindings": plugin_health.total_bindings,
            "unhealthy_bindings": plugin_health.unhealthy_bindings,
            "load_error": plugin_health.load_error,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Plugin tools, hooks, and skill-backed automation depend on plugin operability."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn worker_health_node(
    worker_snapshot: &palyra_workerd::WorkerFleetSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) = if worker_snapshot.failed_closed_workers > 0 {
        (
            "blocked",
            SEVERITY_BLOCKING,
            "Quarantine or force-clean failed-closed workers before accepting remote work.",
        )
    } else if worker_snapshot.orphaned_workers > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Reap expired workers and verify attestation before scaling worker rollout.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No worker remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "workers".to_owned(),
        component: "workers".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "registered_workers": worker_snapshot.registered_workers,
            "attested_workers": worker_snapshot.attested_workers,
            "active_leases": worker_snapshot.active_leases,
            "orphaned_workers": worker_snapshot.orphaned_workers,
            "failed_closed_workers": worker_snapshot.failed_closed_workers,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Networked execution backends and remote tool execution depend on worker health."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn storage_health_node(
    journal_hash_chain_enabled: bool,
    journal_persist_failures: u64,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) = if journal_persist_failures > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Inspect SQLite storage and journal write errors before continuing automation.",
        )
    } else if !journal_hash_chain_enabled {
        (
            "reduced_integrity",
            SEVERITY_WARNING,
            "Re-enable journal hash chaining unless this is an explicit compatibility environment.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No storage remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "storage".to_owned(),
        component: "storage".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "journal_hash_chain_enabled": journal_hash_chain_enabled,
            "journal_persist_failures": journal_persist_failures,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Audit, replay, memory, routines, and learning depend on durable journal storage."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn security_health_node(
    security: &crate::gateway::SecuritySnapshot,
    counters: &crate::gateway::CountersSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let sandbox_violations = counters
        .sandbox_escape_attempts_blocked_workspace
        .saturating_add(counters.sandbox_escape_attempts_blocked_egress)
        .saturating_add(counters.sandbox_escape_attempts_blocked_executable);
    let (status, severity, remediation) = if !security.deny_by_default
        || !security.admin_auth_required
    {
        (
            "blocked",
            SEVERITY_BLOCKING,
            "Restore deny-by-default policy and admin authentication before exposing control-plane APIs.",
        )
    } else if sandbox_violations > 0 || counters.sandbox_policy_denies > 0 {
        (
            "degraded",
            SEVERITY_WARNING,
            "Review sandbox denials and blocked escape attempts for abuse or policy drift.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No security remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "security".to_owned(),
        component: "security".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "deny_by_default": security.deny_by_default,
            "admin_auth_required": security.admin_auth_required,
            "node_rpc_mtls_required": security.node_rpc_mtls_required,
            "sandbox_policy_denies": counters.sandbox_policy_denies,
            "sandbox_escape_attempts_blocked": sandbox_violations,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "All sensitive tool calls, approvals, remote binds, and operator actions depend on security posture."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn memory_health_node(
    memory_status: &journal::MemoryMaintenanceStatus,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let maintenance_due = memory_status
        .next_maintenance_run_at_unix_ms
        .is_some_and(|next_run| next_run <= now_unix_ms);
    let (status, severity, remediation) = if maintenance_due {
        (
            "maintenance_due",
            SEVERITY_WARNING,
            "Run memory maintenance or wait for the scheduled retention tick.",
        )
    } else {
        ("ok", SEVERITY_INFO, "No memory remediation is required.")
    };
    DoctorHealthGraphNode {
        node_id: "memory".to_owned(),
        component: "memory".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "entries": memory_status.usage.entries,
            "approx_bytes": memory_status.usage.approx_bytes,
            "last_run": memory_status.last_run,
            "next_maintenance_run_at_unix_ms": memory_status.next_maintenance_run_at_unix_ms,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Recall, auto-inject, learning candidates, and workspace memory depend on memory health."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn maintenance_health_node(
    maintenance: &MaintenanceStatusSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    DoctorHealthGraphNode {
        node_id: "maintenance".to_owned(),
        component: "maintenance".to_owned(),
        status: maintenance.summary.overall_state.clone(),
        severity: maintenance.summary.highest_severity.clone(),
        evidence: json!({
            "total_tasks": maintenance.summary.total_tasks,
            "due_tasks": maintenance.summary.due_tasks,
            "degraded_tasks": maintenance.summary.degraded_tasks,
            "blocking_tasks": maintenance.summary.blocking_tasks,
            "human_summary": maintenance.summary.human_summary,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Supportability and automatic cleanup depend on maintenance registry health."
            .to_owned(),
        remediation: if maintenance.summary.blocking_tasks > 0 {
            "Resolve blocking maintenance tasks before applying doctor recovery.".to_owned()
        } else {
            "Inspect maintenance status for due or degraded tasks.".to_owned()
        },
    }
}

fn lease_health_node(
    lease_snapshot: &crate::provider_leases::ProviderLeaseManagerSnapshot,
    now_unix_ms: i64,
) -> DoctorHealthGraphNode {
    let (status, severity, remediation) =
        if !lease_snapshot.credential_feedback.is_empty() || lease_snapshot.timed_out_total > 0 {
            (
                "degraded",
                SEVERITY_WARNING,
                "Inspect provider cooldowns, rate limits, and timed-out lease events.",
            )
        } else {
            ("ok", SEVERITY_INFO, "No lease remediation is required.")
        };
    DoctorHealthGraphNode {
        node_id: "leases".to_owned(),
        component: "leases".to_owned(),
        status: status.to_owned(),
        severity: severity.to_owned(),
        evidence: json!({
            "active_leases": lease_snapshot.active_leases,
            "foreground_waiters": lease_snapshot.foreground_waiters,
            "background_waiters": lease_snapshot.background_waiters,
            "deferred_total": lease_snapshot.deferred_total,
            "timed_out_total": lease_snapshot.timed_out_total,
            "credential_feedback": lease_snapshot.credential_feedback,
        }),
        last_checked_at_unix_ms: now_unix_ms,
        impact: "Provider throughput, background learning, and routine dispatch depend on lease fairness."
            .to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn doctor_health_edges() -> Vec<DoctorHealthGraphEdge> {
    vec![
        edge("security", "provider", "provider credentials and API calls are policy-gated", true),
        edge("vault", "provider", "provider credential references resolve through vault", true),
        edge("storage", "memory", "memory state is persisted in journal storage", true),
        edge(
            "storage",
            "scheduler",
            "routine runs and cron history persist through journal storage",
            true,
        ),
        edge("queue", "scheduler", "routine dispatch uses queue delivery", true),
        edge("provider", "memory", "embedding and learning flows may call the provider", false),
        edge("plugins", "scheduler", "routine hooks may dispatch plugin-backed work", false),
        edge("workers", "queue", "networked workers consume queued execution work", false),
        edge("leases", "provider", "provider calls acquire fair-use leases", true),
        edge(
            "maintenance",
            "storage",
            "maintenance evidence and audit records use journal storage",
            true,
        ),
    ]
}

fn edge(from: &str, to: &str, dependency: &str, required: bool) -> DoctorHealthGraphEdge {
    DoctorHealthGraphEdge {
        from: from.to_owned(),
        to: to.to_owned(),
        dependency: dependency.to_owned(),
        required,
    }
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        SEVERITY_BLOCKING => 3,
        SEVERITY_WARNING => 2,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_contains_phase_8_sweepers() {
        let registry = MaintenanceRegistry::default();
        let task_ids = registry
            .definitions()
            .iter()
            .map(|definition| definition.task_id.as_str())
            .collect::<Vec<_>>();
        assert!(task_ids.contains(&"stale_runs"));
        assert!(task_ids.contains(&"orphan_artifacts"));
        assert!(task_ids.contains(&"expired_leases"));
        assert!(task_ids.contains(&"stale_bindings"));
        assert!(task_ids.contains(&"old_delivery_attempts"));
        assert!(registry.definitions().iter().all(|definition| definition.dry_run_supported));
    }

    #[test]
    fn maintenance_summary_promotes_blocking_status() {
        let definition = sweeper(
            "expired_leases",
            "leases",
            "test",
            "repairable",
            1_000,
            1,
            "retention",
            "policy",
            "scope",
        );
        let task = task_status(
            definition,
            "blocked",
            SEVERITY_BLOCKING,
            None,
            None,
            "failed closed",
            0,
            1,
            Some("worker failed closed".to_owned()),
            Some(2_000),
            json!({}),
            "fix",
        );
        let summary = build_maintenance_summary(&[task]);
        assert_eq!(summary.overall_state, "blocking");
        assert_eq!(summary.highest_severity, SEVERITY_BLOCKING);
        assert_eq!(summary.blocking_tasks, 1);
    }

    #[test]
    fn registry_filter_matches_component_case_insensitively() {
        let registry = MaintenanceRegistry::default();
        let filtered = registry.filtered_definitions(&MaintenanceStatusFilter {
            component: Some("LEASES".to_owned()),
            severity: None,
        });
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].task_id, "expired_leases");
    }
}
