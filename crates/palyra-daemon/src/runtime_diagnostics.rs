//! Read-only diagnostics builders for console and ops surfaces: the
//! component-level runtime health snapshot, agent runtime metrics JSON,
//! Prometheus text rendering, and the OTel span, connector-delivery,
//! watchdog, budget-gate, and support-bundle contract payloads.
//!
//! Everything here derives pure values from already-collected state
//! (gateway status counters, journal tool jobs, console payloads) -- no IO
//! and no mutation. Outputs are deterministic (sorted components, BTreeMaps)
//! because contract snapshot tests pin them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[cfg(test)]
use crate::{
    application::{
        channel_commands::ChannelCommandRegistry,
        tool_registry::{
            build_model_visible_tool_catalog_snapshot, ToolCatalogBuildRequest, ToolExposureSurface,
        },
    },
    tool_protocol::ToolRequestContext,
};
use crate::{
    gateway::GatewayStatusSnapshot,
    journal::{ToolJobRecord, ToolJobState},
    model_provider::ProviderRuntimeMetricsSnapshot,
};

/// Schema version stamped on runtime health snapshots; bump on any
/// backward-incompatible shape change.
pub(crate) const RUNTIME_HEALTH_SCHEMA_VERSION: u32 = 1;
/// Schema version for the agent runtime metrics JSON payload.
pub(crate) const AGENT_RUNTIME_METRICS_SCHEMA_VERSION: u32 = 1;
/// Schema version for the OTel span contract payload.
pub(crate) const OTEL_SPAN_CONTRACT_SCHEMA_VERSION: u32 = 1;
/// Schema version for the test-only ABI contract snapshot suite.
#[cfg(test)]
pub(crate) const CONTRACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

// An active tool job whose heartbeat (or start) is older than this is
// reported as stale/stuck by the jobs component and the watchdog.
const STUCK_TOOL_JOB_AFTER_MS: i64 = 120_000;
const STARTUP_CONFIG_BUDGET_MS: u64 = 1_500;
const STARTUP_MIGRATION_BUDGET_MS: u64 = 5_000;
const STARTUP_VAULT_BUDGET_MS: u64 = 1_000;
const STARTUP_PROVIDER_REGISTRY_BUDGET_MS: u64 = 1_000;
const STARTUP_CONNECTOR_BUDGET_MS: u64 = 1_500;
const STARTUP_BACKGROUND_QUEUE_BUDGET_MS: u64 = 1_000;
const PROVIDER_PREPASS_BUDGET_MS: u64 = 1_500;
const CONTEXT_ASSEMBLY_BUDGET_MS: u64 = 750;
const TOOL_CATALOG_BUILD_BUDGET_MS: u64 = 250;
const ROUTE_PLANNING_BUDGET_MS: u64 = 250;
const DAEMON_STARTUP_BASELINE_RSS_BYTES: u64 = 256 * 1024 * 1024;
const AGENT_LOOP_BASELINE_RSS_BYTES: u64 = 384 * 1024 * 1024;

/// Three-level health verdict used per component and overall.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

impl RuntimeHealthStatus {
    /// Returns the snake_case wire label (matches the serde representation).
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }

    // Severity order for the worst-component-wins overall status.
    const fn rank(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Degraded => 1,
            Self::Unavailable => 2,
        }
    }
}

/// Health verdict for one subsystem with stable reason codes, bounded
/// numeric metrics, and operator repair hints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeHealthComponentSnapshot {
    pub component: String,
    pub status: RuntimeHealthStatus,
    pub reason_codes: Vec<String>,
    pub metrics: BTreeMap<String, u64>,
    pub repair_hints: Vec<String>,
}

/// Versioned whole-daemon health snapshot; `status` is the worst component
/// status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeHealthSnapshot {
    pub schema_version: u32,
    pub generated_at_unix_ms: i64,
    pub status: RuntimeHealthStatus,
    pub components: Vec<RuntimeHealthComponentSnapshot>,
}

/// Evaluates every subsystem payload into one health snapshot. Components
/// are sorted by name and the overall status is the worst component rank, so
/// equal inputs always produce byte-equal snapshots.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_runtime_health_snapshot(
    generated_at_unix_ms: i64,
    status: &GatewayStatusSnapshot,
    auth_payload: &Value,
    memory_payload: &Value,
    skills_payload: &Value,
    plugins_payload: &Value,
    networked_workers_payload: &Value,
    support_bundle_payload: &Value,
    runtime_preview_payload: &Value,
    tool_jobs: &[ToolJobRecord],
) -> RuntimeHealthSnapshot {
    let mut components = vec![
        daemon_health_component(status),
        connector_health_component(status, runtime_preview_payload),
        provider_health_component(status),
        auth_health_component(auth_payload),
        memory_health_component(memory_payload),
        jobs_health_component(generated_at_unix_ms, tool_jobs),
        routines_health_component(status),
        extensions_health_component(skills_payload, plugins_payload),
        storage_health_component(status),
        networked_workers_health_component(networked_workers_payload),
        support_bundle_health_component(support_bundle_payload),
    ];
    components.sort_by(|left, right| left.component.cmp(&right.component));
    let overall = components
        .iter()
        .map(|component| component.status)
        .max_by_key(|status| status.rank())
        .unwrap_or(RuntimeHealthStatus::Healthy);
    RuntimeHealthSnapshot {
        schema_version: RUNTIME_HEALTH_SCHEMA_VERSION,
        generated_at_unix_ms,
        status: overall,
        components,
    }
}

/// Builds the agent runtime metrics JSON payload. The embedded
/// `cardinality_policy` is itself part of the contract: labels stay bounded
/// and raw user data, prompts, and per-session ids are forbidden.
pub(crate) fn build_agent_runtime_metrics_snapshot(
    status: &GatewayStatusSnapshot,
    runtime_preview_payload: &Value,
    memory_payload: &Value,
    tool_jobs: &[ToolJobRecord],
) -> Value {
    let provider_metrics = &status.model_provider.runtime_metrics;
    let tool_job_counts = count_tool_jobs_by_state(tool_jobs);
    json!({
        "schema_version": AGENT_RUNTIME_METRICS_SCHEMA_VERSION,
        "cardinality_policy": {
            "bounded_labels": ["component", "provider_kind", "tool_job_state", "status"],
            "forbidden_labels": ["raw_user", "prompt", "path", "session_id", "principal", "channel_id"],
            "redaction_required": true,
        },
        "runs": {
            "started_total": status.counters.orchestrator_runs_started,
            "completed_total": status.counters.orchestrator_runs_completed,
            "cancelled_total": status.counters.orchestrator_runs_cancelled,
        },
        "provider": provider_metrics_json(provider_metrics),
        "tools": {
            "proposals_total": status.counters.tool_proposals,
            "execution_attempts_total": status.counters.tool_execution_attempts,
            "execution_failures_total": status.counters.tool_execution_failures,
            "execution_timeouts_total": status.counters.tool_execution_timeouts,
            "attestations_emitted_total": status.counters.tool_attestations_emitted,
            "job_states": tool_job_counts,
        },
        "approvals": {
            "requested_total": status.counters.approvals_tool_requested,
            "wait_resolved_allow_total": status.counters.approvals_tool_resolved_allow,
            "wait_resolved_deny_total": status.counters.approvals_tool_resolved_deny,
            "wait_resolved_timeout_total": status.counters.approvals_tool_resolved_timeout,
            "wait_resolved_error_total": status.counters.approvals_tool_resolved_error,
        },
        "memory": {
            "recall_requests_total": status.counters.memory_search_requests,
            "recall_cache_hits_total": status.counters.memory_search_cache_hits,
            "auto_inject_events_total": status.counters.memory_auto_inject_events,
            "provider_count": memory_payload.pointer("/providers").and_then(Value::as_array).map_or(0, Vec::len),
            "entries": read_u64(memory_payload, "/usage/entries"),
            "bytes": read_u64(memory_payload, "/usage/bytes"),
            "retrieval_branch_latency_avg_ms": read_u64(runtime_preview_payload, "/metrics/retrieval_branch_latency_avg_ms"),
            "retrieval_branch_latency_max_ms": read_u64(runtime_preview_payload, "/metrics/retrieval_branch_latency_max_ms"),
        },
        "channel_delivery": {
            "inbound_total": status.counters.channel_messages_inbound,
            "routed_total": status.counters.channel_messages_routed,
            "replied_total": status.counters.channel_messages_replied,
            "rejected_total": status.counters.channel_messages_rejected,
            "queued_total": status.counters.channel_messages_queued,
            "quarantined_total": status.counters.channel_messages_quarantined,
            "reply_failures_total": status.counters.channel_reply_failures,
            "queue_depth": status.counters.channel_router_queue_depth,
            "arbitration_suppressions_total": read_u64(runtime_preview_payload, "/metrics/arbitration_suppressions"),
            "queue_delivery_failures_total": read_u64(runtime_preview_payload, "/metrics/queue_delivery_failures"),
        },
    })
}

/// Renders the Prometheus text exposition for the daemon's core counters and
/// gauges. Labels are restricted to bounded values (provider kind, job
/// state, delivery status) -- never principals, sessions, or paths -- to keep
/// cardinality flat and avoid leaking identifiers into scrape targets.
pub(crate) fn render_prometheus_metrics(
    status: &GatewayStatusSnapshot,
    tool_jobs: &[ToolJobRecord],
) -> String {
    let mut output = String::new();
    push_help(
        &mut output,
        "palyra_agent_runs_started_total",
        "Total agent runs started by the daemon.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_agent_runs_started_total",
        &[],
        status.counters.orchestrator_runs_started,
    );
    push_help(
        &mut output,
        "palyra_agent_runs_completed_total",
        "Total agent runs completed by the daemon.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_agent_runs_completed_total",
        &[],
        status.counters.orchestrator_runs_completed,
    );
    push_help(
        &mut output,
        "palyra_model_provider_requests_total",
        "Total model provider calls by bounded provider kind.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_model_provider_requests_total",
        &[("provider_kind", status.model_provider.kind.as_str())],
        status.counters.model_provider_requests,
    );
    push_help(
        &mut output,
        "palyra_model_provider_errors_total",
        "Total model provider failures by bounded provider kind.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_model_provider_errors_total",
        &[("provider_kind", status.model_provider.kind.as_str())],
        status.counters.model_provider_failures,
    );
    push_help(
        &mut output,
        "palyra_model_provider_latency_ms",
        "Current model provider latency gauges.",
        "gauge",
    );
    push_sample(
        &mut output,
        "palyra_model_provider_latency_ms",
        &[("provider_kind", status.model_provider.kind.as_str()), ("stat", "avg")],
        status.model_provider.runtime_metrics.avg_latency_ms,
    );
    push_sample(
        &mut output,
        "palyra_model_provider_latency_ms",
        &[("provider_kind", status.model_provider.kind.as_str()), ("stat", "max")],
        status.model_provider.runtime_metrics.max_latency_ms,
    );
    push_help(
        &mut output,
        "palyra_tool_execution_attempts_total",
        "Total tool execution attempts.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_tool_execution_attempts_total",
        &[],
        status.counters.tool_execution_attempts,
    );
    push_help(
        &mut output,
        "palyra_tool_job_state",
        "Current durable tool jobs by bounded lifecycle state.",
        "gauge",
    );
    let job_counts = count_tool_jobs_by_state(tool_jobs);
    for state in all_tool_job_states() {
        let value = job_counts.get(state.as_str()).copied().unwrap_or_default();
        push_sample(&mut output, "palyra_tool_job_state", &[("state", state.as_str())], value);
    }
    push_help(
        &mut output,
        "palyra_memory_recall_requests_total",
        "Total memory recall requests.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_memory_recall_requests_total",
        &[],
        status.counters.memory_search_requests,
    );
    push_help(
        &mut output,
        "palyra_channel_delivery_events_total",
        "Total channel delivery events by bounded status.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_channel_delivery_events_total",
        &[("status", "routed")],
        status.counters.channel_messages_routed,
    );
    push_sample(
        &mut output,
        "palyra_channel_delivery_events_total",
        &[("status", "replied")],
        status.counters.channel_messages_replied,
    );
    push_sample(
        &mut output,
        "palyra_channel_delivery_events_total",
        &[("status", "failed")],
        status.counters.channel_reply_failures,
    );
    output
}

/// Builds the OTel span contract payload: the span chain shape, required
/// attributes, sampling posture, and forbidden/high-cardinality attribute
/// lists that exporters must honor.
pub(crate) fn build_otel_span_contract(
    generated_at_unix_ms: i64,
    status: &GatewayStatusSnapshot,
) -> Value {
    json!({
        "schema_version": OTEL_SPAN_CONTRACT_SCHEMA_VERSION,
        "generated_at_unix_ms": generated_at_unix_ms,
        "trace_context": {
            "source": "w3c_traceparent_or_daemon_generated",
            "required_ids": ["trace_id", "run_id"],
            "optional_ids": ["turn_id", "tool_call_id", "job_id"],
        },
        "span_chain": [
            {
                "name": "agent.run",
                "parent": null,
                "required_attributes": ["trace_id", "run_id", "surface", "status"],
            },
            {
                "name": "agent.turn",
                "parent": "agent.run",
                "required_attributes": ["trace_id", "run_id", "turn_id", "provider_kind"],
            },
            {
                "name": "provider.call",
                "parent": "agent.turn",
                "required_attributes": ["trace_id", "run_id", "turn_id", "provider_kind", "status"],
            },
            {
                "name": "tool.call",
                "parent": "agent.turn",
                "required_attributes": ["trace_id", "run_id", "turn_id", "tool_call_id", "tool_name", "status"],
            },
            {
                "name": "tool.job",
                "parent": "tool.call",
                "required_attributes": ["trace_id", "run_id", "tool_call_id", "job_id", "status"],
            },
            {
                "name": "memory.recall",
                "parent": "agent.turn",
                "required_attributes": ["trace_id", "run_id", "turn_id", "provider", "status"],
            },
            {
                "name": "channel.delivery",
                "parent": "agent.run",
                "required_attributes": ["trace_id", "run_id", "channel_kind", "delivery_status"],
            }
        ],
        "sampling": {
            "default": "always_on_for_errors_sampled_for_success",
            "configured_centrally": true,
        },
        "redaction": {
            "forbidden_attributes": ["prompt", "raw_user", "raw_path", "secret", "authorization"],
            "high_cardinality_attributes": ["principal", "session_id", "channel_id", "workspace_path"],
        },
        "observed_totals": {
            "provider_calls": status.counters.model_provider_requests,
            "tool_calls": status.counters.tool_execution_attempts,
            "tool_jobs": status.counters.tool_execution_attempts,
            "memory_recalls": status.counters.memory_search_requests,
            "channel_deliveries": status.counters.channel_messages_replied,
        }
    })
}

/// Builds the connector delivery diagnostics payload: queue metrics, the
/// bounded binding-conflict and repair-action vocabularies, and guardrails
/// (mutating repairs require policy and idempotency).
pub(crate) fn build_connector_delivery_diagnostics(
    status: &GatewayStatusSnapshot,
    runtime_preview_payload: &Value,
) -> Value {
    let queue_delivery_failures =
        read_u64(runtime_preview_payload, "/metrics/queue_delivery_failures");
    let arbitration_suppressions =
        read_u64(runtime_preview_payload, "/metrics/arbitration_suppressions");
    let health_status = if status.counters.channel_reply_failures > 0 || queue_delivery_failures > 0
    {
        RuntimeHealthStatus::Degraded
    } else {
        RuntimeHealthStatus::Healthy
    };
    json!({
        "schema_version": 1,
        "status": health_status.as_str(),
        "metrics": {
            "queue_depth": status.counters.channel_router_queue_depth,
            "delivery_latency_ms": null,
            "retry_count": status.counters.channel_messages_queued,
            "dead_letter_count": status.counters.channel_messages_quarantined,
            "reply_failures": status.counters.channel_reply_failures,
            "arbitration_decisions": arbitration_suppressions,
            "queue_delivery_failures": queue_delivery_failures,
        },
        "binding_conflict_kinds": [
            "duplicate_active_binding",
            "stale_thread",
            "principal_mismatch",
            "workspace_mismatch",
            "expired_referenced",
            "parent_missing"
        ],
        "repair_actions": ["retry", "mark_failed", "reroute", "manual_action_required"],
        "safe_binding_repair_actions": ["detach", "rebind", "expire", "split", "mark_stale"],
        "guardrails": {
            "principal_mismatch_auto_widening_allowed": false,
            "idempotency_required": true,
            "policy_required_for_mutating_repair": true,
        }
    })
}

/// Builds the runtime watchdog payload, flagging active tool jobs whose
/// heartbeat went silent past the stuck threshold. Recovery actions are
/// split into safe (automatic) and manual sets; destructive recovery always
/// requires policy.
pub(crate) fn build_runtime_watchdog_diagnostics(
    generated_at_unix_ms: i64,
    self_healing_payload: &Value,
    tool_jobs: &[ToolJobRecord],
) -> Value {
    let stale_tool_jobs = tool_jobs
        .iter()
        .filter(|job| job.state.is_active())
        .filter(|job| {
            job.heartbeat_at_unix_ms
                .or(job.started_at_unix_ms)
                .map(|updated| {
                    generated_at_unix_ms.saturating_sub(updated) > STUCK_TOOL_JOB_AFTER_MS
                })
                .unwrap_or(false)
        })
        .count();
    json!({
        "schema_version": 1,
        "status": if stale_tool_jobs > 0 { "degraded" } else { "healthy" },
        "wait_kinds": [
            "provider_lease",
            "approval",
            "tool_job",
            "channel_delivery",
            "background_queue"
        ],
        "thresholds": {
            "tool_job_stuck_after_ms": STUCK_TOOL_JOB_AFTER_MS,
        },
        "observed": {
            "heartbeats": self_healing_payload.pointer("/heartbeats").and_then(Value::as_array).map_or(0, Vec::len),
            "active_incidents": self_healing_payload.pointer("/summary/active").and_then(Value::as_u64).unwrap_or_default(),
            "stale_tool_jobs": stale_tool_jobs,
        },
        "safe_recovery_actions": ["cleanup_typing", "mark_stale"],
        "manual_recovery_actions": ["cancel_run", "hard_stop_job", "reroute_delivery"],
        "diagnostic_event": {
            "event_kind": "runtime.watchdog.stuck_work",
            "destructive_recovery_requires_policy": true,
        }
    })
}

/// Builds the startup/memory/latency budget-gate payload comparing observed
/// values against the fixed budgets defined above; the thresholds are the
/// stable contract enforced by the performance smoke gate.
pub(crate) fn build_budget_gates_snapshot(
    status: &GatewayStatusSnapshot,
    memory_payload: &Value,
    runtime_preview_payload: &Value,
) -> Value {
    let provider_metrics = &status.model_provider.runtime_metrics;
    json!({
        "schema_version": 1,
        "startup": [
            startup_gate("config", STARTUP_CONFIG_BUDGET_MS, "config.load"),
            startup_gate("migrations", STARTUP_MIGRATION_BUDGET_MS, "journal.migrations"),
            startup_gate("vault", STARTUP_VAULT_BUDGET_MS, "vault.open"),
            startup_gate("provider_registry", STARTUP_PROVIDER_REGISTRY_BUDGET_MS, "model_provider.registry"),
            startup_gate("connectors", STARTUP_CONNECTOR_BUDGET_MS, "channel_router.init"),
            startup_gate("background_queues", STARTUP_BACKGROUND_QUEUE_BUDGET_MS, "orchestrator.background_queue"),
        ],
        "memory": {
            "daemon_startup_baseline_rss_bytes": DAEMON_STARTUP_BASELINE_RSS_BYTES,
            "agent_loop_baseline_rss_bytes": AGENT_LOOP_BASELINE_RSS_BYTES,
            "current_memory_entries": read_u64(memory_payload, "/usage/entries"),
            "current_memory_bytes": read_u64(memory_payload, "/usage/bytes"),
            "retention_max_entries": read_u64(memory_payload, "/retention/max_entries"),
            "retention_max_bytes": read_u64(memory_payload, "/retention/max_bytes"),
        },
        "latency": {
            "provider_prepass": latency_gate("provider_prepass", PROVIDER_PREPASS_BUDGET_MS, provider_metrics.avg_latency_ms),
            "context_assembly": latency_gate("context_assembly", CONTEXT_ASSEMBLY_BUDGET_MS, read_u64(runtime_preview_payload, "/metrics/retrieval_branch_latency_avg_ms")),
            "tool_catalog_build": latency_gate("tool_catalog_build", TOOL_CATALOG_BUILD_BUDGET_MS, 0),
            "route_planning": latency_gate("route_planning", ROUTE_PLANNING_BUDGET_MS, 0),
        },
        "regression_policy": {
            "thresholds_stable": true,
            "explain_required_on_failure": true,
            "ci_gate_script": "scripts/test/run-performance-smoke.sh",
        }
    })
}

/// Builds the static support-bundle collector contract: allowed inputs,
/// redaction posture (no raw secrets or prompts), size caps, and audit
/// expectations.
pub(crate) fn build_support_bundle_collector_contract() -> Value {
    json!({
        "schema_version": 1,
        "collector_inputs": [
            "config_summary",
            "runtime_health_snapshot",
            "recent_journal_refs",
            "provider_trace_refs",
            "tool_job_states",
            "redacted_logs"
        ],
        "redaction": {
            "secret_scanner": "palyra_common.redaction",
            "raw_secrets_allowed": false,
            "raw_prompts_allowed": false,
            "sensitive_payload_projection": "artifact_ref_or_redacted_placeholder",
        },
        "size_caps": {
            "default_max_bytes": 5_242_880,
            "minimum_max_bytes": 2_048,
            "oversized_payload_action": "replace_with_artifact_ref",
        },
        "audit": {
            "operator_action": "support_bundle.export",
            "observability_counters": true,
        }
    })
}

/// Builds the test-only ABI contract snapshot covering provider, tool,
/// channel-command, extension-manifest, and memory-provider surfaces;
/// consumed by the runtime contract snapshot checks.
#[cfg(test)]
pub(crate) fn build_contract_snapshot_suite() -> Value {
    let tool_config = crate::tool_protocol::ToolCallConfig {
        allowed_tools: vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()],
        max_calls_per_run: 4,
        execution_timeout_ms: 1_000,
        process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
            enabled: false,
            tier: crate::sandbox_runner::SandboxProcessRunnerTier::B,
            workspace_root: ".".into(),
            path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
            allowed_executables: Vec::new(),
            allow_interpreters: false,
            egress_enforcement_mode: crate::sandbox_runner::EgressEnforcementMode::Strict,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 1_000,
            memory_limit_bytes: 128 * 1024 * 1024,
            max_output_bytes: 64 * 1024,
        },
        wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
            enabled: false,
            allow_inline_modules: false,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        },
    };
    let request_context = ToolRequestContext {
        principal: "user:contract".to_owned(),
        device_id: Some("device:contract".to_owned()),
        channel: Some("ci".to_owned()),
        session_id: Some("session:contract".to_owned()),
        run_id: Some("run:contract".to_owned()),
        skill_id: None,
    };
    let tool_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &tool_config,
        browser_service_enabled: false,
        request_context: &request_context,
        provider_kind: "deterministic",
        provider_model_id: Some("contract-model"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let channel_registry = ChannelCommandRegistry::builtin();
    json!({
        "schema_version": CONTRACT_SNAPSHOT_SCHEMA_VERSION,
        "provider_abi": {
            "required_fields": [
                "kind",
                "provider_id",
                "credential_id",
                "model_id",
                "capabilities",
                "runtime_metrics",
                "health",
                "discovery",
                "registry",
                "route_selection"
            ],
            "runtime_metrics_fields": [
                "request_count",
                "error_count",
                "error_rate_bps",
                "total_retry_attempts",
                "avg_latency_ms",
                "max_latency_ms"
            ],
        },
        "tool_abi": {
            "snapshot_id": tool_snapshot.snapshot_id,
            "catalog_hash": tool_snapshot.catalog_hash,
            "tool_names": tool_snapshot.tools.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>(),
            "filtered_reason_codes": tool_snapshot.filtered_tools.iter().map(|tool| tool.reason_code.as_str()).collect::<Vec<_>>(),
        },
        "channel_command_abi": {
            "catalog_hash": channel_registry.catalog_hash(),
            "native_spec_count": channel_registry.native_specs().len(),
        },
        "extension_manifest_abi": {
            "required_fields": [
                "manifest_version",
                "id",
                "version",
                "compat.required_protocol_major",
                "compat.min_palyra_version",
                "capabilities",
                "target_surfaces"
            ],
            "reason_codes": [
                "manifest.schema_invalid",
                "manifest.abi_range_unsupported",
                "manifest.host_range_unsupported",
                "manifest.capability_denied"
            ],
        },
        "memory_provider_abi": {
            "required_fields": [
                "provider_id",
                "status",
                "degraded",
                "diagnostics",
                "evidence_refs",
                "score_breakdown"
            ],
            "redaction_required": true,
        }
    })
}

fn daemon_health_component(status: &GatewayStatusSnapshot) -> RuntimeHealthComponentSnapshot {
    let mut reasons = Vec::new();
    if status.status != "ok" {
        reasons.push("daemon.status_not_ok".to_owned());
    }
    component(
        "daemon",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("uptime_seconds", status.uptime_seconds),
            ("denied_requests", status.counters.denied_requests),
        ]),
        Vec::new(),
    )
}

fn connector_health_component(
    status: &GatewayStatusSnapshot,
    runtime_preview_payload: &Value,
) -> RuntimeHealthComponentSnapshot {
    let queue_delivery_failures =
        read_u64(runtime_preview_payload, "/metrics/queue_delivery_failures");
    let mut reasons = Vec::new();
    if status.counters.channel_reply_failures > 0 {
        reasons.push("connectors.reply_failures_present".to_owned());
    }
    if queue_delivery_failures > 0 {
        reasons.push("connectors.queue_delivery_failures_present".to_owned());
    }
    if status.counters.channel_messages_quarantined > 0 {
        reasons.push("connectors.quarantined_messages_present".to_owned());
    }
    component(
        "connectors",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("queue_depth", status.counters.channel_router_queue_depth),
            ("reply_failures", status.counters.channel_reply_failures),
            ("quarantined_messages", status.counters.channel_messages_quarantined),
            ("queue_delivery_failures", queue_delivery_failures),
        ]),
        vec!["inspect_connector_queue_health".to_owned()],
    )
}

fn provider_health_component(status: &GatewayStatusSnapshot) -> RuntimeHealthComponentSnapshot {
    let provider = &status.model_provider;
    let mut degraded = Vec::new();
    let mut unavailable = Vec::new();
    if provider.circuit_breaker.open {
        unavailable.push("providers.circuit_open".to_owned());
    }
    match provider.health.state.as_str() {
        "unavailable" | "failed" => unavailable.push("providers.health_unavailable".to_owned()),
        "degraded" | "missing" | "expired" => {
            degraded.push(format!("providers.health_{}", provider.health.state));
        }
        _ => {}
    }
    if provider.runtime_metrics.error_count > 0 {
        degraded.push("providers.runtime_errors_present".to_owned());
    }
    component(
        "providers",
        status_from_reason_count(degraded.len(), unavailable.len()),
        merged_reasons(degraded, unavailable),
        metrics(&[
            ("request_count", provider.runtime_metrics.request_count),
            ("error_count", provider.runtime_metrics.error_count),
            ("error_rate_bps", u64::from(provider.runtime_metrics.error_rate_bps)),
            ("avg_latency_ms", provider.runtime_metrics.avg_latency_ms),
            ("max_latency_ms", provider.runtime_metrics.max_latency_ms),
        ]),
        vec!["check_provider_health_and_auth_profile".to_owned()],
    )
}

fn auth_health_component(auth_payload: &Value) -> RuntimeHealthComponentSnapshot {
    let missing = read_u64(auth_payload, "/summary/missing");
    let expired = read_u64(auth_payload, "/summary/expired");
    let expiring = read_u64(auth_payload, "/summary/expiring");
    let mut reasons = Vec::new();
    if missing > 0 {
        reasons.push("auth.missing_profiles".to_owned());
    }
    if expired > 0 {
        reasons.push("auth.expired_profiles".to_owned());
    }
    if expiring > 0 {
        reasons.push("auth.expiring_profiles".to_owned());
    }
    component(
        "auth",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[("missing", missing), ("expired", expired), ("expiring", expiring)]),
        vec!["refresh_or_repair_auth_profiles".to_owned()],
    )
}

fn memory_health_component(memory_payload: &Value) -> RuntimeHealthComponentSnapshot {
    let provider_count =
        memory_payload.pointer("/providers").and_then(Value::as_array).map_or(0, Vec::len);
    let degraded_providers = memory_payload
        .pointer("/providers")
        .and_then(Value::as_array)
        .map(|providers| {
            providers
                .iter()
                .filter(|provider| {
                    provider.get("degraded").and_then(Value::as_bool).unwrap_or(false)
                        || provider.get("status").and_then(Value::as_str) == Some("degraded")
                })
                .count()
        })
        .unwrap_or_default();
    let embeddings_degraded = memory_embeddings_degraded(memory_payload);
    let embeddings_degraded_reason = memory_payload
        .pointer("/embeddings/degraded_reason_code")
        .and_then(Value::as_str)
        .filter(|reason| !reason.trim().is_empty());
    let mut reasons = Vec::new();
    if degraded_providers > 0 {
        reasons.push("memory.providers_degraded".to_owned());
    }
    if embeddings_degraded {
        reasons.push("memory.embeddings_degraded".to_owned());
        if let Some(reason) = embeddings_degraded_reason {
            reasons.push(format!("memory.embeddings_{reason}"));
        }
    }
    let mut repair_hints = vec!["run_memory_reindex_or_inspect_retrieval".to_owned()];
    if embeddings_degraded {
        repair_hints.push("configure_memory_embeddings_model".to_owned());
    }
    component(
        "memory",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("provider_count", provider_count as u64),
            ("degraded_provider_count", degraded_providers as u64),
            ("embeddings_degraded", u64::from(embeddings_degraded)),
            ("entries", read_u64(memory_payload, "/usage/entries")),
            ("bytes", read_u64(memory_payload, "/usage/bytes")),
        ]),
        repair_hints,
    )
}

fn memory_embeddings_degraded(memory_payload: &Value) -> bool {
    let posture = memory_payload.pointer("/embeddings/posture").and_then(Value::as_str);
    // An operator who explicitly opted into hash fallback chose that posture;
    // reporting it as degraded would nag about a deliberate decision. Only
    // implicit fallbacks (missing model, degraded postures) count.
    if posture == Some("explicit_hash_fallback") {
        return false;
    }
    if memory_payload.pointer("/embeddings/production_default_active").and_then(Value::as_bool)
        == Some(false)
    {
        return true;
    }
    if posture.is_some_and(|value| value.starts_with("degraded")) {
        return true;
    }
    memory_payload.pointer("/embeddings/mode").and_then(Value::as_str) == Some("hash_fallback")
}

fn jobs_health_component(
    generated_at_unix_ms: i64,
    tool_jobs: &[ToolJobRecord],
) -> RuntimeHealthComponentSnapshot {
    let counts = count_tool_jobs_by_state(tool_jobs);
    let stale_jobs = tool_jobs
        .iter()
        .filter(|job| job.state.is_active())
        .filter(|job| {
            job.heartbeat_at_unix_ms
                .or(job.started_at_unix_ms)
                .map(|updated| {
                    generated_at_unix_ms.saturating_sub(updated) > STUCK_TOOL_JOB_AFTER_MS
                })
                .unwrap_or(false)
        })
        .count() as u64;
    let mut reasons = Vec::new();
    if counts.get("orphaned").copied().unwrap_or_default() > 0 {
        reasons.push("jobs.orphaned_present".to_owned());
    }
    if counts.get("failed").copied().unwrap_or_default() > 0 {
        reasons.push("jobs.failed_present".to_owned());
    }
    if stale_jobs > 0 {
        reasons.push("jobs.stale_active_jobs".to_owned());
    }
    component(
        "jobs",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("active", tool_jobs.iter().filter(|job| job.state.is_active()).count() as u64),
            ("failed", counts.get("failed").copied().unwrap_or_default()),
            ("orphaned", counts.get("orphaned").copied().unwrap_or_default()),
            ("stale", stale_jobs),
        ]),
        vec!["inspect_or_retry_tool_jobs".to_owned()],
    )
}

fn routines_health_component(status: &GatewayStatusSnapshot) -> RuntimeHealthComponentSnapshot {
    let mut reasons = Vec::new();
    if status.counters.cron_runs_failed > 0 {
        reasons.push("routines.failed_runs_present".to_owned());
    }
    component(
        "routines",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("runs_started", status.counters.cron_runs_started),
            ("runs_completed", status.counters.cron_runs_completed),
            ("runs_failed", status.counters.cron_runs_failed),
            ("runs_skipped", status.counters.cron_runs_skipped),
        ]),
        vec!["inspect_routine_runs".to_owned()],
    )
}

fn extensions_health_component(
    skills_payload: &Value,
    plugins_payload: &Value,
) -> RuntimeHealthComponentSnapshot {
    let quarantined = read_u64(skills_payload, "/summary/quarantined");
    let disabled = read_u64(skills_payload, "/summary/disabled");
    let plugin_failures = read_u64(plugins_payload, "/summary/failures");
    let mut reasons = Vec::new();
    if quarantined > 0 {
        reasons.push("extensions.skills_quarantined".to_owned());
    }
    if disabled > 0 {
        reasons.push("extensions.skills_disabled".to_owned());
    }
    if plugin_failures > 0 {
        reasons.push("extensions.plugins_failed".to_owned());
    }
    component(
        "extensions",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("quarantined_skills", quarantined),
            ("disabled_skills", disabled),
            ("plugin_failures", plugin_failures),
        ]),
        vec!["run_extension_doctor_or_reaudit".to_owned()],
    )
}

fn storage_health_component(status: &GatewayStatusSnapshot) -> RuntimeHealthComponentSnapshot {
    let mut reasons = Vec::new();
    if status.counters.journal_persist_failures > 0 {
        reasons.push("storage.journal_persist_failures".to_owned());
    }
    if !status.storage.journal_hash_chain_enabled {
        reasons.push("storage.hash_chain_disabled".to_owned());
    }
    component(
        "storage",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("journal_events", status.counters.journal_events),
            ("journal_persist_failures", status.counters.journal_persist_failures),
            ("journal_redacted_events", status.counters.journal_redacted_events),
        ]),
        vec!["inspect_journal_storage".to_owned()],
    )
}

fn networked_workers_health_component(payload: &Value) -> RuntimeHealthComponentSnapshot {
    let failed_closed = read_u64(payload, "/fleet/failed_closed_workers");
    let orphaned = read_u64(payload, "/fleet/orphaned_workers");
    let mut reasons = Vec::new();
    if failed_closed > 0 {
        reasons.push("workers.failed_closed_present".to_owned());
    }
    if orphaned > 0 {
        reasons.push("workers.orphaned_present".to_owned());
    }
    component(
        "networked_workers",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("registered", read_u64(payload, "/fleet/registered_workers")),
            ("failed_closed", failed_closed),
            ("orphaned", orphaned),
        ]),
        vec!["drain_or_quarantine_worker".to_owned()],
    )
}

fn support_bundle_health_component(payload: &Value) -> RuntimeHealthComponentSnapshot {
    let failures = read_u64(payload, "/failures");
    let mut reasons = Vec::new();
    if failures > 0 {
        reasons.push("support_bundle.export_failures_present".to_owned());
    }
    component(
        "support_bundle",
        status_from_reason_count(reasons.len(), 0),
        reasons,
        metrics(&[
            ("attempts", read_u64(payload, "/attempts")),
            ("successes", read_u64(payload, "/successes")),
            ("failures", failures),
        ]),
        vec!["queue_or_export_fresh_support_bundle".to_owned()],
    )
}

fn provider_metrics_json(metrics: &ProviderRuntimeMetricsSnapshot) -> Value {
    json!({
        "requests_total": metrics.request_count,
        "errors_total": metrics.error_count,
        "error_rate_bps": metrics.error_rate_bps,
        "retry_attempts_total": metrics.total_retry_attempts,
        "prompt_tokens_total": metrics.total_prompt_tokens,
        "completion_tokens_total": metrics.total_completion_tokens,
        "avg_latency_ms": metrics.avg_latency_ms,
        "max_latency_ms": metrics.max_latency_ms,
    })
}

fn status_from_reason_count(
    degraded_count: usize,
    unavailable_count: usize,
) -> RuntimeHealthStatus {
    if unavailable_count > 0 {
        RuntimeHealthStatus::Unavailable
    } else if degraded_count > 0 {
        RuntimeHealthStatus::Degraded
    } else {
        RuntimeHealthStatus::Healthy
    }
}

fn merged_reasons(mut degraded: Vec<String>, unavailable: Vec<String>) -> Vec<String> {
    degraded.extend(unavailable);
    degraded.sort();
    degraded.dedup();
    degraded
}

fn component(
    component: &str,
    status: RuntimeHealthStatus,
    mut reason_codes: Vec<String>,
    metrics: BTreeMap<String, u64>,
    repair_hints: Vec<String>,
) -> RuntimeHealthComponentSnapshot {
    reason_codes.sort();
    reason_codes.dedup();
    RuntimeHealthComponentSnapshot {
        component: component.to_owned(),
        status,
        reason_codes,
        metrics,
        repair_hints,
    }
}

fn metrics(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
    entries.iter().map(|(key, value)| ((*key).to_owned(), *value)).collect()
}

fn read_u64(value: &Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(Value::as_u64).unwrap_or_default()
}

fn count_tool_jobs_by_state(tool_jobs: &[ToolJobRecord]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::<String, u64>::new();
    for job in tool_jobs {
        *counts.entry(job.state.as_str().to_owned()).or_default() += 1;
    }
    counts
}

fn all_tool_job_states() -> &'static [ToolJobState] {
    &[
        ToolJobState::Queued,
        ToolJobState::Starting,
        ToolJobState::Running,
        ToolJobState::Draining,
        ToolJobState::Cancelling,
        ToolJobState::Completed,
        ToolJobState::Failed,
        ToolJobState::Cancelled,
        ToolJobState::Expired,
        ToolJobState::Orphaned,
    ]
}

fn push_help(output: &mut String, name: &str, help: &str, metric_type: &str) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(metric_type);
    output.push('\n');
}

fn push_sample(output: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    output.push_str(name);
    if !labels.is_empty() {
        output.push('{');
        for (index, (key, raw_value)) in labels.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(escape_prometheus_label(raw_value).as_str());
            output.push('"');
        }
        output.push('}');
    }
    output.push(' ');
    output.push_str(value.to_string().as_str());
    output.push('\n');
}

fn escape_prometheus_label(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn startup_gate(component: &str, budget_ms: u64, source: &str) -> Value {
    json!({
        "component": component,
        "budget_ms": budget_ms,
        "source": source,
        "status": "contract_ready",
    })
}

fn latency_gate(name: &str, budget_ms: u64, observed_ms: u64) -> Value {
    json!({
        "name": name,
        "budget_ms": budget_ms,
        "observed_ms": observed_ms,
        "status": if observed_ms > budget_ms { "over_budget" } else { "within_budget" },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_tool_job(job_id: &str, state: ToolJobState, updated_at: i64) -> ToolJobRecord {
        ToolJobRecord {
            job_id: job_id.to_owned(),
            owner_principal: "user:test".to_owned(),
            device_id: "device:test".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: "session:test".to_owned(),
            run_id: "run:test".to_owned(),
            tool_call_id: "toolcall:test".to_owned(),
            tool_name: "palyra.echo".to_owned(),
            backend: "local".to_owned(),
            backend_reason_code: None,
            command_sha256: "0".repeat(64),
            program_sha256: None,
            state,
            attempt_count: 1,
            max_attempts: 1,
            retry_allowed: false,
            idempotency_key: None,
            cancellation_handle: None,
            artifact_refs_json: None,
            tail_preview: String::new(),
            stdout_artifact_id: None,
            stderr_artifact_id: None,
            last_error: None,
            state_reason: None,
            created_at_unix_ms: updated_at,
            updated_at_unix_ms: updated_at,
            started_at_unix_ms: Some(updated_at),
            heartbeat_at_unix_ms: Some(updated_at),
            completed_at_unix_ms: None,
            expires_at_unix_ms: None,
            legal_hold: false,
            active_ref_count: 0,
            lease_expires_at_unix_ms: None,
        }
    }

    #[test]
    fn watchdog_marks_stale_tool_jobs_without_destructive_recovery() {
        let stale_job = empty_tool_job("job-1", ToolJobState::Running, 1_000);
        let payload = build_runtime_watchdog_diagnostics(
            1_000 + STUCK_TOOL_JOB_AFTER_MS + 1,
            &json!({ "summary": { "active": 0 }, "heartbeats": [] }),
            &[stale_job],
        );

        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["observed"]["stale_tool_jobs"], 1);
        assert_eq!(payload["diagnostic_event"]["destructive_recovery_requires_policy"], true);
    }

    #[test]
    fn memory_health_component_reports_embeddings_degradation() {
        let component = memory_health_component(&json!({
            "embeddings": {
                "mode": "hash_fallback",
                "posture": "degraded_config_fallback",
                "production_default_active": false,
                "degraded_reason_code": "embeddings_model_not_configured"
            },
            "providers": [],
            "usage": { "entries": 3, "bytes": 128 }
        }));

        assert_eq!(component.status, RuntimeHealthStatus::Degraded);
        assert!(component.reason_codes.contains(&"memory.embeddings_degraded".to_owned()));
        assert!(component
            .reason_codes
            .contains(&"memory.embeddings_embeddings_model_not_configured".to_owned()));
        assert_eq!(component.metrics.get("embeddings_degraded"), Some(&1_u64));
        assert!(component.repair_hints.contains(&"configure_memory_embeddings_model".to_owned()));
    }

    #[test]
    fn memory_health_component_allows_explicit_hash_fallback() {
        let component = memory_health_component(&json!({
            "embeddings": {
                "mode": "hash_fallback",
                "posture": "explicit_hash_fallback",
                "production_default_active": false,
                "degraded_reason_code": "explicit_hash_fallback"
            },
            "providers": [],
            "usage": { "entries": 0, "bytes": 0 }
        }));

        assert_eq!(component.status, RuntimeHealthStatus::Healthy);
        assert!(!component.reason_codes.contains(&"memory.embeddings_degraded".to_owned()));
        assert_eq!(component.metrics.get("embeddings_degraded"), Some(&0_u64));
    }

    #[test]
    fn prometheus_renderer_uses_bounded_labels() {
        let mut rendered = String::new();
        push_sample(
            &mut rendered,
            "palyra_model_provider_requests_total",
            &[("provider_kind", "deterministic\n\"provider\"")],
            2,
        );

        assert!(rendered.contains(
            "palyra_model_provider_requests_total{provider_kind=\"deterministic\\n\\\"provider\\\"\"} 2"
        ));
        assert!(!rendered.contains("principal"));
        assert!(!rendered.contains("session_id"));
    }

    #[test]
    fn contract_snapshot_suite_covers_phase11_abi_surfaces() {
        let snapshot = build_contract_snapshot_suite();
        assert_eq!(snapshot["schema_version"], CONTRACT_SNAPSHOT_SCHEMA_VERSION);
        assert!(snapshot["provider_abi"]["required_fields"]
            .as_array()
            .expect("provider fields")
            .iter()
            .any(|field| field == "runtime_metrics"));
        assert!(snapshot["tool_abi"]["snapshot_id"]
            .as_str()
            .expect("snapshot id")
            .starts_with("toolcat_"));
        assert!(
            snapshot["channel_command_abi"]["native_spec_count"].as_u64().unwrap_or_default() > 0
        );
        assert_eq!(snapshot["memory_provider_abi"]["redaction_required"], true);
    }
}
