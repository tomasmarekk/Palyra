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

use palyra_common::feature_rollouts::{FeatureRolloutSetting, FeatureRolloutSource};
use palyra_common::redaction::{
    is_sensitive_key, redact_diagnostic_text, redact_internal_runtime_paths,
};
use palyra_common::runtime_contracts::runtime_error_contract_snapshot;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) mod shadow_differential;

#[cfg(test)]
use crate::{
    application::{
        channel_commands::ChannelCommandRegistry,
        tool_registry::{
            build_model_visible_tool_catalog_snapshot, effective_tool_surface_report,
            ToolCatalogBuildRequest, ToolExposureSurface,
        },
    },
    tool_protocol::ToolRequestContext,
};
use crate::{
    config::FeatureRolloutsConfig,
    gateway::{GatewayStatusSnapshot, ManagedRuntimeHealthSnapshot},
    journal::{ToolJobRecord, ToolJobState},
    model_provider::ProviderRuntimeMetricsSnapshot,
    routines::{
        operations::{
            ROUTINE_CRON_SECURITY_SCHEMA_VERSION, ROUTINE_LEASE_LEDGER_SCHEMA_VERSION,
            ROUTINE_STARTUP_CATCH_UP_SCHEMA_VERSION,
        },
        ROUTINE_RUN_LEASE_TTL_MS,
    },
};
#[cfg(test)]
use palyra_common::runtime_contracts::{
    public_runtime_contract_snapshot, validate_public_contract_snapshot,
    PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION,
};
#[cfg(test)]
use palyra_plugins_sdk::{plugin_sdk_contract_snapshot, PLUGIN_SDK_CONTRACT_SNAPSHOT_VERSION};
#[cfg(test)]
use palyra_skills::skill_manifest_contract_snapshot;

/// Schema version stamped on runtime health snapshots; bump on any
/// backward-incompatible shape change.
pub(crate) const RUNTIME_HEALTH_SCHEMA_VERSION: u32 = 1;
/// Schema version for the agent runtime metrics JSON payload.
pub(crate) const AGENT_RUNTIME_METRICS_SCHEMA_VERSION: u32 = 1;
/// Schema version for the OTel span contract payload.
pub(crate) const OTEL_SPAN_CONTRACT_SCHEMA_VERSION: u32 = 1;
/// Schema version for daemon lifecycle drain/resume diagnostics.
pub(crate) const DAEMON_LIFECYCLE_SCHEMA_VERSION: u32 = 1;
/// Schema version for bounded JSONL runtime timeline events.
pub(crate) const RUNTIME_TIMELINE_SCHEMA_VERSION: u32 = 1;
/// Schema version for the Prometheus metric catalog and label policy.
pub(crate) const METRICS_CATALOG_SCHEMA_VERSION: u32 = 1;
/// Schema version for redacted local trace export records.
pub(crate) const TRACE_EXPORT_SCHEMA_VERSION: u32 = 1;
/// Schema version for per-run stage timing records.
pub(crate) const RUN_STAGE_TIMING_SCHEMA_VERSION: u32 = 1;
/// Schema version for per-run runtime path summaries.
pub(crate) const RUN_RUNTIME_PATH_SCHEMA_VERSION: u32 = 1;
/// Journal/tape event name for terminal runtime path summaries.
pub(crate) const RUN_RUNTIME_PATH_SUMMARY_EVENT: &str = "run.runtime_path_summary";
/// Schema version for runtime invariant and error-taxonomy diagnostics.
pub(crate) const RUNTIME_ERROR_DIAGNOSTICS_SCHEMA_VERSION: u32 = 1;
/// Schema version for the test-only ABI contract snapshot suite.
#[cfg(test)]
pub(crate) const CONTRACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Compatibility identity for the managed coding contract snapshot.
#[cfg(test)]
const MANAGED_CODING_CONTRACT_SNAPSHOT_VERSION: &str = "managed-coding-contracts.v1";

#[cfg(test)]
const MANAGED_CODING_SCHEMA_SOURCES: &[(&str, &str)] = &[
    (
        "process_session_record",
        include_str!("../../../schemas/json/common/process-session-record.v2.json"),
    ),
    (
        "process_output_page",
        include_str!("../../../schemas/json/common/process-output-page.v2.json"),
    ),
    (
        "pty_session_descriptor",
        include_str!("../../../schemas/json/common/pty-session-descriptor.v1.json"),
    ),
    (
        "local_resource_registry",
        include_str!("../../../schemas/json/common/local-resource-registry.v2.json"),
    ),
    (
        "resource_pressure_snapshot",
        include_str!("../../../schemas/json/common/resource-pressure-snapshot.v1.json"),
    ),
    (
        "managed_worktree_registry",
        include_str!("../../../schemas/json/common/managed-worktree-registry.v2.json"),
    ),
    (
        "worktree_snapshot_descriptor",
        include_str!("../../../schemas/json/common/worktree-snapshot-descriptor.v1.json"),
    ),
    (
        "worktree_restore_report",
        include_str!("../../../schemas/json/common/worktree-restore-report.v1.json"),
    ),
    ("lsp_registry", include_str!("../../../schemas/json/common/lsp-registry.v2.json")),
    (
        "lsp_diagnostics_snapshot",
        include_str!("../../../schemas/json/common/lsp-diagnostics-snapshot.v2.json"),
    ),
    (
        "diagnostics_baseline",
        include_str!("../../../schemas/json/common/diagnostics-baseline.v2.json"),
    ),
    ("diagnostics_delta", include_str!("../../../schemas/json/common/diagnostics-delta.v2.json")),
    (
        "coding_runtime_capability_report",
        include_str!("../../../schemas/json/common/coding-runtime-capability-report.v2.json"),
    ),
    (
        "coding_patch_outcome",
        include_str!("../../../schemas/json/common/coding-patch-outcome.v2.json"),
    ),
    (
        "coding_command_status",
        include_str!("../../../schemas/json/common/coding-command-status.v2.json"),
    ),
    (
        "coding_task_cleanup_outcome",
        include_str!("../../../schemas/json/common/coding-task-cleanup-outcome.v2.json"),
    ),
    (
        "managed_coding_diagnostics",
        include_str!("../../../schemas/json/common/managed-coding-diagnostics.v1.json"),
    ),
    (
        "managed_coding_recovery",
        include_str!("../../../schemas/json/common/managed-coding-recovery.v1.json"),
    ),
    (
        "coding_runtime_soak_report",
        include_str!("../../../schemas/json/common/coding-runtime-soak-report.v1.json"),
    ),
];

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
const DIAGNOSTICS_TIMELINE_PAYLOAD_LIMIT_BYTES: usize = 2_048;
const PROMETHEUS_SERIES_CAP: u64 = 256;
const RUN_STAGE_NAMES: &[&str] = &[
    "prepare",
    "context_assembly",
    "provider_request",
    "first_token",
    "first_tool_call",
    "tool_wait",
    "compaction",
    "finalization",
    "delivery",
    "abort_settle",
];
const FORBIDDEN_METRIC_LABEL_KEYS: &[&str] =
    &["run_id", "session_id", "tool_call_id", "path", "principal", "raw_user", "prompt"];
const BOUNDED_METRIC_LABEL_KEYS: &[&str] = &[
    "action",
    "boundary",
    "component",
    "phase",
    "provider_kind",
    "scope",
    "state",
    "stat",
    "status",
    "token_type",
];

/// Builds the metadata-only runtime invariant and error-taxonomy diagnostics contract.
///
/// The payload is static and deterministic. It does not inspect live errors or raw runtime
/// payloads, and it documents the existing terminal runtime-path trace field that carries
/// stable reason codes without changing that schema-version-1 tape event.
#[must_use]
pub(crate) fn build_runtime_error_contract_diagnostics() -> Value {
    let error_taxonomy = runtime_error_contract_snapshot();
    json!({
        "schema_version": RUNTIME_ERROR_DIAGNOSTICS_SCHEMA_VERSION,
        "status": "contract_ready",
        "reason_code": "runtime.error_contract.ready",
        "invariant_contract": {
            "schema_version": RUNTIME_ERROR_DIAGNOSTICS_SCHEMA_VERSION,
            "authority": "host_runtime",
            "journal_authoritative": true,
            "diagnostics_authoritative": false,
            "descriptors": error_taxonomy["invariants"].clone(),
        },
        "error_taxonomy": error_taxonomy,
        "metadata_trace": {
            "event_name": RUN_RUNTIME_PATH_SUMMARY_EVENT,
            "terminal_reason_field": "terminal_reason",
            "stable_reason_code_required": true,
            "full_error_envelope_embedded": false,
            "schema_version": palyra_common::metadata_trace::METADATA_TRACE_SCHEMA_VERSION,
            "status": "always_on",
            "storage": "append_only_segments",
            "segment_statuses": ["complete", "interrupted", "corrupt_suffix_isolated"],
            "redaction_level": "metadata_only",
            "failure_signal": "counters.metadata_trace_failures",
            "terminal_reason_source_event": RUN_RUNTIME_PATH_SUMMARY_EVENT,
            "rich_trace_approval_required": true,
            "reason_code": "runtime.error_contract.trace_projection_ready",
            "always_on_reason_code": "metadata_trace.always_on.ready",
        },
    })
}

/// Run-time path posture for one subsystem in the agent loop.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunRuntimePathSubsystem {
    pub(crate) state: String,
    pub(crate) reason_code: String,
    pub(crate) rollout_flag: String,
    pub(crate) rollout_source: String,
    pub(crate) default_posture: String,
}

/// Redacted summary of which host-owned runtime paths a terminal run used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunRuntimePathSummary {
    pub(crate) schema_version: u32,
    pub(crate) event_name: String,
    pub(crate) redaction_level: String,
    pub(crate) journal_surface: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) terminal_reason: Option<String>,
    pub(crate) attempt_owner: String,
    pub(crate) preserved_for_terminal_states: Vec<String>,
    pub(crate) preserved_for_failure_modes: Vec<String>,
    pub(crate) subsystems: BTreeMap<String, RunRuntimePathSubsystem>,
}

/// Builds a redacted runtime-path summary from resolved daemon rollout config.
#[must_use]
pub(crate) fn build_run_runtime_path_summary(
    config: &FeatureRolloutsConfig,
    terminal_state: Option<&str>,
    terminal_reason: Option<&str>,
    attempt_owner: Option<&str>,
) -> RunRuntimePathSummary {
    let subsystems = [
        ("harness", "feature_rollouts.agent_harness_runtime", config.agent_harness_runtime),
        ("context_engine", "feature_rollouts.context_engine", config.context_engine),
        (
            "tool_gate",
            "feature_rollouts.execution_gate_pipeline_v2",
            config.execution_gate_pipeline_v2,
        ),
        ("hooks", "feature_rollouts.inline_runtime_hooks", config.inline_runtime_hooks),
        ("provider_recovery", "feature_rollouts.provider_recovery", config.provider_recovery),
        ("compaction", "feature_rollouts.compaction_safeguard", config.compaction_safeguard),
        ("lsp", "feature_rollouts.lsp_service", config.lsp_service),
        ("verification", "feature_rollouts.verification_runtime", config.verification_runtime),
        ("delivery", "feature_rollouts.delivery_arbitration", config.delivery_arbitration),
    ]
    .into_iter()
    .map(|(name, rollout_flag, setting)| {
        (name.to_owned(), runtime_path_subsystem(rollout_flag, setting))
    })
    .collect();

    RunRuntimePathSummary {
        schema_version: RUN_RUNTIME_PATH_SCHEMA_VERSION,
        event_name: RUN_RUNTIME_PATH_SUMMARY_EVENT.to_owned(),
        redaction_level: "metadata_only".to_owned(),
        journal_surface: "orchestrator_tape_events".to_owned(),
        terminal_state: terminal_state.map(ToOwned::to_owned),
        terminal_reason: terminal_reason
            .map(|reason| sanitize_diagnostics_string(reason, Some("terminal_reason"))),
        attempt_owner: sanitize_diagnostics_string(
            attempt_owner.unwrap_or({
                if config.agent_harness_runtime.enabled {
                    "harness_runtime_v1"
                } else {
                    "embedded_run_stream"
                }
            }),
            Some("attempt_owner"),
        ),
        preserved_for_terminal_states: vec![
            "done".to_owned(),
            "failed".to_owned(),
            "cancelled".to_owned(),
        ],
        preserved_for_failure_modes: vec![
            "cancel".to_owned(),
            "timeout".to_owned(),
            "policy_denial".to_owned(),
            "provider_failure".to_owned(),
            "pre_provider_failure".to_owned(),
        ],
        subsystems,
    }
}

fn runtime_path_subsystem(
    rollout_flag: &str,
    setting: FeatureRolloutSetting,
) -> RunRuntimePathSubsystem {
    let state = if setting.enabled { "enabled" } else { "disabled" };
    RunRuntimePathSubsystem {
        state: state.to_owned(),
        reason_code: format!("runtime_path.rollout.{state}"),
        rollout_flag: rollout_flag.to_owned(),
        rollout_source: feature_rollout_source_as_str(setting.source).to_owned(),
        default_posture: rollout_default_posture(setting).to_owned(),
    }
}

const fn feature_rollout_source_as_str(source: FeatureRolloutSource) -> &'static str {
    match source {
        FeatureRolloutSource::Default => "default",
        FeatureRolloutSource::Config => "config",
        FeatureRolloutSource::Env => "env",
    }
}

const fn rollout_default_posture(setting: FeatureRolloutSetting) -> &'static str {
    match (setting.source, setting.enabled) {
        (FeatureRolloutSource::Default, false) => "default_off",
        (FeatureRolloutSource::Default, true) => "default_on",
        (FeatureRolloutSource::Config, false) => "disabled_by_config",
        (FeatureRolloutSource::Config, true) => "enabled_by_config",
        (FeatureRolloutSource::Env, false) => "disabled_by_env",
        (FeatureRolloutSource::Env, true) => "enabled_by_env",
    }
}

/// Coarse process lifecycle state surfaced by drain/restart diagnostics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DaemonLifecycleState {
    Running,
    Draining,
    Drained,
    ShutdownRequested,
}

impl DaemonLifecycleState {
    pub(crate) const fn accepts_new_runs(self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Aggregated restart-resume guard counters shown in diagnostics and bundles.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResumeGuardCounters {
    pub(crate) incomplete_runs: u64,
    pub(crate) safe_to_resume: u64,
    pub(crate) requires_operator_review: u64,
    pub(crate) stale_tool_execution: u64,
    pub(crate) approval_pending: u64,
    pub(crate) interrupted_by_shutdown: u64,
}

/// Operator-facing lifecycle snapshot for drain and restart-resume posture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DaemonLifecycleSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) state: DaemonLifecycleState,
    pub(crate) accepts_new_runs: bool,
    pub(crate) active_runs: u64,
    pub(crate) queue_depth: u64,
    pub(crate) pending_approvals: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) drain_started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) shutdown_requested_at_unix_ms: Option<i64>,
    pub(crate) resume_guard: ResumeGuardCounters,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) repair_hints: Vec<String>,
}

/// One bounded diagnostics timeline event. Payload values are redacted before
/// JSONL export; labels stay low-cardinality and correlation ids are isolated
/// from Prometheus labels.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiagnosticsTimelineEvent {
    pub(crate) schema_version: u32,
    pub(crate) monotonic_ms: u64,
    pub(crate) wall_time_unix_ms: i64,
    pub(crate) component: String,
    pub(crate) phase: String,
    pub(crate) outcome: String,
    pub(crate) correlation: BTreeMap<String, String>,
    pub(crate) payload: Value,
    pub(crate) redaction_level: String,
}

/// One redacted span record for local JSONL trace export.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TraceSpanRecord {
    pub(crate) schema_version: u32,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_span_id: Option<String>,
    pub(crate) name: String,
    pub(crate) component: String,
    pub(crate) started_at_unix_ms: i64,
    pub(crate) duration_ms: u64,
    pub(crate) outcome: String,
    pub(crate) correlation: BTreeMap<String, String>,
    pub(crate) attributes: Value,
    pub(crate) redaction_level: String,
}

/// Phase that owns a timeout. This keeps provider hard timeouts, provider
/// stream idleness, tool wait timeouts, and abort settling separate in exports.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunStageTimeoutKind {
    ProviderHard,
    ProviderIdle,
    ToolWait,
    AbortSettle,
}

impl RunStageTimeoutKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderHard => "provider_hard_timeout",
            Self::ProviderIdle => "provider_idle_timeout",
            Self::ToolWait => "tool_wait_timeout",
            Self::AbortSettle => "abort_settle_timeout",
        }
    }
}

/// Raw stage timing input collected by a run loop or reconstructed from a
/// replay capture. Payload-bearing fields deliberately stay out of this type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunStageTimingInput {
    pub(crate) stage: String,
    pub(crate) started_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) first_signal_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_kind: Option<RunStageTimeoutKind>,
    pub(crate) outcome: String,
}

/// Redacted and bounded stage timing record stored in run traces and support
/// bundles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunStageTimingRecord {
    pub(crate) schema_version: u32,
    pub(crate) stage: String,
    pub(crate) started_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) first_signal_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) idle_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_classification: Option<String>,
    pub(crate) outcome: String,
    pub(crate) diagnostics_code: String,
    pub(crate) redaction_level: String,
}

/// Full timing summary for one run. Missing stages are reported explicitly so
/// support can distinguish "not observed" from "observed and fast".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunStageTimingReport {
    pub(crate) schema_version: u32,
    pub(crate) redaction_level: String,
    pub(crate) records: Vec<RunStageTimingRecord>,
    pub(crate) missing_stages: Vec<String>,
    pub(crate) timeout_classifications: Vec<String>,
    pub(crate) idle_breaker_triggered: bool,
}

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

/// Unified lifecycle state for extension/runtime components.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ComponentHealthState {
    Healthy,
    Degraded,
    Quarantined,
    DisabledByPolicy,
    Incompatible,
    PendingUpgrade,
    FailedPreflight,
}

impl ComponentHealthState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Quarantined => "quarantined",
            Self::DisabledByPolicy => "disabled_by_policy",
            Self::Incompatible => "incompatible",
            Self::PendingUpgrade => "pending_upgrade",
            Self::FailedPreflight => "failed_preflight",
        }
    }
}

/// Observable outcome of one component call or preflight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentCallOutcome {
    pub(crate) component: String,
    pub(crate) capability: String,
    pub(crate) duration_ms: u64,
    #[serde(default)]
    pub(crate) timed_out: bool,
    #[serde(default)]
    pub(crate) capability_denied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resource_event: Option<String>,
}

/// Snapshot of one component's health-registry state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentHealthRecord {
    pub(crate) component: String,
    pub(crate) state: ComponentHealthState,
    pub(crate) consecutive_failures: u32,
    pub(crate) total_failures: u64,
    pub(crate) quarantine_until_unix_ms: Option<i64>,
    pub(crate) last_error_code: Option<String>,
    pub(crate) last_capability: Option<String>,
    pub(crate) updated_at_unix_ms: i64,
}

/// Context fallback decision emitted when a component cannot be used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentContextFallback {
    pub(crate) component: String,
    pub(crate) use_fallback: bool,
    pub(crate) state: ComponentHealthState,
    pub(crate) audit_reason: String,
}

/// Audit event for health-registry state changes that require operator traceability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentHealthAuditEvent {
    pub(crate) event_kind: String,
    pub(crate) actor: String,
    pub(crate) component: String,
    pub(crate) from_state: ComponentHealthState,
    pub(crate) to_state: ComponentHealthState,
    pub(crate) recorded_at_unix_ms: i64,
}

/// In-memory health registry for plugins, skills, MCP servers, and external packages.
#[derive(Debug, Clone)]
pub(crate) struct ComponentHealthRegistry {
    records: BTreeMap<String, ComponentHealthRecord>,
    quarantine_after_failures: u32,
    quarantine_backoff_ms: i64,
}

impl Default for ComponentHealthRegistry {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            quarantine_after_failures: 3,
            quarantine_backoff_ms: 60_000,
        }
    }
}

impl ComponentHealthRegistry {
    pub(crate) fn record_outcome(
        &mut self,
        outcome: ComponentCallOutcome,
        now_unix_ms: i64,
    ) -> ComponentHealthRecord {
        let failed = outcome.timed_out || outcome.capability_denied || outcome.error_code.is_some();
        let record = self.records.entry(outcome.component.clone()).or_insert_with(|| {
            ComponentHealthRecord {
                component: outcome.component.clone(),
                state: ComponentHealthState::Healthy,
                consecutive_failures: 0,
                total_failures: 0,
                quarantine_until_unix_ms: None,
                last_error_code: None,
                last_capability: None,
                updated_at_unix_ms: now_unix_ms,
            }
        });
        record.last_capability = Some(outcome.capability);
        record.updated_at_unix_ms = now_unix_ms;
        if matches!(
            record.state,
            ComponentHealthState::DisabledByPolicy
                | ComponentHealthState::Incompatible
                | ComponentHealthState::PendingUpgrade
                | ComponentHealthState::FailedPreflight
        ) {
            return record.clone();
        }
        if failed {
            record.consecutive_failures = record.consecutive_failures.saturating_add(1);
            record.total_failures = record.total_failures.saturating_add(1);
            record.last_error_code = outcome.error_code.or_else(|| {
                if outcome.timed_out {
                    Some("timeout".to_owned())
                } else if outcome.capability_denied {
                    Some("capability_denied".to_owned())
                } else {
                    outcome.resource_event
                }
            });
            if record.consecutive_failures >= self.quarantine_after_failures {
                record.state = ComponentHealthState::Quarantined;
                let multiplier = i64::from(record.consecutive_failures);
                record.quarantine_until_unix_ms = Some(
                    now_unix_ms
                        .saturating_add(self.quarantine_backoff_ms.saturating_mul(multiplier)),
                );
            } else {
                record.state = ComponentHealthState::Degraded;
            }
        } else if record.state != ComponentHealthState::Quarantined {
            record.state = ComponentHealthState::Healthy;
            record.consecutive_failures = 0;
            record.last_error_code = None;
            record.quarantine_until_unix_ms = None;
        }
        record.clone()
    }

    pub(crate) fn mark_state(
        &mut self,
        component: &str,
        state: ComponentHealthState,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> ComponentHealthRecord {
        let record =
            self.records.entry(component.to_owned()).or_insert_with(|| ComponentHealthRecord {
                component: component.to_owned(),
                state,
                consecutive_failures: 0,
                total_failures: 0,
                quarantine_until_unix_ms: None,
                last_error_code: None,
                last_capability: None,
                updated_at_unix_ms: now_unix_ms,
            });
        record.state = state;
        record.last_error_code = Some(reason_code.to_owned());
        record.updated_at_unix_ms = now_unix_ms;
        record.clone()
    }

    pub(crate) fn unquarantine_with_audit(
        &mut self,
        component: &str,
        actor: &str,
        now_unix_ms: i64,
    ) -> Option<ComponentHealthAuditEvent> {
        let actor = actor.trim();
        if actor.is_empty() {
            return None;
        }
        let record = self.records.get_mut(component)?;
        if record.state != ComponentHealthState::Quarantined {
            return None;
        }
        if record.quarantine_until_unix_ms.is_some_and(|until| now_unix_ms < until) {
            return None;
        }
        let from_state = record.state;
        record.state = ComponentHealthState::Degraded;
        record.consecutive_failures = 0;
        record.quarantine_until_unix_ms = None;
        record.updated_at_unix_ms = now_unix_ms;
        Some(ComponentHealthAuditEvent {
            event_kind: "component_health.unquarantined".to_owned(),
            actor: actor.to_owned(),
            component: component.to_owned(),
            from_state,
            to_state: record.state,
            recorded_at_unix_ms: now_unix_ms,
        })
    }

    pub(crate) fn fallback_for_component(&self, component: &str) -> ComponentContextFallback {
        let state = self
            .records
            .get(component)
            .map(|record| record.state)
            .unwrap_or(ComponentHealthState::Healthy);
        let use_fallback =
            !matches!(state, ComponentHealthState::Healthy | ComponentHealthState::Degraded);
        ComponentContextFallback {
            component: component.to_owned(),
            use_fallback,
            state,
            audit_reason: if use_fallback {
                format!("component {component} is {}", state.as_str())
            } else {
                "component is available".to_owned()
            },
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<ComponentHealthRecord> {
        self.records.values().cloned().collect()
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
    mcp_payload: &Value,
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
        mcp_health_component(mcp_payload),
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
            "failed_total": status.counters.orchestrator_runs_failed,
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

/// Builds a drain-aware lifecycle snapshot for admin, CLI, and support-bundle surfaces.
pub(crate) fn build_daemon_lifecycle_snapshot(
    state: DaemonLifecycleState,
    active_runs: u64,
    queue_depth: u64,
    pending_approvals: u64,
    drain_started_at_unix_ms: Option<i64>,
    shutdown_requested_at_unix_ms: Option<i64>,
    resume_guard: ResumeGuardCounters,
) -> DaemonLifecycleSnapshot {
    let mut reason_codes = Vec::new();
    let mut repair_hints = Vec::new();
    if !state.accepts_new_runs() {
        reason_codes.push("daemon.lifecycle.not_accepting_new_runs".to_owned());
        repair_hints.push("wait for active runs to drain before restart".to_owned());
    }
    if resume_guard.requires_operator_review > 0 {
        reason_codes.push("daemon.lifecycle.resume_requires_operator_review".to_owned());
        repair_hints
            .push("inspect resume guard findings before replaying interrupted runs".to_owned());
    }
    if resume_guard.approval_pending > 0 {
        reason_codes.push("daemon.lifecycle.approval_pending_after_restart".to_owned());
        repair_hints.push("resolve pending approvals before continuing affected runs".to_owned());
    }
    if resume_guard.stale_tool_execution > 0 {
        reason_codes.push("daemon.lifecycle.stale_tool_execution".to_owned());
        repair_hints.push("verify tool side effects before resuming the run".to_owned());
    }
    if active_runs == 0 && matches!(state, DaemonLifecycleState::Draining) {
        reason_codes.push("daemon.lifecycle.drain_complete".to_owned());
        repair_hints.push("transition daemon lifecycle to drained".to_owned());
    }
    reason_codes.sort();
    reason_codes.dedup();
    repair_hints.sort();
    repair_hints.dedup();

    DaemonLifecycleSnapshot {
        schema_version: DAEMON_LIFECYCLE_SCHEMA_VERSION,
        state,
        accepts_new_runs: state.accepts_new_runs(),
        active_runs,
        queue_depth,
        pending_approvals,
        drain_started_at_unix_ms,
        shutdown_requested_at_unix_ms,
        resume_guard,
        reason_codes,
        repair_hints,
    }
}

/// Projects current gateway counters into the lifecycle contract when no
/// explicit drain marker is active.
pub(crate) fn build_daemon_lifecycle_snapshot_from_status(
    status: &GatewayStatusSnapshot,
    runtime_preview_payload: &Value,
) -> DaemonLifecycleSnapshot {
    let active_runs = status.counters.active_orchestrator_runs();
    let pending_approvals = status
        .counters
        .approvals_tool_requested
        .saturating_sub(status.counters.approvals_tool_resolved_allow)
        .saturating_sub(status.counters.approvals_tool_resolved_deny)
        .saturating_sub(status.counters.approvals_tool_resolved_timeout)
        .saturating_sub(status.counters.approvals_tool_resolved_error);
    let resume_guard = ResumeGuardCounters {
        incomplete_runs: active_runs,
        approval_pending: pending_approvals,
        interrupted_by_shutdown: read_u64(
            runtime_preview_payload,
            "/resume_guard/interrupted_by_shutdown",
        ),
        ..ResumeGuardCounters::default()
    };
    build_daemon_lifecycle_snapshot(
        DaemonLifecycleState::Running,
        active_runs,
        status.counters.channel_router_queue_depth,
        pending_approvals,
        None,
        None,
        resume_guard,
    )
}

/// Returns the metric catalog and cardinality policy consumed by `/metrics`
/// tests and support bundles.
pub(crate) fn build_metrics_catalog_snapshot() -> Value {
    json!({
        "schema_version": METRICS_CATALOG_SCHEMA_VERSION,
        "series_cap": PROMETHEUS_SERIES_CAP,
        "dropped_series_metric": "palyra_metrics_dropped_series_total",
        "label_policy": {
            "bounded_label_keys": BOUNDED_METRIC_LABEL_KEYS,
            "forbidden_label_keys": FORBIDDEN_METRIC_LABEL_KEYS,
            "max_label_value_bytes": 64,
            "rejects_absolute_paths": true,
            "rejects_principals": true,
            "rejects_canonical_ids": true,
            "rejects_secret_like_values": true,
        },
        "metrics": [
            {"name": "palyra_agent_runs_started_total", "type": "counter", "labels": []},
            {"name": "palyra_agent_runs_completed_total", "type": "counter", "labels": []},
            {"name": "palyra_model_provider_requests_total", "type": "counter", "labels": ["provider_kind"]},
            {"name": "palyra_model_provider_errors_total", "type": "counter", "labels": ["provider_kind"]},
            {"name": "palyra_model_provider_tokens_total", "type": "counter", "labels": ["provider_kind", "token_type"]},
            {"name": "palyra_model_provider_latency_ms", "type": "gauge", "labels": ["provider_kind", "stat"]},
            {"name": "palyra_tool_execution_attempts_total", "type": "counter", "labels": []},
            {"name": "palyra_tool_execution_failures_total", "type": "counter", "labels": []},
            {"name": "palyra_tool_execution_timeouts_total", "type": "counter", "labels": []},
            {"name": "palyra_run_stream_backpressure_coalesced_total", "type": "counter", "labels": ["boundary", "action"]},
            {"name": "palyra_run_stream_deadline_exceeded_total", "type": "counter", "labels": ["scope"]},
            {"name": "palyra_run_stream_approval_cancelled_total", "type": "counter", "labels": []},
            {"name": "palyra_run_stream_terminal_delivery_timeouts_total", "type": "counter", "labels": []},
            {"name": "palyra_run_interrupt_latency_observations_total", "type": "counter", "labels": ["phase"], "reason_code": "runtime.interrupt_latency.observed"},
            {"name": "palyra_run_interrupt_latency_ms_total", "type": "counter", "labels": ["phase"], "reason_code": "runtime.interrupt_latency.observed"},
            {"name": "palyra_run_interrupt_latency_ms_max", "type": "gauge", "labels": ["phase"], "reason_code": "runtime.interrupt_latency.observed"},
            {"name": "palyra_run_interrupt_latency_clamped_total", "type": "counter", "labels": ["phase"], "reason_code": "runtime.interrupt_latency.clamped"},
            {"name": "palyra_tool_job_state", "type": "gauge", "labels": ["state"]},
            {"name": "palyra_memory_recall_requests_total", "type": "counter", "labels": []},
            {"name": "palyra_channel_delivery_events_total", "type": "counter", "labels": ["status"]},
            {"name": "palyra_metrics_dropped_series_total", "type": "counter", "labels": []},
        ],
    })
}

/// Returns the bounded core performance decision included in operator
/// diagnostics and support bundles.
pub(crate) fn build_core_performance_qualification_snapshot() -> Value {
    crate::application::core_stability::performance::build_core_performance_qualification_snapshot()
}

/// Returns the bounded core security decision included in operator
/// diagnostics and support bundles.
pub(crate) fn build_security_conformance_snapshot() -> Value {
    crate::application::core_stability::security::build_security_conformance_snapshot()
}

/// Returns the configured V2 rollout stage and release-gate decision.
pub(crate) fn build_v2_rollout_snapshot(
    profile: &crate::application::runtime_kernel_v2::profile_resolver::ResolvedRuntimeProfileV1,
    performance: &Value,
    security: &Value,
) -> Value {
    crate::application::runtime_kernel_v2::rollout::build_v2_rollout_snapshot(
        profile,
        performance,
        security,
    )
}

/// Validates that Prometheus labels stay low-cardinality and secret-free.
pub(crate) fn validate_metric_labels(labels: &[(&str, &str)]) -> Result<(), String> {
    for (key, value) in labels {
        if FORBIDDEN_METRIC_LABEL_KEYS.contains(key) {
            return Err(format!("metric label '{key}' is forbidden"));
        }
        if !BOUNDED_METRIC_LABEL_KEYS.contains(key) {
            return Err(format!("metric label '{key}' is not in the bounded label catalog"));
        }
        validate_metric_label_value(key, value)?;
    }
    Ok(())
}

fn validate_metric_label_value(key: &str, value: &str) -> Result<(), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("metric label '{key}' must not be empty"));
    }
    if trimmed.len() > 64 {
        return Err(format!("metric label '{key}' exceeds 64 bytes"));
    }
    if looks_like_absolute_path(trimmed) {
        return Err(format!("metric label '{key}' must not contain absolute paths"));
    }
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("user:")
        || lowered.starts_with("admin:")
        || lowered.contains(" bearer ")
        || lowered.starts_with("bearer ")
        || lowered.contains("token=")
        || lowered.contains("password=")
        || lowered.contains("secret=")
        || lowered.starts_with("sk-")
        || looks_like_ulid(trimmed)
    {
        return Err(format!(
            "metric label '{key}' contains a high-cardinality or secret-like value"
        ));
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')) {
        return Err(format!("metric label '{key}' contains unsupported characters"));
    }
    Ok(())
}

/// Renders timeline events as bounded redacted JSONL.
pub(crate) fn render_diagnostics_timeline_jsonl(
    events: &[DiagnosticsTimelineEvent],
) -> Result<String, serde_json::Error> {
    let mut output = String::new();
    for event in events {
        let mut redacted = event.clone();
        redact_diagnostics_value(&mut redacted.payload, None);
        bound_timeline_payload(&mut redacted.payload);
        output.push_str(serde_json::to_string(&redacted)?.as_str());
        output.push('\n');
    }
    Ok(output)
}

/// Builds the runtime diagnostics timeline contract and a redacted one-line
/// sample so support bundles can validate JSONL parsing without a live file.
pub(crate) fn build_diagnostics_timeline_contract(generated_at_unix_ms: i64) -> Value {
    let event = DiagnosticsTimelineEvent {
        schema_version: RUNTIME_TIMELINE_SCHEMA_VERSION,
        monotonic_ms: 0,
        wall_time_unix_ms: generated_at_unix_ms,
        component: "daemon".to_owned(),
        phase: "diagnostics_contract".to_owned(),
        outcome: "contract_ready".to_owned(),
        correlation: BTreeMap::new(),
        payload: json!({
            "source": "runtime_diagnostics",
            "path_env": "PALYRA_DIAGNOSTICS_TIMELINE_PATH",
        }),
        redaction_level: "strict_bounded".to_owned(),
    };
    let sample_jsonl =
        render_diagnostics_timeline_jsonl(&[event]).unwrap_or_else(|_| String::new());
    json!({
        "schema_version": RUNTIME_TIMELINE_SCHEMA_VERSION,
        "status": "contract_ready",
        "path_env": "PALYRA_DIAGNOSTICS_TIMELINE_PATH",
        "config_path_key": "diagnostics.timeline.path",
        "payload_limit_bytes": DIAGNOSTICS_TIMELINE_PAYLOAD_LIMIT_BYTES,
        "required_fields": [
            "monotonic_ms",
            "wall_time_unix_ms",
            "component",
            "phase",
            "outcome",
            "correlation",
            "payload",
            "redaction_level"
        ],
        "sample_jsonl": sample_jsonl,
    })
}

/// Returns the trace exporter contract. OTLP and Langfuse stay explicit
/// opt-in adapters; local JSONL is the safe default implementation.
pub(crate) fn build_trace_exporter_contract() -> Value {
    let jsonl_renderer_ok = render_trace_jsonl(&[]).is_ok();
    json!({
        "schema_version": TRACE_EXPORT_SCHEMA_VERSION,
        "default_exporter": "jsonl",
        "exporters": [
            {
                "kind": "jsonl",
                "status": "implemented",
                "redaction": "strict_before_write",
                "blocking_policy": "never_block_agent_loop",
                "renderer_ok": jsonl_renderer_ok,
            },
            {
                "kind": "otlp",
                "status": "config_flag_required",
                "redaction": "strict_before_export",
                "blocking_policy": "drop_on_exporter_failure",
            },
            {
                "kind": "langfuse",
                "status": "config_flag_required",
                "redaction": "strict_before_export",
                "blocking_policy": "drop_on_exporter_failure",
            },
        ],
        "drop_counter": "palyra_trace_export_dropped_spans_total",
    })
}

/// Renders redacted trace spans for the local JSONL exporter.
pub(crate) fn render_trace_jsonl(spans: &[TraceSpanRecord]) -> Result<String, serde_json::Error> {
    let mut output = String::new();
    for span in spans {
        let mut redacted = span.clone();
        redacted.name = sanitize_diagnostics_string(redacted.name.as_str(), Some("name"));
        redacted.component =
            sanitize_low_cardinality_value(redacted.component.as_str(), "trace.component");
        redacted.outcome =
            sanitize_low_cardinality_value(redacted.outcome.as_str(), "trace.outcome");
        for value in redacted.correlation.values_mut() {
            *value = sanitize_diagnostics_string(value.as_str(), Some("correlation"));
        }
        redact_diagnostics_value(&mut redacted.attributes, None);
        output.push_str(serde_json::to_string(&redacted)?.as_str());
        output.push('\n');
    }
    Ok(output)
}

/// Builds a per-run stage timing report with timeout classification and
/// missing-stage visibility.
#[must_use]
pub(crate) fn build_run_stage_timing_report(
    inputs: &[RunStageTimingInput],
) -> RunStageTimingReport {
    let mut records = inputs.iter().map(run_stage_timing_record).collect::<Vec<_>>();
    records.sort_by_key(|record| (record.started_at_unix_ms, record.stage.clone()));

    let observed = records
        .iter()
        .map(|record| record.stage.as_str())
        .collect::<std::collections::HashSet<_>>();
    let missing_stages = RUN_STAGE_NAMES
        .iter()
        .filter(|stage| !observed.contains(**stage))
        .map(|stage| (*stage).to_owned())
        .collect::<Vec<_>>();
    let mut timeout_classifications = records
        .iter()
        .filter_map(|record| record.timeout_classification.clone())
        .collect::<Vec<_>>();
    timeout_classifications.sort();
    timeout_classifications.dedup();
    let idle_breaker_triggered = timeout_classifications
        .iter()
        .any(|classification| classification == "provider_idle_timeout");

    RunStageTimingReport {
        schema_version: RUN_STAGE_TIMING_SCHEMA_VERSION,
        redaction_level: "metadata_only".to_owned(),
        records,
        missing_stages,
        timeout_classifications,
        idle_breaker_triggered,
    }
}

pub(crate) fn build_run_stage_timing_contract() -> Value {
    let timeout_classes = [
        RunStageTimeoutKind::ProviderHard,
        RunStageTimeoutKind::ProviderIdle,
        RunStageTimeoutKind::ToolWait,
        RunStageTimeoutKind::AbortSettle,
    ]
    .into_iter()
    .map(RunStageTimeoutKind::as_str)
    .collect::<Vec<_>>();
    let sample = build_run_stage_timing_report(&[
        RunStageTimingInput {
            stage: "prepare".to_owned(),
            started_at_unix_ms: 1_000,
            completed_at_unix_ms: Some(1_010),
            first_signal_at_unix_ms: None,
            timeout_ms: None,
            timeout_kind: None,
            outcome: "ok".to_owned(),
        },
        RunStageTimingInput {
            stage: "provider_request".to_owned(),
            started_at_unix_ms: 1_020,
            completed_at_unix_ms: Some(1_800),
            first_signal_at_unix_ms: Some(1_800),
            timeout_ms: Some(500),
            timeout_kind: None,
            outcome: "timeout".to_owned(),
        },
    ]);

    json!({
        "schema_version": RUN_STAGE_TIMING_SCHEMA_VERSION,
        "required_stages": RUN_STAGE_NAMES,
        "timeout_classes": timeout_classes,
        "idle_breaker_class": RunStageTimeoutKind::ProviderIdle.as_str(),
        "support_bundle_section": "runtime.stage_timings",
        "sample": sample,
    })
}

fn run_stage_timing_record(input: &RunStageTimingInput) -> RunStageTimingRecord {
    let stage = sanitize_low_cardinality_value(input.stage.as_str(), "stage");
    let outcome = sanitize_low_cardinality_value(input.outcome.as_str(), "stage.outcome");
    let duration_ms = input
        .completed_at_unix_ms
        .and_then(|completed| completed.checked_sub(input.started_at_unix_ms))
        .and_then(|duration| u64::try_from(duration).ok());
    let idle_ms = input
        .first_signal_at_unix_ms
        .and_then(|first_signal| first_signal.checked_sub(input.started_at_unix_ms))
        .and_then(|duration| u64::try_from(duration).ok());
    let timeout_classification =
        classify_stage_timeout(stage.as_str(), input.timeout_kind, input.timeout_ms, idle_ms)
            .map(ToOwned::to_owned);
    let diagnostics_code = timeout_classification
        .as_ref()
        .map(|classification| format!("run_stage.timeout.{classification}"))
        .unwrap_or_else(|| format!("run_stage.{}.{}", stage, outcome));

    RunStageTimingRecord {
        schema_version: RUN_STAGE_TIMING_SCHEMA_VERSION,
        stage,
        started_at_unix_ms: input.started_at_unix_ms,
        completed_at_unix_ms: input.completed_at_unix_ms,
        duration_ms,
        first_signal_at_unix_ms: input.first_signal_at_unix_ms,
        idle_ms,
        timeout_ms: input.timeout_ms,
        timeout_classification,
        outcome,
        diagnostics_code,
        redaction_level: "metadata_only".to_owned(),
    }
}

fn classify_stage_timeout(
    stage: &str,
    timeout_kind: Option<RunStageTimeoutKind>,
    timeout_ms: Option<u64>,
    idle_ms: Option<u64>,
) -> Option<&'static str> {
    match timeout_kind {
        Some(kind) => Some(kind.as_str()),
        None if stage == "provider_request"
            && idle_ms.zip(timeout_ms).is_some_and(|(observed_idle_ms, threshold_ms)| {
                observed_idle_ms >= threshold_ms
            }) =>
        {
            Some(RunStageTimeoutKind::ProviderIdle.as_str())
        }
        None => None,
    }
}

/// Renders the Prometheus text exposition for the daemon's core counters and
/// gauges. Labels are restricted to bounded values (provider kind, job
/// state, delivery status) -- never principals, sessions, or paths -- to keep
/// cardinality flat and avoid leaking identifiers into scrape targets.
pub(crate) fn render_prometheus_metrics(
    status: &GatewayStatusSnapshot,
    tool_jobs: &[ToolJobRecord],
    managed_runtime_health: &ManagedRuntimeHealthSnapshot,
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
        "palyra_agent_runs_failed_total",
        "Total agent runs failed by the daemon.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_agent_runs_failed_total",
        &[],
        status.counters.orchestrator_runs_failed,
    );
    push_help(
        &mut output,
        "palyra_agent_runs_cancelled_total",
        "Total agent runs cancelled by the daemon.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_agent_runs_cancelled_total",
        &[],
        status.counters.orchestrator_runs_cancelled,
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
        "palyra_model_provider_tokens_total",
        "Total model provider token usage by bounded provider kind and token type.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_model_provider_tokens_total",
        &[("provider_kind", status.model_provider.kind.as_str()), ("token_type", "prompt")],
        status.model_provider.runtime_metrics.total_prompt_tokens,
    );
    push_sample(
        &mut output,
        "palyra_model_provider_tokens_total",
        &[("provider_kind", status.model_provider.kind.as_str()), ("token_type", "completion")],
        status.model_provider.runtime_metrics.total_completion_tokens,
    );
    push_help(
        &mut output,
        "palyra_model_provider_retries_total",
        "Total model provider retry attempts by bounded provider kind.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_model_provider_retries_total",
        &[("provider_kind", status.model_provider.kind.as_str())],
        status.model_provider.runtime_metrics.total_retry_attempts,
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
        "palyra_tool_execution_failures_total",
        "Total tool execution failures.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_tool_execution_failures_total",
        &[],
        status.counters.tool_execution_failures,
    );
    push_help(
        &mut output,
        "palyra_tool_execution_timeouts_total",
        "Total tool execution timeouts.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_tool_execution_timeouts_total",
        &[],
        status.counters.tool_execution_timeouts,
    );
    push_help(
        &mut output,
        "palyra_run_stream_backpressure_coalesced_total",
        "Process progress snapshots coalesced by the bounded run-stream mailbox.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_run_stream_backpressure_coalesced_total",
        &[("boundary", "process_progress"), ("action", "coalesce_progress")],
        status.counters.run_stream_progress_coalesced,
    );
    push_help(
        &mut output,
        "palyra_run_stream_deadline_exceeded_total",
        "Run-stream child operations stopped by inherited deadlines.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_run_stream_deadline_exceeded_total",
        &[("scope", "tool_execution")],
        status.counters.run_stream_tool_deadline_exceeded,
    );
    push_help(
        &mut output,
        "palyra_run_stream_approval_cancelled_total",
        "Pending tool approvals resolved fail-closed after run cancellation.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_run_stream_approval_cancelled_total",
        &[],
        status.counters.run_stream_approval_cancelled,
    );
    push_help(
        &mut output,
        "palyra_run_stream_terminal_delivery_timeouts_total",
        "Terminal client deliveries that exceeded their bounded delivery scope after settlement.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_run_stream_terminal_delivery_timeouts_total",
        &[],
        status.counters.run_stream_terminal_delivery_timeouts,
    );
    push_help(
        &mut output,
        "palyra_run_interrupt_latency_observations_total",
        "Observed run interrupts by bounded active phase.",
        "counter",
    );
    push_help(
        &mut output,
        "palyra_run_interrupt_latency_ms_total",
        "Cumulative request-to-observation interrupt latency by bounded active phase.",
        "counter",
    );
    push_help(
        &mut output,
        "palyra_run_interrupt_latency_ms_max",
        "Maximum bounded request-to-observation interrupt latency by active phase.",
        "gauge",
    );
    push_help(
        &mut output,
        "palyra_run_interrupt_latency_clamped_total",
        "Interrupt latency samples clamped for clock skew or the diagnostics bound.",
        "counter",
    );
    for phase in &status.counters.run_interrupt_latency.phases {
        let labels = [("phase", phase.phase.as_str())];
        push_sample(
            &mut output,
            "palyra_run_interrupt_latency_observations_total",
            &labels,
            phase.observations,
        );
        push_sample(
            &mut output,
            "palyra_run_interrupt_latency_ms_total",
            &labels,
            phase.total_latency_ms,
        );
        push_sample(
            &mut output,
            "palyra_run_interrupt_latency_ms_max",
            &labels,
            phase.max_latency_ms,
        );
        push_sample(
            &mut output,
            "palyra_run_interrupt_latency_clamped_total",
            &labels,
            phase.clamped_observations,
        );
    }
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
    push_help(
        &mut output,
        "palyra_runtime_health_components",
        "Current shared-health components by bounded runtime family.",
        "gauge",
    );
    for (family, count) in &managed_runtime_health.components_by_family {
        push_sample(
            &mut output,
            "palyra_runtime_health_components",
            &[("family", family.as_str())],
            *count,
        );
    }
    push_help(
        &mut output,
        "palyra_runtime_health_state",
        "Current shared-health components by bounded health state.",
        "gauge",
    );
    for (state, count) in &managed_runtime_health.components_by_state {
        push_sample(
            &mut output,
            "palyra_runtime_health_state",
            &[("state", state.as_str())],
            *count,
        );
    }
    push_help(
        &mut output,
        "palyra_runtime_health_stale_suppressions_total",
        "Managed runtime callbacks suppressed after their exact generation was superseded.",
        "counter",
    );
    push_sample(
        &mut output,
        "palyra_runtime_health_stale_suppressions_total",
        &[],
        managed_runtime_health.stale_suppressions_total,
    );
    push_help(
        &mut output,
        "palyra_metrics_dropped_series_total",
        "Prometheus series dropped by the metrics label validator or series cap.",
        "counter",
    );
    push_sample(&mut output, "palyra_metrics_dropped_series_total", &[], 0);
    push_help(
        &mut output,
        "palyra_metrics_series_cap",
        "Configured maximum series emitted by the built-in metrics endpoint.",
        "gauge",
    );
    push_sample(&mut output, "palyra_metrics_series_cap", &[], PROMETHEUS_SERIES_CAP);
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
            "provider_recovery_trace",
            "tool_job_states",
            "effective_tool_surface_report",
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
            "recovery_decision_events": [
                "provider.recovery.decision",
                "provider.turn_recovery.decision"
            ],
            "observability_counters": true,
        },
        "run_stage_timings": build_run_stage_timing_contract(),
        "component_health_registry": build_component_health_registry_contract(),
    })
}

fn build_component_health_registry_contract() -> Value {
    let mut registry = ComponentHealthRegistry::default();
    let quarantined = registry.record_outcome(
        ComponentCallOutcome {
            component: "mcp.docs".to_owned(),
            capability: "tool.search".to_owned(),
            duration_ms: 50,
            timed_out: true,
            capability_denied: false,
            error_code: None,
            resource_event: None,
        },
        1_000,
    );
    let fallback = registry.fallback_for_component("mcp.docs");
    let _ = registry.mark_state(
        "plugin.policy",
        ComponentHealthState::PendingUpgrade,
        "plugin.pending_upgrade",
        1_000,
    );
    let _ = registry.mark_state(
        "plugin.recovered",
        ComponentHealthState::Quarantined,
        "plugin.repeated_failures",
        1_000,
    );
    let unquarantine_audit = registry.unquarantine_with_audit("plugin.recovered", "system", 2_000);
    json!({
        "schema_version": 1,
        "states": [
            ComponentHealthState::Healthy.as_str(),
            ComponentHealthState::Degraded.as_str(),
            ComponentHealthState::Quarantined.as_str(),
            ComponentHealthState::DisabledByPolicy.as_str(),
            ComponentHealthState::Incompatible.as_str(),
            ComponentHealthState::PendingUpgrade.as_str(),
            ComponentHealthState::FailedPreflight.as_str(),
        ],
        "tracked_outcomes": [
            "duration_ms",
            "error_code",
            "capability_denied",
            "timed_out",
            "resource_event",
        ],
        "quarantine": {
            "after_consecutive_failures": registry.quarantine_after_failures,
            "backoff_ms": registry.quarantine_backoff_ms,
            "state_after_first_failure": quarantined.state.as_str(),
        },
        "fallback": {
            "audit_reason": fallback.audit_reason,
            "use_fallback": fallback.use_fallback,
        },
        "snapshot_fields": registry.snapshot().iter().map(|record| {
            json!({
                "component": record.component,
                "state": record.state.as_str(),
                "consecutive_failures": record.consecutive_failures,
                "total_failures": record.total_failures,
                "last_error_code": record.last_error_code,
            })
        }).collect::<Vec<_>>(),
        "operator_unquarantine_requires_actor": true,
        "unquarantine_audit_kind": unquarantine_audit
            .as_ref()
            .map(|event| event.event_kind.as_str())
            .unwrap_or("component_health.unquarantined"),
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
    let tool_catalog_policy =
        crate::application::tool_registry::ToolCatalogPolicySnapshot::direct_from_allowed_tools(
            tool_config.allowed_tools.as_slice(),
        );
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
        catalog_policy: &tool_catalog_policy,
        browser_service_enabled: false,
        browser_service_configured: false,
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
        "compatibility_policy": {
            "snapshot_version": "runtime-diagnostics.contract_snapshot_policy.v1",
            "changelog_note": "Runtime contract snapshot drift requires updating the golden snapshot plus a migration note.",
            "breaking_change_requires_version_bump": true,
            "breaking_change_requires_migration_note": true,
        },
        "runtime_contracts_abi": public_runtime_contract_snapshot(),
        "plugin_sdk_abi": plugin_sdk_contract_snapshot(),
        "skill_manifest_abi": skill_manifest_contract_snapshot(),
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
            "effective_surface": effective_tool_surface_report(&tool_snapshot),
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

/// Builds the closed compatibility snapshot for managed coding schemas,
/// wire values, degradation behavior, bounds, and privacy guarantees.
#[cfg(test)]
pub(crate) fn build_managed_coding_contract_snapshot() -> Value {
    let schema_contracts = MANAGED_CODING_SCHEMA_SOURCES
        .iter()
        .map(|(name, source)| managed_coding_schema_contract(name, source))
        .collect::<Vec<_>>();
    let soak_baseline: Value =
        serde_json::from_str(include_str!("../../../qa/baselines/coding-runtime-warm-lsp.v1.json"))
            .expect("managed coding soak baseline is valid JSON");

    json!({
        "snapshot_version": MANAGED_CODING_CONTRACT_SNAPSHOT_VERSION,
        "schema_contracts": schema_contracts,
        "wire_enums": {
            "capability_status": ["active", "configured", "degraded", "disabled", "blocked"],
            "coding_command_backend": ["process", "native_pty", "process_without_pty"],
            "coding_command_lifecycle": ["running", "completed", "failed"],
            "diagnostic_severity": ["error", "warning", "information", "hint"],
            "diagnostics_delta_status": [
                "verified",
                "blocking_diagnostics",
                "diagnostics_timed_out",
                "server_generation_changed",
                "fallback_required"
            ],
            "diagnostics_fallback_tool": ["cargo_check", "tsc_no_emit", "pyright"],
            "lsp_language": ["rust", "type_script", "python"],
            "lsp_server_lifecycle": ["starting", "ready", "broken", "stopped", "evicted"],
            "managed_coding_diagnostics_status": ["available", "unavailable"],
            "managed_worktree_lifecycle": [
                "creating",
                "active",
                "removing",
                "retained",
                "failed",
                "removed"
            ],
            "process_ownership_kind": [
                "unix_process_group",
                "windows_job_object",
                "remote_execution_instance"
            ],
            "process_session_state": [
                "running",
                "draining",
                "succeeded",
                "failed",
                "timed_out",
                "cancelled",
                "cleanup_failed",
                "orphaned"
            ],
            "pty_backend": ["unix_pty", "windows_con_pty"],
            "resource_lease_state": ["active", "revoked", "released", "expired"],
            "resource_pressure_action_state": ["applied", "skipped", "failed"],
            "resource_priority": [
                "interactive",
                "foreground",
                "idle_service",
                "background_fanout"
            ],
            "resource_service": ["process", "pty", "lsp", "mcp", "external_runtime", "worktree"],
            "snapshot_entry_kind": ["tracked", "allowed_untracked"],
            "snapshot_gc_decision": ["removed", "blocked_by_active_lease", "retained"],
            "workspace_admission": ["explicit", "policy"],
            "workspace_isolation": ["managed_worktree", "in_place_explicit"],
            "worktree_disposition": ["removed", "dirty_retained", "in_place_preserved"]
        },
        "fallback_matrix": [
            {
                "capability": "managed_worktree",
                "primary": "managed_worktree",
                "fallback": "in_place_workspace",
                "fallback_authority": "host_policy_only",
                "primary_reason": "coding.managed_worktree_active",
                "fallback_reason": "coding.workspace_in_place_explicit"
            },
            {
                "capability": "native_pty",
                "primary": "native_pty",
                "fallback": "process_without_pty",
                "fallback_authority": "host_policy_only",
                "primary_reason": "coding.pty_configured",
                "fallback_reason": "coding.pty_disabled_process_fallback"
            },
            {
                "capability": "persistent_lsp",
                "primary": "persistent_lsp",
                "fallback": "compiler_cli",
                "fallback_authority": "host_policy_only",
                "primary_reason": "coding.lsp_active",
                "fallback_reason": "coding.lsp_cli_fallback_active"
            },
            {
                "capability": "process_supervisor",
                "primary": "managed_process_actor",
                "fallback": null,
                "fallback_authority": "none",
                "unavailable_outcome": "blocked"
            },
            {
                "capability": "objective_wait_bridge",
                "primary": "durable_wait_barrier",
                "fallback": null,
                "fallback_authority": "none",
                "unavailable_outcome": "blocked"
            },
            {
                "capability": "patch_verification",
                "primary": "generation_aware_lsp_delta",
                "fallback": "host_selected_compiler_cli",
                "fallback_authority": "host_policy_only",
                "unavailable_outcome": "fallback_required"
            }
        ],
        "reason_taxonomy": {
            "format": "lowercase_dot_separated",
            "families": [
                "coding",
                "lsp",
                "managed_worktree",
                "process",
                "resource"
            ],
            "coding": [
                "coding.cleanup_clean_removed",
                "coding.cleanup_dirty_retained",
                "coding.cleanup_dirty_snapshotted_removed",
                "coding.cleanup_in_place_preserved",
                "coding.command_actor_failed",
                "coding.command_local_wait_timed_out",
                "coding.command_running",
                "coding.diagnostics_serialization_failed",
                "coding.dirty_worktree_retained",
                "coding.lsp_active",
                "coding.lsp_cli_fallback_active",
                "coding.lsp_unavailable",
                "coding.managed_worktree_active",
                "coding.operator_retained",
                "coding.patch_blocking_diagnostics",
                "coding.patch_cli_fallback_required",
                "coding.patch_diagnostics_timeout",
                "coding.patch_lsp_generation_changed",
                "coding.patch_lsp_verified",
                "coding.process_actor_failed",
                "coding.process_command_failed",
                "coding.process_command_succeeded",
                "coding.process_completed",
                "coding.process_completion_wake_failed",
                "coding.process_monitor_spawn_failed",
                "coding.process_wait_registered",
                "coding.pty_actor_failed",
                "coding.pty_command_failed",
                "coding.pty_command_succeeded",
                "coding.pty_configured",
                "coding.pty_disabled_process_fallback",
                "coding.pty_unavailable_process_fallback",
                "coding.recovery_inventory_loaded",
                "coding.restart_dirty_worktree_retained",
                "coding.restart_reconciled_without_pid_adoption",
                "coding.runtime_available",
                "coding.runtime_unavailable",
                "coding.snapshot_active_lease",
                "coding.snapshot_removed",
                "coding.snapshot_retained",
                "coding.terminal_completed",
                "coding.terminal_completion_wake_failed",
                "coding.terminal_monitor_spawn_failed",
                "coding.terminal_running",
                "coding.terminal_wait_registered",
                "coding.wait_bridge_failed",
                "coding.workspace_explicitly_selected",
                "coding.workspace_in_place_explicit",
                "coding.workspace_policy_selected",
                "coding_runtime.soak_completed",
                "coding_runtime.warm_lsp_baseline"
            ],
            "lsp": [
                "lsp.active",
                "lsp.capabilities_oversized",
                "lsp.daemon_shutdown",
                "lsp.diagnostics_baseline_captured",
                "lsp.diagnostics_delta_blocking",
                "lsp.diagnostics_delta_verified",
                "lsp.diagnostics_generation_changed",
                "lsp.diagnostics_server_unavailable",
                "lsp.diagnostics_timeout",
                "lsp.document_diagnostics_pending",
                "lsp.document_synchronized",
                "lsp.idle_reaped",
                "lsp.initialize_pending",
                "lsp.initialized",
                "lsp.json_rpc_error",
                "lsp.malformed_frame",
                "lsp.no_active_servers",
                "lsp.oversized_frame",
                "lsp.request_completed",
                "lsp.request_timeout",
                "lsp.resource_pressure_evicted",
                "lsp.restart_requires_relaunch",
                "lsp.rollback_diagnostics_timeout",
                "lsp.rollback_synchronized",
                "lsp.server_crashed",
                "lsp.supervisor_failure"
            ],
            "managed_worktree": [
                "managed_worktree.create_failed_cleanup_required",
                "managed_worktree.create_failed_cleanup_verified",
                "managed_worktree.create_intent",
                "managed_worktree.created",
                "managed_worktree.dirty_retained",
                "managed_worktree.remove_intent",
                "managed_worktree.removed",
                "managed_worktree.run_attached",
                "managed_worktree.run_detached",
                "managed_worktree.status_refreshed"
            ],
            "process": [
                "process.adoption.authority_unavailable",
                "process.adoption.identity_mismatch",
                "process.adoption.provenance_mismatch",
                "process.cancelled",
                "process.completed",
                "process.exited_successfully",
                "process.exited_unsuccessfully",
                "process.interrupted",
                "process.no_output_timeout",
                "process.persistence_failed",
                "process.resource_release_failed",
                "process.restart_requires_verified_adoption",
                "process.supervisor_shutdown",
                "process.timeout",
                "process.wait_failed"
            ],
            "resource": [
                "resource.admitted",
                "resource.capacity_available",
                "resource.lease_expired",
                "resource.lsp_eviction_applied",
                "resource.lsp_eviction_failed",
                "resource.pressure_action_unowned",
                "resource.pressure_detected",
                "resource.pressure_revocation_proposed",
                "resource.released",
                "resource.renewed",
                "resource.restart_requires_reacquire"
            ]
        },
        "hard_limits": {
            "command_argument_bytes": 16_384,
            "command_arguments": 256,
            "command_environment_keys": 256,
            "command_output_chunks": 512,
            "diagnostic_artifact_bytes": 16_777_216,
            "diagnostic_artifacts": 4_096,
            "diagnostics_per_document": 2_048,
            "diagnostics_visible_delta_items": 128,
            "git_operation_timeout_ms": 60_000,
            "identity_bytes": 128,
            "lsp_active_servers": 16,
            "lsp_header_bytes": 8_192,
            "lsp_message_bytes": 1_048_576,
            "lsp_notifications": 512,
            "managed_tasks": 64,
            "patch_files": 128,
            "pending_patch_verifications": 128,
            "process_artifact_bytes_per_session": 16_777_216,
            "process_drain_allowance_ms": 5_000,
            "process_output_chunk_bytes": 8_192,
            "process_output_page_chunks": 4_096,
            "process_output_projection_chars": 32_768,
            "process_retained_bytes_per_session": 4_194_304,
            "process_retained_chunks_per_session": 512,
            "process_sessions": 64,
            "resource_lease_records": 4_096,
            "resource_pressure_actions": 32,
            "source_file_bytes": 8_388_608,
            "worktree_attached_runs": 64,
            "worktree_records": 512,
            "worktree_snapshot_bytes": 67_108_864,
            "worktree_snapshot_file_bytes": 8_388_608,
            "worktree_snapshot_files": 512
        },
        "soak_baseline": soak_baseline,
        "privacy_invariants": {
            "caller_supplied_commands_are_not_executed": true,
            "diagnostic_text_is_absent_from_operator_health": true,
            "durable_environment_values_are_forbidden": true,
            "durable_live_os_handles_are_forbidden": true,
            "durable_raw_process_and_terminal_output_is_forbidden": true,
            "operator_diagnostics_hash_internal_identities": true,
            "operator_diagnostics_omit_host_paths": true,
            "pid_without_verified_authority_is_never_adopted": true,
            "recovery_inventory_is_content_free": true,
            "secret_like_snapshots_are_rejected_before_persistence": true,
            "server_capabilities_are_summarized_then_discarded": true,
            "snapshot_paths_are_workspace_relative": true
        },
        "compatibility_policy": {
            "closed_wire_contracts": true,
            "existing_version_semantics_are_immutable": true,
            "migration": "explicit_versioned_adapter_only",
            "required_field_change": "new_schema_version_and_migration_note",
            "snapshot_change": "version_bump_and_migration_note",
            "unknown_fields": "reject",
            "unknown_schema_versions": "reject_before_rewrite_or_mutation",
            "wire_enum_growth": "new_schema_version_and_migration_note"
        }
    })
}

#[cfg(test)]
fn managed_coding_schema_contract(name: &str, source: &str) -> Value {
    let schema: Value = serde_json::from_str(source).expect("managed coding schema is valid JSON");
    let mut wire_enums = BTreeMap::new();
    let mut required_field_sets = BTreeMap::new();
    collect_managed_coding_schema_shape(&schema, "", &mut wire_enums, &mut required_field_sets);
    json!({
        "name": name,
        "schema_id": schema
            .get("$id")
            .cloned()
            .expect("managed coding schema has an ID"),
        "schema_version": schema
            .pointer("/properties/schema_version/const")
            .cloned()
            .expect("managed coding schema has a closed version"),
        "required_fields": schema
            .get("required")
            .cloned()
            .expect("managed coding schema declares required fields"),
        "declared_limits": schema
            .get("x-palyra-limits")
            .cloned()
            .unwrap_or(Value::Null),
        "declared_bounds": schema
            .get("x-palyra-bounds")
            .cloned()
            .unwrap_or(Value::Null),
        "declared_prohibitions": schema
            .get("x-palyra-prohibits")
            .cloned()
            .unwrap_or(Value::Null),
        "declared_privacy": schema
            .get("x-palyra-privacy")
            .cloned()
            .unwrap_or(Value::Null),
        "required_field_sets": required_field_sets,
        "wire_enums": wire_enums
    })
}

#[cfg(test)]
fn collect_managed_coding_schema_shape(
    value: &Value,
    pointer: &str,
    enums: &mut BTreeMap<String, Value>,
    required_field_sets: &mut BTreeMap<String, Value>,
) {
    match value {
        Value::Object(object) => {
            if let Some(values) = object.get("enum") {
                enums.insert(pointer.to_owned(), values.clone());
            }
            if let Some(fields) = object.get("required") {
                required_field_sets.insert(
                    if pointer.is_empty() { "/".to_owned() } else { pointer.to_owned() },
                    fields.clone(),
                );
            }
            for (key, child) in
                object.iter().filter(|(key, _)| !matches!(key.as_str(), "enum" | "required"))
            {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                collect_managed_coding_schema_shape(
                    child,
                    &format!("{pointer}/{escaped}"),
                    enums,
                    required_field_sets,
                );
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                collect_managed_coding_schema_shape(
                    child,
                    &format!("{pointer}/{index}"),
                    enums,
                    required_field_sets,
                );
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
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
            ("lease_ttl_ms", ROUTINE_RUN_LEASE_TTL_MS as u64),
            ("lease_ledger_schema_version", ROUTINE_LEASE_LEDGER_SCHEMA_VERSION),
            ("startup_catch_up_schema_version", ROUTINE_STARTUP_CATCH_UP_SCHEMA_VERSION),
            ("cron_security_schema_version", ROUTINE_CRON_SECURITY_SCHEMA_VERSION),
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

fn mcp_health_component(payload: &Value) -> RuntimeHealthComponentSnapshot {
    let degraded_servers = read_u64(payload, "/degraded_servers");
    let backoff_servers = read_u64(payload, "/backoff_servers");
    let quarantined_servers = read_u64(payload, "/quarantined_servers");
    let mut degraded = Vec::new();
    let mut unavailable = Vec::new();
    if payload.get("error").is_some()
        || payload.pointer("/status").and_then(Value::as_str) == Some("unavailable")
    {
        unavailable.push("mcp.supervisor_snapshot_unavailable".to_owned());
    }
    if degraded_servers > 0 {
        degraded.push("mcp.servers_degraded".to_owned());
    }
    if backoff_servers > 0 {
        degraded.push("mcp.servers_in_backoff".to_owned());
    }
    if quarantined_servers > 0 {
        degraded.push("mcp.servers_quarantined".to_owned());
    }
    component(
        "mcp",
        status_from_reason_count(degraded.len(), unavailable.len()),
        merged_reasons(degraded, unavailable),
        metrics(&[
            ("total_servers", read_u64(payload, "/total_servers")),
            ("enabled_servers", read_u64(payload, "/enabled_servers")),
            ("healthy_servers", read_u64(payload, "/healthy_servers")),
            ("degraded_servers", degraded_servers),
            ("backoff_servers", backoff_servers),
            ("quarantined_servers", quarantined_servers),
            ("disabled_servers", read_u64(payload, "/disabled_servers")),
            ("catalog_generation", read_u64(payload, "/catalog_generation")),
        ]),
        vec![
            "palyra_mcp_doctor".to_owned(),
            "palyra_mcp_probe".to_owned(),
            "palyra_mcp_reload_after_fix".to_owned(),
        ],
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

fn bound_timeline_payload(value: &mut Value) {
    let Ok(encoded) = serde_json::to_vec(value) else {
        *value = json!({
            "truncated": true,
            "reason": "payload_encode_failed",
        });
        return;
    };
    if encoded.len() <= DIAGNOSTICS_TIMELINE_PAYLOAD_LIMIT_BYTES {
        return;
    }
    *value = json!({
        "truncated": true,
        "original_bytes": encoded.len(),
        "limit_bytes": DIAGNOSTICS_TIMELINE_PAYLOAD_LIMIT_BYTES,
        "redaction_level": "strict_bounded",
    });
}

fn redact_diagnostics_value(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object.iter_mut() {
                redact_diagnostics_value(child, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for child in items {
                redact_diagnostics_value(child, key_context);
            }
        }
        Value::String(raw) => {
            *raw = sanitize_diagnostics_string(raw.as_str(), key_context);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn sanitize_diagnostics_string(raw: &str, key_context: Option<&str>) -> String {
    if key_context.is_some_and(is_sensitive_key) {
        return "<redacted>".to_owned();
    }
    let redacted = redact_diagnostic_text(raw);
    let redacted = redact_internal_runtime_paths(redacted.as_str());
    let redacted = redact_absolute_path_tokens(redacted.as_str());
    if redacted.contains("vault://") || redacted.contains("vault:") {
        return "<vault_ref:redacted>".to_owned();
    }
    redacted
}

fn redact_absolute_path_tokens(raw: &str) -> String {
    raw.split_inclusive(char::is_whitespace)
        .map(|token| {
            let content = token.trim_end_matches(char::is_whitespace);
            let separator = &token[content.len()..];
            if token_contains_absolute_path(content) {
                format!("<redacted>{separator}")
            } else {
                token.to_owned()
            }
        })
        .collect()
}

fn token_contains_absolute_path(raw: &str) -> bool {
    let trimmed = raw.trim_matches(|ch: char| {
        !ch.is_ascii_alphanumeric() && !matches!(ch, ':' | '\\' | '/' | '_' | '-' | '.')
    });
    looks_like_absolute_path(trimmed)
        || trimmed.as_bytes().windows(3).any(|window| {
            window[0].is_ascii_alphabetic()
                && window[1] == b':'
                && matches!(window[2], b'\\' | b'/')
        })
        || trimmed.starts_with("\\\\")
}

fn sanitize_low_cardinality_value(raw: &str, label_name: &str) -> String {
    validate_metric_label_value(label_name, raw)
        .map_or_else(|_| "invalid_label_redacted".to_owned(), |_| raw.to_owned())
}

fn looks_like_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/'))
        || value.starts_with('/')
        || value.starts_with("\\\\")
}

fn looks_like_ulid(value: &str) -> bool {
    value.len() == 26 && value.chars().all(|ch| ch.is_ascii_alphanumeric())
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
    debug_assert!(
        validate_metric_labels(labels).is_ok(),
        "invalid metric labels for {name}: {labels:?}"
    );
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
    fn runtime_error_contract_diagnostics_are_metadata_only_and_traceable() {
        let payload = build_runtime_error_contract_diagnostics();
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["status"], "contract_ready");
        assert_eq!(payload["reason_code"], "runtime.error_contract.ready");
        assert_eq!(payload["invariant_contract"]["descriptors"].as_array().map(Vec::len), Some(6));
        assert_eq!(payload["error_taxonomy"]["classes"].as_array().map(Vec::len), Some(12));
        assert_eq!(payload["metadata_trace"]["status"], "always_on");
        assert_eq!(payload["metadata_trace"]["storage"], "append_only_segments");
        assert_eq!(payload["metadata_trace"]["failure_signal"], "counters.metadata_trace_failures");
        assert_eq!(payload["metadata_trace"]["rich_trace_approval_required"], true);
        assert_eq!(payload["metadata_trace"]["event_name"], RUN_RUNTIME_PATH_SUMMARY_EVENT);
        assert_eq!(payload["metadata_trace"]["terminal_reason_field"], "terminal_reason");
        assert_eq!(payload["metadata_trace"]["stable_reason_code_required"], true);
        assert_eq!(payload["metadata_trace"]["full_error_envelope_embedded"], false);
        assert_eq!(
            payload["metadata_trace"]["reason_code"],
            "runtime.error_contract.trace_projection_ready"
        );
        assert_eq!(
            payload["metadata_trace"]["always_on_reason_code"],
            "metadata_trace.always_on.ready"
        );
        assert_eq!(payload["error_taxonomy"]["raw_provider_payload_allowed"], false);
        assert_eq!(payload["error_taxonomy"]["raw_stderr_allowed"], false);
    }

    #[test]
    fn run_runtime_path_summary_exposes_redacted_rollout_posture() {
        let config = FeatureRolloutsConfig {
            provider_recovery: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };

        let summary = build_run_runtime_path_summary(
            &config,
            Some("failed"),
            Some("provider token=abc failed in C:\\Users\\Palo\\repo"),
            Some("embedded_palyra"),
        );

        assert_eq!(summary.schema_version, RUN_RUNTIME_PATH_SCHEMA_VERSION);
        assert_eq!(summary.event_name, RUN_RUNTIME_PATH_SUMMARY_EVENT);
        assert_eq!(summary.redaction_level, "metadata_only");
        assert_eq!(summary.attempt_owner, "embedded_palyra");
        assert_eq!(summary.terminal_state.as_deref(), Some("failed"));
        assert_eq!(summary.subsystems.len(), 9);
        assert_eq!(
            summary.subsystems.get("provider_recovery").map(|subsystem| subsystem.state.as_str()),
            Some("enabled")
        );
        assert_eq!(
            summary.subsystems.get("harness").map(|subsystem| subsystem.state.as_str()),
            Some("disabled")
        );
        let rendered = serde_json::to_string(&summary).expect("summary should serialize");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("token=abc"));
        assert!(!rendered.contains("C:\\\\Users\\\\Palo"));
    }

    #[test]
    fn component_health_registry_quarantines_after_repeated_failures() {
        let mut registry = ComponentHealthRegistry::default();
        let outcome = ComponentCallOutcome {
            component: "mcp.docs".to_owned(),
            capability: "tool.search".to_owned(),
            duration_ms: 25,
            timed_out: false,
            capability_denied: false,
            error_code: Some("protocol.invalid_response".to_owned()),
            resource_event: None,
        };

        let first = registry.record_outcome(outcome.clone(), 1_000);
        let second = registry.record_outcome(outcome.clone(), 2_000);
        let third = registry.record_outcome(outcome, 3_000);

        assert_eq!(first.state, ComponentHealthState::Degraded);
        assert_eq!(second.state, ComponentHealthState::Degraded);
        assert_eq!(third.state, ComponentHealthState::Quarantined);
        assert!(third.quarantine_until_unix_ms.is_some_and(|until| until > 3_000));
        assert_eq!(
            registry.fallback_for_component("mcp.docs").audit_reason,
            "component mcp.docs is quarantined"
        );
    }

    #[test]
    fn component_health_registry_unquarantine_requires_actor_and_backoff_expiry() {
        let mut registry = ComponentHealthRegistry::default();
        registry.mark_state(
            "plugin.policy",
            ComponentHealthState::Quarantined,
            "operator.quarantined",
            1_000,
        );
        {
            let record = registry.records.get_mut("plugin.policy").expect("record should exist");
            record.quarantine_until_unix_ms = Some(5_000);
        }

        assert!(registry.unquarantine_with_audit("plugin.policy", "", 6_000).is_none());
        assert!(registry.unquarantine_with_audit("plugin.policy", "user:test", 4_000).is_none());

        let audit = registry
            .unquarantine_with_audit("plugin.policy", "user:test", 5_000)
            .expect("unquarantine should be audited");

        assert_eq!(audit.from_state, ComponentHealthState::Quarantined);
        assert_eq!(audit.to_state, ComponentHealthState::Degraded);
        assert_eq!(audit.actor, "user:test");
        assert!(!registry.fallback_for_component("plugin.policy").use_fallback);
    }

    #[test]
    fn component_health_registry_respects_policy_disabled_state() {
        let mut registry = ComponentHealthRegistry::default();
        registry.mark_state(
            "skill.external",
            ComponentHealthState::DisabledByPolicy,
            "policy.disabled",
            1_000,
        );
        let record = registry.record_outcome(
            ComponentCallOutcome {
                component: "skill.external".to_owned(),
                capability: "preflight".to_owned(),
                duration_ms: 1,
                timed_out: false,
                capability_denied: false,
                error_code: None,
                resource_event: None,
            },
            2_000,
        );

        assert_eq!(record.state, ComponentHealthState::DisabledByPolicy);
        assert!(registry.fallback_for_component("skill.external").use_fallback);
        assert_eq!(registry.snapshot().len(), 1);
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
    fn mcp_health_component_reports_supervisor_degradation() {
        let component = mcp_health_component(&json!({
            "schema_version": 1,
            "catalog_generation": 7,
            "mode": "preview_only",
            "total_servers": 3,
            "enabled_servers": 2,
            "healthy_servers": 1,
            "degraded_servers": 1,
            "backoff_servers": 1,
            "quarantined_servers": 0,
            "disabled_servers": 1,
            "servers": [],
        }));

        assert_eq!(component.component, "mcp");
        assert_eq!(component.status, RuntimeHealthStatus::Degraded);
        assert!(component.reason_codes.contains(&"mcp.servers_degraded".to_owned()));
        assert!(component.reason_codes.contains(&"mcp.servers_in_backoff".to_owned()));
        assert_eq!(component.metrics.get("enabled_servers"), Some(&2_u64));
        assert_eq!(component.metrics.get("disabled_servers"), Some(&1_u64));
        assert_eq!(component.metrics.get("catalog_generation"), Some(&7_u64));
        assert!(component.repair_hints.contains(&"palyra_mcp_doctor".to_owned()));
        assert!(component.repair_hints.contains(&"palyra_mcp_reload_after_fix".to_owned()));
    }

    #[test]
    fn mcp_health_component_marks_snapshot_errors_unavailable() {
        let component = mcp_health_component(&json!({
            "schema_version": 1,
            "status": "unavailable",
            "error": "mcp supervisor lock poisoned",
        }));

        assert_eq!(component.status, RuntimeHealthStatus::Unavailable);
        assert!(component.reason_codes.contains(&"mcp.supervisor_snapshot_unavailable".to_owned()));
    }

    #[test]
    fn prometheus_renderer_uses_bounded_labels() {
        let mut rendered = String::new();
        push_sample(
            &mut rendered,
            "palyra_model_provider_requests_total",
            &[("provider_kind", "deterministic_provider")],
            2,
        );
        push_sample(
            &mut rendered,
            "palyra_run_stream_backpressure_coalesced_total",
            &[("boundary", "process_progress"), ("action", "coalesce_progress")],
            3,
        );
        push_sample(
            &mut rendered,
            "palyra_run_stream_deadline_exceeded_total",
            &[("scope", "tool_execution")],
            5,
        );
        push_sample(
            &mut rendered,
            "palyra_run_interrupt_latency_observations_total",
            &[("phase", "approval")],
            1,
        );

        assert!(rendered.contains(
            "palyra_model_provider_requests_total{provider_kind=\"deterministic_provider\"} 2"
        ));
        assert!(rendered.contains(
            "palyra_run_stream_backpressure_coalesced_total{boundary=\"process_progress\",action=\"coalesce_progress\"} 3"
        ));
        assert!(rendered
            .contains("palyra_run_stream_deadline_exceeded_total{scope=\"tool_execution\"} 5"));
        assert!(rendered
            .contains("palyra_run_interrupt_latency_observations_total{phase=\"approval\"} 1"));
        assert!(!rendered.contains("principal"));
        assert!(!rendered.contains("session_id"));
    }

    #[test]
    fn lifecycle_snapshot_blocks_new_runs_while_draining() {
        let snapshot = build_daemon_lifecycle_snapshot(
            DaemonLifecycleState::Draining,
            2,
            3,
            1,
            Some(1_730_000_000_000),
            None,
            ResumeGuardCounters {
                incomplete_runs: 2,
                requires_operator_review: 1,
                approval_pending: 1,
                ..ResumeGuardCounters::default()
            },
        );

        assert_eq!(snapshot.schema_version, DAEMON_LIFECYCLE_SCHEMA_VERSION);
        assert_eq!(snapshot.state, DaemonLifecycleState::Draining);
        assert!(!snapshot.accepts_new_runs);
        assert!(snapshot
            .reason_codes
            .contains(&"daemon.lifecycle.not_accepting_new_runs".to_owned()));
        assert!(snapshot
            .reason_codes
            .contains(&"daemon.lifecycle.resume_requires_operator_review".to_owned()));
    }

    #[test]
    fn metrics_label_validator_rejects_high_cardinality_and_secret_values() {
        assert!(validate_metric_labels(&[("provider_kind", "openai-compatible")]).is_ok());
        assert!(validate_metric_labels(&[
            ("boundary", "process_progress"),
            ("action", "coalesce_progress"),
            ("scope", "tool_execution"),
            ("phase", "delivery_terminal"),
        ])
        .is_ok());
        assert!(validate_metric_labels(&[("principal", "user:alice")]).is_err());
        assert!(validate_metric_labels(&[("provider_kind", "01ARZ3NDEKTSV4RRFFQ69G5FB0")]).is_err());
        assert!(validate_metric_labels(&[("provider_kind", "C:\\Users\\Palo\\secret")]).is_err());
        assert!(validate_metric_labels(&[("provider_kind", "Bearer raw")]).is_err());
        let catalog = build_metrics_catalog_snapshot();
        assert_eq!(catalog["series_cap"], PROMETHEUS_SERIES_CAP);
        assert!(catalog["metrics"].as_array().is_some_and(|metrics| {
            metrics.iter().any(|metric| {
                metric["name"] == "palyra_run_interrupt_latency_observations_total"
                    && metric["reason_code"] == "runtime.interrupt_latency.observed"
                    && metric["labels"] == json!(["phase"])
            })
        }));
    }

    #[test]
    fn timeline_jsonl_redacts_and_bounds_payloads() {
        let event = DiagnosticsTimelineEvent {
            schema_version: RUNTIME_TIMELINE_SCHEMA_VERSION,
            monotonic_ms: 42,
            wall_time_unix_ms: 1_730_000_000_000,
            component: "provider".to_owned(),
            phase: "request".to_owned(),
            outcome: "failed".to_owned(),
            correlation: BTreeMap::from([("run".to_owned(), "run-1".to_owned())]),
            payload: json!({
                "authorization": "Bearer raw",
                "message": format!("token=abc {}", "x".repeat(3_000)),
            }),
            redaction_level: "strict_bounded".to_owned(),
        };

        let jsonl = render_diagnostics_timeline_jsonl(&[event]).expect("timeline should render");
        assert!(jsonl.contains("\"truncated\":true"));
        assert!(!jsonl.contains("Bearer raw"));
        assert!(!jsonl.contains("abc "));
    }

    #[test]
    fn trace_jsonl_redacts_attributes_before_export() {
        let span = TraceSpanRecord {
            schema_version: TRACE_EXPORT_SCHEMA_VERSION,
            trace_id: "trace-1".to_owned(),
            span_id: "span-1".to_owned(),
            parent_span_id: None,
            name: "tool execution".to_owned(),
            component: "tool_runtime".to_owned(),
            started_at_unix_ms: 1_730_000_000_000,
            duration_ms: 25,
            outcome: "failed".to_owned(),
            correlation: BTreeMap::from([("run_id".to_owned(), "run_123".to_owned())]),
            attributes: json!({
                "url": "https://example.test/callback?token=raw",
                "refresh_token": "raw-refresh",
            }),
            redaction_level: "strict".to_owned(),
        };

        let jsonl = render_trace_jsonl(&[span]).expect("trace should render");
        assert!(jsonl.contains("\"component\":\"tool_runtime\""));
        assert!(jsonl.contains("\"url\""));
        assert!(!jsonl.contains("token=raw"));
        assert!(!jsonl.contains("raw-refresh"));
        assert_eq!(build_trace_exporter_contract()["default_exporter"], "jsonl");
    }

    #[test]
    fn run_stage_timing_report_tracks_success_and_missing_stages() {
        let report = build_run_stage_timing_report(&[
            RunStageTimingInput {
                stage: "prepare".to_owned(),
                started_at_unix_ms: 1_000,
                completed_at_unix_ms: Some(1_010),
                first_signal_at_unix_ms: None,
                timeout_ms: None,
                timeout_kind: None,
                outcome: "ok".to_owned(),
            },
            RunStageTimingInput {
                stage: "provider_request".to_owned(),
                started_at_unix_ms: 1_020,
                completed_at_unix_ms: Some(1_080),
                first_signal_at_unix_ms: Some(1_030),
                timeout_ms: Some(500),
                timeout_kind: None,
                outcome: "ok".to_owned(),
            },
        ]);

        assert_eq!(report.schema_version, RUN_STAGE_TIMING_SCHEMA_VERSION);
        assert_eq!(report.records.len(), 2);
        assert!(report.missing_stages.contains(&"tool_wait".to_owned()));
        assert!(!report.idle_breaker_triggered);
        assert!(report.timeout_classifications.is_empty());
    }

    #[test]
    fn run_stage_timing_report_classifies_timeout_owners() {
        let report = build_run_stage_timing_report(&[
            RunStageTimingInput {
                stage: "provider_request".to_owned(),
                started_at_unix_ms: 1_000,
                completed_at_unix_ms: Some(1_900),
                first_signal_at_unix_ms: Some(1_900),
                timeout_ms: Some(500),
                timeout_kind: None,
                outcome: "timeout".to_owned(),
            },
            RunStageTimingInput {
                stage: "provider_request".to_owned(),
                started_at_unix_ms: 2_000,
                completed_at_unix_ms: Some(3_000),
                first_signal_at_unix_ms: Some(2_050),
                timeout_ms: Some(1_000),
                timeout_kind: Some(RunStageTimeoutKind::ProviderHard),
                outcome: "timeout".to_owned(),
            },
            RunStageTimingInput {
                stage: "tool_wait".to_owned(),
                started_at_unix_ms: 3_000,
                completed_at_unix_ms: Some(4_000),
                first_signal_at_unix_ms: None,
                timeout_ms: Some(1_000),
                timeout_kind: Some(RunStageTimeoutKind::ToolWait),
                outcome: "timeout".to_owned(),
            },
            RunStageTimingInput {
                stage: "abort_settle".to_owned(),
                started_at_unix_ms: 4_000,
                completed_at_unix_ms: Some(4_250),
                first_signal_at_unix_ms: None,
                timeout_ms: Some(250),
                timeout_kind: Some(RunStageTimeoutKind::AbortSettle),
                outcome: "timeout".to_owned(),
            },
        ]);

        assert!(report.idle_breaker_triggered);
        assert_eq!(
            report.timeout_classifications,
            vec![
                "abort_settle_timeout".to_owned(),
                "provider_hard_timeout".to_owned(),
                "provider_idle_timeout".to_owned(),
                "tool_wait_timeout".to_owned(),
            ]
        );
        assert!(report.records.iter().any(|record| {
            record.stage == "tool_wait"
                && record.timeout_classification.as_deref() == Some("tool_wait_timeout")
        }));
    }

    #[test]
    fn contract_snapshot_suite_covers_plugin_abi_surfaces() {
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
        assert_eq!(
            snapshot["runtime_contracts_abi"]["snapshot_version"],
            PUBLIC_RUNTIME_CONTRACT_SNAPSHOT_VERSION
        );
        validate_public_contract_snapshot(&snapshot["runtime_contracts_abi"])
            .expect("runtime contracts snapshot should be public-safe");
        assert_eq!(
            snapshot["plugin_sdk_abi"]["snapshot_version"],
            PLUGIN_SDK_CONTRACT_SNAPSHOT_VERSION
        );
        validate_public_contract_snapshot(&snapshot["plugin_sdk_abi"])
            .expect("plugin SDK snapshot should be public-safe");
        assert_eq!(
            snapshot["skill_manifest_abi"]["snapshot_version"],
            "skill-manifest-contracts.v1"
        );
        validate_public_contract_snapshot(&snapshot["skill_manifest_abi"])
            .expect("skill manifest snapshot should be public-safe");
        assert!(
            snapshot["channel_command_abi"]["native_spec_count"].as_u64().unwrap_or_default() > 0
        );
        assert_eq!(snapshot["memory_provider_abi"]["redaction_required"], true);
    }

    #[test]
    fn managed_coding_contract_snapshot_matches_golden() {
        let actual = format!(
            "{}\n",
            serde_json::to_string_pretty(&build_managed_coding_contract_snapshot())
                .expect("managed coding contract snapshot serializes")
        );
        let golden = include_str!("../tests/golden/managed_coding_contract_snapshot.json")
            .replace("\r\n", "\n");
        if std::env::var_os("PALYRA_UPDATE_CONTRACT_SNAPSHOTS").is_some() {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/golden/managed_coding_contract_snapshot.json");
            std::fs::write(path, actual.as_bytes())
                .expect("managed coding contract golden can be updated");
            return;
        }
        assert_eq!(
            actual, golden,
            "managed coding contract drift requires a snapshot version bump and migration note"
        );
    }
}
