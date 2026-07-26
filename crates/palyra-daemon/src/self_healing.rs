//! Self-healing incident tracking and the background remediation loop.
//!
//! [`SelfHealingState`] keeps deduplicated runtime incidents, remediation attempts, and work
//! heartbeats in memory. [`spawn_self_healing_loop`] periodically inspects watchdog heartbeats,
//! stale approvals, the browser daemon, and installed skill artifacts. Per-feature modes come
//! from the `PALYRA_HEALING_*` env vars (default observe-only): low-risk fixes may auto-execute,
//! everything else only records a remediation proposal for the operator.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env, fs,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::{interval, MissedTickBehavior};
use tonic::Request as TonicRequest;
use tracing::warn;

use crate::{
    app::state::AppState,
    apply_browser_service_auth, browser_v1, build_console_browser_client, common_v1,
    gateway::{
        cleanup_run_resources, GatewayRuntimeState, OrchestratorRunWaitRequest,
        ProcessLeaseReconciliationReport,
    },
    journal::{
        OrchestratorCancelRequest, RemediationDecision, SkillExecutionStatus, StuckRunIncidentV2,
        StuckRunRemediationClaimOutcome, StuckRunRemediationCompletionOutcome,
        StuckRunRemediationDecisionKind, StuckRunRemediationPolicy,
    },
    load_installed_skills_index, managed_skill_artifact_path,
    orchestrator::RunLifecycleState,
    resolve_skills_root,
};

const INCIDENT_HISTORY_LIMIT: usize = 128;
const REMEDIATION_HISTORY_LIMIT: usize = 128;
const HEALING_LOOP_INTERVAL: Duration = Duration::from_secs(15);
// Escalation thresholds: active work whose heartbeat stalls for 2 minutes is treated as stuck,
// while pending approvals get a 10 minute review window because a human is expected in the loop.
const RUN_HEARTBEAT_STUCK_AFTER_MS: i64 = 120_000;
const BACKGROUND_TASK_STUCK_AFTER_MS: i64 = 120_000;
const APPROVAL_STUCK_AFTER_MS: i64 = 600_000;
const STUCK_RUN_CLAIM_TTL_MS: i64 = 30_000;
const STUCK_RUN_SETTLE_TIMEOUT: Duration = Duration::from_millis(750);
const STUCK_RUN_REMEDIATION_LIMIT: usize = 4;
const STUCK_RUN_REMEDIATION_WINDOW_MS: i64 = 60_000;
const STUCK_RUN_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const STUCK_RUN_CIRCUIT_OPEN_MS: i64 = 300_000;

const HEALING_MODE_ENV: &str = "PALYRA_HEALING_MODE";
const HEALING_WATCHDOG_MODE_ENV: &str = "PALYRA_HEALING_WATCHDOG_MODE";
const HEALING_BROWSER_MODE_ENV: &str = "PALYRA_HEALING_BROWSER_MODE";
const HEALING_ARTIFACT_MODE_ENV: &str = "PALYRA_HEALING_ARTIFACT_MODE";
const HEALING_APPROVALS_MODE_ENV: &str = "PALYRA_HEALING_APPROVALS_MODE";

/// Subsystem an incident belongs to; part of the incident dedupe identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IncidentDomain {
    Watchdog,
    Browser,
    Artifact,
    Approval,
}

/// Incident severity; `Ord` follows declaration order so `Critical` compares highest.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IncidentSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Incident lifecycle; re-observing a resolved incident reopens it under the same id.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IncidentState {
    Open,
    Remediating,
    Resolved,
}

/// How risky executing a remediation is, shown to operators before they approve it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemediationRiskLevel {
    Low,
    Medium,
    High,
}

/// Scope a remediation can affect, from a single session up to the whole daemon.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemediationBlastRadius {
    Session,
    Workspace,
    Global,
}

/// Operating mode for self-healing: detection off, detect-and-report, or detect-and-fix.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelfHealingMode {
    Disabled,
    ObserveOnly,
    GenerationSafeAutoRecovery,
}

impl SelfHealingMode {
    // Unknown or empty env values fall back to the given default instead of failing startup.
    fn from_env_value(value: Option<String>, default: Self) -> Self {
        match value.as_deref().map(str::trim).filter(|candidate| !candidate.is_empty()) {
            Some("disabled") => Self::Disabled,
            Some("observe_only") => Self::ObserveOnly,
            // `auto` remains accepted as a compatibility alias for existing
            // deployments, but diagnostics always expose the explicit policy.
            Some("generation_safe_auto_recovery" | "auto") => Self::GenerationSafeAutoRecovery,
            _ => default,
        }
    }
}

/// Independently configurable self-healing feature areas.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelfHealingFeature {
    Watchdog,
    Browser,
    Artifact,
    Approval,
}

/// Outcome of one remediation attempt; `Skipped` records why auto-execution did not happen.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemediationAttemptStatus {
    Planned,
    Succeeded,
    Failed,
    Skipped,
}

/// Kind of tracked work behind a heartbeat; part of the heartbeat key.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkHeartbeatKind {
    Run,
    BackgroundTask,
    Approval,
}

/// Proposed fix attached to an incident, including the approval/auto-execution policy flags.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeRemediationDescriptor {
    pub remediation_id: String,
    pub label: String,
    pub description: String,
    pub risk_level: RemediationRiskLevel,
    pub blast_radius: RemediationBlastRadius,
    pub requires_approval: bool,
    pub auto_executable: bool,
}

/// Current state of one deduplicated incident.
///
/// The id is derived from `(domain, dedupe_key)`, so repeated observations of the same problem
/// update this record in place rather than creating new incidents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeIncidentRecord {
    pub incident_id: String,
    pub domain: IncidentDomain,
    pub severity: IncidentSeverity,
    pub state: IncidentState,
    pub summary: String,
    pub detail: String,
    pub dedupe_key: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub resolved_at_unix_ms: Option<i64>,
    pub remediation: Option<RuntimeRemediationDescriptor>,
}

/// Append-only audit entry recorded for every incident observation or resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeIncidentHistoryEntry {
    pub incident_id: String,
    pub domain: IncidentDomain,
    pub state: IncidentState,
    pub summary: String,
    pub recorded_at_unix_ms: i64,
}

/// Audit record of one planned, executed, failed, or skipped remediation attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeRemediationAttemptRecord {
    pub attempt_id: String,
    pub incident_id: String,
    pub remediation_id: String,
    pub feature: SelfHealingFeature,
    pub status: RemediationAttemptStatus,
    pub detail: String,
    pub recorded_at_unix_ms: i64,
}

/// Last reported liveness signal for a tracked unit of work.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkHeartbeatRecord {
    pub heartbeat_key: String,
    pub kind: WorkHeartbeatKind,
    pub object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_generation: Option<u64>,
    pub summary: String,
    pub updated_at_unix_ms: i64,
}

/// Resource family registered with the unified orphan reaper.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OrphanResourceType {
    Run,
    Process,
    Mcp,
    Lsp,
    Acp,
    Pty,
    Worker,
}

/// Result produced by one registered cleanup adapter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrphanReconciliationEntry {
    pub resource_type: OrphanResourceType,
    pub inspected_count: usize,
    pub reconciled_count: usize,
    pub quarantined_count: usize,
    pub reason_code: String,
}

/// Bounded unified report covering every runtime resource family.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrphanReconciliationReport {
    pub report_id: String,
    pub policy: StuckRunRemediationPolicy,
    pub entries: Vec<OrphanReconciliationEntry>,
    pub bounded: bool,
    pub completed_at_unix_ms: i64,
    pub schema_version: u32,
}

/// Effective mode of one feature area, as exposed to status surfaces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SelfHealingFeatureSettingSnapshot {
    pub feature: SelfHealingFeature,
    pub mode: SelfHealingMode,
}

/// Snapshot of the global mode plus all per-feature overrides.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SelfHealingSettingsSnapshot {
    pub mode: SelfHealingMode,
    pub features: Vec<SelfHealingFeatureSettingSnapshot>,
}

/// Aggregated incident counts by state, domain, and severity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeIncidentSummary {
    pub active: usize,
    pub resolving: usize,
    pub resolved: usize,
    pub by_domain: BTreeMap<String, usize>,
    pub by_severity: BTreeMap<String, usize>,
}

/// Input for [`SelfHealingState::observe_incident`]; `dedupe_key` defines incident identity
/// within its domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeIncidentObservation {
    pub domain: IncidentDomain,
    pub severity: IncidentSeverity,
    pub summary: String,
    pub detail: String,
    pub dedupe_key: String,
    pub remediation: Option<RuntimeRemediationDescriptor>,
}

/// Input for [`SelfHealingState::record_heartbeat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkHeartbeatUpdate {
    pub kind: WorkHeartbeatKind,
    pub object_id: String,
    pub execution_generation: Option<u64>,
    pub summary: String,
}

#[derive(Debug, Clone, Copy)]
struct SelfHealingSettings {
    global_mode: SelfHealingMode,
    watchdog_mode: SelfHealingMode,
    browser_mode: SelfHealingMode,
    artifact_mode: SelfHealingMode,
    approval_mode: SelfHealingMode,
}

#[derive(Debug, Default)]
struct SelfHealingStateInner {
    incidents: BTreeMap<String, RuntimeIncidentRecord>,
    incident_index: HashMap<String, String>,
    incident_history: Vec<RuntimeIncidentHistoryEntry>,
    remediation_attempts: Vec<RuntimeRemediationAttemptRecord>,
    heartbeats: HashMap<String, WorkHeartbeatRecord>,
    stuck_run_remediation_window: VecDeque<i64>,
    stuck_run_consecutive_failures: u32,
    stuck_run_circuit_open_until_unix_ms: Option<i64>,
    latest_orphan_reconciliation: Option<OrphanReconciliationReport>,
}

/// In-memory self-healing store: settings are read at construction, mutable state (incidents,
/// history, heartbeats) lives behind one mutex. Nothing here is persisted across restarts.
///
/// All accessor and mutator methods panic if the inner mutex is poisoned, since a panic while
/// holding the lock would already indicate a daemon bug.
#[derive(Debug)]
pub(crate) struct SelfHealingState {
    settings: SelfHealingSettings,
    inner: Mutex<SelfHealingStateInner>,
}

impl Default for SelfHealingState {
    fn default() -> Self {
        Self::new()
    }
}

impl SelfHealingState {
    /// Builds the state with modes resolved from `PALYRA_HEALING_*` env vars; the global mode
    /// (default observe-only) is the fallback for every per-feature override.
    #[must_use]
    pub(crate) fn new() -> Self {
        let global_mode = SelfHealingMode::from_env_value(
            env::var(HEALING_MODE_ENV).ok(),
            SelfHealingMode::ObserveOnly,
        );
        let settings = SelfHealingSettings {
            global_mode,
            watchdog_mode: SelfHealingMode::from_env_value(
                env::var(HEALING_WATCHDOG_MODE_ENV).ok(),
                global_mode,
            ),
            browser_mode: SelfHealingMode::from_env_value(
                env::var(HEALING_BROWSER_MODE_ENV).ok(),
                global_mode,
            ),
            artifact_mode: SelfHealingMode::from_env_value(
                env::var(HEALING_ARTIFACT_MODE_ENV).ok(),
                global_mode,
            ),
            approval_mode: SelfHealingMode::from_env_value(
                env::var(HEALING_APPROVALS_MODE_ENV).ok(),
                global_mode,
            ),
        };
        Self { settings, inner: Mutex::new(SelfHealingStateInner::default()) }
    }

    /// Returns the effective global and per-feature modes.
    #[must_use]
    pub(crate) fn settings_snapshot(&self) -> SelfHealingSettingsSnapshot {
        SelfHealingSettingsSnapshot {
            mode: self.settings.global_mode,
            features: vec![
                SelfHealingFeatureSettingSnapshot {
                    feature: SelfHealingFeature::Watchdog,
                    mode: self.settings.watchdog_mode,
                },
                SelfHealingFeatureSettingSnapshot {
                    feature: SelfHealingFeature::Browser,
                    mode: self.settings.browser_mode,
                },
                SelfHealingFeatureSettingSnapshot {
                    feature: SelfHealingFeature::Artifact,
                    mode: self.settings.artifact_mode,
                },
                SelfHealingFeatureSettingSnapshot {
                    feature: SelfHealingFeature::Approval,
                    mode: self.settings.approval_mode,
                },
            ],
        }
    }

    /// Returns the effective mode for one feature area.
    #[must_use]
    pub(crate) fn mode_for_feature(&self, feature: SelfHealingFeature) -> SelfHealingMode {
        match feature {
            SelfHealingFeature::Watchdog => self.settings.watchdog_mode,
            SelfHealingFeature::Browser => self.settings.browser_mode,
            SelfHealingFeature::Artifact => self.settings.artifact_mode,
            SelfHealingFeature::Approval => self.settings.approval_mode,
        }
    }

    /// Aggregates all known incidents into per-state/domain/severity counts.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn incident_summary(&self) -> RuntimeIncidentSummary {
        let inner = self.inner.lock().expect("self-healing mutex poisoned");
        build_incident_summary(inner.incidents.values())
    }

    /// Returns up to `limit` unresolved incidents, most recently updated first.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn active_incidents(&self, limit: usize) -> Vec<RuntimeIncidentRecord> {
        let mut incidents = self
            .inner
            .lock()
            .expect("self-healing mutex poisoned")
            .incidents
            .values()
            .filter(|incident| incident.state != IncidentState::Resolved)
            .cloned()
            .collect::<Vec<_>>();
        incidents.sort_by_key(|incident| std::cmp::Reverse(incident.updated_at_unix_ms));
        incidents.truncate(limit);
        incidents
    }

    /// Returns up to `limit` incident history entries, newest first.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn recent_incident_history(&self, limit: usize) -> Vec<RuntimeIncidentHistoryEntry> {
        let mut entries =
            self.inner.lock().expect("self-healing mutex poisoned").incident_history.clone();
        entries.reverse();
        entries.truncate(limit);
        entries
    }

    /// Returns up to `limit` remediation attempts, newest first.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn recent_remediation_attempts(
        &self,
        limit: usize,
    ) -> Vec<RuntimeRemediationAttemptRecord> {
        let mut entries =
            self.inner.lock().expect("self-healing mutex poisoned").remediation_attempts.clone();
        entries.reverse();
        entries.truncate(limit);
        entries
    }

    /// Returns all tracked heartbeats, most recently updated first.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn list_heartbeats(&self) -> Vec<WorkHeartbeatRecord> {
        let mut heartbeats = self
            .inner
            .lock()
            .expect("self-healing mutex poisoned")
            .heartbeats
            .values()
            .cloned()
            .collect::<Vec<_>>();
        heartbeats.sort_by_key(|heartbeat| std::cmp::Reverse(heartbeat.updated_at_unix_ms));
        heartbeats
    }

    /// Returns the exact current heartbeat for a generation-sensitive recheck.
    #[must_use]
    pub(crate) fn heartbeat(
        &self,
        kind: WorkHeartbeatKind,
        object_id: &str,
    ) -> Option<WorkHeartbeatRecord> {
        self.inner
            .lock()
            .expect("self-healing mutex poisoned")
            .heartbeats
            .get(heartbeat_key(kind, object_id).as_str())
            .cloned()
    }

    #[must_use]
    pub(crate) fn latest_orphan_reconciliation(&self) -> Option<OrphanReconciliationReport> {
        self.inner.lock().expect("self-healing mutex poisoned").latest_orphan_reconciliation.clone()
    }

    fn record_orphan_reconciliation(&self, report: OrphanReconciliationReport) {
        self.inner.lock().expect("self-healing mutex poisoned").latest_orphan_reconciliation =
            Some(report);
    }

    fn acquire_stuck_run_remediation_permit(
        &self,
        now_unix_ms: i64,
    ) -> Result<(), StuckRunRemediationDecisionKind> {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        if inner
            .stuck_run_circuit_open_until_unix_ms
            .is_some_and(|open_until| open_until > now_unix_ms)
        {
            return Err(StuckRunRemediationDecisionKind::CircuitOpen);
        }
        inner.stuck_run_circuit_open_until_unix_ms = None;
        while inner.stuck_run_remediation_window.front().is_some_and(|recorded_at| {
            now_unix_ms.saturating_sub(*recorded_at) >= STUCK_RUN_REMEDIATION_WINDOW_MS
        }) {
            inner.stuck_run_remediation_window.pop_front();
        }
        if inner.stuck_run_remediation_window.len() >= STUCK_RUN_REMEDIATION_LIMIT {
            return Err(StuckRunRemediationDecisionKind::RateLimited);
        }
        inner.stuck_run_remediation_window.push_back(now_unix_ms);
        Ok(())
    }

    fn record_stuck_run_remediation_success(&self) {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        inner.stuck_run_consecutive_failures = 0;
        inner.stuck_run_circuit_open_until_unix_ms = None;
    }

    fn record_stuck_run_remediation_failure(&self, now_unix_ms: i64) {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        inner.stuck_run_consecutive_failures =
            inner.stuck_run_consecutive_failures.saturating_add(1);
        if inner.stuck_run_consecutive_failures >= STUCK_RUN_CIRCUIT_FAILURE_THRESHOLD {
            inner.stuck_run_circuit_open_until_unix_ms =
                Some(now_unix_ms.saturating_add(STUCK_RUN_CIRCUIT_OPEN_MS));
        }
    }

    /// Records (or refreshes) the liveness heartbeat for one unit of work.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    pub(crate) fn record_heartbeat(&self, update: WorkHeartbeatUpdate) {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        let heartbeat_key = heartbeat_key(update.kind, update.object_id.as_str());
        inner.heartbeats.insert(
            heartbeat_key.clone(),
            WorkHeartbeatRecord {
                heartbeat_key,
                kind: update.kind,
                object_id: update.object_id,
                execution_generation: update.execution_generation,
                summary: update.summary,
                updated_at_unix_ms: current_unix_ms(),
            },
        );
    }

    /// Removes a heartbeat once its work finished and resolves any stuck-watchdog incident that
    /// was opened for it.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    pub(crate) fn clear_heartbeat(&self, kind: WorkHeartbeatKind, object_id: &str) {
        self.clear_heartbeat_if_generation(kind, object_id, None);
    }

    /// Removes a heartbeat only when it still belongs to the supplied execution generation.
    pub(crate) fn clear_heartbeat_if_generation(
        &self,
        kind: WorkHeartbeatKind,
        object_id: &str,
        execution_generation: Option<u64>,
    ) {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        let key = heartbeat_key(kind, object_id);
        let Some(heartbeat) = inner.heartbeats.get(key.as_str()) else {
            return;
        };
        if execution_generation.is_some() && heartbeat.execution_generation != execution_generation
        {
            return;
        }
        let heartbeat =
            inner.heartbeats.remove(key.as_str()).expect("heartbeat exists after generation check");
        resolve_incident_locked(
            &mut inner,
            IncidentDomain::Watchdog,
            heartbeat_dedupe_key(&heartbeat).as_str(),
            "heartbeat cleared",
        );
    }

    /// Upserts an incident keyed by `(domain, dedupe_key)` and returns the stored record.
    ///
    /// Re-observing refreshes severity, summary, detail, and remediation while preserving
    /// `created_at_unix_ms`; a previously resolved incident is reopened under the same id so the
    /// history stays attached to one identity.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    #[must_use]
    pub(crate) fn observe_incident(
        &self,
        observation: RuntimeIncidentObservation,
    ) -> RuntimeIncidentRecord {
        let now = current_unix_ms();
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        let index_key = incident_index_key(observation.domain, observation.dedupe_key.as_str());
        let incident_id = inner
            .incident_index
            .get(index_key.as_str())
            .cloned()
            .unwrap_or_else(|| stable_sha256_id("incident", index_key.as_str()));
        let created_at = inner
            .incidents
            .get(incident_id.as_str())
            .map(|record| record.created_at_unix_ms)
            .unwrap_or(now);
        let record = RuntimeIncidentRecord {
            incident_id: incident_id.clone(),
            domain: observation.domain,
            severity: observation.severity,
            state: IncidentState::Open,
            summary: observation.summary,
            detail: observation.detail,
            dedupe_key: observation.dedupe_key,
            created_at_unix_ms: created_at,
            updated_at_unix_ms: now,
            resolved_at_unix_ms: None,
            remediation: observation.remediation,
        };
        inner.incident_index.insert(index_key, incident_id.clone());
        inner.incidents.insert(incident_id.clone(), record.clone());
        push_incident_history(
            &mut inner.incident_history,
            RuntimeIncidentHistoryEntry {
                incident_id,
                domain: record.domain,
                state: record.state,
                summary: record.summary.clone(),
                recorded_at_unix_ms: now,
            },
        );
        record
    }

    /// Marks the incident identified by `(domain, dedupe_key)` resolved; no-op when the incident
    /// is unknown or already resolved.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    pub(crate) fn resolve_incident(&self, domain: IncidentDomain, dedupe_key: &str, summary: &str) {
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        resolve_incident_locked(&mut inner, domain, dedupe_key, summary);
    }

    /// Appends a remediation attempt to the capped audit history and returns the record.
    ///
    /// # Panics
    ///
    /// Panics if the self-healing mutex is poisoned.
    pub(crate) fn record_remediation_attempt(
        &self,
        incident_id: &str,
        remediation_id: &str,
        feature: SelfHealingFeature,
        status: RemediationAttemptStatus,
        detail: impl Into<String>,
    ) -> RuntimeRemediationAttemptRecord {
        let record = RuntimeRemediationAttemptRecord {
            attempt_id: stable_sha256_id(
                "remediation",
                format!("{incident_id}:{remediation_id}:{feature:?}:{}", current_unix_ms())
                    .as_str(),
            ),
            incident_id: incident_id.to_owned(),
            remediation_id: remediation_id.to_owned(),
            feature,
            status,
            detail: detail.into(),
            recorded_at_unix_ms: current_unix_ms(),
        };
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        inner.remediation_attempts.push(record.clone());
        truncate_vec(&mut inner.remediation_attempts, REMEDIATION_HISTORY_LIMIT);
        record
    }
}

fn resolve_incident_locked(
    inner: &mut SelfHealingStateInner,
    domain: IncidentDomain,
    dedupe_key: &str,
    summary: &str,
) {
    let index_key = incident_index_key(domain, dedupe_key);
    let Some(incident_id) = inner.incident_index.get(index_key.as_str()).cloned() else {
        return;
    };
    let now = current_unix_ms();
    let Some(record) = inner.incidents.get_mut(incident_id.as_str()) else {
        return;
    };
    if record.state == IncidentState::Resolved {
        return;
    }
    record.state = IncidentState::Resolved;
    record.updated_at_unix_ms = now;
    record.resolved_at_unix_ms = Some(now);
    push_incident_history(
        &mut inner.incident_history,
        RuntimeIncidentHistoryEntry {
            incident_id,
            domain,
            state: IncidentState::Resolved,
            summary: summary.to_owned(),
            recorded_at_unix_ms: now,
        },
    );
}

/// Spawns the periodic self-healing loop; it runs until the daemon shuts down and logs (rather
/// than propagates) cycle failures so one bad cycle never kills the watchdog.
pub(crate) fn spawn_self_healing_loop(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = state.runtime.daemon_lifecycle.subscribe();
        let mut ticker = interval(HEALING_LOOP_INTERVAL);
        // Delay instead of bursting after a stall (for example host suspend); back-to-back
        // healing cycles would only re-observe the same incidents.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                    continue;
                }
            }
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
            if let Err(error) = run_self_healing_cycle(&state).await {
                warn!(message = %error, "self-healing cycle failed");
            }
        }
    })
}

async fn run_self_healing_cycle(state: &AppState) -> Result<(), String> {
    let mut errors = Vec::new();
    if let Err(error) = evaluate_watchdog_runtime(state).await {
        errors.push(("watchdog", error));
    }
    if let Err(error) = run_unified_orphan_reaper(state).await {
        errors.push(("orphan_reaper", error));
    }
    if let Err(error) = evaluate_pending_approvals(state).await {
        errors.push(("approvals", error));
    }
    if let Err(error) = evaluate_browser_runtime(state).await {
        errors.push(("browser", error));
    }
    if let Err(error) = evaluate_skill_runtime(state).await {
        errors.push(("artifact", error));
    }
    format_self_healing_cycle_result(errors)
}

async fn evaluate_watchdog_runtime(state: &AppState) -> Result<(), String> {
    if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Watchdog)
        == SelfHealingMode::Disabled
    {
        return Ok(());
    }
    let now = current_unix_ms();
    for heartbeat in state.runtime.self_healing_heartbeats() {
        match heartbeat.kind {
            WorkHeartbeatKind::Run => {
                let snapshot = state
                    .runtime
                    .orchestrator_run_status_snapshot(heartbeat.object_id.clone())
                    .await
                    .map_err(|error| format!("failed to load run heartbeat state: {error}"))?;
                // A missing snapshot is treated like terminal work: the heartbeat is cleared so
                // deleted runs cannot keep a stuck-watchdog incident alive forever.
                if snapshot.as_ref().is_none_or(|run| is_terminal_run_state(run.state.as_str())) {
                    state.runtime.clear_self_healing_heartbeat(
                        WorkHeartbeatKind::Run,
                        heartbeat.object_id.as_str(),
                    );
                    state.runtime.resolve_self_healing_incident(
                        IncidentDomain::Watchdog,
                        heartbeat_dedupe_key(&heartbeat).as_str(),
                        "run heartbeat returned to terminal state",
                    );
                    continue;
                }
                evaluate_stale_run_heartbeat(state, &heartbeat, now).await?;
            }
            WorkHeartbeatKind::BackgroundTask => {
                let snapshot = state
                    .runtime
                    .get_orchestrator_background_task(heartbeat.object_id.clone())
                    .await
                    .map_err(|error| {
                        format!("failed to load background task heartbeat state: {error}")
                    })?;
                if snapshot.as_ref().is_none_or(|task| is_terminal_task_state(task.state.as_str()))
                {
                    state.runtime.clear_self_healing_heartbeat(
                        WorkHeartbeatKind::BackgroundTask,
                        heartbeat.object_id.as_str(),
                    );
                    state.runtime.resolve_self_healing_incident(
                        IncidentDomain::Watchdog,
                        heartbeat_dedupe_key(&heartbeat).as_str(),
                        "background task heartbeat returned to terminal state",
                    );
                    continue;
                }
                evaluate_stale_heartbeat(
                    &state.runtime,
                    &heartbeat,
                    now,
                    BACKGROUND_TASK_STUCK_AFTER_MS,
                    build_background_task_watchdog_remediation(),
                );
            }
            // Approvals are aged by evaluate_pending_approvals against the approval records
            // themselves, so their heartbeats carry no extra signal here.
            WorkHeartbeatKind::Approval => {}
        }
    }
    Ok(())
}

fn evaluate_stale_heartbeat(
    runtime: &Arc<GatewayRuntimeState>,
    heartbeat: &WorkHeartbeatRecord,
    now: i64,
    threshold_ms: i64,
    remediation: RuntimeRemediationDescriptor,
) {
    let age_ms = now.saturating_sub(heartbeat.updated_at_unix_ms);
    if age_ms <= threshold_ms {
        runtime.resolve_self_healing_incident(
            IncidentDomain::Watchdog,
            heartbeat_dedupe_key(heartbeat).as_str(),
            "heartbeat moved again before stuck threshold",
        );
        return;
    }
    let _ = runtime.observe_self_healing_incident(RuntimeIncidentObservation {
        domain: IncidentDomain::Watchdog,
        severity: IncidentSeverity::High,
        summary: format!("{} appears stuck", heartbeat.summary),
        detail: format!(
            "Heartbeat '{}' ({:?}) has not advanced for {} ms.",
            heartbeat.object_id, heartbeat.kind, age_ms
        ),
        dedupe_key: heartbeat_dedupe_key(heartbeat),
        remediation: Some(remediation),
    });
}

async fn evaluate_stale_run_heartbeat(
    state: &AppState,
    heartbeat: &WorkHeartbeatRecord,
    now: i64,
) -> Result<(), String> {
    let age_ms = now.saturating_sub(heartbeat.updated_at_unix_ms);
    let dedupe_key = heartbeat_dedupe_key(heartbeat);
    if age_ms <= RUN_HEARTBEAT_STUCK_AFTER_MS {
        state.runtime.resolve_self_healing_incident(
            IncidentDomain::Watchdog,
            dedupe_key.as_str(),
            "heartbeat moved again before stuck threshold",
        );
        return Ok(());
    }
    let Some(incident) = state
        .runtime
        .inspect_stuck_run_incident(heartbeat)
        .map_err(|error| format!("failed to inspect stuck-run authority: {error}"))?
    else {
        return Ok(());
    };
    let runtime_incident =
        state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Watchdog,
            severity: IncidentSeverity::High,
            summary: "run appears stuck".to_owned(),
            detail: format!(
                "Run reference {} has a stale heartbeat for {} ms under generation {}.",
                short_hash(incident.run_id.as_str()),
                age_ms,
                incident.generation,
            ),
            dedupe_key: dedupe_key.clone(),
            remediation: Some(build_run_watchdog_remediation()),
        });
    let policy = if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Watchdog)
        == SelfHealingMode::GenerationSafeAutoRecovery
    {
        StuckRunRemediationPolicy::GenerationSafeAutoRecovery
    } else {
        StuckRunRemediationPolicy::ObserveOnly
    };
    let current_heartbeat =
        state.runtime.self_healing_heartbeat(WorkHeartbeatKind::Run, incident.run_id.as_str());
    let decision_kind =
        decide_stuck_run_remediation(&incident, current_heartbeat.as_ref(), policy, now);
    let mut decision =
        RemediationDecision::new(incident.incident_id.clone(), policy, decision_kind, now);
    state
        .runtime
        .record_stuck_run_remediation_decision(&decision)
        .map_err(|error| format!("failed to persist stuck-run decision: {error}"))?;
    if decision_kind != StuckRunRemediationDecisionKind::AutoRecover {
        let _ = state.runtime.record_self_healing_remediation_attempt(
            runtime_incident.incident_id.as_str(),
            "generation_safe_stuck_run_recovery",
            SelfHealingFeature::Watchdog,
            RemediationAttemptStatus::Skipped,
            decision.reason_code,
        );
        return Ok(());
    }
    if let Err(blocked) = state.runtime.self_healing.acquire_stuck_run_remediation_permit(now) {
        decision = RemediationDecision::new(incident.incident_id.clone(), policy, blocked, now);
        state
            .runtime
            .record_stuck_run_remediation_decision(&decision)
            .map_err(|error| format!("failed to persist remediation throttle decision: {error}"))?;
        let _ = state.runtime.record_self_healing_remediation_attempt(
            runtime_incident.incident_id.as_str(),
            "generation_safe_stuck_run_recovery",
            SelfHealingFeature::Watchdog,
            RemediationAttemptStatus::Skipped,
            decision.reason_code,
        );
        return Ok(());
    }
    let worker_id = format!("self-healing-watchdog:{}", std::process::id());
    let claim = state
        .runtime
        .claim_stuck_run_remediation(&incident, worker_id.as_str(), STUCK_RUN_CLAIM_TTL_MS)
        .map_err(|error| format!("failed to claim stuck-run remediation: {error}"))?;
    let claim_epoch = match claim {
        StuckRunRemediationClaimOutcome::Claimed { claim_epoch } => claim_epoch,
        StuckRunRemediationClaimOutcome::Busy
        | StuckRunRemediationClaimOutcome::AlreadyCompleted => return Ok(()),
        StuckRunRemediationClaimOutcome::StaleAuthority => {
            let stale = RemediationDecision::new(
                incident.incident_id.clone(),
                policy,
                StuckRunRemediationDecisionKind::LaneOwnerMismatch,
                current_unix_ms(),
            );
            state
                .runtime
                .record_stuck_run_remediation_decision(&stale)
                .map_err(|error| format!("failed to persist stale-authority decision: {error}"))?;
            return Ok(());
        }
    };
    let current_heartbeat =
        state.runtime.self_healing_heartbeat(WorkHeartbeatKind::Run, incident.run_id.as_str());
    if decide_stuck_run_remediation(
        &incident,
        current_heartbeat.as_ref(),
        policy,
        current_unix_ms(),
    ) != StuckRunRemediationDecisionKind::AutoRecover
    {
        let fresh = RemediationDecision::new(
            incident.incident_id.clone(),
            policy,
            StuckRunRemediationDecisionKind::FreshHeartbeat,
            current_unix_ms(),
        );
        state
            .runtime
            .record_stuck_run_remediation_decision(&fresh)
            .map_err(|error| format!("failed to persist post-claim heartbeat decision: {error}"))?;
        return Ok(());
    }
    let attempt =
        remediate_claimed_stuck_run(state, &incident, worker_id.as_str(), claim_epoch).await;
    match attempt {
        Ok(()) => {
            state.runtime.self_healing.record_stuck_run_remediation_success();
            let _ = state.runtime.record_self_healing_remediation_attempt(
                runtime_incident.incident_id.as_str(),
                "generation_safe_stuck_run_recovery",
                SelfHealingFeature::Watchdog,
                RemediationAttemptStatus::Succeeded,
                "runtime.healing.stuck_run.continuation_queued",
            );
            state.runtime.resolve_self_healing_incident(
                IncidentDomain::Watchdog,
                dedupe_key.as_str(),
                "stuck run was generation-fenced and queued for safe continuation",
            );
            Ok(())
        }
        Err(error) => {
            state.runtime.self_healing.record_stuck_run_remediation_failure(current_unix_ms());
            let _ = state.runtime.record_self_healing_remediation_attempt(
                runtime_incident.incident_id.as_str(),
                "generation_safe_stuck_run_recovery",
                SelfHealingFeature::Watchdog,
                RemediationAttemptStatus::Failed,
                error.clone(),
            );
            Err(error)
        }
    }
}

fn decide_stuck_run_remediation(
    incident: &StuckRunIncidentV2,
    current_heartbeat: Option<&WorkHeartbeatRecord>,
    policy: StuckRunRemediationPolicy,
    now_unix_ms: i64,
) -> StuckRunRemediationDecisionKind {
    let Some(current_heartbeat) = current_heartbeat else {
        return StuckRunRemediationDecisionKind::FreshHeartbeat;
    };
    if current_heartbeat.updated_at_unix_ms != incident.heartbeat_updated_at_unix_ms {
        return StuckRunRemediationDecisionKind::FreshHeartbeat;
    }
    if current_heartbeat
        .execution_generation
        .is_some_and(|generation| generation != incident.generation)
        || incident.heartbeat_generation.is_some_and(|generation| generation != incident.generation)
    {
        return StuckRunRemediationDecisionKind::StaleGeneration;
    }
    if incident.generation_lease_expires_at_unix_ms <= now_unix_ms {
        return StuckRunRemediationDecisionKind::ExpiredGenerationLease;
    }
    if incident.mutating_tool_in_flight {
        return StuckRunRemediationDecisionKind::ActiveMutationBlocked;
    }
    if incident.pending_approval {
        return StuckRunRemediationDecisionKind::ApprovalBlocked;
    }
    if !(incident.read_only_tool_wait || incident.provider_wait_in_flight) {
        return StuckRunRemediationDecisionKind::UnsafeWaitState;
    }
    match policy {
        StuckRunRemediationPolicy::ObserveOnly => StuckRunRemediationDecisionKind::ObserveOnly,
        StuckRunRemediationPolicy::GenerationSafeAutoRecovery => {
            StuckRunRemediationDecisionKind::AutoRecover
        }
    }
}

async fn remediate_claimed_stuck_run(
    state: &AppState,
    incident: &StuckRunIncidentV2,
    worker_id: &str,
    claim_epoch: u64,
) -> Result<(), String> {
    state
        .runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest {
            run_id: incident.run_id.clone(),
            reason: "generation-safe stale read-only wait remediation".to_owned(),
        })
        .await
        .map_err(|error| format!("cooperative stuck-run cancellation failed: {error}"))?;
    let wait = state
        .runtime
        .wait_for_orchestrator_run(OrchestratorRunWaitRequest {
            run_id: incident.run_id.clone(),
            timeout: STUCK_RUN_SETTLE_TIMEOUT,
            poll_interval: Duration::from_millis(50),
            return_on_waiting: false,
        })
        .await;
    match wait {
        Ok(outcome) if outcome.snapshot.state != RunLifecycleState::Cancelled.as_str() => {
            state
                .runtime
                .clear_self_healing_heartbeat(WorkHeartbeatKind::Run, incident.run_id.as_str());
            return Ok(());
        }
        Ok(_) => {}
        Err(error) if error.code() == tonic::Code::DeadlineExceeded => {
            state
                .runtime
                .update_orchestrator_run_state(
                    incident.run_id.clone(),
                    RunLifecycleState::Cancelled,
                    Some("generation-safe stuck-run hard abort".to_owned()),
                )
                .await
                .map_err(|error| format!("stuck-run generation invalidation failed: {error}"))?;
            let _ = cleanup_run_resources(
                &state.runtime,
                incident.run_id.as_str(),
                "generation-safe stuck-run hard abort",
            )
            .await;
        }
        Err(error) => return Err(format!("stuck-run bounded settle failed: {error}")),
    }
    match state
        .runtime
        .complete_stuck_run_remediation(incident, worker_id, claim_epoch)
        .map_err(|error| format!("failed to queue stuck-run continuation: {error}"))?
    {
        StuckRunRemediationCompletionOutcome::ContinuationQueued { .. }
        | StuckRunRemediationCompletionOutcome::AlreadyQueued { .. } => {
            state
                .runtime
                .clear_self_healing_heartbeat(WorkHeartbeatKind::Run, incident.run_id.as_str());
            Ok(())
        }
        StuckRunRemediationCompletionOutcome::StaleClaim => {
            Err("stuck-run remediation claim became stale before completion".to_owned())
        }
    }
}

async fn run_unified_orphan_reaper(state: &AppState) -> Result<(), String> {
    let policy = if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Watchdog)
        == SelfHealingMode::GenerationSafeAutoRecovery
    {
        StuckRunRemediationPolicy::GenerationSafeAutoRecovery
    } else {
        StuckRunRemediationPolicy::ObserveOnly
    };
    let now = current_unix_ms();
    let stale_runs = state
        .runtime
        .self_healing_heartbeats()
        .iter()
        .filter(|heartbeat| {
            heartbeat.kind == WorkHeartbeatKind::Run
                && now.saturating_sub(heartbeat.updated_at_unix_ms) > RUN_HEARTBEAT_STUCK_AFTER_MS
        })
        .count();
    let mut entries = vec![OrphanReconciliationEntry {
        resource_type: OrphanResourceType::Run,
        inspected_count: stale_runs,
        reconciled_count: 0,
        quarantined_count: 0,
        reason_code: "runtime.orphan.run_watchdog_adapter".to_owned(),
    }];
    if policy == StuckRunRemediationPolicy::ObserveOnly {
        for resource_type in [
            OrphanResourceType::Process,
            OrphanResourceType::Mcp,
            OrphanResourceType::Lsp,
            OrphanResourceType::Acp,
            OrphanResourceType::Pty,
            OrphanResourceType::Worker,
        ] {
            entries.push(OrphanReconciliationEntry {
                resource_type,
                inspected_count: 0,
                reconciled_count: 0,
                quarantined_count: 0,
                reason_code: "runtime.orphan.observe_only".to_owned(),
            });
        }
    } else {
        let process = state
            .runtime
            .reconcile_persisted_process_leases_async()
            .await
            .map_err(|error| format!("process orphan reconciliation failed: {error}"))?;
        entries.extend(process_orphan_entries(&process));
        let workers = state
            .runtime
            .reap_expired_networked_workers()
            .await
            .map_err(|error| format!("worker orphan reconciliation failed: {error}"))?;
        entries.push(OrphanReconciliationEntry {
            resource_type: OrphanResourceType::Worker,
            inspected_count: workers.len(),
            reconciled_count: workers.len(),
            quarantined_count: 0,
            reason_code: "runtime.orphan.worker_expiry_reconciled".to_owned(),
        });
    }
    let report = OrphanReconciliationReport {
        report_id: stable_sha256_id("orphan-reconciliation", now.to_string().as_str()),
        policy,
        entries,
        bounded: true,
        completed_at_unix_ms: now,
        schema_version: 1,
    };
    state.runtime.self_healing.record_orphan_reconciliation(report);
    Ok(())
}

fn process_orphan_entries(
    process: &ProcessLeaseReconciliationReport,
) -> Vec<OrphanReconciliationEntry> {
    let reconciled = process
        .closed_count
        .saturating_add(process.expired_count)
        .saturating_add(process.pending_cleanup_completed_count);
    let delegated = [
        OrphanResourceType::Mcp,
        OrphanResourceType::Lsp,
        OrphanResourceType::Acp,
        OrphanResourceType::Pty,
    ]
    .into_iter()
    .map(|resource_type| OrphanReconciliationEntry {
        resource_type,
        inspected_count: process.inspected_count,
        reconciled_count: reconciled,
        quarantined_count: process.quarantined_count,
        reason_code: "runtime.orphan.delegated_to_process_lease".to_owned(),
    });
    std::iter::once(OrphanReconciliationEntry {
        resource_type: OrphanResourceType::Process,
        inspected_count: process
            .inspected_count
            .saturating_add(process.pending_cleanup_inspected_count),
        reconciled_count: reconciled,
        quarantined_count: process
            .quarantined_count
            .saturating_add(process.orphaned_count)
            .saturating_add(process.pending_cleanup_count),
        reason_code: "runtime.orphan.process_lease_reconciled".to_owned(),
    })
    .chain(delegated)
    .collect()
}

async fn evaluate_pending_approvals(state: &AppState) -> Result<(), String> {
    if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Approval)
        == SelfHealingMode::Disabled
    {
        return Ok(());
    }
    let now = current_unix_ms();
    let (approvals, _) = state
        .runtime
        .list_approval_records(None, Some(128), None, None, None, None, None, None)
        .await
        .map_err(|error| format!("failed to list approval records: {error}"))?;
    for approval in approvals {
        let dedupe_key = format!("approval:{}", approval.approval_id);
        if approval.decision.is_some() {
            state.runtime.resolve_self_healing_incident(
                IncidentDomain::Approval,
                dedupe_key.as_str(),
                "approval no longer pending",
            );
            continue;
        }
        let age_ms = now.saturating_sub(approval.updated_at_unix_ms);
        if age_ms <= APPROVAL_STUCK_AFTER_MS {
            state.runtime.resolve_self_healing_incident(
                IncidentDomain::Approval,
                dedupe_key.as_str(),
                "pending approval is within allowed review window",
            );
            continue;
        }
        let _ = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Approval,
            severity: IncidentSeverity::Medium,
            summary: format!("approval {} is waiting too long", approval.approval_id),
            detail: format!(
                "Approval '{}' ({:?}) has been pending for {} ms for principal '{}'.",
                approval.approval_id, approval.subject_type, age_ms, approval.principal
            ),
            dedupe_key,
            remediation: Some(RuntimeRemediationDescriptor {
                remediation_id: "approval_review".to_owned(),
                label: "Review or deny approval".to_owned(),
                description: "A human operator should review or deny the stale approval."
                    .to_owned(),
                risk_level: RemediationRiskLevel::Low,
                blast_radius: RemediationBlastRadius::Session,
                requires_approval: false,
                auto_executable: false,
            }),
        });
    }
    Ok(())
}

async fn evaluate_browser_runtime(state: &AppState) -> Result<(), String> {
    if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Browser)
        == SelfHealingMode::Disabled
    {
        return Ok(());
    }
    if !state.browser_service_config.enabled {
        state.runtime.resolve_self_healing_incident(
            IncidentDomain::Browser,
            "browser_service_health",
            "browser service is disabled",
        );
        return Ok(());
    }

    let mut client = match build_console_browser_client(state).await {
        Ok(client) => client,
        Err(response) => {
            let detail = format!("browser service connect failed with http {}", response.status());
            let _ = state.runtime.observe_self_healing_incident(
                build_browser_service_health_incident("browser service connect failed", detail),
            );
            return Ok(());
        }
    };
    let mut health_request = TonicRequest::new(browser_v1::BrowserHealthRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
    });
    if let Err(response) = apply_browser_service_auth(state, health_request.metadata_mut()) {
        let detail = format!("browser service auth failed with http {}", response.status());
        let _ = state.runtime.observe_self_healing_incident(build_browser_service_health_incident(
            "browser service auth failed",
            detail,
        ));
        return Ok(());
    }
    match client.health(health_request).await {
        Ok(_) => state.runtime.resolve_self_healing_incident(
            IncidentDomain::Browser,
            "browser_service_health",
            "browser service health probe recovered",
        ),
        Err(error) => {
            let _ =
                state.runtime.observe_self_healing_incident(build_browser_service_health_incident(
                    "browser service health probe failed",
                    error.to_string(),
                ));
            return Ok(());
        }
    }

    prune_expired_relay_tokens(state);
    heal_missing_active_profiles(state).await?;
    Ok(())
}

fn prune_expired_relay_tokens(state: &AppState) {
    let now = current_unix_ms();
    let expired = {
        let mut tokens = state.relay_tokens.lock().expect("relay token mutex poisoned");
        let expired = tokens
            .values()
            .filter(|record| record.expires_at_unix_ms <= now)
            .map(|record| record.token_hash_sha256.clone())
            .collect::<Vec<_>>();
        tokens.retain(|_, record| record.expires_at_unix_ms > now);
        expired
    };
    if expired.is_empty() {
        state.runtime.resolve_self_healing_incident(
            IncidentDomain::Browser,
            "expired_relay_tokens",
            "no expired relay tokens remain",
        );
        return;
    }

    // The incident is opened, marked remediated, and resolved in one pass on purpose: pruning is
    // immediate, but the observe/attempt/resolve sequence leaves a complete audit trail.
    let incident = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
        domain: IncidentDomain::Browser,
        severity: IncidentSeverity::Low,
        summary: "expired browser relay tokens were pruned".to_owned(),
        detail: format!("Pruned {} expired relay token(s).", expired.len()),
        dedupe_key: "expired_relay_tokens".to_owned(),
        remediation: Some(RuntimeRemediationDescriptor {
            remediation_id: "prune_relay_tokens".to_owned(),
            label: "Prune expired relay tokens".to_owned(),
            description: "Remove stale console relay tokens that can no longer be used.".to_owned(),
            risk_level: RemediationRiskLevel::Low,
            blast_radius: RemediationBlastRadius::Session,
            requires_approval: false,
            auto_executable: true,
        }),
    });
    let _ = state.runtime.record_self_healing_remediation_attempt(
        incident.incident_id.as_str(),
        "prune_relay_tokens",
        SelfHealingFeature::Browser,
        RemediationAttemptStatus::Succeeded,
        format!("pruned {} expired relay token(s)", expired.len()),
    );
    state.runtime.resolve_self_healing_incident(
        IncidentDomain::Browser,
        "expired_relay_tokens",
        "expired relay tokens were pruned successfully",
    );
}

async fn heal_missing_active_profiles(state: &AppState) -> Result<(), String> {
    let principals = collect_browser_principals(state);
    if principals.is_empty() {
        return Ok(());
    }
    for principal in principals {
        let mut client = build_console_browser_client(state).await.map_err(|response| {
            format!("browser profile probe connect failed with http {}", response.status())
        })?;
        let mut request = TonicRequest::new(browser_v1::ListProfilesRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            principal: principal.clone(),
        });
        apply_browser_service_auth(state, request.metadata_mut()).map_err(|response| {
            format!("browser profile probe auth failed with http {}", response.status())
        })?;
        let dedupe_key = format!("browser_active_profile:{principal}");
        let response = match client.list_profiles(request).await {
            Ok(response) => response.into_inner(),
            Err(error) if browser_profiles_intentionally_unavailable(error.message()) => {
                state.runtime.resolve_self_healing_incident(
                    IncidentDomain::Browser,
                    dedupe_key.as_str(),
                    "browser profile self-healing skipped because profile persistence is unavailable",
                );
                continue;
            }
            Err(error) => return Err(format!("browser profile list failed: {error}")),
        };
        if response.active_profile_id.is_some() || response.profiles.is_empty() {
            state.runtime.resolve_self_healing_incident(
                IncidentDomain::Browser,
                dedupe_key.as_str(),
                "browser principal has a valid active profile",
            );
            continue;
        }

        let Some(candidate_profile_id) = response
            .profiles
            .first()
            .and_then(|profile| profile.profile_id.as_ref())
            .map(|value| value.ulid.clone())
        else {
            continue;
        };
        let incident = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Browser,
            severity: IncidentSeverity::Medium,
            summary: format!("browser principal '{principal}' is missing an active profile"),
            detail: format!(
                "No active profile was set for principal '{}'; proposing '{}'.",
                principal, candidate_profile_id
            ),
            dedupe_key: dedupe_key.clone(),
            remediation: Some(RuntimeRemediationDescriptor {
                remediation_id: "restore_active_profile".to_owned(),
                label: "Restore active browser profile".to_owned(),
                description: "Re-point the principal to a valid existing browser profile."
                    .to_owned(),
                risk_level: RemediationRiskLevel::Low,
                blast_radius: RemediationBlastRadius::Session,
                requires_approval: false,
                auto_executable: true,
            }),
        });

        if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Browser)
            != SelfHealingMode::GenerationSafeAutoRecovery
        {
            let _ = state.runtime.record_self_healing_remediation_attempt(
                incident.incident_id.as_str(),
                "restore_active_profile",
                SelfHealingFeature::Browser,
                RemediationAttemptStatus::Skipped,
                "browser feature is not in auto mode",
            );
            continue;
        }

        let mut set_request = TonicRequest::new(browser_v1::SetActiveProfileRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            principal: principal.clone(),
            profile_id: Some(common_v1::CanonicalId { ulid: candidate_profile_id.clone() }),
        });
        apply_browser_service_auth(state, set_request.metadata_mut()).map_err(|response| {
            format!("browser active profile auth failed with http {}", response.status())
        })?;
        match client.set_active_profile(set_request).await {
            Ok(_) => {
                let _ = state.runtime.record_self_healing_remediation_attempt(
                    incident.incident_id.as_str(),
                    "restore_active_profile",
                    SelfHealingFeature::Browser,
                    RemediationAttemptStatus::Succeeded,
                    format!("set active profile '{}' for '{}'", candidate_profile_id, principal),
                );
                state.runtime.resolve_self_healing_incident(
                    IncidentDomain::Browser,
                    dedupe_key.as_str(),
                    "browser active profile restored",
                );
            }
            Err(error) => {
                let _ = state.runtime.record_self_healing_remediation_attempt(
                    incident.incident_id.as_str(),
                    "restore_active_profile",
                    SelfHealingFeature::Browser,
                    RemediationAttemptStatus::Failed,
                    error.to_string(),
                );
            }
        }
    }
    Ok(())
}

// Matches the browserd error for profile persistence that is switched off by configuration
// (missing state encryption key). That setup is deliberate, so it must not raise incidents.
fn browser_profiles_intentionally_unavailable(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("browser profiles require")
        && normalized.contains("palyra_browserd_state_encryption_key")
}

fn build_browser_service_health_incident(
    summary: impl Into<String>,
    detail: impl Into<String>,
) -> RuntimeIncidentObservation {
    RuntimeIncidentObservation {
        domain: IncidentDomain::Browser,
        severity: IncidentSeverity::High,
        summary: summary.into(),
        detail: detail.into(),
        dedupe_key: "browser_service_health".to_owned(),
        remediation: Some(RuntimeRemediationDescriptor {
            remediation_id: "browser_service_probe".to_owned(),
            label: "Inspect browser daemon".to_owned(),
            description: "Verify browserd is reachable and restart it if operator confirms."
                .to_owned(),
            risk_level: RemediationRiskLevel::Medium,
            blast_radius: RemediationBlastRadius::Global,
            requires_approval: true,
            auto_executable: false,
        }),
    }
}

fn format_self_healing_cycle_result(errors: Vec<(&'static str, String)>) -> Result<(), String> {
    if errors.is_empty() {
        return Ok(());
    }
    let summary = errors
        .into_iter()
        .map(|(feature, error)| format!("{feature}: {error}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(format!("self-healing evaluators failed: {summary}"))
}

async fn evaluate_skill_runtime(state: &AppState) -> Result<(), String> {
    if state.runtime.self_healing.mode_for_feature(SelfHealingFeature::Artifact)
        == SelfHealingMode::Disabled
    {
        return Ok(());
    }
    let skills_root = resolve_skills_root()
        .map_err(|response| format!("skills root unavailable: http {}", response.status()))?;
    let index = load_installed_skills_index(skills_root.as_path()).map_err(|response| {
        format!("failed to load installed skills index: http {}", response.status())
    })?;
    for entry in index.entries {
        let artifact_path = managed_skill_artifact_path(
            skills_root.as_path(),
            entry.skill_id.as_str(),
            entry.version.as_str(),
        );
        let artifact_dedupe_key = format!("skill_artifact:{}@{}", entry.skill_id, entry.version);
        if fs::metadata(artifact_path.as_path()).is_ok() {
            state.runtime.resolve_self_healing_incident(
                IncidentDomain::Artifact,
                artifact_dedupe_key.as_str(),
                "managed skill artifact exists",
            );
        } else {
            let _ = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
                domain: IncidentDomain::Artifact,
                severity: IncidentSeverity::High,
                summary: format!("skill artifact missing for {}@{}", entry.skill_id, entry.version),
                detail: format!(
                    "Expected managed skill artifact at '{}'.",
                    artifact_path.display()
                ),
                dedupe_key: artifact_dedupe_key,
                remediation: Some(RuntimeRemediationDescriptor {
                    remediation_id: "reinstall_skill_artifact".to_owned(),
                    label: "Reinstall signed skill artifact".to_owned(),
                    description: "Restore the managed skill artifact from a verified source."
                        .to_owned(),
                    risk_level: RemediationRiskLevel::Medium,
                    blast_radius: RemediationBlastRadius::Workspace,
                    requires_approval: true,
                    auto_executable: false,
                }),
            });
        }

        let status = state
            .runtime
            .latest_skill_status(entry.skill_id.clone())
            .await
            .map_err(|error| format!("failed to load latest skill status: {error}"))?;
        let status_dedupe_key = format!("skill_status:{}@{}", entry.skill_id, entry.version);
        match status.as_ref().map(|record| record.status) {
            Some(SkillExecutionStatus::Quarantined) | Some(SkillExecutionStatus::Disabled) => {
                let record = status.expect("match arm guarantees skill status is Some");
                let _ = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
                    domain: IncidentDomain::Artifact,
                    severity: IncidentSeverity::Medium,
                    summary: format!("skill {} is {}", record.skill_id, record.status.as_str()),
                    detail: record.reason.unwrap_or_else(|| {
                        "skill runtime was removed from active execution".to_owned()
                    }),
                    dedupe_key: status_dedupe_key,
                    remediation: Some(RuntimeRemediationDescriptor {
                        remediation_id: "audit_and_reenable_skill".to_owned(),
                        label: "Audit and re-enable skill".to_owned(),
                        description:
                            "Re-audit the skill artifact before returning it to active use."
                                .to_owned(),
                        risk_level: RemediationRiskLevel::Medium,
                        blast_radius: RemediationBlastRadius::Workspace,
                        requires_approval: true,
                        auto_executable: false,
                    }),
                });
            }
            _ => {
                state.runtime.resolve_self_healing_incident(
                    IncidentDomain::Artifact,
                    status_dedupe_key.as_str(),
                    "skill runtime is active",
                );
            }
        }
    }
    Ok(())
}

// Degrades to 0 when the clock reports a pre-epoch time instead of failing: a wrong timestamp
// only skews incident ages, while an error here would take down the healing loop.
fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn incident_index_key(domain: IncidentDomain, dedupe_key: &str) -> String {
    format!("{domain:?}:{dedupe_key}")
}

// Derives ids from content rather than randomness so the same incident identity hashes to the
// same id across daemon restarts (the in-memory store itself does not survive a restart).
fn stable_sha256_id(prefix: &str, payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b":");
    hasher.update(payload.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("{prefix}_{}", &digest[..16])
}

fn short_hash(payload: &str) -> String {
    hex::encode(Sha256::digest(payload.as_bytes()))[..12].to_owned()
}

fn heartbeat_key(kind: WorkHeartbeatKind, object_id: &str) -> String {
    format!("{kind:?}:{object_id}")
}

fn heartbeat_dedupe_key(heartbeat: &WorkHeartbeatRecord) -> String {
    format!("heartbeat:{:?}:{}", heartbeat.kind, heartbeat.object_id)
}

fn build_run_watchdog_remediation() -> RuntimeRemediationDescriptor {
    RuntimeRemediationDescriptor {
        remediation_id: "generation_safe_stuck_run_recovery".to_owned(),
        label: "Recover safe stuck run".to_owned(),
        description:
            "Generation-fence a stale read-only wait, cancel it, and queue one continuation."
                .to_owned(),
        risk_level: RemediationRiskLevel::Low,
        blast_radius: RemediationBlastRadius::Session,
        requires_approval: false,
        auto_executable: true,
    }
}

fn build_background_task_watchdog_remediation() -> RuntimeRemediationDescriptor {
    RuntimeRemediationDescriptor {
        remediation_id: "inspect_or_requeue_background_task".to_owned(),
        label: "Inspect stuck background task".to_owned(),
        description:
            "Review the task state and requeue or cancel it only after operator confirmation."
                .to_owned(),
        risk_level: RemediationRiskLevel::Medium,
        blast_radius: RemediationBlastRadius::Session,
        requires_approval: true,
        auto_executable: false,
    }
}

fn collect_browser_principals(state: &AppState) -> Vec<String> {
    let mut principals = state
        .console_sessions
        .lock()
        .expect("console session mutex poisoned")
        .values()
        .map(|session| session.context.principal.clone())
        .collect::<Vec<_>>();
    principals.sort();
    principals.dedup();
    principals
}

fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

fn is_terminal_task_state(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "expired")
}

fn build_incident_summary<'a>(
    incidents: impl Iterator<Item = &'a RuntimeIncidentRecord>,
) -> RuntimeIncidentSummary {
    let mut active = 0_usize;
    let mut resolving = 0_usize;
    let mut resolved = 0_usize;
    let mut by_domain = BTreeMap::<String, usize>::new();
    let mut by_severity = BTreeMap::<String, usize>::new();
    for incident in incidents {
        match incident.state {
            IncidentState::Open => active = active.saturating_add(1),
            IncidentState::Remediating => resolving = resolving.saturating_add(1),
            IncidentState::Resolved => resolved = resolved.saturating_add(1),
        }
        *by_domain.entry(format!("{:?}", incident.domain).to_lowercase()).or_default() += 1;
        *by_severity.entry(format!("{:?}", incident.severity).to_lowercase()).or_default() += 1;
    }
    RuntimeIncidentSummary { active, resolving, resolved, by_domain, by_severity }
}

fn push_incident_history(
    history: &mut Vec<RuntimeIncidentHistoryEntry>,
    entry: RuntimeIncidentHistoryEntry,
) {
    history.push(entry);
    truncate_vec(history, INCIDENT_HISTORY_LIMIT);
}

fn truncate_vec<T>(entries: &mut Vec<T>, limit: usize) {
    if entries.len() <= limit {
        return;
    }
    let drop_count = entries.len().saturating_sub(limit);
    entries.drain(0..drop_count);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stuck_run_incident() -> StuckRunIncidentV2 {
        StuckRunIncidentV2 {
            incident_id: "incident".to_owned(),
            run_id: "run".to_owned(),
            session_id: "session".to_owned(),
            generation: 7,
            generation_lease_id: "lease".to_owned(),
            generation_lease_expires_at_unix_ms: i64::MAX,
            lane_owner: "orchestrator_run".to_owned(),
            heartbeat_generation: Some(7),
            heartbeat_updated_at_unix_ms: 1_000,
            provider_wait_in_flight: false,
            read_only_tool_wait: true,
            mutating_tool_in_flight: false,
            pending_approval: false,
            requeue_idempotency_key: "requeue".to_owned(),
            continuation_task_id: "task".to_owned(),
            continuation_run_id: "continuation".to_owned(),
            created_at_unix_ms: 2_000,
            schema_version: 2,
        }
    }

    fn stuck_run_heartbeat(updated_at_unix_ms: i64) -> WorkHeartbeatRecord {
        WorkHeartbeatRecord {
            heartbeat_key: "Run:run".to_owned(),
            kind: WorkHeartbeatKind::Run,
            object_id: "run".to_owned(),
            execution_generation: Some(7),
            summary: "run".to_owned(),
            updated_at_unix_ms,
        }
    }

    #[test]
    fn stale_read_only_wait_is_auto_recoverable_only_under_explicit_policy() {
        let mut incident = stuck_run_incident();
        incident.read_only_tool_wait = false;
        incident.provider_wait_in_flight = true;
        let heartbeat = stuck_run_heartbeat(incident.heartbeat_updated_at_unix_ms);

        assert_eq!(
            decide_stuck_run_remediation(
                &incident,
                Some(&heartbeat),
                StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
                2_000,
            ),
            StuckRunRemediationDecisionKind::AutoRecover
        );
        assert_eq!(
            decide_stuck_run_remediation(
                &incident,
                Some(&heartbeat),
                StuckRunRemediationPolicy::ObserveOnly,
                2_000,
            ),
            StuckRunRemediationDecisionKind::ObserveOnly
        );
    }

    #[test]
    fn fresh_heartbeat_after_detection_blocks_remediation() {
        let incident = stuck_run_incident();
        let heartbeat =
            stuck_run_heartbeat(incident.heartbeat_updated_at_unix_ms.saturating_add(1));

        assert_eq!(
            decide_stuck_run_remediation(
                &incident,
                Some(&heartbeat),
                StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
                2_000,
            ),
            StuckRunRemediationDecisionKind::FreshHeartbeat
        );
    }

    #[test]
    fn active_mutation_blocks_automatic_replay() {
        let mut incident = stuck_run_incident();
        incident.mutating_tool_in_flight = true;
        incident.read_only_tool_wait = false;
        let heartbeat = stuck_run_heartbeat(incident.heartbeat_updated_at_unix_ms);

        assert_eq!(
            decide_stuck_run_remediation(
                &incident,
                Some(&heartbeat),
                StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
                2_000,
            ),
            StuckRunRemediationDecisionKind::ActiveMutationBlocked
        );
    }

    #[test]
    fn expired_generation_lease_blocks_automatic_recovery() {
        let mut incident = stuck_run_incident();
        incident.generation_lease_expires_at_unix_ms = 1_999;
        let heartbeat = stuck_run_heartbeat(incident.heartbeat_updated_at_unix_ms);

        assert_eq!(
            decide_stuck_run_remediation(
                &incident,
                Some(&heartbeat),
                StuckRunRemediationPolicy::GenerationSafeAutoRecovery,
                2_000,
            ),
            StuckRunRemediationDecisionKind::ExpiredGenerationLease
        );
    }

    #[test]
    fn remediation_rate_limit_and_circuit_breaker_are_bounded() {
        let state = SelfHealingState::new();
        for index in 0..STUCK_RUN_REMEDIATION_LIMIT {
            state
                .acquire_stuck_run_remediation_permit(index as i64)
                .expect("bounded attempts should be permitted");
        }
        assert_eq!(
            state.acquire_stuck_run_remediation_permit(10),
            Err(StuckRunRemediationDecisionKind::RateLimited)
        );

        for _ in 0..STUCK_RUN_CIRCUIT_FAILURE_THRESHOLD {
            state.record_stuck_run_remediation_failure(100);
        }
        assert_eq!(
            state.acquire_stuck_run_remediation_permit(101),
            Err(StuckRunRemediationDecisionKind::CircuitOpen)
        );
    }

    #[test]
    fn unified_process_adapter_registers_every_process_backed_resource_type() {
        let entries = process_orphan_entries(&ProcessLeaseReconciliationReport {
            inspected_count: 1,
            closed_count: 1,
            orphaned_count: 0,
            quarantined_count: 0,
            expired_count: 0,
            pending_cleanup_inspected_count: 0,
            pending_cleanup_completed_count: 0,
            pending_cleanup_count: 0,
        });

        assert_eq!(
            entries.iter().map(|entry| entry.resource_type).collect::<Vec<_>>(),
            vec![
                OrphanResourceType::Process,
                OrphanResourceType::Mcp,
                OrphanResourceType::Lsp,
                OrphanResourceType::Acp,
                OrphanResourceType::Pty,
            ]
        );
    }

    #[test]
    fn incident_lifecycle_updates_summary() {
        let state = SelfHealingState::new();
        let incident = state.observe_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Watchdog,
            severity: IncidentSeverity::High,
            summary: "run appears stuck".to_owned(),
            detail: "test detail".to_owned(),
            dedupe_key: "run:01".to_owned(),
            remediation: Some(build_run_watchdog_remediation()),
        });

        let summary = state.incident_summary();
        assert_eq!(summary.active, 1);
        assert_eq!(summary.resolved, 0);
        assert_eq!(state.active_incidents(8).len(), 1);

        state.resolve_incident(
            IncidentDomain::Watchdog,
            "run:01",
            "run returned to terminal state",
        );
        let summary = state.incident_summary();
        assert_eq!(summary.active, 0);
        assert_eq!(summary.resolved, 1);
        assert!(state.active_incidents(8).is_empty());
        assert_eq!(incident.incident_id, state.recent_incident_history(8)[1].incident_id);
    }

    #[test]
    fn heartbeat_recording_and_clearing_round_trips() {
        let state = SelfHealingState::new();
        state.record_heartbeat(WorkHeartbeatUpdate {
            kind: WorkHeartbeatKind::Run,
            object_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            execution_generation: None,
            summary: "run summary".to_owned(),
        });

        let heartbeats = state.list_heartbeats();
        assert_eq!(heartbeats.len(), 1);
        assert_eq!(heartbeats[0].kind, WorkHeartbeatKind::Run);

        state.clear_heartbeat(WorkHeartbeatKind::Run, "01ARZ3NDEKTSV4RRFFQ69G5FAX");
        assert!(state.list_heartbeats().is_empty());
    }

    #[test]
    fn stale_execution_cannot_clear_newer_background_heartbeat() {
        let state = SelfHealingState::new();
        let task_id = "01ARZ3NDEKTSV4RRFFQ69G5FBT";
        state.record_heartbeat(WorkHeartbeatUpdate {
            kind: WorkHeartbeatKind::BackgroundTask,
            object_id: task_id.to_owned(),
            execution_generation: Some(2),
            summary: "newer background execution".to_owned(),
        });

        state.clear_heartbeat_if_generation(WorkHeartbeatKind::BackgroundTask, task_id, Some(1));
        assert_eq!(state.list_heartbeats().len(), 1);

        state.clear_heartbeat_if_generation(WorkHeartbeatKind::BackgroundTask, task_id, Some(2));
        assert!(state.list_heartbeats().is_empty());
    }

    #[test]
    fn clearing_heartbeat_resolves_matching_watchdog_incident() {
        let state = SelfHealingState::new();
        let run_id = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
        state.record_heartbeat(WorkHeartbeatUpdate {
            kind: WorkHeartbeatKind::Run,
            object_id: run_id.to_owned(),
            execution_generation: None,
            summary: "run summary".to_owned(),
        });
        let heartbeat = state.list_heartbeats().pop().expect("heartbeat should exist");
        let _ = state.observe_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Watchdog,
            severity: IncidentSeverity::High,
            summary: "run appears stuck".to_owned(),
            detail: "test detail".to_owned(),
            dedupe_key: heartbeat_dedupe_key(&heartbeat),
            remediation: Some(build_run_watchdog_remediation()),
        });

        state.clear_heartbeat(WorkHeartbeatKind::Run, run_id);

        assert!(state.list_heartbeats().is_empty());
        assert!(state.active_incidents(8).is_empty());
        assert_eq!(state.incident_summary().resolved, 1);
    }

    #[test]
    fn browser_profile_encryption_key_error_is_non_noisy_for_self_healing() {
        assert!(browser_profiles_intentionally_unavailable(
            "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY to be configured"
        ));
        assert!(!browser_profiles_intentionally_unavailable(
            "browser profile list failed: transport error"
        ));
    }

    #[test]
    fn browser_service_health_incident_uses_stable_dedupe_and_remediation() {
        let incident = build_browser_service_health_incident(
            "browser service connect failed",
            "browser service connect failed with http 503",
        );

        assert_eq!(incident.domain, IncidentDomain::Browser);
        assert_eq!(incident.severity, IncidentSeverity::High);
        assert_eq!(incident.dedupe_key, "browser_service_health");
        assert_eq!(incident.summary, "browser service connect failed");
        assert_eq!(incident.detail, "browser service connect failed with http 503");
        let remediation = incident.remediation.expect("browser incident should have remediation");
        assert_eq!(remediation.remediation_id, "browser_service_probe");
        assert!(remediation.requires_approval);
    }

    #[test]
    fn self_healing_cycle_result_reports_all_evaluator_errors() {
        let error = format_self_healing_cycle_result(vec![
            ("watchdog", "load failed".to_owned()),
            ("artifact", "index missing".to_owned()),
        ])
        .expect_err("aggregated evaluator errors should fail the cycle");

        assert!(error.contains("watchdog: load failed"));
        assert!(error.contains("artifact: index missing"));
    }

    #[test]
    fn remediation_attempts_are_retained_in_reverse_chronological_order() {
        let state = SelfHealingState::new();
        let incident = state.observe_incident(RuntimeIncidentObservation {
            domain: IncidentDomain::Browser,
            severity: IncidentSeverity::Low,
            summary: "expired relay tokens".to_owned(),
            detail: "expired relay token cleanup".to_owned(),
            dedupe_key: "relay".to_owned(),
            remediation: None,
        });

        let _ = state.record_remediation_attempt(
            incident.incident_id.as_str(),
            "first",
            SelfHealingFeature::Browser,
            RemediationAttemptStatus::Skipped,
            "first detail",
        );
        let _ = state.record_remediation_attempt(
            incident.incident_id.as_str(),
            "second",
            SelfHealingFeature::Browser,
            RemediationAttemptStatus::Succeeded,
            "second detail",
        );

        let attempts = state.recent_remediation_attempts(8);
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].remediation_id, "second");
        assert_eq!(attempts[1].remediation_id, "first");
    }
}
