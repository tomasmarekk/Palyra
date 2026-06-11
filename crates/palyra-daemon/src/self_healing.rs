//! Self-healing incident tracking and the background remediation loop.
//!
//! [`SelfHealingState`] keeps deduplicated runtime incidents, remediation attempts, and work
//! heartbeats in memory. [`spawn_self_healing_loop`] periodically inspects watchdog heartbeats,
//! stale approvals, the browser daemon, and installed skill artifacts. Per-feature modes come
//! from the `PALYRA_HEALING_*` env vars (default observe-only): low-risk fixes may auto-execute,
//! everything else only records a remediation proposal for the operator.

use std::{
    collections::{BTreeMap, HashMap},
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
    app::state::AppState, apply_browser_service_auth, browser_v1, build_console_browser_client,
    common_v1, gateway::GatewayRuntimeState, journal::SkillExecutionStatus,
    load_installed_skills_index, managed_skill_artifact_path, resolve_skills_root,
};

const INCIDENT_HISTORY_LIMIT: usize = 128;
const REMEDIATION_HISTORY_LIMIT: usize = 128;
const HEALING_LOOP_INTERVAL: Duration = Duration::from_secs(15);
// Escalation thresholds: active work whose heartbeat stalls for 2 minutes is treated as stuck,
// while pending approvals get a 10 minute review window because a human is expected in the loop.
const RUN_HEARTBEAT_STUCK_AFTER_MS: i64 = 120_000;
const BACKGROUND_TASK_STUCK_AFTER_MS: i64 = 120_000;
const APPROVAL_STUCK_AFTER_MS: i64 = 600_000;

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
    Auto,
}

impl SelfHealingMode {
    // Unknown or empty env values fall back to the given default instead of failing startup.
    fn from_env_value(value: Option<String>, default: Self) -> Self {
        match value.as_deref().map(str::trim).filter(|candidate| !candidate.is_empty()) {
            Some("disabled") => Self::Disabled,
            Some("observe_only") => Self::ObserveOnly,
            Some("auto") => Self::Auto,
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
    pub summary: String,
    pub updated_at_unix_ms: i64,
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
        let mut inner = self.inner.lock().expect("self-healing mutex poisoned");
        let Some(heartbeat) = inner.heartbeats.remove(heartbeat_key(kind, object_id).as_str())
        else {
            return;
        };
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
        let mut ticker = interval(HEALING_LOOP_INTERVAL);
        // Delay instead of bursting after a stall (for example host suspend); back-to-back
        // healing cycles would only re-observe the same incidents.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = run_self_healing_cycle(&state).await {
                warn!(message = %error, "self-healing cycle failed");
            }
        }
    })
}

// AIDEV-NOTE: the `?` chain means an early evaluator error skips the later ones for that cycle;
// in particular a browser connect failure (see evaluate_browser_runtime) suppresses the skill
// artifact checks until the next tick. Evaluating each area independently would be a behavior
// change, so the coupling is only flagged here.
async fn run_self_healing_cycle(state: &AppState) -> Result<(), String> {
    evaluate_watchdog_runtime(state).await?;
    evaluate_pending_approvals(state).await?;
    evaluate_browser_runtime(state).await?;
    evaluate_skill_runtime(state).await?;
    Ok(())
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
                evaluate_stale_heartbeat(
                    &state.runtime,
                    &heartbeat,
                    now,
                    RUN_HEARTBEAT_STUCK_AFTER_MS,
                    build_run_watchdog_remediation(),
                );
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

    // AIDEV-NOTE: a connect failure here returns Err without opening a Browser incident, so a
    // fully unreachable browserd is only visible as a warn log (and it also skips the skill
    // checks for the cycle, see run_self_healing_cycle). Only an established client whose health
    // RPC fails produces an incident. Changing that is a behavior change; flagged only.
    let mut client = build_console_browser_client(state).await.map_err(|response| {
        format!("browser service connect failed with http {}", response.status())
    })?;
    let mut health_request = TonicRequest::new(browser_v1::BrowserHealthRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
    });
    apply_browser_service_auth(state, health_request.metadata_mut()).map_err(|response| {
        format!("browser service auth failed with http {}", response.status())
    })?;
    match client.health(health_request).await {
        Ok(_) => state.runtime.resolve_self_healing_incident(
            IncidentDomain::Browser,
            "browser_service_health",
            "browser service health probe recovered",
        ),
        Err(error) => {
            let _ = state.runtime.observe_self_healing_incident(RuntimeIncidentObservation {
                domain: IncidentDomain::Browser,
                severity: IncidentSeverity::High,
                summary: "browser service health probe failed".to_owned(),
                detail: error.to_string(),
                dedupe_key: "browser_service_health".to_owned(),
                remediation: Some(RuntimeRemediationDescriptor {
                    remediation_id: "browser_service_probe".to_owned(),
                    label: "Inspect browser daemon".to_owned(),
                    description:
                        "Verify browserd is reachable and restart it if operator confirms."
                            .to_owned(),
                    risk_level: RemediationRiskLevel::Medium,
                    blast_radius: RemediationBlastRadius::Global,
                    requires_approval: true,
                    auto_executable: false,
                }),
            });
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
            != SelfHealingMode::Auto
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

fn heartbeat_key(kind: WorkHeartbeatKind, object_id: &str) -> String {
    format!("{kind:?}:{object_id}")
}

fn heartbeat_dedupe_key(heartbeat: &WorkHeartbeatRecord) -> String {
    format!("heartbeat:{:?}:{}", heartbeat.kind, heartbeat.object_id)
}

fn build_run_watchdog_remediation() -> RuntimeRemediationDescriptor {
    RuntimeRemediationDescriptor {
        remediation_id: "inspect_or_cancel_run".to_owned(),
        label: "Inspect stuck run".to_owned(),
        description: "Review the stalled run and cancel it only after operator confirmation."
            .to_owned(),
        risk_level: RemediationRiskLevel::Medium,
        blast_radius: RemediationBlastRadius::Session,
        requires_approval: true,
        auto_executable: false,
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
            summary: "run summary".to_owned(),
        });

        let heartbeats = state.list_heartbeats();
        assert_eq!(heartbeats.len(), 1);
        assert_eq!(heartbeats[0].kind, WorkHeartbeatKind::Run);

        state.clear_heartbeat(WorkHeartbeatKind::Run, "01ARZ3NDEKTSV4RRFFQ69G5FAX");
        assert!(state.list_heartbeats().is_empty());
    }

    #[test]
    fn clearing_heartbeat_resolves_matching_watchdog_incident() {
        let state = SelfHealingState::new();
        let run_id = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
        state.record_heartbeat(WorkHeartbeatUpdate {
            kind: WorkHeartbeatKind::Run,
            object_id: run_id.to_owned(),
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
