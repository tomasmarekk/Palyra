//! In-memory registries for tool-runtime processes and background tasks.
//!
//! Pure bookkeeping (no process I/O): callers register work with a
//! cancellation handle and a [`CleanupPolicy`], then drive state transitions
//! and read [`RuntimeProcessDiagnostic`] snapshots. Shutdown escalates from
//! graceful cancellation to hard kill based on elapsed time versus each
//! record's policy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Schema version for execution-environment health snapshots.
pub(crate) const EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION: u32 = 1;
/// Audit event name for a tracked execution environment starting work.
pub(crate) const EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT: &str =
    "execution_environment_health_a_long_command_heartbeat.started";
/// Audit event name for a tracked execution environment completing work.
pub(crate) const EXECUTION_ENVIRONMENT_HEALTH_COMPLETED_EVENT: &str =
    "execution_environment_health_a_long_command_heartbeat.completed";
/// Audit event name for a tracked execution environment failing work.
pub(crate) const EXECUTION_ENVIRONMENT_HEALTH_FAILED_EVENT: &str =
    "execution_environment_health_a_long_command_heartbeat.failed";
/// Metadata-only redaction posture for execution environment health payloads.
pub(crate) const EXECUTION_ENVIRONMENT_HEALTH_REDACTION_LEVEL: &str = "metadata_only";
/// Default age after which a live process is treated as a long-running command.
pub(crate) const DEFAULT_LONG_COMMAND_AFTER_MS: i64 = 30_000;
/// Default heartbeat age after which a live process is treated as stale.
pub(crate) const DEFAULT_HEARTBEAT_STALE_AFTER_MS: i64 = 15_000;
/// Stable reason code for a fresh heartbeat on ordinary live work.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_HEALTHY: &str =
    "execution_environment.health.healthy";
/// Stable reason code for live work whose total age crosses the long-command threshold.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_LONG_RUNNING: &str =
    "execution_environment.health.long_running";
/// Stable reason code for live work whose heartbeat is older than the stale threshold.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_STALE: &str =
    "execution_environment.health.heartbeat_stale";
/// Stable reason code for live work with no heartbeat evidence.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_MISSING: &str =
    "execution_environment.health.heartbeat_missing";
/// Stable reason code for a process exceeding its total runtime budget.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_TIMEOUT_OVERALL: &str =
    "execution_environment.timeout.overall";
/// Stable reason code for a process producing no output within its idle budget.
pub(crate) const EXECUTION_ENVIRONMENT_REASON_TIMEOUT_NO_OUTPUT: &str =
    "execution_environment.timeout.no_output";

/// Cancellation scope recorded for a process or background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeProcessScope {
    Run,
    Session,
    ToolCall,
    Detached,
}

/// Secret-safe environment posture for one tracked process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct RuntimeProcessEnvPosture {
    pub(crate) profile_id: Option<String>,
    pub(crate) explicit_env_keys: Vec<String>,
    pub(crate) vault_ref_count: usize,
    pub(crate) daemon_env_inherited: bool,
}

/// Bookkeeping record for one tracked tool-runtime process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeProcessRecord {
    pub(crate) process_id: String,
    pub(crate) owner: String,
    pub(crate) run_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) scope: RuntimeProcessScope,
    pub(crate) pid: Option<u32>,
    pub(crate) cwd: Option<String>,
    pub(crate) env_posture: RuntimeProcessEnvPosture,
    pub(crate) purpose: String,
    pub(crate) started_at_unix_ms: i64,
    pub(crate) last_heartbeat_at_unix_ms: Option<i64>,
    pub(crate) last_output_at_unix_ms: Option<i64>,
    pub(crate) cancellation_handle: String,
    pub(crate) cleanup_policy: CleanupPolicy,
    pub(crate) state: RuntimeProcessState,
}

/// Bookkeeping record for one tracked background task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackgroundTaskRecord {
    pub(crate) task_id: String,
    pub(crate) owner: String,
    pub(crate) run_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) scope: RuntimeProcessScope,
    pub(crate) purpose: String,
    pub(crate) started_at_unix_ms: i64,
    pub(crate) last_heartbeat_at_unix_ms: Option<i64>,
    pub(crate) last_output_at_unix_ms: Option<i64>,
    pub(crate) cancellation_handle: String,
    pub(crate) cleanup_policy: CleanupPolicy,
    pub(crate) state: RuntimeProcessState,
}

/// Timing thresholds that drive cancellation escalation for one record.
///
/// Invariant (enforced by [`ProcessRegistry::register`]):
/// `graceful_timeout_ms <= hard_kill_after_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CleanupPolicy {
    pub(crate) graceful_timeout_ms: u64,
    pub(crate) hard_kill_after_ms: u64,
    pub(crate) remove_artifacts_on_cancel: bool,
}

impl CleanupPolicy {
    /// Default escalation policy for tool-program steps (1s graceful, 5s hard
    /// kill, artifacts kept on cancel).
    pub(crate) const fn tool_program_default() -> Self {
        Self {
            graceful_timeout_ms: 1_000,
            hard_kill_after_ms: 5_000,
            remove_artifacts_on_cancel: false,
        }
    }
}

/// Lifecycle state of a tracked process or background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeProcessState {
    Running,
    Cancelling,
    Cancelled,
    Completed,
    HardKilled,
}

impl RuntimeProcessState {
    /// Returns whether the state admits no further transitions.
    pub(crate) const fn is_terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Completed | Self::HardKilled)
    }
}

/// Operator-facing health decision for one live execution environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionEnvironmentHealthDecision {
    Healthy,
    LongRunning,
    StaleHeartbeat,
    Unknown,
}

/// Health snapshot carried by process and background-task diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionEnvironmentHealthLongCommandHeartbeat {
    pub(crate) schema_version: u32,
    pub(crate) decision: ExecutionEnvironmentHealthDecision,
    pub(crate) age_ms: i64,
    pub(crate) heartbeat_age_ms: Option<i64>,
    pub(crate) long_command_after_ms: i64,
    pub(crate) heartbeat_stale_after_ms: i64,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Resource family represented by an execution-environment journal projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionEnvironmentResourceKind {
    ToolProgram,
}

/// Metadata-only journal projection for execution-environment health events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionEnvironmentHealthJournalProjection {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) resource_id: String,
    pub(crate) resource_kind: ExecutionEnvironmentResourceKind,
    pub(crate) decision: ExecutionEnvironmentHealthDecision,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Read-only snapshot of one live (non-terminal) record for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeProcessDiagnostic {
    pub(crate) id: String,
    pub(crate) owner: String,
    pub(crate) run_id: String,
    pub(crate) session_id: Option<String>,
    pub(crate) scope: RuntimeProcessScope,
    pub(crate) pid: Option<u32>,
    pub(crate) cwd: Option<String>,
    pub(crate) env_posture: RuntimeProcessEnvPosture,
    pub(crate) purpose: String,
    pub(crate) state: RuntimeProcessState,
    pub(crate) age_ms: i64,
    pub(crate) output_idle_ms: Option<i64>,
    pub(crate) cleanup_policy: CleanupPolicy,
    pub(crate) health: ExecutionEnvironmentHealthLongCommandHeartbeat,
}

/// Timeout family selected for a live process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeProcessTimeoutKind {
    Overall,
    NoOutput,
}

/// Timeout decision for one process, preserving which budget fired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeProcessTimeoutDecision {
    pub(crate) process_id: String,
    pub(crate) kind: RuntimeProcessTimeoutKind,
    pub(crate) elapsed_ms: i64,
    pub(crate) timeout_ms: i64,
    pub(crate) reason_code: String,
}

/// Per-state counts produced by [`ProcessRegistry::shutdown`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShutdownOutcome {
    pub(crate) graceful_cancelled: usize,
    pub(crate) hard_killed: usize,
    pub(crate) already_terminal: usize,
}

/// Registry of tool-runtime processes keyed by process id.
#[derive(Debug, Default)]
pub(crate) struct ProcessRegistry {
    records: BTreeMap<String, RuntimeProcessRecord>,
}

impl ProcessRegistry {
    /// Registers a new process record.
    ///
    /// # Errors
    /// Returns an error when any identifying field is blank, when the cleanup
    /// policy is inconsistent (graceful timeout exceeds the hard-kill
    /// timeout), or when the process id is already registered.
    pub(crate) fn register(&mut self, record: RuntimeProcessRecord) -> Result<(), String> {
        validate_record_fields(
            record.process_id.as_str(),
            record.owner.as_str(),
            record.run_id.as_str(),
            record.session_id.as_deref(),
            record.purpose.as_str(),
            record.cancellation_handle.as_str(),
        )?;
        if record.cleanup_policy.graceful_timeout_ms > record.cleanup_policy.hard_kill_after_ms {
            return Err(
                "cleanup policy graceful timeout must not exceed hard kill timeout".to_owned()
            );
        }
        if self.records.contains_key(record.process_id.as_str()) {
            return Err(format!("process '{}' is already registered", record.process_id));
        }
        self.records.insert(record.process_id.clone(), record);
        Ok(())
    }

    /// Marks the process as completed.
    ///
    /// # Errors
    /// Returns an error when `process_id` is not registered.
    pub(crate) fn complete(&mut self, process_id: &str) -> Result<(), String> {
        let record = self
            .records
            .get_mut(process_id)
            .ok_or_else(|| format!("process '{process_id}' is not registered"))?;
        record.state = RuntimeProcessState::Completed;
        Ok(())
    }

    /// Records a process heartbeat and returns the current health snapshot.
    ///
    /// # Errors
    /// Returns an error when `process_id` is not registered.
    pub(crate) fn heartbeat(
        &mut self,
        process_id: &str,
        now_unix_ms: i64,
    ) -> Result<ExecutionEnvironmentHealthLongCommandHeartbeat, String> {
        let record = self
            .records
            .get_mut(process_id)
            .ok_or_else(|| format!("process '{process_id}' is not registered"))?;
        record.last_heartbeat_at_unix_ms = Some(now_unix_ms);
        Ok(process_health(record, now_unix_ms))
    }

    /// Records process output activity.
    ///
    /// # Errors
    /// Returns an error when `process_id` is not registered.
    pub(crate) fn output_heartbeat(
        &mut self,
        process_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let record = self
            .records
            .get_mut(process_id)
            .ok_or_else(|| format!("process '{process_id}' is not registered"))?;
        record.last_output_at_unix_ms = Some(now_unix_ms);
        Ok(())
    }

    /// Records a cancellation attempt, escalating the state by how long the
    /// cancellation has already been in flight (`elapsed_ms`).
    ///
    /// # Errors
    /// Returns an error when `process_id` is not registered.
    pub(crate) fn cancel(&mut self, process_id: &str, elapsed_ms: u64) -> Result<(), String> {
        let record = self
            .records
            .get_mut(process_id)
            .ok_or_else(|| format!("process '{process_id}' is not registered"))?;
        record.state = cancellation_state(&record.cleanup_policy, elapsed_ms);
        Ok(())
    }

    /// Cancels every live process owned by `run_id`.
    pub(crate) fn cancel_run(&mut self, run_id: &str, elapsed_ms: u64) -> usize {
        self.cancel_matching(elapsed_ms, |record| record.run_id == run_id)
    }

    /// Cancels every live process matching `run_id` and `scope`.
    pub(crate) fn cancel_scope(
        &mut self,
        run_id: &str,
        scope: RuntimeProcessScope,
        elapsed_ms: u64,
    ) -> usize {
        self.cancel_matching(elapsed_ms, |record| record.run_id == run_id && record.scope == scope)
    }

    /// Returns the timeout decision for a live process, preserving whether the
    /// total runtime budget or no-output budget fired first.
    ///
    /// # Errors
    /// Returns an error when `process_id` is not registered.
    pub(crate) fn timeout_decision(
        &self,
        process_id: &str,
        now_unix_ms: i64,
        overall_timeout_ms: i64,
        no_output_timeout_ms: Option<i64>,
    ) -> Result<Option<RuntimeProcessTimeoutDecision>, String> {
        let record = self
            .records
            .get(process_id)
            .ok_or_else(|| format!("process '{process_id}' is not registered"))?;
        if record.state.is_terminal() {
            return Ok(None);
        }
        let age_ms = non_negative_elapsed_ms(now_unix_ms, record.started_at_unix_ms);
        if let Some(timeout_ms) = no_output_timeout_ms.filter(|timeout| *timeout > 0) {
            let last_output_at = record.last_output_at_unix_ms.unwrap_or(record.started_at_unix_ms);
            let idle_ms = non_negative_elapsed_ms(now_unix_ms, last_output_at);
            if idle_ms >= timeout_ms {
                return Ok(Some(RuntimeProcessTimeoutDecision {
                    process_id: process_id.to_owned(),
                    kind: RuntimeProcessTimeoutKind::NoOutput,
                    elapsed_ms: idle_ms,
                    timeout_ms,
                    reason_code: EXECUTION_ENVIRONMENT_REASON_TIMEOUT_NO_OUTPUT.to_owned(),
                }));
            }
        }
        if overall_timeout_ms > 0 && age_ms >= overall_timeout_ms {
            return Ok(Some(RuntimeProcessTimeoutDecision {
                process_id: process_id.to_owned(),
                kind: RuntimeProcessTimeoutKind::Overall,
                elapsed_ms: age_ms,
                timeout_ms: overall_timeout_ms,
                reason_code: EXECUTION_ENVIRONMENT_REASON_TIMEOUT_OVERALL.to_owned(),
            }));
        }
        Ok(None)
    }

    /// Returns snapshots of all non-terminal processes, aged against
    /// `now_unix_ms`.
    pub(crate) fn diagnostics(&self, now_unix_ms: i64) -> Vec<RuntimeProcessDiagnostic> {
        self.records
            .values()
            .filter(|record| !record.state.is_terminal())
            .map(|record| RuntimeProcessDiagnostic {
                id: record.process_id.clone(),
                owner: record.owner.clone(),
                run_id: record.run_id.clone(),
                session_id: record.session_id.clone(),
                scope: record.scope,
                pid: record.pid,
                cwd: record.cwd.clone(),
                env_posture: record.env_posture.clone(),
                purpose: record.purpose.clone(),
                state: record.state,
                age_ms: non_negative_elapsed_ms(now_unix_ms, record.started_at_unix_ms),
                output_idle_ms: record
                    .last_output_at_unix_ms
                    .map(|last_output_at| non_negative_elapsed_ms(now_unix_ms, last_output_at)),
                cleanup_policy: record.cleanup_policy.clone(),
                health: process_health(record, now_unix_ms),
            })
            .collect()
    }

    /// Transitions every live process toward termination during daemon
    /// shutdown: records whose hard-kill deadline has passed (`elapsed_ms`)
    /// are marked hard-killed, the rest are marked cancelled.
    pub(crate) fn shutdown(&mut self, elapsed_ms: u64) -> ShutdownOutcome {
        let mut outcome =
            ShutdownOutcome { graceful_cancelled: 0, hard_killed: 0, already_terminal: 0 };
        for record in self.records.values_mut() {
            if record.state.is_terminal() {
                outcome.already_terminal += 1;
            } else if elapsed_ms > record.cleanup_policy.hard_kill_after_ms {
                record.state = RuntimeProcessState::HardKilled;
                outcome.hard_killed += 1;
            } else {
                record.state = RuntimeProcessState::Cancelled;
                outcome.graceful_cancelled += 1;
            }
        }
        outcome
    }

    fn cancel_matching(
        &mut self,
        elapsed_ms: u64,
        predicate: impl Fn(&RuntimeProcessRecord) -> bool,
    ) -> usize {
        let mut cancelled = 0;
        for record in self.records.values_mut() {
            if record.state.is_terminal() || !predicate(record) {
                continue;
            }
            record.state = cancellation_state(&record.cleanup_policy, elapsed_ms);
            cancelled += 1;
        }
        cancelled
    }
}

/// Registry of background tasks keyed by task id.
#[derive(Debug, Default)]
pub(crate) struct BackgroundTaskRegistry {
    records: BTreeMap<String, BackgroundTaskRecord>,
}

impl BackgroundTaskRegistry {
    /// Registers a new background task record.
    ///
    /// # Errors
    /// Returns an error when any identifying field is blank or when the task
    /// id is already registered.
    pub(crate) fn register(&mut self, record: BackgroundTaskRecord) -> Result<(), String> {
        validate_record_fields(
            record.task_id.as_str(),
            record.owner.as_str(),
            record.run_id.as_str(),
            record.session_id.as_deref(),
            record.purpose.as_str(),
            record.cancellation_handle.as_str(),
        )?;
        if self.records.contains_key(record.task_id.as_str()) {
            return Err(format!("background task '{}' is already registered", record.task_id));
        }
        self.records.insert(record.task_id.clone(), record);
        Ok(())
    }

    /// Marks the background task as completed.
    ///
    /// # Errors
    /// Returns an error when `task_id` is not registered.
    pub(crate) fn complete(&mut self, task_id: &str) -> Result<(), String> {
        let record = self
            .records
            .get_mut(task_id)
            .ok_or_else(|| format!("background task '{task_id}' is not registered"))?;
        record.state = RuntimeProcessState::Completed;
        Ok(())
    }

    /// Records a background task heartbeat and returns the current health snapshot.
    ///
    /// # Errors
    /// Returns an error when `task_id` is not registered.
    pub(crate) fn heartbeat(
        &mut self,
        task_id: &str,
        now_unix_ms: i64,
    ) -> Result<ExecutionEnvironmentHealthLongCommandHeartbeat, String> {
        let record = self
            .records
            .get_mut(task_id)
            .ok_or_else(|| format!("background task '{task_id}' is not registered"))?;
        record.last_heartbeat_at_unix_ms = Some(now_unix_ms);
        Ok(background_task_health(record, now_unix_ms))
    }

    /// Returns snapshots of all non-terminal tasks, aged against
    /// `now_unix_ms`.
    pub(crate) fn diagnostics(&self, now_unix_ms: i64) -> Vec<RuntimeProcessDiagnostic> {
        self.records
            .values()
            .filter(|record| !record.state.is_terminal())
            .map(|record| RuntimeProcessDiagnostic {
                id: record.task_id.clone(),
                owner: record.owner.clone(),
                run_id: record.run_id.clone(),
                session_id: record.session_id.clone(),
                scope: record.scope,
                pid: None,
                cwd: None,
                env_posture: RuntimeProcessEnvPosture::default(),
                purpose: record.purpose.clone(),
                state: record.state,
                age_ms: non_negative_elapsed_ms(now_unix_ms, record.started_at_unix_ms),
                output_idle_ms: record
                    .last_output_at_unix_ms
                    .map(|last_output_at| non_negative_elapsed_ms(now_unix_ms, last_output_at)),
                cleanup_policy: record.cleanup_policy.clone(),
                health: background_task_health(record, now_unix_ms),
            })
            .collect()
    }
}

fn process_health(
    record: &RuntimeProcessRecord,
    now_unix_ms: i64,
) -> ExecutionEnvironmentHealthLongCommandHeartbeat {
    execution_environment_health(
        record.started_at_unix_ms,
        record.last_heartbeat_at_unix_ms,
        now_unix_ms,
    )
}

fn background_task_health(
    record: &BackgroundTaskRecord,
    now_unix_ms: i64,
) -> ExecutionEnvironmentHealthLongCommandHeartbeat {
    execution_environment_health(
        record.started_at_unix_ms,
        record.last_heartbeat_at_unix_ms,
        now_unix_ms,
    )
}

fn execution_environment_health(
    started_at_unix_ms: i64,
    last_heartbeat_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> ExecutionEnvironmentHealthLongCommandHeartbeat {
    let age_ms = non_negative_elapsed_ms(now_unix_ms, started_at_unix_ms);
    let heartbeat_age_ms = last_heartbeat_at_unix_ms
        .map(|heartbeat_at| non_negative_elapsed_ms(now_unix_ms, heartbeat_at));
    let long_running = age_ms >= DEFAULT_LONG_COMMAND_AFTER_MS;
    let heartbeat_stale =
        heartbeat_age_ms.is_some_and(|age| age >= DEFAULT_HEARTBEAT_STALE_AFTER_MS);

    let mut reason_codes = Vec::new();
    let decision = if last_heartbeat_at_unix_ms.is_none() {
        reason_codes.push(EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_MISSING.to_owned());
        ExecutionEnvironmentHealthDecision::Unknown
    } else if heartbeat_stale {
        if long_running {
            reason_codes.push(EXECUTION_ENVIRONMENT_REASON_LONG_RUNNING.to_owned());
        }
        reason_codes.push(EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_STALE.to_owned());
        ExecutionEnvironmentHealthDecision::StaleHeartbeat
    } else if long_running {
        reason_codes.push(EXECUTION_ENVIRONMENT_REASON_LONG_RUNNING.to_owned());
        ExecutionEnvironmentHealthDecision::LongRunning
    } else {
        reason_codes.push(EXECUTION_ENVIRONMENT_REASON_HEALTHY.to_owned());
        ExecutionEnvironmentHealthDecision::Healthy
    };

    ExecutionEnvironmentHealthLongCommandHeartbeat {
        schema_version: EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION,
        decision,
        age_ms,
        heartbeat_age_ms,
        long_command_after_ms: DEFAULT_LONG_COMMAND_AFTER_MS,
        heartbeat_stale_after_ms: DEFAULT_HEARTBEAT_STALE_AFTER_MS,
        reason_codes,
        redaction_level: EXECUTION_ENVIRONMENT_HEALTH_REDACTION_LEVEL.to_owned(),
    }
}

fn non_negative_elapsed_ms(now_unix_ms: i64, then_unix_ms: i64) -> i64 {
    now_unix_ms.saturating_sub(then_unix_ms).max(0)
}

fn validate_record_fields(
    id: &str,
    owner: &str,
    run_id: &str,
    session_id: Option<&str>,
    purpose: &str,
    cancellation_handle: &str,
) -> Result<(), String> {
    if id.trim().is_empty() {
        return Err("runtime registry id must not be empty".to_owned());
    }
    if owner.trim().is_empty() {
        return Err("runtime registry owner must not be empty".to_owned());
    }
    if run_id.trim().is_empty() {
        return Err("runtime registry run id must not be empty".to_owned());
    }
    if session_id.is_some_and(|value| value.trim().is_empty()) {
        return Err("runtime registry session id must not be empty".to_owned());
    }
    if purpose.trim().is_empty() {
        return Err("runtime registry purpose must not be empty".to_owned());
    }
    if cancellation_handle.trim().is_empty() {
        return Err("runtime registry cancellation handle must not be empty".to_owned());
    }
    Ok(())
}

fn cancellation_state(policy: &CleanupPolicy, elapsed_ms: u64) -> RuntimeProcessState {
    if elapsed_ms > policy.hard_kill_after_ms {
        RuntimeProcessState::HardKilled
    } else if elapsed_ms > policy.graceful_timeout_ms {
        RuntimeProcessState::Cancelled
    } else {
        RuntimeProcessState::Cancelling
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundTaskRecord, BackgroundTaskRegistry, CleanupPolicy,
        ExecutionEnvironmentHealthDecision, ExecutionEnvironmentHealthJournalProjection,
        ExecutionEnvironmentResourceKind, ProcessRegistry, RuntimeProcessEnvPosture,
        RuntimeProcessRecord, RuntimeProcessScope, RuntimeProcessState, RuntimeProcessTimeoutKind,
        EXECUTION_ENVIRONMENT_HEALTH_REDACTION_LEVEL, EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION,
        EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT, EXECUTION_ENVIRONMENT_REASON_HEALTHY,
        EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_STALE, EXECUTION_ENVIRONMENT_REASON_LONG_RUNNING,
        EXECUTION_ENVIRONMENT_REASON_TIMEOUT_NO_OUTPUT,
    };

    #[test]
    fn process_registry_tracks_diagnostics_and_shutdown() {
        let mut registry = ProcessRegistry::default();
        registry
            .register(RuntimeProcessRecord {
                process_id: "proc-1".to_owned(),
                owner: "run-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: Some("session-1".to_owned()),
                scope: RuntimeProcessScope::ToolCall,
                pid: Some(4242),
                cwd: Some("/workspace".to_owned()),
                env_posture: RuntimeProcessEnvPosture::default(),
                purpose: "tool-program-step".to_owned(),
                started_at_unix_ms: 1_000,
                last_heartbeat_at_unix_ms: Some(1_000),
                last_output_at_unix_ms: Some(1_000),
                cancellation_handle: "cancel-proc-1".to_owned(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect("process should register");

        let diagnostics = registry.diagnostics(1_250);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].age_ms, 250);
        assert_eq!(diagnostics[0].run_id, "run-1");
        assert_eq!(diagnostics[0].session_id.as_deref(), Some("session-1"));
        assert_eq!(diagnostics[0].scope, RuntimeProcessScope::ToolCall);
        assert_eq!(diagnostics[0].pid, Some(4242));
        assert_eq!(diagnostics[0].cwd.as_deref(), Some("/workspace"));
        assert_eq!(diagnostics[0].output_idle_ms, Some(250));
        assert_eq!(diagnostics[0].health.decision, ExecutionEnvironmentHealthDecision::Healthy);
        assert_eq!(diagnostics[0].health.heartbeat_age_ms, Some(250));
        assert_eq!(
            diagnostics[0].health.reason_codes,
            vec![EXECUTION_ENVIRONMENT_REASON_HEALTHY.to_owned()]
        );

        let shutdown = registry.shutdown(6_000);
        assert_eq!(shutdown.hard_killed, 1);
        assert!(registry.diagnostics(7_000).is_empty());
    }

    #[test]
    fn process_registry_marks_long_running_and_stale_heartbeat() {
        let mut registry = ProcessRegistry::default();
        registry
            .register(RuntimeProcessRecord {
                process_id: "proc-1".to_owned(),
                owner: "run-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: Some("session-1".to_owned()),
                scope: RuntimeProcessScope::ToolCall,
                pid: None,
                cwd: None,
                env_posture: RuntimeProcessEnvPosture::default(),
                purpose: "tool-program-step".to_owned(),
                started_at_unix_ms: 1_000,
                last_heartbeat_at_unix_ms: Some(2_000),
                last_output_at_unix_ms: Some(2_000),
                cancellation_handle: "cancel-proc-1".to_owned(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect("process should register");

        let diagnostics = registry.diagnostics(40_000);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].health.decision,
            ExecutionEnvironmentHealthDecision::StaleHeartbeat
        );
        assert_eq!(
            diagnostics[0].health.reason_codes,
            vec![
                EXECUTION_ENVIRONMENT_REASON_LONG_RUNNING.to_owned(),
                EXECUTION_ENVIRONMENT_REASON_HEARTBEAT_STALE.to_owned()
            ]
        );

        let refreshed = registry
            .heartbeat("proc-1", 39_500)
            .expect("heartbeat should update registered process");
        assert_eq!(refreshed.decision, ExecutionEnvironmentHealthDecision::LongRunning);
        assert_eq!(refreshed.heartbeat_age_ms, Some(0));

        let diagnostics = registry.diagnostics(40_000);
        assert_eq!(diagnostics[0].health.decision, ExecutionEnvironmentHealthDecision::LongRunning);
        assert_eq!(diagnostics[0].health.heartbeat_age_ms, Some(500));
    }

    #[test]
    fn execution_environment_health_snapshot_round_trips_as_json() {
        let mut registry = ProcessRegistry::default();
        registry
            .register(RuntimeProcessRecord {
                process_id: "proc-1".to_owned(),
                owner: "run-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: None,
                scope: RuntimeProcessScope::ToolCall,
                pid: None,
                cwd: None,
                env_posture: RuntimeProcessEnvPosture::default(),
                purpose: "tool-program-step".to_owned(),
                started_at_unix_ms: 1_000,
                last_heartbeat_at_unix_ms: Some(1_000),
                last_output_at_unix_ms: Some(1_000),
                cancellation_handle: "cancel-proc-1".to_owned(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect("process should register");
        let snapshot = registry
            .diagnostics(1_250)
            .into_iter()
            .next()
            .expect("registered process should produce diagnostics")
            .health;

        let encoded = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        let decoded: super::ExecutionEnvironmentHealthLongCommandHeartbeat =
            serde_json::from_str(encoded.as_str()).expect("snapshot should deserialize");

        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.schema_version, EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION);
    }

    #[test]
    fn execution_environment_health_journal_projection_serializes_stable_contract() {
        let projection = ExecutionEnvironmentHealthJournalProjection {
            schema_version: EXECUTION_ENVIRONMENT_HEALTH_SCHEMA_VERSION,
            event_type: EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT.to_owned(),
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            resource_id: "program-1".to_owned(),
            resource_kind: ExecutionEnvironmentResourceKind::ToolProgram,
            decision: ExecutionEnvironmentHealthDecision::Healthy,
            reason_codes: vec![EXECUTION_ENVIRONMENT_REASON_HEALTHY.to_owned()],
            created_at_unix_ms: 1_000,
            evidence_refs: vec!["tool_call:proposal-1".to_owned()],
            redaction_level: EXECUTION_ENVIRONMENT_HEALTH_REDACTION_LEVEL.to_owned(),
        };

        let encoded = serde_json::to_value(&projection).expect("projection should serialize");
        assert_eq!(encoded["event_type"], EXECUTION_ENVIRONMENT_HEALTH_STARTED_EVENT);
        assert_eq!(encoded["resource_kind"], "tool_program");
        assert_eq!(encoded["redaction_level"], EXECUTION_ENVIRONMENT_HEALTH_REDACTION_LEVEL);

        let decoded: ExecutionEnvironmentHealthJournalProjection =
            serde_json::from_value(encoded).expect("projection should deserialize");
        assert_eq!(decoded, projection);
    }

    #[test]
    fn background_task_registry_rejects_missing_cancellation_handle() {
        let mut registry = BackgroundTaskRegistry::default();
        let error = registry
            .register(BackgroundTaskRecord {
                task_id: "task-1".to_owned(),
                owner: "run-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: Some("session-1".to_owned()),
                scope: RuntimeProcessScope::Run,
                purpose: "tool-program".to_owned(),
                started_at_unix_ms: 1_000,
                last_heartbeat_at_unix_ms: Some(1_000),
                last_output_at_unix_ms: Some(1_000),
                cancellation_handle: String::new(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect_err("cancellation handle is required");

        assert!(error.contains("cancellation handle"));
    }

    #[test]
    fn process_registry_cancels_by_run_and_scope() {
        let mut registry = ProcessRegistry::default();
        for (process_id, run_id, scope) in [
            ("proc-1", "run-1", RuntimeProcessScope::ToolCall),
            ("proc-2", "run-1", RuntimeProcessScope::Session),
            ("proc-3", "run-2", RuntimeProcessScope::ToolCall),
        ] {
            registry
                .register(RuntimeProcessRecord {
                    process_id: process_id.to_owned(),
                    owner: run_id.to_owned(),
                    run_id: run_id.to_owned(),
                    session_id: Some(format!("session-{run_id}")),
                    scope,
                    pid: None,
                    cwd: Some("/workspace".to_owned()),
                    env_posture: RuntimeProcessEnvPosture::default(),
                    purpose: "tool-program-step".to_owned(),
                    started_at_unix_ms: 1_000,
                    last_heartbeat_at_unix_ms: Some(1_000),
                    last_output_at_unix_ms: Some(1_000),
                    cancellation_handle: format!("cancel-{process_id}"),
                    cleanup_policy: CleanupPolicy::tool_program_default(),
                    state: RuntimeProcessState::Running,
                })
                .expect("process should register");
        }

        let scoped = registry.cancel_scope("run-1", RuntimeProcessScope::ToolCall, 500);
        assert_eq!(scoped, 1);
        let diagnostics = registry.diagnostics(1_500);
        let by_id = diagnostics
            .iter()
            .map(|diagnostic| (diagnostic.id.as_str(), diagnostic.state))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(by_id.get("proc-1"), Some(&RuntimeProcessState::Cancelling));
        assert_eq!(by_id.get("proc-2"), Some(&RuntimeProcessState::Running));
        assert_eq!(by_id.get("proc-3"), Some(&RuntimeProcessState::Running));

        let run_cancelled = registry.cancel_run("run-1", 6_000);
        assert_eq!(run_cancelled, 2);
        assert!(registry.diagnostics(7_000).iter().all(|diagnostic| diagnostic.run_id != "run-1"));
    }

    #[test]
    fn process_registry_distinguishes_no_output_timeout_from_overall_timeout() {
        let mut registry = ProcessRegistry::default();
        registry
            .register(RuntimeProcessRecord {
                process_id: "proc-1".to_owned(),
                owner: "run-1".to_owned(),
                run_id: "run-1".to_owned(),
                session_id: None,
                scope: RuntimeProcessScope::ToolCall,
                pid: None,
                cwd: None,
                env_posture: RuntimeProcessEnvPosture::default(),
                purpose: "quiet-step".to_owned(),
                started_at_unix_ms: 1_000,
                last_heartbeat_at_unix_ms: Some(1_500),
                last_output_at_unix_ms: Some(1_000),
                cancellation_handle: "cancel-proc-1".to_owned(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect("process should register");

        let no_output = registry
            .timeout_decision("proc-1", 3_100, 10_000, Some(2_000))
            .expect("timeout decision should evaluate")
            .expect("idle timeout should fire");
        assert_eq!(no_output.kind, RuntimeProcessTimeoutKind::NoOutput);
        assert_eq!(no_output.elapsed_ms, 2_100);
        assert_eq!(no_output.reason_code, EXECUTION_ENVIRONMENT_REASON_TIMEOUT_NO_OUTPUT);

        registry
            .output_heartbeat("proc-1", 3_000)
            .expect("output heartbeat should refresh idle timer");
        let overall = registry
            .timeout_decision("proc-1", 11_000, 10_000, Some(9_000))
            .expect("timeout decision should evaluate")
            .expect("overall timeout should fire");
        assert_eq!(overall.kind, RuntimeProcessTimeoutKind::Overall);
        assert_eq!(overall.elapsed_ms, 10_000);
    }
}
