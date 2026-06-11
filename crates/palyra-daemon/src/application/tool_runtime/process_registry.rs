//! In-memory registries for tool-runtime processes and background tasks.
//!
//! Pure bookkeeping (no process I/O): callers register work with a
//! cancellation handle and a [`CleanupPolicy`], then drive state transitions
//! and read [`RuntimeProcessDiagnostic`] snapshots. Shutdown escalates from
//! graceful cancellation to hard kill based on elapsed time versus each
//! record's policy.

use std::collections::BTreeMap;

/// Bookkeeping record for one tracked tool-runtime process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeProcessRecord {
    pub(crate) process_id: String,
    pub(crate) owner: String,
    pub(crate) purpose: String,
    pub(crate) started_at_unix_ms: i64,
    pub(crate) cancellation_handle: String,
    pub(crate) cleanup_policy: CleanupPolicy,
    pub(crate) state: RuntimeProcessState,
}

/// Bookkeeping record for one tracked background task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackgroundTaskRecord {
    pub(crate) task_id: String,
    pub(crate) owner: String,
    pub(crate) purpose: String,
    pub(crate) started_at_unix_ms: i64,
    pub(crate) cancellation_handle: String,
    pub(crate) cleanup_policy: CleanupPolicy,
    pub(crate) state: RuntimeProcessState,
}

/// Timing thresholds that drive cancellation escalation for one record.
///
/// Invariant (enforced by [`ProcessRegistry::register`]):
/// `graceful_timeout_ms <= hard_kill_after_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Read-only snapshot of one live (non-terminal) record for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeProcessDiagnostic {
    pub(crate) id: String,
    pub(crate) owner: String,
    pub(crate) purpose: String,
    pub(crate) state: RuntimeProcessState,
    pub(crate) age_ms: i64,
    pub(crate) cleanup_policy: CleanupPolicy,
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
        record.state = if elapsed_ms > record.cleanup_policy.hard_kill_after_ms {
            RuntimeProcessState::HardKilled
        } else if elapsed_ms > record.cleanup_policy.graceful_timeout_ms {
            RuntimeProcessState::Cancelled
        } else {
            RuntimeProcessState::Cancelling
        };
        Ok(())
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
                purpose: record.purpose.clone(),
                state: record.state,
                age_ms: now_unix_ms.saturating_sub(record.started_at_unix_ms),
                cleanup_policy: record.cleanup_policy.clone(),
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

    /// Returns snapshots of all non-terminal tasks, aged against
    /// `now_unix_ms`.
    pub(crate) fn diagnostics(&self, now_unix_ms: i64) -> Vec<RuntimeProcessDiagnostic> {
        self.records
            .values()
            .filter(|record| !record.state.is_terminal())
            .map(|record| RuntimeProcessDiagnostic {
                id: record.task_id.clone(),
                owner: record.owner.clone(),
                purpose: record.purpose.clone(),
                state: record.state,
                age_ms: now_unix_ms.saturating_sub(record.started_at_unix_ms),
                cleanup_policy: record.cleanup_policy.clone(),
            })
            .collect()
    }
}

fn validate_record_fields(
    id: &str,
    owner: &str,
    purpose: &str,
    cancellation_handle: &str,
) -> Result<(), String> {
    if id.trim().is_empty() {
        return Err("runtime registry id must not be empty".to_owned());
    }
    if owner.trim().is_empty() {
        return Err("runtime registry owner must not be empty".to_owned());
    }
    if purpose.trim().is_empty() {
        return Err("runtime registry purpose must not be empty".to_owned());
    }
    if cancellation_handle.trim().is_empty() {
        return Err("runtime registry cancellation handle must not be empty".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundTaskRecord, BackgroundTaskRegistry, CleanupPolicy, ProcessRegistry,
        RuntimeProcessRecord, RuntimeProcessState,
    };

    #[test]
    fn process_registry_tracks_diagnostics_and_shutdown() {
        let mut registry = ProcessRegistry::default();
        registry
            .register(RuntimeProcessRecord {
                process_id: "proc-1".to_owned(),
                owner: "run-1".to_owned(),
                purpose: "tool-program-step".to_owned(),
                started_at_unix_ms: 1_000,
                cancellation_handle: "cancel-proc-1".to_owned(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect("process should register");

        let diagnostics = registry.diagnostics(1_250);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].age_ms, 250);

        let shutdown = registry.shutdown(6_000);
        assert_eq!(shutdown.hard_killed, 1);
        assert!(registry.diagnostics(7_000).is_empty());
    }

    #[test]
    fn background_task_registry_rejects_missing_cancellation_handle() {
        let mut registry = BackgroundTaskRegistry::default();
        let error = registry
            .register(BackgroundTaskRecord {
                task_id: "task-1".to_owned(),
                owner: "run-1".to_owned(),
                purpose: "tool-program".to_owned(),
                started_at_unix_ms: 1_000,
                cancellation_handle: String::new(),
                cleanup_policy: CleanupPolicy::tool_program_default(),
                state: RuntimeProcessState::Running,
            })
            .expect_err("cancellation handle is required");

        assert!(error.contains("cancellation handle"));
    }
}
