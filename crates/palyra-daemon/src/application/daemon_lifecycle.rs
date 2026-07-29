//! Process-wide lifecycle authority for startup recovery, drain, and shutdown.
//!
//! The controller owns only in-memory coordination. Callers persist a proposed
//! transition before applying it so a process crash cannot publish an
//! operational state that is absent from the journal.

use std::{collections::BTreeMap, sync::Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

/// Ordered process lifecycle phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DaemonLifecyclePhase {
    /// Durable recovery must finish before any ingress is served.
    RecoveryBarrier,
    /// The daemon accepts new work.
    Running,
    /// New work is blocked while active work reaches a safe boundary.
    DrainingAdmission,
    /// Background subsystems are draining in dependency order.
    DrainingSubsystems,
    /// Durable state is being flushed before transports stop.
    Checkpointing,
    /// All servers and background workers must exit.
    ShutdownRequested,
}

impl DaemonLifecyclePhase {
    /// Returns the stable journal and API representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RecoveryBarrier => "recovery_barrier",
            Self::Running => "running",
            Self::DrainingAdmission => "draining_admission",
            Self::DrainingSubsystems => "draining_subsystems",
            Self::Checkpointing => "checkpointing",
            Self::ShutdownRequested => "shutdown_requested",
        }
    }

    /// Returns whether new run authority must not be allocated.
    #[must_use]
    pub(crate) const fn blocks_admission(self) -> bool {
        !matches!(self, Self::Running)
    }

    /// Returns whether background subsystem loops must stop.
    #[must_use]
    pub(crate) const fn stops_subsystems(self) -> bool {
        matches!(self, Self::DrainingSubsystems | Self::Checkpointing | Self::ShutdownRequested)
    }
}

/// Host-owned reason that initiated a drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DaemonDrainTrigger {
    /// Operating-system interrupt signal.
    Sigint,
    /// Operating-system terminate signal.
    Sigterm,
    /// Authenticated administrator request.
    Admin,
    /// A validated configuration change requiring restart.
    ConfigRestart,
}

impl DaemonDrainTrigger {
    /// Returns the stable journal and API representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Sigint => "sigint",
            Self::Sigterm => "sigterm",
            Self::Admin => "admin",
            Self::ConfigRestart => "config_restart",
        }
    }
}

/// Admission behavior while a drain is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DrainAdmissionPolicy {
    /// Reject new work before allocating run authority.
    RejectNew,
    /// Permit only callers that durably queue behind the active boundary.
    DurableQueue,
}

impl DrainAdmissionPolicy {
    /// Returns the stable journal and API representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RejectNew => "reject_new",
            Self::DurableQueue => "durable_queue",
        }
    }
}

/// Background components coordinated by the lifecycle controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleSubsystem {
    /// Run-producing cron scheduler.
    Scheduler,
    /// Hook execution and dispatch.
    Hooks,
    /// Durable background-task dispatcher.
    BackgroundQueue,
    /// Channel ingress and outbox worker.
    Channels,
    /// Self-healing watchdog.
    SelfHealing,
    /// Runtime health reconciliation.
    RuntimeHealth,
    /// Managed coding processes, terminals, language servers, and worktrees.
    ManagedCoding,
    /// Local process-lease reconciliation.
    ProcessLeases,
    /// Networked worker lease expiry.
    NetworkedWorkers,
    /// HTTP, gRPC, node RPC, and QUIC listeners.
    Transports,
}

impl LifecycleSubsystem {
    /// Dependency-safe drain order. Producers stop before their consumers.
    pub(crate) const DRAIN_ORDER: [Self; 10] = [
        Self::Scheduler,
        Self::Hooks,
        Self::BackgroundQueue,
        Self::Channels,
        Self::SelfHealing,
        Self::RuntimeHealth,
        Self::ManagedCoding,
        Self::ProcessLeases,
        Self::NetworkedWorkers,
        Self::Transports,
    ];

    /// Returns the stable journal and API representation.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduler => "scheduler",
            Self::Hooks => "hooks",
            Self::BackgroundQueue => "background_queue",
            Self::Channels => "channels",
            Self::SelfHealing => "self_healing",
            Self::RuntimeHealth => "runtime_health",
            Self::ManagedCoding => "managed_coding",
            Self::ProcessLeases => "process_leases",
            Self::NetworkedWorkers => "networked_workers",
            Self::Transports => "transports",
        }
    }
}

/// Per-subsystem state included in lifecycle diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleSubsystemState {
    /// The subsystem may produce and process work.
    Running,
    /// The subsystem has observed the drain boundary.
    Draining,
    /// The subsystem acknowledged that it no longer owns active work.
    Drained,
    /// The deadline forced cancellation of the subsystem task.
    Aborted,
}

/// One subsystem's latest acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LifecycleSubsystemSnapshot {
    /// Stable subsystem identity.
    pub(crate) subsystem: LifecycleSubsystem,
    /// Current drain state.
    pub(crate) state: LifecycleSubsystemState,
}

/// Immutable process-wide lifecycle observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DaemonLifecycleSnapshot {
    /// Startup/drain generation. It changes once per startup or accepted drain.
    pub(crate) epoch: u64,
    /// Monotonic transition revision within the durable ledger.
    pub(crate) revision: u64,
    /// Current process lifecycle phase.
    pub(crate) phase: DaemonLifecyclePhase,
    /// Trigger for the active drain, if any.
    pub(crate) trigger: Option<DaemonDrainTrigger>,
    /// Stable redacted operational reason.
    pub(crate) reason_code: String,
    /// Authenticated principal or host actor that requested the transition.
    pub(crate) requested_by: String,
    /// Unix timestamp when this lifecycle epoch began.
    pub(crate) requested_at_unix_ms: i64,
    /// Drain deadline. Startup and steady running have no deadline.
    pub(crate) deadline_unix_ms: Option<i64>,
    /// Admission policy applied while the process is not running.
    pub(crate) admission_policy: DrainAdmissionPolicy,
    /// Deterministically ordered subsystem acknowledgements.
    pub(crate) subsystems: Vec<LifecycleSubsystemSnapshot>,
}

impl DaemonLifecycleSnapshot {
    /// Creates the recovery barrier persisted at process startup.
    #[must_use]
    pub(crate) fn recovery_barrier(epoch: u64, revision: u64, now_unix_ms: i64) -> Self {
        Self {
            epoch,
            revision,
            phase: DaemonLifecyclePhase::RecoveryBarrier,
            trigger: None,
            reason_code: "daemon.lifecycle.startup_recovery".to_owned(),
            requested_by: "system:startup".to_owned(),
            requested_at_unix_ms: now_unix_ms,
            deadline_unix_ms: None,
            admission_policy: DrainAdmissionPolicy::RejectNew,
            subsystems: running_subsystems(),
        }
    }
}

/// Validated request to enter drain mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DaemonDrainRequest {
    /// Host-established trigger.
    pub(crate) trigger: DaemonDrainTrigger,
    /// Stable redacted operational reason.
    pub(crate) reason_code: String,
    /// Authenticated principal or host actor.
    pub(crate) requested_by: String,
    /// Absolute drain deadline.
    pub(crate) deadline_unix_ms: i64,
    /// Admission behavior during drain.
    pub(crate) admission_policy: DrainAdmissionPolicy,
}

/// Lifecycle state-machine failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum DaemonLifecycleError {
    /// A synchronization primitive was poisoned.
    #[error("daemon lifecycle state is unavailable")]
    LockPoisoned,
    /// The requested phase edge is not legal from the current state.
    #[error("invalid daemon lifecycle transition from {from} to {to}")]
    InvalidTransition {
        /// Current phase.
        from: &'static str,
        /// Requested phase.
        to: &'static str,
    },
    /// A stale coordinator attempted to mutate a newer lifecycle epoch.
    #[error("stale daemon lifecycle epoch: expected {expected}, actual {actual}")]
    StaleEpoch {
        /// Epoch supplied by the coordinator.
        expected: u64,
        /// Current epoch.
        actual: u64,
    },
}

/// Thread-safe lifecycle state and fan-out notification channel.
pub(crate) struct DaemonLifecycleController {
    state: Mutex<DaemonLifecycleSnapshot>,
    updates: watch::Sender<DaemonLifecycleSnapshot>,
    abort_handles: Mutex<BTreeMap<LifecycleSubsystem, tokio::task::AbortHandle>>,
}

impl DaemonLifecycleController {
    /// Restores the controller from the startup transition already committed to the journal.
    #[must_use]
    pub(crate) fn new(startup: DaemonLifecycleSnapshot) -> Self {
        let (updates, _) = watch::channel(startup.clone());
        Self { state: Mutex::new(startup), updates, abort_handles: Mutex::new(BTreeMap::new()) }
    }

    /// Returns the latest immutable snapshot.
    pub(crate) fn snapshot(&self) -> Result<DaemonLifecycleSnapshot, DaemonLifecycleError> {
        self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned).map(|state| state.clone())
    }

    /// Subscribes a server or worker to process-wide lifecycle changes.
    #[must_use]
    pub(crate) fn subscribe(&self) -> watch::Receiver<DaemonLifecycleSnapshot> {
        self.updates.subscribe()
    }

    /// Registers the task cancellation authority for one background subsystem.
    pub(crate) fn register_subsystem_task(
        &self,
        subsystem: LifecycleSubsystem,
        abort_handle: tokio::task::AbortHandle,
    ) -> Result<(), DaemonLifecycleError> {
        self.abort_handles
            .lock()
            .map_err(|_| DaemonLifecycleError::LockPoisoned)?
            .insert(subsystem, abort_handle);
        Ok(())
    }

    /// Acknowledges that one subsystem crossed its safe drain boundary.
    pub(crate) fn acknowledge_subsystem_drained(
        &self,
        subsystem: LifecycleSubsystem,
    ) -> Result<(), DaemonLifecycleError> {
        let mut current = self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        if let Some(observation) =
            current.subsystems.iter_mut().find(|entry| entry.subsystem == subsystem)
        {
            if observation.state != LifecycleSubsystemState::Aborted {
                observation.state = LifecycleSubsystemState::Drained;
            }
        }
        self.updates.send_replace(current.clone());
        Ok(())
    }

    /// Cancels tasks that did not acknowledge before the drain deadline.
    pub(crate) fn abort_undrained_subsystems(&self) -> Result<(), DaemonLifecycleError> {
        let handles = self.abort_handles.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        let mut current = self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        for observation in &mut current.subsystems {
            if observation.subsystem == LifecycleSubsystem::Transports
                || observation.state == LifecycleSubsystemState::Drained
            {
                continue;
            }
            if let Some(handle) = handles.get(&observation.subsystem) {
                handle.abort();
            }
            observation.state = LifecycleSubsystemState::Aborted;
        }
        self.updates.send_replace(current.clone());
        Ok(())
    }

    /// Cancels one subsystem that does not own a lifecycle-aware loop.
    pub(crate) fn abort_subsystem(
        &self,
        subsystem: LifecycleSubsystem,
    ) -> Result<(), DaemonLifecycleError> {
        let handles = self.abort_handles.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        if let Some(handle) = handles.get(&subsystem) {
            handle.abort();
        }
        let mut current = self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        if let Some(observation) =
            current.subsystems.iter_mut().find(|entry| entry.subsystem == subsystem)
        {
            observation.state = LifecycleSubsystemState::Aborted;
        }
        self.updates.send_replace(current.clone());
        Ok(())
    }

    /// Returns whether every non-transport subsystem acknowledged or was cancelled.
    pub(crate) fn subsystems_settled(&self) -> Result<bool, DaemonLifecycleError> {
        let current = self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        Ok(current.subsystems.iter().all(|observation| {
            observation.subsystem == LifecycleSubsystem::Transports
                || matches!(
                    observation.state,
                    LifecycleSubsystemState::Drained | LifecycleSubsystemState::Aborted
                )
        }))
    }

    /// Builds the next running snapshot without applying it.
    pub(crate) fn propose_startup_ready(
        &self,
    ) -> Result<DaemonLifecycleSnapshot, DaemonLifecycleError> {
        let current = self.snapshot()?;
        if current.phase != DaemonLifecyclePhase::RecoveryBarrier {
            return Err(invalid_transition(current.phase, DaemonLifecyclePhase::Running));
        }
        let mut next = current;
        next.revision = next.revision.saturating_add(1);
        next.phase = DaemonLifecyclePhase::Running;
        next.reason_code = "daemon.lifecycle.running".to_owned();
        next.requested_by = "system:startup".to_owned();
        next.deadline_unix_ms = None;
        next.admission_policy = DrainAdmissionPolicy::RejectNew;
        Ok(next)
    }

    /// Builds the first drain snapshot without applying it.
    pub(crate) fn propose_drain(
        &self,
        request: DaemonDrainRequest,
        now_unix_ms: i64,
    ) -> Result<DaemonLifecycleSnapshot, DaemonLifecycleError> {
        let current = self.snapshot()?;
        if current.phase != DaemonLifecyclePhase::Running {
            return Ok(current);
        }
        let mut next = current;
        next.epoch = next.epoch.saturating_add(1);
        next.revision = next.revision.saturating_add(1);
        next.phase = DaemonLifecyclePhase::DrainingAdmission;
        next.trigger = Some(request.trigger);
        next.reason_code = request.reason_code;
        next.requested_by = request.requested_by;
        next.requested_at_unix_ms = now_unix_ms;
        next.deadline_unix_ms = Some(request.deadline_unix_ms.max(now_unix_ms));
        next.admission_policy = request.admission_policy;
        next.subsystems = running_subsystems();
        Ok(next)
    }

    /// Builds a legal phase transition for one exact drain epoch.
    pub(crate) fn propose_advance(
        &self,
        epoch: u64,
        phase: DaemonLifecyclePhase,
    ) -> Result<DaemonLifecycleSnapshot, DaemonLifecycleError> {
        let current = self.snapshot()?;
        if current.epoch != epoch {
            return Err(DaemonLifecycleError::StaleEpoch {
                expected: epoch,
                actual: current.epoch,
            });
        }
        if !legal_edge(current.phase, phase) {
            return Err(invalid_transition(current.phase, phase));
        }
        let mut next = current;
        next.revision = next.revision.saturating_add(1);
        next.phase = phase;
        next.reason_code = match phase {
            DaemonLifecyclePhase::DrainingSubsystems => "daemon.lifecycle.draining_subsystems",
            DaemonLifecyclePhase::Checkpointing => "daemon.lifecycle.checkpointing",
            DaemonLifecyclePhase::ShutdownRequested => "daemon.lifecycle.shutdown_requested",
            _ => "daemon.lifecycle.transition",
        }
        .to_owned();
        if phase == DaemonLifecyclePhase::DrainingSubsystems {
            for subsystem in &mut next.subsystems {
                subsystem.state = LifecycleSubsystemState::Draining;
            }
        } else if phase == DaemonLifecyclePhase::Checkpointing {
            if let Some(transports) = next
                .subsystems
                .iter_mut()
                .find(|entry| entry.subsystem == LifecycleSubsystem::Transports)
            {
                transports.state = LifecycleSubsystemState::Draining;
            }
        } else if phase == DaemonLifecyclePhase::ShutdownRequested {
            if let Some(transports) = next
                .subsystems
                .iter_mut()
                .find(|entry| entry.subsystem == LifecycleSubsystem::Transports)
            {
                transports.state = LifecycleSubsystemState::Drained;
            }
        }
        Ok(next)
    }

    /// Builds a cancellation transition before checkpointing begins.
    pub(crate) fn propose_cancel(
        &self,
        epoch: u64,
        requested_by: String,
    ) -> Result<DaemonLifecycleSnapshot, DaemonLifecycleError> {
        let current = self.snapshot()?;
        if current.epoch != epoch {
            return Err(DaemonLifecycleError::StaleEpoch {
                expected: epoch,
                actual: current.epoch,
            });
        }
        if !matches!(
            current.phase,
            DaemonLifecyclePhase::DrainingAdmission | DaemonLifecyclePhase::DrainingSubsystems
        ) {
            return Err(invalid_transition(current.phase, DaemonLifecyclePhase::Running));
        }
        let mut next = current;
        next.revision = next.revision.saturating_add(1);
        next.phase = DaemonLifecyclePhase::Running;
        next.trigger = None;
        next.reason_code = "daemon.lifecycle.drain_cancelled".to_owned();
        next.requested_by = requested_by;
        next.deadline_unix_ms = None;
        next.subsystems = running_subsystems();
        Ok(next)
    }

    /// Applies a snapshot only if it extends the current revision by one.
    pub(crate) fn apply(&self, next: DaemonLifecycleSnapshot) -> Result<(), DaemonLifecycleError> {
        let mut current = self.state.lock().map_err(|_| DaemonLifecycleError::LockPoisoned)?;
        if next.revision != current.revision.saturating_add(1)
            || (next.epoch != current.epoch && next.epoch != current.epoch.saturating_add(1))
        {
            return Err(DaemonLifecycleError::StaleEpoch {
                expected: next.epoch,
                actual: current.epoch,
            });
        }
        *current = next.clone();
        self.updates.send_replace(next);
        Ok(())
    }

    /// Waits until the controller publishes the terminal shutdown request.
    pub(crate) async fn wait_for_shutdown(&self) {
        let mut updates = self.subscribe();
        loop {
            if updates.borrow().phase == DaemonLifecyclePhase::ShutdownRequested {
                return;
            }
            if updates.changed().await.is_err() {
                return;
            }
        }
    }
}

fn running_subsystems() -> Vec<LifecycleSubsystemSnapshot> {
    LifecycleSubsystem::DRAIN_ORDER
        .into_iter()
        .map(|subsystem| LifecycleSubsystemSnapshot {
            subsystem,
            state: LifecycleSubsystemState::Running,
        })
        .collect()
}

fn legal_edge(from: DaemonLifecyclePhase, to: DaemonLifecyclePhase) -> bool {
    matches!(
        (from, to),
        (DaemonLifecyclePhase::DrainingAdmission, DaemonLifecyclePhase::DrainingSubsystems)
            | (DaemonLifecyclePhase::DrainingSubsystems, DaemonLifecyclePhase::Checkpointing)
            | (DaemonLifecyclePhase::Checkpointing, DaemonLifecyclePhase::ShutdownRequested)
    )
}

fn invalid_transition(
    from: DaemonLifecyclePhase,
    to: DaemonLifecyclePhase,
) -> DaemonLifecycleError {
    DaemonLifecycleError::InvalidTransition { from: from.as_str(), to: to.as_str() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller() -> DaemonLifecycleController {
        DaemonLifecycleController::new(DaemonLifecycleSnapshot::recovery_barrier(1, 1, 10))
    }

    #[test]
    fn lifecycle_requires_recovery_before_drain() {
        let controller = controller();
        let error = controller
            .propose_drain(
                DaemonDrainRequest {
                    trigger: DaemonDrainTrigger::Admin,
                    reason_code: "daemon.lifecycle.admin_drain".to_owned(),
                    requested_by: "operator".to_owned(),
                    deadline_unix_ms: 20,
                    admission_policy: DrainAdmissionPolicy::RejectNew,
                },
                10,
            )
            .expect("existing recovery barrier should be returned");
        assert_eq!(error.phase, DaemonLifecyclePhase::RecoveryBarrier);
    }

    #[test]
    fn recovery_barrier_rejects_input_until_ready() {
        let controller = controller();
        let barrier = controller.snapshot().expect("recovery barrier should load");
        assert!(barrier.phase.blocks_admission());
        assert_eq!(barrier.admission_policy, DrainAdmissionPolicy::RejectNew);
        assert_eq!(barrier.reason_code, "daemon.lifecycle.startup_recovery");

        let running = controller.propose_startup_ready().expect("startup should finish");
        controller.apply(running).expect("running transition should apply");
        let ready = controller.snapshot().expect("running snapshot should load");
        assert!(!ready.phase.blocks_admission());
    }

    #[test]
    fn drain_advances_through_checkpoint_before_shutdown() {
        let controller = controller();
        let running = controller.propose_startup_ready().expect("startup should finish");
        controller.apply(running).expect("running transition should apply");
        let draining = controller
            .propose_drain(
                DaemonDrainRequest {
                    trigger: DaemonDrainTrigger::Sigterm,
                    reason_code: "daemon.lifecycle.sigterm".to_owned(),
                    requested_by: "system:signal".to_owned(),
                    deadline_unix_ms: 30,
                    admission_policy: DrainAdmissionPolicy::RejectNew,
                },
                20,
            )
            .expect("drain should be proposed");
        let epoch = draining.epoch;
        controller.apply(draining).expect("drain should apply");
        for phase in [
            DaemonLifecyclePhase::DrainingSubsystems,
            DaemonLifecyclePhase::Checkpointing,
            DaemonLifecyclePhase::ShutdownRequested,
        ] {
            let next = controller.propose_advance(epoch, phase).expect("edge should be legal");
            controller.apply(next).expect("edge should apply");
        }
        assert_eq!(
            controller.snapshot().expect("snapshot should load").phase,
            DaemonLifecyclePhase::ShutdownRequested
        );
    }

    #[test]
    fn cancellation_is_blocked_after_checkpointing_starts() {
        let controller = controller();
        let running = controller.propose_startup_ready().expect("startup should finish");
        controller.apply(running).expect("running transition should apply");
        let draining = controller
            .propose_drain(
                DaemonDrainRequest {
                    trigger: DaemonDrainTrigger::Admin,
                    reason_code: "daemon.lifecycle.admin_drain".to_owned(),
                    requested_by: "operator".to_owned(),
                    deadline_unix_ms: 30,
                    admission_policy: DrainAdmissionPolicy::RejectNew,
                },
                20,
            )
            .expect("drain should be proposed");
        let epoch = draining.epoch;
        controller.apply(draining).expect("drain should apply");
        let draining_subsystems = controller
            .propose_advance(epoch, DaemonLifecyclePhase::DrainingSubsystems)
            .expect("subsystem drain should be legal");
        controller.apply(draining_subsystems).expect("subsystem drain should apply");
        let checkpointing = controller
            .propose_advance(epoch, DaemonLifecyclePhase::Checkpointing)
            .expect("checkpoint should be legal");
        controller.apply(checkpointing).expect("checkpoint should apply");
        assert!(matches!(
            controller.propose_cancel(epoch, "operator".to_owned()),
            Err(DaemonLifecycleError::InvalidTransition { .. })
        ));
    }
}
