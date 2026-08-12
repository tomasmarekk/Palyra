//! Run-stream adoption of the shared cancellation and backpressure contracts.
//!
//! The run generation owns the root scope. Child scopes inherit its deadline and
//! settlement budgets, while protected wire events keep their blocking delivery path.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use palyra_common::runtime_contracts::{
    BackpressureOverflowAction, BackpressurePolicy, CancellationContextV1, CancellationReason,
    CancellationScopeKind, RuntimeGeneration, RuntimeOperationId,
    RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
};
use tokio::sync::watch;
use tonic::Status;
use ulid::Ulid;

use crate::gateway::current_unix_ms;

pub(crate) const RUN_STREAM_RESPONSE_CHANNEL_CAPACITY: usize = 16;
pub(crate) const PROCESS_PROGRESS_CHANNEL_CAPACITY: usize = 1;
pub(crate) const PROCESS_PROGRESS_BACKPRESSURE_TAPE_EVENT: &str =
    "runtime.backpressure.process_progress_coalesced";
pub(crate) const PROCESS_PROGRESS_BACKPRESSURE_REASON_CODE: &str =
    "runtime.backpressure.process_progress_coalesced";
pub(crate) const RUN_INTERRUPT_LATENCY_REASON_CODE: &str = "runtime.interrupt_latency.observed";
pub(crate) const RUN_INTERRUPT_LATENCY_CLAMPED_REASON_CODE: &str =
    "runtime.interrupt_latency.clamped";
pub(crate) const RUN_INTERRUPT_LATENCY_MAX_MS: u64 = 300_000;
const RUN_GRACEFUL_SETTLE_MS: u64 = 5_000;
const RUN_HARD_ABORT_AFTER_MS: u64 = 30_000;
const TERMINAL_DELIVERY_TIMEOUT_MS: u64 = 5_000;
#[cfg(test)]
const TEST_TERMINAL_DELIVERY_TIMEOUT_ENV: &str =
    "PALYRA_TEST_RUN_STREAM_TERMINAL_DELIVERY_TIMEOUT_MS";
const RUN_STREAM_OVERFLOW_SUMMARY_BYTES: usize = 512;

/// Active flow-control hierarchy for one admitted run generation.
#[derive(Debug, Clone)]
pub(crate) struct RunStreamFlowControl {
    root: CancellationContextV1,
    cancellation_tx: Arc<watch::Sender<Option<CancellationReason>>>,
    cancellation_requested_at_unix_ms: Arc<AtomicI64>,
    interrupt_observation_recorded: Arc<AtomicBool>,
    active_interrupt_phases: Arc<Mutex<[u32; RunInterruptPhase::COUNT]>>,
}

/// Bounded phase vocabulary used by interrupt-latency diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunInterruptPhase {
    PreProvider,
    Provider,
    Approval,
    Tool,
    DeliveryTerminal,
}

impl RunInterruptPhase {
    pub(crate) const ALL: [Self; 5] =
        [Self::PreProvider, Self::Provider, Self::Approval, Self::Tool, Self::DeliveryTerminal];
    const COUNT: usize = Self::ALL.len();

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PreProvider => "pre_provider",
            Self::Provider => "provider",
            Self::Approval => "approval",
            Self::Tool => "tool",
            Self::DeliveryTerminal => "delivery_terminal",
        }
    }

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::PreProvider => 0,
            Self::Provider => 1,
            Self::Approval => 2,
            Self::Tool => 3,
            Self::DeliveryTerminal => 4,
        }
    }
}

/// One request-to-observation latency sample with a bounded phase and value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunInterruptLatencyObservation {
    pub(crate) phase: RunInterruptPhase,
    pub(crate) latency_ms: u64,
    pub(crate) clamped: bool,
}

/// Restores the active phase when a bounded runtime operation finishes.
#[derive(Debug)]
pub(crate) struct RunInterruptPhaseGuard {
    active_phases: Arc<Mutex<[u32; RunInterruptPhase::COUNT]>>,
    phase: RunInterruptPhase,
}

impl Drop for RunInterruptPhaseGuard {
    fn drop(&mut self) {
        let mut active = self.active_phases.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        active[self.phase.index()] = active[self.phase.index()].saturating_sub(1);
    }
}

/// Live child handle paired with its durable cancellation-context snapshot.
#[derive(Debug, Clone)]
pub(crate) struct LiveCancellationScope {
    context: CancellationContextV1,
    cancellation_rx: watch::Receiver<Option<CancellationReason>>,
}

impl LiveCancellationScope {
    /// Returns the immutable context used for persistence and diagnostics.
    #[must_use]
    pub(crate) fn context(&self) -> &CancellationContextV1 {
        &self.context
    }

    /// Returns whether this child may start work at the supplied wall-clock time.
    #[must_use]
    pub(crate) fn permits_new_work(&self, now_unix_ms: i64) -> bool {
        self.current_reason().is_none() && self.context.permits_new_work(now_unix_ms)
    }

    /// Returns the first root cancellation reason without waiting.
    #[must_use]
    pub(crate) fn current_reason(&self) -> Option<CancellationReason> {
        *self.cancellation_rx.borrow()
    }

    /// Waits until the root cancellation authority commits its first reason.
    pub(crate) async fn cancelled(&mut self) -> CancellationReason {
        loop {
            if let Some(reason) = self.current_reason() {
                return reason;
            }
            if self.cancellation_rx.changed().await.is_err() {
                return CancellationReason::ParentCancelled;
            }
        }
    }
}

impl RunStreamFlowControl {
    /// Creates the root run scope from the host-issued generation and wall-clock budget.
    ///
    /// # Errors
    /// Returns `internal` if the generated identity or bounded timing contract is invalid.
    #[allow(clippy::result_large_err)]
    pub(crate) fn new(
        generation: RuntimeGeneration,
        wall_clock_budget: Duration,
    ) -> Result<Self, Status> {
        let root = Self::new_root_context(generation, wall_clock_budget)?;
        Ok(Self::from_root(root))
    }

    /// Creates a child-run root whose timing authority is bounded by an inherited ChildTask.
    ///
    /// The child run owns a distinct generation, so the inherited scope is causal authority only;
    /// it is deliberately not installed as the structural parent of the new root.
    ///
    /// # Errors
    /// Returns `failed_precondition` when the inherited authority is malformed, cancelled, or
    /// expired, and `internal` if the child root cannot be constructed.
    #[allow(clippy::result_large_err)]
    pub(crate) fn from_delegated_child(
        generation: RuntimeGeneration,
        wall_clock_budget: Duration,
        inherited: &CancellationContextV1,
    ) -> Result<Self, Status> {
        inherited.validate().map_err(|error| {
            Status::failed_precondition(format!(
                "delegated ChildTask cancellation authority is invalid: {error}"
            ))
        })?;
        if inherited.scope != CancellationScopeKind::ChildTask
            || inherited.parent_scope_id.is_none()
            || inherited.reason.is_some()
        {
            return Err(Status::failed_precondition(
                "delegated run requires active parented ChildTask cancellation authority",
            ));
        }
        let now = current_unix_ms();
        if !inherited.permits_new_work(now) {
            return Err(Status::deadline_exceeded(
                "delegated ChildTask cancellation authority no longer permits admission",
            ));
        }
        let mut root = Self::new_root_context(generation, wall_clock_budget)?;
        root.deadline_unix_ms = match (root.deadline_unix_ms, inherited.deadline_unix_ms) {
            (Some(local), Some(parent)) => Some(local.min(parent)),
            (local, None) => local,
            (None, parent) => parent,
        };
        root.graceful_settle_ms = root.graceful_settle_ms.min(inherited.graceful_settle_ms);
        root.hard_abort_after_ms = root.hard_abort_after_ms.min(inherited.hard_abort_after_ms);
        root.validate().map_err(|error| {
            Status::internal(format!("delegated run cancellation contract is invalid: {error}"))
        })?;
        Ok(Self::from_root(root))
    }

    /// Creates a standalone root context for a non-streaming run surface.
    ///
    /// # Errors
    /// Returns `internal` if the generated identity or bounded timing contract is invalid.
    #[allow(clippy::result_large_err)]
    pub(crate) fn new_root_context(
        generation: RuntimeGeneration,
        wall_clock_budget: Duration,
    ) -> Result<CancellationContextV1, Status> {
        let budget_ms = duration_millis_u64(wall_clock_budget);
        let deadline_unix_ms = current_unix_ms()
            .checked_add(i64::try_from(budget_ms).unwrap_or(i64::MAX))
            .ok_or_else(|| Status::internal("run cancellation deadline overflowed"))?;
        let root = CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: new_scope_id("run")?,
            scope: CancellationScopeKind::Run,
            generation,
            parent_scope_id: None,
            reason: None,
            deadline_unix_ms: Some(deadline_unix_ms),
            graceful_settle_ms: RUN_GRACEFUL_SETTLE_MS,
            hard_abort_after_ms: RUN_HARD_ABORT_AFTER_MS,
        };
        root.validate().map_err(|error| {
            Status::internal(format!("run cancellation contract is invalid: {error}"))
        })?;
        Ok(root)
    }

    /// Returns the root scope for asynchronous child-task derivation.
    #[must_use]
    pub(crate) fn root_context(&self) -> &CancellationContextV1 {
        &self.root
    }

    /// Starts an independent root scope for a newer durable run generation.
    ///
    /// The replacement preserves the original wall-clock and settlement
    /// limits exactly. Cancellation state and child scopes stay attached to
    /// the superseded generation and cannot authorize work in the replacement.
    ///
    /// # Errors
    /// Returns `failed_precondition` unless `generation` is newer than the
    /// current root, or `internal` if the replacement scope is invalid.
    #[allow(clippy::result_large_err)]
    pub(crate) fn supersede_generation(
        &self,
        generation: RuntimeGeneration,
    ) -> Result<Self, Status> {
        if generation <= self.root.generation {
            return Err(Status::failed_precondition(
                "superseded run cancellation scope requires a newer generation",
            ));
        }
        let mut root = self.root.clone();
        root.scope_id = new_scope_id(CancellationScopeKind::Run.as_str())?;
        root.generation = generation;
        root.parent_scope_id = None;
        root.reason = None;
        root.validate().map_err(|error| {
            Status::internal(format!("superseded run cancellation contract is invalid: {error}"))
        })?;
        Ok(Self::from_root(root))
    }

    fn from_root(root: CancellationContextV1) -> Self {
        let (cancellation_tx, _) = watch::channel(root.reason);
        Self {
            root,
            cancellation_tx: Arc::new(cancellation_tx),
            cancellation_requested_at_unix_ms: Arc::new(AtomicI64::new(0)),
            interrupt_observation_recorded: Arc::new(AtomicBool::new(false)),
            active_interrupt_phases: Arc::new(Mutex::new([0; RunInterruptPhase::COUNT])),
        }
    }

    /// Derives one child scope without widening the root deadline or abort budget.
    ///
    /// # Errors
    /// Returns `internal` if identity generation or child validation fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn child(
        &self,
        scope: CancellationScopeKind,
        requested_timeout: Duration,
    ) -> Result<CancellationContextV1, Status> {
        let now = current_unix_ms();
        let requested_deadline = now
            .checked_add(i64::try_from(duration_millis_u64(requested_timeout)).unwrap_or(i64::MAX))
            .ok_or_else(|| Status::internal("child cancellation deadline overflowed"))?;
        let deadline_unix_ms = Some(
            self.root
                .deadline_unix_ms
                .map_or(requested_deadline, |parent| parent.min(requested_deadline)),
        );
        self.root
            .derive_child(
                new_scope_id(scope.as_str())?,
                scope,
                deadline_unix_ms,
                self.root.graceful_settle_ms,
                self.root.hard_abort_after_ms,
            )
            .map_err(|error| {
                Status::internal(format!("child cancellation contract is invalid: {error}"))
            })
    }

    /// Derives a child that observes cancellation committed after derivation.
    ///
    /// # Errors
    /// Returns the same validation errors as [`Self::child`].
    #[allow(clippy::result_large_err)]
    pub(crate) fn live_child(
        &self,
        scope: CancellationScopeKind,
        requested_timeout: Duration,
    ) -> Result<LiveCancellationScope, Status> {
        Ok(LiveCancellationScope {
            context: self.child(scope, requested_timeout)?,
            cancellation_rx: self.cancellation_tx.subscribe(),
        })
    }

    /// Subscribes to the live root authority without deriving a new scope.
    #[must_use]
    pub(crate) fn live_root(&self) -> LiveCancellationScope {
        LiveCancellationScope {
            context: self.root.clone(),
            cancellation_rx: self.cancellation_tx.subscribe(),
        }
    }

    /// Derives a bounded child from another scope in this run hierarchy.
    ///
    /// # Errors
    /// Returns `internal` when the child identity or inherited deadline is invalid.
    #[allow(clippy::result_large_err)]
    pub(crate) fn child_from(
        &self,
        parent: &CancellationContextV1,
        scope: CancellationScopeKind,
        requested_timeout: Duration,
    ) -> Result<CancellationContextV1, Status> {
        if parent.generation != self.root.generation {
            return Err(Status::failed_precondition(
                "child cancellation scope generation does not match the run root",
            ));
        }
        let now = current_unix_ms();
        let requested_deadline = now
            .checked_add(i64::try_from(duration_millis_u64(requested_timeout)).unwrap_or(i64::MAX))
            .ok_or_else(|| Status::internal("nested cancellation deadline overflowed"))?;
        let deadline_unix_ms = Some(
            parent
                .deadline_unix_ms
                .map_or(requested_deadline, |deadline| deadline.min(requested_deadline)),
        );
        parent
            .derive_child(
                new_scope_id(scope.as_str())?,
                scope,
                deadline_unix_ms,
                parent.graceful_settle_ms,
                parent.hard_abort_after_ms,
            )
            .map_err(|error| {
                Status::internal(format!("nested cancellation contract is invalid: {error}"))
            })
    }

    /// Returns the strictly positive time left for starting work.
    ///
    /// # Errors
    /// Returns `deadline_exceeded` when cancellation or expiry blocks new work.
    #[allow(clippy::result_large_err)]
    pub(crate) fn remaining_for_new_work(
        cancellation: &CancellationContextV1,
    ) -> Result<Duration, Status> {
        let now = current_unix_ms();
        if !cancellation.permits_new_work(now) {
            return Err(Status::deadline_exceeded(format!(
                "{} cancellation scope no longer permits work",
                cancellation.scope.as_str()
            )));
        }
        let Some(deadline_unix_ms) = cancellation.deadline_unix_ms else {
            return Ok(Duration::from_millis(cancellation.hard_abort_after_ms.max(1)));
        };
        let remaining_ms = deadline_unix_ms.saturating_sub(now);
        Ok(Duration::from_millis(u64::try_from(remaining_ms).unwrap_or(1)))
    }

    /// Derives the protected terminal-delivery scope after durable settlement.
    ///
    /// Delivery remains bounded even when the run root deadline elapsed while
    /// the host was committing terminal state. It still carries the exact run
    /// generation and parent identity, but cannot authorize runtime work.
    ///
    /// # Errors
    /// Returns `internal` when the bounded delivery child is invalid.
    #[allow(clippy::result_large_err)]
    pub(crate) fn delivery(&self) -> Result<CancellationContextV1, Status> {
        let now = current_unix_ms();
        let deadline_unix_ms = now
            .checked_add(
                i64::try_from(duration_millis_u64(terminal_delivery_timeout())).unwrap_or(i64::MAX),
            )
            .ok_or_else(|| Status::internal("terminal delivery deadline overflowed"))?;
        let delivery = CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: new_scope_id(CancellationScopeKind::Delivery.as_str())?,
            scope: CancellationScopeKind::Delivery,
            generation: self.root.generation,
            parent_scope_id: Some(self.root.scope_id.clone()),
            reason: None,
            deadline_unix_ms: Some(deadline_unix_ms),
            graceful_settle_ms: self.root.graceful_settle_ms,
            hard_abort_after_ms: self.root.hard_abort_after_ms,
        };
        delivery.validate().map_err(|error| {
            Status::internal(format!("terminal delivery contract is invalid: {error}"))
        })?;
        Ok(delivery)
    }

    /// Records the host-classified reason observed by this run scope.
    pub(crate) fn request_cancel(&self, reason: CancellationReason) -> bool {
        self.request_cancel_at(reason, current_unix_ms())
    }

    /// Records a persisted reason with the timestamp assigned by the durable authority.
    pub(crate) fn request_cancel_from_persisted_reason(
        &self,
        reason: &str,
        requested_at_unix_ms: i64,
    ) -> bool {
        self.request_cancel_at(classify_cancellation_reason(reason), requested_at_unix_ms.max(1))
    }

    fn request_cancel_at(&self, reason: CancellationReason, requested_at_unix_ms: i64) -> bool {
        self.cancellation_tx.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            self.cancellation_requested_at_unix_ms
                .store(requested_at_unix_ms.max(1), Ordering::Release);
            *current = Some(reason);
            true
        })
    }

    /// Updates the root scope from the persisted cancellation reason.
    pub(crate) fn request_cancel_from_reason(&self, reason: &str) -> bool {
        self.request_cancel(classify_cancellation_reason(reason))
    }

    /// Marks one runtime phase active until the returned guard is dropped.
    #[must_use]
    pub(crate) fn enter_interrupt_phase(&self, phase: RunInterruptPhase) -> RunInterruptPhaseGuard {
        let mut active =
            self.active_interrupt_phases.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        active[phase.index()] = active[phase.index()].saturating_add(1);
        drop(active);
        RunInterruptPhaseGuard { active_phases: Arc::clone(&self.active_interrupt_phases), phase }
    }

    /// Returns the highest-priority phase currently holding runtime work.
    #[must_use]
    pub(crate) fn active_interrupt_phase(&self) -> RunInterruptPhase {
        let active =
            self.active_interrupt_phases.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // A nested approval/tool operation is more precise than its enclosing
        // provider-response scope; terminal delivery wins any late overlap.
        [
            RunInterruptPhase::DeliveryTerminal,
            RunInterruptPhase::Approval,
            RunInterruptPhase::Tool,
            RunInterruptPhase::Provider,
            RunInterruptPhase::PreProvider,
        ]
        .into_iter()
        .find(|phase| active[phase.index()] > 0)
        .unwrap_or(RunInterruptPhase::PreProvider)
    }

    /// Takes the run's only interrupt-latency sample.
    #[must_use]
    pub(crate) fn take_interrupt_latency_observation(
        &self,
        observed_at_unix_ms: i64,
    ) -> Option<RunInterruptLatencyObservation> {
        if self.cancellation_tx.borrow().is_none() {
            return None;
        }
        let requested_at_unix_ms = self.cancellation_requested_at_unix_ms.load(Ordering::Acquire);
        if requested_at_unix_ms <= 0 {
            return None;
        }
        if self
            .interrupt_observation_recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let clock_moved_backwards = observed_at_unix_ms < requested_at_unix_ms;
        let raw_latency_ms =
            u64::try_from(observed_at_unix_ms.saturating_sub(requested_at_unix_ms).max(0))
                .unwrap_or(u64::MAX);
        Some(RunInterruptLatencyObservation {
            phase: self.active_interrupt_phase(),
            latency_ms: raw_latency_ms.min(RUN_INTERRUPT_LATENCY_MAX_MS),
            clamped: clock_moved_backwards || raw_latency_ms > RUN_INTERRUPT_LATENCY_MAX_MS,
        })
    }

    /// Returns the first committed cancellation reason, if any.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn current_cancellation_reason(&self) -> Option<CancellationReason> {
        *self.cancellation_tx.borrow()
    }
}

/// Classifies a persisted cancellation reason into the shared bounded vocabulary.
#[must_use]
pub(crate) fn classify_cancellation_reason(reason: &str) -> CancellationReason {
    let normalized = reason.trim().to_ascii_lowercase();
    if normalized.contains("deadline") || normalized.contains("timeout") {
        CancellationReason::DeadlineExceeded
    } else if normalized.contains("drain") || normalized.contains("shutdown") {
        CancellationReason::DaemonDrain
    } else if normalized.contains("steer")
        || normalized.contains("supersed")
        || normalized.contains("replace")
        || normalized.contains("model_switch")
    {
        CancellationReason::SteerSupersede
    } else if normalized.contains("evict") || normalized.contains("resource") {
        CancellationReason::ResourceEviction
    } else if normalized.contains("generation") || normalized.contains("reconfigur") {
        CancellationReason::GenerationSuperseded
    } else if normalized.contains("parent") || normalized.contains("delegated_parent") {
        CancellationReason::ParentCancelled
    } else {
        CancellationReason::UserCancel
    }
}

/// Bounded response-channel policy used by every gRPC run stream.
///
/// # Errors
/// Returns `internal` if the compile-time policy violates the shared contract.
#[allow(clippy::result_large_err)]
pub(crate) fn run_stream_response_backpressure_policy() -> Result<BackpressurePolicy, Status> {
    validated_policy(BackpressurePolicy {
        schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
        capacity: RUN_STREAM_RESPONSE_CHANNEL_CAPACITY,
        overflow_action: BackpressureOverflowAction::BlockProducer,
        preserve_terminal: true,
        preserve_approval: true,
        max_summary_bytes: RUN_STREAM_OVERFLOW_SUMMARY_BYTES,
    })
}

/// Bounded coalescing policy for process progress snapshots.
///
/// # Errors
/// Returns `internal` if the compile-time policy violates the shared contract.
#[allow(clippy::result_large_err)]
pub(crate) fn process_progress_backpressure_policy() -> Result<BackpressurePolicy, Status> {
    validated_policy(BackpressurePolicy {
        schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
        capacity: PROCESS_PROGRESS_CHANNEL_CAPACITY,
        overflow_action: BackpressureOverflowAction::CoalesceProgress,
        preserve_terminal: true,
        preserve_approval: true,
        max_summary_bytes: RUN_STREAM_OVERFLOW_SUMMARY_BYTES,
    })
}

fn validated_policy(policy: BackpressurePolicy) -> Result<BackpressurePolicy, Status> {
    policy.validate().map_err(|error| {
        Status::internal(format!("run-stream backpressure contract is invalid: {error}"))
    })?;
    Ok(policy)
}

fn new_scope_id(prefix: &str) -> Result<RuntimeOperationId, Status> {
    RuntimeOperationId::parse(format!("{prefix}:{}", Ulid::generate()).as_str()).map_err(|error| {
        Status::internal(format!("cancellation scope identity is invalid: {error}"))
    })
}

fn terminal_delivery_timeout() -> Duration {
    #[cfg(test)]
    if let Ok(raw) = std::env::var(TEST_TERMINAL_DELIVERY_TIMEOUT_ENV) {
        if let Ok(timeout_ms) = raw.parse::<u64>() {
            return Duration::from_millis(timeout_ms.max(1));
        }
    }
    Duration::from_millis(TERMINAL_DELIVERY_TIMEOUT_MS)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Barrier;

    fn generation() -> RuntimeGeneration {
        RuntimeGeneration::new(7).expect("generation")
    }

    #[test]
    fn run_scope_derives_every_v2_child_without_widening_deadline() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        for scope in [
            CancellationScopeKind::ProviderAttempt,
            CancellationScopeKind::ToolExecution,
            CancellationScopeKind::ApprovalWait,
            CancellationScopeKind::ChildTask,
            CancellationScopeKind::Process,
        ] {
            let child = hierarchy.child(scope, Duration::from_secs(120)).expect("child scope");
            assert_eq!(child.parent_scope_id.as_ref(), Some(&hierarchy.root_context().scope_id));
            assert_eq!(child.generation, generation());
            assert!(child.deadline_unix_ms <= hierarchy.root_context().deadline_unix_ms);
            assert!(child.hard_abort_after_ms <= hierarchy.root_context().hard_abort_after_ms);
        }
        let delivery = hierarchy.delivery().expect("delivery scope");
        assert_eq!(delivery.scope, CancellationScopeKind::Delivery);
        assert_eq!(delivery.parent_scope_id.as_ref(), Some(&hierarchy.root_context().scope_id));
        assert_eq!(delivery.generation, generation());
        assert!(delivery.deadline_unix_ms.is_some());
    }

    #[test]
    fn delegated_child_root_clamps_inherited_authority_without_reusing_parent_generation() {
        let now = current_unix_ms();
        let inherited = CancellationContextV1 {
            schema_version: RUNTIME_FLOW_CONTROL_SCHEMA_VERSION,
            scope_id: RuntimeOperationId::parse("child_task:delegated").expect("scope id"),
            scope: CancellationScopeKind::ChildTask,
            generation: generation(),
            parent_scope_id: Some(
                RuntimeOperationId::parse("run:parent").expect("parent scope id"),
            ),
            reason: None,
            deadline_unix_ms: Some(now.saturating_add(5_000)),
            graceful_settle_ms: 250,
            hard_abort_after_ms: 1_000,
        };
        let child_generation = RuntimeGeneration::new(11).expect("child generation");
        let hierarchy = RunStreamFlowControl::from_delegated_child(
            child_generation,
            Duration::from_secs(60),
            &inherited,
        )
        .expect("delegated hierarchy");

        assert_eq!(hierarchy.root_context().generation, child_generation);
        assert_eq!(hierarchy.root_context().parent_scope_id, None);
        assert!(hierarchy.root_context().deadline_unix_ms <= inherited.deadline_unix_ms);
        assert_eq!(hierarchy.root_context().graceful_settle_ms, 250);
        assert_eq!(hierarchy.root_context().hard_abort_after_ms, 1_000);
    }

    #[test]
    fn cancellation_reason_classification_is_bounded() {
        assert_eq!(
            classify_cancellation_reason("delegated_parent_cancelled"),
            CancellationReason::ParentCancelled
        );
        assert_eq!(
            classify_cancellation_reason("cron replace policy preemption"),
            CancellationReason::SteerSupersede
        );
        assert_eq!(
            classify_cancellation_reason("stream_cancel_command"),
            CancellationReason::UserCancel
        );
    }

    #[tokio::test]
    async fn cancellation_propagates_to_all_previously_derived_scopes_once() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        let mut provider = hierarchy
            .live_child(CancellationScopeKind::ProviderAttempt, Duration::from_secs(30))
            .expect("provider scope");
        let mut tool = hierarchy
            .live_child(CancellationScopeKind::ToolExecution, Duration::from_secs(30))
            .expect("tool scope");

        assert!(provider.permits_new_work(current_unix_ms()));
        assert!(tool.permits_new_work(current_unix_ms()));
        assert!(hierarchy.request_cancel(CancellationReason::UserCancel));
        assert!(!hierarchy.request_cancel(CancellationReason::DaemonDrain));

        assert_eq!(provider.cancelled().await, CancellationReason::UserCancel);
        assert_eq!(tool.cancelled().await, CancellationReason::UserCancel);
        assert!(!provider.permits_new_work(current_unix_ms()));
        assert!(!tool.permits_new_work(current_unix_ms()));
        assert_eq!(hierarchy.current_cancellation_reason(), Some(CancellationReason::UserCancel));
    }

    #[tokio::test]
    async fn superseded_generation_preserves_limits_without_reusing_cancellation_authority() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        let original_root = hierarchy.root_context().clone();
        let mut old_child = hierarchy
            .live_child(CancellationScopeKind::ChildTask, Duration::from_secs(30))
            .expect("old child scope");
        assert!(hierarchy.request_cancel(CancellationReason::SteerSupersede));

        let replacement_generation = generation().next().expect("replacement generation");
        let replacement = hierarchy
            .supersede_generation(replacement_generation)
            .expect("newer generation should supersede");
        let replacement_root = replacement.root_context();
        assert_eq!(replacement_root.generation, replacement_generation);
        assert_ne!(replacement_root.scope_id, original_root.scope_id);
        assert_eq!(replacement_root.deadline_unix_ms, original_root.deadline_unix_ms);
        assert_eq!(replacement_root.graceful_settle_ms, original_root.graceful_settle_ms);
        assert_eq!(replacement_root.hard_abort_after_ms, original_root.hard_abort_after_ms);
        assert_eq!(replacement.current_cancellation_reason(), None);

        let mut replacement_child = replacement
            .live_child(CancellationScopeKind::ChildTask, Duration::from_secs(30))
            .expect("replacement child scope");
        assert_eq!(old_child.cancelled().await, CancellationReason::SteerSupersede);
        assert_eq!(replacement_child.current_reason(), None);
        assert!(replacement.request_cancel(CancellationReason::UserCancel));
        assert!(!replacement.request_cancel(CancellationReason::DaemonDrain));
        assert_eq!(replacement_child.cancelled().await, CancellationReason::UserCancel);
    }

    #[tokio::test]
    async fn daemon_drain_and_user_cancel_race_has_one_winner_and_propagates_it() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        let mut child = hierarchy
            .live_child(CancellationScopeKind::ChildTask, Duration::from_secs(30))
            .expect("child scope");
        let barrier = Arc::new(Barrier::new(3));
        let user_hierarchy = hierarchy.clone();
        let user_barrier = Arc::clone(&barrier);
        let user = tokio::spawn(async move {
            user_barrier.wait().await;
            user_hierarchy.request_cancel(CancellationReason::UserCancel)
        });
        let drain_hierarchy = hierarchy.clone();
        let drain_barrier = Arc::clone(&barrier);
        let drain = tokio::spawn(async move {
            drain_barrier.wait().await;
            drain_hierarchy.request_cancel(CancellationReason::DaemonDrain)
        });

        barrier.wait().await;
        let user_won = user.await.expect("user cancellation task");
        let drain_won = drain.await.expect("daemon drain task");

        assert_ne!(user_won, drain_won);
        let winner = hierarchy.current_cancellation_reason().expect("winning reason");
        assert!(matches!(winner, CancellationReason::UserCancel | CancellationReason::DaemonDrain));
        assert_eq!(child.cancelled().await, winner);
    }

    #[test]
    fn interrupt_latency_is_recorded_once_for_the_most_specific_active_phase() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        let _provider = hierarchy.enter_interrupt_phase(RunInterruptPhase::Provider);
        let tool = hierarchy.enter_interrupt_phase(RunInterruptPhase::Tool);
        let approval = hierarchy.enter_interrupt_phase(RunInterruptPhase::Approval);
        assert_eq!(hierarchy.active_interrupt_phase(), RunInterruptPhase::Approval);
        drop(approval);
        assert_eq!(hierarchy.active_interrupt_phase(), RunInterruptPhase::Tool);
        drop(tool);
        assert_eq!(hierarchy.active_interrupt_phase(), RunInterruptPhase::Provider);

        assert!(hierarchy.request_cancel_at(CancellationReason::UserCancel, 1_000));
        assert_eq!(
            hierarchy.take_interrupt_latency_observation(1_125),
            Some(RunInterruptLatencyObservation {
                phase: RunInterruptPhase::Provider,
                latency_ms: 125,
                clamped: false,
            })
        );
        assert_eq!(hierarchy.take_interrupt_latency_observation(1_250), None);
    }

    #[test]
    fn interrupt_latency_clamps_clock_skew_and_unbounded_values() {
        let clock_skew = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("clock-skew hierarchy");
        assert!(clock_skew.request_cancel_at(CancellationReason::UserCancel, 2_000));
        assert_eq!(
            clock_skew.take_interrupt_latency_observation(1_000),
            Some(RunInterruptLatencyObservation {
                phase: RunInterruptPhase::PreProvider,
                latency_ms: 0,
                clamped: true,
            })
        );

        let oversized = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("oversized-latency hierarchy");
        assert!(oversized.request_cancel_at(CancellationReason::UserCancel, 1));
        let observation = oversized
            .take_interrupt_latency_observation(
                i64::try_from(RUN_INTERRUPT_LATENCY_MAX_MS).expect("latency bound") + 2,
            )
            .expect("latency observation");
        assert_eq!(observation.latency_ms, RUN_INTERRUPT_LATENCY_MAX_MS);
        assert!(observation.clamped);
    }

    #[tokio::test]
    async fn approval_wait_cancel_propagates_and_keeps_approval_phase_attribution() {
        let hierarchy = RunStreamFlowControl::new(generation(), Duration::from_secs(60))
            .expect("run flow-control hierarchy");
        let mut approval = hierarchy
            .live_child(CancellationScopeKind::ApprovalWait, Duration::from_secs(30))
            .expect("approval scope");
        let _phase = hierarchy.enter_interrupt_phase(RunInterruptPhase::Approval);

        assert!(hierarchy.request_cancel_at(CancellationReason::UserCancel, 10_000));
        assert_eq!(approval.cancelled().await, CancellationReason::UserCancel);
        assert_eq!(
            hierarchy.take_interrupt_latency_observation(10_025),
            Some(RunInterruptLatencyObservation {
                phase: RunInterruptPhase::Approval,
                latency_ms: 25,
                clamped: false,
            })
        );
    }

    #[tokio::test]
    async fn response_event_flood_blocks_without_dropping_approval_or_terminal_events() {
        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            Progress,
            Approval,
            Terminal,
        }

        let policy = run_stream_response_backpressure_policy().expect("response policy");
        let (sender, mut receiver) = tokio::sync::mpsc::channel(policy.capacity);
        for _ in 0..policy.capacity {
            sender.try_send(Event::Progress).expect("fill bounded mailbox");
        }
        let protected_sender = sender.clone();
        let protected = tokio::spawn(async move {
            protected_sender.send(Event::Approval).await.expect("approval delivery");
            protected_sender.send(Event::Terminal).await.expect("terminal delivery");
        });
        tokio::task::yield_now().await;
        assert!(!protected.is_finished(), "protected events must block behind the flood");

        for _ in 0..policy.capacity {
            assert_eq!(receiver.recv().await, Some(Event::Progress));
        }
        assert_eq!(receiver.recv().await, Some(Event::Approval));
        assert_eq!(receiver.recv().await, Some(Event::Terminal));
        protected.await.expect("protected delivery task");
    }

    #[test]
    fn backpressure_policies_are_bounded_and_preserve_protected_events() {
        let response = run_stream_response_backpressure_policy().expect("response policy");
        assert_eq!(response.capacity, RUN_STREAM_RESPONSE_CHANNEL_CAPACITY);
        assert_eq!(response.overflow_action, BackpressureOverflowAction::BlockProducer);
        assert!(response.preserve_terminal);
        assert!(response.preserve_approval);

        let progress = process_progress_backpressure_policy().expect("progress policy");
        assert_eq!(progress.capacity, PROCESS_PROGRESS_CHANNEL_CAPACITY);
        assert_eq!(progress.overflow_action, BackpressureOverflowAction::CoalesceProgress);
    }
}
