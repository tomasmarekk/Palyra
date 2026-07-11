//! Dependency-injected probes, deterministic activation state, and barrier scheduling.
//!
//! Controller state is process-local; durable restart state enters through an explicit snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error as ThisError;

use super::{
    evidence::QaFaultControllerResumeState,
    is_bounded_actor,
    plan::{
        qa_fault_point_descriptor, QaFaultAction, QaFaultActivation, QaFaultInjectionPlan,
        QaFaultInjectionPlanValidationError, QaFaultRecoveryClass,
    },
    update_length_delimited_hash,
};

/// Synchronous checkpoint input accepted by cross-crate adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QaFaultCheckpoint<'a> {
    /// Exact registered point id.
    pub point_id: &'a str,
    /// Bounded non-secret actor label used for per-actor occurrence counting.
    pub actor: &'a str,
}

/// Typed instruction returned synchronously from a fault checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QaFaultDirective {
    /// Fault injection is disabled or this checkpoint does not match the plan.
    Continue,
    /// The caller must apply the declared action at this exact boundary.
    Activate(QaFaultActivationDirective),
}

/// Activated directive with deterministic occurrence and ordering evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaFaultActivationDirective {
    /// Complete validated activation selected by the plan.
    pub activation: QaFaultActivation,
    /// Bounded actor that reached the checkpoint.
    pub actor: String,
    /// Per-point, per-actor occurrence observed by the controller.
    pub observed_occurrence: u32,
    /// One-based sequence among distinct activated rules.
    pub activation_sequence: u32,
}

/// Controller record exported for later QA evidence projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultControllerRecord {
    /// Unique plan activation id.
    pub activation_id: String,
    /// Exact registered point reached by the adapter.
    pub point_id: String,
    /// Closed action returned to the adapter.
    pub action: QaFaultAction,
    /// Planned occurrence that activated.
    pub occurrence: u32,
    /// One-based ordering among distinct activated rules.
    pub activation_sequence: u32,
    /// Bounded actors observed for this activation.
    pub actors: Vec<String>,
    /// Recovery class, once the caller proves it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_class: Option<QaFaultRecoveryClass>,
}

/// Bounded active barrier state exposed to adapters that must resume a batch after restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultActiveBarrier {
    /// Unique plan activation id.
    pub activation_id: String,
    /// Exact registered barrier point.
    pub point_id: String,
    /// Required number of distinct participants.
    pub participants: u16,
    /// Participants whose joins are already accepted.
    pub actors: Vec<String>,
    /// Seeded release order once the join set is complete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_order: Option<Vec<String>>,
    /// Durable prefix of actors that already consumed their release.
    pub released_actors: Vec<String>,
}

/// Synchronous injected checkpoint contract for connector, worker, and daemon adapters.
pub trait QaFaultProbe: Send + Sync {
    /// Evaluates one point and actor without performing the requested side effect itself.
    fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError>;

    /// Associates a typed recovery outcome with an activated plan entry.
    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError>;

    /// Returns deterministic activation records for QA evidence projection.
    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError>;

    /// Returns active barrier state needed for restart-safe multi-actor adapters.
    fn active_barriers(&self) -> Result<Vec<QaFaultActiveBarrier>, QaFaultProbeError> {
        Ok(Vec::new())
    }
}

/// Cloneable type-erased probe injected into synchronous cross-crate code.
#[derive(Clone)]
pub struct QaFaultProbeHandle {
    probe: Arc<dyn QaFaultProbe>,
}

impl QaFaultProbeHandle {
    /// Wraps an explicit probe implementation for dependency injection.
    #[must_use]
    pub fn new(probe: Arc<dyn QaFaultProbe>) -> Self {
        Self { probe }
    }

    /// Wraps a concrete probe without exposing its storage type to adapters.
    #[must_use]
    pub fn from_probe(probe: impl QaFaultProbe + 'static) -> Self {
        Self::new(Arc::new(probe))
    }

    /// Evaluates one synchronous checkpoint.
    pub fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        self.probe.checkpoint(checkpoint)
    }

    /// Records one typed recovery outcome.
    pub fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        self.probe.record_recovery(activation_id, recovery_class)
    }

    /// Returns activation records for evidence projection.
    pub fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        self.probe.records()
    }

    /// Returns active barrier state for restart-safe adapter batching.
    pub fn active_barriers(&self) -> Result<Vec<QaFaultActiveBarrier>, QaFaultProbeError> {
        self.probe.active_barriers()
    }
}

impl Default for QaFaultProbeHandle {
    fn default() -> Self {
        Self::from_probe(DisabledQaFaultProbe)
    }
}

impl fmt::Debug for QaFaultProbeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("QaFaultProbeHandle").finish_non_exhaustive()
    }
}

/// Side-effect-free production default. It deliberately performs no validation.
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledQaFaultProbe;

impl QaFaultProbe for DisabledQaFaultProbe {
    fn checkpoint(
        &self,
        _checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        Ok(QaFaultDirective::Continue)
    }

    fn record_recovery(
        &self,
        _activation_id: &str,
        _recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        Ok(())
    }

    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        Ok(Vec::new())
    }
}

/// Explicit deterministic controller backed by a validated plan.
#[derive(Debug)]
pub struct DeterministicQaFaultController {
    plan: QaFaultInjectionPlan,
    state: Mutex<DeterministicControllerState>,
}

#[derive(Debug, Default)]
struct DeterministicControllerState {
    occurrences: BTreeMap<(String, String), u32>,
    records: Vec<QaFaultControllerRecord>,
    barrier_releases: BTreeMap<String, Vec<String>>,
    activation_sequence_offset: u32,
}

impl DeterministicQaFaultController {
    /// Builds an isolated controller with no process-global state.
    ///
    /// # Errors
    /// Returns every semantic plan validation issue.
    pub fn new(plan: QaFaultInjectionPlan) -> Result<Self, QaFaultInjectionPlanValidationError> {
        Self::new_resumed(plan, QaFaultControllerResumeState::default())
    }

    /// Builds an isolated controller from counters derived from validated campaign evidence.
    ///
    /// # Errors
    /// Returns every semantic plan validation issue.
    pub fn new_resumed(
        plan: QaFaultInjectionPlan,
        resume_state: QaFaultControllerResumeState,
    ) -> Result<Self, QaFaultInjectionPlanValidationError> {
        plan.validate()?;
        Ok(Self {
            plan,
            state: Mutex::new(DeterministicControllerState {
                occurrences: resume_state.occurrences,
                records: Vec::new(),
                barrier_releases: BTreeMap::new(),
                activation_sequence_offset: resume_state.highest_activation_sequence,
            }),
        })
    }

    /// Returns the immutable validated plan.
    #[must_use]
    pub fn plan(&self) -> &QaFaultInjectionPlan {
        &self.plan
    }

    /// Builds a stateless scheduler using the plan's reproduction seed.
    #[must_use]
    pub const fn scheduler(&self) -> DeterministicQaFaultScheduler {
        DeterministicQaFaultScheduler::new(self.plan.seed)
    }
}

impl QaFaultProbe for DeterministicQaFaultController {
    fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        if qa_fault_point_descriptor(checkpoint.point_id).is_none() {
            return Err(QaFaultProbeError::UnknownPoint(checkpoint.point_id.to_owned()));
        }
        validate_actor(checkpoint.actor)?;
        let mut state = self.state.lock().map_err(|_| QaFaultProbeError::StatePoisoned)?;
        let pending_release = state.records.iter().find_map(|record| {
            if record.point_id != checkpoint.point_id || record.recovery_class.is_some() {
                return None;
            }
            let QaFaultAction::Barrier { participants } = record.action else {
                return None;
            };
            if record.actors.len() != usize::from(participants) {
                return None;
            }
            let activation = self
                .plan
                .activations
                .iter()
                .find(|activation| activation.id == record.activation_id)?;
            let release_order =
                self.scheduler().release_order(activation, record.actors.as_slice()).ok()?;
            Some((record.activation_id.clone(), release_order))
        });
        if let Some((activation_id, release_order)) = pending_release {
            let released = state.barrier_releases.entry(activation_id).or_default();
            if release_order.get(released.len()).is_some_and(|actor| actor == checkpoint.actor) {
                released.push(checkpoint.actor.to_owned());
                return Ok(QaFaultDirective::Continue);
            }
        }
        let key = (checkpoint.point_id.to_owned(), checkpoint.actor.to_owned());
        let observed_occurrence = {
            let occurrence = state.occurrences.entry(key).or_insert(0);
            *occurrence = occurrence.checked_add(1).ok_or(QaFaultProbeError::OccurrenceOverflow)?;
            *occurrence
        };

        let Some(activation) = self.plan.activations.iter().find(|activation| {
            activation.point_id == checkpoint.point_id
                && activation.occurrence == observed_occurrence
                && activation.actor.as_deref().is_none_or(|actor| actor == checkpoint.actor)
        }) else {
            return Ok(QaFaultDirective::Continue);
        };

        if let Some(record) =
            state.records.iter_mut().find(|record| record.activation_id == activation.id)
        {
            let QaFaultAction::Barrier { participants } = activation.action else {
                return Ok(QaFaultDirective::Continue);
            };
            if record.actors.iter().any(|actor| actor == checkpoint.actor)
                || record.actors.len() >= usize::from(participants)
            {
                return Ok(QaFaultDirective::Continue);
            }
            record.actors.push(checkpoint.actor.to_owned());
            return Ok(QaFaultDirective::Activate(QaFaultActivationDirective {
                activation: activation.clone(),
                actor: checkpoint.actor.to_owned(),
                observed_occurrence,
                activation_sequence: record.activation_sequence,
            }));
        }

        let local_activation_sequence = u32::try_from(state.records.len() + 1)
            .map_err(|_| QaFaultProbeError::ActivationSequenceOverflow)?;
        let activation_sequence = state
            .activation_sequence_offset
            .checked_add(local_activation_sequence)
            .ok_or(QaFaultProbeError::ActivationSequenceOverflow)?;
        state.records.push(QaFaultControllerRecord {
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            action: activation.action.clone(),
            occurrence: observed_occurrence,
            activation_sequence,
            actors: vec![checkpoint.actor.to_owned()],
            recovery_class: None,
        });
        Ok(QaFaultDirective::Activate(QaFaultActivationDirective {
            activation: activation.clone(),
            actor: checkpoint.actor.to_owned(),
            observed_occurrence,
            activation_sequence,
        }))
    }

    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        let mut state = self.state.lock().map_err(|_| QaFaultProbeError::StatePoisoned)?;
        let record = state
            .records
            .iter_mut()
            .find(|record| record.activation_id == activation_id)
            .ok_or_else(|| QaFaultProbeError::ActivationNotObserved(activation_id.to_owned()))?;
        let recovery_supported = qa_fault_point_descriptor(record.point_id.as_str())
            .is_some_and(|descriptor| descriptor.supports_recovery(recovery_class));
        if !recovery_supported {
            return Err(QaFaultProbeError::UnsupportedRecoveryClass {
                activation_id: activation_id.to_owned(),
                recovery_class,
            });
        }
        if record.recovery_class.is_some() {
            return Err(QaFaultProbeError::RecoveryAlreadyRecorded(activation_id.to_owned()));
        }
        record.recovery_class = Some(recovery_class);
        Ok(())
    }

    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        self.state
            .lock()
            .map(|state| state.records.clone())
            .map_err(|_| QaFaultProbeError::StatePoisoned)
    }

    fn active_barriers(&self) -> Result<Vec<QaFaultActiveBarrier>, QaFaultProbeError> {
        let state = self.state.lock().map_err(|_| QaFaultProbeError::StatePoisoned)?;
        let scheduler = self.scheduler();
        state
            .records
            .iter()
            .filter(|record| record.recovery_class.is_none())
            .filter_map(|record| {
                let QaFaultAction::Barrier { participants } = &record.action else {
                    return None;
                };
                let participants = *participants;
                let release_order = (record.actors.len() == usize::from(participants))
                    .then(|| {
                        self.plan
                            .activations
                            .iter()
                            .find(|activation| activation.id == record.activation_id)
                            .ok_or_else(|| {
                                QaFaultProbeError::ActivationNotObserved(
                                    record.activation_id.clone(),
                                )
                            })
                            .and_then(|activation| {
                                scheduler
                                    .release_order(activation, record.actors.as_slice())
                                    .map_err(|_| {
                                        QaFaultProbeError::AdapterFailure(
                                            "qa_fault.barrier_schedule_failed",
                                        )
                                    })
                            })
                    })
                    .transpose();
                Some(release_order.map(|release_order| {
                    QaFaultActiveBarrier {
                        activation_id: record.activation_id.clone(),
                        point_id: record.point_id.clone(),
                        participants,
                        actors: record.actors.clone(),
                        release_order,
                        released_actors: state
                            .barrier_releases
                            .get(record.activation_id.as_str())
                            .cloned()
                            .unwrap_or_default(),
                    }
                }))
            })
            .collect()
    }
}

/// Synchronous probe/controller failure.
#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum QaFaultProbeError {
    /// Adapter supplied a point absent from the versioned registry.
    #[error("unknown QA fault point `{0}`")]
    UnknownPoint(String),
    /// Actor label violated the bounded non-secret contract.
    #[error("QA fault actor must be a bounded ASCII identifier")]
    InvalidActor,
    /// Per-actor occurrence exceeded its integer representation.
    #[error("QA fault occurrence counter overflowed")]
    OccurrenceOverflow,
    /// Distinct activation ordering exceeded its integer representation.
    #[error("QA fault activation sequence overflowed")]
    ActivationSequenceOverflow,
    /// A previous panic poisoned the isolated controller lock.
    #[error("QA fault controller state lock was poisoned")]
    StatePoisoned,
    /// A host adapter could not durably apply the probe contract.
    #[error("QA fault adapter failed with reason `{0}`")]
    AdapterFailure(&'static str),
    /// Recovery was reported before its activation was observed.
    #[error("QA fault activation `{0}` has not been observed")]
    ActivationNotObserved(String),
    /// The activated point cannot prove the requested recovery class.
    #[error(
        "QA fault activation `{activation_id}` does not support recovery class `{recovery_class:?}`"
    )]
    UnsupportedRecoveryClass { activation_id: String, recovery_class: QaFaultRecoveryClass },
    /// Recovery was reported more than once for an activation.
    #[error("QA fault activation `{0}` already has recovery evidence")]
    RecoveryAlreadyRecorded(String),
}

/// Stateless seeded scheduler for deterministic barrier release order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeterministicQaFaultScheduler {
    seed: u64,
}

impl DeterministicQaFaultScheduler {
    /// Builds a scheduler from a plan reproduction seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Returns the configured reproduction seed.
    #[must_use]
    pub const fn seed(self) -> u64 {
        self.seed
    }

    /// Orders all declared actors for one barrier activation.
    ///
    /// # Errors
    /// Returns an error for non-barrier actions, invalid or duplicate actors,
    /// or a participant count that differs from the plan.
    pub fn release_order(
        self,
        activation: &QaFaultActivation,
        actors: &[String],
    ) -> Result<Vec<String>, QaFaultScheduleError> {
        let QaFaultAction::Barrier { participants } = activation.action else {
            return Err(QaFaultScheduleError::ActionIsNotBarrier);
        };
        if actors.len() != usize::from(participants) {
            return Err(QaFaultScheduleError::ParticipantCountMismatch {
                expected: participants,
                actual: actors.len(),
            });
        }
        let mut unique = BTreeSet::new();
        let mut ranked = Vec::with_capacity(actors.len());
        for actor in actors {
            validate_schedule_actor(actor)?;
            if !unique.insert(actor.as_str()) {
                return Err(QaFaultScheduleError::DuplicateActor(actor.clone()));
            }
            let mut hasher = Sha256::new();
            hasher.update(self.seed.to_be_bytes());
            update_length_delimited_hash(&mut hasher, activation.id.as_bytes());
            update_length_delimited_hash(&mut hasher, actor.as_bytes());
            ranked.push((hasher.finalize().to_vec(), actor.clone()));
        }
        ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        Ok(ranked.into_iter().map(|(_, actor)| actor).collect())
    }
}

/// Invalid deterministic barrier scheduling input.
#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum QaFaultScheduleError {
    /// The caller requested scheduling for a non-barrier action.
    #[error("fault activation action is not a barrier")]
    ActionIsNotBarrier,
    /// Supplied actor count differs from the planned barrier count.
    #[error("barrier expected {expected} participants, got {actual}")]
    ParticipantCountMismatch { expected: u16, actual: usize },
    /// An actor violated the bounded non-secret label contract.
    #[error("barrier actor must be a bounded ASCII identifier")]
    InvalidActor,
    /// The same actor was supplied more than once.
    #[error("barrier actor `{0}` appears more than once")]
    DuplicateActor(String),
}

fn validate_actor(actor: &str) -> Result<(), QaFaultProbeError> {
    if is_bounded_actor(actor) {
        Ok(())
    } else {
        Err(QaFaultProbeError::InvalidActor)
    }
}

fn validate_schedule_actor(actor: &str) -> Result<(), QaFaultScheduleError> {
    if is_bounded_actor(actor) {
        Ok(())
    } else {
        Err(QaFaultScheduleError::InvalidActor)
    }
}
