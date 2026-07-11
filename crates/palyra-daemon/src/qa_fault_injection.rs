//! Test-build-only fault-injection activation boundary.
//!
//! Default daemon builds cannot activate fault injection. Feature builds
//! require a short-lived, owner-only launch document and a separate random
//! capability file, both confined below the daemon state root.

use std::fmt;

#[cfg(not(feature = "qa-fault-injection"))]
use std::path::Path;

#[cfg(feature = "qa-fault-injection")]
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use palyra_common::qa_fault_injection::{
    qa_fault_point_descriptor, QaFaultActivationDirective, QaFaultCheckpoint, QaFaultDirective,
    QaFaultProbeHandle, QaFaultRecoveryClass,
};
#[cfg(all(test, feature = "qa-fault-injection"))]
use palyra_common::qa_fault_injection::{
    DeterministicQaFaultController, QaFaultEvidenceSidecarRecord, QaFaultLaunchLoadedRecord,
    QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
};
#[cfg(feature = "qa-fault-injection")]
use palyra_common::qa_fault_injection::{
    DeterministicQaFaultScheduler, QaFaultAction, QaFaultActiveBarrier, QaFaultControllerRecord,
    QaFaultControllerResumeState, QaFaultInjectionPlan, QaFaultLaunchDocument, QaFaultProbe,
    QaFaultProbeError, QA_FAULT_TERMINATE_EXIT_CODE,
};
pub(crate) use palyra_common::qa_fault_injection::{
    QA_FAULT_CAPABILITY_PATH_ENV, QA_FAULT_LAUNCH_PATH_ENV,
};

#[cfg(not(feature = "qa-fault-injection"))]
const FEATURE_DISABLED_REASON_CODE: &str = "qa_fault.feature_disabled";

#[derive(Debug)]
pub(crate) struct QaFaultActivationError {
    reason_code: &'static str,
    message: String,
}

impl QaFaultActivationError {
    fn new(reason_code: &'static str, message: impl Into<String>) -> Self {
        Self { reason_code, message: message.into() }
    }
}

impl fmt::Display for QaFaultActivationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.reason_code, self.message)
    }
}

impl std::error::Error for QaFaultActivationError {}

#[derive(Debug)]
#[cfg(feature = "qa-fault-injection")]
struct QaFaultEvidenceState {
    launch: QaFaultLaunchDocument,
    path: PathBuf,
    next_sequence: u32,
    activated_rules: BTreeMap<String, String>,
    activation_actors: BTreeMap<String, Vec<String>>,
    barrier_joins: BTreeMap<String, Vec<String>>,
    barrier_join_points: BTreeMap<String, String>,
    barrier_participants: BTreeMap<String, u16>,
    barrier_release_orders: BTreeMap<String, Vec<String>>,
    barrier_releases: BTreeMap<String, Vec<String>>,
    observed_occurrences: BTreeMap<(String, String), u32>,
    occurrence_targets: BTreeMap<(String, Option<String>), u32>,
    recovered_rule_ids: BTreeSet<String>,
}

#[cfg(feature = "qa-fault-injection")]
#[derive(Debug, Default)]
struct DurableEvidenceSnapshot {
    record_count: usize,
    launch_ids: BTreeSet<String>,
    activated_rules: BTreeMap<String, String>,
    activation_actors: BTreeMap<String, Vec<String>>,
    barrier_joins: BTreeMap<String, Vec<String>>,
    barrier_join_points: BTreeMap<String, String>,
    barrier_release_orders: BTreeMap<String, Vec<String>>,
    barrier_releases: BTreeMap<String, Vec<String>>,
    observed_occurrences: BTreeMap<(String, String), u32>,
    recovered_rule_ids: BTreeSet<String>,
    highest_activation_sequence: u32,
    controller_resume_state: QaFaultControllerResumeState,
}

#[cfg(feature = "qa-fault-injection")]
#[derive(Debug)]
struct QaFaultBarrierCoordinator {
    seed: u64,
    states: Mutex<BTreeMap<String, QaFaultBarrierState>>,
}

#[cfg(feature = "qa-fault-injection")]
#[derive(Debug)]
struct QaFaultBarrierState {
    point_id: String,
    participants: u16,
    actors: Vec<String>,
    release_order: Option<Vec<String>>,
    next_release: usize,
}

/// Explicit daemon dependency used at every fault-capable boundary.
///
/// The default value wraps the shared disabled probe and owns no evidence
/// path. Active construction is private to the authenticated feature loader.
#[derive(Clone, Debug, Default)]
pub(crate) struct QaFaultRuntime {
    probe: QaFaultProbeHandle,
    #[cfg(feature = "qa-fault-injection")]
    evidence: Option<Arc<Mutex<QaFaultEvidenceState>>>,
    #[cfg(feature = "qa-fault-injection")]
    barriers: Option<Arc<QaFaultBarrierCoordinator>>,
    #[cfg(feature = "qa-fault-injection")]
    failure_reason: Arc<Mutex<Option<&'static str>>>,
}

impl QaFaultRuntime {
    #[cfg(feature = "qa-fault-injection")]
    fn active(probe: QaFaultProbeHandle, evidence: QaFaultEvidenceState, seed: u64) -> Self {
        let barrier_states = evidence
            .barrier_joins
            .iter()
            .filter(|(activation_id, _)| {
                !evidence.recovered_rule_ids.contains(activation_id.as_str())
            })
            .filter_map(|(activation_id, actors)| {
                Some((
                    activation_id.clone(),
                    QaFaultBarrierState {
                        point_id: evidence.barrier_join_points.get(activation_id)?.clone(),
                        participants: *evidence.barrier_participants.get(activation_id)?,
                        actors: actors.clone(),
                        release_order: evidence.barrier_release_orders.get(activation_id).cloned(),
                        next_release: evidence
                            .barrier_releases
                            .get(activation_id)
                            .map_or(0, Vec::len),
                    },
                ))
            })
            .collect();
        Self {
            probe,
            evidence: Some(Arc::new(Mutex::new(evidence))),
            barriers: Some(Arc::new(QaFaultBarrierCoordinator {
                seed,
                states: Mutex::new(barrier_states),
            })),
            failure_reason: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(all(test, feature = "qa-fault-injection"))]
    pub(crate) fn active_for_test(
        plan: QaFaultInjectionPlan,
        launch: QaFaultLaunchDocument,
        evidence_path: PathBuf,
    ) -> Result<Self, QaFaultActivationError> {
        let loaded = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: launch.launch_id.clone(),
            plan_sha256: launch.plan_sha256.clone(),
            capability_sha256: launch.capability_sha256.clone(),
        });
        let mut initial_evidence = serde_json::to_vec(&loaded).map_err(|error| {
            QaFaultActivationError::new(
                "qa_fault.test_evidence_serialize_failed",
                format!("failed to serialize test launch evidence: {error}"),
            )
        })?;
        initial_evidence.push(b'\n');
        std::fs::write(evidence_path.as_path(), initial_evidence).map_err(|error| {
            QaFaultActivationError::new(
                "qa_fault.test_evidence_write_failed",
                format!("failed to write test launch evidence: {error}"),
            )
        })?;
        let occurrence_targets = occurrence_targets(&plan);
        let controller = DeterministicQaFaultController::new(plan.clone()).map_err(|error| {
            QaFaultActivationError::new(
                "qa_fault.test_controller_invalid",
                format!("failed to initialize test controller: {error}"),
            )
        })?;
        Ok(Self::active(
            QaFaultProbeHandle::from_probe(controller),
            QaFaultEvidenceState {
                launch,
                path: evidence_path,
                next_sequence: 2,
                activated_rules: BTreeMap::new(),
                activation_actors: BTreeMap::new(),
                barrier_joins: BTreeMap::new(),
                barrier_join_points: BTreeMap::new(),
                barrier_participants: BTreeMap::new(),
                barrier_release_orders: BTreeMap::new(),
                barrier_releases: BTreeMap::new(),
                observed_occurrences: BTreeMap::new(),
                occurrence_targets,
                recovered_rule_ids: BTreeSet::new(),
            },
            plan.seed,
        ))
    }

    /// Evaluates a registered checkpoint and durably records activation
    /// evidence before returning any non-continue directive to its adapter.
    ///
    /// # Errors
    /// Returns a stable adapter error when the shared controller or durable
    /// evidence append fails.
    pub(crate) fn checkpoint(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<QaFaultDirective, QaFaultActivationError> {
        #[cfg(feature = "qa-fault-injection")]
        self.ensure_operational()?;
        #[cfg(feature = "qa-fault-injection")]
        if self.admit_barrier_retry(point_id, actor)? {
            return Ok(QaFaultDirective::Continue);
        }
        let directive =
            self.probe.checkpoint(QaFaultCheckpoint { point_id, actor }).map_err(|error| {
                QaFaultActivationError::new(
                    "qa_fault.checkpoint_failed",
                    format!("fault checkpoint {point_id} failed: {error}"),
                )
            })?;
        if let QaFaultDirective::Activate(activation) = &directive {
            #[cfg(feature = "qa-fault-injection")]
            if matches!(activation.activation.action, QaFaultAction::Barrier { .. }) {
                if let Err(error) = self.coordinate_barrier_activation(activation) {
                    self.latch_failure(error.reason_code);
                    return Err(error);
                }
                return Ok(directive);
            }
            #[cfg(feature = "qa-fault-injection")]
            if let Err(error) = self.record_activation(activation) {
                self.latch_failure(error.reason_code);
                return Err(error);
            }
            #[cfg(not(feature = "qa-fault-injection"))]
            self.record_activation(activation)?;
        }
        #[cfg(feature = "qa-fault-injection")]
        if matches!(&directive, QaFaultDirective::Continue) {
            if let Err(error) = self.record_nonactivating_checkpoint(point_id, actor) {
                self.latch_failure(error.reason_code);
                return Err(error);
            }
        }
        Ok(directive)
    }

    /// Records the deterministic outcome of a non-terminating injected action.
    ///
    /// # Errors
    /// Returns a stable adapter error when the point has no truthful immediate
    /// classification, or when controller/evidence persistence fails.
    pub(crate) fn record_immediate_recovery(
        &self,
        directive: &QaFaultActivationDirective,
    ) -> Result<(), QaFaultActivationError> {
        let point_id = directive.activation.point_id.as_str();
        let recovery_class = immediate_recovery_class(point_id).ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.recovery_unclassified",
                format!("fault point {point_id} has no immediate recovery classification"),
            )
        })?;
        self.record_verified_recovery(
            directive,
            recovery_class,
            "qa_fault.immediate_adapter_outcome_classified",
        )
    }

    /// Persists a recovery outcome only after the owning adapter has completed its proof.
    ///
    /// # Errors
    /// Returns a stable error when the point does not support the claimed class or persistence
    /// fails. Callers must finish the subsystem action before invoking this method.
    pub(crate) fn record_verified_recovery(
        &self,
        directive: &QaFaultActivationDirective,
        recovery_class: QaFaultRecoveryClass,
        reason_code: &'static str,
    ) -> Result<(), QaFaultActivationError> {
        let point_id = directive.activation.point_id.as_str();
        let descriptor = qa_fault_point_descriptor(point_id).ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.recovery_unclassified",
                format!("fault point {point_id} is absent from the registry"),
            )
        })?;
        if !descriptor.supports_recovery(recovery_class) {
            return Err(QaFaultActivationError::new(
                "qa_fault.recovery_class_unsupported",
                format!(
                    "fault point {point_id} does not support recovery class {}",
                    recovery_class.as_str()
                ),
            ));
        }
        self.record_recovery(directive.activation.id.as_str(), recovery_class, reason_code)
    }

    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
        reason_code: &'static str,
    ) -> Result<(), QaFaultActivationError> {
        #[cfg(not(feature = "qa-fault-injection"))]
        let _ = reason_code;
        #[cfg(feature = "qa-fault-injection")]
        self.ensure_operational()?;
        let controller_has_activation = self
            .probe
            .records()
            .map_err(|error| {
                QaFaultActivationError::new(
                    "qa_fault.recovery_failed",
                    format!("failed to inspect activation {activation_id}: {error}"),
                )
            })?
            .iter()
            .any(|record| record.activation_id == activation_id);
        #[cfg(feature = "qa-fault-injection")]
        if let Some(evidence) = self.evidence.as_ref() {
            if let Err(error) = enabled::append_recovery_record(
                evidence,
                activation_id,
                recovery_class,
                reason_code,
            ) {
                self.latch_failure(error.reason_code);
                return Err(error);
            }
        }
        if controller_has_activation {
            self.probe.record_recovery(activation_id, recovery_class).map_err(|error| {
                QaFaultActivationError::new(
                    "qa_fault.recovery_failed",
                    format!("failed to record recovery for activation {activation_id}: {error}"),
                )
            })?;
        }
        #[cfg(feature = "qa-fault-injection")]
        if let Some(coordinator) = self.barriers.as_ref() {
            coordinator
                .states
                .lock()
                .map_err(|_| {
                    QaFaultActivationError::new(
                        "qa_fault.barrier_state_poisoned",
                        "QA fault barrier state lock was poisoned during recovery",
                    )
                })?
                .remove(activation_id);
        }
        Ok(())
    }

    pub(crate) fn record_pending_recovery_for_point_actor(
        &self,
        point_id: &str,
        actor: &str,
        recovery_class: QaFaultRecoveryClass,
        reason_code: &'static str,
    ) -> Result<bool, QaFaultActivationError> {
        #[cfg(feature = "qa-fault-injection")]
        {
            self.ensure_operational()?;
            let activation_id = if let Some(evidence) = self.evidence.as_ref() {
                let evidence = evidence.lock().map_err(|_| {
                    QaFaultActivationError::new(
                        "qa_fault.recovery_failed",
                        "QA fault evidence state lock was poisoned",
                    )
                })?;
                evidence.activated_rules.iter().find_map(|(activation_id, activated_point)| {
                    (activated_point == point_id
                        && !evidence.recovered_rule_ids.contains(activation_id)
                        && evidence.activation_actors.get(activation_id).is_some_and(|actors| {
                            actors.iter().any(|activated_actor| activated_actor == actor)
                        }))
                    .then(|| activation_id.clone())
                })
            } else {
                None
            };
            if let Some(activation_id) = activation_id {
                self.record_recovery(activation_id.as_str(), recovery_class, reason_code)?;
                return Ok(true);
            }
        }
        let _ = (point_id, actor, recovery_class, reason_code);
        Ok(false)
    }

    /// Returns unique actors for unrecovered activations at one registered point.
    ///
    /// The lookup is evidence-only and deliberately does not infer or record a recovery outcome.
    ///
    /// # Errors
    /// Returns a stable adapter error if active evidence cannot be inspected.
    pub(crate) fn pending_activation_actors_for_point(
        &self,
        point_id: &str,
    ) -> Result<Vec<String>, QaFaultActivationError> {
        #[cfg(feature = "qa-fault-injection")]
        {
            self.ensure_operational()?;
            let Some(evidence) = self.evidence.as_ref() else {
                return Ok(Vec::new());
            };
            let evidence = evidence.lock().map_err(|_| {
                QaFaultActivationError::new(
                    "qa_fault.recovery_failed",
                    "QA fault evidence state lock was poisoned",
                )
            })?;
            let actors = evidence
                .activated_rules
                .iter()
                .filter(|(activation_id, activated_point)| {
                    activated_point.as_str() == point_id
                        && !evidence.recovered_rule_ids.contains(activation_id.as_str())
                })
                .filter_map(|(activation_id, _)| evidence.activation_actors.get(activation_id))
                .flatten()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            Ok(actors)
        }
        #[cfg(not(feature = "qa-fault-injection"))]
        {
            let _ = point_id;
            Ok(Vec::new())
        }
    }

    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn probe_handle(&self) -> QaFaultProbeHandle {
        QaFaultProbeHandle::from_probe(QaFaultRuntimeProbe { runtime: self.clone() })
    }

    #[cfg(feature = "qa-fault-injection")]
    fn coordinate_barrier_activation(
        &self,
        directive: &QaFaultActivationDirective,
    ) -> Result<(), QaFaultActivationError> {
        let QaFaultAction::Barrier { participants } = directive.activation.action else {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_action_invalid",
                "barrier coordinator received a non-barrier directive",
            ));
        };
        let coordinator = self.barriers.as_ref().ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.barrier_unavailable",
                "active fault runtime has no barrier coordinator",
            )
        })?;
        let evidence = self.evidence.as_ref().ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.barrier_evidence_unavailable",
                "active fault runtime has no durable evidence state",
            )
        })?;
        let activation_id = directive.activation.id.as_str();
        let actor = directive.actor.as_str();
        let mut states = coordinator.states.lock().map_err(|_| {
            QaFaultActivationError::new(
                "qa_fault.barrier_state_poisoned",
                "QA fault barrier state lock was poisoned",
            )
        })?;
        let state = states.entry(activation_id.to_owned()).or_insert_with(|| QaFaultBarrierState {
            point_id: directive.activation.point_id.clone(),
            participants,
            actors: Vec::with_capacity(usize::from(participants)),
            release_order: None,
            next_release: 0,
        });
        if state.point_id != directive.activation.point_id || state.participants != participants {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_contract_changed",
                format!("barrier contract changed for activation {activation_id}"),
            ));
        }
        if state.actors.iter().any(|candidate| candidate == actor) {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_duplicate_join",
                format!("actor {actor} already joined barrier activation {activation_id}"),
            ));
        }
        enabled::append_barrier_join_record(evidence, directive)?;
        state.actors.push(actor.to_owned());
        if state.actors.len() > usize::from(participants) {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_participant_overflow",
                format!("barrier activation {activation_id} exceeded its participant count"),
            ));
        }
        if state.actors.len() == usize::from(participants) {
            let release_order = DeterministicQaFaultScheduler::new(coordinator.seed)
                .release_order(&directive.activation, state.actors.as_slice())
                .map_err(|error| {
                    QaFaultActivationError::new(
                        "qa_fault.barrier_schedule_failed",
                        format!("failed to schedule barrier activation {activation_id}: {error}"),
                    )
                })?;
            enabled::append_barrier_activation_record(
                evidence,
                directive,
                state.actors.as_slice(),
                release_order.as_slice(),
            )?;
            state.release_order = Some(release_order);
        }
        Ok(())
    }

    #[cfg(feature = "qa-fault-injection")]
    fn admit_barrier_retry(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<bool, QaFaultActivationError> {
        let Some(coordinator) = self.barriers.as_ref() else {
            return Ok(false);
        };
        let mut states = coordinator.states.lock().map_err(|_| {
            QaFaultActivationError::new(
                "qa_fault.barrier_state_poisoned",
                "QA fault barrier state lock was poisoned",
            )
        })?;
        let Some((activation_id, state)) =
            states.iter_mut().find(|(_, state)| state.point_id == point_id)
        else {
            return Ok(false);
        };
        let Some(actor_position) = state.actors.iter().position(|joined| joined == actor) else {
            if state.actors.len() >= usize::from(state.participants) {
                return Err(QaFaultActivationError::new(
                    "qa_fault.barrier_participant_overflow",
                    format!(
                        "actor {actor} is not a participant in completed barrier {activation_id}"
                    ),
                ));
            }
            return Ok(false);
        };
        let Some(release_order) = state.release_order.as_ref() else {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_waiting_for_participants",
                format!("barrier activation {activation_id} is not complete"),
            ));
        };
        let release_position = release_order
            .iter()
            .position(|released_actor| released_actor == actor)
            .unwrap_or(actor_position);
        if release_position < state.next_release {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_actor_already_released",
                format!("actor {actor} already consumed its release for barrier {activation_id}"),
            ));
        }
        if release_position != state.next_release {
            return Err(QaFaultActivationError::new(
                "qa_fault.barrier_release_not_ready",
                format!("actor {actor} is not next for barrier {activation_id}"),
            ));
        }
        let evidence = self.evidence.as_ref().ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.barrier_evidence_unavailable",
                "active fault runtime has no durable evidence state",
            )
        })?;
        let durable_position = u16::try_from(release_position.saturating_add(1)).map_err(|_| {
            QaFaultActivationError::new(
                "qa_fault.barrier_release_position_invalid",
                format!("barrier release position is too large for {activation_id}"),
            )
        })?;
        if let Err(error) = enabled::append_barrier_release_record(
            evidence,
            activation_id,
            point_id,
            actor,
            durable_position,
        ) {
            self.latch_failure(error.reason_code);
            return Err(error);
        }
        state.next_release = state.next_release.checked_add(1).ok_or_else(|| {
            QaFaultActivationError::new(
                "qa_fault.barrier_release_position_invalid",
                format!("barrier release cursor overflowed for {activation_id}"),
            )
        })?;
        Ok(true)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn ensure_operational(&self) -> Result<(), QaFaultActivationError> {
        let failure_reason = self.failure_reason.lock().map_err(|_| {
            QaFaultActivationError::new(
                "qa_fault.adapter_state_poisoned",
                "QA fault adapter failure latch was poisoned",
            )
        })?;
        if let Some(reason_code) = *failure_reason {
            return Err(QaFaultActivationError::new(
                reason_code,
                "QA fault adapter is latched fail-closed after an earlier durable evidence failure",
            ));
        }
        Ok(())
    }

    #[cfg(feature = "qa-fault-injection")]
    fn latch_failure(&self, reason_code: &'static str) {
        if let Ok(mut failure_reason) = self.failure_reason.lock() {
            failure_reason.get_or_insert(reason_code);
        }
    }

    /// Records only conservative boundary facts for activations left pending by a daemon crash.
    ///
    /// # Errors
    /// Returns a stable evidence error when a durable append fails. Pending points that require
    /// subsystem-specific proof remain unresolved for their owning adapter; startup must never
    /// fabricate resume, cleanup, or deduplication.
    pub(crate) fn record_startup_orphan_recoveries(&self) -> Result<usize, QaFaultActivationError> {
        #[cfg(feature = "qa-fault-injection")]
        if let Some(evidence) = self.evidence.as_ref() {
            let active_barrier_ids = self
                .active_barrier_snapshots()?
                .into_iter()
                .map(|barrier| barrier.activation_id)
                .collect::<BTreeSet<_>>();
            let pending = {
                let evidence = evidence.lock().map_err(|_| {
                    QaFaultActivationError::new(
                        "qa_fault.recovery_failed",
                        "QA fault evidence state lock was poisoned",
                    )
                })?;
                evidence
                    .activated_rules
                    .iter()
                    .filter(|(activation_id, _)| {
                        !evidence.recovered_rule_ids.contains(activation_id.as_str())
                            && !active_barrier_ids.contains(activation_id.as_str())
                    })
                    .map(|(activation_id, point_id)| (activation_id.clone(), point_id.clone()))
                    .collect::<Vec<_>>()
            };
            let mut recorded = 0usize;
            for (activation_id, point_id) in pending {
                let Some(recovery_class) = startup_orphan_recovery_class(point_id.as_str()) else {
                    continue;
                };
                enabled::append_recovery_record(
                    evidence,
                    activation_id.as_str(),
                    recovery_class,
                    "qa_fault.startup_recovery_classified",
                )?;
                recorded = recorded.saturating_add(1);
            }
            return Ok(recorded);
        }
        Ok(0)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn active_barrier_snapshots(
        &self,
    ) -> Result<Vec<QaFaultActiveBarrier>, QaFaultActivationError> {
        self.ensure_operational()?;
        let Some(coordinator) = self.barriers.as_ref() else {
            return Ok(Vec::new());
        };
        let states = coordinator.states.lock().map_err(|_| {
            QaFaultActivationError::new(
                "qa_fault.barrier_state_poisoned",
                "QA fault barrier state lock was poisoned while reading active barriers",
            )
        })?;
        Ok(states
            .iter()
            .map(|(activation_id, state)| {
                let released_actors = state
                    .release_order
                    .as_ref()
                    .map(|order| order.iter().take(state.next_release).cloned().collect())
                    .unwrap_or_default();
                QaFaultActiveBarrier {
                    activation_id: activation_id.clone(),
                    point_id: state.point_id.clone(),
                    participants: state.participants,
                    actors: state.actors.clone(),
                    release_order: state.release_order.clone(),
                    released_actors,
                }
            })
            .collect())
    }

    /// Exits with the closed QA termination code after activation evidence
    /// has already been synced by [`Self::checkpoint`].
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn terminate_process(&self) -> ! {
        std::process::exit(QA_FAULT_TERMINATE_EXIT_CODE)
    }

    fn record_activation(
        &self,
        activation: &QaFaultActivationDirective,
    ) -> Result<(), QaFaultActivationError> {
        #[cfg(feature = "qa-fault-injection")]
        if let Some(evidence) = self.evidence.as_ref() {
            return enabled::append_activation_record(evidence, activation);
        }
        let _ = activation;
        Ok(())
    }

    #[cfg(feature = "qa-fault-injection")]
    fn record_nonactivating_checkpoint(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<(), QaFaultActivationError> {
        let Some(evidence) = self.evidence.as_ref() else {
            return Ok(());
        };
        enabled::append_checkpoint_observed_record(evidence, point_id, actor)
    }
}

#[cfg(feature = "qa-fault-injection")]
#[derive(Clone, Debug)]
struct QaFaultRuntimeProbe {
    runtime: QaFaultRuntime,
}

#[cfg(feature = "qa-fault-injection")]
impl QaFaultProbe for QaFaultRuntimeProbe {
    fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        let directive = self
            .runtime
            .checkpoint(checkpoint.point_id, checkpoint.actor)
            .map_err(qa_fault_probe_adapter_error)?;
        if matches!(
            &directive,
            QaFaultDirective::Activate(activation)
                if matches!(&activation.activation.action, QaFaultAction::TerminateProcess)
        ) {
            self.runtime.terminate_process();
        }
        Ok(directive)
    }

    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        self.runtime
            .record_recovery(
                activation_id,
                recovery_class,
                "qa_fault.subsystem_recovery_classified",
            )
            .map_err(qa_fault_probe_adapter_error)
    }

    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        self.runtime.ensure_operational().map_err(qa_fault_probe_adapter_error)?;
        self.runtime.probe.records()
    }

    fn active_barriers(&self) -> Result<Vec<QaFaultActiveBarrier>, QaFaultProbeError> {
        self.runtime.active_barrier_snapshots().map_err(qa_fault_probe_adapter_error)
    }
}

#[cfg(feature = "qa-fault-injection")]
fn qa_fault_probe_adapter_error(error: QaFaultActivationError) -> QaFaultProbeError {
    QaFaultProbeError::AdapterFailure(error.reason_code)
}

fn immediate_recovery_class(point_id: &str) -> Option<QaFaultRecoveryClass> {
    match point_id {
        "journal.before_effect"
        | "provider.fixture.before_intent"
        | "provider.fixture.before_effect"
        | "managed_process.before_effect"
        | "tool.before_effect" => Some(QaFaultRecoveryClass::FailedClosed),
        "execution_backend.during_cleanup" | "managed_process.during_cleanup" => {
            Some(QaFaultRecoveryClass::CleanupSucceeded)
        }
        "provider.fixture.after_intent" => Some(QaFaultRecoveryClass::FailedClosed),
        "provider.fixture.after_effect_before_ack" => Some(QaFaultRecoveryClass::OutcomeUnknown),
        _ => None,
    }
}

#[cfg(feature = "qa-fault-injection")]
fn occurrence_targets(plan: &QaFaultInjectionPlan) -> BTreeMap<(String, Option<String>), u32> {
    let mut targets = BTreeMap::new();
    for activation in &plan.activations {
        targets
            .entry((activation.point_id.clone(), activation.actor.clone()))
            .and_modify(|occurrence: &mut u32| {
                *occurrence = (*occurrence).max(activation.occurrence);
            })
            .or_insert(activation.occurrence);
    }
    targets
}

#[cfg(feature = "qa-fault-injection")]
fn startup_orphan_recovery_class(point_id: &str) -> Option<QaFaultRecoveryClass> {
    match point_id {
        "journal.before_effect"
        | "provider.fixture.before_intent"
        | "provider.fixture.before_effect"
        | "managed_process.before_effect"
        | "tool.before_effect" => Some(QaFaultRecoveryClass::FailedClosed),
        "provider.fixture.after_effect_before_ack"
        | "run.final_delivery.after_effect_before_ack"
        | "tool.after_effect_before_ack"
        | "managed_process.after_effect_before_ack" => Some(QaFaultRecoveryClass::OutcomeUnknown),
        _ => None,
    }
}

/// Rejects fault-launch environment variables in normal daemon builds.
///
/// Feature builds replace this preflight with the authenticated launch
/// loader. Keeping the feature-off check in the binary prevents a QA runner
/// from mistaking a production-shaped daemon for an injection-capable one.
///
/// # Errors
/// Returns `qa_fault.feature_disabled` when either activation variable is
/// present in a build that cannot execute a fault plan.
#[cfg(not(feature = "qa-fault-injection"))]
pub(crate) fn load_fault_injection(
    _state_root: &Path,
) -> Result<QaFaultRuntime, QaFaultActivationError> {
    let launch_present = std::env::var_os(QA_FAULT_LAUNCH_PATH_ENV).is_some();
    let capability_present = std::env::var_os(QA_FAULT_CAPABILITY_PATH_ENV).is_some();
    if launch_present || capability_present {
        return Err(QaFaultActivationError::new(
            FEATURE_DISABLED_REASON_CODE,
            "fault-injection activation was requested, but palyrad was built without the non-default qa-fault-injection feature",
        ));
    }
    Ok(QaFaultRuntime::default())
}
#[cfg(feature = "qa-fault-injection")]
mod loader;
#[cfg(feature = "qa-fault-injection")]
mod persistence;

#[cfg(feature = "qa-fault-injection")]
mod enabled {
    pub(crate) use super::loader::load_fault_injection;
    #[cfg(test)]
    pub(super) use super::loader::read_bounded_file;
    #[cfg(test)]
    pub(super) use super::persistence::append_evidence_record;
    pub(super) use super::persistence::{
        append_activation_record, append_barrier_activation_record, append_barrier_join_record,
        append_barrier_release_record, append_checkpoint_observed_record, append_recovery_record,
    };
}

#[cfg(feature = "qa-fault-injection")]
pub(crate) use enabled::load_fault_injection;

#[cfg(test)]
mod tests;
