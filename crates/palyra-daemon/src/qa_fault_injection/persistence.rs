//! Durable QA fault evidence replay and append-only sidecar persistence.
//!
//! Every activation and recovery record is bounded and synced before adapters act.
//! Loader authentication remains isolated in `loader`.

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write as _,
    path::Path,
};

use palyra_common::qa_fault_injection::{
    parse_qa_fault_evidence_sidecar_ndjson, validate_qa_fault_evidence_campaign_before_launch,
    QaFaultAction, QaFaultActivationDirective, QaFaultBarrierJoinedRecord,
    QaFaultBarrierReleasedRecord, QaFaultCheckpointObservedRecord, QaFaultEvidenceSidecarRecord,
    QaFaultLaunchDocument, QaFaultLaunchLoadedRecord, QaFaultRecoveryClass,
    QaFaultRecoveryRecordedRecord, QaFaultRuleActivatedRecord, QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS, QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORD_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
};

use super::{
    loader::{activation_error, read_bounded_file, INVALID_ACTIVATION_REASON_CODE},
    DurableEvidenceSnapshot, QaFaultActivationError, QaFaultEvidenceState,
};

pub(super) fn read_durable_evidence(
    evidence_path: &Path,
    launch: &QaFaultLaunchDocument,
    plan: &palyra_common::qa_fault_injection::QaFaultInjectionPlan,
    before_current_launch: bool,
) -> Result<DurableEvidenceSnapshot, QaFaultActivationError> {
    let max_evidence_bytes = u64::try_from(QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES).unwrap_or(u64::MAX);
    let bytes = read_bounded_file(evidence_path, max_evidence_bytes, "evidence sidecar")?;
    let sidecar = if before_current_launch {
        validate_qa_fault_evidence_campaign_before_launch(bytes.as_slice(), launch, plan)
    } else {
        parse_qa_fault_evidence_sidecar_ndjson(bytes.as_slice(), launch, plan)
    }
    .map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to validate QA fault evidence sidecar: {error}"),
        )
    })?;
    let controller_resume_state = sidecar.controller_resume_state();
    let mut snapshot = DurableEvidenceSnapshot {
        record_count: sidecar.records().len(),
        controller_resume_state,
        ..DurableEvidenceSnapshot::default()
    };
    for record in sidecar.records() {
        match record {
            QaFaultEvidenceSidecarRecord::LaunchLoaded(loaded) => {
                snapshot.launch_ids.insert(loaded.launch_id.clone());
            }
            QaFaultEvidenceSidecarRecord::CheckpointObserved(observed) => {
                update_observed_occurrence(
                    &mut snapshot.observed_occurrences,
                    observed.point_id.as_str(),
                    observed.actor.as_str(),
                    observed.occurrence,
                );
            }
            QaFaultEvidenceSidecarRecord::BarrierJoined(joined) => {
                snapshot
                    .barrier_joins
                    .entry(joined.activation_id.clone())
                    .or_default()
                    .push(joined.actor.clone());
                snapshot
                    .barrier_join_points
                    .entry(joined.activation_id.clone())
                    .or_insert_with(|| joined.point_id.clone());
                update_observed_occurrence(
                    &mut snapshot.observed_occurrences,
                    joined.point_id.as_str(),
                    joined.actor.as_str(),
                    joined.occurrence,
                );
            }
            QaFaultEvidenceSidecarRecord::RuleActivated(activated) => {
                snapshot
                    .activated_rules
                    .entry(activated.activation_id.clone())
                    .or_insert_with(|| activated.point_id.clone());
                snapshot
                    .activation_actors
                    .entry(activated.activation_id.clone())
                    .or_insert_with(|| activated.actors.clone());
                if matches!(activated.action, QaFaultAction::Barrier { .. }) {
                    snapshot
                        .barrier_release_orders
                        .entry(activated.activation_id.clone())
                        .or_insert_with(|| activated.release_order.clone());
                }
                snapshot.highest_activation_sequence =
                    snapshot.highest_activation_sequence.max(activated.activation_sequence);
                for actor in &activated.actors {
                    update_observed_occurrence(
                        &mut snapshot.observed_occurrences,
                        activated.point_id.as_str(),
                        actor.as_str(),
                        activated.occurrence,
                    );
                }
            }
            QaFaultEvidenceSidecarRecord::BarrierReleased(released) => {
                snapshot
                    .barrier_releases
                    .entry(released.activation_id.clone())
                    .or_default()
                    .push(released.actor.clone());
            }
            QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery) => {
                snapshot.recovered_rule_ids.insert(recovery.activation_id.clone());
            }
        }
    }
    Ok(snapshot)
}

fn update_observed_occurrence(
    occurrences: &mut BTreeMap<(String, String), u32>,
    point_id: &str,
    actor: &str,
    occurrence: u32,
) {
    occurrences
        .entry((point_id.to_owned(), actor.to_owned()))
        .and_modify(|observed| *observed = (*observed).max(occurrence))
        .or_insert(occurrence);
}

pub(super) fn append_loaded_record(
    evidence_path: &Path,
    launch: &QaFaultLaunchDocument,
    sequence: u32,
) -> Result<(), QaFaultActivationError> {
    let record = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence,
        launch_id: launch.launch_id.clone(),
        plan_sha256: launch.plan_sha256.clone(),
        capability_sha256: launch.capability_sha256.clone(),
    });
    append_evidence_record(evidence_path, &record)
}

pub(super) fn append_checkpoint_observed_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    point_id: &str,
    actor: &str,
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    let target_occurrence = evidence
        .occurrence_targets
        .iter()
        .filter(|((target_point, target_actor), _)| {
            target_point == point_id && target_actor.as_deref().is_none_or(|target| target == actor)
        })
        .map(|(_, occurrence)| *occurrence)
        .max();
    let Some(target_occurrence) = target_occurrence else {
        return Ok(());
    };
    let key = (point_id.to_owned(), actor.to_owned());
    let current_occurrence = evidence.observed_occurrences.get(&key).copied().unwrap_or_default();
    if current_occurrence >= target_occurrence {
        return Ok(());
    }
    let occurrence = current_occurrence.checked_add(1).ok_or_else(|| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("checkpoint occurrence overflowed for {point_id} and actor {actor}"),
        )
    })?;
    let record =
        QaFaultEvidenceSidecarRecord::CheckpointObserved(QaFaultCheckpointObservedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: evidence.next_sequence,
            launch_id: evidence.launch.launch_id.clone(),
            plan_sha256: evidence.launch.plan_sha256.clone(),
            point_id: point_id.to_owned(),
            actor: actor.to_owned(),
            occurrence,
        });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence.observed_occurrences.insert(key, occurrence);
    Ok(())
}

pub(super) fn append_barrier_join_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    directive: &QaFaultActivationDirective,
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    if evidence
        .barrier_joins
        .get(directive.activation.id.as_str())
        .is_some_and(|actors| actors.iter().any(|actor| actor == &directive.actor))
    {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!(
                "actor {} already joined barrier activation {}",
                directive.actor, directive.activation.id
            ),
        ));
    }
    let record = QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: evidence.next_sequence,
        launch_id: evidence.launch.launch_id.clone(),
        plan_sha256: evidence.launch.plan_sha256.clone(),
        activation_id: directive.activation.id.clone(),
        point_id: directive.activation.point_id.clone(),
        actor: directive.actor.clone(),
        occurrence: directive.observed_occurrence,
    });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence
        .barrier_joins
        .entry(directive.activation.id.clone())
        .or_default()
        .push(directive.actor.clone());
    evidence
        .barrier_join_points
        .entry(directive.activation.id.clone())
        .or_insert_with(|| directive.activation.point_id.clone());
    evidence.observed_occurrences.insert(
        (directive.activation.point_id.clone(), directive.actor.clone()),
        directive.observed_occurrence,
    );
    Ok(())
}

pub(super) fn append_barrier_activation_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    directive: &QaFaultActivationDirective,
    actors: &[String],
    release_order: &[String],
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    if evidence.activated_rules.contains_key(directive.activation.id.as_str()) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!(
                "barrier activation {} already has durable activation evidence",
                directive.activation.id
            ),
        ));
    }
    let QaFaultAction::Barrier { participants } = directive.activation.action else {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "barrier evidence append received a non-barrier directive",
        ));
    };
    if actors.len() != usize::from(participants) || release_order.len() != actors.len() {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!(
                "barrier activation {} has incomplete actor or release-order evidence",
                directive.activation.id
            ),
        ));
    }
    let record = QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: evidence.next_sequence,
        launch_id: evidence.launch.launch_id.clone(),
        plan_sha256: evidence.launch.plan_sha256.clone(),
        activation_id: directive.activation.id.clone(),
        point_id: directive.activation.point_id.clone(),
        actors: actors.to_vec(),
        occurrence: directive.observed_occurrence,
        action: directive.activation.action.clone(),
        activation_sequence: directive.activation_sequence,
        release_order: release_order.to_vec(),
    });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence
        .activated_rules
        .insert(directive.activation.id.clone(), directive.activation.point_id.clone());
    evidence.activation_actors.insert(directive.activation.id.clone(), actors.to_vec());
    evidence.barrier_release_orders.insert(directive.activation.id.clone(), release_order.to_vec());
    Ok(())
}

pub(super) fn append_barrier_release_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    activation_id: &str,
    point_id: &str,
    actor: &str,
    release_position: u16,
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    if evidence.activated_rules.get(activation_id).map(String::as_str) != Some(point_id) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("barrier {activation_id} has no matching durable activation evidence"),
        ));
    }
    let release_order = evidence.barrier_release_orders.get(activation_id).ok_or_else(|| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("barrier {activation_id} has no durable release order"),
        )
    })?;
    let releases = evidence.barrier_releases.get(activation_id);
    let expected_position =
        u16::try_from(releases.map_or(0, Vec::len).saturating_add(1)).map_err(|_| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("barrier {activation_id} release position exceeds the supported range"),
            )
        })?;
    let expected_actor = usize::from(release_position)
        .checked_sub(1)
        .and_then(|position| release_order.get(position));
    if release_position != expected_position || expected_actor.map(String::as_str) != Some(actor) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("barrier {activation_id} release does not match the next seeded actor"),
        ));
    }
    let record = QaFaultEvidenceSidecarRecord::BarrierReleased(QaFaultBarrierReleasedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: evidence.next_sequence,
        launch_id: evidence.launch.launch_id.clone(),
        plan_sha256: evidence.launch.plan_sha256.clone(),
        activation_id: activation_id.to_owned(),
        point_id: point_id.to_owned(),
        actor: actor.to_owned(),
        release_position,
    });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence.barrier_releases.entry(activation_id.to_owned()).or_default().push(actor.to_owned());
    Ok(())
}

pub(super) fn append_activation_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    directive: &QaFaultActivationDirective,
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    if evidence.activated_rules.contains_key(directive.activation.id.as_str()) {
        return Ok(());
    }
    if matches!(directive.activation.action, QaFaultAction::Barrier { .. }) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "daemon barrier activation requires coordinated participant release evidence",
        ));
    }
    let record = QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: evidence.next_sequence,
        launch_id: evidence.launch.launch_id.clone(),
        plan_sha256: evidence.launch.plan_sha256.clone(),
        activation_id: directive.activation.id.clone(),
        point_id: directive.activation.point_id.clone(),
        actors: vec![directive.actor.clone()],
        occurrence: directive.observed_occurrence,
        action: directive.activation.action.clone(),
        activation_sequence: directive.activation_sequence,
        release_order: vec![directive.actor.clone()],
    });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence
        .activated_rules
        .insert(directive.activation.id.clone(), directive.activation.point_id.clone());
    evidence
        .activation_actors
        .insert(directive.activation.id.clone(), vec![directive.actor.clone()]);
    evidence.observed_occurrences.insert(
        (directive.activation.point_id.clone(), directive.actor.clone()),
        directive.observed_occurrence,
    );
    Ok(())
}

pub(super) fn append_recovery_record(
    evidence: &std::sync::Arc<std::sync::Mutex<QaFaultEvidenceState>>,
    activation_id: &str,
    recovery_class: QaFaultRecoveryClass,
    reason_code: &str,
) -> Result<(), QaFaultActivationError> {
    let mut evidence = evidence.lock().map_err(|_| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence state lock was poisoned",
        )
    })?;
    let point_id = evidence.activated_rules.get(activation_id).ok_or_else(|| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("cannot record recovery for unactivated rule {activation_id}"),
        )
    })?;
    let recovery_supported = palyra_common::qa_fault_injection::qa_fault_point_descriptor(point_id)
        .is_some_and(|descriptor| descriptor.supports_recovery(recovery_class));
    if !recovery_supported || evidence.recovered_rule_ids.contains(activation_id) {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("recovery for rule {activation_id} is unsupported or already recorded"),
        ));
    }
    if let Some(release_order) = evidence.barrier_release_orders.get(activation_id) {
        let released = evidence.barrier_releases.get(activation_id).map_or(0, Vec::len);
        if released != release_order.len() {
            return Err(activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!(
                    "barrier {activation_id} cannot recover before every durable release is consumed"
                ),
            ));
        }
    }
    let record = QaFaultEvidenceSidecarRecord::RecoveryRecorded(QaFaultRecoveryRecordedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: evidence.next_sequence,
        launch_id: evidence.launch.launch_id.clone(),
        plan_sha256: evidence.launch.plan_sha256.clone(),
        activation_id: activation_id.to_owned(),
        recovery_class,
        reason_code: reason_code.to_owned(),
    });
    append_evidence_record(evidence.path.as_path(), &record)?;
    evidence.next_sequence = evidence.next_sequence.saturating_add(1);
    evidence.recovered_rule_ids.insert(activation_id.to_owned());
    Ok(())
}

pub(super) fn append_evidence_record(
    evidence_path: &Path,
    record: &QaFaultEvidenceSidecarRecord,
) -> Result<(), QaFaultActivationError> {
    let sequence = match record {
        QaFaultEvidenceSidecarRecord::LaunchLoaded(record) => record.sequence,
        QaFaultEvidenceSidecarRecord::CheckpointObserved(record) => record.sequence,
        QaFaultEvidenceSidecarRecord::BarrierJoined(record) => record.sequence,
        QaFaultEvidenceSidecarRecord::RuleActivated(record) => record.sequence,
        QaFaultEvidenceSidecarRecord::BarrierReleased(record) => record.sequence,
        QaFaultEvidenceSidecarRecord::RecoveryRecorded(record) => record.sequence,
    };
    if usize::try_from(sequence).unwrap_or(usize::MAX) > QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence sidecar would exceed its record budget",
        ));
    }
    let encoded = serde_json::to_vec(record).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to serialize QA fault evidence: {error}"),
        )
    })?;
    if encoded.is_empty() || encoded.len() > QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORD_BYTES {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "serialized QA fault evidence record exceeds its line budget",
        ));
    }
    let existing_len = fs::metadata(evidence_path)
        .map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("failed to inspect QA fault evidence before append: {error}"),
            )
        })?
        .len();
    let appended_len = u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX);
    if existing_len.saturating_add(appended_len)
        > u64::try_from(QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES).unwrap_or(u64::MAX)
    {
        return Err(activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            "QA fault evidence sidecar would exceed its byte budget",
        ));
    }
    let mut file = OpenOptions::new().append(true).open(evidence_path).map_err(|error| {
        activation_error(
            INVALID_ACTIVATION_REASON_CODE,
            format!("failed to open QA fault evidence sidecar for append: {error}"),
        )
    })?;
    file.write_all(encoded.as_slice())
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_data())
        .map_err(|error| {
            activation_error(
                INVALID_ACTIVATION_REASON_CODE,
                format!("failed to durably append QA fault evidence: {error}"),
            )
        })
}
