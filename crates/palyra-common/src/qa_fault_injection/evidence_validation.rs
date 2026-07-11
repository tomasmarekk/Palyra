//! Semantic validation for restart-spanning fault-evidence campaigns.
//!
//! Validation is fail-closed because the sidecar is the durable reproduction proof.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    evidence::{
        update_resume_occurrence, QaFaultEvidenceSidecarError, QaFaultEvidenceSidecarIssue,
        QaFaultEvidenceSidecarRecord,
    },
    is_bounded_actor, is_bounded_identifier,
    launch::{is_lowercase_sha256, QaFaultLaunchDocument},
    plan::{qa_fault_point_descriptor, QaFaultAction, QaFaultInjectionPlan},
    DeterministicQaFaultScheduler, QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS, QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORD_BYTES,
    QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION, QA_FAULT_INJECTION_MAX_OCCURRENCE,
};

pub(super) fn decode_fault_evidence_sidecar_records(
    bytes: &[u8],
    allow_empty: bool,
) -> Result<Vec<QaFaultEvidenceSidecarRecord>, QaFaultEvidenceSidecarError> {
    if bytes.len() > QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES {
        return Err(QaFaultEvidenceSidecarError::SidecarTooLarge);
    }
    if bytes.is_empty() && allow_empty {
        return Ok(Vec::new());
    }
    if !bytes.ends_with(b"\n") {
        return Err(QaFaultEvidenceSidecarError::UnterminatedRecord);
    }
    let encoded_records = bytes[..bytes.len().saturating_sub(1)].split(|byte| *byte == b'\n');
    let mut records = Vec::new();
    for (record_index, encoded) in encoded_records.enumerate() {
        if record_index >= QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS {
            return Err(QaFaultEvidenceSidecarError::TooManyRecords);
        }
        if encoded.is_empty() || encoded.len() > QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORD_BYTES {
            return Err(QaFaultEvidenceSidecarError::InvalidRecordSize { record_index });
        }
        let record =
            serde_json::from_slice::<QaFaultEvidenceSidecarRecord>(encoded).map_err(|source| {
                QaFaultEvidenceSidecarError::MalformedRecord { record_index, source }
            })?;
        records.push(record);
    }
    Ok(records)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SidecarCampaignValidationMode {
    BeforeCurrentLaunch,
    CurrentLaunchLoaded,
}

pub(super) fn validate_fault_evidence_sidecar(
    records: &[QaFaultEvidenceSidecarRecord],
    launch: &QaFaultLaunchDocument,
    plan: &QaFaultInjectionPlan,
    mode: SidecarCampaignValidationMode,
) -> Vec<QaFaultEvidenceSidecarIssue> {
    let mut issues = Vec::new();
    if let Err(error) = launch.validate() {
        for issue in error.issues() {
            push_sidecar_issue(
                &mut issues,
                format!("launch_{}", issue.code),
                None,
                issue.message.clone(),
            );
        }
    }
    if let Err(error) = plan.validate() {
        for issue in error.issues() {
            push_sidecar_issue(
                &mut issues,
                format!("plan_{}", issue.code),
                None,
                issue.message.clone(),
            );
        }
    }
    match plan.canonical_sha256() {
        Ok(digest) if digest == launch.plan_sha256 => {}
        Ok(_) => push_sidecar_issue(
            &mut issues,
            "launch_plan_digest_mismatch",
            None,
            "launch plan digest does not match canonical plan content",
        ),
        Err(error) => {
            push_sidecar_issue(&mut issues, "plan_digest_unavailable", None, error.to_string())
        }
    }
    if records.is_empty() {
        if mode == SidecarCampaignValidationMode::CurrentLaunchLoaded {
            push_sidecar_issue(
                &mut issues,
                "missing_launch_loaded",
                None,
                "sidecar must begin with one launch_loaded record",
            );
        }
        return issues;
    }
    if !matches!(records.first(), Some(QaFaultEvidenceSidecarRecord::LaunchLoaded(_))) {
        push_sidecar_issue(
            &mut issues,
            "launch_loaded_not_first",
            Some(0),
            "the first sidecar record must be launch_loaded",
        );
    }

    let planned_by_id = plan
        .activations
        .iter()
        .map(|activation| (activation.id.as_str(), activation))
        .collect::<BTreeMap<_, _>>();
    let mut loaded_launches = BTreeMap::<String, (usize, String)>::new();
    let mut latest_loaded_launch = None;
    let mut activated_ids = BTreeMap::<String, usize>::new();
    let mut activation_sequences = BTreeMap::<u32, String>::new();
    let mut barrier_joins = BTreeMap::<String, BTreeMap<String, usize>>::new();
    let mut barrier_activation_release_orders = BTreeMap::<String, Vec<String>>::new();
    let mut barrier_releases = BTreeMap::<String, Vec<String>>::new();
    let mut checkpoint_occurrences = BTreeMap::<(String, String), u32>::new();
    let mut recoveries = BTreeSet::<String>::new();

    for (record_index, record) in records.iter().enumerate() {
        let expected_sequence = u32::try_from(record_index + 1).unwrap_or(u32::MAX);
        let (schema_version, sequence, record_launch_id, record_plan_sha256) =
            sidecar_record_header(record);
        if schema_version != QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION {
            push_sidecar_issue(
                &mut issues,
                "unsupported_record_schema_version",
                Some(record_index),
                format!(
                    "expected record schema_version {QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION}, got {schema_version}"
                ),
            );
        }
        if sequence != expected_sequence {
            push_sidecar_issue(
                &mut issues,
                "record_sequence_mismatch",
                Some(record_index),
                format!("expected sequence {expected_sequence}, got {sequence}"),
            );
        }
        if record_plan_sha256 != launch.plan_sha256 {
            push_sidecar_issue(
                &mut issues,
                "record_plan_digest_mismatch",
                Some(record_index),
                "record belongs to a different fault plan",
            );
        }
        if !matches!(record, QaFaultEvidenceSidecarRecord::LaunchLoaded(_))
            && latest_loaded_launch != Some(record_launch_id)
        {
            push_sidecar_issue(
                &mut issues,
                "record_launch_not_latest",
                Some(record_index),
                "non-launch records must belong to the most recently loaded launch",
            );
        }

        match record {
            QaFaultEvidenceSidecarRecord::LaunchLoaded(loaded) => {
                if !is_bounded_actor(loaded.launch_id.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_loaded_launch_id",
                        Some(record_index),
                        "loaded launch_id must be a bounded non-secret ASCII identifier",
                    );
                }
                if !is_lowercase_sha256(loaded.capability_sha256.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_loaded_capability_digest",
                        Some(record_index),
                        "loaded capability digest must be lowercase SHA-256",
                    );
                }
                if loaded_launches
                    .insert(
                        loaded.launch_id.clone(),
                        (record_index, loaded.capability_sha256.clone()),
                    )
                    .is_some()
                {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_launch_loaded",
                        Some(record_index),
                        format!("launch `{}` was loaded more than once", loaded.launch_id),
                    );
                }
                latest_loaded_launch = Some(loaded.launch_id.as_str());
                if loaded.launch_id == launch.launch_id {
                    if mode == SidecarCampaignValidationMode::BeforeCurrentLaunch {
                        push_sidecar_issue(
                            &mut issues,
                            "current_launch_replayed",
                            Some(record_index),
                            "current launch_id already exists in the persistent campaign",
                        );
                    } else if loaded.capability_sha256 != launch.capability_sha256 {
                        push_sidecar_issue(
                            &mut issues,
                            "current_capability_digest_mismatch",
                            Some(record_index),
                            "current launch capability digest does not match its launch document",
                        );
                    }
                }
            }
            QaFaultEvidenceSidecarRecord::CheckpointObserved(observed) => {
                if !loaded_launches.contains_key(record_launch_id) {
                    push_sidecar_issue(
                        &mut issues,
                        "checkpoint_observation_launch_not_loaded",
                        Some(record_index),
                        format!(
                            "checkpoint observation references launch `{record_launch_id}` before launch_loaded"
                        ),
                    );
                }
                if qa_fault_point_descriptor(observed.point_id.as_str()).is_none() {
                    push_sidecar_issue(
                        &mut issues,
                        "unknown_observed_checkpoint",
                        Some(record_index),
                        format!("checkpoint `{}` is absent from the registry", observed.point_id),
                    );
                }
                if !is_bounded_actor(observed.actor.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_checkpoint_observation_actor",
                        Some(record_index),
                        "checkpoint observation actor must be a bounded non-secret ASCII identifier",
                    );
                }
                if observed.occurrence == 0
                    || observed.occurrence > QA_FAULT_INJECTION_MAX_OCCURRENCE
                {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_checkpoint_observation_occurrence",
                        Some(record_index),
                        format!(
                            "checkpoint observation occurrence must be in range 1..={QA_FAULT_INJECTION_MAX_OCCURRENCE}"
                        ),
                    );
                }
                let matching_activations = plan.activations.iter().filter(|activation| {
                    activation.point_id == observed.point_id
                        && activation.actor.as_deref().is_none_or(|actor| actor == observed.actor)
                });
                let mut has_future_activation = false;
                let mut matches_activation = false;
                for activation in matching_activations {
                    has_future_activation |= activation.occurrence > observed.occurrence;
                    matches_activation |= activation.occurrence == observed.occurrence;
                }
                if !has_future_activation || matches_activation {
                    push_sidecar_issue(
                        &mut issues,
                        "unplanned_checkpoint_observation",
                        Some(record_index),
                        "non-activating checkpoint evidence must precede a matching planned occurrence",
                    );
                }
                let key = (observed.point_id.clone(), observed.actor.clone());
                let expected_occurrence =
                    checkpoint_occurrences.get(&key).copied().unwrap_or_default().saturating_add(1);
                if observed.occurrence != expected_occurrence {
                    push_sidecar_issue(
                        &mut issues,
                        "checkpoint_observation_sequence_mismatch",
                        Some(record_index),
                        format!(
                            "expected checkpoint occurrence {expected_occurrence}, got {}",
                            observed.occurrence
                        ),
                    );
                }
                checkpoint_occurrences.insert(key, observed.occurrence);
            }
            QaFaultEvidenceSidecarRecord::BarrierJoined(joined) => {
                if !loaded_launches.contains_key(record_launch_id) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_join_launch_not_loaded",
                        Some(record_index),
                        format!(
                            "barrier join references launch `{record_launch_id}` before launch_loaded"
                        ),
                    );
                }
                let Some(planned) = planned_by_id.get(joined.activation_id.as_str()) else {
                    push_sidecar_issue(
                        &mut issues,
                        "unplanned_barrier_join",
                        Some(record_index),
                        format!(
                            "barrier join references unplanned activation `{}`",
                            joined.activation_id
                        ),
                    );
                    continue;
                };
                let QaFaultAction::Barrier { participants } = planned.action else {
                    push_sidecar_issue(
                        &mut issues,
                        "join_for_non_barrier_activation",
                        Some(record_index),
                        format!("activation `{}` is not a barrier", joined.activation_id),
                    );
                    continue;
                };
                if joined.point_id != planned.point_id || joined.occurrence != planned.occurrence {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_join_contract_mismatch",
                        Some(record_index),
                        format!(
                            "barrier join `{}` does not match its planned point or occurrence",
                            joined.activation_id
                        ),
                    );
                }
                if !is_bounded_actor(joined.actor.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_barrier_join_actor",
                        Some(record_index),
                        "barrier join actor must be a bounded non-secret ASCII identifier",
                    );
                }
                if activated_ids.contains_key(joined.activation_id.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_join_after_activation",
                        Some(record_index),
                        format!(
                            "barrier join for `{}` appears after its rule activation",
                            joined.activation_id
                        ),
                    );
                }
                let joins = barrier_joins.entry(joined.activation_id.clone()).or_default();
                if joins.insert(joined.actor.clone(), record_index).is_some() {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_barrier_join",
                        Some(record_index),
                        format!(
                            "actor `{}` joined barrier `{}` more than once",
                            joined.actor, joined.activation_id
                        ),
                    );
                }
                if joins.len() > usize::from(participants) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_join_participant_overflow",
                        Some(record_index),
                        format!(
                            "barrier `{}` exceeds its {participants} planned participants",
                            joined.activation_id
                        ),
                    );
                }
                let occurrence_key = (joined.point_id.clone(), joined.actor.clone());
                let expected_occurrence = checkpoint_occurrences
                    .get(&occurrence_key)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(1);
                if joined.occurrence != expected_occurrence {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_join_occurrence_sequence_mismatch",
                        Some(record_index),
                        format!(
                            "expected barrier checkpoint occurrence {expected_occurrence}, got {}",
                            joined.occurrence
                        ),
                    );
                }
                update_resume_occurrence(
                    &mut checkpoint_occurrences,
                    joined.point_id.as_str(),
                    joined.actor.as_str(),
                    joined.occurrence,
                );
            }
            QaFaultEvidenceSidecarRecord::RuleActivated(activated) => {
                if !loaded_launches.contains_key(record_launch_id) {
                    push_sidecar_issue(
                        &mut issues,
                        "activation_launch_not_loaded",
                        Some(record_index),
                        format!(
                            "activation references launch `{record_launch_id}` before launch_loaded"
                        ),
                    );
                }
                let Some(planned) = planned_by_id.get(activated.activation_id.as_str()) else {
                    push_sidecar_issue(
                        &mut issues,
                        "unplanned_activation",
                        Some(record_index),
                        format!(
                            "activation `{}` is not declared by the fault plan",
                            activated.activation_id
                        ),
                    );
                    continue;
                };
                if activated.point_id != planned.point_id
                    || activated.occurrence != planned.occurrence
                    || activated.action != planned.action
                {
                    push_sidecar_issue(
                        &mut issues,
                        "activation_contract_mismatch",
                        Some(record_index),
                        format!(
                            "activation `{}` does not match its planned point, occurrence, or action",
                            activated.activation_id
                        ),
                    );
                }
                let actor_set =
                    activated.actors.iter().map(String::as_str).collect::<BTreeSet<_>>();
                if activated.actors.is_empty()
                    || actor_set.len() != activated.actors.len()
                    || activated.actors.iter().any(|actor| !is_bounded_actor(actor))
                {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_activation_actors",
                        Some(record_index),
                        "activation actors must be non-empty, unique, and bounded",
                    );
                }
                match planned.action {
                    QaFaultAction::Barrier { participants } => {
                        if activated.actors.len() != usize::from(participants) {
                            push_sidecar_issue(
                                &mut issues,
                                "barrier_actor_count_mismatch",
                                Some(record_index),
                                format!(
                                    "barrier expected {participants} actors, got {}",
                                    activated.actors.len()
                                ),
                            );
                        }
                        let joined_actor_set = barrier_joins
                            .get(activated.activation_id.as_str())
                            .map(|joins| joins.keys().map(String::as_str).collect::<BTreeSet<_>>())
                            .unwrap_or_default();
                        if joined_actor_set != actor_set {
                            push_sidecar_issue(
                                &mut issues,
                                "barrier_activation_join_mismatch",
                                Some(record_index),
                                "barrier activation actors must exactly match the durable join set",
                            );
                        }
                        match DeterministicQaFaultScheduler::new(plan.seed)
                            .release_order(planned, activated.actors.as_slice())
                        {
                            Ok(expected) if expected == activated.release_order => {}
                            Ok(_) | Err(_) => push_sidecar_issue(
                                &mut issues,
                                "invalid_barrier_release_order",
                                Some(record_index),
                                "barrier release_order does not match the seeded schedule",
                            ),
                        }
                    }
                    _ => {
                        if activated.actors.len() != 1
                            || activated.release_order != activated.actors
                            || planned.actor.as_deref().is_some_and(|actor| {
                                activated.actors.first().is_none_or(|actual| actual != actor)
                            })
                        {
                            push_sidecar_issue(
                                &mut issues,
                                "activation_actor_mismatch",
                                Some(record_index),
                                "non-barrier activation must contain its one planned actor",
                            );
                        }
                    }
                }
                for actor in &activated.actors {
                    let current_occurrence = checkpoint_occurrences
                        .get(&(activated.point_id.clone(), actor.clone()))
                        .copied()
                        .unwrap_or_default();
                    let expected_occurrence =
                        if matches!(planned.action, QaFaultAction::Barrier { .. }) {
                            current_occurrence
                        } else {
                            current_occurrence.saturating_add(1)
                        };
                    if activated.occurrence != expected_occurrence {
                        push_sidecar_issue(
                            &mut issues,
                            "activation_occurrence_sequence_mismatch",
                            Some(record_index),
                            format!(
                                "expected checkpoint occurrence {expected_occurrence}, got {}",
                                activated.occurrence
                            ),
                        );
                    }
                }
                if activated.activation_sequence == 0 {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_activation_sequence",
                        Some(record_index),
                        "activation_sequence must be one-based",
                    );
                } else if activation_sequences
                    .insert(activated.activation_sequence, activated.activation_id.clone())
                    .is_some()
                {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_activation_sequence",
                        Some(record_index),
                        "activation_sequence appears more than once",
                    );
                }
                if activated_ids.insert(activated.activation_id.clone(), record_index).is_some() {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_activation",
                        Some(record_index),
                        format!(
                            "activation `{}` appears more than once in the restart campaign",
                            activated.activation_id
                        ),
                    );
                }
                if matches!(planned.action, QaFaultAction::Barrier { .. }) {
                    barrier_activation_release_orders
                        .entry(activated.activation_id.clone())
                        .or_insert_with(|| activated.release_order.clone());
                }
                for actor in &activated.actors {
                    update_resume_occurrence(
                        &mut checkpoint_occurrences,
                        activated.point_id.as_str(),
                        actor.as_str(),
                        activated.occurrence,
                    );
                }
            }
            QaFaultEvidenceSidecarRecord::BarrierReleased(released) => {
                if !loaded_launches.contains_key(record_launch_id) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_launch_not_loaded",
                        Some(record_index),
                        format!(
                            "barrier release references launch `{record_launch_id}` before launch_loaded"
                        ),
                    );
                }
                let Some(planned) = planned_by_id.get(released.activation_id.as_str()) else {
                    push_sidecar_issue(
                        &mut issues,
                        "unplanned_barrier_release",
                        Some(record_index),
                        format!(
                            "barrier release references unplanned activation `{}`",
                            released.activation_id
                        ),
                    );
                    continue;
                };
                let QaFaultAction::Barrier { participants } = planned.action else {
                    push_sidecar_issue(
                        &mut issues,
                        "release_for_non_barrier_activation",
                        Some(record_index),
                        format!("activation `{}` is not a barrier", released.activation_id),
                    );
                    continue;
                };
                if released.point_id != planned.point_id {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_contract_mismatch",
                        Some(record_index),
                        format!(
                            "barrier release `{}` does not match its planned point",
                            released.activation_id
                        ),
                    );
                }
                if !is_bounded_actor(released.actor.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_barrier_release_actor",
                        Some(record_index),
                        "barrier release actor must be a bounded non-secret ASCII identifier",
                    );
                }
                let Some(release_order) =
                    barrier_activation_release_orders.get(released.activation_id.as_str())
                else {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_before_activation",
                        Some(record_index),
                        format!(
                            "barrier release for `{}` precedes its campaign activation",
                            released.activation_id
                        ),
                    );
                    continue;
                };
                if recoveries.contains(released.activation_id.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_after_recovery",
                        Some(record_index),
                        format!(
                            "barrier release for `{}` follows its recovery record",
                            released.activation_id
                        ),
                    );
                }
                let releases = barrier_releases.entry(released.activation_id.clone()).or_default();
                let expected_position =
                    u16::try_from(releases.len().saturating_add(1)).unwrap_or(u16::MAX);
                if released.release_position == 0 || released.release_position > participants {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_position_out_of_range",
                        Some(record_index),
                        format!("barrier release position must be in range 1..={participants}"),
                    );
                }
                if released.release_position != expected_position {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_sequence_mismatch",
                        Some(record_index),
                        format!(
                            "expected barrier release position {expected_position}, got {}",
                            released.release_position
                        ),
                    );
                }
                let expected_actor = usize::from(released.release_position)
                    .checked_sub(1)
                    .and_then(|position| release_order.get(position));
                if expected_actor != Some(&released.actor) {
                    push_sidecar_issue(
                        &mut issues,
                        "barrier_release_actor_mismatch",
                        Some(record_index),
                        "barrier release actor does not match the seeded position",
                    );
                }
                if releases.iter().any(|actor| actor == &released.actor) {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_barrier_release",
                        Some(record_index),
                        format!(
                            "actor `{}` consumed more than one release for barrier `{}`",
                            released.actor, released.activation_id
                        ),
                    );
                }
                releases.push(released.actor.clone());
            }
            QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery) => {
                if !loaded_launches.contains_key(record_launch_id) {
                    push_sidecar_issue(
                        &mut issues,
                        "recovery_launch_not_loaded",
                        Some(record_index),
                        format!(
                            "recovery references launch `{record_launch_id}` before launch_loaded"
                        ),
                    );
                }
                if !activated_ids.contains_key(recovery.activation_id.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "recovery_before_activation",
                        Some(record_index),
                        format!(
                            "recovery for `{}` precedes its campaign activation",
                            recovery.activation_id
                        ),
                    );
                }
                if !recoveries.insert(recovery.activation_id.clone()) {
                    push_sidecar_issue(
                        &mut issues,
                        "duplicate_recovery",
                        Some(record_index),
                        format!(
                            "activation `{}` has more than one recovery record",
                            recovery.activation_id
                        ),
                    );
                }
                if let Some(release_order) =
                    barrier_activation_release_orders.get(recovery.activation_id.as_str())
                {
                    let released =
                        barrier_releases.get(recovery.activation_id.as_str()).map_or(0, Vec::len);
                    if released != release_order.len() {
                        push_sidecar_issue(
                            &mut issues,
                            "barrier_recovery_before_all_releases",
                            Some(record_index),
                            format!(
                                "barrier recovery requires {} durable releases, got {released}",
                                release_order.len()
                            ),
                        );
                    }
                }
                if !is_bounded_identifier(recovery.reason_code.as_str()) {
                    push_sidecar_issue(
                        &mut issues,
                        "invalid_recovery_reason_code",
                        Some(record_index),
                        "recovery reason_code must be a bounded lowercase identifier",
                    );
                }
                if let Some(planned) = planned_by_id.get(recovery.activation_id.as_str()) {
                    if qa_fault_point_descriptor(planned.point_id.as_str()).is_some_and(
                        |descriptor| !descriptor.supports_recovery(recovery.recovery_class),
                    ) {
                        push_sidecar_issue(
                            &mut issues,
                            "unsupported_recovery_class",
                            Some(record_index),
                            format!(
                                "fault point `{}` cannot prove `{}`",
                                planned.point_id,
                                recovery.recovery_class.as_str()
                            ),
                        );
                    }
                } else {
                    push_sidecar_issue(
                        &mut issues,
                        "unplanned_recovery",
                        Some(record_index),
                        format!(
                            "recovery references unplanned activation `{}`",
                            recovery.activation_id
                        ),
                    );
                }
            }
        }
    }
    if mode == SidecarCampaignValidationMode::CurrentLaunchLoaded {
        if !loaded_launches.contains_key(launch.launch_id.as_str()) {
            push_sidecar_issue(
                &mut issues,
                "current_launch_not_loaded",
                None,
                format!("current launch `{}` is absent from the sidecar", launch.launch_id),
            );
        }
        if latest_loaded_launch != Some(launch.launch_id.as_str()) {
            push_sidecar_issue(
                &mut issues,
                "current_launch_not_latest",
                None,
                "current launch must be the most recent launch_loaded record",
            );
        }
    }
    issues
}

fn sidecar_record_header(record: &QaFaultEvidenceSidecarRecord) -> (u32, u32, &str, &str) {
    match record {
        QaFaultEvidenceSidecarRecord::LaunchLoaded(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
        QaFaultEvidenceSidecarRecord::CheckpointObserved(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
        QaFaultEvidenceSidecarRecord::BarrierJoined(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
        QaFaultEvidenceSidecarRecord::RuleActivated(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
        QaFaultEvidenceSidecarRecord::BarrierReleased(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
        QaFaultEvidenceSidecarRecord::RecoveryRecorded(record) => (
            record.schema_version,
            record.sequence,
            record.launch_id.as_str(),
            record.plan_sha256.as_str(),
        ),
    }
}

fn push_sidecar_issue(
    issues: &mut Vec<QaFaultEvidenceSidecarIssue>,
    code: impl Into<String>,
    record_index: Option<usize>,
    message: impl Into<String>,
) {
    issues.push(QaFaultEvidenceSidecarIssue {
        code: code.into(),
        record_index,
        message: message.into(),
    });
}
