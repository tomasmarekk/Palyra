//! Regression coverage for schemas, validation, scheduling, and restart evidence.
//!
//! The facade owns this module so test paths remain stable after internal splits.

use std::collections::BTreeSet;

use serde_json::Value;

use super::*;

const PLAN_SCHEMA_GOLDEN: &str =
    include_str!("../../../../fixtures/golden/qa_fault_injection_plan_schema_v1.json");
const REGISTRY_GOLDEN: &str =
    include_str!("../../../../fixtures/golden/qa_fault_injection_registry_v1.json");

#[test]
fn plan_schema_and_registry_match_golden_fixtures() {
    let expected_schema: Value =
        serde_json::from_str(PLAN_SCHEMA_GOLDEN).expect("plan schema golden should parse");
    let expected_registry: Value =
        serde_json::from_str(REGISTRY_GOLDEN).expect("registry golden should parse");
    let actual_schema = qa_fault_injection_plan_schema_snapshot();
    let actual_registry = qa_fault_point_registry_snapshot();

    assert_eq!(actual_schema, expected_schema);
    assert_eq!(actual_registry, expected_registry);
}

#[test]
fn registry_is_sorted_unique_and_claims_only_implemented_barriers() {
    let ids = QA_FAULT_POINT_REGISTRY_V1.iter().map(|descriptor| descriptor.id).collect::<Vec<_>>();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(ids.iter().copied().collect::<BTreeSet<_>>().len(), ids.len());

    for point_id in [
        "connector.outbox.after_ack_before_transition",
        "connector.outbox.after_effect_before_ack",
        "connector.outbox.before_effect",
        "connector.outbox.during_delivery",
        "run.final_delivery.after_effect_before_ack",
        "tool.after_ack_before_transition",
        "tool.after_effect_before_ack",
        "worker.stale_reclaim.before_effect",
    ] {
        let descriptor = qa_fault_point_descriptor(point_id).expect("point should exist");
        assert!(!descriptor.supports(QaFaultActionKind::Barrier), "{point_id}");
    }
    for point_id in [
        "connector.outbox.batch_before_effect",
        "worker.claim.before_effect",
        "worker.stale_reclaim.batch_before_effect",
    ] {
        let descriptor = qa_fault_point_descriptor(point_id).expect("point should exist");
        assert!(descriptor.supports(QaFaultActionKind::Barrier), "{point_id}");
    }

    let terminate_boundaries = QA_FAULT_POINT_REGISTRY_V1
        .iter()
        .filter(|descriptor| descriptor.supports(QaFaultActionKind::TerminateProcess))
        .map(|descriptor| descriptor.boundary)
        .collect::<BTreeSet<_>>();
    for boundary in [
        QaFaultInjectionBoundary::BeforeIntent,
        QaFaultInjectionBoundary::AfterIntent,
        QaFaultInjectionBoundary::BeforeEffect,
        QaFaultInjectionBoundary::AfterEffectBeforeAck,
        QaFaultInjectionBoundary::AfterAckBeforeTransition,
        QaFaultInjectionBoundary::DuringDelivery,
        QaFaultInjectionBoundary::DuringCleanup,
    ] {
        assert!(
            terminate_boundaries.contains(&boundary),
            "missing terminate-capable point for {}",
            boundary.as_str()
        );
    }
}

#[test]
fn plan_parser_is_strict_and_canonical_digest_survives_round_trips() {
    let plan = sample_plan();
    let yaml = yaml_serde::to_string(&plan).expect("sample plan should serialize as YAML");
    let parsed_yaml = parse_qa_fault_injection_plan_yaml(yaml.as_str())
        .expect("serialized YAML plan should parse");
    let json = serde_json::to_string(&plan).expect("sample plan should serialize as JSON");
    let parsed_json = parse_qa_fault_injection_plan_yaml(json.as_str())
        .expect("serialized JSON plan should parse");

    assert_eq!(parsed_yaml, plan);
    assert_eq!(parsed_json, plan);
    assert_eq!(
        parsed_yaml.canonical_sha256().expect("YAML plan should hash"),
        parsed_json.canonical_sha256().expect("JSON plan should hash")
    );

    let unknown_field = json.strip_suffix('}').expect("JSON object should end").to_owned()
        + ",\"unexpected\":true}";
    assert!(matches!(
        parse_qa_fault_injection_plan_yaml(unknown_field.as_str()),
        Err(QaFaultInjectionPlanParseError::Parse { .. })
    ));
}

#[test]
fn plan_validation_rejects_duplicate_triggers_and_unsupported_actions() {
    let mut duplicate = sample_plan();
    let mut second = duplicate.activations[0].clone();
    second.id = "second-crash".to_owned();
    duplicate.activations.push(second);
    let duplicate_error = duplicate.validate().expect_err("duplicate trigger must fail");
    assert!(duplicate_error
        .issues()
        .iter()
        .any(|issue| issue.code == "duplicate_activation_trigger"));

    let mut unsupported = sample_plan();
    unsupported.activations[0].action = QaFaultAction::Timeout;
    let unsupported_error = unsupported.validate().expect_err("unsupported action must fail");
    assert!(unsupported_error
        .issues()
        .iter()
        .any(|issue| issue.code == "unsupported_fault_action"));
}

#[test]
fn plan_validation_rejects_overlapping_wildcard_and_exact_actor_selectors() {
    let mut base = sample_plan();
    let mut wildcard = base.activations.remove(0);
    wildcard.id = "wildcard-crash".to_owned();
    wildcard.actor = None;
    let mut exact = wildcard.clone();
    exact.id = "exact-crash".to_owned();
    exact.actor = Some("daemon".to_owned());

    for activations in [vec![wildcard.clone(), exact.clone()], vec![exact, wildcard]] {
        let plan = QaFaultInjectionPlan {
            schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
            format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
            seed: 4242,
            activations,
        };
        let error = plan.validate().expect_err("overlapping selectors must fail closed");
        assert!(error.issues().iter().any(|issue| issue.code == "overlapping_activation_selector"));
    }
}

#[test]
fn plan_validation_enforces_campaign_evidence_budget() {
    let mut plan = sample_plan();
    let mut highest_accepted = 0;
    let mut first_rejected = None;
    for occurrence in 1..=QA_FAULT_INJECTION_MAX_OCCURRENCE {
        plan.activations[0].occurrence = occurrence;
        if plan.validate().is_ok() {
            highest_accepted = occurrence;
        } else {
            first_rejected = Some(occurrence);
            break;
        }
    }
    let first_rejected = first_rejected.expect("physical evidence budget must be finite");
    assert!(highest_accepted > 1);
    assert_eq!(first_rejected, highest_accepted.saturating_add(1));
    plan.activations[0].occurrence = highest_accepted;
    plan.validate().expect("maximum accepted occurrence should fit the evidence budget");
    plan.activations[0].occurrence = first_rejected;
    let error = plan.validate().expect_err("first over-budget occurrence must fail");
    assert!(error
        .issues()
        .iter()
        .any(|issue| issue.code == "campaign_evidence_byte_budget_exceeded"));

    let barrier_plan = QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 9,
        activations: vec![QaFaultActivation {
            id: "unbounded-barrier".to_owned(),
            point_id: "worker.claim.before_effect".to_owned(),
            actor: None,
            occurrence: 2,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    };
    let error = barrier_plan
        .validate()
        .expect_err("unselected actor occurrence cannot fit a deterministic evidence budget");
    assert!(error.issues().iter().any(|issue| issue.code == "unbounded_occurrence_actor"));
}

#[test]
fn disabled_handle_is_a_semantics_free_cloneable_noop() {
    let handle = QaFaultProbeHandle::default();
    let cloned = handle.clone();

    assert_eq!(
        cloned
            .checkpoint(QaFaultCheckpoint { point_id: "not.registered", actor: "not valid!" })
            .expect("disabled probe must not reject ambient inputs"),
        QaFaultDirective::Continue
    );
    cloned
        .record_recovery("not-observed", QaFaultRecoveryClass::FailedClosed)
        .expect("disabled recovery recording must be a no-op");
    assert!(cloned.records().expect("disabled records should load").is_empty());
}

#[test]
fn deterministic_controller_activates_exact_actor_occurrence_and_records_recovery() {
    let mut plan = sample_plan();
    plan.activations[0].occurrence = 2;
    let handle = QaFaultProbeHandle::from_probe(
        DeterministicQaFaultController::new(plan).expect("sample plan should validate"),
    );
    let checkpoint =
        QaFaultCheckpoint { point_id: "tool.after_effect_before_ack", actor: "daemon" };

    assert_eq!(
        handle.checkpoint(checkpoint).expect("first checkpoint should evaluate"),
        QaFaultDirective::Continue
    );
    let QaFaultDirective::Activate(directive) =
        handle.checkpoint(checkpoint).expect("second checkpoint should evaluate")
    else {
        panic!("second checkpoint should activate");
    };
    assert_eq!(directive.observed_occurrence, 2);
    assert_eq!(directive.activation_sequence, 1);
    assert_eq!(directive.actor, "daemon");
    assert!(matches!(
        handle.record_recovery("tool-crash", QaFaultRecoveryClass::FailedClosed),
        Err(QaFaultProbeError::UnsupportedRecoveryClass {
            activation_id,
            recovery_class: QaFaultRecoveryClass::FailedClosed,
        }) if activation_id == "tool-crash"
    ));
    handle
        .record_recovery("tool-crash", QaFaultRecoveryClass::DuplicateSuppressed)
        .expect("observed activation should accept recovery");
    let records = handle.records().expect("controller records should load");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::DuplicateSuppressed));
}

#[test]
fn seeded_scheduler_returns_a_stable_complete_barrier_order() {
    let activation = QaFaultActivation {
        id: "claim-race".to_owned(),
        point_id: "worker.claim.before_effect".to_owned(),
        actor: None,
        occurrence: 1,
        action: QaFaultAction::Barrier { participants: 4 },
    };
    let actors = ["worker-a", "worker-b", "worker-c", "worker-d"].map(str::to_owned).to_vec();
    let scheduler = DeterministicQaFaultScheduler::new(4242);
    let first =
        scheduler.release_order(&activation, actors.as_slice()).expect("barrier should schedule");
    let second = scheduler
        .release_order(&activation, actors.as_slice())
        .expect("same barrier should schedule again");

    assert_eq!(first, second);
    assert_eq!(first.iter().collect::<BTreeSet<_>>(), actors.iter().collect());
}

#[test]
fn controller_exposes_bounded_active_barrier_snapshots() {
    let plan = QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 4242,
        activations: vec![QaFaultActivation {
            id: "claim-race".to_owned(),
            point_id: "worker.claim.before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    };
    let handle = QaFaultProbeHandle::from_probe(
        DeterministicQaFaultController::new(plan).expect("barrier plan should validate"),
    );
    for actor in ["claim-a", "claim-b"] {
        assert!(matches!(
            handle
                .checkpoint(QaFaultCheckpoint { point_id: "worker.claim.before_effect", actor })
                .expect("barrier checkpoint should evaluate"),
            QaFaultDirective::Activate(_)
        ));
    }

    let barriers = handle.active_barriers().expect("active barrier should project");
    assert_eq!(barriers.len(), 1);
    assert_eq!(barriers[0].actors, ["claim-a", "claim-b"]);
    assert_eq!(barriers[0].participants, 2);
    assert_eq!(barriers[0].release_order.as_ref().map(Vec::len), Some(2));
    assert!(barriers[0].released_actors.is_empty());

    handle
        .record_recovery("claim-race", QaFaultRecoveryClass::RetrySucceeded)
        .expect("barrier recovery should record");
    assert!(handle.active_barriers().expect("recovered barrier should project").is_empty());
}

#[test]
fn sidecar_rejects_activation_and_barrier_occurrence_gaps() {
    let mut plan = sample_plan();
    plan.activations[0].occurrence = 3;
    let plan_sha256 = plan.canonical_sha256().expect("sample plan should hash");
    let launch = sample_launch("launch-gap", "a", plan_sha256.as_str());
    let records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: launch.launch_id.clone(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: launch.capability_sha256.clone(),
        }),
        QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: launch.launch_id.clone(),
            plan_sha256,
            activation_id: plan.activations[0].id.clone(),
            point_id: plan.activations[0].point_id.clone(),
            actors: vec!["daemon".to_owned()],
            occurrence: 3,
            action: plan.activations[0].action.clone(),
            activation_sequence: 1,
            release_order: vec!["daemon".to_owned()],
        }),
    ];
    let error = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(records.as_slice()).as_bytes(),
        &launch,
        &plan,
    )
    .expect_err("activation occurrence gap must fail strict validation");
    assert!(sidecar_error_has_code(&error, "activation_occurrence_sequence_mismatch"));

    let barrier_activation = QaFaultActivation {
        id: "barrier-gap".to_owned(),
        point_id: "worker.claim.before_effect".to_owned(),
        actor: None,
        occurrence: 1,
        action: QaFaultAction::Barrier { participants: 2 },
    };
    let barrier_plan = QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 7,
        activations: vec![barrier_activation.clone()],
    };
    let barrier_sha256 = barrier_plan.canonical_sha256().expect("barrier plan should hash");
    let barrier_launch = sample_launch("barrier-gap-launch", "b", barrier_sha256.as_str());
    let barrier_records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: barrier_launch.launch_id.clone(),
            plan_sha256: barrier_sha256.clone(),
            capability_sha256: barrier_launch.capability_sha256.clone(),
        }),
        QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: barrier_launch.launch_id.clone(),
            plan_sha256: barrier_sha256,
            activation_id: barrier_activation.id.clone(),
            point_id: barrier_activation.point_id.clone(),
            actor: "claim-a".to_owned(),
            occurrence: 2,
        }),
    ];
    let error = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(barrier_records.as_slice()).as_bytes(),
        &barrier_launch,
        &barrier_plan,
    )
    .expect_err("barrier join occurrence gap must fail strict validation");
    assert!(sidecar_error_has_code(&error, "barrier_join_occurrence_sequence_mismatch"));
}

#[test]
fn sidecar_preserves_barrier_joins_across_restart_before_seeded_release() {
    let activation = QaFaultActivation {
        id: "claim-race".to_owned(),
        point_id: "worker.claim.before_effect".to_owned(),
        actor: None,
        occurrence: 1,
        action: QaFaultAction::Barrier { participants: 2 },
    };
    let plan = QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 4242,
        activations: vec![activation.clone()],
    };
    let plan_sha256 = plan.canonical_sha256().expect("barrier plan should hash");
    let current_launch = sample_launch("launch-2", "b", plan_sha256.as_str());
    let actors = vec!["claim-a".to_owned(), "claim-b".to_owned()];
    let release_order = DeterministicQaFaultScheduler::new(plan.seed)
        .release_order(&activation, actors.as_slice())
        .expect("barrier should have a seeded release order");
    let records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: "launch-1".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "a".repeat(64),
        }),
        QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: "launch-1".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: actors[0].clone(),
            occurrence: activation.occurrence,
        }),
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 3,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "b".repeat(64),
        }),
        QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 4,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: actors[1].clone(),
            occurrence: activation.occurrence,
        }),
        QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 5,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actors: actors.clone(),
            occurrence: activation.occurrence,
            action: activation.action.clone(),
            activation_sequence: 1,
            release_order: release_order.clone(),
        }),
        QaFaultEvidenceSidecarRecord::BarrierReleased(QaFaultBarrierReleasedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 6,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: release_order[0].clone(),
            release_position: 1,
        }),
        QaFaultEvidenceSidecarRecord::BarrierReleased(QaFaultBarrierReleasedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 7,
            launch_id: "launch-2".to_owned(),
            plan_sha256,
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: release_order[1].clone(),
            release_position: 2,
        }),
    ];

    validate_qa_fault_evidence_campaign_before_launch(
        encode_sidecar(&records[..2]).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect("an incomplete prior-launch barrier join must remain restartable");

    let parsed = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(records.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect("restart-spanning barrier joins should validate");
    assert_eq!(parsed.records(), records.as_slice());

    let mut premature_recovery = records[..6].to_vec();
    premature_recovery.push(QaFaultEvidenceSidecarRecord::RecoveryRecorded(
        QaFaultRecoveryRecordedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 7,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan.canonical_sha256().expect("barrier plan should hash"),
            activation_id: activation.id.clone(),
            recovery_class: QaFaultRecoveryClass::RetrySucceeded,
            reason_code: "worker.retry_succeeded".to_owned(),
        },
    ));
    let error = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(premature_recovery.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect_err("partially released barrier must reject recovery evidence");
    assert!(sidecar_error_has_code(&error, "barrier_recovery_before_all_releases"));

    let mut duplicate = records;
    let QaFaultEvidenceSidecarRecord::BarrierJoined(second_join) = &mut duplicate[3] else {
        panic!("fourth record should be a barrier join");
    };
    second_join.actor = "claim-a".to_owned();
    let error = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(duplicate.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect_err("duplicate barrier actor must fail strict validation");
    assert!(sidecar_error_has_code(&error, "duplicate_barrier_join"));

    let mut reordered_release = duplicate;
    let Some(QaFaultEvidenceSidecarRecord::BarrierReleased(second_release)) =
        reordered_release.last_mut()
    else {
        panic!("last record should be the second barrier release");
    };
    second_release.actor = release_order[0].clone();
    let error = parse_qa_fault_evidence_sidecar_ndjson(
        encode_sidecar(reordered_release.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect_err("reordered barrier release must fail strict validation");
    assert!(sidecar_error_has_code(&error, "barrier_release_actor_mismatch"));
}

#[test]
fn sidecar_accepts_activation_before_restart_and_recovery_after_restart() {
    let plan = sample_plan();
    let plan_sha256 = plan.canonical_sha256().expect("sample plan should hash");
    let current_launch = sample_launch("launch-2", "b", plan_sha256.as_str());
    let records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: "launch-1".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "a".repeat(64),
        }),
        QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: "launch-1".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: "tool-crash".to_owned(),
            point_id: "tool.after_effect_before_ack".to_owned(),
            actors: vec!["daemon".to_owned()],
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
            activation_sequence: 1,
            release_order: vec!["daemon".to_owned()],
        }),
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 3,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "b".repeat(64),
        }),
        QaFaultEvidenceSidecarRecord::RecoveryRecorded(QaFaultRecoveryRecordedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 4,
            launch_id: "launch-2".to_owned(),
            plan_sha256,
            activation_id: "tool-crash".to_owned(),
            recovery_class: QaFaultRecoveryClass::DuplicateSuppressed,
            reason_code: "tool.duplicate_suppressed".to_owned(),
        }),
    ];
    let ndjson = encode_sidecar(records.as_slice());

    let parsed = parse_qa_fault_evidence_sidecar_ndjson(ndjson.as_bytes(), &current_launch, &plan)
        .expect("restart campaign sidecar should validate");

    assert_eq!(parsed.records(), records.as_slice());
}

#[test]
fn campaign_preflight_rejects_tampering_and_replayed_current_launch() {
    let plan = sample_plan();
    let plan_sha256 = plan.canonical_sha256().expect("sample plan should hash");
    let current_launch = sample_launch("launch-2", "b", plan_sha256.as_str());
    let prior = vec![QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: "launch-1".to_owned(),
        plan_sha256: plan_sha256.clone(),
        capability_sha256: "a".repeat(64),
    })];
    validate_qa_fault_evidence_campaign_before_launch(
        encode_sidecar(prior.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect("valid prior campaign should pass preflight");

    let replayed = vec![
        prior[0].clone(),
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: "launch-2".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "b".repeat(64),
        }),
    ];
    let replay_error = validate_qa_fault_evidence_campaign_before_launch(
        encode_sidecar(replayed.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect_err("replayed current launch must fail preflight");
    assert!(sidecar_error_has_code(&replay_error, "current_launch_replayed"));

    let tampered = vec![QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: "launch-1".to_owned(),
        plan_sha256: "c".repeat(64),
        capability_sha256: "a".repeat(64),
    })];
    let tamper_error = validate_qa_fault_evidence_campaign_before_launch(
        encode_sidecar(tampered.as_slice()).as_bytes(),
        &current_launch,
        &plan,
    )
    .expect_err("tampered prior plan digest must fail preflight");
    assert!(sidecar_error_has_code(&tamper_error, "record_plan_digest_mismatch"));
}

fn sample_plan() -> QaFaultInjectionPlan {
    QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 4242,
        activations: vec![QaFaultActivation {
            id: "tool-crash".to_owned(),
            point_id: "tool.after_effect_before_ack".to_owned(),
            actor: Some("daemon".to_owned()),
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
        }],
    }
}

fn sample_launch(
    launch_id: &str,
    capability_digit: &str,
    plan_sha256: &str,
) -> QaFaultLaunchDocument {
    let root = std::env::temp_dir().join("palyra-fault-sidecar-campaign");
    QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: launch_id.to_owned(),
        plan_path: root.join("plan.json").to_string_lossy().into_owned(),
        plan_sha256: plan_sha256.to_owned(),
        capability_sha256: capability_digit.repeat(64),
        evidence_path: root.join("evidence.ndjson").to_string_lossy().into_owned(),
        expires_at_unix_ms: 2_000_000_000_000,
    }
}

fn encode_sidecar(records: &[QaFaultEvidenceSidecarRecord]) -> String {
    let mut ndjson = records
        .iter()
        .map(|record| serde_json::to_string(record).expect("record should serialize"))
        .collect::<Vec<_>>()
        .join("\n");
    ndjson.push('\n');
    ndjson
}

fn sidecar_error_has_code(error: &QaFaultEvidenceSidecarError, code: &str) -> bool {
    matches!(
        error,
        QaFaultEvidenceSidecarError::Invalid(validation)
            if validation.issues().iter().any(|issue| issue.code == code)
    )
}
