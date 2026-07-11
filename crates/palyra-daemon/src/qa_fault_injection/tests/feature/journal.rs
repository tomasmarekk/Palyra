//! Journal post-commit crash and durable recovery tests.

use super::*;

#[test]
fn journal_post_commit_crash_reopen_suppresses_duplicate() {
    if std::env::var(JOURNAL_TERMINATE_CHILD_ENV).ok().as_deref() == Some("1") {
        run_journal_terminate_child();
    }
    let temporary = tempdir().expect("temporary journal fault root should be created");
    let output = Command::new(
        std::env::current_exe().expect("current test executable should resolve"),
    )
    .args([
        "--exact",
        "qa_fault_injection::tests::feature::journal::journal_post_commit_crash_reopen_suppresses_duplicate",
        "--nocapture",
    ])
    .env(JOURNAL_TERMINATE_CHILD_ENV, "1")
    .env(JOURNAL_TERMINATE_ROOT_ENV, temporary.path())
    .output()
    .expect("journal terminate child should launch");
    assert_eq!(
        output.status.code(),
        Some(palyra_common::qa_fault_injection::QA_FAULT_TERMINATE_EXIT_CODE),
        "child stderr: {}",
        String::from_utf8_lossy(output.stderr.as_slice())
    );

    let plan = journal_post_commit_terminate_plan();
    let launch = journal_terminate_launch(temporary.path(), &plan);
    let runtime = QaFaultRuntime::active(
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new(plan.clone())
                .expect("journal controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch: launch.clone(),
            path: temporary.path().join("evidence.ndjson"),
            next_sequence: 3,
            activated_rules: BTreeMap::from([(
                "journal-post-commit".to_owned(),
                "journal.after_effect_before_ack".to_owned(),
            )]),
            activation_actors: BTreeMap::from([(
                "journal-post-commit".to_owned(),
                vec!["journal-event".to_owned()],
            )]),
            barrier_joins: BTreeMap::new(),
            barrier_join_points: BTreeMap::new(),
            barrier_participants: BTreeMap::new(),
            barrier_release_orders: BTreeMap::new(),
            barrier_releases: BTreeMap::new(),
            observed_occurrences: BTreeMap::new(),
            occurrence_targets: occurrence_targets(&plan),
            recovered_rule_ids: BTreeSet::new(),
        },
        plan.seed,
    );
    assert_eq!(
        runtime
            .record_startup_orphan_recoveries()
            .expect("startup must preserve recovery that requires journal proof"),
        0,
        "journal deduplication cannot be inferred generically at startup"
    );
    let store = open_faulted_test_journal(temporary.path(), runtime);
    assert_eq!(
        store
            .reconcile_pending_qa_fault_recoveries()
            .expect("committed journal row should prove the pending effect"),
        1
    );
    let duplicate = store
        .append(&journal_fault_request())
        .expect_err("reopening and retrying the committed event must be suppressed");
    assert!(matches!(
        duplicate,
        JournalError::DuplicateEventId { ref event_id } if event_id == "journal-event"
    ));
    assert_eq!(store.total_events().expect("journal count should load"), 1);

    let evidence = fs::read(temporary.path().join("evidence.ndjson"))
        .expect("journal evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
        .expect("journal crash evidence should validate");
    assert_eq!(parsed.records().len(), 3);
    assert!(matches!(
        parsed.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
            if record.activation_id == "journal-post-commit"
                && record.recovery_class == QaFaultRecoveryClass::EffectConfirmed
    ));
}

#[test]
fn journal_recovery_rejects_pending_effect_without_committed_row() {
    let temporary = tempdir().expect("temporary journal fault root should be created");
    let plan = journal_post_commit_terminate_plan();
    let launch = journal_terminate_launch(temporary.path(), &plan);
    let activation = plan.activations.first().expect("journal activation should exist");
    let evidence_path = temporary.path().join("evidence.ndjson");
    let records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: launch.launch_id.clone(),
            plan_sha256: launch.plan_sha256.clone(),
            capability_sha256: launch.capability_sha256.clone(),
        }),
        QaFaultEvidenceSidecarRecord::RuleActivated(QaFaultRuleActivatedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: launch.launch_id.clone(),
            plan_sha256: launch.plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actors: vec!["journal-event".to_owned()],
            occurrence: activation.occurrence,
            action: activation.action.clone(),
            activation_sequence: 1,
            release_order: vec!["journal-event".to_owned()],
        }),
    ];
    write_evidence_records(evidence_path.as_path(), records.as_slice());
    let runtime = QaFaultRuntime::active(
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new(plan.clone())
                .expect("journal controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch: launch.clone(),
            path: evidence_path.clone(),
            next_sequence: 3,
            activated_rules: BTreeMap::from([(activation.id.clone(), activation.point_id.clone())]),
            activation_actors: BTreeMap::from([(
                activation.id.clone(),
                vec!["journal-event".to_owned()],
            )]),
            barrier_joins: BTreeMap::new(),
            barrier_join_points: BTreeMap::new(),
            barrier_participants: BTreeMap::new(),
            barrier_release_orders: BTreeMap::new(),
            barrier_releases: BTreeMap::new(),
            observed_occurrences: BTreeMap::from([(
                (activation.point_id.clone(), "journal-event".to_owned()),
                activation.occurrence,
            )]),
            occurrence_targets: occurrence_targets(&plan),
            recovered_rule_ids: BTreeSet::new(),
        },
        plan.seed,
    );
    let store = open_faulted_test_journal(temporary.path(), runtime);

    let error = store
        .reconcile_pending_qa_fault_recoveries()
        .expect_err("missing journal row must fail loud");

    assert!(matches!(
        error,
        JournalError::FaultInjection {
            point_id: "journal.after_effect_before_ack",
            ref message,
        } if message.contains("no committed journal event")
    ));
    let evidence =
        fs::read(evidence_path.as_path()).expect("unrecovered evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
        .expect("unrecovered journal evidence should remain valid");
    assert_eq!(parsed.records().len(), 2);
    assert!(!parsed
        .records()
        .iter()
        .any(|record| matches!(record, QaFaultEvidenceSidecarRecord::RecoveryRecorded(_))));
}
