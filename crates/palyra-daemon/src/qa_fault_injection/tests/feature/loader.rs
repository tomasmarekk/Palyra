//! Authenticated-loader, evidence-resume, and recovery-mapping tests.

use super::*;

#[test]
fn bounded_reader_rejects_limit_plus_one() {
    let temporary = tempdir().expect("temporary read root should be created");
    let path = temporary.path().join("oversized.bin");
    fs::write(path.as_path(), [0_u8; 9]).expect("oversized fixture should be written");

    let error = enabled::read_bounded_file(path.as_path(), 8, "test fixture")
        .expect_err("bounded reader must reject limit plus one");

    assert!(error.to_string().contains("exceeds 8 bytes"));
}

#[test]
fn evidence_append_rejects_record_and_byte_budget_overflow() {
    let temporary = tempdir().expect("temporary evidence root should be created");
    let record_path = temporary.path().join("record-budget.ndjson");
    fs::write(record_path.as_path(), []).expect("empty evidence file should be written");
    let over_record_budget =
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: u32::try_from(QA_FAULT_EVIDENCE_SIDECAR_MAX_RECORDS + 1)
                .expect("record budget should fit u32"),
            launch_id: "record-budget".to_owned(),
            plan_sha256: "a".repeat(64),
            capability_sha256: "b".repeat(64),
        });
    let error = enabled::append_evidence_record(record_path.as_path(), &over_record_budget)
        .expect_err("append must reject a sequence beyond the record budget");
    assert!(error.to_string().contains("exceed its record budget"));
    assert_eq!(
        fs::metadata(record_path.as_path())
            .expect("record-budget file should remain inspectable")
            .len(),
        0
    );

    let byte_path = temporary.path().join("byte-budget.ndjson");
    fs::write(byte_path.as_path(), vec![b'x'; QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES])
        .expect("full byte-budget file should be written");
    let valid_sequence = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: "byte-budget".to_owned(),
        plan_sha256: "a".repeat(64),
        capability_sha256: "b".repeat(64),
    });
    let error = enabled::append_evidence_record(byte_path.as_path(), &valid_sequence)
        .expect_err("append must reject growth beyond the byte budget");
    assert!(error.to_string().contains("exceed its byte budget"));
    assert_eq!(
        fs::metadata(byte_path.as_path())
            .expect("byte-budget file should remain inspectable")
            .len(),
        u64::try_from(QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES).expect("byte budget should fit u64")
    );
}

#[test]
fn loader_requires_explicit_preview_mode_without_consuming_capability() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::remove_var(QA_LAB_MODE_ENV);
    let temporary = tempdir().expect("temporary loader root should be created");
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &delivery_unknown_plan(),
        "preview-required",
        "71".repeat(32).as_str(),
    );

    let error = load_fault_injection(fixture.state_root.as_path())
        .expect_err("feature-on loader must still require preview mode");

    assert!(error.to_string().starts_with("qa_fault.preview_gate_required"));
    assert!(fixture.capability_path.exists());
    assert_no_launch_loaded(fixture.evidence_path.as_path());
}

#[test]
fn loader_rejects_wrong_capability_without_loading_or_consuming_it() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");
    let temporary = tempdir().expect("temporary loader root should be created");
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &delivery_unknown_plan(),
        "wrong-capability",
        "72".repeat(32).as_str(),
    );
    fs::write(
        fixture.capability_path.as_path(),
        format!("{QA_FAULT_CAPABILITY_PREFIX}{}\n", "ff".repeat(32)),
    )
    .expect("wrong capability should be written");
    ensure_owner_only_file(fixture.capability_path.as_path())
        .expect("wrong capability should remain hardened");

    let error = load_fault_injection(fixture.state_root.as_path())
        .expect_err("capability digest mismatch must fail closed");

    assert!(error.to_string().contains("capability SHA-256 digest does not match"));
    assert!(fixture.capability_path.exists());
    assert_no_launch_loaded(fixture.evidence_path.as_path());
}

#[test]
fn loader_rejects_plan_digest_mismatch_without_loading_or_consuming_capability() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");
    let temporary = tempdir().expect("temporary loader root should be created");
    let plan = delivery_unknown_plan();
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &plan,
        "wrong-plan-digest",
        "73".repeat(32).as_str(),
    );
    let mut changed_plan = plan;
    changed_plan.seed = changed_plan.seed.saturating_add(1);
    fs::write(
        fixture.plan_path.as_path(),
        changed_plan.canonical_json().expect("changed plan should serialize"),
    )
    .expect("changed plan should be written");
    ensure_owner_only_file(fixture.plan_path.as_path())
        .expect("changed plan should remain hardened");

    let error = load_fault_injection(fixture.state_root.as_path())
        .expect_err("plan digest mismatch must fail closed");

    assert!(error.to_string().contains("canonical plan digest does not match"));
    assert!(fixture.capability_path.exists());
    assert_no_launch_loaded(fixture.evidence_path.as_path());
}

#[test]
fn loader_rejects_out_of_root_plan_without_loading_or_consuming_capability() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");
    let temporary = tempdir().expect("temporary loader root should be created");
    let plan = delivery_unknown_plan();
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &plan,
        "out-of-root-plan",
        "74".repeat(32).as_str(),
    );
    let outside_plan = temporary.path().join("outside-plan.json");
    fs::write(
        outside_plan.as_path(),
        plan.canonical_json().expect("outside plan should serialize"),
    )
    .expect("outside plan should be written");
    ensure_owner_only_file(outside_plan.as_path()).expect("outside plan should be hardened");
    let mut escaped_launch = fixture.launch.clone();
    escaped_launch.plan_path = fs::canonicalize(outside_plan.as_path())
        .expect("outside plan should canonicalize")
        .to_string_lossy()
        .into_owned();
    fs::write(
        fixture.launch_path.as_path(),
        serde_json::to_vec(&escaped_launch).expect("escaped launch should serialize"),
    )
    .expect("escaped launch should be written");
    ensure_owner_only_file(fixture.launch_path.as_path())
        .expect("escaped launch should remain hardened");

    let error = load_fault_injection(fixture.state_root.as_path())
        .expect_err("out-of-root plan must fail closed");

    assert!(error.to_string().contains("must be confined beneath the canonical state root"));
    assert!(fixture.capability_path.exists());
    assert_no_launch_loaded(fixture.evidence_path.as_path());
}

#[test]
fn loader_preserves_activation_and_records_unknown_delivery_after_restart() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");

    let temporary = tempdir().expect("temporary state root should be created");
    let state_root = temporary.path().join("state");
    let private_root = state_root.join("qa-fault");
    ensure_owner_only_dir(private_root.as_path()).expect("private QA directory should be hardened");
    let canonical_state_root =
        fs::canonicalize(state_root.as_path()).expect("state root should canonicalize");
    let canonical_private_root =
        fs::canonicalize(private_root.as_path()).expect("private QA directory should canonicalize");

    let plan = delivery_unknown_plan();
    let plan_bytes = plan.canonical_json().expect("test plan should serialize");
    let plan_sha256 = plan.canonical_sha256().expect("test plan should hash");
    let plan_path = canonical_private_root.join("plan.json");
    fs::write(plan_path.as_path(), plan_bytes).expect("test plan should be written");
    ensure_owner_only_file(plan_path.as_path()).expect("test plan should be hardened");
    let evidence_path = canonical_private_root.join("evidence.ndjson");

    write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "launch-one",
        "11".repeat(32).as_str(),
    );
    let first_runtime =
        load_fault_injection(canonical_state_root.as_path()).expect("first launch should activate");
    let directive = first_runtime
        .checkpoint("run.final_delivery.after_effect_before_ack", "delivery")
        .expect("delivery checkpoint should activate");
    assert!(matches!(directive, QaFaultDirective::Activate(_)));
    drop(first_runtime);

    let second_launch = write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "launch-two",
        "22".repeat(32).as_str(),
    );
    let second_runtime = load_fault_injection(canonical_state_root.as_path())
        .expect("second launch should resume the campaign");
    assert_eq!(
        second_runtime
            .record_startup_orphan_recoveries()
            .expect("startup recovery should be durable"),
        1
    );

    let evidence = fs::read(evidence_path.as_path()).expect("evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &second_launch, &plan)
        .expect("two-launch evidence should validate");
    assert_eq!(parsed.records().len(), 4);
    let QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery) = &parsed.records()[3] else {
        panic!("fourth evidence record should classify recovery");
    };
    assert_eq!(recovery.launch_id, "launch-two");
    assert_eq!(recovery.activation_id, "final-delivery-unknown");
    assert_eq!(recovery.recovery_class, QaFaultRecoveryClass::OutcomeUnknown);
}

#[test]
fn loader_resumes_occurrence_and_activation_sequence_counters() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");

    let temporary = tempdir().expect("temporary state root should be created");
    let state_root = temporary.path().join("state");
    let private_root = state_root.join("qa-fault");
    ensure_owner_only_dir(private_root.as_path()).expect("private QA directory should be hardened");
    let canonical_state_root =
        fs::canonicalize(state_root.as_path()).expect("state root should canonicalize");
    let canonical_private_root =
        fs::canonicalize(private_root.as_path()).expect("private QA directory should canonicalize");

    let plan = restart_sequence_plan();
    let plan_sha256 = plan.canonical_sha256().expect("restart plan should hash");
    let plan_path = canonical_private_root.join("restart-plan.json");
    fs::write(plan_path.as_path(), plan.canonical_json().expect("restart plan should serialize"))
        .expect("restart plan should be written");
    ensure_owner_only_file(plan_path.as_path()).expect("restart plan should be hardened");
    let evidence_path = canonical_private_root.join("restart-evidence.ndjson");

    write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "restart-launch-one",
        "55".repeat(32).as_str(),
    );
    let runtime_one = load_fault_injection(canonical_state_root.as_path())
        .expect("first restart launch should activate");
    for occurrence in 1..=2 {
        assert_eq!(
            runtime_one
                .checkpoint("provider.fixture.before_effect", "provider")
                .expect("pre-activation occurrence should be durable"),
            QaFaultDirective::Continue,
            "provider occurrence {occurrence} activated too early"
        );
    }
    let QaFaultDirective::Activate(first) = runtime_one
        .checkpoint("tool.before_effect", "tool")
        .expect("independent tool checkpoint should activate")
    else {
        panic!("tool checkpoint should return an activation");
    };
    assert_eq!(first.observed_occurrence, 1);
    assert_eq!(first.activation_sequence, 1);
    drop(runtime_one);

    let launch_two = write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "restart-launch-two",
        "66".repeat(32).as_str(),
    );
    let runtime_two = load_fault_injection(canonical_state_root.as_path())
        .expect("second restart launch should restore controller counters");
    let QaFaultDirective::Activate(second) = runtime_two
        .checkpoint("provider.fixture.before_effect", "provider")
        .expect("third provider occurrence should activate after restart")
    else {
        panic!("third provider occurrence should return an activation");
    };
    assert_eq!(second.observed_occurrence, 3);
    assert_eq!(second.activation_sequence, 2);

    let evidence = fs::read(evidence_path.as_path()).expect("restart evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch_two, &plan)
        .expect("restart-separated activations should validate");
    let activation_sequences = parsed
        .records()
        .iter()
        .filter_map(|record| match record {
            QaFaultEvidenceSidecarRecord::RuleActivated(activated) => {
                Some(activated.activation_sequence)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(activation_sequences, vec![1, 2]);
    let observed_occurrences = parsed
        .records()
        .iter()
        .filter_map(|record| match record {
            QaFaultEvidenceSidecarRecord::CheckpointObserved(observed) => Some(observed.occurrence),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(observed_occurrences, vec![1, 2]);
}

#[test]
fn loader_completes_joined_barrier_and_resumes_durable_release_cursor() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");

    let temporary = tempdir().expect("temporary state root should be created");
    let state_root = temporary.path().join("state");
    let private_root = state_root.join("qa-fault");
    ensure_owner_only_dir(private_root.as_path()).expect("private QA directory should be hardened");
    let canonical_state_root =
        fs::canonicalize(state_root.as_path()).expect("state root should canonicalize");
    let canonical_private_root =
        fs::canonicalize(private_root.as_path()).expect("private QA directory should canonicalize");

    let plan = worker_claim_barrier_plan();
    let activation = plan.activations.first().expect("barrier activation should exist");
    let plan_sha256 = plan.canonical_sha256().expect("barrier plan should hash");
    let plan_path = canonical_private_root.join("barrier-plan.json");
    fs::write(plan_path.as_path(), plan.canonical_json().expect("barrier plan should serialize"))
        .expect("barrier plan should be written");
    ensure_owner_only_file(plan_path.as_path()).expect("barrier plan should be hardened");
    let evidence_path = canonical_private_root.join("barrier-evidence.ndjson");
    let actors = ["claim-a".to_owned(), "claim-b".to_owned()];
    let prior_records = vec![
        QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 1,
            launch_id: "barrier-launch-one".to_owned(),
            plan_sha256: plan_sha256.clone(),
            capability_sha256: "a".repeat(64),
        }),
        QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 2,
            launch_id: "barrier-launch-one".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: actors[0].clone(),
            occurrence: activation.occurrence,
        }),
        QaFaultEvidenceSidecarRecord::BarrierJoined(QaFaultBarrierJoinedRecord {
            schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
            sequence: 3,
            launch_id: "barrier-launch-one".to_owned(),
            plan_sha256: plan_sha256.clone(),
            activation_id: activation.id.clone(),
            point_id: activation.point_id.clone(),
            actor: actors[1].clone(),
            occurrence: activation.occurrence,
        }),
    ];
    write_evidence_records(evidence_path.as_path(), prior_records.as_slice());

    let launch_two = write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "barrier-launch-two",
        "22".repeat(32).as_str(),
    );
    let runtime_two = load_fault_injection(canonical_state_root.as_path())
        .expect("loader should complete the durable full join set");
    let repaired_bytes =
        fs::read(evidence_path.as_path()).expect("repaired evidence should be readable");
    let repaired =
        parse_qa_fault_evidence_sidecar_ndjson(repaired_bytes.as_slice(), &launch_two, &plan)
            .expect("repaired barrier evidence should validate");
    let release_order = repaired
        .records()
        .iter()
        .find_map(|record| match record {
            QaFaultEvidenceSidecarRecord::RuleActivated(activated)
                if activated.activation_id == activation.id =>
            {
                Some(activated.release_order.clone())
            }
            _ => None,
        })
        .expect("loader should append the missing barrier activation");
    assert_eq!(release_order.len(), 2);
    assert_eq!(
        runtime_two
            .record_startup_orphan_recoveries()
            .expect("active barrier must be excluded from startup orphan recovery"),
        0
    );
    let active_barriers = runtime_two
        .probe_handle()
        .active_barriers()
        .expect("loader should expose the reconstructed active barrier");
    assert_eq!(active_barriers.len(), 1);
    assert_eq!(active_barriers[0].actors, actors.to_vec());
    assert_eq!(active_barriers[0].release_order.as_ref(), Some(&release_order));
    assert_eq!(
        runtime_two
            .checkpoint(activation.point_id.as_str(), release_order[0].as_str())
            .expect("first seeded actor should consume its release"),
        QaFaultDirective::Continue
    );
    assert!(matches!(
        runtime_two
            .probe_handle()
            .record_recovery(activation.id.as_str(), QaFaultRecoveryClass::RetrySucceeded),
        Err(QaFaultProbeError::AdapterFailure("qa_fault.activation_invalid"))
    ));
    drop(runtime_two);

    let _launch_three = write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "barrier-launch-three",
        "33".repeat(32).as_str(),
    );
    let runtime_three = load_fault_injection(canonical_state_root.as_path())
        .expect("third launch should restore the release cursor");
    let duplicate = runtime_three
        .checkpoint(activation.point_id.as_str(), release_order[0].as_str())
        .expect_err("durably released actor must not cross the effect again");
    assert!(duplicate.to_string().starts_with("qa_fault.barrier_actor_already_released"));
    assert_eq!(
        runtime_three
            .checkpoint(activation.point_id.as_str(), release_order[1].as_str())
            .expect("second seeded actor should consume the remaining release"),
        QaFaultDirective::Continue
    );
    runtime_three
        .probe_handle()
        .record_recovery(activation.id.as_str(), QaFaultRecoveryClass::RetrySucceeded)
        .expect("fully released barrier should accept recovery");
    drop(runtime_three);

    let launch_four = write_launch(
        canonical_state_root.as_path(),
        plan_path.as_path(),
        evidence_path.as_path(),
        plan_sha256.as_str(),
        "barrier-launch-four",
        "44".repeat(32).as_str(),
    );
    let runtime_four = load_fault_injection(canonical_state_root.as_path())
        .expect("recovered campaign should load without a stale barrier");
    assert_eq!(
        runtime_four
            .checkpoint(activation.point_id.as_str(), release_order[0].as_str())
            .expect("recovered barrier must not intercept the checkpoint"),
        QaFaultDirective::Continue
    );

    let final_bytes = fs::read(evidence_path.as_path()).expect("final evidence should be readable");
    let final_sidecar =
        parse_qa_fault_evidence_sidecar_ndjson(final_bytes.as_slice(), &launch_four, &plan)
            .expect("multi-restart release evidence should validate");
    let durable_releases = final_sidecar
        .records()
        .iter()
        .filter_map(|record| match record {
            QaFaultEvidenceSidecarRecord::BarrierReleased(released) => {
                Some((released.release_position, released.actor.as_str()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        durable_releases,
        vec![(1, release_order[0].as_str()), (2, release_order[1].as_str())]
    );
}

#[test]
fn stale_reclaim_barrier_resumes_from_durable_state_after_daemon_reload() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");

    let temporary = tempdir().expect("temporary stale barrier root should be created");
    let plan = worker_stale_reclaim_barrier_plan();
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &plan,
        "stale-barrier-launch-one",
        "77".repeat(32).as_str(),
    );
    let runtime_one = load_fault_injection(fixture.state_root.as_path())
        .expect("first stale barrier launch should activate");
    let policy = WorkerFleetPolicy::default();
    let mut manager = WorkerFleetManager::default();
    for (worker_id, run_id) in [
        ("worker-stale-a", "stale-run-a"),
        ("worker-stale-b", "stale-run-b"),
        ("worker-stale-c", "stale-run-c"),
    ] {
        let mut attestation = worker_attestation();
        attestation.worker_id = worker_id.to_owned();
        manager.register_worker(attestation, &policy, 2_000).unwrap();
        manager.assign_work(worker_id, worker_lease_request(run_id), &policy, 2_100).unwrap();
    }
    manager = manager.with_qa_fault_probe(runtime_one.probe_handle());

    let joined = manager
        .reap_expired_workers(2_601)
        .expect_err("first scan must join the stale barrier without mutation");
    assert!(matches!(
        joined,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 2 },
            ..
        }
    ));
    assert_eq!(manager.snapshot().active_leases, 3);
    drop(runtime_one);

    let launch_two = write_launch(
        fixture.state_root.as_path(),
        fixture.plan_path.as_path(),
        fixture.evidence_path.as_path(),
        plan.canonical_sha256().expect("stale barrier plan should hash").as_str(),
        "stale-barrier-launch-two",
        "88".repeat(32).as_str(),
    );
    let runtime_two = load_fault_injection(fixture.state_root.as_path())
        .expect("second launch should restore the stale barrier");
    assert_eq!(
        runtime_two
            .record_startup_orphan_recoveries()
            .expect("generic startup sweep must skip the active barrier"),
        0
    );
    let barriers = runtime_two
        .probe_handle()
        .active_barriers()
        .expect("reloaded barrier snapshot should be readable");
    assert_eq!(barriers.len(), 1);
    assert_eq!(barriers[0].actors.len(), 2);
    assert_eq!(barriers[0].release_order.as_ref().map(Vec::len), Some(2));
    manager = manager.with_qa_fault_probe(runtime_two.probe_handle());

    let events = manager
        .reap_expired_workers(2_601)
        .expect("reloaded adapter must consume all releases before reclaiming");
    assert_eq!(events.len(), 3);
    assert_eq!(manager.snapshot().active_leases, 0);

    let evidence = fs::read(fixture.evidence_path.as_path())
        .expect("stale barrier evidence should be readable");
    let sidecar = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch_two, &plan)
        .expect("reloaded stale barrier evidence should validate");
    assert_eq!(
        sidecar
            .records()
            .iter()
            .filter(|record| matches!(record, QaFaultEvidenceSidecarRecord::BarrierReleased(_)))
            .count(),
        2
    );
    assert!(matches!(
        sidecar.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery))
            if recovery.activation_id == "worker-stale-barrier"
                && recovery.recovery_class == QaFaultRecoveryClass::Reclaimed
    ));
}

#[test]
fn startup_recovery_mapping_distinguishes_tool_and_delivery_boundaries() {
    assert_eq!(
        startup_orphan_recovery_class("tool.before_effect"),
        Some(QaFaultRecoveryClass::FailedClosed)
    );
    assert_eq!(
        startup_orphan_recovery_class("tool.after_effect_before_ack"),
        Some(QaFaultRecoveryClass::OutcomeUnknown)
    );
    assert_eq!(
        startup_orphan_recovery_class("tool.after_ack_before_transition"),
        None,
        "an acknowledged tool still requires subsystem transition proof"
    );
    assert_eq!(
        startup_orphan_recovery_class("run.final_delivery.after_effect_before_ack"),
        Some(QaFaultRecoveryClass::OutcomeUnknown)
    );
    assert_eq!(
        startup_orphan_recovery_class("journal.after_effect_before_ack"),
        None,
        "a committed journal write still requires replay or deduplication proof"
    );
    assert_eq!(
        startup_orphan_recovery_class("managed_process.during_cleanup"),
        None,
        "cleanup success must be recorded by the process adapter after verification"
    );

    for descriptor in
        palyra_common::qa_fault_injection::QA_FAULT_POINT_REGISTRY_V1.iter().filter(|descriptor| {
            descriptor
                .supports(palyra_common::qa_fault_injection::QaFaultActionKind::TerminateProcess)
        })
    {
        if let Some(recovery_class) = startup_orphan_recovery_class(descriptor.id) {
            assert!(
                matches!(
                    recovery_class,
                    QaFaultRecoveryClass::FailedClosed | QaFaultRecoveryClass::OutcomeUnknown
                ),
                "startup inferred an action-bearing recovery for {}",
                descriptor.id
            );
            assert!(
                descriptor.supports_recovery(recovery_class),
                "{} does not support mapped recovery {recovery_class:?}",
                descriptor.id
            );
        }
    }
}

#[test]
fn immediate_adapter_recovery_is_durable_without_a_restart() {
    let temporary = tempdir().expect("temporary evidence root should be created");
    let plan = provider_malformed_plan();
    let plan_sha256 = plan.canonical_sha256().expect("test plan should hash");
    let plan_path = temporary.path().join("plan.json");
    fs::write(plan_path.as_path(), plan.canonical_json().expect("test plan should serialize"))
        .expect("test plan should be written");
    let evidence_path = temporary.path().join("evidence.ndjson");
    let launch = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: "immediate-launch".to_owned(),
        plan_path: plan_path.to_string_lossy().into_owned(),
        plan_sha256: plan_sha256.clone(),
        capability_sha256: "a".repeat(64),
        evidence_path: evidence_path.to_string_lossy().into_owned(),
        expires_at_unix_ms: current_unix_ms().saturating_add(60_000),
    };
    let loaded = QaFaultEvidenceSidecarRecord::LaunchLoaded(QaFaultLaunchLoadedRecord {
        schema_version: QA_FAULT_EVIDENCE_SIDECAR_SCHEMA_VERSION,
        sequence: 1,
        launch_id: launch.launch_id.clone(),
        plan_sha256: plan_sha256.clone(),
        capability_sha256: launch.capability_sha256.clone(),
    });
    let mut initial_evidence =
        serde_json::to_vec(&loaded).expect("launch evidence should serialize");
    initial_evidence.push(b'\n');
    fs::write(evidence_path.as_path(), initial_evidence)
        .expect("launch evidence should be written");

    let runtime = QaFaultRuntime::active(
        QaFaultProbeHandle::from_probe(
            DeterministicQaFaultController::new(plan.clone())
                .expect("test controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch: launch.clone(),
            path: evidence_path.clone(),
            next_sequence: 2,
            activated_rules: BTreeMap::new(),
            activation_actors: BTreeMap::new(),
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
    let QaFaultDirective::Activate(directive) = runtime
        .checkpoint("provider.fixture.after_effect_before_ack", "provider")
        .expect("provider checkpoint should activate")
    else {
        panic!("provider checkpoint should return an activation");
    };
    runtime.record_immediate_recovery(&directive).expect("immediate outcome should be recorded");

    let evidence = fs::read(evidence_path.as_path()).expect("evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
        .expect("no-restart evidence should validate");
    assert_eq!(parsed.records().len(), 3);
    let QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery) = &parsed.records()[2] else {
        panic!("third evidence record should classify recovery");
    };
    assert_eq!(recovery.recovery_class, QaFaultRecoveryClass::OutcomeUnknown);
    assert_eq!(
        runtime.probe.records().expect("controller records should load")[0].recovery_class,
        Some(QaFaultRecoveryClass::OutcomeUnknown)
    );
}

#[test]
fn pending_recovery_lookup_fails_loudly_when_evidence_lock_is_poisoned() {
    let temporary = tempdir().expect("temporary evidence root should be created");
    let plan = provider_malformed_plan();
    let plan_sha256 = plan.canonical_sha256().expect("test plan should hash");
    let launch = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: "poisoned-evidence-launch".to_owned(),
        plan_path: temporary.path().join("plan.json").to_string_lossy().into_owned(),
        plan_sha256,
        capability_sha256: "a".repeat(64),
        evidence_path: temporary.path().join("evidence.ndjson").to_string_lossy().into_owned(),
        expires_at_unix_ms: current_unix_ms().saturating_add(60_000),
    };
    let runtime =
        QaFaultRuntime::active_for_test(plan, launch, temporary.path().join("evidence.ndjson"))
            .expect("test runtime should initialize");
    let evidence = runtime.evidence.as_ref().expect("evidence should exist").clone();
    assert!(std::thread::spawn(move || {
        let _guard = evidence.lock().expect("evidence lock should start healthy");
        panic!("poison evidence lock for regression coverage");
    })
    .join()
    .is_err());

    let error = runtime
        .record_pending_recovery_for_point_actor(
            "provider.fixture.after_effect_before_ack",
            "provider",
            QaFaultRecoveryClass::OutcomeUnknown,
            "qa_fault.test_recovery",
        )
        .expect_err("poisoned evidence must not look like an absent activation");

    assert!(error.to_string().starts_with("qa_fault.recovery_failed"));
}

#[test]
fn immediate_recovery_mapping_matches_registry_capabilities() {
    for (point_id, expected) in [
        ("journal.before_effect", QaFaultRecoveryClass::FailedClosed),
        ("provider.fixture.before_intent", QaFaultRecoveryClass::FailedClosed),
        ("provider.fixture.after_intent", QaFaultRecoveryClass::FailedClosed),
        ("provider.fixture.before_effect", QaFaultRecoveryClass::FailedClosed),
        ("provider.fixture.after_effect_before_ack", QaFaultRecoveryClass::OutcomeUnknown),
        ("execution_backend.during_cleanup", QaFaultRecoveryClass::CleanupSucceeded),
        ("managed_process.before_effect", QaFaultRecoveryClass::FailedClosed),
        ("managed_process.during_cleanup", QaFaultRecoveryClass::CleanupSucceeded),
        ("tool.before_effect", QaFaultRecoveryClass::FailedClosed),
    ] {
        assert_eq!(immediate_recovery_class(point_id), Some(expected));
        let descriptor = palyra_common::qa_fault_injection::qa_fault_point_descriptor(point_id)
            .expect("mapped point should exist in the common registry");
        assert!(
            descriptor.supports_recovery(expected),
            "{point_id} does not support mapped recovery {expected:?}"
        );
    }
}
