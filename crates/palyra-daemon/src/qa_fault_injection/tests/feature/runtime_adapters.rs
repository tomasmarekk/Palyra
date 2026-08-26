//! Worker, connector, barrier, and exact-termination adapter tests.

use super::*;

#[test]
fn worker_claim_barrier_joins_nonblocking_and_retries_in_seeded_order() {
    let temporary = tempdir().expect("temporary evidence root should be created");
    let plan = worker_claim_barrier_plan();
    let plan_sha256 = plan.canonical_sha256().expect("barrier plan should hash");
    let plan_path = temporary.path().join("plan.json");
    fs::write(plan_path.as_path(), plan.canonical_json().expect("barrier plan should serialize"))
        .expect("barrier plan should be written");
    let evidence_path = temporary.path().join("evidence.ndjson");
    let launch = QaFaultLaunchDocument {
        schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
        launch_id: "barrier-launch".to_owned(),
        plan_path: plan_path.to_string_lossy().into_owned(),
        plan_sha256: plan_sha256.clone(),
        capability_sha256: "b".repeat(64),
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
                .expect("barrier controller should initialize"),
        ),
        QaFaultEvidenceState {
            launch: launch.clone(),
            path: evidence_path.clone(),
            next_sequence: 2,
            activated_rules: BTreeMap::new(),
            activation_actors: BTreeMap::new(),
            barrier_joins: BTreeMap::new(),
            barrier_join_points: BTreeMap::new(),
            barrier_participants: BTreeMap::from([("worker-claim-race".to_owned(), 2)]),
            barrier_release_orders: BTreeMap::new(),
            barrier_releases: BTreeMap::new(),
            observed_occurrences: BTreeMap::new(),
            occurrence_targets: occurrence_targets(&plan),
            recovered_rule_ids: BTreeSet::new(),
        },
        plan.seed,
    );
    let probe = runtime.probe_handle();
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(worker_attestation(), &policy, 2_000).expect("worker should register");

    let first_join = manager
        .assign_next_work(worker_lease_request("claim-a"), &policy, 2_100)
        .expect_err("first actor should join without crossing the effect");
    assert!(matches!(
        first_join,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 2 },
            ..
        }
    ));
    let premature_retry = manager
        .assign_next_work(worker_lease_request("claim-a"), &policy, 2_101)
        .expect_err("joined actor must wait for the complete durable set");
    assert!(matches!(
        premature_retry,
        WorkerLifecycleError::QaFaultProbe(QaFaultProbeError::AdapterFailure(
            "qa_fault.barrier_waiting_for_participants"
        ))
    ));
    let second_join = manager
        .assign_next_work(worker_lease_request("claim-b"), &policy, 2_102)
        .expect_err("second actor should complete the barrier without assigning work");
    assert!(matches!(
        second_join,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 2 },
            ..
        }
    ));

    let joined_evidence =
        fs::read(evidence_path.as_path()).expect("barrier evidence should be readable");
    let joined = parse_qa_fault_evidence_sidecar_ndjson(joined_evidence.as_slice(), &launch, &plan)
        .expect("complete barrier evidence should validate");
    let QaFaultEvidenceSidecarRecord::RuleActivated(activation) =
        joined.records().last().expect("activation record should exist")
    else {
        panic!("complete barrier must end with rule_activated");
    };
    assert_eq!(activation.actors.len(), 2);
    assert_eq!(activation.release_order.len(), 2);

    let mut winner = None;
    let mut successful_assignments = 0usize;
    for (index, actor) in activation.release_order.iter().enumerate() {
        match manager.assign_next_work(
            worker_lease_request(actor.as_str()),
            &policy,
            2_103 + i64::try_from(index).unwrap_or_default(),
        ) {
            Ok((lease, _)) => {
                successful_assignments = successful_assignments.saturating_add(1);
                winner = Some(lease.run_id);
            }
            Err(WorkerLifecycleError::NoAvailableWorker) => {}
            other => panic!("unexpected seeded claim result: {other:?}"),
        }
    }
    assert!(winner.is_some(), "one seeded actor should acquire the lease");
    assert_eq!(successful_assignments, 1, "the fresh claim must have exactly one winner");
    let released_evidence =
        fs::read(evidence_path.as_path()).expect("released evidence should be readable");
    let released =
        parse_qa_fault_evidence_sidecar_ndjson(released_evidence.as_slice(), &launch, &plan)
            .expect("released barrier evidence should validate");
    assert_eq!(
        released
            .records()
            .iter()
            .filter(|record| matches!(record, QaFaultEvidenceSidecarRecord::BarrierReleased(_)))
            .count(),
        2,
        "both auto-assignment actors must durably consume their release"
    );
    manager
        .record_qa_fault_recovery("worker-claim-race", QaFaultRecoveryClass::RetrySucceeded)
        .expect("worker recovery should flow through the durable daemon bridge");
    let recovered_evidence =
        fs::read(evidence_path.as_path()).expect("recovered evidence should be readable");
    let recovered =
        parse_qa_fault_evidence_sidecar_ndjson(recovered_evidence.as_slice(), &launch, &plan)
            .expect("barrier recovery evidence should validate");
    assert!(matches!(
        recovered.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
            if record.recovery_class == QaFaultRecoveryClass::RetrySucceeded
    ));
}

#[tokio::test]
async fn connector_non_terminating_fault_records_recovery_after_releasing_claim() {
    let temporary = tempdir().expect("temporary connector fault root should be created");
    let plan = connector_timeout_plan();
    let (runtime, launch, evidence_path) =
        connector_runtime_fixture(temporary.path(), &plan, "connector-timeout-launch");
    let (supervisor, _adapter, store) =
        connector_supervisor_fixture(temporary.path(), runtime.probe_handle());
    supervisor
        .enqueue_outbound(&connector_outbound("connector-timeout"))
        .expect("connector timeout row should enqueue");

    supervisor
        .drain_due_outbox(1)
        .await
        .expect_err("non-terminating timeout must surface its typed activation");

    let reclaimed = store
        .load_due_outbox(current_unix_ms().saturating_add(100), 1, Some("echo:qa"), false)
        .expect("failed-closed connector claim should be immediately reclaimable");
    assert_eq!(reclaimed.len(), 1);
    let evidence =
        fs::read(evidence_path.as_path()).expect("connector evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
        .expect("connector non-terminating evidence should validate");
    assert!(matches!(
        parsed.records().get(1),
        Some(QaFaultEvidenceSidecarRecord::RuleActivated(record))
            if record.activation_id == "connector-timeout"
                && record.point_id == "connector.outbox.before_effect"
    ));
    assert!(matches!(
        parsed.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
            if record.activation_id == "connector-timeout"
                && record.recovery_class == QaFaultRecoveryClass::FailedClosed
    ));
}

#[tokio::test]
async fn connector_batch_barrier_records_seeded_releases_before_final_transitions() {
    let temporary = tempdir().expect("temporary connector barrier root should be created");
    let plan = connector_barrier_plan();
    let (runtime, launch, evidence_path) =
        connector_runtime_fixture(temporary.path(), &plan, "connector-barrier-launch");
    let (supervisor, adapter, _store) =
        connector_supervisor_fixture(temporary.path(), runtime.probe_handle());
    for envelope_id in ["connector-barrier-a", "connector-barrier-b"] {
        supervisor
            .enqueue_outbound(&connector_outbound(envelope_id))
            .expect("connector barrier row should enqueue");
    }

    let outcome = supervisor
        .drain_due_outbox(2)
        .await
        .expect("full connector barrier batch should release and deliver");

    assert_eq!(outcome.delivered, 2);
    assert_eq!(supervisor.drain_due_outbox(2).await.unwrap().processed, 0);
    let evidence =
        fs::read(evidence_path.as_path()).expect("connector evidence should be readable");
    let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
        .expect("connector barrier evidence should validate");
    let joins = parsed
        .records()
        .iter()
        .filter_map(|record| match record {
            QaFaultEvidenceSidecarRecord::BarrierJoined(joined) => Some(joined.actor.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(joins, ["outbox-1", "outbox-2"]);
    let activation = parsed
        .records()
        .iter()
        .find_map(|record| match record {
            QaFaultEvidenceSidecarRecord::RuleActivated(activation) => Some(activation),
            _ => None,
        })
        .expect("connector barrier activation should be recorded");
    let releases = parsed
        .records()
        .iter()
        .filter_map(|record| match record {
            QaFaultEvidenceSidecarRecord::BarrierReleased(released) => Some(released.actor.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(releases, activation.release_order);
    let expected_delivery_order = activation
        .release_order
        .iter()
        .map(|actor| match actor.as_str() {
            "outbox-1" => "connector-barrier-a".to_owned(),
            "outbox-2" => "connector-barrier-b".to_owned(),
            other => panic!("unexpected connector barrier actor {other}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(adapter.delivery_order(), expected_delivery_order);
    assert!(matches!(
        parsed.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
            if record.activation_id == "connector-batch-barrier"
                && record.recovery_class == QaFaultRecoveryClass::Resumed
    ));
}

#[test]
fn connector_reclaims_released_barrier_claims_before_recording_restart_recovery() {
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("connector restart executor should initialize");
    executor.block_on(async {
        let temporary = tempdir().expect("temporary connector restart root should be created");
        let plan = connector_barrier_plan();
        let fixture = prepare_loader_fixture(
            temporary.path(),
            &plan,
            "connector-barrier-first",
            "aa".repeat(32).as_str(),
        );
        let runtime_one = load_fault_injection(fixture.state_root.as_path())
            .expect("first connector barrier launch should activate");
        let (supervisor_one, adapter_one, store_one) =
            connector_supervisor_fixture(temporary.path(), runtime_one.probe_handle());
        for envelope_id in ["connector-restart-a", "connector-restart-b"] {
            supervisor_one
                .enqueue_outbound(&connector_outbound(envelope_id))
                .expect("connector restart row should enqueue");
        }
        let claimed = store_one
            .load_due_outbox(current_unix_ms(), 2, Some("echo:qa"), false)
            .expect("connector restart rows should be claimed");
        assert_eq!(claimed.len(), 2);
        let actors =
            claimed.iter().map(|entry| format!("outbox-{}", entry.outbox_id)).collect::<Vec<_>>();
        let actor_envelopes = claimed
            .iter()
            .map(|entry| (format!("outbox-{}", entry.outbox_id), entry.envelope_id.clone()))
            .collect::<BTreeMap<_, _>>();
        for (entry, actor) in claimed.iter().zip(&actors) {
            store_one
                .mark_outbox_delivery_intent_started(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    current_unix_ms(),
                )
                .expect("connector restart intent should be durable before the barrier");
            assert!(matches!(
                runtime_one
                    .checkpoint("connector.outbox.batch_before_effect", actor.as_str())
                    .expect("connector restart actor should join the barrier"),
                QaFaultDirective::Activate(_)
            ));
        }
        let active = runtime_one
            .active_barrier_snapshots()
            .expect("connector restart barrier should be inspectable")
            .into_iter()
            .next()
            .expect("connector restart barrier should be active");
        let release_order = active
            .release_order
            .expect("full connector restart barrier should have a seeded release order");
        for actor in &release_order {
            assert_eq!(
                runtime_one
                    .checkpoint("connector.outbox.batch_before_effect", actor.as_str())
                    .expect("connector restart actor release should be durable"),
                QaFaultDirective::Continue
            );
        }
        assert!(adapter_one.delivery_order().is_empty());
        drop(claimed);
        drop(supervisor_one);
        drop(store_one);
        drop(runtime_one);

        let launch_two = write_launch(
            fixture.state_root.as_path(),
            fixture.plan_path.as_path(),
            fixture.evidence_path.as_path(),
            plan.canonical_sha256().expect("connector restart plan should hash").as_str(),
            "connector-barrier-second",
            "bb".repeat(32).as_str(),
        );
        let runtime_two = load_fault_injection(fixture.state_root.as_path())
            .expect("second connector barrier launch should resume");
        assert_eq!(
            runtime_two
                .record_startup_orphan_recoveries()
                .expect("active connector barrier must not be classified as an orphan"),
            0
        );
        let (supervisor_two, adapter_two, _store_two) =
            connector_supervisor_fixture(temporary.path(), runtime_two.probe_handle());

        supervisor_two
            .drain_due_outbox(2)
            .await
            .expect_err("zero-due restart pass must reclaim the abandoned barrier claims");
        assert!(adapter_two.delivery_order().is_empty());
        let unrecovered_evidence = fs::read(fixture.evidence_path.as_path())
            .expect("unrecovered connector restart evidence should be readable");
        let unrecovered = parse_qa_fault_evidence_sidecar_ndjson(
            unrecovered_evidence.as_slice(),
            &launch_two,
            &plan,
        )
        .expect("unrecovered connector restart evidence should remain valid");
        assert!(!unrecovered
            .records()
            .iter()
            .any(|record| matches!(record, QaFaultEvidenceSidecarRecord::RecoveryRecorded(_))));

        let outcome = supervisor_two
            .drain_due_outbox(2)
            .await
            .expect("reclaimed connector barrier rows should transition on the next pass");
        assert_eq!(outcome.delivered, 2);
        let expected_delivery_order = release_order
            .iter()
            .map(|actor| {
                actor_envelopes
                    .get(actor)
                    .expect("released connector actor should map to its durable outbox row")
                    .clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(adapter_two.delivery_order(), expected_delivery_order);
        let recovered_evidence = fs::read(fixture.evidence_path.as_path())
            .expect("recovered connector restart evidence should be readable");
        let recovered = parse_qa_fault_evidence_sidecar_ndjson(
            recovered_evidence.as_slice(),
            &launch_two,
            &plan,
        )
        .expect("recovered connector restart evidence should validate");
        assert!(matches!(
            recovered.records().last(),
            Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
                if record.activation_id == "connector-batch-barrier"
                    && record.recovery_class == QaFaultRecoveryClass::Resumed
        ));
    });
}

#[test]
fn bridge_terminate_process_exits_after_durable_connector_activation() {
    if std::env::var(BRIDGE_TERMINATE_CHILD_ENV).ok().as_deref() == Some("1") {
        run_bridge_terminate_child();
    }
    let _environment_lock = crate::test_env::lock();
    let _environment_restore = EnvironmentRestore::capture(&[
        QA_FAULT_LAUNCH_PATH_ENV,
        QA_FAULT_CAPABILITY_PATH_ENV,
        QA_LAB_MODE_ENV,
    ]);
    std::env::set_var(QA_LAB_MODE_ENV, "preview_only");
    let temporary = tempdir().expect("temporary bridge root should be created");
    let plan = connector_terminate_plan();
    let fixture = prepare_loader_fixture(
        temporary.path(),
        &plan,
        "bridge-terminate-first",
        "cc".repeat(32).as_str(),
    );
    let launch_relative = fixture
        .launch_path
        .strip_prefix(fixture.state_root.as_path())
        .expect("terminate launch should stay inside the state root");
    let capability_relative = fixture
        .capability_path
        .strip_prefix(fixture.state_root.as_path())
        .expect("terminate capability should stay inside the state root");
    let output = Command::new(
        std::env::current_exe().expect("current test executable should resolve"),
    )
    .args([
        "--exact",
        "qa_fault_injection::tests::feature::runtime_adapters::bridge_terminate_process_exits_after_durable_connector_activation",
        "--nocapture",
    ])
    .env(BRIDGE_TERMINATE_CHILD_ENV, "1")
    .env(BRIDGE_TERMINATE_ROOT_ENV, temporary.path())
    .env(QA_LAB_MODE_ENV, "preview_only")
    .env(QA_FAULT_LAUNCH_PATH_ENV, launch_relative)
    .env(QA_FAULT_CAPABILITY_PATH_ENV, capability_relative)
    .output()
    .expect("terminate bridge child should launch");
    assert_eq!(
        output.status.code(),
        Some(palyra_common::qa_fault_injection::QA_FAULT_TERMINATE_EXIT_CODE),
        "child stderr: {}",
        String::from_utf8_lossy(output.stderr.as_slice())
    );

    let activated_evidence = fs::read(fixture.evidence_path.as_path())
        .expect("terminate activation evidence should be readable");
    let activated = parse_qa_fault_evidence_sidecar_ndjson(
        activated_evidence.as_slice(),
        &fixture.launch,
        &plan,
    )
    .expect("terminate activation evidence should validate");
    assert_eq!(activated.records().len(), 2);
    assert!(matches!(
        activated.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RuleActivated(record))
            if record.activation_id == "connector-terminate"
                && record.point_id == "connector.outbox.before_intent"
    ));

    let connector_db = temporary.path().join("connector-runtime.sqlite3");
    let store = ConnectorStore::open(connector_db.as_path())
        .expect("terminated connector store should reopen");
    let still_claimed = store
        .load_due_outbox(current_unix_ms().saturating_add(100), 1, Some("echo:qa"), false)
        .expect("terminated connector claim should remain fenced before startup recovery");
    assert!(
        still_claimed.is_empty(),
        "the exact crash boundary must not release its claim before restart proof"
    );
    drop(store);

    let launch_two = write_launch(
        fixture.state_root.as_path(),
        fixture.plan_path.as_path(),
        fixture.evidence_path.as_path(),
        plan.canonical_sha256().expect("terminate plan should hash").as_str(),
        "bridge-terminate-second",
        "dd".repeat(32).as_str(),
    );
    let runtime_two = load_fault_injection(fixture.state_root.as_path())
        .expect("second terminate launch should restore pending activation evidence");
    let (interrupted_recovery, _adapter, interrupted_store) =
        connector_supervisor_fixture(temporary.path(), runtime_two.probe_handle());
    assert_eq!(
        interrupted_recovery
            .reconcile_pending_qa_fault_actor("connector.outbox.before_intent", "outbox-1",)
            .expect("first startup should commit the exact store transition"),
        QaFaultRecoveryClass::FailedClosed
    );
    drop(interrupted_recovery);
    drop(interrupted_store);
    let store_only_evidence = fs::read(fixture.evidence_path.as_path())
        .expect("store-only recovery evidence should be readable");
    let store_only =
        parse_qa_fault_evidence_sidecar_ndjson(store_only_evidence.as_slice(), &launch_two, &plan)
            .expect("store-only recovery evidence should validate");
    assert_eq!(store_only.records().len(), 3);
    assert!(matches!(
        store_only.records().last(),
        Some(QaFaultEvidenceSidecarRecord::LaunchLoaded(_))
    ));

    // A process can stop after the store commit but before the sidecar append. The real
    // startup path must adopt that exact transition idempotently and finish the evidence.
    let platform = ChannelPlatform::initialize_with_qa_fault_probe(
        "http://127.0.0.1:7443".to_owned(),
        GatewayAuthConfig {
            require_auth: false,
            admin_token: None,
            connector_token: None,
            connector_allowed_channels: Vec::new(),
            bound_principal: None,
        },
        connector_db.clone(),
        MediaRuntimeConfig::default(),
        runtime_two.probe_handle(),
    )
    .expect("connector platform should reopen the terminated store");
    assert_eq!(
        platform
            .reconcile_pending_qa_fault_recoveries(&runtime_two)
            .expect("connector startup recovery should prove the exact durable fence"),
        1
    );
    assert_eq!(
        runtime_two
            .record_startup_orphan_recoveries()
            .expect("generic startup sweep should remain a no-op for connector recovery"),
        0
    );

    let store =
        ConnectorStore::open(connector_db).expect("recovered connector store should reopen");
    let reclaimable = store
        .load_due_outbox(current_unix_ms().saturating_add(100), 1, Some("echo:qa"), false)
        .expect("failed-closed connector claim should be immediately reclaimable");
    assert_eq!(reclaimable.len(), 1);
    assert_eq!(reclaimable[0].effect_state, OutboxEffectState::Ready);

    let recovered_evidence = fs::read(fixture.evidence_path.as_path())
        .expect("recovered terminate evidence should be readable");
    let recovered =
        parse_qa_fault_evidence_sidecar_ndjson(recovered_evidence.as_slice(), &launch_two, &plan)
            .expect("recovered terminate evidence should validate");
    assert_eq!(recovered.records().len(), 4);
    assert!(matches!(
        recovered.records().get(1),
        Some(QaFaultEvidenceSidecarRecord::RuleActivated(record))
            if record.activation_id == "connector-terminate"
                && record.point_id == "connector.outbox.before_intent"
    ));
    assert!(matches!(
        recovered.records().last(),
        Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
            if record.activation_id == "connector-terminate"
                && record.recovery_class == QaFaultRecoveryClass::FailedClosed
    ));
}
