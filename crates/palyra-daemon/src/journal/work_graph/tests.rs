//! Durable work graph creation, replay, and transition tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
    thread,
};

use crate::domain::work_graph::{
    ClaimReadyWorkItemOutcome, ClaimReadyWorkItemRequest, StaleReclaimDecision,
    StaleReclaimRequest, WorkBudgetV1, WorkClaimAuthority, WorkClaimSettlementOutcome,
    WorkClaimSettlementRequest, WorkGraphCommentCreateRequest, WorkGraphConcurrencyPolicy,
    WorkGraphCreateRequest, WorkGraphOwnerScopeV1, WorkGraphReviewDecision, WorkGraphReviewRequest,
    WorkGraphState, WorkItemHandoffCreateRequest, WorkItemHeartbeatOutcome,
    WorkItemHeartbeatRequest, WorkItemSideEffectFenceOutcome, WorkItemSideEffectFenceRequest,
    WorkItemSpecV1, WorkItemState, WorkItemTransitionRequest, WorkResourceClass,
    WorkRuntimeLiveness, WorkSideEffectFenceState, WorkVerificationState,
    MAX_WORK_HANDOFF_RESULT_BYTES,
};

use super::*;

fn store(path: PathBuf) -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path: path,
        hash_chain_enabled: true,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .expect("journal should open")
}

fn item(id: &str, dependencies: &[&str]) -> WorkItemSpecV1 {
    WorkItemSpecV1 {
        work_item_id: id.to_owned(),
        title: id.to_owned(),
        description: format!("execute {id}"),
        priority: 1,
        capability_profile: "general".to_owned(),
        dependency_ids: dependencies.iter().map(|value| (*value).to_owned()).collect(),
        compensates_work_item_id: None,
        serialization_key: None,
        resource_class: WorkResourceClass::IoHeavy,
        provider_profile: None,
        workspace_scope: None,
        budget: WorkBudgetV1 { max_turns: Some(2), ..WorkBudgetV1::default() },
        max_runtime_ms: 30_000,
        requires_review: false,
    }
}

fn request() -> WorkGraphCreateRequest {
    WorkGraphCreateRequest {
        graph_id: "graph-1".to_owned(),
        owner: WorkGraphOwnerScopeV1 {
            principal: "principal-1".to_owned(),
            device_id: "device-1".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some("session-1".to_owned()),
            origin_run_id: Some("run-1".to_owned()),
        },
        objective_id: Some("objective-1".to_owned()),
        routine_id: None,
        flow_id: None,
        flow_step_id: None,
        budget: WorkBudgetV1 { max_turns: Some(10), ..WorkBudgetV1::default() },
        concurrency_policy: WorkGraphConcurrencyPolicy::default(),
        items: vec![item("a", &[]), item("b", &["a"])],
        actor_principal: "principal-1".to_owned(),
    }
}

fn claim(
    store: &JournalStore,
    work_item_id: &str,
    expected_item_revision: u64,
) -> crate::domain::work_graph::WorkItemClaimGrant {
    let outcome = store
        .claim_ready_work_item(&claim_request(work_item_id, expected_item_revision, "worker-1"))
        .expect("claim should be evaluated");
    let ClaimReadyWorkItemOutcome::Granted(grant) = outcome else {
        panic!("ready item should be claimed");
    };
    *grant
}

fn claim_request(
    work_item_id: &str,
    expected_item_revision: u64,
    worker_id: &str,
) -> ClaimReadyWorkItemRequest {
    ClaimReadyWorkItemRequest {
        graph_id: "graph-1".to_owned(),
        work_item_id: Some(work_item_id.to_owned()),
        expected_item_revision: Some(expected_item_revision),
        worker_id: worker_id.to_owned(),
        worker_principal: "principal-1".to_owned(),
        authorized_owner_principal: "principal-1".to_owned(),
        capability_profiles: BTreeSet::from(["general".to_owned()]),
        provider_backpressure_profiles: BTreeSet::new(),
        memory_pressure: false,
        resource_lease_id: Some(format!("resource-{worker_id}")),
        runtime_instance_id: format!("runtime-{worker_id}"),
        process_start_token: format!("process-{worker_id}"),
        lease_ttl_ms: 5_000,
    }
}

fn authority(grant: &crate::domain::work_graph::WorkItemClaimGrant) -> WorkClaimAuthority {
    WorkClaimAuthority {
        graph_id: grant.item.graph_id.clone(),
        work_item_id: grant.item.work_item_id.clone(),
        worker_id: grant.claim.worker_id.clone(),
        generation: grant.claim.generation,
        token: grant.token.clone(),
    }
}

fn expire_claim(store: &JournalStore, work_item_id: &str) {
    let guard = store.connection.lock().expect("journal lock should be available");
    guard
        .execute(
            r#"
                UPDATE work_graph_items
                SET claim_expires_at_unix_ms = 1
                WHERE graph_ulid = 'graph-1' AND work_item_ulid = ?1
            "#,
            params![work_item_id],
        )
        .expect("claim should expire");
}

fn start_claim(
    store: &JournalStore,
    grant: &crate::domain::work_graph::WorkItemClaimGrant,
) -> crate::domain::work_graph::WorkItemRecordV1 {
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: grant.item.graph_id.clone(),
            work_item_id: grant.item.work_item_id.clone(),
            expected_revision: grant.item.revision,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.test.running".to_owned(),
            actor_principal: grant.claim.worker_principal.clone(),
        })
        .expect("claim should enter running")
        .item
}

fn record_handoff(
    store: &JournalStore,
    grant: &crate::domain::work_graph::WorkItemClaimGrant,
    expected_item_revision: u64,
    verification_state: WorkVerificationState,
    structured_result: Value,
) -> crate::domain::work_graph::WorkItemHandoffCommitOutcome {
    store
        .record_work_item_handoff(&WorkItemHandoffCreateRequest {
            authority: authority(grant),
            expected_item_revision,
            actor_principal: grant.claim.worker_principal.clone(),
            summary: format!("bounded result for {}", grant.item.work_item_id),
            structured_result,
            evidence_refs: vec![format!("evidence:{}", grant.item.work_item_id)],
            artifact_refs: vec![format!("artifact:{}", grant.item.work_item_id)],
            verification_state,
        })
        .expect("handoff should commit")
}

#[test]
fn graph_creation_and_restart_projection_preserve_dag() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let first = store(path.clone());
    let created = first.create_work_graph(&request()).expect("graph should be created");
    assert_eq!(created.graph.revision, 1);
    assert_eq!(created.items[0].state, WorkItemState::Ready);
    assert_eq!(created.items[1].state, WorkItemState::BlockedByDependencies);
    drop(first);

    let reopened = store(path);
    let restored = reopened
        .work_graph_snapshot("graph-1")
        .expect("projection should load")
        .expect("graph should exist");
    assert_eq!(restored, created);
    assert_eq!(reopened.work_graph_events("graph-1", 10).expect("events should load").len(), 3);
}

#[test]
fn expected_revision_and_verification_gate_transitions() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let created = store.create_work_graph(&request()).expect("graph should be created");
    let a = &created.items[0];
    let grant = claim(&store, "a", a.revision);
    let running = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: grant.item.revision,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("claim should start");
    let authority = WorkClaimAuthority {
        graph_id: "graph-1".to_owned(),
        work_item_id: "a".to_owned(),
        worker_id: grant.claim.worker_id.clone(),
        generation: grant.claim.generation,
        token: grant.token.clone(),
    };
    let error = store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority.clone(),
            expected_item_revision: running.item.revision,
            target_state: WorkItemState::Succeeded,
            verification_state: WorkVerificationState::Pending,
            result_sha256: "ab".repeat(32),
            reason_code: "work_graph.complete_requested".to_owned(),
            actor_principal: "worker".to_owned(),
        })
        .expect_err("unverified success must fail");
    assert!(matches!(error, JournalError::InvalidWorkGraph { .. }));

    let completed = store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority,
            expected_item_revision: running.item.revision,
            target_state: WorkItemState::Succeeded,
            verification_state: WorkVerificationState::Verified,
            result_sha256: "cd".repeat(32),
            reason_code: "work_graph.host_verified".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("verified success should commit");
    assert!(matches!(completed, WorkClaimSettlementOutcome::Applied { .. }));
    let snapshot = store.work_graph_snapshot("graph-1").unwrap().unwrap();
    assert_eq!(snapshot.items[1].state, WorkItemState::Ready);
    assert_eq!(snapshot.graph.state, WorkGraphState::Active);

    let stale = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: 1,
            target_state: WorkItemState::Archived,
            verification_state: None,
            reason_code: "work_graph.stale_revision".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect_err("stale revision must fail");
    assert!(matches!(stale, JournalError::WorkGraphRevisionConflict { .. }));
}

#[test]
fn failed_dependency_activates_compensation_and_blocks_dependents() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    let mut compensation = item("compensate-a", &[]);
    compensation.compensates_work_item_id = Some("a".to_owned());
    create.items.push(compensation);
    let created = store.create_work_graph(&create).expect("graph should be created");
    let a = created.items.iter().find(|item| item.work_item_id == "a").unwrap();
    let grant = claim(&store, "a", a.revision);
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: grant.item.revision,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .unwrap();
    let failed = store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: WorkClaimAuthority {
                graph_id: "graph-1".to_owned(),
                work_item_id: "a".to_owned(),
                worker_id: grant.claim.worker_id,
                generation: grant.claim.generation,
                token: grant.token,
            },
            expected_item_revision: grant.item.revision + 1,
            target_state: WorkItemState::Failed,
            verification_state: WorkVerificationState::Unverified,
            result_sha256: "ef".repeat(32),
            reason_code: "work_graph.execution_failed".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .unwrap();
    assert!(matches!(failed, WorkClaimSettlementOutcome::Applied { .. }));
    let snapshot = store.work_graph_snapshot("graph-1").unwrap().unwrap();
    assert_eq!(
        snapshot.items.iter().find(|item| item.work_item_id == "b").unwrap().state,
        WorkItemState::Failed
    );
    assert_eq!(
        snapshot.items.iter().find(|item| item.work_item_id == "compensate-a").unwrap().state,
        WorkItemState::Ready
    );
}

#[test]
fn concurrent_claimers_have_exactly_one_winner() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = Arc::new(store(directory.path().join("journal.sqlite3")));
    store.create_work_graph(&request()).expect("graph should be created");
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for worker in ["worker-a", "worker-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            store
                .claim_ready_work_item(&ClaimReadyWorkItemRequest {
                    graph_id: "graph-1".to_owned(),
                    work_item_id: Some("a".to_owned()),
                    expected_item_revision: Some(1),
                    worker_id: worker.to_owned(),
                    worker_principal: "principal-1".to_owned(),
                    authorized_owner_principal: "principal-1".to_owned(),
                    capability_profiles: BTreeSet::from(["general".to_owned()]),
                    provider_backpressure_profiles: BTreeSet::new(),
                    memory_pressure: false,
                    resource_lease_id: Some(format!("resource-{worker}")),
                    runtime_instance_id: format!("runtime-{worker}"),
                    process_start_token: format!("process-{worker}"),
                    lease_ttl_ms: 30_000,
                })
                .expect("claim race should settle")
        }));
    }
    barrier.wait();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("claimer should not panic"))
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimReadyWorkItemOutcome::Granted(_)))
            .count(),
        1
    );
}

#[test]
fn heartbeat_and_reclaim_are_generation_fenced() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    store.create_work_graph(&request()).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    let heartbeat = store
        .heartbeat_work_item(&WorkItemHeartbeatRequest {
            authority: authority(&grant),
            extend_by_ms: 5_000,
        })
        .expect("heartbeat should be evaluated");
    let WorkItemHeartbeatOutcome::Renewed(renewed) = heartbeat else {
        panic!("current generation should renew");
    };
    assert!(renewed.expires_at_unix_ms > grant.claim.expires_at_unix_ms);

    let decision = store
        .reclaim_stale_work_item(&StaleReclaimRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_item_revision: renewed.record_revision,
            expected_generation: renewed.generation,
            runtime_instance_id: renewed.runtime_instance_id,
            process_start_token: renewed.process_start_token,
            liveness: WorkRuntimeLiveness::Dead,
            observed_side_effect_fence: WorkSideEffectFenceState::Clear,
            actor_principal: "system:reclaimer".to_owned(),
        })
        .expect("reclaim should be evaluated");
    assert!(matches!(decision, StaleReclaimDecision::NotExpired { .. }));
}

#[test]
fn expired_heartbeat_reclaim_race_has_one_authority() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = Arc::new(store(directory.path().join("journal.sqlite3")));
    store.create_work_graph(&request()).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    expire_claim(&store, "a");
    let barrier = Arc::new(Barrier::new(3));

    let heartbeat_store = Arc::clone(&store);
    let heartbeat_barrier = Arc::clone(&barrier);
    let heartbeat_authority = authority(&grant);
    let heartbeat = thread::spawn(move || {
        heartbeat_barrier.wait();
        heartbeat_store
            .heartbeat_work_item(&WorkItemHeartbeatRequest {
                authority: heartbeat_authority,
                extend_by_ms: 5_000,
            })
            .expect("heartbeat race should settle")
    });

    let reclaim_store = Arc::clone(&store);
    let reclaim_barrier = Arc::clone(&barrier);
    let reclaim_claim = grant.claim.clone();
    let reclaim = thread::spawn(move || {
        reclaim_barrier.wait();
        reclaim_store
            .reclaim_stale_work_item(&StaleReclaimRequest {
                graph_id: "graph-1".to_owned(),
                work_item_id: "a".to_owned(),
                expected_item_revision: reclaim_claim.record_revision,
                expected_generation: reclaim_claim.generation,
                runtime_instance_id: reclaim_claim.runtime_instance_id,
                process_start_token: reclaim_claim.process_start_token,
                liveness: WorkRuntimeLiveness::Dead,
                observed_side_effect_fence: WorkSideEffectFenceState::Clear,
                actor_principal: "system:reclaimer".to_owned(),
            })
            .expect("reclaim race should settle")
    });
    barrier.wait();
    let heartbeat = heartbeat.join().expect("heartbeat should not panic");
    let reclaim = reclaim.join().expect("reclaimer should not panic");
    assert!(matches!(
        heartbeat,
        WorkItemHeartbeatOutcome::Expired { .. } | WorkItemHeartbeatOutcome::StaleAuthority { .. }
    ));
    assert!(matches!(reclaim, StaleReclaimDecision::Reclaimed { .. }));
    let item = store.work_graph_snapshot("graph-1").unwrap().unwrap().items.remove(0);
    assert_eq!(item.state, WorkItemState::Ready);
    assert!(item.claim.is_none());
}

#[test]
fn expired_live_worker_is_deferred_and_pid_reuse_is_reclaimed() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    store.create_work_graph(&request()).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    expire_claim(&store, "a");
    let base = StaleReclaimRequest {
        graph_id: "graph-1".to_owned(),
        work_item_id: "a".to_owned(),
        expected_item_revision: grant.item.revision,
        expected_generation: grant.claim.generation,
        runtime_instance_id: grant.claim.runtime_instance_id.clone(),
        process_start_token: grant.claim.process_start_token.clone(),
        liveness: WorkRuntimeLiveness::Alive,
        observed_side_effect_fence: WorkSideEffectFenceState::Clear,
        actor_principal: "system:reclaimer".to_owned(),
    };
    assert!(matches!(
        store.reclaim_stale_work_item(&base).unwrap(),
        StaleReclaimDecision::DeferredLive { .. }
    ));
    let reclaimed = store
        .reclaim_stale_work_item(&StaleReclaimRequest {
            liveness: WorkRuntimeLiveness::ProcessIdentityReused,
            ..base
        })
        .expect("PID reuse should be reclaimable");
    assert!(matches!(reclaimed, StaleReclaimDecision::Reclaimed { .. }));
}

#[test]
fn unknown_side_effect_requires_review_and_late_success_is_orphaned() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    store.create_work_graph(&request()).expect("graph should be created");
    let old = claim(&store, "a", 1);
    expire_claim(&store, "a");
    let review = store
        .reclaim_stale_work_item(&StaleReclaimRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_item_revision: old.item.revision,
            expected_generation: old.claim.generation,
            runtime_instance_id: old.claim.runtime_instance_id.clone(),
            process_start_token: old.claim.process_start_token.clone(),
            liveness: WorkRuntimeLiveness::Dead,
            observed_side_effect_fence: WorkSideEffectFenceState::Unknown,
            actor_principal: "system:reclaimer".to_owned(),
        })
        .expect("unknown effect should settle to review");
    assert!(matches!(review, StaleReclaimDecision::RequiresReview { .. }));
    let reviewed_item = store.work_graph_snapshot("graph-1").unwrap().unwrap().items.remove(0);
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: reviewed_item.revision,
            target_state: WorkItemState::Ready,
            verification_state: Some(WorkVerificationState::Rejected),
            reason_code: "work_graph.review.rework".to_owned(),
            actor_principal: "reviewer".to_owned(),
        })
        .expect("review should permit rework");
    let current = claim(&store, "a", reviewed_item.revision + 1);
    let orphan = store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority(&old),
            expected_item_revision: old.item.revision,
            target_state: WorkItemState::Succeeded,
            verification_state: WorkVerificationState::Verified,
            result_sha256: "12".repeat(32),
            reason_code: "work_graph.late_success".to_owned(),
            actor_principal: "worker-1".to_owned(),
        })
        .expect("late result should be recorded");
    assert!(matches!(orphan, WorkClaimSettlementOutcome::Orphaned { .. }));
    let snapshot = store.work_graph_snapshot("graph-1").unwrap().unwrap();
    let item = snapshot.items.iter().find(|item| item.work_item_id == "a").unwrap();
    assert_eq!(item.claim.as_ref().unwrap().generation, current.claim.generation);
    assert_eq!(item.state, WorkItemState::Claimed);
}

#[test]
fn claim_authority_survives_restart() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let first = store(path.clone());
    first.create_work_graph(&request()).expect("graph should be created");
    let grant = claim(&first, "a", 1);
    drop(first);

    let reopened = store(path);
    let item = reopened
        .work_graph_snapshot("graph-1")
        .unwrap()
        .unwrap()
        .items
        .into_iter()
        .find(|item| item.work_item_id == "a")
        .unwrap();
    assert_eq!(item.claim.as_ref().unwrap(), &grant.claim);
}

#[test]
fn side_effect_fence_and_claim_diagnostics_are_generation_fenced() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    store.create_work_graph(&request()).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    let updated = store
        .record_work_item_side_effect_fence(&WorkItemSideEffectFenceRequest {
            authority: authority(&grant),
            expected_item_revision: grant.item.revision,
            state: WorkSideEffectFenceState::InFlight,
            actor_principal: "worker-1".to_owned(),
        })
        .expect("side-effect fence should be evaluated");
    let WorkItemSideEffectFenceOutcome::Updated(updated) = updated else {
        panic!("current generation should update its side-effect fence");
    };
    assert_eq!(updated.side_effect_fence, WorkSideEffectFenceState::InFlight);

    let downgrade = store
        .record_work_item_side_effect_fence(&WorkItemSideEffectFenceRequest {
            authority: authority(&grant),
            expected_item_revision: updated.record_revision,
            state: WorkSideEffectFenceState::Clear,
            actor_principal: "worker-1".to_owned(),
        })
        .expect_err("side-effect knowledge must not be downgraded");
    assert!(matches!(downgrade, JournalError::InvalidWorkGraph { .. }));

    let diagnostics = store
        .work_graph_claim_diagnostics("graph-1")
        .expect("diagnostics should load")
        .expect("graph should exist");
    assert_eq!(diagnostics.active_claim_count, 1);
    assert_eq!(diagnostics.total_attempt_count, 1);
    assert_eq!(
        diagnostics.last_reason_code.as_deref(),
        Some("work_graph.side_effect_fence.updated")
    );
}

#[test]
fn global_and_profile_caps_throttle_visible_claim_admission() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[]), item("c", &[])];
    create.concurrency_policy.max_active_items = 1;
    create.concurrency_policy.max_active_per_profile = BTreeMap::from([("general".to_owned(), 1)]);
    store.create_work_graph(&create).expect("graph should be created");
    claim(&store, "a", 1);
    let denied = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("second claim should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.global_limit"
        }
    ));

    let guard = store.connection.lock().expect("journal lock should be available");
    guard
        .execute(
            "UPDATE work_graphs SET concurrency_policy_json = json_set(concurrency_policy_json, '$.max_active_items', 2) WHERE graph_ulid = 'graph-1'",
            [],
        )
        .expect("test should widen only the global cap");
    drop(guard);
    let denied = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("profile cap should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.profile_limit"
        }
    ));
}

#[test]
fn workspace_mutation_and_provider_pressure_are_serialized() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    let mut mutation = item("a", &[]);
    mutation.resource_class = WorkResourceClass::WorkspaceMutation;
    mutation.workspace_scope = Some("C:/workspace/project".to_owned());
    mutation.provider_profile = Some("provider-a".to_owned());
    let mut reader = item("c", &[]);
    reader.resource_class = WorkResourceClass::WorkspaceRead;
    reader.workspace_scope = Some("C:\\workspace\\project\\src".to_owned());
    reader.provider_profile = Some("provider-a".to_owned());
    create.items = vec![mutation, reader];
    create.concurrency_policy.max_active_per_provider =
        BTreeMap::from([("provider-a".to_owned(), 2)]);
    store.create_work_graph(&create).expect("graph should be created");
    claim(&store, "a", 1);
    let denied = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("workspace collision should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.workspace_conflict"
        }
    ));

    let guard = store.connection.lock().expect("journal lock should be available");
    guard
        .execute(
            "UPDATE work_graphs SET concurrency_policy_json = json_set(concurrency_policy_json, '$.max_active_per_provider.\"provider-a\"', 1) WHERE graph_ulid = 'graph-1'",
            [],
        )
        .expect("test should tighten the provider cap");
    drop(guard);
    let denied = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("provider cap should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.provider_limit"
        }
    ));

    let mut pressured = claim_request("c", 1, "worker-2");
    pressured.provider_backpressure_profiles.insert("provider-a".to_owned());
    let denied =
        store.claim_ready_work_item(&pressured).expect("provider pressure should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.provider_rate_limited"
        }
    ));
}

#[test]
fn repeated_failures_back_off_then_open_the_circuit() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[])];
    create.concurrency_policy.failure_limit = 2;
    create.concurrency_policy.retry_backoff_base_ms = 1;
    create.concurrency_policy.retry_backoff_max_ms = 1;
    store.create_work_graph(&create).expect("graph should be created");
    let first = claim(&store, "a", 1);
    let running = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: first.item.revision,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("claim should start");
    store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority(&first),
            expected_item_revision: running.item.revision,
            target_state: WorkItemState::Failed,
            verification_state: WorkVerificationState::Unverified,
            result_sha256: "34".repeat(32),
            reason_code: "work_graph.worker_failed".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("first failure should schedule retry");
    let retry = store.work_graph_snapshot("graph-1").unwrap().unwrap().items.remove(0);
    assert_eq!(retry.state, WorkItemState::Ready);
    assert_eq!(retry.failure_circuit.consecutive_failures, 1);
    assert_eq!(retry.reason_code, "work_graph.failure.retry_backoff");
    std::thread::sleep(std::time::Duration::from_millis(3));

    let second = claim(&store, "a", retry.revision);
    let running = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: second.item.revision,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("retry should start");
    store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority(&second),
            expected_item_revision: running.item.revision,
            target_state: WorkItemState::Failed,
            verification_state: WorkVerificationState::Unverified,
            result_sha256: "56".repeat(32),
            reason_code: "work_graph.worker_failed_again".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("second failure should open the circuit");
    let failed = store.work_graph_snapshot("graph-1").unwrap().unwrap().items.remove(0);
    assert_eq!(failed.state, WorkItemState::Failed);
    assert_eq!(failed.failure_circuit.consecutive_failures, 2);
    assert!(failed.failure_circuit.opened_at_unix_ms.is_some());
    assert_eq!(failed.reason_code, "work_graph.failure.circuit_open");
}

#[test]
fn cancel_fanout_returns_active_generations_and_cancels_every_branch() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[]), item("c", &[])];
    store.create_work_graph(&create).expect("graph should be created");
    claim(&store, "a", 1);
    let c = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("second independent item should claim");
    assert!(matches!(c, ClaimReadyWorkItemOutcome::Granted(_)));
    let revision = store.work_graph_snapshot("graph-1").unwrap().unwrap().graph.revision;
    let plan = store
        .cancel_work_graph("graph-1", revision, "principal-1")
        .expect("graph cancellation should commit");
    assert_eq!(plan.targets.len(), 2);
    let snapshot = store.work_graph_snapshot("graph-1").unwrap().unwrap();
    assert_eq!(snapshot.graph.state, WorkGraphState::Cancelled);
    assert!(snapshot.items.iter().all(|item| item.state == WorkItemState::Cancelled));
    assert!(snapshot.items.iter().all(|item| item.claim.is_none()));
}

#[test]
fn memory_pressure_preserves_interactive_work_only() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[])];
    store.create_work_graph(&create).expect("graph should be created");
    let mut pressured = claim_request("a", 1, "worker-1");
    pressured.memory_pressure = true;
    let denied =
        store.claim_ready_work_item(&pressured).expect("memory pressure should be evaluated");
    assert!(matches!(
        denied,
        ClaimReadyWorkItemOutcome::NoEligibleItem {
            reason_code: "work_graph.admission.memory_pressure"
        }
    ));
}

#[test]
fn parallel_handoffs_commit_distinct_generation_bound_results() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = Arc::new(store(directory.path().join("journal.sqlite3")));
    let mut create = request();
    create.items = vec![item("a", &[]), item("c", &[])];
    store.create_work_graph(&create).expect("graph should be created");
    let a = claim(&store, "a", 1);
    let c_outcome = store
        .claim_ready_work_item(&claim_request("c", 1, "worker-2"))
        .expect("second independent item should claim");
    let ClaimReadyWorkItemOutcome::Granted(c) = c_outcome else {
        panic!("second independent item should be granted");
    };
    let a_running = start_claim(&store, &a);
    let c_running = start_claim(&store, &c);
    let barrier = Arc::new(Barrier::new(3));
    let first_store = Arc::clone(&store);
    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        record_handoff(
            &first_store,
            &a,
            a_running.revision,
            WorkVerificationState::Waived,
            json!({"worker": "a"}),
        )
    });
    let second_store = Arc::clone(&store);
    let second_barrier = Arc::clone(&barrier);
    let second = thread::spawn(move || {
        second_barrier.wait();
        record_handoff(
            &second_store,
            &c,
            c_running.revision,
            WorkVerificationState::Waived,
            json!({"worker": "c"}),
        )
    });
    barrier.wait();
    let first = first.join().expect("first handoff thread should finish");
    let second = second.join().expect("second handoff thread should finish");

    assert_ne!(first.handoff.handoff_id, second.handoff.handoff_id);
    assert_ne!(first.handoff.provenance_sha256, second.handoff.provenance_sha256);
    assert_eq!(
        store
            .work_item_handoff(
                "principal-1",
                "device-1",
                "session-1",
                "graph-1",
                first.handoff.handoff_id.as_str(),
            )
            .expect("first handoff should load")
            .expect("first handoff should exist")
            .structured_result["worker"],
        "a"
    );
    assert_eq!(
        store
            .work_item_handoff(
                "principal-1",
                "device-1",
                "session-1",
                "graph-1",
                second.handoff.handoff_id.as_str(),
            )
            .expect("second handoff should load")
            .expect("second handoff should exist")
            .structured_result["worker"],
        "c"
    );
}

#[test]
fn review_rejection_reopens_item_and_restart_restores_handoff_comments_and_evidence() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let path = directory.path().join("journal.sqlite3");
    let first = store(path.clone());
    let mut create = request();
    create.items = vec![WorkItemSpecV1 { requires_review: true, ..item("a", &[]) }];
    first.create_work_graph(&create).expect("graph should be created");
    let grant = claim(&first, "a", 1);
    let running = start_claim(&first, &grant);
    first
        .create_work_graph_comment(&WorkGraphCommentCreateRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            actor_principal: "principal-1".to_owned(),
            body: "worker context note".to_owned(),
        })
        .expect("current worker comment should pass ACL");
    let handoff = record_handoff(
        &first,
        &grant,
        running.revision,
        WorkVerificationState::Pending,
        json!({"result": "candidate"}),
    );
    first
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority(&grant),
            expected_item_revision: handoff.item_revision,
            target_state: WorkItemState::Review,
            verification_state: WorkVerificationState::Pending,
            result_sha256: handoff.handoff.provenance_sha256.clone(),
            reason_code: "work_graph.complete.review_requested".to_owned(),
            actor_principal: "principal-1".to_owned(),
        })
        .expect("handoff should enter review");
    drop(first);

    let reopened = store(path);
    let restored = reopened
        .work_item_handoff(
            "principal-1",
            "device-1",
            "session-1",
            "graph-1",
            handoff.handoff.handoff_id.as_str(),
        )
        .expect("handoff replay should load")
        .expect("handoff should survive restart");
    assert_eq!(restored.evidence_refs, vec!["evidence:a"]);
    assert_eq!(restored.artifact_refs, vec!["artifact:a"]);
    assert_eq!(
        reopened
            .work_graph_comments("principal-1", "graph-1", Some("a"), 10)
            .expect("comments should replay")
            .len(),
        1
    );
    let rejected = reopened
        .review_work_item_handoff(&WorkGraphReviewRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            handoff_id: restored.handoff_id,
            reviewer_principal: "principal-1".to_owned(),
            decision: WorkGraphReviewDecision::Reject,
            reason_code: "verification_failed".to_owned(),
        })
        .expect("owner review should reject for rework");
    assert_eq!(rejected.item_state, WorkItemState::Ready);
    assert_eq!(
        reopened.work_graph_snapshot("graph-1").unwrap().unwrap().items[0].verification_state,
        WorkVerificationState::Rejected
    );
}

#[test]
fn terminal_summary_bounds_parent_context_and_retrieval_enforces_owner_session_acl() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[])];
    store.create_work_graph(&create).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    let running = start_claim(&store, &grant);
    let large_result = "x".repeat(MAX_WORK_HANDOFF_RESULT_BYTES - 2_048);
    let handoff = record_handoff(
        &store,
        &grant,
        running.revision,
        WorkVerificationState::Waived,
        json!({"large_child_output": large_result}),
    );
    store
        .settle_work_item_claim(&WorkClaimSettlementRequest {
            authority: authority(&grant),
            expected_item_revision: handoff.item_revision,
            target_state: WorkItemState::Succeeded,
            verification_state: WorkVerificationState::Waived,
            result_sha256: handoff.handoff.provenance_sha256.clone(),
            reason_code: "work_graph.complete.host_verified".to_owned(),
            actor_principal: "principal-1".to_owned(),
        })
        .expect("handoff should settle");
    let terminal = store
        .work_graph_terminal_summary("principal-1", "graph-1")
        .expect("terminal summary should load")
        .expect("graph should be terminal");
    assert_eq!(terminal.state, WorkGraphState::Succeeded);
    assert_eq!(terminal.handoffs.len(), 1);
    assert_eq!(terminal.handoffs[0].summary, "bounded result for a");
    assert!(serde_json::to_vec(&terminal).unwrap().len() < 16 * 1024);
    assert!(
        store
            .work_item_handoff(
                "principal-1",
                "device-1",
                "session-1",
                "graph-1",
                handoff.handoff.handoff_id.as_str(),
            )
            .unwrap()
            .unwrap()
            .structured_result["large_child_output"]
            .as_str()
            .unwrap()
            .len()
            > 32 * 1024
    );
    assert!(store
        .work_item_handoff(
            "principal-2",
            "device-1",
            "session-1",
            "graph-1",
            handoff.handoff.handoff_id.as_str(),
        )
        .unwrap()
        .is_none());
    assert!(store
        .work_item_handoff(
            "principal-1",
            "device-2",
            "session-1",
            "graph-1",
            handoff.handoff.handoff_id.as_str(),
        )
        .unwrap()
        .is_none());
    assert!(store
        .work_item_handoff(
            "principal-1",
            "device-1",
            "session-2",
            "graph-1",
            handoff.handoff.handoff_id.as_str(),
        )
        .unwrap()
        .is_none());
}

#[test]
fn oversized_handoff_and_out_of_scope_comment_fail_closed() {
    let directory = tempfile::tempdir().expect("tempdir should exist");
    let store = store(directory.path().join("journal.sqlite3"));
    let mut create = request();
    create.items = vec![item("a", &[])];
    store.create_work_graph(&create).expect("graph should be created");
    let grant = claim(&store, "a", 1);
    let running = start_claim(&store, &grant);
    let error = store
        .record_work_item_handoff(&WorkItemHandoffCreateRequest {
            authority: authority(&grant),
            expected_item_revision: running.revision,
            actor_principal: "principal-1".to_owned(),
            summary: "oversized".to_owned(),
            structured_result: json!({
                "payload": "x".repeat(MAX_WORK_HANDOFF_RESULT_BYTES)
            }),
            evidence_refs: Vec::new(),
            artifact_refs: Vec::new(),
            verification_state: WorkVerificationState::Waived,
        })
        .expect_err("oversized structured result must fail closed");
    assert!(error.to_string().contains("structured result exceeds"));
    let acl_error = store
        .create_work_graph_comment(&WorkGraphCommentCreateRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            actor_principal: "principal-2".to_owned(),
            body: "not allowed".to_owned(),
        })
        .expect_err("unrelated principal must not comment");
    assert!(acl_error.to_string().contains("outside the graph ACL"));
}
