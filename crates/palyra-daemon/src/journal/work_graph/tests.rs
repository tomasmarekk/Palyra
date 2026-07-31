//! Durable work graph creation, replay, and transition tests.

use crate::domain::work_graph::{
    reason, WorkBudgetV1, WorkGraphCreateRequest, WorkGraphOwnerScopeV1, WorkGraphState,
    WorkItemSpecV1, WorkItemState, WorkItemTransitionRequest, WorkResourceClass,
    WorkVerificationState,
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
        items: vec![item("a", &[]), item("b", &["a"])],
        actor_principal: "principal-1".to_owned(),
    }
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
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: a.revision,
            target_state: WorkItemState::Claimed,
            verification_state: None,
            reason_code: "work_graph.claimed".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("ready item should claim");
    let running = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: 2,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("claim should start");
    let error = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: running.item.revision,
            target_state: WorkItemState::Succeeded,
            verification_state: Some(WorkVerificationState::Pending),
            reason_code: "work_graph.complete_requested".to_owned(),
            actor_principal: "worker".to_owned(),
        })
        .expect_err("unverified success must fail");
    assert!(matches!(error, JournalError::InvalidWorkGraph { .. }));

    let completed = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: running.item.revision,
            target_state: WorkItemState::Succeeded,
            verification_state: Some(WorkVerificationState::Verified),
            reason_code: "work_graph.host_verified".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .expect("verified success should commit");
    assert_eq!(completed.dependency_states_changed, vec!["b"]);
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
            reason_code: reason::STALE_REVISION.to_owned(),
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
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: a.revision,
            target_state: WorkItemState::Claimed,
            verification_state: None,
            reason_code: "work_graph.claimed".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .unwrap();
    store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: 2,
            target_state: WorkItemState::Running,
            verification_state: None,
            reason_code: "work_graph.running".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .unwrap();
    let failed = store
        .transition_work_graph_item(&WorkItemTransitionRequest {
            graph_id: "graph-1".to_owned(),
            work_item_id: "a".to_owned(),
            expected_revision: 3,
            target_state: WorkItemState::Failed,
            verification_state: None,
            reason_code: "work_graph.execution_failed".to_owned(),
            actor_principal: "host".to_owned(),
        })
        .unwrap();
    assert!(failed.dependency_states_changed.contains(&"b".to_owned()));
    assert!(failed.dependency_states_changed.contains(&"compensate-a".to_owned()));
}
