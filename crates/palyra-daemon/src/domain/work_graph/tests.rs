//! Unit tests for work graph invariants that do not require journal storage.

use super::*;

fn item(id: &str, dependencies: &[&str]) -> WorkItemSpecV1 {
    WorkItemSpecV1 {
        work_item_id: id.to_owned(),
        title: format!("work {id}"),
        description: String::new(),
        priority: 0,
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

fn request(items: Vec<WorkItemSpecV1>) -> WorkGraphCreateRequest {
    WorkGraphCreateRequest {
        graph_id: "graph-1".to_owned(),
        owner: WorkGraphOwnerScopeV1 {
            principal: "principal-1".to_owned(),
            device_id: "device-1".to_owned(),
            channel: None,
            session_id: None,
            origin_run_id: None,
        },
        objective_id: None,
        routine_id: None,
        flow_id: None,
        flow_step_id: None,
        budget: WorkBudgetV1 { max_turns: Some(10), ..WorkBudgetV1::default() },
        concurrency_policy: WorkGraphConcurrencyPolicy::default(),
        items,
        actor_principal: "principal-1".to_owned(),
    }
}

#[test]
fn valid_dag_and_child_budgets_are_accepted() {
    let request = request(vec![item("a", &[]), item("b", &["a"]), item("c", &["a", "b"])]);
    assert_eq!(validate_graph_create_request(&request), Ok(()));
}

#[test]
fn cycles_and_unknown_dependencies_fail_closed() {
    let cycle = request(vec![item("a", &["b"]), item("b", &["a"])]);
    assert_eq!(
        validate_graph_create_request(&cycle).unwrap_err().reason_code,
        "work_graph.dependency_cycle"
    );

    let unknown = request(vec![item("a", &["missing"])]);
    assert_eq!(
        validate_graph_create_request(&unknown).unwrap_err().reason_code,
        "work_graph.unknown_dependency"
    );
}

#[test]
fn unbounded_child_cannot_escape_a_bounded_parent() {
    let mut child = item("a", &[]);
    child.budget.max_turns = None;
    let error = validate_graph_create_request(&request(vec![child])).unwrap_err();
    assert_eq!(error.reason_code, "work_graph.child_budget_exceeded");
}

#[test]
fn success_requires_host_verification() {
    let error = validate_transition(
        WorkItemState::Running,
        WorkItemState::Succeeded,
        WorkVerificationState::Pending,
    )
    .unwrap_err();
    assert_eq!(error.reason_code, reason::INVALID_TRANSITION);
    assert_eq!(
        validate_transition(
            WorkItemState::Running,
            WorkItemState::Succeeded,
            WorkVerificationState::Verified,
        ),
        Ok(())
    );
}

#[test]
fn terminal_items_only_transition_to_archived() {
    assert!(validate_transition(
        WorkItemState::Failed,
        WorkItemState::Ready,
        WorkVerificationState::Unverified,
    )
    .is_err());
    assert_eq!(
        validate_transition(
            WorkItemState::Failed,
            WorkItemState::Archived,
            WorkVerificationState::Unverified,
        ),
        Ok(())
    );
}
