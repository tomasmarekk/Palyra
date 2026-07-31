//! Fail-closed validation for work graph creation, restore, and state transitions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{
    reason, WorkGraphConcurrencyPolicy, WorkGraphCreateRequest, WorkGraphRecordV1, WorkGraphState,
    WorkGraphValidationError, WorkItemRecordV1, WorkItemSpecV1, WorkItemState,
    WorkVerificationState, MAX_WORK_GRAPH_CONCURRENCY, MAX_WORK_GRAPH_ITEMS,
    MAX_WORK_ITEM_DEPENDENCIES, MAX_WORK_ITEM_DESCRIPTION_BYTES, MAX_WORK_ITEM_TITLE_BYTES,
    WORK_GRAPH_SCHEMA_VERSION,
};

const INVALID_PAYLOAD: &str = "work_graph.invalid_payload";
const UNKNOWN_DEPENDENCY: &str = "work_graph.unknown_dependency";
const DEPENDENCY_CYCLE: &str = "work_graph.dependency_cycle";
const DUPLICATE_ITEM: &str = "work_graph.duplicate_item";
const BUDGET_EXCEEDED: &str = "work_graph.child_budget_exceeded";
const INVALID_RESTORE: &str = "work_graph.invalid_restore";

/// Validates the complete graph before any row is written.
pub(crate) fn validate_graph_create_request(
    request: &WorkGraphCreateRequest,
) -> Result<(), WorkGraphValidationError> {
    validate_nonempty(request.graph_id.as_str(), "graph_id")?;
    validate_nonempty(request.owner.principal.as_str(), "owner.principal")?;
    validate_nonempty(request.owner.device_id.as_str(), "owner.device_id")?;
    validate_nonempty(request.actor_principal.as_str(), "actor_principal")?;
    if request.items.is_empty() || request.items.len() > MAX_WORK_GRAPH_ITEMS {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("items must contain 1..={MAX_WORK_GRAPH_ITEMS} entries"),
        ));
    }
    validate_concurrency_policy(&request.concurrency_policy)?;
    validate_specs(request.items.as_slice(), request.budget)
}

/// Revalidates a durable graph after load so corruption never becomes execution authority.
pub(crate) fn validate_loaded_graph(
    graph: &WorkGraphRecordV1,
    items: &[WorkItemRecordV1],
) -> Result<(), WorkGraphValidationError> {
    if graph.schema_version != WORK_GRAPH_SCHEMA_VERSION {
        return Err(WorkGraphValidationError::new(
            INVALID_RESTORE,
            format!("unsupported graph schema version {}", graph.schema_version),
        ));
    }
    if items.is_empty() || items.len() > MAX_WORK_GRAPH_ITEMS {
        return Err(WorkGraphValidationError::new(
            INVALID_RESTORE,
            "durable graph contains an invalid number of items",
        ));
    }
    if WorkGraphState::parse(graph.state.as_str()).is_none() {
        return Err(WorkGraphValidationError::new(INVALID_RESTORE, "unknown graph state"));
    }
    validate_concurrency_policy(&graph.concurrency_policy)?;
    let specs = items
        .iter()
        .map(|item| {
            if item.schema_version != WORK_GRAPH_SCHEMA_VERSION || item.graph_id != graph.graph_id {
                return Err(WorkGraphValidationError::new(
                    INVALID_RESTORE,
                    format!("item {} has incompatible graph identity", item.work_item_id),
                ));
            }
            if WorkItemState::parse(item.state.as_str()).is_none() {
                return Err(WorkGraphValidationError::new(
                    INVALID_RESTORE,
                    format!("item {} has unknown state", item.work_item_id),
                ));
            }
            if item.state.is_claimed() != item.claim.is_some() {
                return Err(WorkGraphValidationError::new(
                    INVALID_RESTORE,
                    format!("item {} claim authority does not match its state", item.work_item_id),
                ));
            }
            if let Some(claim) = item.claim.as_ref() {
                if claim.claim_token_sha256.len() != 64
                    || !claim.claim_token_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || claim.generation == 0
                    || claim.record_revision != item.revision
                    || claim.attempt_id.trim().is_empty()
                    || claim.runtime_instance_id.trim().is_empty()
                    || claim.process_start_token.trim().is_empty()
                    || claim.expires_at_unix_ms < claim.issued_at_unix_ms
                    || claim.heartbeat_at_unix_ms < claim.issued_at_unix_ms
                {
                    return Err(WorkGraphValidationError::new(
                        INVALID_RESTORE,
                        format!("item {} has malformed claim authority", item.work_item_id),
                    ));
                }
            }
            if item.failure_circuit.failure_limit == 0
                || item.failure_circuit.failure_limit > MAX_WORK_GRAPH_CONCURRENCY
                || item.failure_circuit.consecutive_failures > item.failure_circuit.failure_limit
                || item.failure_circuit.opened_at_unix_ms.is_some()
                    != (item.failure_circuit.consecutive_failures
                        >= item.failure_circuit.failure_limit)
            {
                return Err(WorkGraphValidationError::new(
                    INVALID_RESTORE,
                    format!("item {} has malformed failure circuit", item.work_item_id),
                ));
            }
            Ok(WorkItemSpecV1 {
                work_item_id: item.work_item_id.clone(),
                title: item.title.clone(),
                description: item.description.clone(),
                priority: item.priority,
                capability_profile: item.capability_profile.clone(),
                dependency_ids: item.dependency_ids.clone(),
                compensates_work_item_id: item.compensates_work_item_id.clone(),
                serialization_key: item.serialization_key.clone(),
                resource_class: item.resource_class,
                provider_profile: item.provider_profile.clone(),
                workspace_scope: item.workspace_scope.clone(),
                budget: item.budget,
                max_runtime_ms: item.max_runtime_ms,
                requires_review: item.requires_review,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_specs(specs.as_slice(), graph.budget)
}

/// Validates bounded graph concurrency and retry policy.
pub(crate) fn validate_concurrency_policy(
    policy: &WorkGraphConcurrencyPolicy,
) -> Result<(), WorkGraphValidationError> {
    if policy.max_active_items == 0
        || policy.max_active_items > MAX_WORK_GRAPH_CONCURRENCY
        || policy.max_workspace_readers_per_scope == 0
        || policy.max_workspace_readers_per_scope > MAX_WORK_GRAPH_CONCURRENCY
        || policy.failure_limit == 0
        || policy.failure_limit > MAX_WORK_GRAPH_CONCURRENCY
        || policy.retry_backoff_base_ms == 0
        || policy.retry_backoff_base_ms > policy.retry_backoff_max_ms
        || policy.cancel_settle_timeout_ms == 0
        || policy.cancel_settle_timeout_ms > 60_000
    {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            "work graph concurrency policy is outside host bounds",
        ));
    }
    for (dimension, limits) in
        [("profile", &policy.max_active_per_profile), ("provider", &policy.max_active_per_provider)]
    {
        if limits.iter().any(|(key, limit)| {
            key.trim().is_empty() || *limit == 0 || *limit > MAX_WORK_GRAPH_CONCURRENCY
        }) {
            return Err(WorkGraphValidationError::new(
                INVALID_PAYLOAD,
                format!("work graph {dimension} concurrency policy is invalid"),
            ));
        }
    }
    Ok(())
}

fn validate_specs(
    items: &[WorkItemSpecV1],
    parent_budget: super::WorkBudgetV1,
) -> Result<(), WorkGraphValidationError> {
    let mut ids = BTreeSet::new();
    for item in items {
        validate_item(item, parent_budget)?;
        if !ids.insert(item.work_item_id.as_str()) {
            return Err(WorkGraphValidationError::new(
                DUPLICATE_ITEM,
                format!("duplicate work item id {}", item.work_item_id),
            ));
        }
    }

    for item in items {
        let mut dependencies = BTreeSet::new();
        for dependency_id in &item.dependency_ids {
            if dependency_id == &item.work_item_id {
                return Err(WorkGraphValidationError::new(
                    DEPENDENCY_CYCLE,
                    format!("item {} depends on itself", item.work_item_id),
                ));
            }
            if !ids.contains(dependency_id.as_str()) {
                return Err(WorkGraphValidationError::new(
                    UNKNOWN_DEPENDENCY,
                    format!(
                        "item {} references unknown dependency {dependency_id}",
                        item.work_item_id
                    ),
                ));
            }
            if !dependencies.insert(dependency_id.as_str()) {
                return Err(WorkGraphValidationError::new(
                    INVALID_PAYLOAD,
                    format!("item {} repeats dependency {dependency_id}", item.work_item_id),
                ));
            }
        }
        if let Some(compensated_id) = item.compensates_work_item_id.as_deref() {
            if compensated_id == item.work_item_id {
                return Err(WorkGraphValidationError::new(
                    DEPENDENCY_CYCLE,
                    format!("item {} cannot compensate itself", item.work_item_id),
                ));
            }
            if !ids.contains(compensated_id) {
                return Err(WorkGraphValidationError::new(
                    UNKNOWN_DEPENDENCY,
                    format!("item {} compensates unknown item {compensated_id}", item.work_item_id),
                ));
            }
        }
    }
    validate_acyclic(items)
}

fn validate_item(
    item: &WorkItemSpecV1,
    parent_budget: super::WorkBudgetV1,
) -> Result<(), WorkGraphValidationError> {
    validate_nonempty(item.work_item_id.as_str(), "work_item_id")?;
    validate_nonempty(item.title.as_str(), "title")?;
    validate_nonempty(item.capability_profile.as_str(), "capability_profile")?;
    if item.title.len() > MAX_WORK_ITEM_TITLE_BYTES {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("item {} title exceeds byte limit", item.work_item_id),
        ));
    }
    if item.description.len() > MAX_WORK_ITEM_DESCRIPTION_BYTES {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("item {} description exceeds byte limit", item.work_item_id),
        ));
    }
    if item.dependency_ids.len() > MAX_WORK_ITEM_DEPENDENCIES {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("item {} has too many dependencies", item.work_item_id),
        ));
    }
    if item.max_runtime_ms == 0 {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("item {} max runtime must be positive", item.work_item_id),
        ));
    }
    if !item.budget.fits_within(parent_budget) {
        return Err(WorkGraphValidationError::new(
            BUDGET_EXCEEDED,
            format!("item {} budget exceeds delegated graph budget", item.work_item_id),
        ));
    }
    Ok(())
}

fn validate_nonempty(value: &str, field: &str) -> Result<(), WorkGraphValidationError> {
    if value.trim().is_empty() {
        return Err(WorkGraphValidationError::new(
            INVALID_PAYLOAD,
            format!("{field} cannot be empty"),
        ));
    }
    Ok(())
}

fn validate_acyclic(items: &[WorkItemSpecV1]) -> Result<(), WorkGraphValidationError> {
    let mut indegree = items
        .iter()
        .map(|item| (item.work_item_id.as_str(), item.dependency_ids.len()))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<&str, Vec<&str>>::new();
    for item in items {
        for dependency in &item.dependency_ids {
            dependents.entry(dependency.as_str()).or_default().push(item.work_item_id.as_str());
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect::<VecDeque<_>>();
    let mut visited = 0usize;
    while let Some(id) = ready.pop_front() {
        visited += 1;
        for dependent in dependents.get(id).into_iter().flatten() {
            let count = indegree
                .get_mut(dependent)
                .expect("validated dependent must have an indegree entry");
            *count -= 1;
            if *count == 0 {
                ready.push_back(dependent);
            }
        }
    }
    if visited != items.len() {
        return Err(WorkGraphValidationError::new(
            DEPENDENCY_CYCLE,
            "work graph dependency cycle detected",
        ));
    }
    Ok(())
}

/// Enforces the host state machine and verification gate.
pub(crate) fn validate_transition(
    current: WorkItemState,
    target: WorkItemState,
    verification: WorkVerificationState,
) -> Result<(), WorkGraphValidationError> {
    if current == target {
        return Ok(());
    }
    let allowed = match current {
        WorkItemState::Draft => {
            matches!(
                target,
                WorkItemState::BlockedByDependencies
                    | WorkItemState::Ready
                    | WorkItemState::Cancelled
            )
        }
        WorkItemState::BlockedByDependencies => {
            matches!(
                target,
                WorkItemState::Ready | WorkItemState::Failed | WorkItemState::Cancelled
            )
        }
        WorkItemState::Ready => matches!(target, WorkItemState::Claimed | WorkItemState::Cancelled),
        WorkItemState::Claimed => {
            matches!(
                target,
                WorkItemState::Running | WorkItemState::Stale | WorkItemState::Cancelled
            )
        }
        WorkItemState::Running => matches!(
            target,
            WorkItemState::Waiting
                | WorkItemState::Review
                | WorkItemState::Succeeded
                | WorkItemState::Failed
                | WorkItemState::Cancelled
                | WorkItemState::Stale
        ),
        WorkItemState::Waiting => matches!(
            target,
            WorkItemState::Running
                | WorkItemState::Review
                | WorkItemState::Failed
                | WorkItemState::Cancelled
                | WorkItemState::Stale
        ),
        WorkItemState::Review => matches!(
            target,
            WorkItemState::Ready
                | WorkItemState::Succeeded
                | WorkItemState::Failed
                | WorkItemState::Cancelled
        ),
        WorkItemState::Stale => {
            matches!(
                target,
                WorkItemState::Ready | WorkItemState::Review | WorkItemState::Cancelled
            )
        }
        WorkItemState::Succeeded | WorkItemState::Failed | WorkItemState::Cancelled => {
            target == WorkItemState::Archived
        }
        WorkItemState::Archived => false,
    };
    if !allowed || (target == WorkItemState::Succeeded && !verification.permits_success()) {
        return Err(WorkGraphValidationError::new(
            reason::INVALID_TRANSITION,
            format!("transition {} -> {} is not permitted", current.as_str(), target.as_str()),
        ));
    }
    Ok(())
}
