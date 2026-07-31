//! WorkGraph admission and cancellation over the daemon's shared resource authority.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    application::local_resource_governor::{
        LocalResourceGovernor, LocalResourceGovernorError, ResourceLeaseRequestV1,
        ResourcePriority, ResourceServiceKind, ResourceUnitsV1,
    },
    domain::work_graph::{
        concurrency_reason, ClaimReadyWorkItemOutcome, ClaimReadyWorkItemRequest,
        WorkClaimSettlementOutcome, WorkClaimSettlementRequest, WorkGraphCancellationTargetV1,
        WorkResourceClass,
    },
    journal::{JournalError, JournalStore, OrchestratorCancelRequest},
};

/// Production coordinator that charges WorkGraph execution to the shared local governor.
#[derive(Clone)]
pub(crate) struct WorkGraphResourceCoordinator {
    governor: LocalResourceGovernor,
}

impl WorkGraphResourceCoordinator {
    /// Creates a coordinator over the daemon-owned resource authority.
    pub(crate) const fn new(governor: LocalResourceGovernor) -> Self {
        Self { governor }
    }

    /// Acquires resource capacity before the journal atomically awards worker authority.
    pub(crate) fn claim_ready_work_item(
        &self,
        journal: &JournalStore,
        mut request: ClaimReadyWorkItemRequest,
    ) -> Result<ClaimReadyWorkItemOutcome, WorkGraphCoordinatorError> {
        let snapshot =
            journal.work_graph_snapshot(request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        let candidate = snapshot
            .items
            .iter()
            .find(|item| {
                item.state == crate::domain::work_graph::WorkItemState::Ready
                    && request
                        .work_item_id
                        .as_ref()
                        .is_none_or(|requested| requested == &item.work_item_id)
                    && request.capability_profiles.contains(item.capability_profile.as_str())
            })
            .ok_or(WorkGraphCoordinatorError::NoReadyItem)?;
        let lease = match self.governor.acquire(ResourceLeaseRequestV1 {
            owner_id: format!("work-graph:{}/{}", request.graph_id, candidate.work_item_id),
            generation: candidate.attempt_count.saturating_add(1),
            service: ResourceServiceKind::WorkGraph,
            priority: resource_priority(candidate.resource_class),
            requested: resource_units(candidate.resource_class),
            // Capacity remains charged for the item's absolute runtime bound, while the shorter
            // claim lease can still expire and be reclaimed independently.
            duration: Duration::from_millis(candidate.max_runtime_ms),
        }) {
            Ok(lease) => lease,
            Err(
                LocalResourceGovernorError::GlobalCapacityExhausted
                | LocalResourceGovernorError::OwnerCapacityExhausted,
            ) => {
                return Ok(ClaimReadyWorkItemOutcome::NoEligibleItem {
                    reason_code: concurrency_reason::MEMORY_PRESSURE,
                });
            }
            Err(error) => return Err(error.into()),
        };
        request.resource_lease_id = Some(lease.lease_id.clone());
        let outcome = journal.claim_ready_work_item(&request)?;
        if !matches!(outcome, ClaimReadyWorkItemOutcome::Granted(_)) {
            self.governor.release(lease.lease_id.as_str(), lease.generation)?;
        }
        Ok(outcome)
    }

    /// Settles one generation and releases its resource lease only after durable acceptance.
    pub(crate) fn settle_work_item_claim(
        &self,
        journal: &JournalStore,
        request: &WorkClaimSettlementRequest,
    ) -> Result<WorkClaimSettlementOutcome, WorkGraphCoordinatorError> {
        let lease = journal
            .work_graph_snapshot(request.authority.graph_id.as_str())?
            .and_then(|snapshot| {
                snapshot
                    .items
                    .into_iter()
                    .find(|item| item.work_item_id == request.authority.work_item_id)
            })
            .and_then(|item| item.claim)
            .and_then(|claim| claim.resource_lease_id.map(|lease_id| (lease_id, claim.generation)));
        let outcome = journal.settle_work_item_claim(request)?;
        if matches!(outcome, WorkClaimSettlementOutcome::Applied { .. }) {
            if let Some((lease_id, generation)) = lease {
                self.governor.release(lease_id.as_str(), generation)?;
            }
        }
        Ok(outcome)
    }

    /// Cancels durable graph authority, fans out to workers, and releases charged capacity.
    pub(crate) fn cancel_work_graph(
        &self,
        journal: &JournalStore,
        graph_id: &str,
        expected_graph_revision: u64,
        actor_principal: &str,
        cancellation: &dyn WorkGraphWorkerCancellationPort,
    ) -> Result<WorkGraphCancellationReportV1, WorkGraphCoordinatorError> {
        let plan = journal.cancel_work_graph(graph_id, expected_graph_revision, actor_principal)?;
        let deadline = Instant::now() + Duration::from_millis(plan.settle_timeout_ms);
        let mut requested = 0_u32;
        let mut acknowledged = 0_u32;
        let mut failed = 0_u32;
        let mut timed_out = 0_u32;
        for target in &plan.targets {
            if Instant::now() >= deadline {
                timed_out = timed_out.saturating_add(1);
                release_target_lease(&self.governor, target)?;
                continue;
            }
            requested = requested.saturating_add(1);
            match cancellation.request_cancel(target, deadline) {
                Ok(()) => acknowledged = acknowledged.saturating_add(1),
                Err(_) => failed = failed.saturating_add(1),
            }
            release_target_lease(&self.governor, target)?;
        }
        Ok(WorkGraphCancellationReportV1 {
            graph_id: plan.graph_id,
            graph_revision: plan.graph_revision,
            target_count: u32::try_from(plan.targets.len()).unwrap_or(u32::MAX),
            requested_count: requested,
            acknowledged_count: acknowledged,
            failed_count: failed,
            timed_out_count: timed_out,
            reason_code: plan.reason_code,
        })
    }

    /// Cancels active workers through the durable orchestrator-run cancellation flag.
    pub(crate) fn cancel_work_graph_workers(
        &self,
        journal: &JournalStore,
        graph_id: &str,
        expected_graph_revision: u64,
        actor_principal: &str,
    ) -> Result<WorkGraphCancellationReportV1, WorkGraphCoordinatorError> {
        self.cancel_work_graph(
            journal,
            graph_id,
            expected_graph_revision,
            actor_principal,
            &JournalWorkGraphCancellationPort { journal },
        )
    }

    /// Returns the shared governor snapshot used by WorkGraph diagnostics.
    pub(crate) fn resource_snapshot(
        &self,
    ) -> crate::application::local_resource_governor::LocalResourceSnapshotV1 {
        self.governor.snapshot()
    }
}

/// Worker-runtime cancellation boundary; the coordinator retains host authority.
pub(crate) trait WorkGraphWorkerCancellationPort: Send + Sync {
    /// Requests cancellation for one exact worker generation before the shared deadline.
    fn request_cancel(
        &self,
        target: &WorkGraphCancellationTargetV1,
        deadline: Instant,
    ) -> Result<(), WorkGraphCancellationPortError>;
}

/// Redaction-safe worker cancellation transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkGraphCancellationPortError;

struct JournalWorkGraphCancellationPort<'a> {
    journal: &'a JournalStore,
}

impl WorkGraphWorkerCancellationPort for JournalWorkGraphCancellationPort<'_> {
    fn request_cancel(
        &self,
        target: &WorkGraphCancellationTargetV1,
        deadline: Instant,
    ) -> Result<(), WorkGraphCancellationPortError> {
        if Instant::now() >= deadline {
            return Err(WorkGraphCancellationPortError);
        }
        self.journal
            .request_orchestrator_cancel(&OrchestratorCancelRequest {
                run_id: target.worker_id.clone(),
                reason: format!(
                    "work graph cancellation for item {} generation {}",
                    target.work_item_id, target.generation
                ),
            })
            .map(|_| ())
            .map_err(|_| WorkGraphCancellationPortError)
    }
}

/// Bounded redacted cancellation fanout evidence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkGraphCancellationReportV1 {
    pub(crate) graph_id: String,
    pub(crate) graph_revision: u64,
    pub(crate) target_count: u32,
    pub(crate) requested_count: u32,
    pub(crate) acknowledged_count: u32,
    pub(crate) failed_count: u32,
    pub(crate) timed_out_count: u32,
    pub(crate) reason_code: String,
}

/// WorkGraph coordinator failure outside ordinary admission throttling.
#[derive(Debug, Error)]
pub(crate) enum WorkGraphCoordinatorError {
    #[error("work graph journal operation failed")]
    Journal(#[from] JournalError),
    #[error("work graph resource governor failed")]
    Resource(#[from] LocalResourceGovernorError),
    #[error("work graph has no ready item for resource admission")]
    NoReadyItem,
}

fn resource_priority(class: WorkResourceClass) -> ResourcePriority {
    match class {
        WorkResourceClass::Interactive => ResourcePriority::Interactive,
        WorkResourceClass::CpuHeavy
        | WorkResourceClass::IoHeavy
        | WorkResourceClass::ProviderBound
        | WorkResourceClass::WorkspaceMutation => ResourcePriority::Foreground,
        WorkResourceClass::WorkspaceRead => ResourcePriority::BackgroundFanout,
    }
}

fn resource_units(class: WorkResourceClass) -> ResourceUnitsV1 {
    const MIB: u64 = 1024 * 1024;
    match class {
        WorkResourceClass::Interactive => ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 128 * MIB,
            file_descriptors: 32,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
        WorkResourceClass::CpuHeavy => ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 512 * MIB,
            file_descriptors: 16,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
        WorkResourceClass::IoHeavy => ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 256 * MIB,
            file_descriptors: 64,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
        WorkResourceClass::ProviderBound => ResourceUnitsV1 {
            memory_bytes: 64 * MIB,
            sockets: 1,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
        WorkResourceClass::WorkspaceRead => ResourceUnitsV1 {
            memory_bytes: 64 * MIB,
            file_descriptors: 16,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
        WorkResourceClass::WorkspaceMutation => ResourceUnitsV1 {
            memory_bytes: 128 * MIB,
            file_descriptors: 32,
            concurrency: 1,
            ..ResourceUnitsV1::default()
        },
    }
}

fn release_target_lease(
    governor: &LocalResourceGovernor,
    target: &WorkGraphCancellationTargetV1,
) -> Result<(), WorkGraphCoordinatorError> {
    if let Some(lease_id) = target.resource_lease_id.as_deref() {
        governor.release(lease_id, target.generation)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::atomic::{AtomicU32, Ordering},
    };

    use crate::{
        application::local_resource_governor::LocalResourceGovernorConfig,
        domain::work_graph::{
            ClaimReadyWorkItemRequest, WorkBudgetV1, WorkGraphConcurrencyPolicy,
            WorkGraphCreateRequest, WorkGraphOwnerScopeV1, WorkItemSpecV1, WorkResourceClass,
        },
        journal::{JournalConfig, JournalStore},
    };

    use super::*;

    struct RecordingCancellationPort(AtomicU32);

    impl WorkGraphWorkerCancellationPort for RecordingCancellationPort {
        fn request_cancel(
            &self,
            _target: &WorkGraphCancellationTargetV1,
            _deadline: Instant,
        ) -> Result<(), WorkGraphCancellationPortError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn shared_governor_accounts_claim_and_bounded_cancel_fanout() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        let journal = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: true,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("journal should open");
        journal
            .create_work_graph(&WorkGraphCreateRequest {
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
                budget: WorkBudgetV1::default(),
                concurrency_policy: WorkGraphConcurrencyPolicy::default(),
                items: vec![WorkItemSpecV1 {
                    work_item_id: "item-1".to_owned(),
                    title: "interactive work".to_owned(),
                    description: String::new(),
                    priority: 1,
                    capability_profile: "general".to_owned(),
                    dependency_ids: Vec::new(),
                    compensates_work_item_id: None,
                    serialization_key: None,
                    resource_class: WorkResourceClass::Interactive,
                    provider_profile: None,
                    workspace_scope: None,
                    budget: WorkBudgetV1::default(),
                    max_runtime_ms: 30_000,
                    requires_review: false,
                }],
                actor_principal: "principal-1".to_owned(),
            })
            .expect("graph should be created");
        let units = ResourceUnitsV1 {
            processes: 8,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            file_descriptors: 512,
            sockets: 64,
            spool_bytes: 1024 * 1024 * 1024,
            concurrency: 32,
        };
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: temp.path().join("resource-leases.json"),
            global_limit: units,
            per_owner_limit: units,
            max_records: 128,
        })
        .expect("governor should open");
        let coordinator = WorkGraphResourceCoordinator::new(governor);
        let claimed = coordinator
            .claim_ready_work_item(
                &journal,
                ClaimReadyWorkItemRequest {
                    graph_id: "graph-1".to_owned(),
                    work_item_id: Some("item-1".to_owned()),
                    expected_item_revision: Some(1),
                    worker_id: "worker-1".to_owned(),
                    worker_principal: "principal-1".to_owned(),
                    authorized_owner_principal: "principal-1".to_owned(),
                    capability_profiles: BTreeSet::from(["general".to_owned()]),
                    provider_backpressure_profiles: BTreeSet::new(),
                    memory_pressure: false,
                    resource_lease_id: None,
                    runtime_instance_id: "runtime-1".to_owned(),
                    process_start_token: "process-1".to_owned(),
                    lease_ttl_ms: 5_000,
                },
            )
            .expect("claim should be admitted");
        assert!(matches!(claimed, ClaimReadyWorkItemOutcome::Granted(_)));
        let resources = coordinator.resource_snapshot();
        assert_eq!(resources.active_leases, 1);
        assert!(resources.owner_usage.keys().any(|key| key.starts_with("work-graph:")));

        let revision = journal.work_graph_snapshot("graph-1").unwrap().unwrap().graph.revision;
        let cancellation = RecordingCancellationPort(AtomicU32::new(0));
        let report = coordinator
            .cancel_work_graph(&journal, "graph-1", revision, "principal-1", &cancellation)
            .expect("cancel should settle");
        assert_eq!(report.acknowledged_count, 1);
        assert_eq!(cancellation.0.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.resource_snapshot().active_leases, 0);
    }
}
