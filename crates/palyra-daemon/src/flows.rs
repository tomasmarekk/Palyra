//! Flow coordination: reconciling multi-step flows with their external work.
//!
//! The [`FlowCoordinator`] poll loop mirrors background tasks, child orchestrator runs, and
//! approval decisions into flow step states, applies step timeouts, derives the overall flow
//! state, and dispatches the next runnable step. It is invoked from the gateway runtime tick and
//! persists every transition through the journal-backed flow APIs on [`GatewayRuntimeState`].

use std::sync::Arc;

use palyra_common::{
    runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState, FlowState, FlowStepState},
    runtime_preview::RuntimePreviewMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    acceptance::{
        attach_acceptance_criteria, flow_acceptance_metadata, flow_step_acceptance_criteria,
        flow_step_acceptance_projection,
    },
    application::delivery_arbitration::{
        merge_delivery_progress_updates, DeliveryProgressUpdate, DeliverySurface,
        MergedDeliveryProgress,
    },
    domain::flow_dependencies::{
        parse_flow_dependency_ids, validate_flow_dependency_graph, FlowDependencyGate,
        FlowDependencyNode, FlowDependencyReasonCode, FlowDependencyValidationReport,
        ValidatedFlowDependencyGraph,
    },
    gateway::GatewayRuntimeState,
    journal::{
        ApprovalDecision, FlowBundleRecord, FlowCreateRequest, FlowDependenciesQuarantineRequest,
        FlowRecord, FlowStepCreateRequest, FlowStepRecord, FlowStepUpdateRequest,
        FlowTransitionRequest, OrchestratorBackgroundTaskCreateRequest,
        OrchestratorBackgroundTaskUpdateRequest, OrchestratorCancelRequest,
    },
};

const FLOW_COORDINATOR_LIMIT: usize = 64;
const FLOW_EVENT_LIMIT: usize = 512;
const FLOW_COORDINATOR_ACTOR: &str = "system:flow-coordinator";
const FLOW_COORDINATOR_LEASE_MS: i64 = 60_000;
const DEFAULT_FLOW_RETRY_MAX_ATTEMPTS: u64 = 1;
const DEFAULT_FLOW_BACKOFF_MS: u64 = 1_000;
const DEFAULT_BACKGROUND_TASK_BUDGET_TOKENS: u64 = 1_200;

/// Ownership model of a flow: `Managed` flows dispatch their own work, `Mirrored` flows only
/// track lineage created elsewhere (routines, objectives, webhooks).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FlowMode {
    Managed,
    Mirrored,
}

impl FlowMode {
    /// Parses a wire name (trimmed, case-insensitive); returns `None` for unknown values.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "managed" => Some(Self::Managed),
            "mirrored" => Some(Self::Mirrored),
            _ => None,
        }
    }

    /// Returns the canonical snake_case wire name.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "managed",
            Self::Mirrored => "mirrored",
        }
    }
}

/// Retry budget serialized into a flow's `retry_policy_json` column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FlowRetryPolicy {
    #[serde(default = "default_retry_max_attempts")]
    pub max_attempts: u64,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for FlowRetryPolicy {
    fn default() -> Self {
        Self { max_attempts: DEFAULT_FLOW_RETRY_MAX_ATTEMPTS, backoff_ms: DEFAULT_FLOW_BACKOFF_MS }
    }
}

/// External identifiers a flow step is bound to, serialized into `lineage_json`.
///
/// Exactly which ids are set determines how [`FlowCoordinator::sync_external_step`] mirrors
/// state: background task first, then child run, then approval.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct FlowLineage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routine_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_task_id: Option<String>,
}

/// Human-readable contract description for one flow step adapter, used by console surfaces.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FlowAdapterContract {
    pub adapter: &'static str,
    pub input_contract: &'static str,
    pub output_contract: &'static str,
    pub ownership: &'static str,
}

/// Stateless reconciler for non-terminal flows; all state lives in the journal.
pub(crate) struct FlowCoordinator;

impl FlowCoordinator {
    /// Reconciles the next bounded, fair batch of eligible flows.
    ///
    /// # Errors
    ///
    /// Propagates the first [`Status`] returned by the runtime flow APIs.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn poll(runtime: &Arc<GatewayRuntimeState>) -> Result<(), Status> {
        if !flow_runtime_enabled(runtime) {
            return Ok(());
        }

        let flows = runtime.list_flows_for_reconciliation(FLOW_COORDINATOR_LIMIT).await?;
        for flow in flows {
            Self::reconcile_flow(runtime, &flow).await?;
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn acquire_flow_lease(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
    ) -> Result<Option<FlowRecord>, Status> {
        let now = crate::gateway::current_unix_ms();
        let lock_is_live = flow.lock_expires_at_unix_ms.is_some_and(|expires_at| expires_at > now);
        if lock_is_live
            && flow.lock_owner.as_deref().is_some_and(|owner| owner != FLOW_COORDINATOR_ACTOR)
        {
            return Ok(None);
        }
        if lock_is_live && flow.lock_owner.as_deref() == Some(FLOW_COORDINATOR_ACTOR) {
            return Ok(Some(flow.clone()));
        }

        match runtime
            .transition_flow(FlowTransitionRequest {
                flow_id: flow.flow_id.clone(),
                expected_revision: Some(flow.revision),
                state: flow.state.clone(),
                current_step_id: None,
                lock_owner: Some(Some(FLOW_COORDINATOR_ACTOR.to_owned())),
                lock_expires_at_unix_ms: Some(Some(now.saturating_add(FLOW_COORDINATOR_LEASE_MS))),
                completed_at_unix_ms: None,
                actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                event_type: "flow.lease_acquired".to_owned(),
                summary: "flow coordinator lease acquired".to_owned(),
                payload_json: json!({
                    "lease_owner": FLOW_COORDINATOR_ACTOR,
                    "lease_expires_at_unix_ms": now.saturating_add(FLOW_COORDINATOR_LEASE_MS),
                })
                .to_string(),
            })
            .await
        {
            Ok(flow) => Ok(Some(flow)),
            Err(status) if status.code() == tonic::Code::Aborted => Ok(None),
            Err(status) => Err(status),
        }
    }

    #[allow(clippy::result_large_err)]
    async fn drive_cancel_requested_flow(
        runtime: &Arc<GatewayRuntimeState>,
        bundle: &FlowBundleRecord,
    ) -> Result<(), Status> {
        let Some(leased_flow) = Self::acquire_flow_lease(runtime, &bundle.flow).await? else {
            return Ok(());
        };
        let Some(bundle) =
            runtime.get_flow_bundle(leased_flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        for step in &bundle.steps {
            if FlowStepState::from_str(step.state.as_str()).is_some_and(FlowStepState::is_terminal)
            {
                continue;
            }
            Self::request_step_cancel(runtime, &bundle.flow, step).await?;
        }

        let Some(updated) =
            runtime.get_flow_bundle(bundle.flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let all_terminal = updated.steps.iter().all(|step| {
            FlowStepState::from_str(step.state.as_str()).is_some_and(FlowStepState::is_terminal)
        });
        if all_terminal {
            runtime
                .transition_flow(FlowTransitionRequest {
                    flow_id: updated.flow.flow_id.clone(),
                    expected_revision: Some(updated.flow.revision),
                    state: FlowState::Cancelled.as_str().to_owned(),
                    current_step_id: Some(None),
                    lock_owner: Some(None),
                    lock_expires_at_unix_ms: Some(None),
                    completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                    actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                    event_type: "flow.cancel.completed".to_owned(),
                    summary: "flow cancellation completed".to_owned(),
                    payload_json: json!({ "source": "flow_coordinator" }).to_string(),
                })
                .await?;
            return Ok(());
        }

        let next_active_step = active_step_id(updated.steps.as_slice());
        if next_active_step.clone().flatten() != updated.flow.current_step_id {
            runtime
                .transition_flow(FlowTransitionRequest {
                    flow_id: updated.flow.flow_id.clone(),
                    expected_revision: Some(updated.flow.revision),
                    state: FlowState::CancelRequested.as_str().to_owned(),
                    current_step_id: next_active_step,
                    lock_owner: None,
                    lock_expires_at_unix_ms: None,
                    completed_at_unix_ms: None,
                    actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                    event_type: "flow.cancel.waiting".to_owned(),
                    summary: "flow cancellation waiting for child work".to_owned(),
                    payload_json: json!({ "source": "flow_coordinator" }).to_string(),
                })
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn drive_cancel_requested_flow_and_audit(
        runtime: &Arc<GatewayRuntimeState>,
        bundle: &FlowBundleRecord,
    ) -> Result<(), Status> {
        Self::drive_cancel_requested_flow(runtime, bundle).await?;
        let Some(latest) =
            runtime.get_flow_bundle(bundle.flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        if validated_dependency_graph(latest.steps.as_slice()).is_err() {
            Self::quarantine_invalid_dependencies(runtime, &latest).await?;
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn request_step_cancel(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
        step: &FlowStepRecord,
    ) -> Result<(), Status> {
        let lineage = parse_lineage(step)?;
        if let Some(task_id) = lineage.background_task_id.as_deref() {
            let Some(task) = runtime.get_orchestrator_background_task(task_id.to_owned()).await?
            else {
                return mark_step_cancelled(runtime, step, "background task missing").await;
            };
            if !same_flow_scope(
                flow,
                task.owner_principal.as_str(),
                task.device_id.as_str(),
                task.channel.as_deref(),
            ) {
                return Ok(());
            }
            let Some(mapped_state) = map_auxiliary_task_state(task.state.as_str()) else {
                return Ok(());
            };
            if mapped_state.is_terminal() {
                return update_step_to_external_terminal(
                    runtime,
                    step,
                    ExternalTerminalStepUpdate {
                        mapped_state,
                        output_json: task.result_json.clone(),
                        last_error: task.last_error.clone(),
                        completed_at_unix_ms: task.completed_at_unix_ms,
                        payload: json!({
                            "background_task_id": task_id,
                            "background_task_state": task.state,
                        }),
                        event_type: "flow.step.cancel_external_sync",
                    },
                )
                .await;
            }
            if task.state != AuxiliaryTaskState::CancelRequested.as_str() {
                runtime
                    .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                        task_id: task.task_id.clone(),
                        expected_revision: task.revision,
                        state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
                        target_run_id: None,
                        last_error: Some(Some("cancelled by parent flow".to_owned())),
                        result_json: Some(Some(
                            json!({
                                "cancel_requested_by": "flow",
                                "flow_id": flow.flow_id,
                                "step_id": step.step_id,
                            })
                            .to_string(),
                        )),
                        started_at_unix_ms: None,
                        completed_at_unix_ms: None,
                    })
                    .await?;
            }
            return mark_step_cancel_requested(
                runtime,
                step,
                "waiting for background task cancellation",
                json!({ "background_task_id": task_id }),
            )
            .await;
        }

        if let Some(run_id) = lineage.child_run_id.as_deref() {
            let Some(run) = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?
            else {
                return mark_step_cancelled(runtime, step, "child run missing").await;
            };
            if !same_flow_scope(
                flow,
                run.principal.as_str(),
                run.device_id.as_str(),
                run.channel.as_deref(),
            ) {
                return Ok(());
            }
            if let Some(mapped_state) = map_run_state(run.state.as_str()) {
                if mapped_state.is_terminal() {
                    return update_step_to_external_terminal(
                        runtime,
                        step,
                        ExternalTerminalStepUpdate {
                            mapped_state,
                            output_json: None,
                            last_error: run.last_error.clone(),
                            completed_at_unix_ms: run.completed_at_unix_ms,
                            payload: json!({
                                "child_run_id": run_id,
                                "run_state": run.state,
                            }),
                            event_type: "flow.step.cancel_child_run_sync",
                        },
                    )
                    .await;
                }
            }
            if !run.cancel_requested {
                runtime
                    .request_orchestrator_cancel(OrchestratorCancelRequest {
                        run_id: run_id.to_owned(),
                        reason: format!("cancelled by flow {}", flow.flow_id),
                    })
                    .await?;
            }
            return mark_step_cancel_requested(
                runtime,
                step,
                "waiting for child run cancellation",
                json!({ "child_run_id": run_id }),
            )
            .await;
        }

        mark_step_cancelled(runtime, step, "no external child to cancel").await
    }

    #[allow(clippy::result_large_err)]
    async fn reconcile_flow(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
    ) -> Result<(), Status> {
        let Some(bundle) = runtime.get_flow_bundle(flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let state = FlowState::from_str(bundle.flow.state.as_str());
        if state.is_some_and(FlowState::is_terminal) || matches!(state, Some(FlowState::Paused)) {
            return Ok(());
        }
        if matches!(state, Some(FlowState::CancelRequested)) {
            Self::drive_cancel_requested_flow_and_audit(runtime, &bundle).await?;
            return Ok(());
        }

        let dependency_graph = match validated_dependency_graph(bundle.steps.as_slice()) {
            Ok(graph) => graph,
            Err(_) => {
                Self::quarantine_invalid_dependencies(runtime, &bundle).await?;
                return Ok(());
            }
        };

        let Some(leased_flow) = Self::acquire_flow_lease(runtime, &bundle.flow).await? else {
            return Ok(());
        };
        let Some(leased_bundle) =
            runtime.get_flow_bundle(leased_flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let leased_state = FlowState::from_str(leased_bundle.flow.state.as_str());
        if leased_state.is_some_and(FlowState::is_terminal)
            || matches!(leased_state, Some(FlowState::Paused))
        {
            return Ok(());
        }
        if matches!(leased_state, Some(FlowState::CancelRequested)) {
            Self::drive_cancel_requested_flow_and_audit(runtime, &leased_bundle).await?;
            return Ok(());
        }
        let dependency_graph = match dependency_graph_for_reloaded_snapshot(
            bundle.steps.as_slice(),
            leased_bundle.steps.as_slice(),
            dependency_graph,
        ) {
            Ok(graph) => graph,
            Err(_) => {
                Self::quarantine_invalid_dependencies(runtime, &leased_bundle).await?;
                return Ok(());
            }
        };

        for step in &leased_bundle.steps {
            if let Some(next_state) =
                Self::sync_external_step(runtime, &leased_bundle.flow, step).await?
            {
                if next_state.is_terminal() {
                    continue;
                }
            }
            Self::apply_step_timeout(runtime, step).await?;
        }

        // Each mutation above persisted through the journal, so re-read the bundle before
        // deriving the flow state; acting on the stale pre-sync rows would undo those updates.
        let Some(updated) = runtime.get_flow_bundle(flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let updated_state = FlowState::from_str(updated.flow.state.as_str());
        if updated_state.is_some_and(FlowState::is_terminal)
            || matches!(updated_state, Some(FlowState::Paused))
        {
            return Ok(());
        }
        if matches!(updated_state, Some(FlowState::CancelRequested)) {
            Self::drive_cancel_requested_flow_and_audit(runtime, &updated).await?;
            return Ok(());
        }
        let dependency_graph = match dependency_graph_for_reloaded_snapshot(
            leased_bundle.steps.as_slice(),
            updated.steps.as_slice(),
            dependency_graph,
        ) {
            Ok(graph) => graph,
            Err(_) => {
                Self::quarantine_invalid_dependencies(runtime, &updated).await?;
                return Ok(());
            }
        };
        let next_flow_state = derive_flow_state(updated.steps.as_slice());
        if Some(next_flow_state) != FlowState::from_str(updated.flow.state.as_str()) {
            let completed_at = if next_flow_state.is_terminal() {
                Some(Some(crate::gateway::current_unix_ms()))
            } else {
                None
            };
            let clear_lock_owner = next_flow_state.is_terminal().then_some(None);
            let clear_lock_expires_at = next_flow_state.is_terminal().then_some(None);
            runtime
                .transition_flow(FlowTransitionRequest {
                    flow_id: updated.flow.flow_id.clone(),
                    expected_revision: Some(updated.flow.revision),
                    state: next_flow_state.as_str().to_owned(),
                    current_step_id: active_step_id(updated.steps.as_slice()),
                    lock_owner: clear_lock_owner,
                    lock_expires_at_unix_ms: clear_lock_expires_at,
                    completed_at_unix_ms: completed_at,
                    actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                    event_type: "flow.reconciled".to_owned(),
                    summary: format!("flow reconciled to {}", next_flow_state.as_str()),
                    payload_json: json!({ "source": "flow_coordinator" }).to_string(),
                })
                .await?;
        }

        let Some(latest) = runtime.get_flow_bundle(flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let latest_state = FlowState::from_str(latest.flow.state.as_str());
        if latest_state.is_some_and(FlowState::is_terminal)
            || matches!(latest_state, Some(FlowState::Paused))
        {
            return Ok(());
        }
        if matches!(latest_state, Some(FlowState::CancelRequested)) {
            Self::drive_cancel_requested_flow_and_audit(runtime, &latest).await?;
            return Ok(());
        }
        let dependency_graph = match dependency_graph_for_reloaded_snapshot(
            updated.steps.as_slice(),
            latest.steps.as_slice(),
            dependency_graph,
        ) {
            Ok(graph) => graph,
            Err(_) => {
                Self::quarantine_invalid_dependencies(runtime, &latest).await?;
                return Ok(());
            }
        };
        if !has_active_step(latest.steps.as_slice()) {
            if let Some(step) =
                next_dispatchable_step_with_graph(latest.steps.as_slice(), &dependency_graph)
            {
                Self::dispatch_step(runtime, &latest.flow, step).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn quarantine_invalid_dependencies(
        runtime: &Arc<GatewayRuntimeState>,
        bundle: &FlowBundleRecord,
    ) -> Result<(), Status> {
        match runtime
            .quarantine_invalid_flow_dependencies(FlowDependenciesQuarantineRequest {
                flow_id: bundle.flow.flow_id.clone(),
                expected_revision: bundle.flow.revision,
                actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::Aborted => Ok(()),
            Err(status) => Err(status),
        }
    }

    /// Mirrors the state of a step's external lineage (background task, child run, or approval)
    /// into the step record, returning the step state after synchronization.
    ///
    /// Returns `Ok(None)` when the lineage target is missing or belongs to a different
    /// principal/device/channel scope.
    ///
    /// # Errors
    ///
    /// Propagates [`Status`] failures from the runtime lookup and update APIs.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn sync_external_step(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
        step: &FlowStepRecord,
    ) -> Result<Option<FlowStepState>, Status> {
        let state = FlowStepState::from_str(step.state.as_str());
        if state.is_some_and(FlowStepState::is_terminal) {
            return Ok(state);
        }

        let lineage = parse_lineage(step)?;
        if let Some(task_id) = lineage.background_task_id.as_deref() {
            let Some(task) = runtime.get_orchestrator_background_task(task_id.to_owned()).await?
            else {
                return Ok(None);
            };
            // Lineage ids are stored as plain strings, so ownership is re-checked before trusting
            // the external record; a scope mismatch must never leak state across principals.
            if !same_flow_scope(
                flow,
                task.owner_principal.as_str(),
                task.device_id.as_str(),
                task.channel.as_deref(),
            ) {
                return Ok(None);
            }
            let Some(mapped_state) = map_auxiliary_task_state(task.state.as_str()) else {
                return Ok(None);
            };
            if Some(mapped_state) != state {
                let output_json = task.result_json.clone();
                runtime
                    .update_flow_step(FlowStepUpdateRequest {
                        flow_id: flow.flow_id.clone(),
                        step_id: step.step_id.clone(),
                        state: Some(mapped_state.as_str().to_owned()),
                        increment_attempt_count: false,
                        output_json: Some(output_json),
                        lineage_json: None,
                        not_before_unix_ms: None,
                        // Double-Option update semantics: outer Some means "write this field",
                        // inner value is the new content. A task error clears any waiting reason.
                        waiting_reason: task.last_error.as_ref().map(|_| None),
                        last_error: Some(task.last_error.clone()),
                        started_at_unix_ms: Some(task.started_at_unix_ms),
                        completed_at_unix_ms: Some(task.completed_at_unix_ms),
                        actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                        event_type: "flow.step.external_sync".to_owned(),
                        summary: format!(
                            "background task {task_id} mapped to {}",
                            mapped_state.as_str()
                        ),
                        payload_json: json!({
                            "background_task_id": task_id,
                            "background_task_state": task.state,
                            "target_run_id": task.target_run_id,
                        })
                        .to_string(),
                    })
                    .await?;
                return Ok(Some(mapped_state));
            }
            return Ok(state);
        }

        if let Some(run_id) = lineage.child_run_id.as_deref() {
            let snapshot = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
            if let Some(run) = snapshot {
                if !same_flow_scope(
                    flow,
                    run.principal.as_str(),
                    run.device_id.as_str(),
                    run.channel.as_deref(),
                ) {
                    return Ok(None);
                }
                if let Some(mapped_state) = map_run_state(run.state.as_str()) {
                    if Some(mapped_state) != state {
                        runtime
                            .update_flow_step(FlowStepUpdateRequest {
                                flow_id: flow.flow_id.clone(),
                                step_id: step.step_id.clone(),
                                state: Some(mapped_state.as_str().to_owned()),
                                increment_attempt_count: false,
                                output_json: None,
                                lineage_json: None,
                                not_before_unix_ms: None,
                                waiting_reason: None,
                                last_error: Some(run.last_error.clone()),
                                started_at_unix_ms: Some(Some(run.started_at_unix_ms)),
                                completed_at_unix_ms: Some(run.completed_at_unix_ms),
                                actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                                event_type: "flow.step.child_run_sync".to_owned(),
                                summary: format!(
                                    "child run {run_id} mapped to {}",
                                    mapped_state.as_str()
                                ),
                                payload_json: json!({
                                    "child_run_id": run_id,
                                    "run_state": run.state,
                                })
                                .to_string(),
                            })
                            .await?;
                        return Ok(Some(mapped_state));
                    }
                }
            }
            return Ok(state);
        }

        if let Some(approval_id) = lineage.approval_id.as_deref() {
            let Some(approval) = runtime.approval_record(approval_id.to_owned()).await? else {
                return Ok(state);
            };
            if !same_flow_scope(
                flow,
                approval.principal.as_str(),
                approval.device_id.as_str(),
                approval.channel.as_deref(),
            ) {
                return Ok(None);
            }
            let mapped_state = match approval.decision {
                Some(ApprovalDecision::Allow) => FlowStepState::Succeeded,
                Some(ApprovalDecision::Deny | ApprovalDecision::Error) => FlowStepState::Failed,
                Some(ApprovalDecision::Timeout) => FlowStepState::TimedOut,
                None => FlowStepState::WaitingForApproval,
            };
            if Some(mapped_state) != state {
                runtime
                    .update_flow_step(FlowStepUpdateRequest {
                        flow_id: flow.flow_id.clone(),
                        step_id: step.step_id.clone(),
                        state: Some(mapped_state.as_str().to_owned()),
                        increment_attempt_count: false,
                        output_json: None,
                        lineage_json: None,
                        not_before_unix_ms: None,
                        waiting_reason: if mapped_state == FlowStepState::WaitingForApproval {
                            Some(Some("approval pending".to_owned()))
                        } else {
                            Some(None)
                        },
                        last_error: None,
                        started_at_unix_ms: None,
                        completed_at_unix_ms: if mapped_state.is_terminal() {
                            Some(Some(crate::gateway::current_unix_ms()))
                        } else {
                            None
                        },
                        actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                        event_type: "flow.step.approval_sync".to_owned(),
                        summary: format!(
                            "approval {approval_id} mapped to {}",
                            mapped_state.as_str()
                        ),
                        payload_json: json!({
                            "approval_id": approval_id,
                            "decision": approval.decision.map(|decision| decision.as_str()),
                        })
                        .to_string(),
                    })
                    .await?;
                return Ok(Some(mapped_state));
            }
        }

        Ok(state)
    }

    #[allow(clippy::result_large_err)]
    async fn apply_step_timeout(
        runtime: &Arc<GatewayRuntimeState>,
        step: &FlowStepRecord,
    ) -> Result<(), Status> {
        let state = FlowStepState::from_str(step.state.as_str());
        if state.is_none_or(FlowStepState::is_terminal) {
            return Ok(());
        }
        let Some(timeout_ms) = step.timeout_ms else {
            return Ok(());
        };
        let Some(started_at) = step.started_at_unix_ms else {
            return Ok(());
        };
        let now = crate::gateway::current_unix_ms();
        if started_at.saturating_add(timeout_ms) > now {
            return Ok(());
        }
        runtime
            .update_flow_step(FlowStepUpdateRequest {
                flow_id: step.flow_id.clone(),
                step_id: step.step_id.clone(),
                state: Some(FlowStepState::TimedOut.as_str().to_owned()),
                increment_attempt_count: false,
                output_json: None,
                lineage_json: None,
                not_before_unix_ms: None,
                waiting_reason: Some(None),
                last_error: Some(Some("flow step timed out".to_owned())),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(now)),
                actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                event_type: "flow.step.timed_out".to_owned(),
                summary: "flow step timed out".to_owned(),
                payload_json: json!({
                    "timeout_ms": timeout_ms,
                    "started_at_unix_ms": started_at,
                    "timed_out_at_unix_ms": now,
                })
                .to_string(),
            })
            .await?;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn dispatch_step(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
        step: &FlowStepRecord,
    ) -> Result<(), Status> {
        match step.adapter.as_str() {
            "background_prompt" | "delegation" | "auxiliary_task" => {
                Self::dispatch_background_step(runtime, flow, step).await
            }
            "approval_wait" => {
                mark_step_waiting(runtime, step, "waiting for external approval").await
            }
            "routine" | "objective" | "webhook" => {
                let lineage = parse_lineage(step)?;
                if lineage.background_task_id.is_some()
                    || lineage.child_run_id.is_some()
                    || lineage.approval_id.is_some()
                    || lineage.external_task_id.is_some()
                {
                    runtime
                        .update_flow_step(FlowStepUpdateRequest {
                            flow_id: flow.flow_id.clone(),
                            step_id: step.step_id.clone(),
                            state: Some(FlowStepState::Running.as_str().to_owned()),
                            increment_attempt_count: true,
                            output_json: None,
                            lineage_json: None,
                            not_before_unix_ms: None,
                            waiting_reason: Some(None),
                            last_error: Some(None),
                            started_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                            completed_at_unix_ms: Some(None),
                            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                            event_type: "flow.step.mirror_started".to_owned(),
                            summary: "mirrored step is tracking existing lineage".to_owned(),
                            payload_json: json!({ "adapter": step.adapter }).to_string(),
                        })
                        .await
                        .map(|_| ())
                } else {
                    mark_step_blocked(runtime, step, "mirrored step requires existing lineage")
                        .await
                }
            }
            "manual_gate" | "compensation" => {
                mark_step_blocked(runtime, step, "manual operator action required").await
            }
            _ => mark_step_blocked(runtime, step, "unsupported flow step adapter").await,
        }
    }

    #[allow(clippy::result_large_err)]
    async fn dispatch_background_step(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
        step: &FlowStepRecord,
    ) -> Result<(), Status> {
        let mut lineage = parse_lineage(step)?;
        if lineage.background_task_id.is_some() {
            return runtime
                .update_flow_step(FlowStepUpdateRequest {
                    flow_id: flow.flow_id.clone(),
                    step_id: step.step_id.clone(),
                    state: Some(FlowStepState::Running.as_str().to_owned()),
                    increment_attempt_count: false,
                    output_json: None,
                    lineage_json: None,
                    not_before_unix_ms: None,
                    waiting_reason: Some(None),
                    last_error: Some(None),
                    started_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                    completed_at_unix_ms: Some(None),
                    actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                    event_type: "flow.step.dispatch_reused".to_owned(),
                    summary: "flow step reused existing background task lineage".to_owned(),
                    payload_json: json!({ "adapter": step.adapter }).to_string(),
                })
                .await
                .map(|_| ());
        }

        let input = parse_step_input(step);
        let session_id =
            lineage.session_id.clone().or_else(|| flow.session_id.clone()).ok_or_else(|| {
                Status::failed_precondition("flow step dispatch requires a session_id")
            })?;
        let task_kind = resolve_background_task_kind(step.adapter.as_str(), &input)?;
        let task_id = Ulid::new().to_string();
        let task = runtime
            .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
                task_id: task_id.clone(),
                task_kind,
                session_id,
                child_session_id: None,
                parent_run_id: flow.origin_run_id.clone(),
                target_run_id: None,
                planned_child_run_id: None,
                queued_input_id: None,
                owner_principal: flow.owner_principal.clone(),
                device_id: flow.device_id.clone(),
                channel: flow.channel.clone(),
                state: AuxiliaryTaskState::Queued.as_str().to_owned(),
                priority: input.get("priority").and_then(Value::as_i64).unwrap_or(0),
                max_attempts: step.max_attempts.max(1),
                budget_tokens: input
                    .get("budget_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(DEFAULT_BACKGROUND_TASK_BUDGET_TOKENS),
                delegation: None,
                cancellation_context: None,
                not_before_unix_ms: step.not_before_unix_ms,
                expires_at_unix_ms: input.get("expires_at_unix_ms").and_then(Value::as_i64),
                notification_target_json: None,
                input_text: input
                    .get("input_text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .or_else(|| Some(step.title.clone())),
                payload_json: Some(
                    json!({
                        "flow_id": flow.flow_id,
                        "flow_step_id": step.step_id,
                        "adapter": step.adapter,
                        "input": input,
                    })
                    .to_string(),
                ),
            })
            .await?;
        lineage.background_task_id = Some(task.task_id.clone());
        runtime
            .update_flow_step(FlowStepUpdateRequest {
                flow_id: flow.flow_id.clone(),
                step_id: step.step_id.clone(),
                state: Some(FlowStepState::Running.as_str().to_owned()),
                increment_attempt_count: true,
                output_json: None,
                lineage_json: Some(serialize_lineage(&lineage)?),
                not_before_unix_ms: None,
                waiting_reason: Some(None),
                last_error: Some(None),
                started_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                completed_at_unix_ms: Some(None),
                actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
                event_type: "flow.step.dispatched".to_owned(),
                summary: format!("flow step dispatched background task {}", task.task_id),
                payload_json: json!({
                    "background_task_id": task.task_id,
                    "task_kind": task.task_kind,
                })
                .to_string(),
            })
            .await?;
        Ok(())
    }
}

/// Returns the static adapter contract catalog surfaced to operators.
pub(crate) fn flow_adapter_contracts() -> Vec<FlowAdapterContract> {
    vec![
        FlowAdapterContract {
            adapter: "routine",
            input_contract: "routine_id plus optional run lineage",
            output_contract: "routine run status mapped to flow step state plus acceptance projection",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "objective",
            input_contract: "objective_id plus attempt lineage",
            output_contract: "objective attempt status mapped to flow step state plus acceptance projection",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "delegation",
            input_contract: "delegation prompt or child run lineage",
            output_contract: "background task or child run terminal state plus acceptance projection",
            ownership: "managed_or_mirrored",
        },
        FlowAdapterContract {
            adapter: "webhook",
            input_contract: "webhook integration id plus dispatch lineage",
            output_contract: "dispatch outcome mapped to flow step state plus acceptance projection",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "auxiliary_task",
            input_contract: "task_kind, input_text, optional budget_tokens",
            output_contract: "auxiliary executor result JSON plus acceptance projection",
            ownership: "managed",
        },
        FlowAdapterContract {
            adapter: "approval_wait",
            input_contract: "approval_id lineage",
            output_contract: "approval decision mapped to flow step state plus acceptance projection",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "manual_gate",
            input_contract: "operator note",
            output_contract: "operator retry, skip, resume, cancel, or compensation action plus acceptance projection",
            ownership: "operator",
        },
    ]
}

/// Folds flow step records into a channel-aware delivery progress summary.
pub(crate) fn merge_flow_step_progress_for_delivery(
    steps: &[FlowStepRecord],
    channel: Option<&str>,
    observed_at_unix_ms: i64,
) -> MergedDeliveryProgress {
    let updates = steps.iter().map(flow_step_progress_update).collect::<Vec<_>>();
    merge_delivery_progress_updates(
        updates.as_slice(),
        DeliverySurface::from_channel(channel),
        observed_at_unix_ms,
    )
}

fn flow_step_progress_update(step: &FlowStepRecord) -> DeliveryProgressUpdate {
    let state = FlowStepState::from_str(step.state.as_str());
    let acceptance = flow_step_acceptance_projection(step);
    let detail = step.last_error.clone().or_else(|| step.waiting_reason.clone()).or_else(|| {
        (!step.adapter.trim().is_empty())
            .then(|| format!("{} acceptance={}", step.adapter, acceptance.decision.as_str()))
    });
    DeliveryProgressUpdate::flow_step(
        format!("{}/{}", step.flow_id, step.step_id),
        step.title.clone(),
        step.state.clone(),
        detail,
        state.is_some_and(|value| {
            matches!(
                value,
                FlowStepState::Running
                    | FlowStepState::WaitingForApproval
                    | FlowStepState::Failed
                    | FlowStepState::TimedOut
                    | FlowStepState::Succeeded
                    | FlowStepState::Skipped
            )
        }),
        state.is_some_and(FlowStepState::is_terminal),
        step.updated_at_unix_ms,
    )
}

/// Caller-facing parameters for creating a flow; converted by [`build_flow_create_request`].
pub(crate) struct FlowCreateDescriptor {
    pub(crate) owner_principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) title: String,
    pub(crate) summary: String,
    pub(crate) mode: FlowMode,
    pub(crate) session_id: Option<String>,
    pub(crate) origin_run_id: Option<String>,
    pub(crate) steps: Vec<FlowStepCreateRequest>,
}

/// Builds a journal create request for a new pending flow with a fresh ULID and default retry
/// policy; the descriptor owner is recorded as both flow owner and acting principal.
pub(crate) fn build_flow_create_request(descriptor: FlowCreateDescriptor) -> FlowCreateRequest {
    let owner_principal = descriptor.owner_principal;
    let acceptance = flow_acceptance_metadata(descriptor.steps.as_slice());
    FlowCreateRequest {
        flow_id: Ulid::new().to_string(),
        mode: descriptor.mode.as_str().to_owned(),
        state: FlowState::Pending.as_str().to_owned(),
        owner_principal: owner_principal.clone(),
        device_id: descriptor.device_id,
        channel: descriptor.channel,
        session_id: descriptor.session_id,
        origin_run_id: descriptor.origin_run_id,
        objective_id: None,
        routine_id: None,
        webhook_id: None,
        title: descriptor.title,
        summary: descriptor.summary,
        retry_policy_json: serde_json::to_string(&FlowRetryPolicy::default())
            .expect("default flow retry policy is serializable"),
        timeout_ms: None,
        metadata_json: json!({
            "schema": "palyra.flow.metadata.v1",
            "created_by": owner_principal,
            "acceptance": acceptance,
        })
        .to_string(),
        actor_principal: owner_principal,
        steps: descriptor.steps,
    }
}

/// Builds a pending flow step create request with default retry/backoff and no dependencies.
///
/// # Panics
///
/// Panics if `lineage` fails to serialize, which cannot happen for [`FlowLineage`]'s
/// plain-string fields.
pub(crate) fn build_flow_step(
    step_index: i64,
    adapter: &str,
    step_kind: &str,
    title: String,
    input_json: Value,
    lineage: FlowLineage,
) -> FlowStepCreateRequest {
    let acceptance = flow_step_acceptance_criteria(adapter, step_kind, title.as_str());
    let input_json = attach_acceptance_criteria(input_json, acceptance);
    FlowStepCreateRequest {
        step_id: Ulid::new().to_string(),
        step_index,
        step_kind: step_kind.to_owned(),
        adapter: adapter.to_owned(),
        state: FlowStepState::Pending.as_str().to_owned(),
        title,
        input_json: input_json.to_string(),
        lineage_json: serialize_lineage(&lineage).expect("flow lineage is serializable"),
        depends_on_step_ids_json: "[]".to_owned(),
        max_attempts: DEFAULT_FLOW_RETRY_MAX_ATTEMPTS,
        backoff_ms: DEFAULT_FLOW_BACKOFF_MS,
        timeout_ms: None,
        not_before_unix_ms: None,
    }
}

fn default_retry_max_attempts() -> u64 {
    DEFAULT_FLOW_RETRY_MAX_ATTEMPTS
}

fn default_backoff_ms() -> u64 {
    DEFAULT_FLOW_BACKOFF_MS
}

fn flow_runtime_enabled(runtime: &GatewayRuntimeState) -> bool {
    !matches!(runtime.config.flow_orchestration.mode, RuntimePreviewMode::Disabled)
}

// Aggregation precedence (most decisive first): any timed-out step times out the whole flow
// immediately, while a failed step only fails the flow once every step has settled -- remaining
// steps may still be retrying or compensating.
fn derive_flow_state(steps: &[FlowStepRecord]) -> FlowState {
    if steps.is_empty() {
        return FlowState::Succeeded;
    }
    let mut has_running = false;
    let mut has_waiting = false;
    let mut has_blocked = false;
    let mut has_failed = false;
    let mut has_timed_out = false;
    let mut all_terminal = true;
    for step in steps {
        match FlowStepState::from_str(step.state.as_str()) {
            Some(FlowStepState::Failed) => has_failed = true,
            Some(FlowStepState::TimedOut) => has_timed_out = true,
            Some(FlowStepState::WaitingForApproval) => {
                has_waiting = true;
                all_terminal = false;
            }
            Some(FlowStepState::Blocked) => {
                has_blocked = true;
                all_terminal = false;
            }
            Some(
                FlowStepState::Running | FlowStepState::Retrying | FlowStepState::Compensating,
            ) => {
                has_running = true;
                all_terminal = false;
            }
            Some(state) if state.is_terminal() => {}
            _ => all_terminal = false,
        }
    }
    if has_timed_out {
        FlowState::TimedOut
    } else if has_failed && all_terminal {
        FlowState::Failed
    } else if all_terminal {
        FlowState::Succeeded
    } else if has_waiting {
        FlowState::WaitingForApproval
    } else if has_blocked {
        FlowState::Blocked
    } else if has_running {
        FlowState::Running
    } else {
        FlowState::Pending
    }
}

// Outer Option is the update flag for FlowTransitionRequest::current_step_id: None leaves the
// stored pointer untouched (all steps terminal), Some(Some(id)) points at the first live step.
fn active_step_id(steps: &[FlowStepRecord]) -> Option<Option<String>> {
    steps.iter().find_map(|step| {
        let state = FlowStepState::from_str(step.state.as_str())?;
        (!state.is_terminal()).then(|| Some(step.step_id.clone()))
    })
}

fn has_active_step(steps: &[FlowStepRecord]) -> bool {
    steps.iter().any(|step| {
        matches!(
            FlowStepState::from_str(step.state.as_str()),
            Some(
                FlowStepState::Running
                    | FlowStepState::Retrying
                    | FlowStepState::WaitingForApproval
                    | FlowStepState::Compensating
            )
        )
    })
}

#[cfg(test)]
fn next_dispatchable_step(
    steps: &[FlowStepRecord],
) -> Result<Option<&FlowStepRecord>, FlowDependencyValidationReport> {
    let graph = validated_dependency_graph(steps)?;
    Ok(next_dispatchable_step_with_graph(steps, &graph))
}

fn next_dispatchable_step_with_graph<'a>(
    steps: &'a [FlowStepRecord],
    graph: &ValidatedFlowDependencyGraph,
) -> Option<&'a FlowStepRecord> {
    let now = crate::gateway::current_unix_ms();
    steps.iter().find(|step| {
        matches!(
            FlowStepState::from_str(step.state.as_str()),
            Some(FlowStepState::Pending | FlowStepState::Ready | FlowStepState::Retrying)
        ) && step.not_before_unix_ms.is_none_or(|not_before| not_before <= now)
            && matches!(
                graph.gate_for(step.step_id.as_str(), |dependency_id| {
                    steps
                        .iter()
                        .find(|candidate| candidate.step_id == dependency_id)
                        .and_then(|candidate| FlowStepState::from_str(candidate.state.as_str()))
                }),
                Some(FlowDependencyGate::Satisfied)
            )
    })
}

fn validated_dependency_graph(
    steps: &[FlowStepRecord],
) -> Result<ValidatedFlowDependencyGraph, FlowDependencyValidationReport> {
    validate_flow_dependency_graph(steps.iter().map(|step| FlowDependencyNode {
        step_id: step.step_id.as_str(),
        dependencies_json: step.depends_on_step_ids_json.as_str(),
    }))
}

fn dependency_graph_for_reloaded_snapshot(
    previous_steps: &[FlowStepRecord],
    reloaded_steps: &[FlowStepRecord],
    previous_graph: ValidatedFlowDependencyGraph,
) -> Result<ValidatedFlowDependencyGraph, FlowDependencyValidationReport> {
    if flow_dependency_snapshots_match(previous_steps, reloaded_steps) {
        Ok(previous_graph)
    } else {
        validated_dependency_graph(reloaded_steps)
    }
}

fn flow_dependency_snapshots_match(
    previous_steps: &[FlowStepRecord],
    reloaded_steps: &[FlowStepRecord],
) -> bool {
    previous_steps.len() == reloaded_steps.len()
        && previous_steps.iter().zip(reloaded_steps).all(|(previous, reloaded)| {
            previous.step_id == reloaded.step_id
                && previous.depends_on_step_ids_json == reloaded.depends_on_step_ids_json
        })
}

/// Builds the redacted dependency validation projection used by console and runtime diagnostics.
pub(crate) fn flow_dependency_validation_diagnostics(steps: &[FlowStepRecord]) -> Value {
    match validated_dependency_graph(steps) {
        Ok(graph) => graph.diagnostic_value(),
        Err(report) => report.diagnostic_value(),
    }
}

/// Builds dependency-list projections that hide ids for every affected or invalid step.
pub(crate) fn flow_step_dependency_views(
    steps: &[FlowStepRecord],
) -> std::collections::BTreeMap<String, Value> {
    let invalid_report = validated_dependency_graph(steps).err();
    steps
        .iter()
        .map(|step| {
            let reason_code = invalid_report
                .as_ref()
                .and_then(|report| report.reason_code_for_step(step.step_id.as_str()));
            let view = match reason_code {
                Some(reason_code) => invalid_dependency_view(reason_code),
                None => parse_flow_dependency_ids(step.depends_on_step_ids_json.as_str())
                    .map_or_else(invalid_dependency_view, |dependency_ids| json!(dependency_ids)),
            };
            (step.step_id.clone(), view)
        })
        .collect()
}

fn invalid_dependency_view(reason_code: FlowDependencyReasonCode) -> Value {
    json!({
        "valid": false,
        "reason_code": reason_code.as_str(),
    })
}

fn map_auxiliary_task_state(value: &str) -> Option<FlowStepState> {
    match AuxiliaryTaskState::from_str(value)? {
        AuxiliaryTaskState::Queued => Some(FlowStepState::Pending),
        AuxiliaryTaskState::Running => Some(FlowStepState::Running),
        AuxiliaryTaskState::Paused => Some(FlowStepState::Paused),
        AuxiliaryTaskState::Succeeded => Some(FlowStepState::Succeeded),
        AuxiliaryTaskState::Failed => Some(FlowStepState::Failed),
        AuxiliaryTaskState::CancelRequested => Some(FlowStepState::CancelRequested),
        AuxiliaryTaskState::Cancelled => Some(FlowStepState::Cancelled),
        AuxiliaryTaskState::Expired => Some(FlowStepState::TimedOut),
    }
}

fn map_run_state(value: &str) -> Option<FlowStepState> {
    match value {
        "accepted" => Some(FlowStepState::Pending),
        "running" => Some(FlowStepState::Running),
        "succeeded" => Some(FlowStepState::Succeeded),
        "failed" => Some(FlowStepState::Failed),
        "cancelled" | "canceled" => Some(FlowStepState::Cancelled),
        _ => None,
    }
}

fn same_flow_scope(
    flow: &FlowRecord,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
) -> bool {
    flow.owner_principal == principal
        && flow.device_id == device_id
        && flow.channel.as_deref() == channel
}

#[allow(clippy::result_large_err)]
fn parse_lineage(step: &FlowStepRecord) -> Result<FlowLineage, Status> {
    serde_json::from_str(step.lineage_json.as_str()).map_err(|error| {
        Status::internal(format!("failed to parse flow lineage for step {}: {error}", step.step_id))
    })
}

fn serialize_lineage(lineage: &FlowLineage) -> Result<String, Status> {
    serde_json::to_string(lineage)
        .map_err(|error| Status::internal(format!("failed to serialize flow lineage: {error}")))
}

fn parse_step_input(step: &FlowStepRecord) -> Value {
    serde_json::from_str(step.input_json.as_str()).unwrap_or_else(|_| json!({}))
}

fn resolve_background_task_kind(adapter: &str, input: &Value) -> Result<String, Status> {
    if adapter == "delegation" {
        return Err(Status::failed_precondition(
            "flow delegation dispatch requires admitted Run-root cancellation authority",
        ));
    }
    let kind = if let Some(task_kind) = input.get("task_kind").and_then(Value::as_str) {
        AuxiliaryTaskKind::from_str(task_kind).ok_or_else(|| {
            Status::invalid_argument(format!("unsupported flow task_kind '{task_kind}'"))
        })?
    } else {
        match adapter {
            "auxiliary_task" => AuxiliaryTaskKind::Summary,
            "background_prompt" => AuxiliaryTaskKind::BackgroundPrompt,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "adapter '{adapter}' cannot dispatch a background task"
                )));
            }
        }
    };
    if kind == AuxiliaryTaskKind::DelegationPrompt {
        return Err(Status::failed_precondition(
            "flow delegation dispatch requires admitted Run-root cancellation authority",
        ));
    }
    let adapter_allows_kind = match adapter {
        "background_prompt" => kind == AuxiliaryTaskKind::BackgroundPrompt,
        "auxiliary_task" => matches!(
            kind,
            AuxiliaryTaskKind::Summary
                | AuxiliaryTaskKind::RecallSearch
                | AuxiliaryTaskKind::Classification
                | AuxiliaryTaskKind::Extraction
                | AuxiliaryTaskKind::ObjectiveJudge
                | AuxiliaryTaskKind::Vision
        ),
        _ => false,
    };
    if !adapter_allows_kind {
        return Err(Status::invalid_argument(format!(
            "flow adapter '{adapter}' does not support task_kind '{}'",
            kind.as_str()
        )));
    }
    Ok(kind.as_str().to_owned())
}

#[allow(clippy::result_large_err)]
async fn mark_step_waiting(
    runtime: &Arc<GatewayRuntimeState>,
    step: &FlowStepRecord,
    reason: &str,
) -> Result<(), Status> {
    runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: step.flow_id.clone(),
            step_id: step.step_id.clone(),
            state: Some(FlowStepState::WaitingForApproval.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: None,
            lineage_json: None,
            not_before_unix_ms: None,
            waiting_reason: Some(Some(reason.to_owned())),
            last_error: Some(None),
            started_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
            completed_at_unix_ms: Some(None),
            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            event_type: "flow.step.waiting".to_owned(),
            summary: reason.to_owned(),
            payload_json: json!({ "reason": reason }).to_string(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn mark_step_blocked(
    runtime: &Arc<GatewayRuntimeState>,
    step: &FlowStepRecord,
    reason: &str,
) -> Result<(), Status> {
    runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: step.flow_id.clone(),
            step_id: step.step_id.clone(),
            state: Some(FlowStepState::Blocked.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: None,
            lineage_json: None,
            not_before_unix_ms: None,
            waiting_reason: Some(Some(reason.to_owned())),
            last_error: Some(Some(reason.to_owned())),
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            event_type: "flow.step.blocked".to_owned(),
            summary: reason.to_owned(),
            payload_json: json!({ "reason": reason }).to_string(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn mark_step_cancel_requested(
    runtime: &Arc<GatewayRuntimeState>,
    step: &FlowStepRecord,
    reason: &str,
    payload: Value,
) -> Result<(), Status> {
    if step.state == FlowStepState::CancelRequested.as_str() {
        return Ok(());
    }
    runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: step.flow_id.clone(),
            step_id: step.step_id.clone(),
            state: Some(FlowStepState::CancelRequested.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: None,
            lineage_json: None,
            not_before_unix_ms: None,
            waiting_reason: Some(Some(reason.to_owned())),
            last_error: Some(None),
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            event_type: "flow.step.cancel_requested".to_owned(),
            summary: reason.to_owned(),
            payload_json: payload.to_string(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn mark_step_cancelled(
    runtime: &Arc<GatewayRuntimeState>,
    step: &FlowStepRecord,
    reason: &str,
) -> Result<(), Status> {
    if step.state == FlowStepState::Cancelled.as_str() {
        return Ok(());
    }
    runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: step.flow_id.clone(),
            step_id: step.step_id.clone(),
            state: Some(FlowStepState::Cancelled.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: None,
            lineage_json: None,
            not_before_unix_ms: None,
            waiting_reason: Some(None),
            last_error: Some(Some(reason.to_owned())),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            event_type: "flow.step.cancelled".to_owned(),
            summary: reason.to_owned(),
            payload_json: json!({ "reason": reason }).to_string(),
        })
        .await
        .map(|_| ())
}

struct ExternalTerminalStepUpdate<'a> {
    mapped_state: FlowStepState,
    output_json: Option<String>,
    last_error: Option<String>,
    completed_at_unix_ms: Option<i64>,
    payload: Value,
    event_type: &'a str,
}

#[allow(clippy::result_large_err)]
async fn update_step_to_external_terminal(
    runtime: &Arc<GatewayRuntimeState>,
    step: &FlowStepRecord,
    update: ExternalTerminalStepUpdate<'_>,
) -> Result<(), Status> {
    if step.state == update.mapped_state.as_str() {
        return Ok(());
    }
    runtime
        .update_flow_step(FlowStepUpdateRequest {
            flow_id: step.flow_id.clone(),
            step_id: step.step_id.clone(),
            state: Some(update.mapped_state.as_str().to_owned()),
            increment_attempt_count: false,
            output_json: Some(update.output_json),
            lineage_json: None,
            not_before_unix_ms: None,
            waiting_reason: Some(None),
            last_error: Some(update.last_error),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(update.completed_at_unix_ms.or_else(|| {
                update.mapped_state.is_terminal().then(crate::gateway::current_unix_ms)
            })),
            actor_principal: FLOW_COORDINATOR_ACTOR.to_owned(),
            event_type: update.event_type.to_owned(),
            summary: format!("external child mapped to {}", update.mapped_state.as_str()),
            payload_json: update.payload.to_string(),
        })
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn step(id: &str, state: FlowStepState) -> FlowStepRecord {
        FlowStepRecord {
            step_id: id.to_owned(),
            flow_id: "flow".to_owned(),
            step_index: 0,
            step_kind: "auxiliary_task".to_owned(),
            adapter: "auxiliary_task".to_owned(),
            state: state.as_str().to_owned(),
            title: id.to_owned(),
            input_json: "{}".to_owned(),
            output_json: None,
            lineage_json: "{}".to_owned(),
            depends_on_step_ids_json: "[]".to_owned(),
            attempt_count: 0,
            max_attempts: 1,
            backoff_ms: 0,
            timeout_ms: None,
            not_before_unix_ms: None,
            waiting_reason: None,
            last_error: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
        }
    }

    #[test]
    fn background_task_kind_resolution_is_canonical_and_adapter_scoped() {
        assert_eq!(
            resolve_background_task_kind(
                "auxiliary_task",
                &json!({"task_kind": "auxiliary_summary"})
            )
            .expect("summary alias should resolve"),
            AuxiliaryTaskKind::Summary.as_str()
        );
        assert_eq!(
            resolve_background_task_kind("background_prompt", &json!({}))
                .expect("background prompt default should resolve"),
            AuxiliaryTaskKind::BackgroundPrompt.as_str()
        );
        assert_eq!(
            resolve_background_task_kind("background_prompt", &json!({"task_kind": "summary"}),)
                .expect_err("background prompt adapter must reject auxiliary kinds")
                .code(),
            tonic::Code::InvalidArgument
        );
        for request in [
            ("delegation", json!({})),
            ("auxiliary_task", json!({"task_kind": "delegation_prompt"})),
        ] {
            assert_eq!(
                resolve_background_task_kind(request.0, &request.1)
                    .expect_err("flow delegation must fail without Run-root authority")
                    .code(),
                tonic::Code::FailedPrecondition
            );
        }
    }

    #[test]
    fn maps_auxiliary_states_to_flow_step_states() {
        assert_eq!(map_auxiliary_task_state("queued"), Some(FlowStepState::Pending));
        assert_eq!(map_auxiliary_task_state("running"), Some(FlowStepState::Running));
        assert_eq!(map_auxiliary_task_state("succeeded"), Some(FlowStepState::Succeeded));
        assert_eq!(map_auxiliary_task_state("expired"), Some(FlowStepState::TimedOut));
    }

    #[test]
    fn parse_lineage_rejects_corrupt_json() {
        let mut record = step("corrupt-lineage", FlowStepState::Pending);
        record.lineage_json = "{".to_owned();

        let error = parse_lineage(&record).expect_err("corrupt lineage must fail closed");

        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.message().contains("corrupt-lineage"));
    }

    #[test]
    fn derives_flow_state_from_step_states() {
        assert_eq!(
            derive_flow_state(&[step("one", FlowStepState::Succeeded)]),
            FlowState::Succeeded
        );
        assert_eq!(derive_flow_state(&[step("one", FlowStepState::Failed)]), FlowState::Failed);
        assert_eq!(
            derive_flow_state(&[step("one", FlowStepState::WaitingForApproval)]),
            FlowState::WaitingForApproval
        );
        assert_eq!(
            derive_flow_state(&[
                step("one", FlowStepState::Succeeded),
                step("two", FlowStepState::Pending),
            ]),
            FlowState::Pending
        );
    }

    #[test]
    fn dependency_gate_requires_terminal_success_like_state() {
        let mut dependent = step("two", FlowStepState::Pending);
        dependent.depends_on_step_ids_json = json!(["one"]).to_string();
        for state in [FlowStepState::Succeeded, FlowStepState::Skipped, FlowStepState::Compensated]
        {
            let steps = [step("one", state), dependent.clone()];
            assert_eq!(
                next_dispatchable_step(&steps)
                    .expect("valid graph should evaluate")
                    .map(|step| step.step_id.as_str()),
                Some("two")
            );
        }
        for state in [FlowStepState::Failed, FlowStepState::Cancelled, FlowStepState::TimedOut] {
            let steps = [step("one", state), dependent.clone()];
            assert!(next_dispatchable_step(&steps).expect("valid graph should evaluate").is_none());
        }
    }

    #[test]
    fn invalid_dependency_graph_blocks_all_dispatch() {
        let mut invalid = step("invalid", FlowStepState::Pending);
        invalid.depends_on_step_ids_json = "{".to_owned();
        let independent = step("independent", FlowStepState::Pending);

        let report = next_dispatchable_step(&[invalid, independent])
            .expect_err("graph-wide corruption must prevent later dispatch");

        assert_eq!(report.primary_issue().reason_code().as_str(), "malformed_dependency_json");
    }

    #[test]
    fn invalid_dependency_projection_never_reflects_unknown_ids() {
        let marker = "api_key=secret_should_not_appear";
        let mut invalid = step("child", FlowStepState::Pending);
        invalid.depends_on_step_ids_json =
            serde_json::to_string(&vec![marker]).expect("fixture should serialize");

        let views = flow_step_dependency_views(&[invalid]);
        let child = views.get("child").expect("child projection should exist");

        assert_eq!(child["valid"], false);
        assert_eq!(child["reason_code"], "unknown_dependency");
        assert!(!child.to_string().contains(marker));
    }

    proptest! {
        #[test]
        fn arbitrary_invalid_dependency_payload_never_dispatches(raw in ".*") {
            prop_assume!(serde_json::from_str::<Vec<String>>(raw.as_str()).is_err());
            let mut invalid = step("invalid", FlowStepState::Pending);
            invalid.depends_on_step_ids_json = raw;
            let independent = step("independent", FlowStepState::Pending);

            prop_assert!(next_dispatchable_step(&[invalid, independent]).is_err());
        }
    }

    #[test]
    fn build_flow_step_embeds_acceptance_criteria() {
        let step = build_flow_step(
            0,
            "auxiliary_task",
            "summary",
            "Summarize status".to_owned(),
            json!({ "input_text": "summarize" }),
            FlowLineage::default(),
        );
        let input = serde_json::from_str::<Value>(step.input_json.as_str())
            .expect("step input should be json");

        assert_eq!(input["input_text"], "summarize");
        assert_eq!(input["acceptance_criteria"]["reason_code"], "flow_step_acceptance_required");
        assert_eq!(
            input["acceptance_criteria"]["criteria"][0]["evidence_refs"][0],
            "flow_step:summary:auxiliary_task"
        );
    }

    #[test]
    fn flow_step_progress_merge_uses_channel_cadence_and_preserves_terminal_state() {
        let mut failed = step("terminal", FlowStepState::Failed);
        failed.updated_at_unix_ms = 20;
        failed.last_error = Some("adapter failed".to_owned());
        let merged = merge_flow_step_progress_for_delivery(
            &[step("one", FlowStepState::Running), failed],
            Some("discord"),
            25,
        );

        assert_eq!(merged.presentation, "periodic_summary");
        assert_eq!(merged.refresh_cadence_ms, 30_000);
        assert_eq!(merged.terminal_state.as_deref(), Some("failed"));
        assert_eq!(merged.items[0].state, "failed");
    }
}
