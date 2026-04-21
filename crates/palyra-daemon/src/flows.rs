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
    application::delivery_arbitration::{
        merge_delivery_progress_updates, DeliveryProgressUpdate, DeliverySurface,
        MergedDeliveryProgress,
    },
    gateway::GatewayRuntimeState,
    journal::{
        ApprovalDecision, FlowCreateRequest, FlowListFilter, FlowRecord, FlowStepCreateRequest,
        FlowStepRecord, FlowStepUpdateRequest, FlowTransitionRequest,
        OrchestratorBackgroundTaskCreateRequest,
    },
};

const FLOW_COORDINATOR_LIMIT: usize = 64;
const FLOW_EVENT_LIMIT: usize = 512;
const FLOW_COORDINATOR_ACTOR: &str = "system:flow-coordinator";
const DEFAULT_FLOW_RETRY_MAX_ATTEMPTS: u64 = 1;
const DEFAULT_FLOW_BACKOFF_MS: u64 = 1_000;
const DEFAULT_BACKGROUND_TASK_BUDGET_TOKENS: u64 = 1_200;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FlowMode {
    Managed,
    Mirrored,
}

impl FlowMode {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "managed" => Some(Self::Managed),
            "mirrored" => Some(Self::Mirrored),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "managed",
            Self::Mirrored => "mirrored",
        }
    }
}

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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FlowAdapterContract {
    pub adapter: &'static str,
    pub input_contract: &'static str,
    pub output_contract: &'static str,
    pub ownership: &'static str,
}

pub(crate) struct FlowCoordinator;

impl FlowCoordinator {
    #[allow(clippy::result_large_err)]
    pub(crate) async fn poll(runtime: &Arc<GatewayRuntimeState>) -> Result<(), Status> {
        if !flow_runtime_enabled(runtime) {
            return Ok(());
        }

        let flows = runtime
            .list_flows(FlowListFilter {
                owner_principal: None,
                device_id: None,
                channel: None,
                state: None,
                include_terminal: false,
                limit: FLOW_COORDINATOR_LIMIT,
            })
            .await?;
        for flow in flows {
            Self::reconcile_flow(runtime, &flow).await?;
        }
        Ok(())
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
        if state.is_some_and(FlowState::is_terminal)
            || matches!(state, Some(FlowState::Paused | FlowState::CancelRequested))
        {
            return Ok(());
        }

        for step in &bundle.steps {
            if let Some(next_state) = Self::sync_external_step(runtime, flow, step).await? {
                if next_state.is_terminal() {
                    continue;
                }
            }
            Self::apply_step_timeout(runtime, step).await?;
        }

        let Some(updated) = runtime.get_flow_bundle(flow.flow_id.clone(), FLOW_EVENT_LIMIT).await?
        else {
            return Ok(());
        };
        let next_flow_state = derive_flow_state(updated.steps.as_slice());
        if Some(next_flow_state) != FlowState::from_str(updated.flow.state.as_str()) {
            let completed_at = if next_flow_state.is_terminal() {
                Some(Some(crate::gateway::current_unix_ms()))
            } else {
                None
            };
            runtime
                .transition_flow(FlowTransitionRequest {
                    flow_id: updated.flow.flow_id.clone(),
                    expected_revision: Some(updated.flow.revision),
                    state: next_flow_state.as_str().to_owned(),
                    current_step_id: active_step_id(updated.steps.as_slice()),
                    lock_owner: None,
                    lock_expires_at_unix_ms: None,
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
        if !has_active_step(latest.steps.as_slice()) {
            if let Some(step) = next_dispatchable_step(latest.steps.as_slice()) {
                Self::dispatch_step(runtime, &latest.flow, step).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    async fn sync_external_step(
        runtime: &Arc<GatewayRuntimeState>,
        flow: &FlowRecord,
        step: &FlowStepRecord,
    ) -> Result<Option<FlowStepState>, Status> {
        let state = FlowStepState::from_str(step.state.as_str());
        if state.is_some_and(FlowStepState::is_terminal) {
            return Ok(state);
        }

        let lineage = parse_lineage(step);
        if let Some(task_id) = lineage.background_task_id.as_deref() {
            let Some(task) = runtime.get_orchestrator_background_task(task_id.to_owned()).await?
            else {
                return Ok(None);
            };
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
                let lineage = parse_lineage(step);
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
        let mut lineage = parse_lineage(step);
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
                parent_run_id: flow.origin_run_id.clone(),
                target_run_id: None,
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

pub(crate) fn flow_adapter_contracts() -> Vec<FlowAdapterContract> {
    vec![
        FlowAdapterContract {
            adapter: "routine",
            input_contract: "routine_id plus optional run lineage",
            output_contract: "routine run status mapped to flow step state",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "objective",
            input_contract: "objective_id plus attempt lineage",
            output_contract: "objective attempt status mapped to flow step state",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "delegation",
            input_contract: "delegation prompt or child run lineage",
            output_contract: "background task or child run terminal state",
            ownership: "managed_or_mirrored",
        },
        FlowAdapterContract {
            adapter: "webhook",
            input_contract: "webhook integration id plus dispatch lineage",
            output_contract: "dispatch outcome mapped to flow step state",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "auxiliary_task",
            input_contract: "task_kind, input_text, optional budget_tokens",
            output_contract: "auxiliary executor result JSON",
            ownership: "managed",
        },
        FlowAdapterContract {
            adapter: "approval_wait",
            input_contract: "approval_id lineage",
            output_contract: "approval decision mapped to flow step state",
            ownership: "mirrored",
        },
        FlowAdapterContract {
            adapter: "manual_gate",
            input_contract: "operator note",
            output_contract: "operator retry, skip, resume, cancel, or compensation action",
            ownership: "operator",
        },
    ]
}

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
    let detail = step
        .last_error
        .clone()
        .or_else(|| step.waiting_reason.clone())
        .or_else(|| (!step.adapter.trim().is_empty()).then(|| step.adapter.clone()));
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

pub(crate) fn build_flow_create_request(descriptor: FlowCreateDescriptor) -> FlowCreateRequest {
    let owner_principal = descriptor.owner_principal;
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
        })
        .to_string(),
        actor_principal: owner_principal,
        steps: descriptor.steps,
    }
}

pub(crate) fn build_flow_step(
    step_index: i64,
    adapter: &str,
    step_kind: &str,
    title: String,
    input_json: Value,
    lineage: FlowLineage,
) -> FlowStepCreateRequest {
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

fn next_dispatchable_step(steps: &[FlowStepRecord]) -> Option<&FlowStepRecord> {
    let now = crate::gateway::current_unix_ms();
    steps.iter().find(|step| {
        matches!(
            FlowStepState::from_str(step.state.as_str()),
            Some(FlowStepState::Pending | FlowStepState::Ready | FlowStepState::Retrying)
        ) && step.not_before_unix_ms.is_none_or(|not_before| not_before <= now)
            && dependencies_satisfied(steps, step)
    })
}

fn dependencies_satisfied(steps: &[FlowStepRecord], step: &FlowStepRecord) -> bool {
    let dependencies = serde_json::from_str::<Vec<String>>(step.depends_on_step_ids_json.as_str())
        .unwrap_or_default();
    dependencies.iter().all(|dependency_id| {
        steps
            .iter()
            .find(|candidate| candidate.step_id == *dependency_id)
            .and_then(|candidate| FlowStepState::from_str(candidate.state.as_str()))
            .is_some_and(|state| {
                matches!(
                    state,
                    FlowStepState::Succeeded | FlowStepState::Skipped | FlowStepState::Compensated
                )
            })
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

fn parse_lineage(step: &FlowStepRecord) -> FlowLineage {
    serde_json::from_str(step.lineage_json.as_str()).unwrap_or_default()
}

fn serialize_lineage(lineage: &FlowLineage) -> Result<String, Status> {
    serde_json::to_string(lineage)
        .map_err(|error| Status::internal(format!("failed to serialize flow lineage: {error}")))
}

fn parse_step_input(step: &FlowStepRecord) -> Value {
    serde_json::from_str(step.input_json.as_str()).unwrap_or_else(|_| json!({}))
}

fn resolve_background_task_kind(adapter: &str, input: &Value) -> Result<String, Status> {
    if let Some(task_kind) = input.get("task_kind").and_then(Value::as_str) {
        if AuxiliaryTaskKind::from_str(task_kind).is_some() {
            return Ok(task_kind.to_owned());
        }
        return Err(Status::invalid_argument(format!("unsupported flow task_kind '{task_kind}'")));
    }
    match adapter {
        "delegation" => Ok(AuxiliaryTaskKind::DelegationPrompt.as_str().to_owned()),
        "auxiliary_task" => Ok(AuxiliaryTaskKind::Summary.as_str().to_owned()),
        "background_prompt" => Ok(AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned()),
        _ => Err(Status::invalid_argument(format!(
            "adapter '{adapter}' cannot dispatch a background task"
        ))),
    }
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

#[cfg(test)]
mod tests {
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
    fn maps_auxiliary_states_to_flow_step_states() {
        assert_eq!(map_auxiliary_task_state("queued"), Some(FlowStepState::Pending));
        assert_eq!(map_auxiliary_task_state("running"), Some(FlowStepState::Running));
        assert_eq!(map_auxiliary_task_state("succeeded"), Some(FlowStepState::Succeeded));
        assert_eq!(map_auxiliary_task_state("expired"), Some(FlowStepState::TimedOut));
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
        assert!(dependencies_satisfied(
            &[step("one", FlowStepState::Succeeded), dependent.clone()],
            &dependent
        ));
        assert!(!dependencies_satisfied(
            &[step("one", FlowStepState::Failed), dependent.clone()],
            &dependent
        ));
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
