//! Background task queue for orchestrator auxiliary and delegated work.
//!
//! A single polling loop (`spawn_background_queue_loop`) leases non-terminal
//! `OrchestratorBackgroundTaskRecord` rows, enforces delegation scheduler
//! limits (lineage depth, fan-out, concurrency, serial ordering), and
//! dispatches work either to in-process executors (reflection, auxiliary task
//! kinds) or as child gateway runs supervised over a RunStream. Child
//! lifecycle progress is mirrored onto the parent run tape under throttles
//! and budgets; delegated children additionally produce a merge result.
//!
//! Terminal task states (succeeded/failed/cancelled/expired) are persisted
//! back through `GatewayRuntimeState`; retry accounting lives in the task
//! record's `attempt_count`/`max_attempts`.

#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use palyra_common::{
    runtime_contracts::AuxiliaryTaskState,
    runtime_preview::{
        RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
        RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef, RuntimePreviewCapability,
        RuntimeResourceBudget,
    },
};
use serde_json::{json, Value};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::conversation_bindings::{
        ConversationBindingCreateRequest, ConversationBindingKind, ConversationBindingLifecycle,
    },
    application::{
        delivery_arbitration::{
            arbitrate_delivery, delivery_review_summary, merge_delivery_progress_updates,
            resolve_delivery_policy, DeliveryDecision, DeliveryDecisionAction,
            DeliveryDecisionInput, DeliveryPolicySet, DeliveryProgressUpdate,
            MergedDeliveryProgress, DELIVERY_ARBITRATION_POLICY_ID,
        },
        learning::{process_post_run_reflection_task, REFLECTION_TASK_KIND},
    },
    auxiliary_executor::{execute_auxiliary_task, AuxiliaryExecutionRequest, AuxiliaryTaskType},
    delegation::{
        build_delegated_run_graph, build_delegated_run_record, build_delegated_scope,
        DelegatedReferenceInput, DelegatedRunRecordBuildRequest, DelegatedRunState,
        DelegatedScopeBuildRequest, DelegationExecutionMode, DelegationMergeApprovalSummary,
        DelegationMergeArtifactReference, DelegationMergeFailureCategory,
        DelegationMergeProvenanceRecord, DelegationMergeResult, DelegationMergeStatus,
        DelegationMergeStrategy, DelegationMergeUsageSummary, DelegationSnapshot,
        DelegationToolTraceSummary,
    },
    gateway::{
        proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
        GatewayAuthConfig, GatewayRuntimeState, RequestContext, HEADER_CHANNEL, HEADER_DEVICE_ID,
        HEADER_PRINCIPAL,
    },
    journal::{
        OrchestratorBackgroundTaskRecord, OrchestratorBackgroundTaskUpdateRequest,
        OrchestratorRunMetadataUpdateRequest, OrchestratorTapeAppendRequest,
    },
    objective_judge::{
        build_objective_judge_prompt_from_payload, invalid_objective_judge_input_result,
        materialize_objective_judge_result,
    },
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
};

const BACKGROUND_QUEUE_IDLE_SLEEP: Duration = Duration::from_secs(3);
const DEFAULT_BACKGROUND_CHANNEL: &str = "console:background";
// Throttle for mirroring non-terminal child progress onto the parent tape.
const CHILD_PROGRESS_MIN_INTERVAL_MS: i64 = 2_000;
const CHILD_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
// Hard caps on child events mirrored to one parent tape; once exceeded, a
// single compaction notice is emitted and further events of that kind drop.
const CHILD_PARENT_PROGRESS_TAPE_EVENT_LIMIT: usize = 1_024;
const CHILD_PARENT_HEARTBEAT_TAPE_EVENT_LIMIT: usize = 240;
const CHILD_PROGRESS_HISTORY_LIMIT: usize = 64;
// RunStream accepts a request before the child run row is persisted; attach
// polls briefly so target_run_id never references a missing run.
const CHILD_RUN_ATTACH_TIMEOUT: Duration = Duration::from_secs(5);
const CHILD_RUN_ATTACH_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Spawns the background queue polling loop on the Tokio runtime.
///
/// Polls every `BACKGROUND_QUEUE_IDLE_SLEEP` for non-terminal background
/// tasks, advances each through its lifecycle (expiry, cancellation sync,
/// delegation limits, dispatch), and drives the flow coordinator. Poll
/// errors are logged and retried on the next tick; the loop runs until the
/// returned handle is aborted.
pub(crate) fn spawn_background_queue_loop(
    runtime: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = poll_background_queue(&runtime, &auth, grpc_url.as_str()).await {
                warn!(status_code = ?error.code(), status_message = %error.message(), "background queue poll failed");
            }
            tokio::time::sleep(BACKGROUND_QUEUE_IDLE_SLEEP).await;
        }
    })
}

/// One poll pass: lists non-terminal tasks and advances each one.
///
/// Tasks are processed in list order; after each task the in-memory snapshot
/// is refreshed so later siblings observe the state transition (delegation
/// limits and serial ordering depend on this). A task failure is recorded on
/// that task and does not abort the rest of the pass.
async fn poll_background_queue(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
) -> Result<(), Status> {
    let mut tasks = runtime
        .list_orchestrator_background_tasks(crate::journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: None,
            device_id: None,
            channel: None,
            session_id: None,
            include_completed: false,
            limit: 256,
        })
        .await?;
    // Indexed loop because the snapshot vector is refreshed in place after
    // each task; an iterator would hold a borrow across the await points.
    for index in 0..tasks.len() {
        let task = tasks[index].clone();
        if let Err(error) =
            process_background_task(runtime, auth, grpc_url, &task, tasks.as_slice()).await
        {
            warn!(
                task_id = %task.task_id,
                status_code = ?error.code(),
                status_message = %error.message(),
                "background task processing failed"
            );
            let _ = runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some(error.message().to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "failed",
                            "task_id": task.task_id,
                            "error": error.message(),
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                })
                .await;
            if let Err(refresh_error) =
                refresh_background_task_snapshot(runtime, tasks.as_mut_slice(), index).await
            {
                warn!(
                    task_id = %task.task_id,
                    status_code = ?refresh_error.code(),
                    status_message = %refresh_error.message(),
                    "failed to refresh background task snapshot after error"
                );
            }
        } else {
            refresh_background_task_snapshot(runtime, tasks.as_mut_slice(), index).await?;
        }
    }
    crate::flows::FlowCoordinator::poll(runtime).await?;
    Ok(())
}

/// Advances a single task one lifecycle step.
///
/// Gate order: terminal/paused cleanup, expiry, parent-cancel sync, pending
/// cancellation, running-run supervision, retry budget, delegation scheduler
/// limits, serial sibling ordering, and finally dispatch.
async fn process_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
    all_tasks: &[OrchestratorBackgroundTaskRecord],
) -> Result<(), Status> {
    if is_terminal_task_state(task.state.as_str())
        || AuxiliaryTaskState::from_str(task.state.as_str()) == Some(AuxiliaryTaskState::Paused)
    {
        runtime
            .clear_self_healing_heartbeat(WorkHeartbeatKind::BackgroundTask, task.task_id.as_str());
        return Ok(());
    }

    runtime.record_self_healing_heartbeat(WorkHeartbeatUpdate {
        kind: WorkHeartbeatKind::BackgroundTask,
        object_id: task.task_id.clone(),
        summary: format!("background task {} ({})", task.task_id, task.task_kind),
    });

    let now = crate::gateway::current_unix_ms();
    if let Some(expires_at_unix_ms) = task.expires_at_unix_ms {
        if expires_at_unix_ms <= now {
            runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some(AuxiliaryTaskState::Expired.as_str().to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some("background task expired before dispatch".to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "expired",
                            "task_id": task.task_id,
                            "expired_at_unix_ms": expires_at_unix_ms,
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(now)),
                })
                .await?;
            runtime.clear_self_healing_heartbeat(
                WorkHeartbeatKind::BackgroundTask,
                task.task_id.as_str(),
            );
            return Ok(());
        }
    }
    if task.not_before_unix_ms.is_some_and(|not_before| not_before > now) {
        return Ok(());
    }
    if sync_parent_run_cancellation(runtime, task).await? {
        return Ok(());
    }
    if AuxiliaryTaskState::from_str(task.state.as_str())
        == Some(AuxiliaryTaskState::CancelRequested)
    {
        if let Some(target_run_id) = task.target_run_id.as_deref() {
            let snapshot =
                runtime.orchestrator_run_status_snapshot(target_run_id.to_owned()).await?;
            if snapshot.as_ref().is_none_or(|run| is_terminal_run_state(run.state.as_str())) {
                finalize_task_from_run(runtime, task, snapshot.as_ref(), "cancelled").await?;
            } else if let Some(reason) = pending_child_cancel_reason(task) {
                request_background_child_cancel(runtime, target_run_id, reason).await?;
            }
        } else {
            if task_has_in_flight_work_without_target(task) {
                return Ok(());
            }
            runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
                    target_run_id: None,
                    increment_attempt_count: false,
                    last_error: Some(Some("cancelled before dispatch".to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "cancelled",
                            "task_id": task.task_id,
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(now)),
                })
                .await?;
            runtime.clear_self_healing_heartbeat(
                WorkHeartbeatKind::BackgroundTask,
                task.task_id.as_str(),
            );
        }
        return Ok(());
    }
    if AuxiliaryTaskState::from_str(task.state.as_str()) == Some(AuxiliaryTaskState::Running) {
        if running_task_should_wait_for_in_flight_work(task) {
            return Ok(());
        }
        if let Some(target_run_id) = task.target_run_id.as_deref() {
            let snapshot =
                runtime.orchestrator_run_status_snapshot(target_run_id.to_owned()).await?;
            if let Some(run) = snapshot.as_ref() {
                if is_terminal_run_state(run.state.as_str()) {
                    finalize_task_from_run(runtime, task, Some(run), run.state.as_str()).await?;
                } else if let Some(message) = delegated_child_timeout_message(task, now) {
                    request_delegated_child_timeout_cancel(runtime, task, target_run_id, message)
                        .await?;
                }
                return Ok(());
            }
            return Ok(());
        }
        return Ok(());
    }
    // max_attempts == 0 means unlimited retries.
    if task.max_attempts > 0 && task.attempt_count >= task.max_attempts {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                target_run_id: None,
                increment_attempt_count: false,
                last_error: Some(Some("background task exhausted retry budget".to_owned())),
                result_json: Some(Some(
                    json!({
                        "status": "failed",
                        "task_id": task.task_id,
                        "attempt_count": task.attempt_count,
                        "max_attempts": task.max_attempts,
                    })
                    .to_string(),
                )),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(now)),
            })
            .await?;
        runtime
            .clear_self_healing_heartbeat(WorkHeartbeatKind::BackgroundTask, task.task_id.as_str());
        return Ok(());
    }

    if let Some(decision) = evaluate_delegation_scheduler_limits(all_tasks, task) {
        match decision {
            DelegationSchedulerDecision::Defer { reason, message } => {
                mark_delegation_task_waiting(runtime, task, reason, message).await?;
                return Ok(());
            }
            DelegationSchedulerDecision::Fail { reason, message } => {
                fail_delegation_task(runtime, task, reason, message).await?;
                return Ok(());
            }
        }
    }

    if task_is_blocked_by_serial_sibling(all_tasks, task) {
        mark_delegation_task_waiting(
            runtime,
            task,
            "flow_dependency",
            "delegated child is waiting for an earlier serial sibling".to_owned(),
        )
        .await?;
        return Ok(());
    }

    dispatch_background_task(runtime, auth, grpc_url, task).await
}

/// Re-reads one task from the journal into the local poll snapshot.
async fn refresh_background_task_snapshot(
    runtime: &Arc<GatewayRuntimeState>,
    tasks: &mut [OrchestratorBackgroundTaskRecord],
    index: usize,
) -> Result<(), Status> {
    let Some(task) = tasks.get(index) else {
        return Ok(());
    };
    if let Some(updated) = runtime.get_orchestrator_background_task(task.task_id.clone()).await? {
        replace_background_task_snapshot(tasks, updated);
    }
    Ok(())
}

fn replace_background_task_snapshot(
    tasks: &mut [OrchestratorBackgroundTaskRecord],
    updated: OrchestratorBackgroundTaskRecord,
) {
    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == updated.task_id) {
        *task = updated;
    }
}

/// Marks the task running and routes it to the matching executor: reflection
/// tasks and auxiliary task kinds run in-process; everything else becomes a
/// child gateway run supervised by `run_background_task_stream`.
async fn dispatch_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<(), Status> {
    let started_at_unix_ms = crate::gateway::current_unix_ms();
    if task.task_kind == REFLECTION_TASK_KIND {
        runtime
            .update_orchestrator_background_task(build_background_task_running_update(
                task.task_id.as_str(),
                started_at_unix_ms,
            ))
            .await?;
        let runtime = Arc::clone(runtime);
        let task = task.clone();
        tokio::spawn(async move {
            match process_post_run_reflection_task(&runtime, &task).await {
                Ok(result) => {
                    let _ = runtime
                        .update_orchestrator_background_task(
                            OrchestratorBackgroundTaskUpdateRequest {
                                task_id: task.task_id.clone(),
                                state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                                target_run_id: Some(None),
                                increment_attempt_count: false,
                                last_error: Some(None),
                                result_json: Some(Some(result.to_string())),
                                started_at_unix_ms: None,
                                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                            },
                        )
                        .await;
                    runtime.clear_self_healing_heartbeat(
                        WorkHeartbeatKind::BackgroundTask,
                        task.task_id.as_str(),
                    );
                }
                Err(error) => {
                    warn!(
                        task_id = %task.task_id,
                        status_code = ?error.code(),
                        status_message = %error.message(),
                        "post-run reflection task failed"
                    );
                    let _ = runtime
                        .update_orchestrator_background_task(
                            OrchestratorBackgroundTaskUpdateRequest {
                                task_id: task.task_id.clone(),
                                state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                                target_run_id: Some(None),
                                increment_attempt_count: false,
                                last_error: Some(Some(error.message().to_owned())),
                                result_json: Some(Some(
                                    json!({
                                        "status": "failed",
                                        "task_id": task.task_id,
                                        "error": error.message(),
                                    })
                                    .to_string(),
                                )),
                                started_at_unix_ms: None,
                                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                            },
                        )
                        .await;
                    runtime.clear_self_healing_heartbeat(
                        WorkHeartbeatKind::BackgroundTask,
                        task.task_id.as_str(),
                    );
                }
            }
        });
        return Ok(());
    }

    if let Some(task_type) = AuxiliaryTaskType::from_task_kind_str(task.task_kind.as_str()) {
        dispatch_auxiliary_executor_task(runtime, task, task_type, started_at_unix_ms).await?;
        return Ok(());
    }

    let run_id = Ulid::new().to_string();
    runtime
        .update_orchestrator_background_task(build_background_task_running_update(
            task.task_id.as_str(),
            started_at_unix_ms,
        ))
        .await?;
    let runtime = Arc::clone(runtime);
    let auth = auth.clone();
    let grpc_url = grpc_url.to_owned();
    let task = task.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_background_task_stream(&runtime, &auth, grpc_url.as_str(), &task, run_id.as_str())
                .await
        {
            warn!(
                task_id = %task.task_id,
                run_id = %run_id,
                status_code = ?error.code(),
                status_message = %error.message(),
                "background task stream failed"
            );
            let _ = runtime
                .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                    task_id: task.task_id.clone(),
                    state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                    target_run_id: Some(None),
                    increment_attempt_count: false,
                    last_error: Some(Some(error.message().to_owned())),
                    result_json: Some(Some(
                        json!({
                            "status": "failed",
                            "task_id": task.task_id,
                            "run_id": run_id,
                            "error": error.message(),
                        })
                        .to_string(),
                    )),
                    started_at_unix_ms: None,
                    completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                })
                .await;
            runtime.clear_self_healing_heartbeat(
                WorkHeartbeatKind::BackgroundTask,
                task.task_id.as_str(),
            );
        }
    });
    Ok(())
}

/// Runs an auxiliary task kind through the in-process auxiliary executor on
/// a detached worker; the worker persists the terminal state itself.
async fn dispatch_auxiliary_executor_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    task_type: AuxiliaryTaskType,
    started_at_unix_ms: i64,
) -> Result<(), Status> {
    runtime
        .update_orchestrator_background_task(build_background_task_running_update(
            task.task_id.as_str(),
            started_at_unix_ms,
        ))
        .await?;

    let runtime = Arc::clone(runtime);
    let task = task.clone();
    tokio::spawn(async move {
        let parameter_delta_json = if task_type == AuxiliaryTaskType::ObjectiveJudge {
            None
        } else {
            extract_parameter_delta_value(task.payload_json.as_deref())
                .ok()
                .flatten()
                .map(|value| value.to_string())
        };
        let context = RequestContext {
            principal: task.owner_principal.clone(),
            device_id: task.device_id.clone(),
            channel: task.channel.clone(),
        };
        let input_text = if task_type == AuxiliaryTaskType::ObjectiveJudge {
            match build_objective_judge_prompt_from_payload(task.payload_json.as_deref()) {
                Ok(prompt) => prompt,
                Err(error) => {
                    warn!(
                        task_id = %task.task_id,
                        status_message = %error,
                        "objective judge input rejected before auxiliary provider call"
                    );
                    let materialized = invalid_objective_judge_input_result(
                        error.clone(),
                        json!({
                            "status": "failed",
                            "task_id": task.task_id.as_str(),
                            "task_type": task_type.as_str(),
                            "error": error,
                        }),
                    );
                    let _ = runtime
                        .update_orchestrator_background_task(
                            OrchestratorBackgroundTaskUpdateRequest {
                                task_id: task.task_id.clone(),
                                state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                                target_run_id: Some(None),
                                increment_attempt_count: false,
                                last_error: Some(materialized.last_error.clone()),
                                result_json: Some(Some(materialized.result_json.to_string())),
                                started_at_unix_ms: None,
                                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                            },
                        )
                        .await;
                    runtime.clear_self_healing_heartbeat(
                        WorkHeartbeatKind::BackgroundTask,
                        task.task_id.as_str(),
                    );
                    return;
                }
            }
        } else {
            task.input_text
                .clone()
                .unwrap_or_else(|| format!("Auxiliary task {} ({})", task.task_id, task.task_kind))
        };
        match execute_auxiliary_task(
            &runtime,
            AuxiliaryExecutionRequest {
                task_id: task.task_id.clone(),
                session_id: task.session_id.clone(),
                run_id: None,
                context,
                task_type,
                input_text,
                parameter_delta_json,
                token_budget: Some(task.budget_tokens),
                vision_inputs: Vec::new(),
            },
        )
        .await
        {
            Ok(result) => {
                let materialized = (task_type == AuxiliaryTaskType::ObjectiveJudge).then(|| {
                    materialize_objective_judge_result(
                        task.payload_json.as_deref(),
                        result.output_text.as_str(),
                        result.to_result_json(),
                    )
                });
                let task_state = if materialized.as_ref().is_some_and(|entry| entry.parse_failed) {
                    AuxiliaryTaskState::Failed
                } else {
                    AuxiliaryTaskState::Succeeded
                };
                let last_error = materialized.as_ref().and_then(|entry| entry.last_error.clone());
                let result_json = materialized
                    .map(|entry| entry.result_json)
                    .unwrap_or_else(|| result.to_result_json());
                let _ = runtime
                    .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                        task_id: task.task_id.clone(),
                        state: Some(task_state.as_str().to_owned()),
                        target_run_id: Some(None),
                        increment_attempt_count: false,
                        last_error: Some(last_error),
                        result_json: Some(Some(result_json.to_string())),
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                    })
                    .await;
                runtime.clear_self_healing_heartbeat(
                    WorkHeartbeatKind::BackgroundTask,
                    task.task_id.as_str(),
                );
            }
            Err(error) => {
                warn!(
                    task_id = %task.task_id,
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "auxiliary executor task failed"
                );
                let _ = runtime
                    .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                        task_id: task.task_id.clone(),
                        state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
                        target_run_id: Some(None),
                        increment_attempt_count: false,
                        last_error: Some(Some(error.message().to_owned())),
                        result_json: Some(Some(
                            json!({
                                "status": "failed",
                                "task_id": task.task_id,
                                "task_type": task_type.as_str(),
                                "error": error.message(),
                            })
                            .to_string(),
                        )),
                        started_at_unix_ms: None,
                        completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
                    })
                    .await;
                runtime.clear_self_healing_heartbeat(
                    WorkHeartbeatKind::BackgroundTask,
                    task.task_id.as_str(),
                );
            }
        }
    });
    Ok(())
}

/// Scheduler verdict for a delegated child: `Defer` keeps it queued
/// (transient capacity pressure), `Fail` rejects it permanently (structural
/// limit violated).
enum DelegationSchedulerDecision {
    Defer { reason: &'static str, message: String },
    Fail { reason: &'static str, message: String },
}

/// Enforces delegation runtime limits for a queued child.
///
/// Structural violations (lineage cycle, max depth, total/per-parent
/// fan-out) fail closed; capacity pressure (concurrent children, parallel
/// groups) defers. Counts come from the current poll snapshot, so the
/// per-task refresh in `poll_background_queue` is what keeps them honest.
fn evaluate_delegation_scheduler_limits(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
) -> Option<DelegationSchedulerDecision> {
    let delegation = task.delegation.as_ref()?;
    let parent_run_id = task.parent_run_id.as_deref()?;
    let limits = &delegation.runtime_limits;
    if delegated_lineage_has_cycle(all_tasks, parent_run_id) {
        return Some(DelegationSchedulerDecision::Fail {
            reason: "delegation_cycle",
            message: format!(
                "delegated child would create a parent-child cycle under parent run {parent_run_id}"
            ),
        });
    }

    let next_depth = delegated_lineage_depth(all_tasks, parent_run_id).saturating_add(1);
    if next_depth > limits.max_depth {
        return Some(DelegationSchedulerDecision::Fail {
            reason: "max_depth",
            message: format!(
                "delegated child would exceed max_depth={} under parent run {}",
                limits.max_depth, parent_run_id
            ),
        });
    }

    let root_parent_run_id = delegated_root_parent_run_id(all_tasks, parent_run_id);
    let total_child_count =
        delegated_total_child_count_for_root(all_tasks, task, root_parent_run_id.as_str());
    if total_child_count > limits.max_total_children {
        return Some(DelegationSchedulerDecision::Fail {
            reason: "max_total_children",
            message: format!(
                "delegated graph would exceed max_total_children={} under root run {}",
                limits.max_total_children, root_parent_run_id
            ),
        });
    }

    let child_rank = delegated_child_rank_for_parent(all_tasks, task, parent_run_id);
    if child_rank > limits.max_children_per_parent {
        return Some(DelegationSchedulerDecision::Fail {
            reason: "max_children_per_parent",
            message: format!(
                "delegated child would exceed max_children_per_parent={} for parent run {}",
                limits.max_children_per_parent, parent_run_id
            ),
        });
    }

    let running_children =
        running_delegated_children_for_parent(all_tasks, parent_run_id).collect::<Vec<_>>();
    let running_child_count = u64::try_from(running_children.len()).unwrap_or(u64::MAX);
    if running_child_count >= limits.max_concurrent_children {
        return Some(DelegationSchedulerDecision::Defer {
            reason: "max_concurrent_children",
            message: format!(
                "delegated child is waiting for max_concurrent_children={} under parent run {}",
                limits.max_concurrent_children, parent_run_id
            ),
        });
    }

    if delegation.execution_mode == DelegationExecutionMode::Parallel {
        let mut active_groups = Vec::<&str>::new();
        for running in running_children {
            let Some(running_delegation) = running.delegation.as_ref() else {
                continue;
            };
            if running_delegation.execution_mode != DelegationExecutionMode::Parallel {
                continue;
            }
            let group_id = running_delegation.group_id.as_str();
            if !active_groups.contains(&group_id) {
                active_groups.push(group_id);
            }
        }
        // A group that is already running never blocks its own remaining
        // members; only opening a new group counts against the limit.
        let current_group_active = active_groups.contains(&delegation.group_id.as_str());
        let active_group_count = u64::try_from(active_groups.len()).unwrap_or(u64::MAX);
        if !current_group_active && active_group_count >= limits.max_parallel_groups {
            return Some(DelegationSchedulerDecision::Defer {
                reason: "max_parallel_groups",
                message: format!(
                    "delegated child is waiting for max_parallel_groups={} under parent run {}",
                    limits.max_parallel_groups, parent_run_id
                ),
            });
        }
    }

    None
}

/// True when walking parent links upward from `parent_run_id` revisits a run
/// id; `seen` also guards against malformed self-referencing records.
fn delegated_lineage_has_cycle(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    parent_run_id: &str,
) -> bool {
    let mut seen = Vec::<&str>::new();
    let mut current = parent_run_id;
    while let Some(parent_task) = delegated_task_for_child_run(all_tasks, current) {
        let Some(next_parent) = parent_task.parent_run_id.as_deref() else {
            return false;
        };
        if seen.contains(&next_parent) || next_parent == parent_run_id {
            return true;
        }
        seen.push(next_parent);
        current = next_parent;
    }
    false
}

/// Number of delegation hops above `parent_run_id` (0 for a root parent).
fn delegated_lineage_depth(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    parent_run_id: &str,
) -> u64 {
    let mut depth = 0_u64;
    let mut seen = Vec::<&str>::new();
    let mut current = parent_run_id;
    while let Some(parent_task) = delegated_task_for_child_run(all_tasks, current) {
        let Some(next_parent) = parent_task.parent_run_id.as_deref() else {
            break;
        };
        if seen.contains(&next_parent) {
            break;
        }
        seen.push(next_parent);
        depth = depth.saturating_add(1);
        current = next_parent;
    }
    depth
}

/// Topmost run id reachable by walking parent links from `parent_run_id`.
fn delegated_root_parent_run_id(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    parent_run_id: &str,
) -> String {
    let mut root = parent_run_id.to_owned();
    let mut seen = Vec::<String>::new();
    while let Some(parent_task) = delegated_task_for_child_run(all_tasks, root.as_str()) {
        let Some(next_parent) = parent_task.parent_run_id.as_deref() else {
            break;
        };
        if seen.iter().any(|value| value == next_parent) {
            break;
        }
        seen.push(next_parent.to_owned());
        root = next_parent.to_owned();
    }
    root
}

/// Finds the delegated task whose child run is `child_run_id`, i.e. the edge
/// pointing at that run in the lineage graph.
fn delegated_task_for_child_run<'a>(
    all_tasks: &'a [OrchestratorBackgroundTaskRecord],
    child_run_id: &str,
) -> Option<&'a OrchestratorBackgroundTaskRecord> {
    all_tasks.iter().find(|candidate| {
        candidate.delegation.is_some() && candidate.target_run_id.as_deref() == Some(child_run_id)
    })
}

/// Non-terminal delegated descendants under `root_parent_run_id`, counted up
/// to and including `task` in deterministic FIFO order so the same task
/// always observes the same total within a snapshot.
fn delegated_total_child_count_for_root(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
    root_parent_run_id: &str,
) -> u64 {
    let count = all_tasks
        .iter()
        .filter(|candidate| {
            let Some(candidate_parent) = candidate.parent_run_id.as_deref() else {
                return false;
            };
            let candidate_root = delegated_root_parent_run_id(all_tasks, candidate_parent);
            candidate.delegation.is_some()
                && !is_terminal_task_state(candidate.state.as_str())
                && task_precedes_or_equals(candidate, task)
                && candidate_root == root_parent_run_id
        })
        .count();
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// 1-based rank of `task` among its parent's non-terminal delegated children
/// in deterministic FIFO order.
fn delegated_child_rank_for_parent(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
    parent_run_id: &str,
) -> u64 {
    let rank = all_tasks
        .iter()
        .filter(|candidate| {
            candidate.parent_run_id.as_deref() == Some(parent_run_id)
                && candidate.delegation.is_some()
                && !is_terminal_task_state(candidate.state.as_str())
                && task_precedes_or_equals(candidate, task)
        })
        .count();
    u64::try_from(rank).unwrap_or(u64::MAX)
}

fn running_delegated_children_for_parent<'a>(
    all_tasks: &'a [OrchestratorBackgroundTaskRecord],
    parent_run_id: &'a str,
) -> impl Iterator<Item = &'a OrchestratorBackgroundTaskRecord> {
    all_tasks.iter().filter(move |candidate| {
        candidate.parent_run_id.as_deref() == Some(parent_run_id)
            && candidate.delegation.is_some()
            && task_counts_as_active_delegated_child(candidate)
    })
}

/// Cancel-requested children still hold a concurrency slot while their work
/// can be in flight (attached run, or executor work without a run yet).
fn task_counts_as_active_delegated_child(task: &OrchestratorBackgroundTaskRecord) -> bool {
    match AuxiliaryTaskState::from_str(task.state.as_str()) {
        Some(AuxiliaryTaskState::Running) => true,
        Some(AuxiliaryTaskState::CancelRequested) => {
            task.target_run_id.is_some() || task_has_in_flight_work_without_target(task)
        }
        _ => false,
    }
}

/// True when a Running task has no child run to supervise because its work
/// executes in-process (reflection/auxiliary executor); the spawned worker
/// finalizes the task itself, so the poll loop must leave it alone.
fn running_task_should_wait_for_in_flight_work(task: &OrchestratorBackgroundTaskRecord) -> bool {
    task.target_run_id.is_none()
        && (task.task_kind == REFLECTION_TASK_KIND || task_has_in_flight_work_without_target(task))
}

/// Executor-backed work in flight: started, no child run attached, and still
/// Running or CancelRequested.
fn task_has_in_flight_work_without_target(task: &OrchestratorBackgroundTaskRecord) -> bool {
    task.target_run_id.is_none()
        && task.started_at_unix_ms.is_some()
        && matches!(
            AuxiliaryTaskState::from_str(task.state.as_str()),
            Some(AuxiliaryTaskState::Running | AuxiliaryTaskState::CancelRequested)
        )
}

fn pending_child_cancel_reason(task: &OrchestratorBackgroundTaskRecord) -> Option<&'static str> {
    (AuxiliaryTaskState::from_str(task.state.as_str()) == Some(AuxiliaryTaskState::CancelRequested))
        .then_some("background_task_cancel_requested")
}

/// Deterministic FIFO order (created_at, then task id as tiebreaker);
/// includes `task` itself so filtered counts behave as 1-based ranks.
fn task_precedes_or_equals(
    candidate: &OrchestratorBackgroundTaskRecord,
    task: &OrchestratorBackgroundTaskRecord,
) -> bool {
    candidate.created_at_unix_ms < task.created_at_unix_ms
        || (candidate.created_at_unix_ms == task.created_at_unix_ms
            && candidate.task_id.as_str() <= task.task_id.as_str())
}

/// Records a waiting reason on a deferred delegated child and mirrors it to
/// the parent tape.
async fn mark_delegation_task_waiting(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    reason: &'static str,
    message: String,
) -> Result<(), Status> {
    // Idempotent: the same waiting reason is recorded once, so steady-state
    // deferral does not spam task updates and parent tape events every poll.
    if task.last_error.as_deref() == Some(message.as_str()) {
        return Ok(());
    }
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: None,
            target_run_id: None,
            increment_attempt_count: false,
            last_error: Some(Some(message.clone())),
            result_json: Some(Some(
                json!({
                    "status": "waiting",
                    "task_id": task.task_id,
                    "reason": reason,
                    "message": message,
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
        })
        .await?;
    append_child_lifecycle_event(
        runtime,
        task,
        None,
        "child_waiting",
        reason,
        true,
        json!({
            "reason": reason,
            "message": message,
            "runtime_limits": task.delegation.as_ref().map(|delegation| &delegation.runtime_limits),
        }),
    )
    .await
}

/// Fails a delegated child closed (structural limit violated) and mirrors
/// the failure to the parent tape.
async fn fail_delegation_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    reason: &'static str,
    message: String,
) -> Result<(), Status> {
    let completed_at_unix_ms = crate::gateway::current_unix_ms();
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(AuxiliaryTaskState::Failed.as_str().to_owned()),
            target_run_id: Some(None),
            increment_attempt_count: false,
            last_error: Some(Some(message.clone())),
            result_json: Some(Some(
                json!({
                    "status": "failed",
                    "task_id": task.task_id,
                    "reason": reason,
                    "error": message,
                    "runtime_limits": task.delegation.as_ref().map(|delegation| &delegation.runtime_limits),
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
        })
        .await?;
    append_child_lifecycle_event(
        runtime,
        task,
        None,
        "child_failed",
        reason,
        true,
        json!({
            "reason": reason,
            "message": message,
        }),
    )
    .await?;
    runtime.clear_self_healing_heartbeat(WorkHeartbeatKind::BackgroundTask, task.task_id.as_str());
    Ok(())
}

/// Timeout message when the child's wall-clock runtime has exceeded its
/// delegation limit; `None` while still within budget.
fn delegated_child_timeout_message(
    task: &OrchestratorBackgroundTaskRecord,
    now_unix_ms: i64,
) -> Option<String> {
    let delegation = task.delegation.as_ref()?;
    let started_at_unix_ms = task.started_at_unix_ms?;
    let timeout_ms = i64::try_from(delegation.runtime_limits.child_timeout_ms).unwrap_or(i64::MAX);
    let elapsed_ms = now_unix_ms.saturating_sub(started_at_unix_ms);
    (elapsed_ms >= timeout_ms).then(|| {
        format!(
            "delegated child timed out after {} ms (limit {} ms)",
            elapsed_ms, delegation.runtime_limits.child_timeout_ms
        )
    })
}

/// Requests cancellation of a timed-out child run and marks the task
/// cancel-requested; the terminal state lands once the run actually stops.
async fn request_delegated_child_timeout_cancel(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    target_run_id: &str,
    message: String,
) -> Result<(), Status> {
    request_background_child_cancel(runtime, target_run_id, "delegated_child_timeout").await?;
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
            target_run_id: None,
            increment_attempt_count: false,
            last_error: Some(Some(message.clone())),
            result_json: Some(Some(
                json!({
                    "status": "cancel_requested",
                    "task_id": task.task_id,
                    "run_id": target_run_id,
                    "reason": "child_timeout",
                    "message": message,
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
        })
        .await?;
    append_child_lifecycle_event(
        runtime,
        task,
        Some(target_run_id),
        "child_failed",
        "timeout_cancel_requested",
        true,
        json!({
            "reason": "child_timeout",
            "message": message,
        }),
    )
    .await
}

async fn request_background_child_cancel(
    runtime: &Arc<GatewayRuntimeState>,
    target_run_id: &str,
    reason: &str,
) -> Result<(), Status> {
    runtime
        .request_orchestrator_cancel(crate::journal::OrchestratorCancelRequest {
            run_id: target_run_id.to_owned(),
            reason: reason.to_owned(),
        })
        .await?;
    Ok(())
}

/// Supervises one child gateway run end to end: opens the RunStream, attaches
/// the run id to the task, mirrors throttled/budgeted progress onto the
/// parent tape, builds the delegation merge result, and finalizes the task
/// from the run's terminal snapshot.
async fn run_background_task_stream(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
    run_id: &str,
) -> Result<(), Status> {
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(grpc_url.to_owned())
            .await
            .map_err(|error| {
                Status::unavailable(format!("failed to connect background queue gateway: {error}"))
            })?;
    let prompt_text = task
        .input_text
        .clone()
        .unwrap_or_else(|| format!("Background task {} ({})", task.task_id, task.task_kind));
    let origin_kind = if task.delegation.is_some() { "delegation" } else { "background" };
    let mut run_request = Request::new(tokio_stream::iter(vec![common_v1::RunStreamRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: task.session_id.clone() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
        input: Some(common_v1::MessageEnvelope {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            envelope_id: Some(common_v1::CanonicalId { ulid: Ulid::new().to_string() }),
            timestamp_unix_ms: crate::gateway::current_unix_ms(),
            origin: Some(common_v1::EnvelopeOrigin {
                r#type: common_v1::envelope_origin::OriginType::System as i32,
                channel: task
                    .channel
                    .clone()
                    .unwrap_or_else(|| DEFAULT_BACKGROUND_CHANNEL.to_owned()),
                conversation_id: task.session_id.clone(),
                sender_display: "palyra-background".to_owned(),
                sender_handle: "background".to_owned(),
                sender_verified: true,
            }),
            content: Some(common_v1::MessageContent { text: prompt_text, attachments: Vec::new() }),
            security: None,
            max_payload_bytes: 0,
        }),
        allow_sensitive_tools: false,
        session_key: String::new(),
        session_label: String::new(),
        reset_session: false,
        require_existing: true,
        tool_approval_response: None,
        origin_kind: origin_kind.to_owned(),
        origin_run_id: task
            .parent_run_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
        parameter_delta_json: build_parameter_delta_bytes(task)?,
        queued_input_id: task
            .queued_input_id
            .as_ref()
            .map(|ulid| common_v1::CanonicalId { ulid: ulid.clone() }),
    }]));
    inject_background_metadata(
        run_request.metadata_mut(),
        auth,
        task.owner_principal.as_str(),
        task.device_id.as_str(),
        task.channel.as_deref(),
    )?;

    let mut stream = client
        .run_stream(run_request)
        .await
        .map_err(|error| Status::internal(format!("background RunStream failed: {error}")))?
        .into_inner();

    attach_background_task_child_run(runtime, task, run_id).await?;

    let delivery_policy = resolve_delivery_policy(
        &runtime.config.delivery_arbitration,
        task.delegation.as_ref(),
        None,
        task.channel.as_deref(),
    );
    let delivery_progress_active = crate::runtime_preview_controls::capability_active(
        &runtime.config,
        RuntimePreviewCapability::DeliveryArbitration,
    );
    let mut delivery_progress_updates = Vec::<DeliveryProgressUpdate>::new();
    let mut stream_error = None::<String>;
    let mut latest_child_state = "running".to_owned();
    let mut last_progress_at_unix_ms = 0_i64;
    let mut model_token_chars = 0_usize;
    let mut parent_tape_budget = ChildLifecycleTapeBudget::default();
    let mut heartbeat = tokio::time::interval(CHILD_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // An interval's first tick fires immediately; consume it so heartbeats
    // start one full period from now.
    let _ = heartbeat.tick().await;
    loop {
        tokio::select! {
            maybe_event = stream.next() => {
                match maybe_event {
                    Some(Ok(event)) => {
                        if let Some(progress) =
                            summarize_child_stream_event(&event, &mut model_token_chars)
                        {
                            latest_child_state = progress.child_state.clone();
                            let now = crate::gateway::current_unix_ms();
                            if delivery_progress_active {
                                delivery_progress_updates.push(
                                    progress.to_delivery_progress_update(run_id, now),
                                );
                                trim_delivery_progress_history(&mut delivery_progress_updates);
                            }
                            let should_emit = should_emit_child_stream_progress(
                                &progress,
                                now,
                                last_progress_at_unix_ms,
                            );
                            if should_emit {
                                let lifecycle_decision =
                                    parent_tape_budget.record_stream_event(&progress, &progress.details);
                                let details = if delivery_progress_active {
                                    let merged_progress = merge_delivery_progress_updates(
                                        delivery_progress_updates.as_slice(),
                                        delivery_policy.surface,
                                        now,
                                    );
                                    attach_delivery_progress_details(
                                        progress.details,
                                        &delivery_policy,
                                        &merged_progress,
                                    )
                                } else {
                                    progress.details
                                };
                                match lifecycle_decision {
                                    ChildLifecycleTapeDecision::Emit => {
                                        append_child_lifecycle_event(
                                            runtime,
                                            task,
                                            Some(run_id),
                                            progress.event_type,
                                            progress.child_state.as_str(),
                                            progress.user_visible,
                                            details,
                                        )
                                        .await?;
                                        last_progress_at_unix_ms = now;
                                    }
                                    ChildLifecycleTapeDecision::EmitLimitNotice {
                                        event_type,
                                        details,
                                    } => {
                                        append_child_lifecycle_event(
                                            runtime,
                                            task,
                                            Some(run_id),
                                            event_type,
                                            progress.child_state.as_str(),
                                            false,
                                            details,
                                        )
                                        .await?;
                                        last_progress_at_unix_ms = now;
                                    }
                                    ChildLifecycleTapeDecision::Suppress => {}
                                }
                            }
                        }
                    }
                    Some(Err(error)) => {
                        let message = format!("background run stream read failed: {error}");
                        stream_error = Some(message.clone());
                        append_child_lifecycle_event(
                            runtime,
                            task,
                            Some(run_id),
                            "child_failed",
                            "transport_error",
                            true,
                            json!({ "error": message }),
                        )
                        .await?;
                        break;
                    }
                    None => break,
                }
            }
            _ = heartbeat.tick() => {
                // Defensive exit: if the run reached a terminal state but the
                // stream stays open, stop supervising instead of hanging.
                if child_run_is_terminal(runtime, run_id).await? {
                    break;
                }
                match parent_tape_budget.record_scheduled_heartbeat() {
                    ChildLifecycleTapeDecision::Emit => {
                        append_child_lifecycle_event(
                            runtime,
                            task,
                            Some(run_id),
                            "child_heartbeat",
                            latest_child_state.as_str(),
                            false,
                            json!({ "state": latest_child_state }),
                        )
                        .await?;
                    }
                    ChildLifecycleTapeDecision::EmitLimitNotice {
                        event_type,
                        details,
                    } => {
                        append_child_lifecycle_event(
                            runtime,
                            task,
                            Some(run_id),
                            event_type,
                            latest_child_state.as_str(),
                            false,
                            details,
                        )
                        .await?;
                    }
                    ChildLifecycleTapeDecision::Suppress => {}
                }
            }
        }
    }

    // The run snapshot is authoritative: persist merge results and the
    // terminal task state first; a transport error is surfaced as an error
    // only when no snapshot exists at all.
    let run_snapshot = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
    if let Some(run) = run_snapshot.as_ref() {
        let run_with_merge = if let Some(delegation) = task.delegation.as_ref() {
            let merge_result = build_merge_result(runtime, run, delegation).await?;
            runtime
                .update_orchestrator_run_metadata(OrchestratorRunMetadataUpdateRequest {
                    run_id: run_id.to_owned(),
                    parent_run_id: Some(task.parent_run_id.clone()),
                    delegation: Some(Some(delegation.clone())),
                    merge_result: Some(Some(merge_result.clone())),
                })
                .await?;
            let refreshed = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?;
            append_parent_merge_event(runtime, task, run, &merge_result).await?;
            refreshed.unwrap_or_else(|| run.clone())
        } else {
            run.clone()
        };
        finalize_task_from_run(runtime, task, Some(&run_with_merge), run_with_merge.state.as_str())
            .await?;
        if let Some(error_message) = stream_error {
            warn!(
                task_id = %task.task_id,
                run_id = %run_id,
                status = %run_with_merge.state,
                error = %error_message,
                "background run stream ended with a transport error after persistence"
            );
        }
        return Ok(());
    }

    if let Some(error_message) = stream_error {
        return Err(Status::internal(error_message));
    }

    Err(Status::internal(format!("background run {run_id} finished without a persisted snapshot")))
}

/// Update that marks a task Running and counts the attempt; `target_run_id`
/// is reset because the child run does not exist yet (see attach below).
fn build_background_task_running_update(
    task_id: &str,
    started_at_unix_ms: i64,
) -> OrchestratorBackgroundTaskUpdateRequest {
    OrchestratorBackgroundTaskUpdateRequest {
        task_id: task_id.to_owned(),
        state: Some(AuxiliaryTaskState::Running.as_str().to_owned()),
        target_run_id: Some(None),
        increment_attempt_count: true,
        last_error: Some(None),
        result_json: Some(None),
        started_at_unix_ms: Some(Some(started_at_unix_ms)),
        completed_at_unix_ms: Some(None),
    }
}

/// Update that attaches the persisted child run id to the task, leaving all
/// other fields untouched.
fn build_background_task_child_run_attach_update(
    task_id: &str,
    run_id: &str,
) -> OrchestratorBackgroundTaskUpdateRequest {
    OrchestratorBackgroundTaskUpdateRequest {
        task_id: task_id.to_owned(),
        target_run_id: Some(Some(run_id.to_owned())),
        ..Default::default()
    }
}

/// Waits for the child run row, attaches it to the task, and emits the spawn
/// and conversation-binding events.
async fn attach_background_task_child_run(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run_id: &str,
) -> Result<(), Status> {
    wait_for_background_child_run(runtime, run_id).await?;
    runtime
        .update_orchestrator_background_task(build_background_task_child_run_attach_update(
            task.task_id.as_str(),
            run_id,
        ))
        .await?;
    deliver_pending_child_cancel_after_attach(runtime, task.task_id.as_str(), run_id).await?;
    append_parent_spawned_event(runtime, task, run_id).await?;
    create_delegated_child_binding(runtime, task, run_id).await
}

async fn deliver_pending_child_cancel_after_attach(
    runtime: &Arc<GatewayRuntimeState>,
    task_id: &str,
    run_id: &str,
) -> Result<(), Status> {
    let Some(task) = runtime.get_orchestrator_background_task(task_id.to_owned()).await? else {
        return Ok(());
    };
    if let Some(reason) = pending_child_cancel_reason(&task) {
        request_background_child_cancel(runtime, run_id, reason).await?;
    }
    Ok(())
}

/// Polls until the child run row is persisted or the attach timeout elapses;
/// the gateway accepts RunStream requests before persisting the run.
async fn wait_for_background_child_run(
    runtime: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<(), Status> {
    let deadline = Instant::now() + CHILD_RUN_ATTACH_TIMEOUT;
    loop {
        if runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Status::internal(format!(
                "background child run {run_id} was not persisted before target attachment"
            )));
        }
        tokio::time::sleep(CHILD_RUN_ATTACH_POLL_INTERVAL).await;
    }
}

async fn child_run_is_terminal(
    runtime: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<bool, Status> {
    let Some(snapshot) = runtime.orchestrator_run_status_snapshot(run_id.to_owned()).await? else {
        return Ok(false);
    };
    Ok(is_terminal_run_state(snapshot.state.as_str()))
}

/// Records a conversation binding for a delegated child run; no-op for plain
/// background tasks.
async fn create_delegated_child_binding(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: &str,
) -> Result<(), Status> {
    if task.delegation.is_none() {
        return Ok(());
    }
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    let now = crate::gateway::current_unix_ms();
    let outcome = runtime
        .conversation_bindings
        .create_or_touch(ConversationBindingCreateRequest {
            binding_kind: ConversationBindingKind::DelegatedRun,
            channel: task.channel.clone().unwrap_or_else(|| DEFAULT_BACKGROUND_CHANNEL.to_owned()),
            conversation_id: Some(task.session_id.clone()),
            thread_id: Some(format!("delegation:{}", task.task_id)),
            sender_identity: Some(format!("delegated-run:{child_run_id}")),
            principal: task.owner_principal.clone(),
            session_id: task.session_id.clone(),
            workspace_id: None,
            policy_scope: format!("delegation:{parent_run_id}"),
            parent_binding_id: None,
            lifecycle: ConversationBindingLifecycle::default(),
            now_unix_ms: now,
        })
        .map_err(|error| Status::internal(error.safe_message()))?;

    append_child_lifecycle_event(
        runtime,
        task,
        Some(child_run_id),
        "child_binding_created",
        "running",
        false,
        json!({
            "binding": outcome.record.safe_snapshot_json(),
            "created": outcome.created,
            "reason": outcome.reason,
        }),
    )
    .await
}

/// Propagates a parent run's cancellation down to this child task; returns
/// `true` when the task was handled and processing should stop this poll.
async fn sync_parent_run_cancellation(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(false);
    };
    let Some(parent_run) =
        runtime.orchestrator_run_status_snapshot(parent_run_id.to_owned()).await?
    else {
        return Ok(false);
    };
    if !parent_run.cancel_requested && parent_run.state != "cancelled" {
        return Ok(false);
    }

    let cancellation_reason = "cancelled because the parent run was cancelled".to_owned();
    if let Some(target_run_id) = task.target_run_id.as_ref() {
        let child_run = runtime.orchestrator_run_status_snapshot(target_run_id.clone()).await?;
        if child_run.as_ref().is_some_and(|snapshot| is_terminal_run_state(snapshot.state.as_str()))
        {
            finalize_task_from_run(
                runtime,
                task,
                child_run.as_ref(),
                child_run.as_ref().map(|snapshot| snapshot.state.as_str()).unwrap_or("cancelled"),
            )
            .await?;
            return Ok(true);
        }
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
                target_run_id: None,
                increment_attempt_count: false,
                last_error: Some(Some(cancellation_reason.clone())),
                result_json: None,
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
            })
            .await?;
        runtime
            .request_orchestrator_cancel(crate::journal::OrchestratorCancelRequest {
                run_id: target_run_id.clone(),
                reason: "delegated_parent_cancelled".to_owned(),
            })
            .await?;
        return Ok(true);
    }

    if task_has_in_flight_work_without_target(task) {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::CancelRequested.as_str().to_owned()),
                target_run_id: None,
                increment_attempt_count: false,
                last_error: Some(Some(cancellation_reason)),
                result_json: None,
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
            })
            .await?;
    } else {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
                target_run_id: Some(None),
                increment_attempt_count: false,
                last_error: Some(Some(cancellation_reason.clone())),
                result_json: Some(Some(
                    json!({
                        "status": "cancelled",
                        "task_id": task.task_id,
                        "reason": cancellation_reason,
                        "parent_run_id": parent_run_id,
                    })
                    .to_string(),
                )),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
            })
            .await?;
    }
    Ok(true)
}

/// True when an earlier or still-active sibling in the same serial group
/// must finish before `task` may dispatch.
fn task_is_blocked_by_serial_sibling(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
) -> bool {
    let Some(group_id) = delegation_serial_group(task) else {
        return false;
    };
    all_tasks.iter().any(|candidate| {
        candidate.task_id != task.task_id
            && delegation_serial_group(candidate).is_some_and(|candidate_group| {
                candidate_group == group_id && serial_sibling_blocks(candidate, task)
            })
    })
}

/// The serial group id, when the task participates in serial execution.
fn delegation_serial_group(task: &OrchestratorBackgroundTaskRecord) -> Option<&str> {
    let delegation = task.delegation.as_ref()?;
    (delegation.execution_mode == DelegationExecutionMode::Serial)
        .then_some(delegation.group_id.as_str())
}

/// Active siblings always block; queued/paused siblings block only when they
/// precede `current` in FIFO order. Terminal siblings never block, so one
/// failure cannot wedge the rest of the group.
fn serial_sibling_blocks(
    sibling: &OrchestratorBackgroundTaskRecord,
    current: &OrchestratorBackgroundTaskRecord,
) -> bool {
    if is_terminal_task_state(sibling.state.as_str())
        || AuxiliaryTaskState::from_str(sibling.state.as_str()) == Some(AuxiliaryTaskState::Failed)
    {
        return false;
    }
    match AuxiliaryTaskState::from_str(sibling.state.as_str()) {
        Some(AuxiliaryTaskState::Running) => true,
        Some(AuxiliaryTaskState::CancelRequested) => {
            sibling.target_run_id.is_some() || task_has_in_flight_work_without_target(sibling)
        }
        Some(AuxiliaryTaskState::Queued | AuxiliaryTaskState::Paused) => {
            task_precedes_in_serial_group(sibling, current)
        }
        _ => false,
    }
}

/// Strict FIFO predecessor test (created_at, then task id tiebreaker).
fn task_precedes_in_serial_group(
    sibling: &OrchestratorBackgroundTaskRecord,
    current: &OrchestratorBackgroundTaskRecord,
) -> bool {
    sibling.created_at_unix_ms < current.created_at_unix_ms
        || (sibling.created_at_unix_ms == current.created_at_unix_ms
            && sibling.task_id < current.task_id)
}

/// Folds a child run's terminal state into the task record and clears the
/// heartbeat; non-terminal states are a no-op so supervision continues.
async fn finalize_task_from_run(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: Option<&crate::journal::OrchestratorRunStatusSnapshot>,
    fallback_state: &str,
) -> Result<(), Status> {
    let normalized_state = match run.map(|value| value.state.as_str()).unwrap_or(fallback_state) {
        "done" => AuxiliaryTaskState::Succeeded,
        "cancelled" => AuxiliaryTaskState::Cancelled,
        "failed" => AuxiliaryTaskState::Failed,
        "running" | "accepted" | "in_progress" => AuxiliaryTaskState::Running,
        "expired" => AuxiliaryTaskState::Expired,
        other => AuxiliaryTaskState::from_str(other).unwrap_or(AuxiliaryTaskState::Failed),
    };
    if normalized_state == AuxiliaryTaskState::Running {
        return Ok(());
    }
    let completed_at_unix_ms = run
        .and_then(|value| value.completed_at_unix_ms)
        .unwrap_or_else(crate::gateway::current_unix_ms);
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(normalized_state.as_str().to_owned()),
            target_run_id: None,
            increment_attempt_count: false,
            last_error: Some(run.and_then(|value| value.last_error.clone())),
            result_json: Some(Some(
                json!({
                    "status": normalized_state.as_str(),
                    "task_id": task.task_id,
                    "run": run.map(run_status_to_json).unwrap_or_else(|| json!({
                        "state": fallback_state,
                    })),
                })
                .to_string(),
            )),
            started_at_unix_ms: None,
            completed_at_unix_ms: Some(Some(completed_at_unix_ms)),
        })
        .await?;
    runtime.clear_self_healing_heartbeat(WorkHeartbeatKind::BackgroundTask, task.task_id.as_str());
    Ok(())
}

/// Extracts the raw `parameter_delta` object from a task payload as bytes;
/// empty when the payload or the key is absent.
fn extract_parameter_delta_bytes(payload_json: Option<&str>) -> Result<Vec<u8>, Status> {
    let Some(payload_json) = payload_json else {
        return Ok(Vec::new());
    };
    if payload_json.trim().is_empty() {
        return Ok(Vec::new());
    }
    let payload = serde_json::from_str::<Value>(payload_json).map_err(|error| {
        Status::invalid_argument(format!("invalid background payload_json: {error}"))
    })?;
    let Some(parameter_delta) = payload.get("parameter_delta") else {
        return Ok(Vec::new());
    };
    serde_json::to_vec(parameter_delta).map_err(|error| {
        Status::internal(format!("failed to encode background parameter_delta: {error}"))
    })
}

/// Merges the task's stored parameter delta with synthesized
/// `background_task` and delegation context forwarded to the child run.
fn build_parameter_delta_bytes(task: &OrchestratorBackgroundTaskRecord) -> Result<Vec<u8>, Status> {
    let mut merged = match extract_parameter_delta_value(task.payload_json.as_deref())? {
        Some(Value::Object(object)) => Value::Object(object),
        // A non-object stored delta cannot take extra keys; preserve it under
        // a dedicated key instead of dropping it.
        Some(other) => json!({ "prior_parameter_delta": other }),
        None => json!({}),
    };
    if let Some(root) = merged.as_object_mut() {
        root.insert(
            "background_task".to_owned(),
            json!({
                "task_id": task.task_id,
                "task_kind": task.task_kind,
                "parent_run_id": task.parent_run_id,
                "budget_tokens": task.budget_tokens,
            }),
        );
        if let Some(delegation) = task.delegation.as_ref() {
            root.insert(
                "delegation".to_owned(),
                serde_json::to_value(delegation).map_err(|error| {
                    Status::internal(format!(
                        "failed to encode background delegation parameter_delta: {error}"
                    ))
                })?,
            );
            root.insert(
                "delegation_scope".to_owned(),
                build_task_delegated_scope(task, delegation)?.safe_snapshot_json(),
            );
        }
    }
    serde_json::to_vec(&merged).map_err(|error| {
        Status::internal(format!("failed to encode background parameter_delta bytes: {error}"))
    })
}

/// Short objective excerpt used in delegated scope and provenance records.
fn task_objective(task: &OrchestratorBackgroundTaskRecord) -> String {
    task.input_text
        .as_deref()
        .map(truncate_excerpt_512)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("Delegated task {} ({})", task.task_id, task.task_kind))
}

/// Builds the bounded scope (context/memory refs, tool and skill allowlists)
/// a delegated child is allowed to operate within.
fn build_task_delegated_scope(
    task: &OrchestratorBackgroundTaskRecord,
    delegation: &DelegationSnapshot,
) -> Result<crate::delegation::DelegatedRunScope, Status> {
    let mut context_refs = Vec::new();
    if let Some(parent_run_id) = task.parent_run_id.as_deref() {
        context_refs.push(DelegatedReferenceInput {
            ref_id: parent_run_id.to_owned(),
            reason: "parent run summary and progress refs".to_owned(),
            sensitivity: "internal".to_owned(),
        });
    }
    let memory_refs =
        if delegation.memory_scope == crate::delegation::DelegationMemoryScopeKind::None {
            Vec::new()
        } else {
            vec![DelegatedReferenceInput {
                ref_id: task.session_id.clone(),
                reason: "delegated memory recall scope".to_owned(),
                sensitivity: "internal".to_owned(),
            }]
        };

    build_delegated_scope(DelegatedScopeBuildRequest {
        objective: task_objective(task),
        delegation: delegation.clone(),
        parent_tool_allowlist: delegation.tool_allowlist.clone(),
        parent_skill_allowlist: delegation.skill_allowlist.clone(),
        context_refs,
        memory_refs,
        artifact_refs: Vec::new(),
    })
}

/// Builds the delegated-run record snapshot emitted with lifecycle events;
/// requires delegation metadata and a parent run id.
fn build_task_delegated_record(
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: Option<&str>,
    state: DelegatedRunState,
    merge_status: DelegationMergeStatus,
    event_type: &str,
    event_reason: &str,
) -> Result<crate::delegation::DelegatedRunRecord, Status> {
    let Some(delegation) = task.delegation.as_ref() else {
        return Err(Status::failed_precondition("delegated run record requires delegation"));
    };
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Err(Status::failed_precondition("delegated run record requires a parent run id"));
    };
    let scope = build_task_delegated_scope(task, delegation)?;
    Ok(build_delegated_run_record(DelegatedRunRecordBuildRequest {
        parent_run_id: parent_run_id.to_owned(),
        child_run_id: child_run_id.map(ToOwned::to_owned),
        task_id: Some(task.task_id.clone()),
        objective: task_objective(task),
        scope,
        delegation: delegation.clone(),
        state,
        merge_status,
        event_type: event_type.to_owned(),
        event_reason: event_reason.to_owned(),
        observed_at_unix_ms: crate::gateway::current_unix_ms(),
    }))
}

fn truncate_excerpt_512(value: &str) -> String {
    truncate_excerpt(value, 512)
}

fn extract_parameter_delta_value(payload_json: Option<&str>) -> Result<Option<Value>, Status> {
    let bytes = extract_parameter_delta_bytes(payload_json)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(bytes.as_slice()).map(Some).map_err(|error| {
        Status::internal(format!("failed to parse background parameter_delta value: {error}"))
    })
}

/// Parent-tape projection of one child run stream event.
struct ChildStreamProgress {
    event_type: &'static str,
    child_state: String,
    user_visible: bool,
    details: Value,
}

/// What to write to the parent tape for one child event.
#[derive(Debug)]
enum ChildLifecycleTapeDecision {
    Emit,
    EmitLimitNotice { event_type: &'static str, details: Value },
    Suppress,
}

/// Budget for child events mirrored onto a parent tape.
///
/// Progress and heartbeats are capped separately; the first overflow of each
/// kind emits one `*_compacted` notice and everything after it is suppressed.
/// Terminal progress events bypass the budget so completion is never lost.
struct ChildLifecycleTapeBudget {
    progress_events: usize,
    heartbeat_events: usize,
    progress_limit: usize,
    heartbeat_limit: usize,
    progress_limit_notice_emitted: bool,
    heartbeat_limit_notice_emitted: bool,
}

impl Default for ChildLifecycleTapeBudget {
    fn default() -> Self {
        Self::with_limits(
            CHILD_PARENT_PROGRESS_TAPE_EVENT_LIMIT,
            CHILD_PARENT_HEARTBEAT_TAPE_EVENT_LIMIT,
        )
    }
}

impl ChildLifecycleTapeBudget {
    fn with_limits(progress_limit: usize, heartbeat_limit: usize) -> Self {
        Self {
            progress_events: 0,
            heartbeat_events: 0,
            progress_limit,
            heartbeat_limit,
            progress_limit_notice_emitted: false,
            heartbeat_limit_notice_emitted: false,
        }
    }

    fn record_stream_event(
        &mut self,
        progress: &ChildStreamProgress,
        details: &Value,
    ) -> ChildLifecycleTapeDecision {
        match budgeted_child_lifecycle_kind(progress.event_type) {
            Some(ChildLifecycleTapeKind::Progress)
                if !is_terminal_child_progress(
                    progress.event_type,
                    progress.child_state.as_str(),
                ) =>
            {
                Self::record_budgeted_event(
                    &mut self.progress_events,
                    self.progress_limit,
                    &mut self.progress_limit_notice_emitted,
                    "child_progress_compacted",
                    "parent_tape_child_progress_limit",
                    progress.event_type,
                    stream_event_name(details),
                )
            }
            Some(ChildLifecycleTapeKind::Heartbeat) => Self::record_budgeted_event(
                &mut self.heartbeat_events,
                self.heartbeat_limit,
                &mut self.heartbeat_limit_notice_emitted,
                "child_heartbeat_compacted",
                "parent_tape_child_heartbeat_limit",
                progress.event_type,
                stream_event_name(details),
            ),
            _ => ChildLifecycleTapeDecision::Emit,
        }
    }

    fn record_scheduled_heartbeat(&mut self) -> ChildLifecycleTapeDecision {
        Self::record_budgeted_event(
            &mut self.heartbeat_events,
            self.heartbeat_limit,
            &mut self.heartbeat_limit_notice_emitted,
            "child_heartbeat_compacted",
            "parent_tape_child_heartbeat_limit",
            "child_heartbeat",
            Some("scheduled_heartbeat"),
        )
    }

    fn record_budgeted_event(
        count: &mut usize,
        limit: usize,
        limit_notice_emitted: &mut bool,
        limit_event_type: &'static str,
        reason: &'static str,
        suppressed_event_type: &'static str,
        stream_event: Option<&str>,
    ) -> ChildLifecycleTapeDecision {
        if *count < limit {
            *count += 1;
            return ChildLifecycleTapeDecision::Emit;
        }
        if *limit_notice_emitted {
            return ChildLifecycleTapeDecision::Suppress;
        }
        *limit_notice_emitted = true;
        ChildLifecycleTapeDecision::EmitLimitNotice {
            event_type: limit_event_type,
            details: json!({
                "reason": reason,
                "max_events": limit,
                "suppressed_event_type": suppressed_event_type,
                "stream_event": stream_event,
            }),
        }
    }
}

enum ChildLifecycleTapeKind {
    Progress,
    Heartbeat,
}

fn budgeted_child_lifecycle_kind(event_type: &str) -> Option<ChildLifecycleTapeKind> {
    match event_type {
        "child_progress" => Some(ChildLifecycleTapeKind::Progress),
        "child_heartbeat" => Some(ChildLifecycleTapeKind::Heartbeat),
        _ => None,
    }
}

fn stream_event_name(details: &Value) -> Option<&str> {
    details.get("stream_event").and_then(Value::as_str)
}

/// Throttles non-terminal `child_progress` to one parent-tape event per
/// `CHILD_PROGRESS_MIN_INTERVAL_MS`; all other event types always pass.
fn should_emit_child_stream_progress(
    progress: &ChildStreamProgress,
    now_unix_ms: i64,
    last_progress_at_unix_ms: i64,
) -> bool {
    progress.event_type != "child_progress"
        || is_terminal_child_progress(progress.event_type, progress.child_state.as_str())
        || now_unix_ms.saturating_sub(last_progress_at_unix_ms) >= CHILD_PROGRESS_MIN_INTERVAL_MS
}

impl ChildStreamProgress {
    fn to_delivery_progress_update(
        &self,
        child_run_id: &str,
        observed_at_unix_ms: i64,
    ) -> DeliveryProgressUpdate {
        let detail = delivery_progress_detail(&self.details);
        if self.event_type == "child_waiting" || self.child_state == "waiting_for_approval" {
            return DeliveryProgressUpdate::approval_wait(
                child_run_id.to_owned(),
                detail,
                observed_at_unix_ms,
            );
        }
        DeliveryProgressUpdate::child_run(
            child_run_id.to_owned(),
            self.child_state.clone(),
            detail,
            self.user_visible,
            is_terminal_child_progress(self.event_type, self.child_state.as_str()),
            observed_at_unix_ms,
        )
    }
}

/// Maps a run stream event to its parent-tape projection; `None` for events
/// that are never mirrored. Free-text fields are excerpted so tape payloads
/// stay bounded regardless of stream contents.
fn summarize_child_stream_event(
    event: &common_v1::RunStreamEvent,
    model_token_chars: &mut usize,
) -> Option<ChildStreamProgress> {
    match event.body.as_ref()? {
        common_v1::run_stream_event::Body::Status(status) => {
            let (event_type, child_state, user_visible) = match status_kind(status.kind) {
                Some(common_v1::stream_status::StatusKind::Accepted) => {
                    ("child_progress", "accepted", false)
                }
                Some(common_v1::stream_status::StatusKind::InProgress) => {
                    ("child_progress", "running", true)
                }
                Some(common_v1::stream_status::StatusKind::Done) => {
                    ("child_completed", "completed", true)
                }
                Some(common_v1::stream_status::StatusKind::Failed) => {
                    ("child_failed", "failed", true)
                }
                _ => ("child_progress", "unknown", false),
            };
            Some(ChildStreamProgress {
                event_type,
                child_state: child_state.to_owned(),
                user_visible,
                details: json!({
                    "stream_event": "status",
                    "message": truncate_excerpt(status.message.as_str(), 240),
                }),
            })
        }
        common_v1::run_stream_event::Body::ModelToken(model_token) => {
            *model_token_chars =
                model_token_chars.saturating_add(model_token.token.chars().count());
            Some(ChildStreamProgress {
                event_type: "child_progress",
                child_state: if model_token.is_final {
                    "model_stream_final".to_owned()
                } else {
                    "model_streaming".to_owned()
                },
                user_visible: true,
                details: json!({
                    "stream_event": "model_token",
                    "token_chars_seen": *model_token_chars,
                    "is_final": model_token.is_final,
                }),
            })
        }
        common_v1::run_stream_event::Body::ToolProposal(proposal) => Some(ChildStreamProgress {
            event_type: "child_progress",
            child_state: "tool_proposed".to_owned(),
            user_visible: false,
            details: json!({
                "stream_event": "tool_proposal",
                "proposal_id": proposal.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "tool_name": proposal.tool_name,
                "approval_required": proposal.approval_required,
            }),
        }),
        common_v1::run_stream_event::Body::ToolDecision(decision) => Some(ChildStreamProgress {
            event_type: if decision.approval_required { "child_waiting" } else { "child_progress" },
            child_state: if decision.approval_required {
                "waiting_for_approval".to_owned()
            } else {
                "tool_decided".to_owned()
            },
            user_visible: decision.approval_required,
            details: json!({
                "stream_event": "tool_decision",
                "proposal_id": decision.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "decision": tool_decision_kind(decision.kind),
                "approval_required": decision.approval_required,
                "policy_enforced": decision.policy_enforced,
                "reason": truncate_excerpt(decision.reason.as_str(), 240),
            }),
        }),
        common_v1::run_stream_event::Body::ToolApprovalRequest(request) => {
            Some(ChildStreamProgress {
                event_type: "child_waiting",
                child_state: "waiting_for_approval".to_owned(),
                user_visible: true,
                details: json!({
                    "stream_event": "tool_approval_request",
                    "proposal_id": request.proposal_id.as_ref().map(|value| value.ulid.clone()),
                    "approval_id": request.approval_id.as_ref().map(|value| value.ulid.clone()),
                    "tool_name": request.tool_name,
                    "request_summary": truncate_excerpt(request.request_summary.as_str(), 240),
                }),
            })
        }
        common_v1::run_stream_event::Body::ToolApprovalResponse(response) => {
            Some(ChildStreamProgress {
                event_type: "child_progress",
                child_state: "approval_resolved".to_owned(),
                user_visible: true,
                details: json!({
                    "stream_event": "tool_approval_response",
                    "proposal_id": response.proposal_id.as_ref().map(|value| value.ulid.clone()),
                    "approval_id": response.approval_id.as_ref().map(|value| value.ulid.clone()),
                    "approved": response.approved,
                    "reason": truncate_excerpt(response.reason.as_str(), 240),
                }),
            })
        }
        common_v1::run_stream_event::Body::ToolResult(result) => Some(ChildStreamProgress {
            event_type: "child_progress",
            child_state: if result.success {
                "tool_completed".to_owned()
            } else {
                "tool_failed".to_owned()
            },
            user_visible: !result.success,
            details: json!({
                "stream_event": "tool_result",
                "proposal_id": result.proposal_id.as_ref().map(|value| value.ulid.clone()),
                "success": result.success,
                "error": truncate_excerpt(result.error.as_str(), 240),
            }),
        }),
        common_v1::run_stream_event::Body::ToolAttestation(attestation) => {
            Some(ChildStreamProgress {
                event_type: "child_heartbeat",
                child_state: "tool_attested".to_owned(),
                user_visible: false,
                details: json!({
                    "stream_event": "tool_attestation",
                    "proposal_id": attestation.proposal_id.as_ref().map(|value| value.ulid.clone()),
                    "attestation_id": attestation.attestation_id.as_ref().map(|value| value.ulid.clone()),
                    "timed_out": attestation.timed_out,
                    "executor": attestation.executor,
                }),
            })
        }
        common_v1::run_stream_event::Body::A2uiUpdate(_)
        | common_v1::run_stream_event::Body::JournalEvent(_) => None,
    }
}

fn status_kind(raw: i32) -> Option<common_v1::stream_status::StatusKind> {
    common_v1::stream_status::StatusKind::try_from(raw).ok()
}

fn tool_decision_kind(raw: i32) -> &'static str {
    match common_v1::tool_decision::DecisionKind::try_from(raw)
        .unwrap_or(common_v1::tool_decision::DecisionKind::Unspecified)
    {
        common_v1::tool_decision::DecisionKind::Allow => "allow",
        common_v1::tool_decision::DecisionKind::Deny => "deny",
        common_v1::tool_decision::DecisionKind::Unspecified => "unspecified",
    }
}

/// Replays the child run tape into a `DelegationMergeResult`: model output,
/// tool provenance (trace summary capped at 24 entries), artifact references
/// (capped at 16), and approval signals, summarized per the merge strategy.
async fn build_merge_result(
    runtime: &Arc<GatewayRuntimeState>,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    delegation: &DelegationSnapshot,
) -> Result<DelegationMergeResult, Status> {
    let tape_events = load_run_tape(runtime, run.run_id.as_str()).await?;
    let mut proposals = HashMap::<String, (String, bool)>::new();
    let mut model_output = String::new();
    let mut warnings = Vec::new();
    let mut provenance = Vec::new();
    let mut approval_summary = DelegationMergeApprovalSummary {
        approval_required: delegation.merge_contract.approval_required,
        ..DelegationMergeApprovalSummary::default()
    };
    let mut artifact_references = Vec::new();
    let mut tool_trace_summary = Vec::new();

    for event in tape_events {
        let payload =
            serde_json::from_str::<Value>(event.payload_json.as_str()).unwrap_or(Value::Null);
        match event.event_type.as_str() {
            "tool_proposal" => {
                let Some(proposal_id) = payload.get("proposal_id").and_then(Value::as_str) else {
                    continue;
                };
                let tool_name =
                    payload.get("tool_name").and_then(Value::as_str).unwrap_or("unknown_tool");
                let approval_required =
                    payload.get("approval_required").and_then(Value::as_bool).unwrap_or(false);
                approval_summary.approval_required |= approval_required;
                proposals.insert(proposal_id.to_owned(), (tool_name.to_owned(), approval_required));
            }
            "tool_approval_request" => {
                approval_summary.approval_required = true;
                approval_summary.approval_events =
                    approval_summary.approval_events.saturating_add(1);
                approval_summary.approval_pending = true;
            }
            "tool_approval_response" => {
                approval_summary.approval_events =
                    approval_summary.approval_events.saturating_add(1);
                approval_summary.approval_pending = false;
                if !payload.get("approved").and_then(Value::as_bool).unwrap_or(false) {
                    approval_summary.approval_denied = true;
                }
            }
            "tool_decision" => {
                let approval_required =
                    payload.get("approval_required").and_then(Value::as_bool).unwrap_or(false);
                approval_summary.approval_required |= approval_required;
                if approval_required {
                    approval_summary.approval_events =
                        approval_summary.approval_events.saturating_add(1);
                }
                if payload.get("kind").and_then(Value::as_str) == Some("deny") {
                    approval_summary.approval_denied = true;
                }
            }
            "model_token" => {
                if let Some(token) = payload.get("token").and_then(Value::as_str) {
                    model_output.push_str(token);
                }
            }
            "tool_result" => {
                let proposal_id = payload
                    .get("proposal_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown-proposal");
                let (tool_name, approval_required) = proposals
                    .get(proposal_id)
                    .cloned()
                    .unwrap_or_else(|| ("unknown_tool".to_owned(), false));
                let success = payload.get("success").and_then(Value::as_bool).unwrap_or(true);
                let excerpt = payload
                    .get("output_json")
                    .map(value_excerpt)
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        payload.get("error").and_then(Value::as_str).map(ToString::to_string)
                    })
                    .unwrap_or_else(|| "tool completed without a structured payload".to_owned());
                provenance.push(DelegationMergeProvenanceRecord {
                    child_run_id: run.run_id.clone(),
                    kind: "tool_result".to_owned(),
                    label: tool_name.clone(),
                    excerpt: truncate_excerpt(excerpt.as_str(), 240),
                    tool_name: Some(tool_name),
                    requires_approval: approval_required,
                });
                if tool_trace_summary.len() < 24 {
                    tool_trace_summary.push(DelegationToolTraceSummary {
                        child_run_id: run.run_id.clone(),
                        proposal_id: Some(proposal_id.to_owned()),
                        tool_name: provenance
                            .last()
                            .and_then(|record| record.tool_name.clone())
                            .unwrap_or_else(|| "unknown_tool".to_owned()),
                        status: if success { "succeeded" } else { "failed" }.to_owned(),
                        excerpt: truncate_excerpt(excerpt.as_str(), 320),
                        requires_approval: approval_required,
                    });
                }
                if let Some(output_json) = payload.get("output_json") {
                    append_artifact_references(
                        &mut artifact_references,
                        output_json,
                        run.run_id.as_str(),
                    );
                }
            }
            _ => {}
        }
    }

    if model_output.trim().is_empty() {
        warnings.push("child run finished without model output tokens".to_owned());
    } else {
        provenance.insert(
            0,
            DelegationMergeProvenanceRecord {
                child_run_id: run.run_id.clone(),
                kind: "model_summary".to_owned(),
                label: "Model output".to_owned(),
                excerpt: truncate_excerpt(model_output.trim(), 320),
                tool_name: None,
                requires_approval: delegation.merge_contract.approval_required,
            },
        );
    }
    if run.state == "failed" {
        warnings.push(
            run.last_error.clone().unwrap_or_else(|| "child run failed before merge".to_owned()),
        );
    } else if run.state == "cancelled" {
        warnings.push("child run was cancelled before merge".to_owned());
    }

    let summary_text = build_merge_summary(
        delegation.merge_contract.strategy,
        run,
        model_output.trim(),
        provenance.as_slice(),
        warnings.as_slice(),
    );
    let usage_summary = DelegationMergeUsageSummary {
        prompt_tokens: run.prompt_tokens,
        completion_tokens: run.completion_tokens,
        total_tokens: run.total_tokens,
        started_at_unix_ms: Some(run.started_at_unix_ms),
        completed_at_unix_ms: run.completed_at_unix_ms,
        duration_ms: run
            .completed_at_unix_ms
            .map(|completed_at| completed_at.saturating_sub(run.started_at_unix_ms)),
    };
    let failure_category = categorize_child_failure(
        run,
        warnings.as_slice(),
        tool_trace_summary.as_slice(),
        &approval_summary,
    );
    Ok(DelegationMergeResult {
        status: run.state.clone(),
        strategy: delegation.merge_contract.strategy,
        summary_text,
        warnings,
        failure_category,
        approval_required: delegation.merge_contract.approval_required,
        approval_summary,
        usage_summary,
        artifact_references,
        tool_trace_summary,
        provenance,
        merged_at_unix_ms: Some(crate::gateway::current_unix_ms()),
    })
}

/// Depth-first scan for artifact references in tool output JSON; dedupes by
/// artifact id and stops at 16 references to bound payload size.
fn append_artifact_references(
    references: &mut Vec<DelegationMergeArtifactReference>,
    value: &Value,
    child_run_id: &str,
) {
    if references.len() >= 16 {
        return;
    }
    match value {
        Value::Object(object) => {
            if let Some(artifact_id) = object.get("artifact_id").and_then(Value::as_str) {
                let artifact_kind = object
                    .get("artifact_kind")
                    .or_else(|| object.get("kind"))
                    .and_then(Value::as_str)
                    .unwrap_or("artifact");
                let label = object
                    .get("label")
                    .or_else(|| object.get("filename"))
                    .or_else(|| object.get("path"))
                    .and_then(Value::as_str)
                    .unwrap_or(child_run_id);
                if !references.iter().any(|reference| reference.artifact_id == artifact_id) {
                    references.push(DelegationMergeArtifactReference {
                        artifact_id: artifact_id.to_owned(),
                        artifact_kind: artifact_kind.to_owned(),
                        label: truncate_excerpt(label, 160),
                    });
                }
            }
            for key in ["artifact", "artifacts", "artifact_reference", "artifact_references"] {
                if let Some(candidate) = object.get(key) {
                    append_artifact_references(references, candidate, child_run_id);
                }
                if references.len() >= 16 {
                    break;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                append_artifact_references(references, item, child_run_id);
                if references.len() >= 16 {
                    break;
                }
            }
        }
        _ => {}
    }
}

/// Best-effort failure taxonomy: structured signals first (cancellation,
/// approval state, failed tool traces), then substring heuristics over the
/// error and warning text. `None` for successful runs.
fn categorize_child_failure(
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    warnings: &[String],
    tool_trace_summary: &[DelegationToolTraceSummary],
    approval_summary: &DelegationMergeApprovalSummary,
) -> Option<DelegationMergeFailureCategory> {
    if run.state == "done" {
        return None;
    }
    if run.state == "cancelled" || run.cancel_requested {
        return Some(DelegationMergeFailureCategory::Cancellation);
    }
    if approval_summary.approval_denied || approval_summary.approval_pending {
        return Some(DelegationMergeFailureCategory::Approval);
    }
    if tool_trace_summary.iter().any(|trace| trace.status == "failed") {
        return Some(DelegationMergeFailureCategory::Tool);
    }
    let message = run
        .last_error
        .iter()
        .chain(warnings.iter())
        .map(|value| value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    if message.contains("budget") || message.contains("quota") || message.contains("limit") {
        return Some(DelegationMergeFailureCategory::Budget);
    }
    if message.contains("approval") {
        return Some(DelegationMergeFailureCategory::Approval);
    }
    if message.contains("tool") || message.contains("sandbox") {
        return Some(DelegationMergeFailureCategory::Tool);
    }
    if message.contains("provider") || message.contains("model") || message.contains("circuit") {
        return Some(DelegationMergeFailureCategory::Model);
    }
    if message.contains("transport") || message.contains("stream") || message.contains("connect") {
        return Some(DelegationMergeFailureCategory::Transport);
    }
    Some(DelegationMergeFailureCategory::Unknown)
}

/// Loads the full child run tape using fixed-size pages.
async fn load_run_tape(
    runtime: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<Vec<crate::journal::OrchestratorTapeRecord>, Status> {
    let mut after_seq = None;
    let mut events = Vec::new();
    loop {
        let page =
            runtime.orchestrator_tape_snapshot(run_id.to_owned(), after_seq, Some(128)).await?;
        let next_after_seq = advance_tape_cursor(after_seq, page.next_after_seq, run_id)?;
        events.extend(page.events);
        let Some(next_after_seq) = next_after_seq else {
            break;
        };
        after_seq = Some(next_after_seq);
    }
    Ok(events)
}

fn advance_tape_cursor(
    previous_after_seq: Option<i64>,
    next_after_seq: Option<i64>,
    run_id: &str,
) -> Result<Option<i64>, Status> {
    if next_after_seq.is_some() && next_after_seq == previous_after_seq {
        return Err(Status::internal(format!(
            "orchestrator tape pagination cursor did not advance for run {run_id}"
        )));
    }
    Ok(next_after_seq)
}

fn build_merge_summary(
    strategy: DelegationMergeStrategy,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    model_output: &str,
    provenance: &[DelegationMergeProvenanceRecord],
    warnings: &[String],
) -> String {
    let base_summary = if model_output.is_empty() {
        format!("Child run {} completed with state '{}'.", run.run_id, run.state)
    } else {
        truncate_excerpt(model_output, 600)
    };
    match strategy {
        DelegationMergeStrategy::Summarize => base_summary,
        DelegationMergeStrategy::Compare => {
            format!("{} Sources captured: {}.", base_summary, provenance.len())
        }
        DelegationMergeStrategy::PatchReview => format!(
            "{} Patch-oriented evidence entries: {}.",
            base_summary,
            provenance
                .iter()
                .filter(|record| record.tool_name.as_deref() == Some("palyra.fs.apply_patch"))
                .count()
        ),
        DelegationMergeStrategy::Triage => {
            if warnings.is_empty() {
                format!("{} No merge warnings were raised.", base_summary)
            } else {
                format!("{} Warnings: {}.", base_summary, warnings.join(" | "))
            }
        }
    }
}

/// Emits the `child_run_spawned` parent-tape event plus the matching
/// `child_started` lifecycle event for a newly attached child run.
async fn append_parent_spawned_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: &str,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    let (delegated_run, graph_explain) = build_optional_delegated_run_context(
        task,
        parent_run_id,
        Some(child_run_id),
        DelegatedRunState::Running,
        DelegationMergeStatus::NotReady,
        "child_run_spawned",
        "child run spawned through bounded delegation executor",
    )?;
    append_parent_tape_event(
        runtime,
        parent_run_id,
        "child_run_spawned",
        json!({
            "task_id": task.task_id,
            "child_run_id": child_run_id,
            "session_id": task.session_id,
            "delegation": task.delegation,
            "delegated_run": delegated_run.clone(),
            "graph_explain": graph_explain.clone(),
        }),
    )
    .await?;
    append_child_lifecycle_event(
        runtime,
        task,
        Some(child_run_id),
        "child_started",
        "running",
        true,
        json!({
            "legacy_event_type": "child_run_spawned",
            "delegation": task.delegation,
            "delegated_run": delegated_run,
        }),
    )
    .await
}

/// Emits the merge outcome to the parent tape and the child lifecycle stream.
///
/// When delivery arbitration holds the output for final review, the payloads
/// carry only the hold reason; the merge result itself is withheld until
/// released (pinned by tests).
async fn append_parent_merge_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    merge_result: &DelegationMergeResult,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    let event_type = match run.state.as_str() {
        "done" => "child_run_merged",
        "failed" => "child_run_failed",
        "cancelled" => "child_run_cancelled",
        _ => "child_run_merged",
    };
    let (delivery_policy, delivery_decision) =
        evaluate_delivery_arbitration_for_merge(runtime, task, run, merge_result).await?;
    let hold_for_review = delivery_holds_merge_result(&delivery_decision);
    let merge_status = if hold_for_review {
        DelegationMergeStatus::ApprovalRequired
    } else {
        merge_status_for_result(run, merge_result)
    };
    let delegated_state = if hold_for_review {
        DelegatedRunState::WaitingForApproval
    } else if run.state == "done" {
        DelegatedRunState::Merged
    } else {
        DelegatedRunState::from_child_state(run.state.as_str())
    };
    let delegated_reason = if hold_for_review {
        "child output held for final review"
    } else {
        "child output merge preview produced for parent context"
    };
    let delegated_run = build_task_delegated_record(
        task,
        Some(run.run_id.as_str()),
        delegated_state,
        merge_status,
        event_type,
        delegated_reason,
    )?;
    let graph_explain =
        build_delegated_run_graph(parent_run_id.to_owned(), vec![delegated_run.clone()])
            .explain_json();
    let merge_preview = merge_preview_json(run, merge_result);
    let delegated_run_snapshot = delegated_run.safe_snapshot_json();
    let payload_context = MergeDeliveryPayloadContext {
        task_id: task.task_id.as_str(),
        child_run_id: run.run_id.as_str(),
        child_state: run.state.as_str(),
        legacy_event_type: event_type,
        merge_result,
        merge_preview: &merge_preview,
        delegated_run: &delegated_run_snapshot,
        graph_explain: &graph_explain,
        delivery_decision: &delivery_decision,
    };
    let parent_event_type = if hold_for_review { "child_run_delivery_held" } else { event_type };
    append_parent_tape_event(
        runtime,
        parent_run_id,
        parent_event_type,
        parent_merge_event_payload(&payload_context),
    )
    .await?;
    emit_delivery_arbitration_audit(runtime, task, run, &delivery_policy, &delivery_decision)
        .await?;
    let (child_event_type, child_state) = if hold_for_review {
        ("child_review_required", "waiting_for_approval")
    } else {
        match run.state.as_str() {
            "done" => ("child_completed", "completed"),
            "failed" => ("child_failed", "failed"),
            "cancelled" => ("child_failed", "cancelled"),
            other => ("child_completed", other),
        }
    };
    append_child_lifecycle_event(
        runtime,
        task,
        Some(run.run_id.as_str()),
        child_event_type,
        child_state,
        true,
        child_merge_lifecycle_details(&payload_context),
    )
    .await
}

fn delivery_holds_merge_result(decision: &DeliveryDecision) -> bool {
    matches!(decision.action, DeliveryDecisionAction::HoldForReview)
}

/// Shared inputs for building parent-tape and child-lifecycle merge payloads.
struct MergeDeliveryPayloadContext<'a> {
    task_id: &'a str,
    child_run_id: &'a str,
    child_state: &'a str,
    legacy_event_type: &'a str,
    merge_result: &'a DelegationMergeResult,
    merge_preview: &'a Value,
    delegated_run: &'a Value,
    graph_explain: &'a Value,
    delivery_decision: &'a DeliveryDecision,
}

fn parent_merge_event_payload(context: &MergeDeliveryPayloadContext<'_>) -> Value {
    if delivery_holds_merge_result(context.delivery_decision) {
        return json!({
            "task_id": context.task_id,
            "child_run_id": context.child_run_id,
            "child_state": context.child_state,
            "merge_held": true,
            "hold_reason": context.delivery_decision.reason.as_str(),
            "delegated_run": context.delegated_run,
            "graph_explain": context.graph_explain,
            "delivery_review": delivery_review_summary(&context.merge_result.approval_summary),
            "delivery_arbitration": context.delivery_decision.explain_json.clone(),
        });
    }

    json!({
        "task_id": context.task_id,
        "child_run_id": context.child_run_id,
        "child_state": context.child_state,
        "merge_result": context.merge_result,
        "merge_preview": context.merge_preview,
        "delegated_run": context.delegated_run,
        "graph_explain": context.graph_explain,
        "delivery_review": delivery_review_summary(&context.merge_result.approval_summary),
        "delivery_arbitration": context.delivery_decision.explain_json.clone(),
    })
}

fn child_merge_lifecycle_details(context: &MergeDeliveryPayloadContext<'_>) -> Value {
    if delivery_holds_merge_result(context.delivery_decision) {
        return json!({
            "legacy_event_type": context.legacy_event_type,
            "merge_held": true,
            "hold_reason": context.delivery_decision.reason.as_str(),
            "delegated_run": context.delegated_run,
            "delivery_review": delivery_review_summary(&context.merge_result.approval_summary),
            "delivery_arbitration": context.delivery_decision.explain_json.clone(),
        });
    }

    json!({
        "legacy_event_type": context.legacy_event_type,
        "merge_result": context.merge_result,
        "merge_preview": context.merge_preview,
        "delegated_run": context.delegated_run,
        "delivery_review": delivery_review_summary(&context.merge_result.approval_summary),
        "delivery_arbitration": context.delivery_decision.explain_json.clone(),
    })
}

async fn evaluate_delivery_arbitration_for_merge(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    merge_result: &DelegationMergeResult,
) -> Result<(DeliveryPolicySet, DeliveryDecision), Status> {
    let parent_state = if let Some(parent_run_id) = task.parent_run_id.as_deref() {
        runtime
            .orchestrator_run_status_snapshot(parent_run_id.to_owned())
            .await?
            .map(|snapshot| snapshot.state)
    } else {
        None
    };
    let policy = resolve_delivery_policy(
        &runtime.config.delivery_arbitration,
        task.delegation.as_ref(),
        None,
        task.channel.as_deref(),
    );
    let approval_summary = &merge_result.approval_summary;
    let decision = arbitrate_delivery(DeliveryDecisionInput {
        policy: &policy,
        parent_run_id: task.parent_run_id.as_deref(),
        parent_state: parent_state.as_deref(),
        descendant_run_id: Some(run.run_id.as_str()),
        descendant_state: run.state.as_str(),
        approval_required: merge_result.approval_required || approval_summary.approval_required,
        approval_events: approval_summary.approval_events,
        approval_pending: approval_summary.approval_pending,
        approval_denied: approval_summary.approval_denied,
        observed_at_unix_ms: crate::gateway::current_unix_ms(),
    });
    Ok((policy, decision))
}

/// Merge status ladder: failure first, then denied/pending approval, then
/// merged vs preview depending on the child's terminal state.
fn merge_status_for_result(
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    merge_result: &DelegationMergeResult,
) -> DelegationMergeStatus {
    if run.state == "failed" {
        return DelegationMergeStatus::Failed;
    }
    if merge_result.approval_summary.approval_denied {
        return DelegationMergeStatus::Rejected;
    }
    if merge_result.approval_summary.approval_pending || merge_result.approval_required {
        return DelegationMergeStatus::ApprovalRequired;
    }
    if run.state == "done" {
        DelegationMergeStatus::Merged
    } else {
        DelegationMergeStatus::PreviewReady
    }
}

fn merge_preview_json(
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    merge_result: &DelegationMergeResult,
) -> Value {
    json!({
        "child_run_id": run.run_id,
        "child_state": run.state,
        "summary": truncate_excerpt(merge_result.summary_text.as_str(), 600),
        "evidence_refs": merge_result
            .provenance
            .iter()
            .map(|record| json!({
                "kind": record.kind,
                "label": truncate_excerpt(record.label.as_str(), 160),
                "child_run_id": record.child_run_id,
                "requires_approval": record.requires_approval,
            }))
            .collect::<Vec<_>>(),
        "changed_artifacts": merge_result
            .artifact_references
            .iter()
            .map(|artifact| json!({
                "artifact_id": artifact.artifact_id,
                "artifact_kind": artifact.artifact_kind,
                "label": truncate_excerpt(artifact.label.as_str(), 160),
            }))
            .collect::<Vec<_>>(),
        "sensitivity": if merge_result.approval_required { "review_required" } else { "internal" },
        "approval_required": merge_result.approval_required,
        "warnings": merge_result.warnings,
    })
}

/// Records the delivery arbitration decision as a parent tape event and a
/// structured runtime decision; no-op while the capability is inactive.
async fn emit_delivery_arbitration_audit(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    run: &crate::journal::OrchestratorRunStatusSnapshot,
    policy: &DeliveryPolicySet,
    decision: &DeliveryDecision,
) -> Result<(), Status> {
    if !crate::runtime_preview_controls::capability_active(
        &runtime.config,
        RuntimePreviewCapability::DeliveryArbitration,
    ) {
        return Ok(());
    }
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    append_parent_tape_event(
        runtime,
        parent_run_id,
        "delivery.arbitrated",
        decision.explain_json.clone(),
    )
    .await?;

    let payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::DeliveryArbitration,
        RuntimeDecisionActor::new(
            RuntimeDecisionActorKind::System,
            task.owner_principal.clone(),
            task.device_id.clone(),
            task.channel.clone(),
        ),
        decision.reason.clone(),
        DELIVERY_ARBITRATION_POLICY_ID,
        RuntimeDecisionTiming::observed(crate::gateway::current_unix_ms()),
    )
    .with_input(
        RuntimeEntityRef::new("candidate_parent", "orchestrator_run", parent_run_id.to_owned())
            .with_state(
                decision.explain_json["parent_output"]["state"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_owned(),
            ),
    )
    .with_output(
        RuntimeEntityRef::new("preferred_descendant", "orchestrator_run", run.run_id.clone())
            .with_state(run.state.clone()),
    )
    .with_resource_budget(RuntimeResourceBudget {
        queue_depth: None,
        token_budget: None,
        pruning_token_delta: None,
        retrieval_branch_latency_ms: None,
        retry_count: None,
        suppression_count: Some(decision.suppression_count()),
    })
    .with_related_entity(RuntimeEntityRef::new(
        "background_task",
        "orchestrator_background_task",
        task.task_id.clone(),
    ))
    .with_details(json!({
        "policy": policy.snapshot_json(),
        "decision": decision.explain_json.clone(),
    }));

    runtime
        .record_system_runtime_decision_event(
            task.owner_principal.as_str(),
            task.device_id.as_str(),
            task.channel.as_deref(),
            Some(task.session_id.as_str()),
            Some(parent_run_id),
            payload,
        )
        .await
}

/// Appends a normalized child lifecycle event to the parent run tape; no-op
/// for tasks without a parent run.
async fn append_child_lifecycle_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: Option<&str>,
    event_type: &str,
    child_state: &str,
    user_visible: bool,
    details: Value,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    let (delegated_run, graph_explain) = build_optional_delegated_run_context(
        task,
        parent_run_id,
        child_run_id,
        DelegatedRunState::from_child_state(child_state),
        DelegationMergeStatus::NotReady,
        event_type,
        child_state,
    )?;
    append_parent_tape_event(
        runtime,
        parent_run_id,
        event_type,
        json!({
            "task_id": task.task_id,
            "child_run_id": child_run_id,
            "session_id": task.session_id,
            "child_state": child_state,
            "user_visible": user_visible,
            "delegation": task.delegation,
            "delegated_run": delegated_run,
            "graph_explain": graph_explain,
            "observed_at_unix_ms": crate::gateway::current_unix_ms(),
            "details": details,
        }),
    )
    .await
}

/// Builds `(delegated_run, graph_explain)` payload snippets for delegated
/// tasks; `(None, None)` for plain background tasks.
fn build_optional_delegated_run_context(
    task: &OrchestratorBackgroundTaskRecord,
    parent_run_id: &str,
    child_run_id: Option<&str>,
    state: DelegatedRunState,
    merge_status: DelegationMergeStatus,
    event_type: &str,
    reason: &str,
) -> Result<(Option<Value>, Option<Value>), Status> {
    if task.delegation.is_none() {
        return Ok((None, None));
    }
    let delegated_run =
        build_task_delegated_record(task, child_run_id, state, merge_status, event_type, reason)?;
    let graph_explain =
        build_delegated_run_graph(parent_run_id.to_owned(), vec![delegated_run.clone()])
            .explain_json();
    Ok((Some(delegated_run.safe_snapshot_json()), Some(graph_explain)))
}

/// Appends to the parent tape with optimistic sequencing.
///
/// `seq` is the run snapshot's current event count; `AlreadyExists` means a
/// concurrent writer claimed it, so the snapshot is re-read (3 attempts).
/// Events are dropped, not errored, when the parent run is missing or still
/// active.
async fn append_parent_tape_event(
    runtime: &Arc<GatewayRuntimeState>,
    parent_run_id: &str,
    event_type: &str,
    payload: Value,
) -> Result<(), Status> {
    for _ in 0..3 {
        let Some(run) = runtime.orchestrator_run_status_snapshot(parent_run_id.to_owned()).await?
        else {
            return Ok(());
        };
        if !parent_tape_accepts_background_event(run.state.as_str()) {
            warn!(
                parent_run_id,
                state = %run.state,
                event_type,
                "skipping background parent tape append while parent run is active"
            );
            return Ok(());
        }
        let seq = i64::try_from(run.tape_events).unwrap_or(i64::MAX);
        match runtime
            .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
                run_id: parent_run_id.to_owned(),
                seq,
                event_type: event_type.to_owned(),
                payload_json: payload.to_string(),
            })
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) if error.code() == Code::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Status::aborted(format!("failed to append parent tape event '{event_type}' after retries")))
}

/// Background events may only land on a parent tape after the parent run is
/// terminal; appending earlier would race the orchestrator's own writes for
/// the same sequence numbers (pinned by tests).
fn parent_tape_accepts_background_event(parent_run_state: &str) -> bool {
    is_terminal_run_state(parent_run_state)
}

fn value_excerpt(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

/// Truncates to `max_chars` characters, appending `...` when trimmed (the
/// result may therefore exceed `max_chars` by the ellipsis).
fn truncate_excerpt(value: &str, max_chars: usize) -> String {
    let mut excerpt = value.trim().chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        excerpt.push_str("...");
    }
    excerpt
}

fn delivery_progress_detail(details: &Value) -> Option<String> {
    if let Some(message) = details.get("message").and_then(Value::as_str) {
        return Some(truncate_excerpt(message, 180));
    }
    if let Some(state) = details.get("state").and_then(Value::as_str) {
        return Some(truncate_excerpt(state, 180));
    }
    let excerpt = truncate_excerpt(value_excerpt(details).as_str(), 180);
    (!excerpt.is_empty()).then_some(excerpt)
}

fn is_terminal_child_progress(event_type: &str, child_state: &str) -> bool {
    event_type == "child_completed"
        || event_type == "child_failed"
        || matches!(child_state, "completed" | "failed" | "cancelled" | "canceled")
}

fn trim_delivery_progress_history(updates: &mut Vec<DeliveryProgressUpdate>) {
    if updates.len() <= CHILD_PROGRESS_HISTORY_LIMIT {
        return;
    }
    let remove_count = updates.len().saturating_sub(CHILD_PROGRESS_HISTORY_LIMIT);
    updates.drain(0..remove_count);
}

fn attach_delivery_progress_details(
    details: Value,
    policy: &DeliveryPolicySet,
    merged_progress: &MergedDeliveryProgress,
) -> Value {
    let delivery = json!({
        "policy": policy.snapshot_json(),
        "progress": merged_progress.snapshot_json(),
    });
    match details {
        Value::Object(mut object) => {
            object.insert("delivery".to_owned(), delivery);
            Value::Object(object)
        }
        Value::Null => json!({ "delivery": delivery }),
        other => json!({
            "raw_details": other,
            "delivery": delivery,
        }),
    }
}

/// Injects task identity (and admin bearer auth when required) into outgoing
/// gateway metadata. Blank channels are omitted entirely so the receiver
/// resolves `RequestContext.channel` to `None` (pinned by tests).
fn inject_background_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    auth: &GatewayAuthConfig,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
) -> Result<(), Status> {
    if auth.require_auth {
        let token = auth.admin_token.as_ref().ok_or_else(|| {
            Status::permission_denied("admin token is required for background queue auth")
        })?;
        metadata.insert(
            "authorization",
            format!("Bearer {token}").parse().map_err(|_| {
                Status::internal("failed to encode background queue authorization metadata")
            })?,
        );
    }
    metadata.insert(
        HEADER_PRINCIPAL,
        principal
            .parse()
            .map_err(|_| Status::invalid_argument("background principal metadata is invalid"))?,
    );
    metadata.insert(
        HEADER_DEVICE_ID,
        device_id
            .parse()
            .map_err(|_| Status::invalid_argument("background device_id metadata is invalid"))?,
    );
    if let Some(header_channel) = channel.filter(|value| !value.trim().is_empty()) {
        metadata.insert(
            HEADER_CHANNEL,
            header_channel
                .parse()
                .map_err(|_| Status::invalid_argument("background channel metadata is invalid"))?,
        );
    }
    Ok(())
}

fn run_status_to_json(run: &crate::journal::OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "run_id": run.run_id,
        "session_id": run.session_id,
        "state": run.state,
        "cancel_requested": run.cancel_requested,
        "cancel_reason": run.cancel_reason,
        "prompt_tokens": run.prompt_tokens,
        "completion_tokens": run.completion_tokens,
        "total_tokens": run.total_tokens,
        "origin_kind": run.origin_kind,
        "origin_run_id": run.origin_run_id,
        "parent_run_id": run.parent_run_id,
        "delegation": run.delegation,
        "merge_result": run.merge_result,
        "updated_at_unix_ms": run.updated_at_unix_ms,
        "completed_at_unix_ms": run.completed_at_unix_ms,
        "last_error": run.last_error,
    })
}

/// Terminal background-task states per `AuxiliaryTaskState::is_terminal`.
fn is_terminal_task_state(state: &str) -> bool {
    AuxiliaryTaskState::from_str(state).is_some_and(AuxiliaryTaskState::is_terminal)
}

/// Terminal orchestrator run states; the string values are pinned by the
/// journal layer.
fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

#[cfg(test)]
mod tests {
    use super::{
        advance_tape_cursor, append_artifact_references,
        build_background_task_child_run_attach_update, build_background_task_running_update,
        build_optional_delegated_run_context, build_parameter_delta_bytes,
        categorize_child_failure, child_merge_lifecycle_details, delegated_child_timeout_message,
        evaluate_delegation_scheduler_limits, inject_background_metadata,
        parent_merge_event_payload, parent_tape_accepts_background_event,
        pending_child_cancel_reason, replace_background_task_snapshot,
        running_delegated_children_for_parent, running_task_should_wait_for_in_flight_work,
        should_emit_child_stream_progress, task_has_in_flight_work_without_target,
        ChildLifecycleTapeBudget, ChildLifecycleTapeDecision, ChildStreamProgress,
        DelegationSchedulerDecision, MergeDeliveryPayloadContext,
    };
    use crate::{
        application::delivery_arbitration::{DeliveryDecision, DeliveryDecisionAction},
        delegation::{
            DelegatedRunState, DelegationExecutionMode, DelegationMemoryScopeKind,
            DelegationMergeApprovalSummary, DelegationMergeContract,
            DelegationMergeFailureCategory, DelegationMergeResult, DelegationMergeStatus,
            DelegationMergeStrategy, DelegationMergeUsageSummary, DelegationRole,
            DelegationRuntimeLimits, DelegationSnapshot,
        },
        gateway::{GatewayAuthConfig, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL},
        journal::{OrchestratorBackgroundTaskRecord, OrchestratorRunStatusSnapshot},
    };
    use palyra_common::runtime_contracts::AuxiliaryTaskState;
    use serde_json::{json, Value};
    use tonic::{metadata::MetadataMap, Code};

    fn unauthenticated_gateway_auth() -> GatewayAuthConfig {
        GatewayAuthConfig {
            require_auth: false,
            admin_token: None,
            connector_token: None,
            bound_principal: None,
        }
    }

    #[test]
    fn tape_cursor_rejects_non_advancing_pagination() {
        assert_eq!(
            advance_tape_cursor(None, Some(128), "run-1").expect("cursor should advance"),
            Some(128)
        );
        assert_eq!(
            advance_tape_cursor(Some(128), None, "run-1").expect("missing cursor ends pagination"),
            None
        );
        let error = advance_tape_cursor(Some(128), Some(128), "run-1")
            .expect_err("repeated cursor must fail instead of looping");
        assert_eq!(error.code(), Code::Internal);
        assert!(error.message().contains("did not advance"));
    }

    #[test]
    fn background_metadata_preserves_missing_channel_identity() {
        let mut metadata = MetadataMap::new();

        inject_background_metadata(
            &mut metadata,
            &unauthenticated_gateway_auth(),
            "operator",
            "01J8ZK6M36QK7V8E7C38QMDVZS",
            None,
        )
        .expect("metadata should encode");

        assert_eq!(
            metadata.get(HEADER_PRINCIPAL).and_then(|value| value.to_str().ok()),
            Some("operator")
        );
        assert_eq!(
            metadata.get(HEADER_DEVICE_ID).and_then(|value| value.to_str().ok()),
            Some("01J8ZK6M36QK7V8E7C38QMDVZS")
        );
        assert!(
            !metadata.contains_key(HEADER_CHANNEL),
            "background tasks created without a channel must resolve to RequestContext.channel=None"
        );
    }

    #[test]
    fn background_metadata_omits_blank_channel_identity() {
        let mut metadata = MetadataMap::new();

        inject_background_metadata(
            &mut metadata,
            &unauthenticated_gateway_auth(),
            "operator",
            "01J8ZK6M36QK7V8E7C38QMDVZS",
            Some("   "),
        )
        .expect("metadata should encode");

        assert!(
            !metadata.contains_key(HEADER_CHANNEL),
            "blank channel input should be treated like a missing channel"
        );
    }

    #[test]
    fn background_metadata_preserves_explicit_channel_identity() {
        let mut metadata = MetadataMap::new();

        inject_background_metadata(
            &mut metadata,
            &unauthenticated_gateway_auth(),
            "operator",
            "01J8ZK6M36QK7V8E7C38QMDVZS",
            Some("discord:channel:engineering"),
        )
        .expect("metadata should encode");

        assert_eq!(
            metadata.get(HEADER_CHANNEL).and_then(|value| value.to_str().ok()),
            Some("discord:channel:engineering")
        );
    }

    #[test]
    fn user_visible_child_progress_is_throttled_on_parent_tape() {
        let progress = ChildStreamProgress {
            event_type: "child_progress",
            child_state: "model_streaming".to_owned(),
            user_visible: true,
            details: json!({ "stream_event": "model_token" }),
        };

        assert!(
            !should_emit_child_stream_progress(&progress, 1_001, 1_000),
            "user-visible model tokens should not bypass the parent tape throttle"
        );
        assert!(
            should_emit_child_stream_progress(&progress, 3_000, 1_000),
            "parent tape progress should still be emitted after the throttle interval"
        );

        let completed = ChildStreamProgress {
            event_type: "child_completed",
            child_state: "completed".to_owned(),
            user_visible: true,
            details: json!({ "stream_event": "status" }),
        };
        assert!(
            should_emit_child_stream_progress(&completed, 1_001, 1_000),
            "terminal child lifecycle events must not be throttled"
        );
    }

    #[test]
    fn background_parent_tape_events_wait_for_terminal_parent_run() {
        for state in ["accepted", "in_progress"] {
            assert!(
                !parent_tape_accepts_background_event(state),
                "background lifecycle events must not race an active parent tape state={state}"
            );
        }
        for state in ["done", "failed", "cancelled"] {
            assert!(
                parent_tape_accepts_background_event(state),
                "terminal parent runs can accept post-run background lifecycle events state={state}"
            );
        }
    }

    #[test]
    fn child_lifecycle_tape_budget_caps_progress_and_heartbeats() {
        let mut budget = ChildLifecycleTapeBudget::with_limits(2, 1);
        let progress = ChildStreamProgress {
            event_type: "child_progress",
            child_state: "model_streaming".to_owned(),
            user_visible: true,
            details: json!({ "stream_event": "model_token" }),
        };

        assert!(matches!(
            budget.record_stream_event(&progress, &progress.details),
            ChildLifecycleTapeDecision::Emit
        ));
        assert!(matches!(
            budget.record_stream_event(&progress, &progress.details),
            ChildLifecycleTapeDecision::Emit
        ));
        match budget.record_stream_event(&progress, &progress.details) {
            ChildLifecycleTapeDecision::EmitLimitNotice { event_type, details } => {
                assert_eq!(event_type, "child_progress_compacted");
                assert_eq!(
                    details.get("reason").and_then(serde_json::Value::as_str),
                    Some("parent_tape_child_progress_limit")
                );
                assert_eq!(
                    details.get("suppressed_event_type").and_then(serde_json::Value::as_str),
                    Some("child_progress")
                );
            }
            other => panic!("expected progress limit notice, got {other:?}"),
        }
        assert!(matches!(
            budget.record_stream_event(&progress, &progress.details),
            ChildLifecycleTapeDecision::Suppress
        ));

        let completed = ChildStreamProgress {
            event_type: "child_completed",
            child_state: "completed".to_owned(),
            user_visible: true,
            details: json!({ "stream_event": "status" }),
        };
        assert!(matches!(
            budget.record_stream_event(&completed, &completed.details),
            ChildLifecycleTapeDecision::Emit
        ));

        assert!(matches!(budget.record_scheduled_heartbeat(), ChildLifecycleTapeDecision::Emit));
        match budget.record_scheduled_heartbeat() {
            ChildLifecycleTapeDecision::EmitLimitNotice { event_type, details } => {
                assert_eq!(event_type, "child_heartbeat_compacted");
                assert_eq!(
                    details.get("reason").and_then(serde_json::Value::as_str),
                    Some("parent_tape_child_heartbeat_limit")
                );
            }
            other => panic!("expected heartbeat limit notice, got {other:?}"),
        }
        assert!(matches!(
            budget.record_scheduled_heartbeat(),
            ChildLifecycleTapeDecision::Suppress
        ));
    }

    #[test]
    fn non_delegated_background_tasks_do_not_require_delegation_records() {
        let mut task = sample_task(
            "task-1",
            AuxiliaryTaskState::Running.as_str(),
            100,
            "group-a",
            DelegationRuntimeLimits::default(),
        );
        task.task_kind = "background_prompt".to_owned();
        task.delegation = None;

        let (delegated_run, graph_explain) = build_optional_delegated_run_context(
            &task,
            "parent-run",
            Some("child-run"),
            DelegatedRunState::Running,
            DelegationMergeStatus::NotReady,
            "child_run_spawned",
            "child run spawned",
        )
        .expect("plain background prompts should not require delegation metadata");

        assert!(delegated_run.is_none());
        assert!(graph_explain.is_none());
    }

    #[test]
    fn background_run_parameter_delta_includes_task_budget() {
        let task = sample_task(
            "task-budget",
            AuxiliaryTaskState::Queued.as_str(),
            100,
            "group-a",
            DelegationRuntimeLimits {
                child_budget_override: Some(1_234),
                ..DelegationRuntimeLimits::default()
            },
        );

        let bytes =
            build_parameter_delta_bytes(&task).expect("background parameter delta should encode");
        let parsed: Value =
            serde_json::from_slice(bytes.as_slice()).expect("parameter delta should be JSON");

        assert_eq!(
            parsed.pointer("/background_task/budget_tokens").and_then(Value::as_u64),
            Some(1_234)
        );
    }

    #[test]
    fn append_artifact_references_extracts_nested_artifacts() {
        let mut references = Vec::new();
        append_artifact_references(
            &mut references,
            &json!({
                "artifacts": [
                    { "artifact_id": "artifact-1", "kind": "patch", "label": "Patch report" },
                    { "artifact_id": "artifact-1", "kind": "patch", "label": "Duplicate" },
                    { "artifact_id": "artifact-2", "artifact_kind": "log", "path": "logs/run.txt" }
                ]
            }),
            "child-run",
        );

        assert_eq!(references.len(), 2);
        assert_eq!(references[0].artifact_kind, "patch");
        assert_eq!(references[1].label, "logs/run.txt");
    }

    #[test]
    fn categorize_child_failure_prefers_runtime_and_approval_categories() {
        let mut run = sample_run("cancelled", None);
        assert_eq!(
            categorize_child_failure(&run, &[], &[], &DelegationMergeApprovalSummary::default()),
            Some(DelegationMergeFailureCategory::Cancellation)
        );

        run = sample_run("failed", Some("usage budget exhausted"));
        assert_eq!(
            categorize_child_failure(&run, &[], &[], &DelegationMergeApprovalSummary::default()),
            Some(DelegationMergeFailureCategory::Budget)
        );

        let approval = DelegationMergeApprovalSummary {
            approval_required: true,
            approval_events: 1,
            approval_pending: false,
            approval_denied: true,
        };
        run = sample_run("failed", Some("tool denied"));
        assert_eq!(
            categorize_child_failure(&run, &[], &[], &approval),
            Some(DelegationMergeFailureCategory::Approval)
        );
    }

    #[test]
    fn hold_for_review_payloads_withhold_merge_result() {
        let merge_result = sample_merge_result(true);
        let decision = DeliveryDecision {
            action: DeliveryDecisionAction::HoldForReview,
            reason: "final_review_required".to_owned(),
            parent_superseded: false,
            parent_suppressed: false,
            would_suppress_parent: false,
            descendant_preferred: false,
            review_required: true,
            approval_pending: true,
            audit_retained: true,
            explain_json: json!({
                "action": "hold_for_review",
                "reason": "final_review_required"
            }),
        };

        let merge_preview = json!({ "summary": "unreviewed child output" });
        let delegated_run = json!({ "state": "waiting_for_approval" });
        let graph_explain = json!({ "nodes": [] });
        let context = MergeDeliveryPayloadContext {
            task_id: "task-1",
            child_run_id: "child-run",
            child_state: "done",
            legacy_event_type: "child_run_merged",
            merge_result: &merge_result,
            merge_preview: &merge_preview,
            delegated_run: &delegated_run,
            graph_explain: &graph_explain,
            delivery_decision: &decision,
        };

        let parent_payload = parent_merge_event_payload(&context);
        assert_eq!(parent_payload.get("merge_held").and_then(|value| value.as_bool()), Some(true));
        assert_eq!(
            parent_payload.get("hold_reason").and_then(|value| value.as_str()),
            Some("final_review_required")
        );
        assert!(parent_payload.get("merge_result").is_none());
        assert!(parent_payload.get("merge_preview").is_none());

        let child_details = child_merge_lifecycle_details(&context);
        assert_eq!(child_details.get("merge_held").and_then(|value| value.as_bool()), Some(true));
        assert!(child_details.get("merge_result").is_none());
        assert!(child_details.get("merge_preview").is_none());
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_defers_for_concurrency() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let running = sample_task(
            "task-running",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        let queued =
            sample_task("task-queued", AuxiliaryTaskState::Queued.as_str(), 20, "group-b", limits);

        let decision = evaluate_delegation_scheduler_limits(&[running, queued.clone()], &queued)
            .expect("queued child should be deferred");
        match decision {
            DelegationSchedulerDecision::Defer { reason, .. } => {
                assert_eq!(reason, "max_concurrent_children");
            }
            DelegationSchedulerDecision::Fail { .. } => {
                panic!("concurrency pressure should defer, not fail");
            }
        }
    }

    #[test]
    fn refreshed_poll_snapshot_defers_later_siblings() {
        let concurrency_limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let first = sample_task(
            "task-first",
            AuxiliaryTaskState::Queued.as_str(),
            10,
            "group-a",
            concurrency_limits.clone(),
        );
        let second = sample_task(
            "task-second",
            AuxiliaryTaskState::Queued.as_str(),
            20,
            "group-b",
            concurrency_limits,
        );
        let mut tasks = vec![first.clone(), second.clone()];

        assert!(
            evaluate_delegation_scheduler_limits(tasks.as_slice(), &first).is_none(),
            "first queued child should be dispatchable before the local snapshot is refreshed"
        );
        let mut running_first = first;
        running_first.state = AuxiliaryTaskState::Running.as_str().to_owned();
        running_first.target_run_id = Some("run-task-first".to_owned());
        running_first.started_at_unix_ms = Some(30);
        replace_background_task_snapshot(tasks.as_mut_slice(), running_first);

        let decision = evaluate_delegation_scheduler_limits(tasks.as_slice(), &second)
            .expect("refreshed snapshot should make the second child wait");
        match decision {
            DelegationSchedulerDecision::Defer { reason, .. } => {
                assert_eq!(reason, "max_concurrent_children");
            }
            DelegationSchedulerDecision::Fail { .. } => {
                panic!("concurrency pressure should defer, not fail");
            }
        }

        let parallel_limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 1,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let first = sample_task(
            "task-parallel-first",
            AuxiliaryTaskState::Queued.as_str(),
            10,
            "group-a",
            parallel_limits.clone(),
        );
        let second = sample_task(
            "task-parallel-second",
            AuxiliaryTaskState::Queued.as_str(),
            20,
            "group-b",
            parallel_limits,
        );
        let mut tasks = vec![first.clone(), second.clone()];
        let mut running_first = first;
        running_first.state = AuxiliaryTaskState::Running.as_str().to_owned();
        running_first.target_run_id = Some("run-task-parallel-first".to_owned());
        running_first.started_at_unix_ms = Some(30);
        replace_background_task_snapshot(tasks.as_mut_slice(), running_first);

        let decision = evaluate_delegation_scheduler_limits(tasks.as_slice(), &second)
            .expect("refreshed snapshot should enforce parallel group limits");
        match decision {
            DelegationSchedulerDecision::Defer { reason, .. } => {
                assert_eq!(reason, "max_parallel_groups");
            }
            DelegationSchedulerDecision::Fail { .. } => {
                panic!("parallel group pressure should defer, not fail");
            }
        }
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_counts_attach_pending_child_for_concurrency() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut attach_pending = sample_task(
            "task-attach-pending",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        attach_pending.target_run_id = None;
        let queued =
            sample_task("task-queued", AuxiliaryTaskState::Queued.as_str(), 20, "group-b", limits);

        let active_tasks = vec![attach_pending.clone()];
        let running_children =
            running_delegated_children_for_parent(active_tasks.as_slice(), "parent-run")
                .collect::<Vec<_>>();
        assert_eq!(running_children.len(), 1, "attach-pending child must count as active");

        let decision =
            evaluate_delegation_scheduler_limits(&[attach_pending, queued.clone()], &queued)
                .expect("queued child should be deferred while earlier child is attaching");
        match decision {
            DelegationSchedulerDecision::Defer { reason, .. } => {
                assert_eq!(reason, "max_concurrent_children");
            }
            DelegationSchedulerDecision::Fail { .. } => {
                panic!("attach-pending concurrency pressure should defer, not fail");
            }
        }
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_fails_child_overflow() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 1,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let older = sample_task(
            "task-older",
            AuxiliaryTaskState::Queued.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        let current =
            sample_task("task-current", AuxiliaryTaskState::Queued.as_str(), 20, "group-b", limits);

        let decision = evaluate_delegation_scheduler_limits(&[older, current.clone()], &current)
            .expect("overflow child should fail closed");
        match decision {
            DelegationSchedulerDecision::Fail { reason, .. } => {
                assert_eq!(reason, "max_children_per_parent");
            }
            DelegationSchedulerDecision::Defer { .. } => {
                panic!("child overflow should fail closed");
            }
        }
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_fails_depth_overflow() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 1,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut parent_child = sample_task(
            "task-parent-child",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        parent_child.parent_run_id = Some("root-run".to_owned());
        parent_child.target_run_id = Some("parent-run".to_owned());
        let current =
            sample_task("task-nested", AuxiliaryTaskState::Queued.as_str(), 20, "group-b", limits);

        let decision =
            evaluate_delegation_scheduler_limits(&[parent_child, current.clone()], &current)
                .expect("nested child should fail");
        match decision {
            DelegationSchedulerDecision::Fail { reason, .. } => assert_eq!(reason, "max_depth"),
            DelegationSchedulerDecision::Defer { .. } => {
                panic!("depth overflow should fail closed");
            }
        }
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_fails_total_child_overflow() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 8,
            max_total_children: 1,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let older = sample_task(
            "task-older",
            AuxiliaryTaskState::Queued.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        let current =
            sample_task("task-current", AuxiliaryTaskState::Queued.as_str(), 20, "group-b", limits);

        let decision = evaluate_delegation_scheduler_limits(&[older, current.clone()], &current)
            .expect("total child overflow should fail");
        match decision {
            DelegationSchedulerDecision::Fail { reason, .. } => {
                assert_eq!(reason, "max_total_children");
            }
            DelegationSchedulerDecision::Defer { .. } => {
                panic!("total child overflow should fail closed");
            }
        }
    }

    #[test]
    fn evaluate_delegation_scheduler_limits_rejects_lineage_cycle() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 2,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut first = sample_task(
            "task-first",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        first.parent_run_id = Some("run-second".to_owned());
        first.target_run_id = Some("parent-run".to_owned());
        let mut second = sample_task(
            "task-second",
            AuxiliaryTaskState::Running.as_str(),
            11,
            "group-b",
            limits.clone(),
        );
        second.parent_run_id = Some("parent-run".to_owned());
        second.target_run_id = Some("run-second".to_owned());
        let current =
            sample_task("task-current", AuxiliaryTaskState::Queued.as_str(), 20, "group-c", limits);

        let decision =
            evaluate_delegation_scheduler_limits(&[first, second, current.clone()], &current)
                .expect("cyclic lineage should fail");
        match decision {
            DelegationSchedulerDecision::Fail { reason, .. } => {
                assert_eq!(reason, "delegation_cycle");
            }
            DelegationSchedulerDecision::Defer { .. } => {
                panic!("lineage cycle should fail closed");
            }
        }
    }

    #[test]
    fn delegated_child_timeout_message_uses_runtime_limit() {
        let mut task = sample_task(
            "task-timeout",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 8,
                max_total_children: 16,
                max_parallel_groups: 1,
                max_depth: 3,
                max_budget_share_bps: 10_000,
                child_budget_override: None,
                child_timeout_ms: 25,
            },
        );
        task.started_at_unix_ms = Some(100);

        assert!(delegated_child_timeout_message(&task, 124).is_none());
        assert!(delegated_child_timeout_message(&task, 125)
            .expect("task should time out at the limit")
            .contains("limit 25 ms"));
    }

    #[test]
    fn running_update_does_not_attach_child_run_before_it_exists() {
        let update = build_background_task_running_update("task-1", 1234);

        assert_eq!(update.task_id, "task-1");
        assert_eq!(update.state.as_deref(), Some(AuxiliaryTaskState::Running.as_str()));
        assert_eq!(update.target_run_id, Some(None));
        assert!(update.increment_attempt_count);
        assert_eq!(update.result_json, Some(None));
        assert_eq!(update.started_at_unix_ms, Some(Some(1234)));
        assert_eq!(update.completed_at_unix_ms, Some(None));
    }

    #[test]
    fn in_flight_work_without_target_requires_started_running_or_cancel_requested_task() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 1,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut running = sample_task(
            "task-running",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits.clone(),
        );
        running.target_run_id = None;
        assert!(task_has_in_flight_work_without_target(&running));

        let mut not_started = running.clone();
        not_started.started_at_unix_ms = None;
        assert!(!task_has_in_flight_work_without_target(&not_started));

        let mut cancel_requested = running.clone();
        cancel_requested.state = AuxiliaryTaskState::CancelRequested.as_str().to_owned();
        assert!(task_has_in_flight_work_without_target(&cancel_requested));

        let mut queued = running;
        queued.state = AuxiliaryTaskState::Queued.as_str().to_owned();
        assert!(!task_has_in_flight_work_without_target(&queued));
    }

    #[test]
    fn cancel_requested_attached_task_requires_child_cancel_delivery() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 1,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut task = sample_task(
            "task-cancel",
            AuxiliaryTaskState::CancelRequested.as_str(),
            10,
            "group-a",
            limits,
        );
        task.target_run_id = Some("run-1".to_owned());

        assert_eq!(pending_child_cancel_reason(&task), Some("background_task_cancel_requested"));

        task.state = AuxiliaryTaskState::Running.as_str().to_owned();
        assert_eq!(pending_child_cancel_reason(&task), None);
    }

    #[test]
    fn running_auxiliary_task_without_target_waits_for_in_flight_work() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_total_children: 16,
            max_parallel_groups: 1,
            max_depth: 3,
            max_budget_share_bps: 10_000,
            child_budget_override: None,
            child_timeout_ms: 60_000,
        };
        let mut task = sample_task(
            "task-aux-summary",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            limits,
        );
        task.task_kind = "summary".to_owned();
        task.target_run_id = None;
        task.started_at_unix_ms = Some(123);

        assert!(
            running_task_should_wait_for_in_flight_work(&task),
            "running auxiliary tasks without target_run_id are provider work in flight"
        );
    }

    #[test]
    fn child_run_attach_update_sets_target_after_run_exists() {
        let update = build_background_task_child_run_attach_update("task-1", "run-1");

        assert_eq!(update.task_id, "task-1");
        assert_eq!(update.state, None);
        assert_eq!(update.target_run_id, Some(Some("run-1".to_owned())));
        assert!(!update.increment_attempt_count);
        assert_eq!(update.result_json, None);
    }

    fn sample_run(state: &str, last_error: Option<&str>) -> OrchestratorRunStatusSnapshot {
        OrchestratorRunStatusSnapshot {
            run_id: "child-run".to_owned(),
            session_id: "session".to_owned(),
            state: state.to_owned(),
            cancel_requested: state == "cancelled",
            cancel_reason: None,
            principal: "principal".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("web".to_owned()),
            prompt_tokens: 10,
            completion_tokens: 5,
            total_tokens: 15,
            created_at_unix_ms: 1,
            started_at_unix_ms: 2,
            completed_at_unix_ms: Some(10),
            updated_at_unix_ms: 10,
            last_error: last_error.map(ToOwned::to_owned),
            origin_kind: "delegation".to_owned(),
            origin_run_id: Some("parent".to_owned()),
            parent_run_id: Some("parent".to_owned()),
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegation: None,
            merge_result: None,
            tape_events: 0,
        }
    }

    fn sample_merge_result(approval_required: bool) -> DelegationMergeResult {
        DelegationMergeResult {
            status: "done".to_owned(),
            strategy: DelegationMergeStrategy::Summarize,
            summary_text: "unreviewed child output".to_owned(),
            warnings: Vec::new(),
            failure_category: None,
            approval_required,
            approval_summary: DelegationMergeApprovalSummary {
                approval_required,
                approval_events: 0,
                approval_pending: approval_required,
                approval_denied: false,
            },
            usage_summary: DelegationMergeUsageSummary {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(2),
                duration_ms: Some(1),
            },
            artifact_references: Vec::new(),
            tool_trace_summary: Vec::new(),
            provenance: Vec::new(),
            merged_at_unix_ms: Some(3),
        }
    }

    fn sample_task(
        task_id: &str,
        state: &str,
        created_at_unix_ms: i64,
        group_id: &str,
        runtime_limits: DelegationRuntimeLimits,
    ) -> OrchestratorBackgroundTaskRecord {
        OrchestratorBackgroundTaskRecord {
            task_id: task_id.to_owned(),
            task_kind: "delegation_prompt".to_owned(),
            session_id: "session".to_owned(),
            parent_run_id: Some("parent-run".to_owned()),
            target_run_id: (state == AuxiliaryTaskState::Running.as_str())
                .then(|| format!("run-{task_id}")),
            queued_input_id: None,
            owner_principal: "principal".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("web".to_owned()),
            state: state.to_owned(),
            priority: 0,
            attempt_count: 0,
            max_attempts: 3,
            budget_tokens: runtime_limits.child_budget_override.unwrap_or(1_000),
            delegation: Some(DelegationSnapshot {
                profile_id: "research".to_owned(),
                display_name: "Research".to_owned(),
                description: None,
                template_id: None,
                role: DelegationRole::Research,
                execution_mode: DelegationExecutionMode::Parallel,
                group_id: group_id.to_owned(),
                model_profile: "gpt-4o-mini".to_owned(),
                tool_allowlist: Vec::new(),
                skill_allowlist: Vec::new(),
                memory_scope: DelegationMemoryScopeKind::ParentSession,
                budget_tokens: runtime_limits.child_budget_override.unwrap_or(1_000),
                max_attempts: 3,
                merge_contract: DelegationMergeContract {
                    strategy: DelegationMergeStrategy::Summarize,
                    approval_required: false,
                },
                runtime_limits,
                agent_id: Some("main".to_owned()),
            }),
            not_before_unix_ms: None,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some("delegate".to_owned()),
            payload_json: None,
            last_error: None,
            result_json: None,
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            started_at_unix_ms: (state == AuxiliaryTaskState::Running.as_str())
                .then_some(created_at_unix_ms),
            completed_at_unix_ms: None,
        }
    }
}
