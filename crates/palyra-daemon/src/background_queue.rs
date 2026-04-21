#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use palyra_common::runtime_contracts::AuxiliaryTaskState;
use serde_json::{json, Value};
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::learning::{process_post_run_reflection_task, REFLECTION_TASK_KIND},
    auxiliary_executor::{execute_auxiliary_task, AuxiliaryExecutionRequest, AuxiliaryTaskType},
    delegation::{
        DelegationExecutionMode, DelegationMergeApprovalSummary, DelegationMergeArtifactReference,
        DelegationMergeFailureCategory, DelegationMergeProvenanceRecord, DelegationMergeResult,
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
    self_healing::{WorkHeartbeatKind, WorkHeartbeatUpdate},
};

const BACKGROUND_QUEUE_IDLE_SLEEP: Duration = Duration::from_secs(3);
const DEFAULT_BACKGROUND_CHANNEL: &str = "console:background";
const CHILD_PROGRESS_MIN_INTERVAL_MS: i64 = 2_000;
const CHILD_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

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

async fn poll_background_queue(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
) -> Result<(), Status> {
    let tasks = runtime
        .list_orchestrator_background_tasks(crate::journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: None,
            device_id: None,
            channel: None,
            session_id: None,
            include_completed: false,
            limit: 256,
        })
        .await?;
    for task in tasks.iter() {
        if let Err(error) =
            process_background_task(runtime, auth, grpc_url, task, tasks.as_slice()).await
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
        }
    }
    crate::flows::FlowCoordinator::poll(runtime).await?;
    Ok(())
}

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
            if snapshot
                .as_ref()
                .map(|run| is_terminal_run_state(run.state.as_str()))
                .unwrap_or(true)
            {
                finalize_task_from_run(runtime, task, snapshot.as_ref(), "cancelled").await?;
            }
        } else {
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
        if task.task_kind == REFLECTION_TASK_KIND && task.target_run_id.is_none() {
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
        }
    }
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

async fn dispatch_background_task(
    runtime: &Arc<GatewayRuntimeState>,
    auth: &GatewayAuthConfig,
    grpc_url: &str,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<(), Status> {
    let started_at_unix_ms = crate::gateway::current_unix_ms();
    if task.task_kind == REFLECTION_TASK_KIND {
        runtime
            .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                task_id: task.task_id.clone(),
                state: Some(AuxiliaryTaskState::Running.as_str().to_owned()),
                target_run_id: Some(None),
                increment_attempt_count: true,
                last_error: Some(None),
                result_json: Some(None),
                started_at_unix_ms: Some(Some(started_at_unix_ms)),
                completed_at_unix_ms: Some(None),
            })
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
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(AuxiliaryTaskState::Running.as_str().to_owned()),
            target_run_id: Some(Some(run_id.clone())),
            increment_attempt_count: true,
            last_error: Some(None),
            result_json: Some(None),
            started_at_unix_ms: Some(Some(started_at_unix_ms)),
            completed_at_unix_ms: Some(None),
        })
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

async fn dispatch_auxiliary_executor_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    task_type: AuxiliaryTaskType,
    started_at_unix_ms: i64,
) -> Result<(), Status> {
    runtime
        .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
            task_id: task.task_id.clone(),
            state: Some(AuxiliaryTaskState::Running.as_str().to_owned()),
            target_run_id: Some(None),
            increment_attempt_count: true,
            last_error: Some(None),
            result_json: Some(None),
            started_at_unix_ms: Some(Some(started_at_unix_ms)),
            completed_at_unix_ms: Some(None),
        })
        .await?;

    let runtime = Arc::clone(runtime);
    let task = task.clone();
    tokio::spawn(async move {
        let parameter_delta_json = extract_parameter_delta_value(task.payload_json.as_deref())
            .ok()
            .flatten()
            .map(|value| value.to_string());
        let context = RequestContext {
            principal: task.owner_principal.clone(),
            device_id: task.device_id.clone(),
            channel: task.channel.clone(),
        };
        let input_text = task
            .input_text
            .clone()
            .unwrap_or_else(|| format!("Auxiliary task {} ({})", task.task_id, task.task_kind));
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
                let _ = runtime
                    .update_orchestrator_background_task(OrchestratorBackgroundTaskUpdateRequest {
                        task_id: task.task_id.clone(),
                        state: Some(AuxiliaryTaskState::Succeeded.as_str().to_owned()),
                        target_run_id: Some(None),
                        increment_attempt_count: false,
                        last_error: Some(None),
                        result_json: Some(Some(result.to_result_json().to_string())),
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

enum DelegationSchedulerDecision {
    Defer { reason: &'static str, message: String },
    Fail { reason: &'static str, message: String },
}

fn evaluate_delegation_scheduler_limits(
    all_tasks: &[OrchestratorBackgroundTaskRecord],
    task: &OrchestratorBackgroundTaskRecord,
) -> Option<DelegationSchedulerDecision> {
    let delegation = task.delegation.as_ref()?;
    let parent_run_id = task.parent_run_id.as_deref()?;
    let limits = &delegation.runtime_limits;
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
            && matches!(
                AuxiliaryTaskState::from_str(candidate.state.as_str()),
                Some(AuxiliaryTaskState::Running | AuxiliaryTaskState::CancelRequested)
            )
            && candidate.target_run_id.is_some()
    })
}

fn task_precedes_or_equals(
    candidate: &OrchestratorBackgroundTaskRecord,
    task: &OrchestratorBackgroundTaskRecord,
) -> bool {
    candidate.created_at_unix_ms < task.created_at_unix_ms
        || (candidate.created_at_unix_ms == task.created_at_unix_ms
            && candidate.task_id.as_str() <= task.task_id.as_str())
}

async fn mark_delegation_task_waiting(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    reason: &'static str,
    message: String,
) -> Result<(), Status> {
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

async fn request_delegated_child_timeout_cancel(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    target_run_id: &str,
    message: String,
) -> Result<(), Status> {
    runtime
        .request_orchestrator_cancel(crate::journal::OrchestratorCancelRequest {
            run_id: target_run_id.to_owned(),
            reason: "delegated_child_timeout".to_owned(),
        })
        .await?;
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

    append_parent_spawned_event(runtime, task, run_id).await?;

    let mut stream_error = None::<String>;
    let mut latest_child_state = "running".to_owned();
    let mut last_progress_at_unix_ms = 0_i64;
    let mut model_token_chars = 0_usize;
    let mut heartbeat = tokio::time::interval(CHILD_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
                            let should_emit = progress.event_type != "child_progress"
                                || progress.user_visible
                                || now.saturating_sub(last_progress_at_unix_ms)
                                    >= CHILD_PROGRESS_MIN_INTERVAL_MS;
                            if should_emit {
                                append_child_lifecycle_event(
                                    runtime,
                                    task,
                                    Some(run_id),
                                    progress.event_type,
                                    progress.child_state.as_str(),
                                    progress.user_visible,
                                    progress.details,
                                )
                                .await?;
                                last_progress_at_unix_ms = now;
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
        }
    }

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
    Ok(true)
}

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

fn delegation_serial_group(task: &OrchestratorBackgroundTaskRecord) -> Option<&str> {
    let delegation = task.delegation.as_ref()?;
    (delegation.execution_mode == DelegationExecutionMode::Serial)
        .then_some(delegation.group_id.as_str())
}

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
        Some(AuxiliaryTaskState::CancelRequested) => sibling.target_run_id.is_some(),
        Some(AuxiliaryTaskState::Queued | AuxiliaryTaskState::Paused) => {
            task_precedes_in_serial_group(sibling, current)
        }
        _ => false,
    }
}

fn task_precedes_in_serial_group(
    sibling: &OrchestratorBackgroundTaskRecord,
    current: &OrchestratorBackgroundTaskRecord,
) -> bool {
    sibling.created_at_unix_ms < current.created_at_unix_ms
        || (sibling.created_at_unix_ms == current.created_at_unix_ms
            && sibling.task_id < current.task_id)
}

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

fn build_parameter_delta_bytes(task: &OrchestratorBackgroundTaskRecord) -> Result<Vec<u8>, Status> {
    let mut merged = match extract_parameter_delta_value(task.payload_json.as_deref())? {
        Some(Value::Object(object)) => Value::Object(object),
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
        }
    }
    serde_json::to_vec(&merged).map_err(|error| {
        Status::internal(format!("failed to encode background parameter_delta bytes: {error}"))
    })
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

struct ChildStreamProgress {
    event_type: &'static str,
    child_state: String,
    user_visible: bool,
    details: Value,
}

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

async fn load_run_tape(
    runtime: &Arc<GatewayRuntimeState>,
    run_id: &str,
) -> Result<Vec<crate::journal::OrchestratorTapeRecord>, Status> {
    let mut after_seq = None;
    let mut events = Vec::new();
    for _ in 0..16 {
        let page =
            runtime.orchestrator_tape_snapshot(run_id.to_owned(), after_seq, Some(128)).await?;
        after_seq = page.next_after_seq;
        events.extend(page.events);
        if after_seq.is_none() {
            break;
        }
    }
    Ok(events)
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

async fn append_parent_spawned_event(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    child_run_id: &str,
) -> Result<(), Status> {
    let Some(parent_run_id) = task.parent_run_id.as_deref() else {
        return Ok(());
    };
    append_parent_tape_event(
        runtime,
        parent_run_id,
        "child_run_spawned",
        json!({
            "task_id": task.task_id,
            "child_run_id": child_run_id,
            "session_id": task.session_id,
            "delegation": task.delegation,
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
        }),
    )
    .await
}

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
    append_parent_tape_event(
        runtime,
        parent_run_id,
        event_type,
        json!({
            "task_id": task.task_id,
            "child_run_id": run.run_id,
            "child_state": run.state,
            "merge_result": merge_result,
        }),
    )
    .await?;
    let (child_event_type, child_state) = match run.state.as_str() {
        "done" => ("child_completed", "completed"),
        "failed" => ("child_failed", "failed"),
        "cancelled" => ("child_failed", "cancelled"),
        other => ("child_completed", other),
    };
    append_child_lifecycle_event(
        runtime,
        task,
        Some(run.run_id.as_str()),
        child_event_type,
        child_state,
        true,
        json!({
            "legacy_event_type": event_type,
            "merge_result": merge_result,
        }),
    )
    .await
}

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
            "observed_at_unix_ms": crate::gateway::current_unix_ms(),
            "details": details,
        }),
    )
    .await
}

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

fn value_excerpt(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn truncate_excerpt(value: &str, max_chars: usize) -> String {
    let mut excerpt = value.trim().chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        excerpt.push_str("...");
    }
    excerpt
}

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
    let header_channel =
        channel.filter(|value| !value.trim().is_empty()).unwrap_or(DEFAULT_BACKGROUND_CHANNEL);
    metadata.insert(
        HEADER_CHANNEL,
        header_channel
            .parse()
            .map_err(|_| Status::invalid_argument("background channel metadata is invalid"))?,
    );
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

fn is_terminal_task_state(state: &str) -> bool {
    AuxiliaryTaskState::from_str(state).is_some_and(AuxiliaryTaskState::is_terminal)
}

fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

#[cfg(test)]
mod tests {
    use super::{
        append_artifact_references, categorize_child_failure, delegated_child_timeout_message,
        evaluate_delegation_scheduler_limits, DelegationSchedulerDecision,
    };
    use crate::{
        delegation::{
            DelegationExecutionMode, DelegationMemoryScopeKind, DelegationMergeApprovalSummary,
            DelegationMergeContract, DelegationMergeFailureCategory, DelegationMergeStrategy,
            DelegationRole, DelegationRuntimeLimits, DelegationSnapshot,
        },
        journal::{OrchestratorBackgroundTaskRecord, OrchestratorRunStatusSnapshot},
    };
    use palyra_common::runtime_contracts::AuxiliaryTaskState;
    use serde_json::json;

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
    fn evaluate_delegation_scheduler_limits_defers_for_concurrency() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 1,
            max_children_per_parent: 8,
            max_parallel_groups: 2,
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
    fn evaluate_delegation_scheduler_limits_fails_child_overflow() {
        let limits = DelegationRuntimeLimits {
            max_concurrent_children: 4,
            max_children_per_parent: 1,
            max_parallel_groups: 2,
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
    fn delegated_child_timeout_message_uses_runtime_limit() {
        let mut task = sample_task(
            "task-timeout",
            AuxiliaryTaskState::Running.as_str(),
            10,
            "group-a",
            DelegationRuntimeLimits {
                max_concurrent_children: 1,
                max_children_per_parent: 8,
                max_parallel_groups: 1,
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
