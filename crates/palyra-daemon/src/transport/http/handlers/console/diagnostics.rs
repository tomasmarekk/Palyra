use crate::*;
use palyra_common::feature_rollouts::{
    AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH, AUXILIARY_EXECUTOR_ROLLOUT_ENV,
    CONTEXT_ENGINE_ROLLOUT_CONFIG_PATH, CONTEXT_ENGINE_ROLLOUT_ENV,
    DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH, DELIVERY_ARBITRATION_ROLLOUT_ENV,
    DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH, DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
    EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH,
    EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
    EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_CONFIG_PATH, EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV,
    EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_CONFIG_PATH, EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
    EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH, EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV,
    FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH, FLOW_ORCHESTRATION_ROLLOUT_ENV,
    NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH, NETWORKED_WORKERS_ROLLOUT_ENV,
    PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH, PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
    REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH, REPLAY_CAPTURE_ROLLOUT_ENV,
    RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH, RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
    SAFETY_BOUNDARY_ROLLOUT_CONFIG_PATH, SAFETY_BOUNDARY_ROLLOUT_ENV,
    SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH, SESSION_QUEUE_POLICY_ROLLOUT_ENV,
};
use palyra_common::replay_bundle::replay_contract_snapshot;
use palyra_common::runtime_contracts::{FlowState, FlowStepState};

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

pub(crate) async fn console_diagnostics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let status_snapshot = state
        .runtime
        .status_snapshot_async(session.context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;

    let mut provider_payload =
        serde_json::to_value(&status_snapshot.model_provider).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to serialize diagnostics model provider payload: {error}"
            )))
        })?;
    redact_console_diagnostics_value(&mut provider_payload, None);

    let mut auth_payload = serde_json::to_value(&auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize diagnostics auth payload: {error}"
        )))
    })?;
    redact_console_diagnostics_value(&mut auth_payload, None);

    let browser_payload = collect_console_browser_diagnostics(&state).await;
    let skills_payload = collect_console_skills_diagnostics(&state).await;
    let plugins_payload = collect_console_plugins_diagnostics();
    let hooks_payload = collect_console_hooks_diagnostics();
    let media_payload = state.channels.media_snapshot().map_err(channel_platform_error_response)?;
    let webhook_payload = serde_json::to_value(
        state.webhooks.diagnostics_snapshot(state.vault.as_ref()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to summarize webhook diagnostics: {error}"
            )))
        })?,
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize webhook diagnostics payload: {error}"
        )))
    })?;
    let memory_status =
        state.runtime.memory_maintenance_status().await.map_err(runtime_status_response)?;
    let memory_embeddings =
        state.runtime.memory_embeddings_status().await.map_err(runtime_status_response)?;
    let retrieval_backend =
        state.runtime.retrieval_backend_snapshot().map_err(runtime_status_response)?;
    let retrieval_config = state.runtime.retrieval_config_snapshot();
    let memory_runtime_config = state.runtime.memory_config_snapshot();
    let learning_runtime_config = state.runtime.learning_config_snapshot();
    let access_snapshot = {
        let registry = super::access::lock_access_registry(&state.access_registry);
        registry.snapshot(session.context.principal.as_str())
    };
    let objectives_payload =
        collect_console_objectives_diagnostics(&state, session.context.principal.as_str())?;
    let flows_payload =
        collect_console_flows_diagnostics(&state, session.context.principal.as_str()).await?;
    let delegation_payload = collect_console_delegation_diagnostics(&state, &session.context)
        .await
        .map_err(runtime_status_response)?;
    let deployment_payload = collect_console_deployment_diagnostics(&state);
    let execution_backends_payload =
        collect_console_execution_backend_diagnostics(&state).map_err(runtime_status_response)?;
    let runtime_controls_payload = serde_json::to_value(
        crate::runtime_preview_controls::build_runtime_preview_config_snapshot(
            &state.runtime.config,
        ),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize runtime controls diagnostics payload: {error}"
        )))
    })?;
    let canvas_experiments_payload =
        serde_json::to_value(crate::gateway::build_canvas_experiment_governance_snapshot(
            &state.runtime.config.canvas_host,
        ))
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to serialize canvas experiment diagnostics payload: {error}"
            )))
        })?;
    let observability_payload = build_observability_payload(
        &state,
        &session.context,
        &status_snapshot.model_provider,
        &auth_payload,
        &media_payload,
    )
    .await?;
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;

    Ok(Json(json!({
        "contract": contract_descriptor(),
        "generated_at_unix_ms": generated_at_unix_ms,
        "model_provider": provider_payload,
        "rate_limits": {
            "admin_api_window_ms": ADMIN_RATE_LIMIT_WINDOW_MS,
            "admin_api_max_requests_per_window": ADMIN_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
            "canvas_api_window_ms": CANVAS_RATE_LIMIT_WINDOW_MS,
            "canvas_api_max_requests_per_window": CANVAS_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW,
            "denied_requests_total": status_snapshot.counters.denied_requests,
        },
        "auth_profiles": auth_payload,
        "browserd": browser_payload,
        "skills": skills_payload,
        "plugins": plugins_payload,
        "hooks": hooks_payload,
        "webhooks": webhook_payload,
        "media": media_payload,
        "objectives": objectives_payload,
        "flows": flows_payload,
        "delegation": delegation_payload,
        "access": {
            "feature_flags": access_snapshot.feature_flags,
            "migration": access_snapshot.migration,
            "rollout": access_snapshot.rollout,
            "telemetry": access_snapshot.telemetry,
        },
        "feature_rollouts": collect_console_feature_rollouts_diagnostics(&state),
        "runtime_controls": runtime_controls_payload,
        "deployment": deployment_payload,
        "execution_backends": execution_backends_payload,
        "canvas_experiments": canvas_experiments_payload,
        "observability": observability_payload,
        "memory": {
            "usage": memory_status.usage,
            "embeddings": memory_embeddings,
            "retrieval": {
                "backend": retrieval_backend,
                "scoring": retrieval_config.scoring,
            },
            "retention": {
                "max_entries": memory_runtime_config.retention_max_entries,
                "max_bytes": memory_runtime_config.retention_max_bytes,
                "ttl_days": memory_runtime_config.retention_ttl_days,
                "vacuum_schedule": memory_runtime_config.retention_vacuum_schedule,
            },
            "maintenance": {
                "interval_ms": i64::try_from(MEMORY_MAINTENANCE_INTERVAL.as_millis())
                    .unwrap_or(i64::MAX),
                "last_run": memory_status.last_run,
                "last_vacuum_at_unix_ms": memory_status.last_vacuum_at_unix_ms,
                "next_vacuum_due_at_unix_ms": memory_status.next_vacuum_due_at_unix_ms,
                "next_run_at_unix_ms": memory_status.next_maintenance_run_at_unix_ms,
            }
        },
        "learning": {
            "enabled": learning_runtime_config.enabled,
            "sampling_percent": learning_runtime_config.sampling_percent,
            "cooldown_ms": learning_runtime_config.cooldown_ms,
            "budget_tokens": learning_runtime_config.budget_tokens,
            "max_candidates_per_run": learning_runtime_config.max_candidates_per_run,
            "thresholds": {
                "durable_fact": {
                    "review_min_confidence_bps": learning_runtime_config.durable_fact_review_min_confidence_bps,
                    "auto_apply_confidence_bps": learning_runtime_config.durable_fact_auto_write_threshold_bps,
                },
                "preference": {
                    "review_min_confidence_bps": learning_runtime_config.preference_review_min_confidence_bps,
                },
                "procedure": {
                    "review_min_confidence_bps": learning_runtime_config.procedure_review_min_confidence_bps,
                    "min_occurrences": learning_runtime_config.procedure_min_occurrences,
                }
            },
            "counters": {
                "reflections_scheduled": status_snapshot.counters.learning_reflections_scheduled,
                "reflections_completed": status_snapshot.counters.learning_reflections_completed,
                "candidates_created": status_snapshot.counters.learning_candidates_created,
                "candidates_auto_applied": status_snapshot.counters.learning_candidates_auto_applied,
            }
        },
    })))
}

async fn collect_console_delegation_diagnostics(
    state: &AppState,
    context: &crate::gateway::RequestContext,
) -> Result<Value, tonic::Status> {
    let tasks = state
        .runtime
        .list_orchestrator_background_tasks(crate::journal::OrchestratorBackgroundTaskListFilter {
            owner_principal: Some(context.principal.clone()),
            device_id: None,
            channel: None,
            session_id: None,
            include_completed: false,
            limit: 256,
        })
        .await?;
    let delegated_tasks = tasks.iter().filter(|task| task.delegation.is_some()).collect::<Vec<_>>();
    let mut parent_groups =
        std::collections::BTreeMap::<String, DelegationParentDiagnostics>::new();
    let mut recent_children = Vec::new();
    let mut running_children = 0_usize;
    let mut queued_children = 0_usize;
    let mut waiting_children = 0_usize;
    let mut failed_children = 0_usize;

    for task in delegated_tasks.iter() {
        match task.state.as_str() {
            "running" | "cancel_requested" => running_children += 1,
            "queued" => queued_children += 1,
            "failed" => failed_children += 1,
            _ => {}
        }
        let waiting_reason = delegation_waiting_reason(task);
        if waiting_reason.is_some() {
            waiting_children += 1;
        }
        if let (Some(parent_run_id), Some(delegation)) =
            (task.parent_run_id.as_deref(), task.delegation.as_ref())
        {
            let group = parent_groups.entry(parent_run_id.to_owned()).or_insert_with(|| {
                DelegationParentDiagnostics {
                    parent_run_id: parent_run_id.to_owned(),
                    child_count: 0,
                    running_children: 0,
                    queued_children: 0,
                    waiting_children: 0,
                    failed_children: 0,
                    active_parallel_groups: Vec::new(),
                    max_concurrent_children: delegation.runtime_limits.max_concurrent_children,
                    max_children_per_parent: delegation.runtime_limits.max_children_per_parent,
                    max_parallel_groups: delegation.runtime_limits.max_parallel_groups,
                    child_timeout_ms: delegation.runtime_limits.child_timeout_ms,
                }
            });
            group.child_count += 1;
            group.max_concurrent_children = group
                .max_concurrent_children
                .min(delegation.runtime_limits.max_concurrent_children);
            group.max_children_per_parent = group
                .max_children_per_parent
                .min(delegation.runtime_limits.max_children_per_parent);
            group.max_parallel_groups =
                group.max_parallel_groups.min(delegation.runtime_limits.max_parallel_groups);
            group.child_timeout_ms =
                group.child_timeout_ms.min(delegation.runtime_limits.child_timeout_ms);
            match task.state.as_str() {
                "running" | "cancel_requested" => {
                    group.running_children += 1;
                    if delegation.execution_mode
                        == crate::delegation::DelegationExecutionMode::Parallel
                        && !group
                            .active_parallel_groups
                            .iter()
                            .any(|value| value == &delegation.group_id)
                    {
                        group.active_parallel_groups.push(delegation.group_id.clone());
                    }
                }
                "queued" => group.queued_children += 1,
                "failed" => group.failed_children += 1,
                _ => {}
            }
            if waiting_reason.is_some() {
                group.waiting_children += 1;
            }
        }
        if recent_children.len() < 24 {
            let delegation = task.delegation.as_ref().expect("filtered delegated task");
            recent_children.push(json!({
                "task_id": task.task_id,
                "session_id": task.session_id,
                "parent_run_id": task.parent_run_id,
                "child_run_id": task.target_run_id,
                "state": task.state,
                "task_kind": task.task_kind,
                "profile_id": delegation.profile_id,
                "display_name": delegation.display_name,
                "execution_mode": delegation.execution_mode,
                "group_id": delegation.group_id,
                "budget_tokens": task.budget_tokens,
                "runtime_limits": delegation.runtime_limits,
                "waiting_reason": waiting_reason,
                "last_error": task.last_error.as_deref().map(|value| truncate_diagnostic_text(value, 240)),
                "created_at_unix_ms": task.created_at_unix_ms,
                "started_at_unix_ms": task.started_at_unix_ms,
                "updated_at_unix_ms": task.updated_at_unix_ms,
            }));
        }
    }

    let parents = parent_groups
        .into_values()
        .map(|group| {
            json!({
                "parent_run_id": group.parent_run_id,
                "child_count": group.child_count,
                "running_children": group.running_children,
                "queued_children": group.queued_children,
                "waiting_children": group.waiting_children,
                "failed_children": group.failed_children,
                "active_parallel_groups": group.active_parallel_groups,
                "active_parallel_group_count": group.active_parallel_groups.len(),
                "max_concurrent_children": group.max_concurrent_children,
                "max_children_per_parent": group.max_children_per_parent,
                "max_parallel_groups": group.max_parallel_groups,
                "child_timeout_ms": group.child_timeout_ms,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "active_child_count": delegated_tasks.len(),
        "running_children": running_children,
        "queued_children": queued_children,
        "waiting_children": waiting_children,
        "failed_children": failed_children,
        "parent_count": parents.len(),
        "parents": parents,
        "recent_children": recent_children,
        "catalog": crate::delegation::built_in_delegation_catalog(),
    }))
}

struct DelegationParentDiagnostics {
    parent_run_id: String,
    child_count: usize,
    running_children: usize,
    queued_children: usize,
    waiting_children: usize,
    failed_children: usize,
    active_parallel_groups: Vec<String>,
    max_concurrent_children: u64,
    max_children_per_parent: u64,
    max_parallel_groups: u64,
    child_timeout_ms: u64,
}

fn delegation_waiting_reason(
    task: &crate::journal::OrchestratorBackgroundTaskRecord,
) -> Option<String> {
    task.result_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| {
            (value.get("status").and_then(Value::as_str) == Some("waiting")).then(|| {
                value.get("reason").and_then(Value::as_str).unwrap_or("waiting").to_owned()
            })
        })
}

fn truncate_diagnostic_text(value: &str, max_chars: usize) -> String {
    let mut truncated = value.trim().chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated
}

fn collect_console_execution_backend_diagnostics(state: &AppState) -> Result<Value, tonic::Status> {
    let now_unix_ms = crate::gateway::current_unix_ms_status()?;
    let nodes = state.node_runtime.nodes()?;
    serde_json::to_value(
        crate::execution_backends::build_execution_backend_inventory_with_worker_state(
            &state.runtime.config.tool_call.process_runner,
            nodes.as_slice(),
            now_unix_ms,
            &state.runtime.config.feature_rollouts,
            &state.runtime.config.networked_workers,
            state.runtime.worker_fleet_snapshot(),
            &state.runtime.worker_fleet_policy(),
        ),
    )
    .map_err(|error| {
        tonic::Status::internal(format!("failed to serialize execution backends: {error}"))
    })
}

fn collect_console_feature_rollouts_diagnostics(state: &AppState) -> Value {
    let feature_rollouts = &state.runtime.config.feature_rollouts;
    json!({
        "dynamic_tool_builder": {
            "enabled": feature_rollouts.dynamic_tool_builder.enabled,
            "source": feature_rollouts.dynamic_tool_builder.source,
            "config_path": DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH,
            "env_var": DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
        },
        "context_engine": {
            "enabled": feature_rollouts.context_engine.enabled,
            "source": feature_rollouts.context_engine.source,
            "config_path": CONTEXT_ENGINE_ROLLOUT_CONFIG_PATH,
            "env_var": CONTEXT_ENGINE_ROLLOUT_ENV,
        },
        "execution_backend_remote_node": {
            "enabled": feature_rollouts.execution_backend_remote_node.enabled,
            "source": feature_rollouts.execution_backend_remote_node.source,
            "config_path": EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_CONFIG_PATH,
            "env_var": EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV,
        },
        "execution_backend_networked_worker": {
            "enabled": feature_rollouts.execution_backend_networked_worker.enabled,
            "source": feature_rollouts.execution_backend_networked_worker.source,
            "config_path": EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH,
            "env_var": EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
        },
        "execution_backend_ssh_tunnel": {
            "enabled": feature_rollouts.execution_backend_ssh_tunnel.enabled,
            "source": feature_rollouts.execution_backend_ssh_tunnel.source,
            "config_path": EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_CONFIG_PATH,
            "env_var": EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
        },
        "safety_boundary": {
            "enabled": feature_rollouts.safety_boundary.enabled,
            "source": feature_rollouts.safety_boundary.source,
            "config_path": SAFETY_BOUNDARY_ROLLOUT_CONFIG_PATH,
            "env_var": SAFETY_BOUNDARY_ROLLOUT_ENV,
        },
        "execution_gate_pipeline_v2": {
            "enabled": feature_rollouts.execution_gate_pipeline_v2.enabled,
            "source": feature_rollouts.execution_gate_pipeline_v2.source,
            "config_path": EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH,
            "env_var": EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV,
        },
        "session_queue_policy": {
            "enabled": feature_rollouts.session_queue_policy.enabled,
            "source": feature_rollouts.session_queue_policy.source,
            "config_path": SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH,
            "env_var": SESSION_QUEUE_POLICY_ROLLOUT_ENV,
        },
        "pruning_policy_matrix": {
            "enabled": feature_rollouts.pruning_policy_matrix.enabled,
            "source": feature_rollouts.pruning_policy_matrix.source,
            "config_path": PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH,
            "env_var": PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
        },
        "retrieval_dual_path": {
            "enabled": feature_rollouts.retrieval_dual_path.enabled,
            "source": feature_rollouts.retrieval_dual_path.source,
            "config_path": RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH,
            "env_var": RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
        },
        "auxiliary_executor": {
            "enabled": feature_rollouts.auxiliary_executor.enabled,
            "source": feature_rollouts.auxiliary_executor.source,
            "config_path": AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH,
            "env_var": AUXILIARY_EXECUTOR_ROLLOUT_ENV,
        },
        "flow_orchestration": {
            "enabled": feature_rollouts.flow_orchestration.enabled,
            "source": feature_rollouts.flow_orchestration.source,
            "config_path": FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH,
            "env_var": FLOW_ORCHESTRATION_ROLLOUT_ENV,
        },
        "delivery_arbitration": {
            "enabled": feature_rollouts.delivery_arbitration.enabled,
            "source": feature_rollouts.delivery_arbitration.source,
            "config_path": DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH,
            "env_var": DELIVERY_ARBITRATION_ROLLOUT_ENV,
        },
        "replay_capture": {
            "enabled": feature_rollouts.replay_capture.enabled,
            "source": feature_rollouts.replay_capture.source,
            "config_path": REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH,
            "env_var": REPLAY_CAPTURE_ROLLOUT_ENV,
        },
        "networked_workers": {
            "enabled": feature_rollouts.networked_workers.enabled,
            "source": feature_rollouts.networked_workers.source,
            "config_path": NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH,
            "env_var": NETWORKED_WORKERS_ROLLOUT_ENV,
        },
    })
}

#[allow(clippy::result_large_err)]
fn collect_console_objectives_diagnostics(
    state: &AppState,
    principal: &str,
) -> Result<Value, Response> {
    let objectives = state
        .objectives
        .list_objectives()
        .map_err(|error| runtime_status_response(tonic::Status::internal(error.to_string())))?
        .into_iter()
        .filter(|entry| entry.owner_principal == principal)
        .collect::<Vec<_>>();
    let mut by_state = std::collections::BTreeMap::<String, u64>::new();
    let mut by_kind = std::collections::BTreeMap::<String, u64>::new();
    for objective in &objectives {
        *by_state.entry(objective.state.as_str().to_owned()).or_default() += 1;
        *by_kind.entry(objective.kind.as_str().to_owned()).or_default() += 1;
    }
    let recent = objectives
        .iter()
        .rev()
        .take(10)
        .map(|entry| {
            json!({
                "objective_id": entry.objective_id,
                "kind": entry.kind.as_str(),
                "state": entry.state.as_str(),
                "name": entry.name,
                "updated_at_unix_ms": entry.updated_at_unix_ms,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "count": objectives.len(),
        "by_state": by_state,
        "by_kind": by_kind,
        "recent": recent,
    }))
}

#[allow(clippy::result_large_err)]
async fn collect_console_flows_diagnostics(
    state: &AppState,
    principal: &str,
) -> Result<Value, Response> {
    let limit = 50_usize;
    let flows = state
        .runtime
        .list_flows(crate::journal::FlowListFilter {
            owner_principal: Some(principal.to_owned()),
            device_id: None,
            channel: None,
            state: None,
            include_terminal: false,
            limit,
        })
        .await
        .map_err(runtime_status_response)?;
    let mut by_state = std::collections::BTreeMap::<String, u64>::new();
    let mut active_flows = 0_u64;
    let mut blocked_flows = 0_u64;
    let mut waiting_flows = 0_u64;
    for flow in &flows {
        *by_state.entry(flow.state.clone()).or_default() += 1;
        match FlowState::from_str(flow.state.as_str()) {
            Some(FlowState::Blocked) => blocked_flows += 1,
            Some(FlowState::WaitingForApproval) => waiting_flows += 1,
            Some(state) if !state.is_terminal() => active_flows += 1,
            _ => {}
        }
    }

    let mut blocked_steps = 0_u64;
    let mut retrying_steps = 0_u64;
    let mut timed_out_steps = 0_u64;
    let mut waiting_steps = 0_u64;
    let mut recent = Vec::new();
    for flow in flows.iter().take(10) {
        let Some(bundle) = state
            .runtime
            .get_flow_bundle(flow.flow_id.clone(), 64)
            .await
            .map_err(runtime_status_response)?
        else {
            continue;
        };
        for step in &bundle.steps {
            match FlowStepState::from_str(step.state.as_str()) {
                Some(FlowStepState::Blocked) => blocked_steps += 1,
                Some(FlowStepState::Retrying) => retrying_steps += 1,
                Some(FlowStepState::TimedOut) => timed_out_steps += 1,
                Some(FlowStepState::WaitingForApproval) => waiting_steps += 1,
                _ => {}
            }
        }
        let latest_event = bundle.events.last().map(|event| {
            json!({
                "event_id": event.event_id.clone(),
                "event_type": event.event_type.clone(),
                "step_id": event.step_id.clone(),
                "summary": event.summary.clone(),
                "created_at_unix_ms": event.created_at_unix_ms,
            })
        });
        recent.push(json!({
            "flow_id": bundle.flow.flow_id,
            "mode": bundle.flow.mode,
            "state": bundle.flow.state,
            "title": bundle.flow.title,
            "current_step_id": bundle.flow.current_step_id,
            "revision": bundle.flow.revision,
            "updated_at_unix_ms": bundle.flow.updated_at_unix_ms,
            "step_count": bundle.steps.len(),
            "event_count": bundle.events.len(),
            "latest_event": latest_event,
        }));
    }

    Ok(json!({
        "active_count": flows.len(),
        "active_flows": active_flows,
        "blocked_flows": blocked_flows,
        "waiting_for_approval_flows": waiting_flows,
        "by_state": by_state,
        "steps": {
            "blocked": blocked_steps,
            "retrying": retrying_steps,
            "timed_out": timed_out_steps,
            "waiting_for_approval": waiting_steps,
        },
        "runtime": {
            "mode": state.runtime.config.flow_orchestration.mode.as_str(),
            "rollout_enabled": state.runtime.config.feature_rollouts.flow_orchestration.enabled,
            "rollout_source": state.runtime.config.feature_rollouts.flow_orchestration.source,
            "max_retry_count": state.runtime.config.flow_orchestration.max_retry_count,
            "cancellation_gate_enabled": state.runtime.config.flow_orchestration.cancellation_gate_enabled,
        },
        "adapters": crate::flows::flow_adapter_contracts(),
        "recent": recent,
    }))
}

pub(crate) async fn collect_console_browser_diagnostics(state: &AppState) -> Value {
    let mut failure_messages = Vec::<String>::new();
    let (relay_failures, relay_failure_messages) =
        collect_console_browser_relay_failure_metrics(state).await;
    failure_messages.extend(relay_failure_messages);

    let mut recent_health_failures = 0_u64;
    let mut health_payload = Value::Null;
    if state.browser_service_config.enabled {
        match build_console_browser_client(state).await {
            Ok(mut client) => {
                let mut request = TonicRequest::new(browser_v1::BrowserHealthRequest {
                    v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
                });
                match apply_browser_service_auth(state, request.metadata_mut()) {
                    Ok(()) => match client.health(request).await {
                        Ok(response) => {
                            let response = response.into_inner();
                            health_payload = json!({
                                "status": response.status,
                                "uptime_seconds": response.uptime_seconds,
                                "active_sessions": response.active_sessions,
                            });
                        }
                        Err(error) => {
                            recent_health_failures = recent_health_failures.saturating_add(1);
                            failure_messages
                                .push(sanitize_http_error_message(error.to_string().as_str()));
                        }
                    },
                    Err(response) => {
                        recent_health_failures = recent_health_failures.saturating_add(1);
                        failure_messages.push(format!(
                            "failed to apply browser diagnostics auth metadata (http {})",
                            response.status()
                        ));
                    }
                }
            }
            Err(response) => {
                recent_health_failures = recent_health_failures.saturating_add(1);
                failure_messages.push(format!(
                    "failed to connect browser service for diagnostics (http {})",
                    response.status()
                ));
            }
        }
    }

    while failure_messages.len() > 5 {
        failure_messages.pop();
    }

    let mut payload = json!({
        "enabled": state.browser_service_config.enabled,
        "endpoint": state.browser_service_config.endpoint,
        "sessions": {
            "active": health_payload.get("active_sessions").and_then(Value::as_u64).unwrap_or(0),
        },
        "budgets": {
            "connect_timeout_ms": state.browser_service_config.connect_timeout_ms,
            "request_timeout_ms": state.browser_service_config.request_timeout_ms,
            "max_screenshot_bytes": state.browser_service_config.max_screenshot_bytes,
            "max_title_bytes": state.browser_service_config.max_title_bytes,
        },
        "health": health_payload,
        "failures": {
            "recent_relay_action_failures": relay_failures,
            "recent_health_failures": recent_health_failures,
            "samples": failure_messages,
        },
    });
    redact_console_diagnostics_value(&mut payload, None);
    payload
}

pub(crate) async fn collect_console_skills_diagnostics(state: &AppState) -> Value {
    let skills_root = match resolve_skills_root() {
        Ok(path) => path,
        Err(response) => {
            return json!({
                "skills_root": "unavailable",
                "error": format!("failed to resolve skills root (http {})", response.status()),
            });
        }
    };
    let index = match load_installed_skills_index(skills_root.as_path()) {
        Ok(index) => index,
        Err(response) => {
            return json!({
                "skills_root": skills_root,
                "error": format!("failed to load installed skills index (http {})", response.status()),
            });
        }
    };
    let builder_index =
        crate::transport::http::handlers::console::skills::load_skill_builder_candidate_index(
            skills_root.as_path(),
        )
        .ok();

    let mut publishers =
        index.entries.iter().map(|entry| entry.publisher.clone()).collect::<Vec<_>>();
    publishers.sort();
    publishers.dedup();

    let mut trust_decisions = serde_json::Map::new();
    let mut runtime_active = 0_usize;
    let mut runtime_default_active = 0_usize;
    let mut runtime_quarantined = 0_usize;
    let mut runtime_disabled = 0_usize;
    let mut runtime_errors = Vec::new();
    let mut missing_secrets_total = 0_usize;

    for entry in &index.entries {
        let next = trust_decisions
            .get(entry.trust_decision.as_str())
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .saturating_add(1);
        trust_decisions.insert(entry.trust_decision.clone(), Value::from(next));
        if !entry.missing_secrets.is_empty() {
            missing_secrets_total = missing_secrets_total.saturating_add(1);
        }

        match state.runtime.skill_status(entry.skill_id.clone(), entry.version.clone()).await {
            Ok(Some(record)) => match record.status {
                SkillExecutionStatus::Active => runtime_active = runtime_active.saturating_add(1),
                SkillExecutionStatus::Quarantined => {
                    runtime_quarantined = runtime_quarantined.saturating_add(1)
                }
                SkillExecutionStatus::Disabled => {
                    runtime_disabled = runtime_disabled.saturating_add(1)
                }
            },
            Ok(None) => runtime_default_active = runtime_default_active.saturating_add(1),
            Err(error) => {
                runtime_errors.push(sanitize_http_error_message(error.to_string().as_str()));
            }
        }
    }

    while runtime_errors.len() > 5 {
        runtime_errors.pop();
    }

    json!({
        "skills_root": skills_root,
        "installed_total": index.entries.len(),
        "current_total": index.entries.iter().filter(|entry| entry.current).count(),
        "missing_secrets_total": missing_secrets_total,
        "publishers": publishers,
        "trust_decisions": Value::Object(trust_decisions),
        "runtime": {
            "active": runtime_active,
            "default_active": runtime_default_active,
            "quarantined": runtime_quarantined,
            "disabled": runtime_disabled,
            "errors": runtime_errors,
        },
        "builder": {
            "rollout_flag": DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            "rollout_source": state.runtime.config.feature_rollouts.dynamic_tool_builder.source,
            "rollout_enabled": state.runtime.config.feature_rollouts.dynamic_tool_builder.enabled,
            "candidate_total": builder_index.as_ref().map(|index| index.entries.len()).unwrap_or(0),
            "procedure_candidates": builder_index
                .as_ref()
                .map(|index| index.entries.iter().filter(|entry| entry.source_kind == "procedure").count())
                .unwrap_or(0),
            "prompt_candidates": builder_index
                .as_ref()
                .map(|index| index.entries.iter().filter(|entry| entry.source_kind == "prompt").count())
                .unwrap_or(0),
        },
    })
}

pub(crate) fn collect_console_plugins_diagnostics() -> Value {
    let plugins_root = match plugins::resolve_plugins_root() {
        Ok(path) => path,
        Err(error) => {
            return json!({
                "plugins_root": "unavailable",
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };
    let index = match plugins::load_plugin_bindings_index(plugins_root.as_path()) {
        Ok(index) => index,
        Err(error) => {
            return json!({
                "plugins_root": plugins_root,
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };

    let distinct_skills = index
        .entries
        .iter()
        .map(|entry| entry.skill_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut discovery_states = std::collections::BTreeMap::<String, usize>::new();
    let mut config_states = std::collections::BTreeMap::<String, usize>::new();
    let mut capability_drift_total = 0_usize;
    let mut config_references_total = 0_usize;
    let mut last_scanned_at_unix_ms = 0_i64;
    for entry in &index.entries {
        let discovery_state = serde_json::to_value(entry.discovery.state)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "unknown".to_owned());
        *discovery_states.entry(discovery_state).or_default() += 1;
        if let Some(config) = entry.config.as_ref() {
            config_references_total += 1;
            let config_state = serde_json::to_value(config.validation.state)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned());
            *config_states.entry(config_state).or_default() += 1;
        } else {
            *config_states.entry("unknown".to_owned()).or_default() += 1;
        }
        if !entry.capability_diff.valid {
            capability_drift_total += 1;
        }
        if let Some(scanned_at) = entry.discovery.last_scanned_at_unix_ms {
            last_scanned_at_unix_ms = last_scanned_at_unix_ms.max(scanned_at);
        }
    }
    json!({
        "plugins_root": plugins_root,
        "schema_version": index.schema_version,
        "bindings_total": index.entries.len(),
        "enabled_total": index.entries.iter().filter(|entry| entry.enabled).count(),
        "disabled_total": index.entries.iter().filter(|entry| !entry.enabled).count(),
        "distinct_skill_bindings": distinct_skills.len(),
        "config_references_total": config_references_total,
        "capability_drift_total": capability_drift_total,
        "last_scanned_at_unix_ms": (last_scanned_at_unix_ms > 0).then_some(last_scanned_at_unix_ms),
        "discovery_states": discovery_states,
        "config_validation_states": config_states,
    })
}

pub(crate) fn collect_console_hooks_diagnostics() -> Value {
    let hooks_root = match hooks::resolve_hooks_root() {
        Ok(path) => path,
        Err(error) => {
            return json!({
                "hooks_root": "unavailable",
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };
    let index = match hooks::load_hook_bindings_index(hooks_root.as_path()) {
        Ok(index) => index,
        Err(error) => {
            return json!({
                "hooks_root": hooks_root,
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };

    let distinct_events = index
        .entries
        .iter()
        .map(|entry| entry.event.clone())
        .collect::<std::collections::BTreeSet<_>>();
    json!({
        "hooks_root": hooks_root,
        "bindings_total": index.entries.len(),
        "enabled_total": index.entries.iter().filter(|entry| entry.enabled).count(),
        "disabled_total": index.entries.iter().filter(|entry| !entry.enabled).count(),
        "event_kinds": distinct_events.into_iter().collect::<Vec<_>>(),
    })
}

pub(crate) fn collect_console_deployment_diagnostics(state: &AppState) -> Value {
    serde_json::to_value(build_deployment_posture_summary(state)).unwrap_or_else(|_| {
        json!({
            "contract": { "contract_version": control_plane::CONTROL_PLANE_CONTRACT_VERSION },
            "mode": state.deployment.mode,
            "bind_profile": state.deployment.bind_profile,
            "warnings": ["failed to encode typed deployment posture summary"],
        })
    })
}

pub(crate) async fn build_observability_payload(
    state: &AppState,
    context: &gateway::RequestContext,
    provider_snapshot: &crate::model_provider::ProviderStatusSnapshot,
    auth_payload: &Value,
    media_payload: &Value,
) -> Result<Value, Response> {
    let provider_auth =
        build_provider_auth_observability(auth_payload, state.observability.as_ref());
    let operator_insights = super::usage::build_operator_insights_for_context(
        state,
        context,
        provider_snapshot,
        &provider_auth,
    )
    .await?;
    let connector = build_connector_observability(state, media_payload).map_err(|error| *error)?;
    let browser = collect_console_browser_action_diagnostics(state).await;
    let support_bundle = build_support_bundle_observability(state);
    let doctor_recovery = build_doctor_recovery_observability(state);
    let recent_failures = state.observability.recent_failures();
    let failure_classes = build_failure_class_summary(recent_failures.as_slice());
    let runtime_preview = serde_json::to_value(state.observability.runtime_decision_snapshot())
        .unwrap_or_else(|_| json!({ "state": "encode_failed" }));
    let healing_settings = state.runtime.self_healing_settings_snapshot();
    let healing_summary = state.runtime.self_healing_incident_summary();
    let healing_incidents = state.runtime.self_healing_active_incidents(32);
    let healing_history = state.runtime.self_healing_recent_history(64);
    let healing_remediation_attempts = state.runtime.self_healing_recent_remediation_attempts(64);
    let healing_heartbeats = state.runtime.self_healing_heartbeats();

    Ok(json!({
        "failure_classes": failure_classes,
        "provider_auth": provider_auth,
        "operator_insights": operator_insights,
        "config_ref_health": build_config_ref_health_observability(state),
        "dashboard": serde_json::to_value(state.observability.dashboard_mutation_snapshot())
            .unwrap_or_else(|_| json!({})),
        "support_bundle": support_bundle,
        "doctor_recovery": doctor_recovery,
        "connector": connector,
        "lease_manager": state.runtime.provider_lease_snapshot(),
        "browser": browser,
        "runtime_preview": runtime_preview,
        "self_healing": {
            "settings": healing_settings,
            "summary": healing_summary,
            "active_incidents": healing_incidents,
            "recent_history": healing_history,
            "recent_remediation_attempts": healing_remediation_attempts,
            "heartbeats": healing_heartbeats,
        },
        "chat": {
            "active_console_streams": lock_console_chat_streams(&state.console_chat_streams).len(),
        },
        "recent_failures": recent_failures,
        "triage": {
            "failure_classes": ["config_failure", "upstream_provider_failure", "product_failure"],
            "common_order": [
                "Check deployment posture and operator auth first.",
                "Check OpenAI profile health and refresh metrics next.",
                "Check Discord queue depth, dead letters, and upload failures next.",
                "Check browser relay failures and service health next.",
                "If still unresolved, export a support bundle and inspect recent failure correlations."
            ]
        }
    }))
}

pub(crate) fn build_config_ref_health_observability(state: &AppState) -> Value {
    let snapshot = super::secrets::configured_secrets_snapshot(state);
    let reload_state = state.reload_state.lock().unwrap_or_else(|error| error.into_inner());
    let latest_plan = reload_state.latest_plan.clone();
    let recent_events = reload_state.recent_events.iter().take(5).cloned().collect::<Vec<_>>();
    drop(reload_state);

    let config_migration = current_config_migration_observability(state);
    let latest_active_runs = latest_plan.as_ref().map(|plan| plan.active_runs).unwrap_or(0);
    let mut healthy = 0_u64;
    let mut missing = 0_u64;
    let mut blocked = 0_u64;
    let mut failed = 0_u64;
    let mut stale = 0_u64;
    let mut hot_safe_refs = 0_u64;
    let mut restart_required_refs = 0_u64;
    let mut blocked_refs = 0_u64;
    let mut manual_review_refs = 0_u64;
    let mut blocking_refs = 0_u64;
    let mut warning_refs = 0_u64;
    let mut recommendations = std::collections::BTreeSet::<String>::new();

    let items = snapshot
        .secrets
        .iter()
        .map(|entry| {
            match entry.status.as_str() {
                "healthy" => healthy = healthy.saturating_add(1),
                "missing" => missing = missing.saturating_add(1),
                "blocked" => blocked = blocked.saturating_add(1),
                "failed" => failed = failed.saturating_add(1),
                "stale" => stale = stale.saturating_add(1),
                _ => {}
            }
            match entry.reload_action.as_str() {
                "hot_safe" => hot_safe_refs = hot_safe_refs.saturating_add(1),
                "restart_required" => {
                    restart_required_refs = restart_required_refs.saturating_add(1);
                }
                "blocked_while_runs_active" => {
                    blocked_refs = blocked_refs.saturating_add(1);
                }
                "manual_review" => {
                    manual_review_refs = manual_review_refs.saturating_add(1);
                }
                _ => {}
            }

            let (severity, advice) =
                config_ref_item_guidance(entry, latest_active_runs, latest_plan.as_ref());
            match severity {
                "blocking" => blocking_refs = blocking_refs.saturating_add(1),
                "warning" => warning_refs = warning_refs.saturating_add(1),
                _ => {}
            }
            if let Some(advice) = advice.as_ref() {
                recommendations.insert(advice.clone());
            }

            json!({
                "ref_id": entry.secret_id,
                "component": entry.component,
                "config_path": entry.config_path,
                "state": entry.status,
                "severity": severity,
                "reload_mode": entry.reload_action,
                "scope": entry.resolution_scope,
                "source_kind": entry.source.kind,
                "fingerprint": entry.source.fingerprint,
                "required": entry.source.required,
                "refresh_policy": entry.source.refresh_policy,
                "snapshot_policy": entry.source.snapshot_policy,
                "display_name": entry.source.display_name,
                "description": entry.source.description,
                "redaction_label": entry.source.redaction_label,
                "trusted_dir_count": entry.source.trusted_dir_count,
                "inherited_env_count": entry.source.inherited_env_count,
                "allow_symlinks": entry.source.allow_symlinks,
                "exec_timeout_ms": entry.source.exec_timeout_ms,
                "last_checked_at_unix_ms": entry.last_resolved_at_unix_ms,
                "last_error_kind": entry.last_error_kind,
                "last_error": entry.last_error,
                "value_bytes": entry.value_bytes,
                "advice": advice,
                "affected_components": entry.affected_components,
            })
        })
        .collect::<Vec<_>>();

    if let Some(plan) = latest_plan.as_ref() {
        if plan.summary.blocked_while_runs_active > 0 {
            recommendations.insert(
                "Wait for active runs to finish, then rerun the reload plan or apply step."
                    .to_owned(),
            );
        }
        if plan.summary.restart_required > 0 {
            recommendations.insert(
                "Schedule a daemon restart after applying the pending config changes.".to_owned(),
            );
        }
        if plan.summary.manual_review > 0 {
            recommendations.insert(
                "Review safety and routing changes manually before attempting a hot reload."
                    .to_owned(),
            );
        }
    }
    if let Some(advice) = config_migration
        .get("advice")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        recommendations.insert(advice.to_owned());
    }

    let migration_severity =
        config_migration.get("severity").and_then(Value::as_str).unwrap_or("info");
    let overall = if blocking_refs > 0 || migration_severity == "blocking" {
        ("blocking", "blocking")
    } else if warning_refs > 0
        || migration_severity == "warning"
        || latest_plan.as_ref().is_some_and(|plan| {
            plan.summary.restart_required > 0
                || plan.summary.blocked_while_runs_active > 0
                || plan.summary.manual_review > 0
        })
    {
        ("degraded", "warning")
    } else {
        ("ok", "info")
    };

    json!({
        "state": overall.0,
        "severity": overall.1,
        "summary": {
            "total_refs": snapshot.secrets.len(),
            "healthy": healthy,
            "missing": missing,
            "blocked": blocked,
            "failed": failed,
            "stale": stale,
            "hot_safe_refs": hot_safe_refs,
            "restart_required_refs": restart_required_refs,
            "blocked_while_runs_active_refs": blocked_refs,
            "manual_review_refs": manual_review_refs,
            "blocking_refs": blocking_refs,
            "warning_refs": warning_refs,
            "latest_snapshot_generation": snapshot.snapshot_generation,
            "latest_snapshot_at_unix_ms": snapshot.generated_at_unix_ms,
            "active_runs": latest_active_runs,
        },
        "recommendations": recommendations.into_iter().collect::<Vec<_>>(),
        "config_migration": config_migration,
        "latest_plan": latest_plan.as_ref().map(config_ref_reload_plan_observability),
        "recent_events": recent_events
            .iter()
            .map(config_ref_reload_event_observability)
            .collect::<Vec<_>>(),
        "items": items,
    })
}

fn current_config_migration_observability(state: &AppState) -> Value {
    let loaded = state.loaded_config.lock().unwrap_or_else(|error| error.into_inner()).clone();
    let source_path = loaded
        .source
        .split(" +env(")
        .next()
        .map(str::trim)
        .unwrap_or(loaded.source.as_str())
        .to_owned();
    if source_path.eq_ignore_ascii_case("defaults") {
        return json!({
            "state": "defaults_only",
            "severity": "info",
            "source_path": source_path,
            "advice": "Persist a config file before relying on reload planning or migration workflows.",
        });
    }

    let path = PathBuf::from(source_path.as_str());
    let content = match std::fs::read_to_string(path.as_path()) {
        Ok(content) => content,
        Err(error) => {
            return json!({
                "state": "unavailable",
                "severity": "warning",
                "source_path": source_path,
                "error": sanitize_http_error_message(error.to_string().as_str()),
                "advice": "Restore or recreate the active config file before attempting reload planning.",
            });
        }
    };

    match palyra_common::config_system::parse_document_with_migration(content.as_str()) {
        Ok((_document, migration)) => json!({
            "state": if migration.migrated {
                "migration_available"
            } else {
                "current"
            },
            "severity": if migration.migrated { "warning" } else { "info" },
            "source_path": source_path,
            "source_version": migration.source_version,
            "target_version": migration.target_version,
            "requires_writeback": migration.migrated,
            "advice": if migration.migrated {
                "Run `palyra config migrate --path <active-config>` before relying on repeated reload plans."
            } else {
                "The active config document already matches the current schema version."
            },
        }),
        Err(error) => json!({
            "state": "unavailable",
            "severity": "warning",
            "source_path": source_path,
            "error": sanitize_http_error_message(error.to_string().as_str()),
            "advice": "Validate or recover the config document before planning reloads.",
        }),
    }
}

fn config_ref_item_guidance(
    entry: &control_plane::ConfiguredSecretRecord,
    active_runs: u64,
    latest_plan: Option<&control_plane::ConfigReloadPlanEnvelope>,
) -> (&'static str, Option<String>) {
    let required = entry.source.required;
    let source_kind = entry.source.kind.as_str();
    let error_kind = entry.last_error_kind.as_deref().unwrap_or_default();
    match entry.status.as_str() {
        "healthy" => ("info", None),
        "stale" => {
            let advice = match entry.reload_action.as_str() {
                "blocked_while_runs_active" if active_runs > 0 => {
                    "Wait for active runs to finish, then rerun the reload apply step for this config ref."
                }
                "restart_required" => {
                    "Restart the daemon to refresh this config ref in the running runtime."
                }
                "manual_review" => {
                    "Review the latest reload plan before revalidating this config ref."
                }
                _ if latest_plan.is_some() => {
                    "Reapply the latest hot-safe reload plan to refresh this config ref snapshot."
                }
                _ => "Rebuild the runtime snapshot before relying on this config ref.",
            };
            ("warning", Some(advice.to_owned()))
        }
        "missing" => {
            let advice = match source_kind {
                "env" => "Set the missing environment variable or migrate this ref to vault.",
                "file" => {
                    "Create the referenced file inside a trusted directory or update the file ref path."
                }
                "exec" => {
                    "Fix the command so it returns a value within the configured timeout, or move this ref to vault."
                }
                _ => "Write the missing value into the referenced vault scope/key or update the ref.",
            };
            (if required { "blocking" } else { "warning" }, Some(advice.to_owned()))
        }
        "blocked" => {
            let advice = match (source_kind, error_kind) {
                ("file", "policy_blocked") => {
                    "Move the file under a trusted directory or expand trusted_dirs explicitly."
                }
                ("exec", "policy_blocked") => {
                    "Adjust inherited env/cwd policy or move this ref to vault."
                }
                (_, "timeout") => {
                    "Lower command latency or raise exec_timeout_ms only after reviewing the command."
                }
                (_, "invalid_reference") => {
                    "Fix the ref shape and rerun config validation before retrying."
                }
                _ => "Adjust the source policy or move this ref to vault.",
            };
            ("blocking", Some(advice.to_owned()))
        }
        "failed" => {
            let advice = match error_kind {
                "too_large" => "Reduce the returned bytes or tighten max_bytes for this ref.",
                "exec_failed" => {
                    "Inspect the command exit status and stderr, then rerun validation."
                }
                "io" => "Fix filesystem access and confirm the referenced path is readable.",
                "decode_failed" => {
                    "Ensure the source returns valid UTF-8 text for this runtime field."
                }
                "invalid_reference" => {
                    "Fix the ref shape and rerun config validation before retrying."
                }
                "timeout" => {
                    "Lower command latency or raise exec_timeout_ms only after reviewing the command."
                }
                _ => "Repair the source and rerun validation before retrying.",
            };
            ("blocking", Some(advice.to_owned()))
        }
        _ => (
            "warning",
            Some("Inspect and revalidate this config ref before relying on it.".to_owned()),
        ),
    }
}

fn config_ref_reload_plan_observability(plan: &control_plane::ConfigReloadPlanEnvelope) -> Value {
    json!({
        "plan_id": plan.plan_id,
        "generated_at_unix_ms": plan.generated_at_unix_ms,
        "active_runs": plan.active_runs,
        "requires_restart": plan.requires_restart,
        "hot_safe_applicable": plan.hot_safe_applicable,
        "summary": plan.summary,
        "steps": plan.steps,
    })
}

fn config_ref_reload_event_observability(
    event: &control_plane::ConfigReloadApplyEnvelope,
) -> Value {
    let severity = match event.outcome.as_str() {
        "rejected" => "warning",
        "applied_partial" => "warning",
        _ => "info",
    };
    json!({
        "outcome": event.outcome,
        "severity": severity,
        "message": event.message,
        "plan_id": event.plan.plan_id,
        "generated_at_unix_ms": event.plan.generated_at_unix_ms,
        "applied_step_count": event.applied_steps.len(),
        "skipped_step_count": event.skipped_steps.len(),
    })
}

pub(crate) fn build_provider_auth_observability(
    auth_payload: &Value,
    observability: &ObservabilityState,
) -> Value {
    let auth_attempts = observability.provider_auth_snapshot();
    let refresh_failures = auth_payload
        .pointer("/refresh_metrics/failures")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| observability.provider_refresh_failures());
    let total_profiles =
        auth_payload.pointer("/summary/total").and_then(Value::as_u64).unwrap_or(0);
    let expired = auth_payload.pointer("/summary/expired").and_then(Value::as_u64).unwrap_or(0);
    let missing = auth_payload.pointer("/summary/missing").and_then(Value::as_u64).unwrap_or(0);
    json!({
        "attempts": auth_attempts.attempts,
        "failures": auth_attempts.failures,
        "failure_rate_bps": auth_attempts.failure_rate_bps,
        "refresh_failures": refresh_failures,
        "profiles": {
            "total": total_profiles,
            "expired": expired,
            "missing": missing,
        },
        "state": if missing > 0 || expired > 0 || auth_attempts.failures > 0 {
            "degraded"
        } else {
            "ok"
        },
    })
}

pub(crate) fn build_connector_observability(
    state: &AppState,
    media_payload: &Value,
) -> Result<Value, Box<Response>> {
    let connectors =
        state.channels.list().map_err(|error| Box::new(channel_platform_error_response(error)))?;
    let connector_count = connectors.len();
    let mut queue_depth = 0_u64;
    let mut dead_letters = 0_u64;
    let mut paused = 0_u64;
    let mut degraded = 0_u64;
    let mut last_errors = Vec::<Value>::new();
    for connector in connectors {
        let queue = state
            .channels
            .queue_snapshot(connector.connector_id.as_str())
            .map_err(|error| Box::new(channel_platform_error_response(error)))?;
        queue_depth = queue_depth
            .saturating_add(queue.pending_outbox)
            .saturating_add(queue.due_outbox)
            .saturating_add(queue.claimed_outbox);
        dead_letters = dead_letters.saturating_add(queue.dead_letters);
        if queue.paused {
            paused = paused.saturating_add(1);
        }
        if connector.readiness.as_str() != "ready" || connector.liveness.as_str() != "running" {
            degraded = degraded.saturating_add(1);
        }
        if let Some(runtime) = state
            .channels
            .runtime_snapshot(connector.connector_id.as_str())
            .map_err(|error| Box::new(channel_platform_error_response(error)))?
        {
            if let Some(error) = runtime.get("last_error").and_then(Value::as_str) {
                if !error.trim().is_empty() && last_errors.len() < 5 {
                    last_errors.push(json!({
                        "connector_id": connector.connector_id,
                        "message": sanitize_http_error_message(error),
                    }));
                }
            }
        }
    }
    let upload_failures = media_payload
        .get("recent_upload_failures")
        .and_then(Value::as_array)
        .map(|entries| entries.len() as u64)
        .unwrap_or(0);
    let upload_failure_rate_bps = if upload_failures > 0 { 10_000 } else { 0 };
    Ok(json!({
        "connectors": connector_count,
        "degraded_connectors": degraded,
        "paused_connectors": paused,
        "queue_depth": queue_depth,
        "dead_letters": dead_letters,
        "upload_failures": upload_failures,
        "upload_failure_rate_bps": upload_failure_rate_bps,
        "upload_failure_rate_basis": "recent_upload_failures_only",
        "recent_errors": last_errors,
    }))
}

pub(crate) async fn collect_console_browser_action_diagnostics(state: &AppState) -> Value {
    let snapshot = match state.runtime.recent_journal_snapshot(256).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return json!({
                "relay_actions": {
                    "attempts": 0,
                    "failures": 0,
                    "failure_rate_bps": 0,
                },
                "error": sanitize_http_error_message(
                    format!("failed to query browser journal diagnostics: {error}").as_str()
                ),
            });
        }
    };

    let mut attempts = 0_u64;
    let mut failures = 0_u64;
    let mut samples = Vec::<String>::new();
    for event in snapshot.events {
        let Ok(payload) = serde_json::from_str::<Value>(event.payload_json.as_str()) else {
            continue;
        };
        if payload.get("event").and_then(Value::as_str) != Some("browser.relay.action") {
            continue;
        }
        attempts = attempts.saturating_add(1);
        let success = payload.get("success").and_then(Value::as_bool).unwrap_or(false);
        if success {
            continue;
        }
        failures = failures.saturating_add(1);
        if let Some(message) = payload.get("error").and_then(Value::as_str) {
            if !message.trim().is_empty() && samples.len() < 5 {
                samples.push(sanitize_http_error_message(message));
            }
        }
    }

    json!({
        "relay_actions": {
            "attempts": attempts,
            "failures": failures,
            "failure_rate_bps": if attempts == 0 {
                0
            } else {
                u32::try_from(failures.saturating_mul(10_000) / attempts).unwrap_or(u32::MAX)
            },
        },
        "recent_failure_samples": samples,
    })
}

pub(crate) fn build_support_bundle_observability(state: &AppState) -> Value {
    let summary = state.observability.support_bundle_snapshot();
    let runtime_preview = serde_json::to_value(state.observability.runtime_decision_snapshot())
        .unwrap_or_else(|_| json!({ "state": "encode_failed" }));
    let latest_job = lock_support_bundle_jobs(&state.support_bundle_jobs)
        .values()
        .cloned()
        .max_by(|left, right| left.requested_at_unix_ms.cmp(&right.requested_at_unix_ms));
    json!({
        "attempts": summary.attempts,
        "successes": summary.successes,
        "failures": summary.failures,
        "success_rate_bps": if summary.attempts == 0 {
            10_000
        } else {
            10_000_u32.saturating_sub(summary.failure_rate_bps)
        },
        "workspace_restore": build_workspace_restore_observability(state),
        "replay": build_replay_support_observability(),
        "runtime_preview": runtime_preview,
        "last_job": latest_job.map(|job| json!({
            "job_id": job.job_id,
            "state": match job.state {
                control_plane::SupportBundleJobState::Queued => "queued",
                control_plane::SupportBundleJobState::Running => "running",
                control_plane::SupportBundleJobState::Succeeded => "succeeded",
                control_plane::SupportBundleJobState::Failed => "failed",
            },
            "requested_at_unix_ms": job.requested_at_unix_ms,
            "completed_at_unix_ms": job.completed_at_unix_ms,
            "output_path": job.output_path,
            "error": job.error,
        })),
    })
}

fn build_replay_support_observability() -> Value {
    json!({
        "contract": replay_contract_snapshot(),
        "cli_workflows": [
            "support-bundle replay-export",
            "support-bundle replay-import",
            "support-bundle replay-run",
            "support-bundle replay-baseline"
        ],
        "gate_profiles": [
            "scripts/test/run-replay-gate.sh",
            "scripts/test/run-replay-gate.ps1",
            "cli-full-regression replay gate"
        ],
        "incident_workflow": [
            "export a run-scoped bundle from the journal",
            "import only after redaction validation and offline replay pass",
            "run offline replay with an optional diff report during triage",
            "promote verified bundles into a baseline artifact"
        ],
        "reporting": {
            "metrics": [
                "success_rate_bps",
                "unstable_bundle_rate_bps",
                "diff_category_breakdown"
            ],
            "diff_categories": [
                "validation",
                "model",
                "tape",
                "tool",
                "approval",
                "http",
                "auxiliary",
                "flow",
                "artifact"
            ]
        },
        "offline_only": true,
    })
}

fn build_workspace_restore_observability(state: &AppState) -> Value {
    let summary = match state.runtime.journal_store.summarize_workspace_restore_activity(
        &crate::journal::WorkspaceRestoreActivityFilter::default(),
    ) {
        Ok(summary) => summary,
        Err(error) => {
            return json!({
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };
    let recent_checkpoints = match state.runtime.journal_store.list_workspace_checkpoints(
        &crate::journal::WorkspaceCheckpointListFilter {
            session_id: None,
            run_id: None,
            device_id: None,
            limit: Some(5),
        },
    ) {
        Ok(records) => records,
        Err(error) => {
            return json!({
                "summary": summary,
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };
    let recent_restore_reports = match state.runtime.journal_store.list_workspace_restore_reports(
        &crate::journal::WorkspaceRestoreReportListFilter {
            checkpoint_id: None,
            session_id: None,
            run_id: None,
            device_id: None,
            limit: Some(5),
        },
    ) {
        Ok(records) => records,
        Err(error) => {
            return json!({
                "summary": summary,
                "error": sanitize_http_error_message(error.to_string().as_str()),
            });
        }
    };
    let latest_compare_latency_ms = recent_checkpoints
        .iter()
        .filter_map(|checkpoint| {
            serde_json::from_str::<Value>(checkpoint.compare_summary_json.as_str())
                .ok()
                .and_then(|summary| summary.get("compare_latency_ms").and_then(Value::as_u64))
        })
        .next();

    json!({
        "summary": summary,
        "latest_compare_latency_ms": latest_compare_latency_ms,
        "recent_checkpoints": recent_checkpoints
            .into_iter()
            .map(workspace_checkpoint_support_summary)
            .collect::<Vec<_>>(),
        "recent_restore_reports": recent_restore_reports
            .into_iter()
            .map(workspace_restore_report_support_summary)
            .collect::<Vec<_>>(),
    })
}

fn workspace_checkpoint_support_summary(
    checkpoint: crate::journal::WorkspaceCheckpointRecord,
) -> Value {
    json!({
        "checkpoint_id": checkpoint.checkpoint_id,
        "session_id": checkpoint.session_id,
        "run_id": checkpoint.run_id,
        "source_kind": checkpoint.source_kind,
        "source_label": checkpoint.source_label,
        "checkpoint_stage": checkpoint.checkpoint_stage,
        "mutation_id": checkpoint.mutation_id,
        "paired_checkpoint_id": checkpoint.paired_checkpoint_id,
        "tool_name": checkpoint.tool_name,
        "proposal_id": checkpoint.proposal_id,
        "actor_principal": checkpoint.actor_principal,
        "device_id": checkpoint.device_id,
        "channel": checkpoint.channel,
        "summary_text": checkpoint.summary_text,
        "diff_summary": serde_json::from_str::<Value>(checkpoint.diff_summary_json.as_str())
            .unwrap_or_else(|_| Value::String(checkpoint.diff_summary_json)),
        "compare_summary": serde_json::from_str::<Value>(checkpoint.compare_summary_json.as_str())
            .unwrap_or_else(|_| Value::String(checkpoint.compare_summary_json)),
        "risk_level": checkpoint.risk_level,
        "review_posture": checkpoint.review_posture,
        "created_at_unix_ms": checkpoint.created_at_unix_ms,
        "restore_count": checkpoint.restore_count,
        "last_restored_at_unix_ms": checkpoint.last_restored_at_unix_ms,
        "latest_restore_report_id": checkpoint.latest_restore_report_id,
    })
}

fn workspace_restore_report_support_summary(
    report: crate::journal::WorkspaceRestoreReportRecord,
) -> Value {
    let restored_path_count =
        serde_json::from_str::<Vec<String>>(report.restored_paths_json.as_str())
            .map(|entries| entries.len())
            .unwrap_or_default();
    let failed_path_count = serde_json::from_str::<Vec<Value>>(report.failed_paths_json.as_str())
        .map(|entries| entries.len())
        .unwrap_or_default();
    json!({
        "report_id": report.report_id,
        "checkpoint_id": report.checkpoint_id,
        "session_id": report.session_id,
        "run_id": report.run_id,
        "actor_principal": report.actor_principal,
        "device_id": report.device_id,
        "channel": report.channel,
        "scope_kind": report.scope_kind,
        "target_path": report.target_path,
        "restored_path_count": restored_path_count,
        "failed_path_count": failed_path_count,
        "reconciliation_summary": report.reconciliation_summary,
        "reconciliation_prompt": report.reconciliation_prompt,
        "branched_session_id": report.branched_session_id,
        "result_state": report.result_state,
        "created_at_unix_ms": report.created_at_unix_ms,
    })
}

pub(crate) fn build_doctor_recovery_observability(state: &AppState) -> Value {
    let jobs = lock_doctor_jobs(&state.doctor_jobs);
    let mut queued = 0_u64;
    let mut running = 0_u64;
    let mut succeeded = 0_u64;
    let mut failed = 0_u64;
    let latest_job = jobs
        .values()
        .cloned()
        .inspect(|job| match job.state {
            control_plane::DoctorRecoveryJobState::Queued => queued = queued.saturating_add(1),
            control_plane::DoctorRecoveryJobState::Running => running = running.saturating_add(1),
            control_plane::DoctorRecoveryJobState::Succeeded => {
                succeeded = succeeded.saturating_add(1);
            }
            control_plane::DoctorRecoveryJobState::Failed => failed = failed.saturating_add(1),
        })
        .max_by(|left, right| left.requested_at_unix_ms.cmp(&right.requested_at_unix_ms));
    json!({
        "queued": queued,
        "running": running,
        "succeeded": succeeded,
        "failed": failed,
        "last_job": latest_job.map(|job| build_doctor_recovery_job_summary(&job)),
    })
}

fn build_doctor_recovery_job_summary(job: &control_plane::DoctorRecoveryJob) -> Value {
    let report = job.report.as_ref();
    let recovery = report.and_then(|value| value.get("recovery"));
    let planned_step_count = recovery
        .and_then(|value| value.get("planned_steps"))
        .and_then(Value::as_array)
        .map(Vec::len);
    let applied_step_count = recovery
        .and_then(|value| value.get("applied_steps"))
        .and_then(Value::as_array)
        .map(Vec::len);
    let available_run_count = recovery
        .and_then(|value| value.get("available_runs"))
        .and_then(Value::as_array)
        .map(Vec::len);
    json!({
        "job_id": job.job_id,
        "state": match job.state {
            control_plane::DoctorRecoveryJobState::Queued => "queued",
            control_plane::DoctorRecoveryJobState::Running => "running",
            control_plane::DoctorRecoveryJobState::Succeeded => "succeeded",
            control_plane::DoctorRecoveryJobState::Failed => "failed",
        },
        "requested_at_unix_ms": job.requested_at_unix_ms,
        "started_at_unix_ms": job.started_at_unix_ms,
        "completed_at_unix_ms": job.completed_at_unix_ms,
        "command": job.command.clone(),
        "mode": report
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "requested": recovery
            .and_then(|value| value.get("requested"))
            .and_then(Value::as_bool),
        "dry_run": recovery
            .and_then(|value| value.get("dry_run"))
            .and_then(Value::as_bool),
        "force": recovery
            .and_then(|value| value.get("force"))
            .and_then(Value::as_bool),
        "rollback_run": recovery
            .and_then(|value| value.get("rollback_run"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "run_id": recovery
            .and_then(|value| value.get("run_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "backup_manifest_path": recovery
            .and_then(|value| value.get("backup_manifest_path"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        "planned_step_count": planned_step_count,
        "applied_step_count": applied_step_count,
        "available_run_count": available_run_count,
        "next_steps": recovery
            .and_then(|value| value.get("next_steps"))
            .cloned()
            .unwrap_or_else(|| json!([])),
        "error": job.error.clone(),
    })
}

pub(crate) fn build_failure_class_summary(failures: &[observability::FailureSnapshot]) -> Value {
    let mut config = 0_u64;
    let mut upstream = 0_u64;
    let mut product = 0_u64;
    for failure in failures {
        match failure.failure_class {
            FailureClass::Config => config = config.saturating_add(1),
            FailureClass::UpstreamProvider => upstream = upstream.saturating_add(1),
            FailureClass::Product => product = product.saturating_add(1),
        }
    }
    json!({
        "config_failure": config,
        "upstream_provider_failure": upstream,
        "product_failure": product,
    })
}

pub(crate) fn auth_correlation_from_context(
    context: &RequestContext,
    auth_profile_id: Option<&str>,
    run_id: Option<&str>,
    approval_id: Option<&str>,
    envelope_id: Option<&str>,
) -> ObservabilityCorrelationSnapshot {
    let _ = context;
    ObservabilityCorrelationSnapshot {
        session_id: None,
        run_id: run_id.map(ToOwned::to_owned),
        approval_id: approval_id.map(ToOwned::to_owned),
        envelope_id: envelope_id.map(ToOwned::to_owned),
        auth_profile_id: auth_profile_id.map(ToOwned::to_owned),
        onboarding_flow_id: None,
        browser_session_id: None,
    }
}

pub(crate) fn record_provider_auth_failure(
    state: &AppState,
    operation: &str,
    status: StatusCode,
    correlation: ObservabilityCorrelationSnapshot,
    refresh_failure: bool,
) {
    let message = format!("provider auth request failed with http {}", status.as_u16());
    let failure_class = classify_console_mutation_failure(status);
    let observed_at_unix_ms = unix_ms_now().unwrap_or_default();
    state.observability.record_provider_auth_failure(
        operation,
        failure_class,
        message.clone(),
        observed_at_unix_ms,
        correlation.clone(),
    );
    if refresh_failure {
        state.observability.record_provider_refresh_failure(
            operation,
            failure_class,
            message,
            observed_at_unix_ms,
            correlation,
        );
    }
}

pub(crate) fn classify_console_mutation_failure(status: StatusCode) -> FailureClass {
    match status {
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => {
            FailureClass::UpstreamProvider
        }
        status if status.is_client_error() => FailureClass::Config,
        _ => FailureClass::Product,
    }
}

pub(crate) fn contract_descriptor() -> control_plane::ContractDescriptor {
    control_plane::ContractDescriptor {
        contract_version: control_plane::CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
    }
}

pub(crate) fn build_page_info(
    limit: usize,
    returned: usize,
    next_cursor: Option<String>,
) -> control_plane::PageInfo {
    control_plane::PageInfo { limit, returned, has_more: next_cursor.is_some(), next_cursor }
}

pub(crate) fn build_deployment_posture_summary(
    state: &AppState,
) -> control_plane::DeploymentPostureSummary {
    let last_remote_admin_access =
        transport::http::middleware::lock_remote_admin_access(&state.remote_admin_access)
            .as_ref()
            .cloned()
            .map(|attempt| control_plane::RemoteAdminAccessAttempt {
                observed_at_unix_ms: attempt.observed_at_unix_ms,
                remote_ip_fingerprint: attempt.remote_ip_fingerprint,
                method: attempt.method,
                path: attempt.path,
                status_code: attempt.status_code,
                outcome: attempt.outcome,
            });
    let admin_remote = !state
        .deployment
        .admin_bind_addr
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false);
    let grpc_remote = !state
        .deployment
        .grpc_bind_addr
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false);
    let quic_remote = state.deployment.quic_enabled
        && !state
            .deployment
            .quic_bind_addr
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);

    let mut warnings = Vec::<String>::new();
    if !state.deployment.gateway_tls_enabled && (admin_remote || grpc_remote || quic_remote) {
        warnings.push("Remote bind without TLS blocked".to_owned());
    }
    if admin_remote || grpc_remote || quic_remote {
        warnings.push("Dashboard exposed publicly; ensure WAF/reverse proxy".to_owned());
    }

    control_plane::DeploymentPostureSummary {
        contract: contract_descriptor(),
        mode: state.deployment.mode.clone(),
        bind_profile: state.deployment.bind_profile.clone(),
        bind_addresses: control_plane::DeploymentBindAddresses {
            admin: format!("{}:{}", state.deployment.admin_bind_addr, state.deployment.admin_port),
            grpc: format!("{}:{}", state.deployment.grpc_bind_addr, state.deployment.grpc_port),
            quic: if state.deployment.quic_enabled {
                format!("{}:{}", state.deployment.quic_bind_addr, state.deployment.quic_port)
            } else {
                "disabled".to_owned()
            },
        },
        tls: control_plane::DeploymentTlsSummary {
            gateway_enabled: state.deployment.gateway_tls_enabled,
        },
        admin_auth_required: state.deployment.admin_auth_required,
        dangerous_remote_bind_ack: control_plane::DangerousRemoteBindAckSummary {
            config: state.deployment.dangerous_remote_bind_ack_config,
            env: state.deployment.dangerous_remote_bind_ack_env,
            env_name: DANGEROUS_REMOTE_BIND_ACK_ENV.to_owned(),
        },
        remote_bind_detected: admin_remote || grpc_remote || quic_remote,
        last_remote_admin_access_attempt: last_remote_admin_access,
        warnings,
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn parse_console_auth_provider_kind(
    raw: Option<&str>,
) -> gateway::proto::palyra::auth::v1::AuthProviderKind {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "openai" => gateway::proto::palyra::auth::v1::AuthProviderKind::Openai,
            "anthropic" => gateway::proto::palyra::auth::v1::AuthProviderKind::Anthropic,
            "telegram" => gateway::proto::palyra::auth::v1::AuthProviderKind::Telegram,
            "slack" => gateway::proto::palyra::auth::v1::AuthProviderKind::Slack,
            "discord" => gateway::proto::palyra::auth::v1::AuthProviderKind::Discord,
            "webhook" => gateway::proto::palyra::auth::v1::AuthProviderKind::Webhook,
            "custom" => gateway::proto::palyra::auth::v1::AuthProviderKind::Custom,
            _ => gateway::proto::palyra::auth::v1::AuthProviderKind::Unspecified,
        },
        None => gateway::proto::palyra::auth::v1::AuthProviderKind::Unspecified,
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn parse_console_auth_scope_kind(
    raw: Option<&str>,
) -> gateway::proto::palyra::auth::v1::AuthScopeKind {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "global" => gateway::proto::palyra::auth::v1::AuthScopeKind::Global,
            "agent" => gateway::proto::palyra::auth::v1::AuthScopeKind::Agent,
            _ => gateway::proto::palyra::auth::v1::AuthScopeKind::Unspecified,
        },
        None => gateway::proto::palyra::auth::v1::AuthScopeKind::Unspecified,
    }
}

pub(crate) fn build_console_auth_service(state: &AppState) -> gateway::AuthServiceImpl {
    gateway::AuthServiceImpl::new(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        Arc::clone(&state.auth_runtime),
    )
}

pub(crate) fn build_console_vault_service(state: &AppState) -> gateway::VaultServiceImpl {
    gateway::VaultServiceImpl::new(Arc::clone(&state.runtime), state.auth.clone())
}

#[allow(clippy::result_large_err)]
pub(crate) fn control_plane_auth_profile_from_proto(
    profile: &gateway::proto::palyra::auth::v1::AuthProfile,
) -> Result<control_plane::AuthProfileView, Response> {
    let provider = profile.provider.as_ref().ok_or_else(|| {
        runtime_status_response(tonic::Status::internal("auth profile missing provider"))
    })?;
    let scope = profile.scope.as_ref().ok_or_else(|| {
        runtime_status_response(tonic::Status::internal("auth profile missing scope"))
    })?;
    let credential = profile.credential.as_ref().ok_or_else(|| {
        runtime_status_response(tonic::Status::internal("auth profile missing credential"))
    })?;
    let credential = match credential.kind.as_ref() {
        Some(gateway::proto::palyra::auth::v1::auth_credential::Kind::ApiKey(api_key)) => {
            control_plane::AuthCredentialView::ApiKey {
                api_key_vault_ref: api_key.api_key_vault_ref.clone(),
            }
        }
        Some(gateway::proto::palyra::auth::v1::auth_credential::Kind::Oauth(oauth)) => {
            control_plane::AuthCredentialView::Oauth {
                access_token_vault_ref: oauth.access_token_vault_ref.clone(),
                refresh_token_vault_ref: oauth.refresh_token_vault_ref.clone(),
                token_endpoint: oauth.token_endpoint.clone(),
                client_id: trim_to_option(oauth.client_id.clone()),
                client_secret_vault_ref: trim_to_option(oauth.client_secret_vault_ref.clone()),
                scopes: oauth.scopes.clone(),
                expires_at_unix_ms: if oauth.expires_at_unix_ms > 0 {
                    Some(oauth.expires_at_unix_ms)
                } else {
                    None
                },
                refresh_state: oauth
                    .refresh_state
                    .as_ref()
                    .map(auth_oauth_refresh_state_json)
                    .unwrap_or_else(|| json!({})),
            }
        }
        None => {
            return Err(runtime_status_response(tonic::Status::internal(
                "auth profile credential kind is missing",
            )))
        }
    };
    Ok(control_plane::AuthProfileView {
        profile_id: profile.profile_id.clone(),
        provider: control_plane::AuthProfileProvider {
            kind: auth_provider_kind_to_text(provider.kind).to_owned(),
            custom_name: trim_to_option(provider.custom_name.clone()),
        },
        profile_name: profile.profile_name.clone(),
        scope: control_plane::AuthProfileScope {
            kind: auth_scope_kind_to_text(scope.kind).to_owned(),
            agent_id: trim_to_option(scope.agent_id.clone()),
        },
        credential,
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn control_plane_auth_profile_to_proto(
    profile: &control_plane::AuthProfileView,
) -> Result<gateway::proto::palyra::auth::v1::AuthProfile, Response> {
    let provider_kind = auth_provider_kind_from_text(profile.provider.kind.as_str())?;
    let scope_kind = auth_scope_kind_from_text(profile.scope.kind.as_str())?;
    let credential = match &profile.credential {
        control_plane::AuthCredentialView::ApiKey { api_key_vault_ref } => {
            gateway::proto::palyra::auth::v1::AuthCredential {
                kind: Some(gateway::proto::palyra::auth::v1::auth_credential::Kind::ApiKey(
                    gateway::proto::palyra::auth::v1::ApiKeyCredential {
                        api_key_vault_ref: api_key_vault_ref.clone(),
                    },
                )),
            }
        }
        control_plane::AuthCredentialView::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => gateway::proto::palyra::auth::v1::AuthCredential {
            kind: Some(gateway::proto::palyra::auth::v1::auth_credential::Kind::Oauth(
                gateway::proto::palyra::auth::v1::OAuthCredential {
                    access_token_vault_ref: access_token_vault_ref.clone(),
                    refresh_token_vault_ref: refresh_token_vault_ref.clone(),
                    token_endpoint: token_endpoint.clone(),
                    client_id: client_id.clone().unwrap_or_default(),
                    client_secret_vault_ref: client_secret_vault_ref.clone().unwrap_or_default(),
                    scopes: scopes.clone(),
                    expires_at_unix_ms: expires_at_unix_ms.unwrap_or_default(),
                    refresh_state: Some(auth_oauth_refresh_state_from_json(refresh_state)?),
                },
            )),
        },
    };
    Ok(gateway::proto::palyra::auth::v1::AuthProfile {
        profile_id: profile.profile_id.clone(),
        provider: Some(gateway::proto::palyra::auth::v1::AuthProvider {
            kind: provider_kind as i32,
            custom_name: profile.provider.custom_name.clone().unwrap_or_default(),
        }),
        profile_name: profile.profile_name.clone(),
        scope: Some(gateway::proto::palyra::auth::v1::AuthScope {
            kind: scope_kind as i32,
            agent_id: profile.scope.agent_id.clone().unwrap_or_default(),
        }),
        credential: Some(credential),
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
    })
}

pub(crate) fn auth_provider_kind_to_text(value: i32) -> &'static str {
    match gateway::proto::palyra::auth::v1::AuthProviderKind::try_from(value)
        .unwrap_or(gateway::proto::palyra::auth::v1::AuthProviderKind::Unspecified)
    {
        gateway::proto::palyra::auth::v1::AuthProviderKind::Openai => "openai",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Anthropic => "anthropic",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Telegram => "telegram",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Slack => "slack",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Discord => "discord",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Webhook => "webhook",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Custom => "custom",
        gateway::proto::palyra::auth::v1::AuthProviderKind::Unspecified => "unspecified",
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn auth_provider_kind_from_text(
    raw: &str,
) -> Result<gateway::proto::palyra::auth::v1::AuthProviderKind, Response> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "openai" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Openai),
        "anthropic" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Anthropic),
        "telegram" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Telegram),
        "slack" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Slack),
        "discord" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Discord),
        "webhook" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Webhook),
        "custom" => Ok(gateway::proto::palyra::auth::v1::AuthProviderKind::Custom),
        _ => Err(validation_error_response(
            "provider.kind",
            "invalid_enum",
            "provider.kind must be one of openai|anthropic|telegram|slack|discord|webhook|custom",
        )),
    }
}

pub(crate) fn auth_scope_kind_to_text(value: i32) -> &'static str {
    match gateway::proto::palyra::auth::v1::AuthScopeKind::try_from(value)
        .unwrap_or(gateway::proto::palyra::auth::v1::AuthScopeKind::Unspecified)
    {
        gateway::proto::palyra::auth::v1::AuthScopeKind::Global => "global",
        gateway::proto::palyra::auth::v1::AuthScopeKind::Agent => "agent",
        gateway::proto::palyra::auth::v1::AuthScopeKind::Unspecified => "unspecified",
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn auth_scope_kind_from_text(
    raw: &str,
) -> Result<gateway::proto::palyra::auth::v1::AuthScopeKind, Response> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "global" => Ok(gateway::proto::palyra::auth::v1::AuthScopeKind::Global),
        "agent" => Ok(gateway::proto::palyra::auth::v1::AuthScopeKind::Agent),
        _ => Err(validation_error_response(
            "scope.kind",
            "invalid_enum",
            "scope.kind must be one of global|agent",
        )),
    }
}

pub(crate) fn auth_oauth_refresh_state_json(
    refresh_state: &gateway::proto::palyra::auth::v1::OAuthRefreshState,
) -> Value {
    json!({
        "failure_count": refresh_state.failure_count,
        "last_error": refresh_state.last_error,
        "last_attempt_unix_ms": refresh_state.last_attempt_unix_ms,
        "last_success_unix_ms": refresh_state.last_success_unix_ms,
        "next_allowed_refresh_unix_ms": refresh_state.next_allowed_refresh_unix_ms,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn auth_oauth_refresh_state_from_json(
    refresh_state: &Value,
) -> Result<gateway::proto::palyra::auth::v1::OAuthRefreshState, Response> {
    Ok(gateway::proto::palyra::auth::v1::OAuthRefreshState {
        failure_count: refresh_state
            .get("failure_count")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_default(),
        last_error: refresh_state
            .get("last_error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        last_attempt_unix_ms: refresh_state
            .get("last_attempt_unix_ms")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        last_success_unix_ms: refresh_state
            .get("last_success_unix_ms")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        next_allowed_refresh_unix_ms: refresh_state
            .get("next_allowed_refresh_unix_ms")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
    })
}

pub(crate) fn auth_health_summary_json(
    summary: Option<&gateway::proto::palyra::auth::v1::AuthHealthSummary>,
) -> Value {
    summary
        .map(|summary| {
            json!({
                "total": summary.total,
                "ok": summary.ok,
                "expiring": summary.expiring,
                "expired": summary.expired,
                "missing": summary.missing,
                "static_count": summary.static_count,
            })
        })
        .unwrap_or_else(|| json!({}))
}

pub(crate) fn auth_expiry_distribution_json(
    summary: Option<&gateway::proto::palyra::auth::v1::AuthExpiryDistribution>,
) -> Value {
    summary
        .map(|summary| {
            json!({
                "expired": summary.expired,
                "under_5m": summary.under_5m,
                "between_5m_15m": summary.between_5m_15m,
                "between_15m_60m": summary.between_15m_60m,
                "between_1h_24h": summary.between_1h_24h,
                "over_24h": summary.over_24h,
                "unknown": summary.unknown,
                "static_count": summary.static_count,
                "missing": summary.missing,
            })
        })
        .unwrap_or_else(|| json!({}))
}

pub(crate) fn auth_profile_health_json(
    profile: &gateway::proto::palyra::auth::v1::AuthProfileHealth,
) -> Value {
    json!({
        "profile_id": profile.profile_id,
        "provider": profile.provider,
        "profile_name": profile.profile_name,
        "scope": profile.scope,
        "credential_type": profile.credential_type,
        "state": match gateway::proto::palyra::auth::v1::AuthHealthState::try_from(profile.state)
            .unwrap_or(gateway::proto::palyra::auth::v1::AuthHealthState::Unspecified)
        {
            gateway::proto::palyra::auth::v1::AuthHealthState::Ok => "ok",
            gateway::proto::palyra::auth::v1::AuthHealthState::Expiring => "expiring",
            gateway::proto::palyra::auth::v1::AuthHealthState::Expired => "expired",
            gateway::proto::palyra::auth::v1::AuthHealthState::Missing => "missing",
            gateway::proto::palyra::auth::v1::AuthHealthState::Static => "static",
            gateway::proto::palyra::auth::v1::AuthHealthState::Unspecified => "unspecified",
        },
        "reason": profile.reason,
        "expires_at_unix_ms": if profile.expires_at_unix_ms > 0 {
            Value::from(profile.expires_at_unix_ms)
        } else {
            Value::Null
        },
    })
}

pub(crate) fn auth_refresh_metrics_json(
    metrics: Option<&gateway::proto::palyra::auth::v1::AuthRefreshMetrics>,
) -> Value {
    metrics
        .map(|metrics| {
            json!({
                "attempts": metrics.attempts,
                "successes": metrics.successes,
                "failures": metrics.failures,
                "by_provider": metrics.by_provider.iter().map(|entry| json!({
                    "provider": entry.provider,
                    "attempts": entry.attempts,
                    "successes": entry.successes,
                    "failures": entry.failures,
                })).collect::<Vec<_>>(),
            })
        })
        .unwrap_or_else(|| json!({}))
}

pub(crate) async fn list_console_auth_profiles(
    state: &AppState,
    session: &ConsoleSession,
    provider_kind: gateway::proto::palyra::auth::v1::AuthProviderKind,
) -> Result<Vec<control_plane::AuthProfileView>, Response> {
    let mut request =
        TonicRequest::new(gateway::proto::palyra::auth::v1::ListAuthProfilesRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            after_profile_id: String::new(),
            limit: 256,
            provider_kind: provider_kind as i32,
            provider_custom_name: String::new(),
            scope_kind: gateway::proto::palyra::auth::v1::AuthScopeKind::Unspecified as i32,
            scope_agent_id: String::new(),
        });
    apply_console_rpc_context(state, session, request.metadata_mut())?;
    let service = build_console_auth_service(state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::list_profiles(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    response.profiles.iter().map(control_plane_auth_profile_from_proto).collect()
}

pub(crate) fn provider_action_envelope(
    provider: &str,
    action: &str,
    state: &str,
    message: &str,
    profile_id: Option<String>,
) -> control_plane::ProviderAuthActionEnvelope {
    control_plane::ProviderAuthActionEnvelope {
        contract: contract_descriptor(),
        provider: provider.to_owned(),
        action: action.to_owned(),
        state: state.to_owned(),
        message: message.to_owned(),
        profile_id,
    }
}

pub(crate) fn openai_provider_action_envelope(
    action: &str,
    state: &str,
    message: &str,
    profile_id: Option<String>,
) -> control_plane::ProviderAuthActionEnvelope {
    provider_action_envelope("openai", action, state, message, profile_id)
}

fn provider_selection_matches(
    document: &toml::Value,
    provider: ModelProviderAuthProviderKind,
) -> bool {
    let selected_profile_id = get_value_at_path(document, "model_provider.auth_profile_id")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if selected_profile_id.is_none() {
        return false;
    }

    let configured_provider = get_value_at_path(document, "model_provider.auth_provider_kind")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .and_then(|value| ModelProviderAuthProviderKind::parse(value).ok())
        .or_else(|| {
            get_value_at_path(document, "model_provider.kind")
                .ok()
                .and_then(|value| value.and_then(toml::Value::as_str))
                .and_then(|value| ModelProviderKind::parse(value).ok())
                .and_then(|kind| match kind {
                    ModelProviderKind::OpenAiCompatible => {
                        Some(ModelProviderAuthProviderKind::Openai)
                    }
                    ModelProviderKind::Anthropic => Some(ModelProviderAuthProviderKind::Anthropic),
                    ModelProviderKind::Deterministic => None,
                })
        });
    configured_provider == Some(provider)
}

pub(crate) fn build_provider_state(
    document: &toml::Value,
    profiles: Vec<control_plane::AuthProfileView>,
    provider: ModelProviderAuthProviderKind,
) -> control_plane::ProviderAuthStateEnvelope {
    let selected_profile_id = get_value_at_path(document, "model_provider.auth_profile_id")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str).map(str::to_owned));
    let default_profile_id =
        if provider_selection_matches(document, provider) { selected_profile_id } else { None };
    let (provider_name, api_key_path, inline_key_path, oauth_supported) = match provider {
        ModelProviderAuthProviderKind::Openai => (
            "openai",
            "model_provider.openai_api_key_vault_ref",
            "model_provider.openai_api_key",
            true,
        ),
        ModelProviderAuthProviderKind::Anthropic => (
            "anthropic",
            "model_provider.anthropic_api_key_vault_ref",
            "model_provider.anthropic_api_key",
            false,
        ),
    };
    let api_key_vault_ref = get_value_at_path(document, api_key_path)
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str).map(str::to_owned))
        .filter(|value| !value.trim().is_empty());
    let api_key_inline = get_value_at_path(document, inline_key_path)
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str).map(str::to_owned))
        .filter(|value| !value.trim().is_empty());
    let state = if default_profile_id.is_some() {
        "selected_profile"
    } else if api_key_vault_ref.is_some() || api_key_inline.is_some() {
        "api_key_configured"
    } else {
        "not_configured"
    };
    let note = match provider {
        ModelProviderAuthProviderKind::Openai => {
            if api_key_inline.is_some() {
                Some("Inline API keys remain supported for backward compatibility, but operator auth flows should prefer auth profiles or vault refs.".to_owned())
            } else if api_key_vault_ref.is_some() {
                Some("Vault-backed API key is configured; selecting an auth profile will supersede direct API-key usage.".to_owned())
            } else {
                None
            }
        }
        ModelProviderAuthProviderKind::Anthropic => {
            if api_key_inline.is_some() {
                Some("Inline Anthropic API keys remain supported for backward compatibility, but operator auth flows should prefer auth profiles or vault refs.".to_owned())
            } else if api_key_vault_ref.is_some() {
                Some("Vault-backed Anthropic API key is configured; selecting an auth profile will supersede direct API-key usage.".to_owned())
            } else {
                None
            }
        }
    };
    control_plane::ProviderAuthStateEnvelope {
        contract: contract_descriptor(),
        provider: provider_name.to_owned(),
        oauth_supported,
        bootstrap_supported: oauth_supported,
        callback_supported: oauth_supported,
        reconnect_supported: oauth_supported,
        revoke_supported: true,
        default_selection_supported: true,
        default_profile_id,
        available_profile_ids: profiles.into_iter().map(|profile| profile.profile_id).collect(),
        state: state.to_owned(),
        note,
    }
}

pub(crate) fn build_openai_provider_state(
    document: &toml::Value,
    profiles: Vec<control_plane::AuthProfileView>,
) -> control_plane::ProviderAuthStateEnvelope {
    build_provider_state(document, profiles, ModelProviderAuthProviderKind::Openai)
}

#[allow(clippy::result_large_err)]
pub(crate) fn load_console_document_from_existing_path(
    path: &FsPath,
) -> Result<(toml::Value, ConfigMigrationInfo), Response> {
    let content = fs::read_to_string(path).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read {}: {error}",
            path.display()
        )))
    })?;
    parse_document_with_migration(content.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to migrate config document {}: {error}",
            path.display()
        )))
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn load_console_document_for_mutation(
    path: &FsPath,
) -> Result<(toml::Value, ConfigMigrationInfo), Response> {
    if path.exists() {
        return load_console_document_from_existing_path(path);
    }
    parse_document_with_migration("").map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to initialize empty config document: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn resolve_console_config_path(
    path: Option<&str>,
    require_existing: bool,
) -> Result<Option<String>, Response> {
    let resolved = match path.map(str::trim).filter(|value| !value.is_empty()) {
        Some(explicit) => {
            let parsed = parse_config_path(explicit).map_err(|error| {
                runtime_status_response(tonic::Status::invalid_argument(format!(
                    "config path is invalid: {error}"
                )))
            })?;
            Some(parsed.to_string_lossy().into_owned())
        }
        None => {
            if let Ok(path_raw) = std::env::var("PALYRA_CONFIG") {
                let parsed = parse_config_path(path_raw.as_str()).map_err(|error| {
                    runtime_status_response(tonic::Status::invalid_argument(format!(
                        "PALYRA_CONFIG contains an invalid config path: {error}"
                    )))
                })?;
                Some(parsed.to_string_lossy().into_owned())
            } else {
                default_config_search_paths()
                    .into_iter()
                    .find(|candidate| candidate.exists())
                    .map(|candidate| candidate.to_string_lossy().into_owned())
            }
        }
    };

    if require_existing {
        if let Some(path) = resolved.as_deref() {
            if !FsPath::new(path).exists() {
                return Err(runtime_status_response(tonic::Status::not_found(format!(
                    "config file does not exist: {path}"
                ))));
            }
        }
    }

    Ok(resolved)
}

#[allow(clippy::result_large_err)]
pub(crate) fn load_console_config_snapshot(
    path: Option<&str>,
    allow_defaults: bool,
) -> Result<(toml::Value, ConfigMigrationInfo, String), Response> {
    match resolve_console_config_path(path, false)? {
        Some(path) => {
            let path_ref = FsPath::new(path.as_str());
            let (document, migration) = load_console_document_from_existing_path(path_ref)?;
            Ok((document, migration, path))
        }
        None if allow_defaults => {
            let (document, migration) = parse_document_with_migration("").map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to load default config snapshot: {error}"
                )))
            })?;
            Ok((document, migration, "defaults".to_owned()))
        }
        None => {
            Err(runtime_status_response(tonic::Status::not_found("no daemon config file found")))
        }
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn config_backup_records(
    path: Option<&str>,
    backups: usize,
    require_existing: bool,
) -> Result<Vec<control_plane::ConfigBackupRecord>, Response> {
    let Some(path) = path.filter(|value| *value != "defaults") else {
        return Ok(Vec::new());
    };
    if require_existing && !FsPath::new(path).exists() {
        return Err(runtime_status_response(tonic::Status::not_found(format!(
            "config file does not exist: {path}"
        ))));
    }
    let path_ref = FsPath::new(path);
    Ok((1..=backups)
        .map(|index| {
            let backup = backup_path(path_ref, index);
            control_plane::ConfigBackupRecord {
                index,
                path: backup.to_string_lossy().into_owned(),
                exists: backup.exists(),
            }
        })
        .collect())
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_daemon_compatible_document(document: &toml::Value) -> Result<(), Response> {
    let content = toml::to_string(document).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize daemon config document: {error}"
        )))
    })?;
    let parsed: RootFileConfig = toml::from_str(&content).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid daemon config schema: {error}"
        )))
    })?;
    let bind_addr = parsed
        .daemon
        .as_ref()
        .and_then(|daemon| daemon.bind_addr.as_deref())
        .unwrap_or("127.0.0.1");
    let port = parsed.daemon.as_ref().and_then(|daemon| daemon.port).unwrap_or(7142);
    let _ = parse_daemon_bind_socket(bind_addr, port).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid daemon bind address or port: {error}"
        )))
    })?;

    let grpc_bind_addr = parsed
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.grpc_bind_addr.as_deref())
        .unwrap_or("127.0.0.1");
    let grpc_port = parsed.gateway.as_ref().and_then(|gateway| gateway.grpc_port).unwrap_or(7443);
    let _ = parse_daemon_bind_socket(grpc_bind_addr, grpc_port).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid gateway gRPC bind address or port: {error}"
        )))
    })?;

    let quic_enabled =
        parsed.gateway.as_ref().and_then(|gateway| gateway.quic_enabled).unwrap_or(true);
    if quic_enabled {
        let quic_bind_addr = parsed
            .gateway
            .as_ref()
            .and_then(|gateway| gateway.quic_bind_addr.as_deref())
            .unwrap_or("127.0.0.1");
        let quic_port =
            parsed.gateway.as_ref().and_then(|gateway| gateway.quic_port).unwrap_or(7444);
        let _ = parse_daemon_bind_socket(quic_bind_addr, quic_port).map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "invalid gateway QUIC bind address or port: {error}"
            )))
        })?;
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) fn read_console_config_profile_id(path: &str) -> Result<Option<String>, Response> {
    let (document, _) = load_console_document_from_existing_path(FsPath::new(path))?;
    Ok(get_value_at_path(&document, "model_provider.auth_profile_id")
        .map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "invalid config key path for model_provider.auth_profile_id: {error}"
            )))
        })?
        .and_then(toml::Value::as_str)
        .map(str::to_owned))
}

pub(crate) async fn secret_metadata_from_runtime(
    state: &AppState,
    session: &ConsoleSession,
    scope: &str,
    key: &str,
) -> Result<control_plane::SecretMetadata, Response> {
    if scope.trim().is_empty() {
        return Err(validation_error_response("scope", "required", "scope is required"));
    }
    if key.trim().is_empty() {
        return Err(validation_error_response("key", "required", "key is required"));
    }
    let mut request = TonicRequest::new(gateway::proto::palyra::gateway::v1::ListSecretsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        scope: scope.to_owned(),
    });
    apply_console_rpc_context(state, session, request.metadata_mut())?;
    let service = build_console_vault_service(state);
    let response =
        <gateway::VaultServiceImpl as gateway::proto::palyra::gateway::v1::vault_service_server::VaultService>::list_secrets(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    response
        .secrets
        .iter()
        .find(|secret| secret.key == key)
        .map(control_plane_secret_metadata_from_proto)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("secret metadata not found"))
        })
}

pub(crate) fn control_plane_secret_metadata_from_proto(
    secret: &gateway::proto::palyra::gateway::v1::VaultSecretMetadata,
) -> control_plane::SecretMetadata {
    control_plane::SecretMetadata {
        scope: secret.scope.clone(),
        key: secret.key.clone(),
        created_at_unix_ms: secret.created_at_unix_ms,
        updated_at_unix_ms: secret.updated_at_unix_ms,
        value_bytes: secret.value_bytes,
    }
}

pub(crate) fn control_plane_pairing_snapshot_from_runtime(
    snapshot: &channel_router::ChannelPairingSnapshot,
) -> control_plane::PairingChannelSnapshot {
    control_plane::PairingChannelSnapshot {
        channel: snapshot.channel.clone(),
        pending: snapshot
            .pending
            .iter()
            .map(|pending| control_plane::PairingPendingRecord {
                channel: pending.channel.clone(),
                sender_identity: pending.sender_identity.clone(),
                code: pending.code.clone(),
                requested_at_unix_ms: pending.requested_at_unix_ms,
                expires_at_unix_ms: pending.expires_at_unix_ms,
                approval_id: pending.approval_id.clone(),
            })
            .collect(),
        paired: snapshot
            .paired
            .iter()
            .map(|paired| control_plane::PairingGrantRecord {
                channel: paired.channel.clone(),
                sender_identity: paired.sender_identity.clone(),
                approved_at_unix_ms: paired.approved_at_unix_ms,
                expires_at_unix_ms: paired.expires_at_unix_ms,
                approval_id: paired.approval_id.clone(),
            })
            .collect(),
        active_codes: snapshot
            .active_codes
            .iter()
            .map(|code| control_plane::PairingCodeRecord {
                code: code.code.clone(),
                channel: code.channel.clone(),
                issued_by: code.issued_by.clone(),
                created_at_unix_ms: code.created_at_unix_ms,
                expires_at_unix_ms: code.expires_at_unix_ms,
            })
            .collect(),
    }
}

pub(crate) fn lock_support_bundle_jobs(
    jobs: &Arc<Mutex<HashMap<String, control_plane::SupportBundleJob>>>,
) -> std::sync::MutexGuard<'_, HashMap<String, control_plane::SupportBundleJob>> {
    jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn lock_doctor_jobs(
    jobs: &Arc<Mutex<HashMap<String, control_plane::DoctorRecoveryJob>>>,
) -> std::sync::MutexGuard<'_, HashMap<String, control_plane::DoctorRecoveryJob>> {
    jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn list_support_bundle_jobs(
    state: &AppState,
    after_job_id: Option<&str>,
    limit: usize,
) -> Vec<control_plane::SupportBundleJob> {
    let jobs = lock_support_bundle_jobs(&state.support_bundle_jobs);
    let mut entries = jobs.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    entries
        .into_iter()
        .filter(|job| after_job_id.is_none_or(|after| job.job_id.as_str() > after))
        .take(limit)
        .collect()
}

pub(crate) fn list_doctor_jobs(
    state: &AppState,
    after_job_id: Option<&str>,
    limit: usize,
) -> Vec<control_plane::DoctorRecoveryJob> {
    let jobs = lock_doctor_jobs(&state.doctor_jobs);
    let mut entries = jobs.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    entries
        .into_iter()
        .filter(|job| after_job_id.is_none_or(|after| job.job_id.as_str() > after))
        .take(limit)
        .collect()
}

#[allow(clippy::result_large_err)]
pub(crate) fn create_support_bundle_job(
    state: &AppState,
    retain_jobs: usize,
) -> Result<control_plane::SupportBundleJob, Response> {
    let job_id = Ulid::new().to_string();
    let requested_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let job = control_plane::SupportBundleJob {
        job_id: job_id.clone(),
        state: control_plane::SupportBundleJobState::Queued,
        requested_at_unix_ms,
        started_at_unix_ms: None,
        completed_at_unix_ms: None,
        output_path: None,
        command_output: String::new(),
        error: None,
    };
    {
        let mut jobs = lock_support_bundle_jobs(&state.support_bundle_jobs);
        jobs.insert(job_id.clone(), job.clone());
    }
    state.observability.record_support_bundle_export_started();

    let jobs = Arc::clone(&state.support_bundle_jobs);
    let admin_port = state.deployment.admin_port;
    let admin_token = state.auth.admin_token.clone();
    let observability = Arc::clone(&state.observability);
    tokio::spawn(async move {
        run_support_bundle_job(
            jobs,
            observability,
            job_id,
            admin_port,
            admin_token,
            retain_jobs.max(1),
        )
        .await;
    });

    Ok(job)
}

pub(crate) async fn run_support_bundle_job(
    jobs: Arc<Mutex<HashMap<String, control_plane::SupportBundleJob>>>,
    observability: Arc<ObservabilityState>,
    job_id: String,
    admin_port: u16,
    admin_token: Option<String>,
    retain_jobs: usize,
) {
    let started_at = unix_ms_now().unwrap_or_default();
    {
        let mut guard = lock_support_bundle_jobs(&jobs);
        if let Some(job) = guard.get_mut(job_id.as_str()) {
            job.state = control_plane::SupportBundleJobState::Running;
            job.started_at_unix_ms = Some(started_at);
        }
    }

    let result = run_support_bundle_export_command(admin_port, admin_token).await;
    let completed_at = unix_ms_now().unwrap_or_default();
    let mut guard = lock_support_bundle_jobs(&jobs);
    if let Some(job) = guard.get_mut(job_id.as_str()) {
        job.completed_at_unix_ms = Some(completed_at);
        match result {
            Ok((output_path, command_output)) => {
                job.state = control_plane::SupportBundleJobState::Succeeded;
                job.output_path = Some(output_path);
                job.command_output = command_output;
                job.error = None;
                observability.record_support_bundle_export_result(
                    true,
                    "support_bundle.export",
                    "ok",
                    completed_at,
                    ObservabilityCorrelationSnapshot::default(),
                );
            }
            Err(error) => {
                job.state = control_plane::SupportBundleJobState::Failed;
                observability.record_support_bundle_export_result(
                    false,
                    "support_bundle.export",
                    error.clone(),
                    completed_at,
                    ObservabilityCorrelationSnapshot::default(),
                );
                job.error = Some(error);
            }
        }
    }

    let mut finished = guard.values().cloned().collect::<Vec<_>>();
    finished.sort_by(|left, right| left.requested_at_unix_ms.cmp(&right.requested_at_unix_ms));
    while finished.len() > retain_jobs {
        if let Some(first) = finished.first() {
            guard.remove(first.job_id.as_str());
        }
        finished.remove(0);
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn create_doctor_job(
    state: &AppState,
    payload: control_plane::DoctorRecoveryCreateRequest,
) -> Result<control_plane::DoctorRecoveryJob, Response> {
    let job_id = Ulid::new().to_string();
    let requested_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let command = build_doctor_command_args(&payload);
    let job = control_plane::DoctorRecoveryJob {
        job_id: job_id.clone(),
        state: control_plane::DoctorRecoveryJobState::Queued,
        requested_at_unix_ms,
        started_at_unix_ms: None,
        completed_at_unix_ms: None,
        command: command.clone(),
        report: None,
        command_output: String::new(),
        error: None,
    };
    {
        let mut jobs = lock_doctor_jobs(&state.doctor_jobs);
        jobs.insert(job_id.clone(), job.clone());
    }

    let retain_jobs = payload.retain_jobs.max(1);
    let config_path = std::env::var("PALYRA_CONFIG").ok().filter(|value| !value.trim().is_empty());
    let support_bundle_root = resolve_support_bundle_root()
        .map_err(|error| runtime_status_response(tonic::Status::internal(error)))?;
    let state_root = support_bundle_root
        .parent()
        .map(FsPath::to_path_buf)
        .unwrap_or_else(|| support_bundle_root.clone());
    let jobs = Arc::clone(&state.doctor_jobs);
    tokio::spawn(async move {
        run_doctor_job(jobs, job_id, command, state_root, config_path, retain_jobs).await;
    });

    Ok(job)
}

pub(crate) async fn run_doctor_job(
    jobs: Arc<Mutex<HashMap<String, control_plane::DoctorRecoveryJob>>>,
    job_id: String,
    command: Vec<String>,
    state_root: PathBuf,
    config_path: Option<String>,
    retain_jobs: usize,
) {
    let started_at = unix_ms_now().unwrap_or_default();
    {
        let mut guard = lock_doctor_jobs(&jobs);
        if let Some(job) = guard.get_mut(job_id.as_str()) {
            job.state = control_plane::DoctorRecoveryJobState::Running;
            job.started_at_unix_ms = Some(started_at);
        }
    }

    let result =
        run_doctor_command(command.as_slice(), state_root.as_path(), config_path.as_deref()).await;
    let completed_at = unix_ms_now().unwrap_or_default();
    let mut guard = lock_doctor_jobs(&jobs);
    if let Some(job) = guard.get_mut(job_id.as_str()) {
        job.completed_at_unix_ms = Some(completed_at);
        match result {
            Ok((report, command_output)) => {
                job.state = control_plane::DoctorRecoveryJobState::Succeeded;
                job.report = Some(report);
                job.command_output = command_output;
                job.error = None;
            }
            Err(failure) => {
                job.state = control_plane::DoctorRecoveryJobState::Failed;
                job.command_output = failure.command_output;
                job.error = Some(failure.error);
            }
        }
    }

    let mut finished = guard.values().cloned().collect::<Vec<_>>();
    finished.sort_by(|left, right| left.requested_at_unix_ms.cmp(&right.requested_at_unix_ms));
    while finished.len() > retain_jobs {
        if let Some(first) = finished.first() {
            guard.remove(first.job_id.as_str());
        }
        finished.remove(0);
    }
}

fn build_doctor_command_args(payload: &control_plane::DoctorRecoveryCreateRequest) -> Vec<String> {
    let mut command = vec!["doctor".to_owned(), "--json".to_owned()];
    if payload.repair {
        command.push("--repair".to_owned());
    }
    if payload.dry_run {
        command.push("--dry-run".to_owned());
    }
    if payload.force {
        command.push("--force".to_owned());
    }
    for value in &payload.only {
        command.push("--only".to_owned());
        command.push(value.clone());
    }
    for value in &payload.skip {
        command.push("--skip".to_owned());
        command.push(value.clone());
    }
    if let Some(run_id) = payload.rollback_run.as_ref().filter(|value| !value.trim().is_empty()) {
        command.push("--rollback-run".to_owned());
        command.push(run_id.clone());
    }
    command
}

struct DoctorCommandFailure {
    error: String,
    command_output: String,
}

async fn run_doctor_command(
    command_args: &[String],
    state_root: &FsPath,
    config_path: Option<&str>,
) -> Result<(Value, String), DoctorCommandFailure> {
    let cli_path = resolve_console_cli_binary_path().map_err(|error| DoctorCommandFailure {
        error: sanitize_http_error_message(&error),
        command_output: String::new(),
    })?;
    let mut command = TokioCommand::new(cli_path.as_path());
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    command.env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    command.env("LANG", "C").env("LC_ALL", "C");
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PALYRA_STATE_ROOT", state_root)
        .args(command_args);
    if let Some(config_path) = config_path {
        command.env("PALYRA_CONFIG", config_path);
    }

    let output = command.output().await.map_err(|error| DoctorCommandFailure {
        error: sanitize_http_error_message(&format!(
            "failed to run doctor recovery command: {error}"
        )),
        command_output: String::new(),
    })?;
    let stdout_raw = String::from_utf8_lossy(output.stdout.as_slice()).into_owned();
    let stderr_raw = String::from_utf8_lossy(output.stderr.as_slice()).into_owned();
    let command_output = [
        sanitize_http_error_message(stdout_raw.as_str()),
        sanitize_http_error_message(stderr_raw.as_str()),
    ]
    .into_iter()
    .filter(|value| !value.trim().is_empty())
    .collect::<Vec<_>>()
    .join("\n");
    if !output.status.success() {
        return Err(DoctorCommandFailure {
            error: sanitize_http_error_message(
                format!(
                    "doctor recovery command failed (status={}): {}",
                    output
                        .status
                        .code()
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    command_output
                )
                .as_str(),
            ),
            command_output,
        });
    }
    let report =
        serde_json::from_str::<Value>(stdout_raw.trim()).map_err(|error| DoctorCommandFailure {
            error: sanitize_http_error_message(
                format!("doctor recovery command returned invalid JSON: {error}").as_str(),
            ),
            command_output: command_output.clone(),
        })?;
    Ok((report, command_output))
}

pub(crate) async fn run_support_bundle_export_command(
    admin_port: u16,
    admin_token: Option<String>,
) -> Result<(String, String), String> {
    let cli_path =
        resolve_console_cli_binary_path().map_err(|error| sanitize_http_error_message(&error))?;
    let support_bundle_root =
        resolve_support_bundle_root().map_err(|error| sanitize_http_error_message(&error))?;
    let state_root = support_bundle_root
        .parent()
        .map(FsPath::to_path_buf)
        .unwrap_or_else(|| support_bundle_root.clone());
    fs::create_dir_all(support_bundle_root.as_path()).map_err(|error| {
        sanitize_http_error_message(
            format!(
                "failed to create support-bundle directory {}: {error}",
                support_bundle_root.display()
            )
            .as_str(),
        )
    })?;
    let output_path = support_bundle_root
        .join(format!("support-bundle-{}.json", unix_ms_now().unwrap_or_default()));
    let mut command = TokioCommand::new(cli_path.as_path());
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    command.env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    command.env("LANG", "C").env("LC_ALL", "C");
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("support-bundle")
        .arg("export")
        .arg("--output")
        .arg(output_path.as_os_str())
        .env("PALYRA_STATE_ROOT", state_root)
        .env("PALYRA_DAEMON_URL", format!("http://127.0.0.1:{admin_port}"));
    if let Some(token) = admin_token.filter(|token| !token.trim().is_empty()) {
        command.env("PALYRA_ADMIN_TOKEN", token);
    }

    let output = command.output().await.map_err(|error| {
        sanitize_http_error_message(&format!("failed to run support-bundle export: {error}"))
    })?;
    let stdout =
        sanitize_http_error_message(String::from_utf8_lossy(output.stdout.as_slice()).as_ref());
    let stderr =
        sanitize_http_error_message(String::from_utf8_lossy(output.stderr.as_slice()).as_ref());
    let command_output = [stdout, stderr]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if !output.status.success() {
        return Err(sanitize_http_error_message(
            format!(
                "support-bundle export failed (status={}): {}",
                output
                    .status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_owned()),
                command_output
            )
            .as_str(),
        ));
    }
    Ok((output_path.to_string_lossy().into_owned(), command_output))
}

pub(crate) fn resolve_console_cli_binary_path() -> Result<PathBuf, String> {
    if let Ok(current_exe) = std::env::current_exe() {
        let executable_name = if cfg!(windows) { "palyra.exe" } else { "palyra" };
        let mut candidates = Vec::<PathBuf>::new();
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join(executable_name));
        }
        for ancestor in current_exe.ancestors().take(8) {
            candidates.push(ancestor.join("target").join("debug").join(executable_name));
            candidates.push(ancestor.join("target").join("release").join(executable_name));
        }
        for candidate in candidates {
            if candidate.is_file() {
                return fs::canonicalize(candidate).map_err(|error| error.to_string());
            }
        }
    }
    Err("unable to locate `palyra` CLI binary near daemon executable".to_owned())
}

pub(crate) fn resolve_support_bundle_root() -> Result<PathBuf, String> {
    if let Some(raw) = std::env::var_os("PALYRA_STATE_ROOT") {
        if raw.is_empty() {
            return Err("PALYRA_STATE_ROOT must not be empty".to_owned());
        }
        return Ok(PathBuf::from(raw).join("support-bundles"));
    }
    let identity_root = default_identity_store_root().map_err(|error| error.to_string())?;
    let state_root =
        identity_root.parent().map(FsPath::to_path_buf).unwrap_or_else(|| identity_root.clone());
    Ok(state_root.join("support-bundles"))
}

#[allow(clippy::result_large_err)]
pub(crate) fn build_capability_catalog() -> Result<control_plane::CapabilityCatalog, Response> {
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    Ok(control_plane::CapabilityCatalog {
        contract: contract_descriptor(),
        version: "capability-catalog.v2".to_owned(),
        generated_at_unix_ms,
        capabilities: vec![
            capability_entry(
                "runtime.health",
                "runtime",
                "operations",
                "Daemon and runtime health",
                "palyrad",
                &["backend", "dashboard", "desktop"],
                "direct_ui",
                &["deployment"],
                &["/console/v1/diagnostics", "/console/v1/deployment/posture"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                Some("Dashboard exposes the redacted health snapshot directly."),
            ),
            capability_entry(
                "chat.sessions",
                "chat",
                "chat",
                "Chat sessions and run status",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["none"],
                &["/console/v1/chat/sessions", "/console/v1/chat/runs/{run_id}/status"],
                &["crates/palyra-daemon/tests/admin_surface.rs", "apps/web/src/consoleApi.test.ts"],
                &[],
                None,
            ),
            capability_entry(
                "chat.stream",
                "chat",
                "chat",
                "Chat streaming execution",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["tool_calls"],
                &["/console/v1/chat/sessions/{session_id}/messages/stream"],
                &["apps/web/src/consoleApi.test.ts"],
                &[],
                None,
            ),
            capability_entry(
                "approvals",
                "approvals",
                "approvals",
                "Approval inbox and decisions",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["policy"],
                &["/console/v1/approvals", "/console/v1/approvals/{approval_id}/decision"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "cron",
                "cron",
                "cron",
                "Cron job create, update, run-now, and logs",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["scheduler"],
                &["/console/v1/cron/jobs", "/console/v1/cron/jobs/{job_id}/runs"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "channels",
                "channels",
                "channels",
                "Channel connector status, test, and enablement",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["deployment"],
                &["/console/v1/channels", "/console/v1/channels/{connector_id}/enabled"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "webhooks",
                "webhooks",
                "integrations",
                "Webhook integration registry and payload validation",
                "palyrad",
                &["backend", "cli"],
                "direct_ui",
                &["secrets"],
                &[
                    "/console/v1/webhooks",
                    "/console/v1/webhooks/{integration_id}",
                    "/console/v1/webhooks/{integration_id}/enabled",
                    "/console/v1/webhooks/{integration_id}/test",
                ],
                &["crates/palyra-daemon/src/webhooks.rs"],
                &["cargo run -p palyra-cli -- webhooks list --json"],
                Some("First iteration stays backend and CLI only; it manages secret-aware integrations without provisioning a public ingress endpoint."),
            ),
            capability_entry(
                "channel.router",
                "channels",
                "channels",
                "Router previews, pairings, and warnings",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["policy"],
                &[
                    "/console/v1/channels/router/rules",
                    "/console/v1/channels/router/warnings",
                    "/console/v1/channels/router/preview",
                    "/console/v1/channels/router/pairings",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "discord.onboarding",
                "channels",
                "channels",
                "Discord onboarding probe and apply",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["secrets", "deployment"],
                &[
                    "/console/v1/channels/discord/onboarding/probe",
                    "/console/v1/channels/discord/onboarding/apply",
                    "/console/v1/channels/{connector_id}/test-send",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                Some("Dashboard handles probe, apply, and verification against the live daemon contract."),
            ),
            capability_entry(
                "browser.profiles",
                "browser",
                "browser",
                "Browser profile lifecycle",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["browser"],
                &[
                    "/console/v1/browser/profiles",
                    "/console/v1/browser/profiles/create",
                    "/console/v1/browser/profiles/{profile_id}/rename",
                ],
                &["apps/web/src/App.test.tsx"],
                &[],
                None,
            ),
            capability_entry(
                "browser.relay",
                "browser",
                "browser",
                "Browser relay tokens and actions",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["browser"],
                &["/console/v1/browser/relay/tokens", "/console/v1/browser/relay/actions"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "browser.downloads",
                "browser",
                "browser",
                "Browser download artifact inspection",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["browser"],
                &["/console/v1/browser/downloads"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "memory",
                "memory",
                "memory",
                "Memory status, workspace docs, recall preview, unified search, and purge",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["memory"],
                &[
                    "/console/v1/memory/status",
                    "/console/v1/memory/search",
                    "/console/v1/memory/recall/preview",
                    "/console/v1/memory/search-all",
                    "/console/v1/memory/session-search",
                    "/console/v1/memory/recall-artifacts",
                    "/console/v1/memory/workspace/documents",
                    "/console/v1/memory/workspace/document",
                    "/console/v1/memory/workspace/document/move",
                    "/console/v1/memory/workspace/document/delete",
                    "/console/v1/memory/workspace/document/pin",
                    "/console/v1/memory/workspace/document/versions",
                    "/console/v1/memory/workspace/bootstrap",
                    "/console/v1/memory/workspace/search",
                    "/console/v1/memory/purge",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "skills",
                "skills",
                "skills",
                "Skill install, verify, audit, quarantine, and enable",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["skills"],
                &["/console/v1/skills", "/console/v1/skills/{skill_id}/audit"],
                &["apps/web/src/App.test.tsx"],
                &[],
                None,
            ),
            capability_entry(
                "plugins",
                "plugins",
                "extensions",
                "Trusted plugin bindings over signed installed skills",
                "palyrad",
                &["backend", "cli"],
                "direct_ui",
                &["skills", "sandbox"],
                &[
                    "/console/v1/plugins",
                    "/console/v1/plugins/{plugin_id}",
                    "/console/v1/plugins/{plugin_id}/check",
                    "/console/v1/plugins/{plugin_id}/enable",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &["cargo run -p palyra-cli -- plugins list --json"],
                Some("Plugin bindings stay deny-by-default around skill trust, capability profiles, and runtime policy."),
            ),
            capability_entry(
                "hooks",
                "hooks",
                "automation",
                "Event-driven hook bindings over trusted plugins",
                "palyrad",
                &["backend", "cli"],
                "direct_ui",
                &["plugins", "audit"],
                &[
                    "/console/v1/hooks",
                    "/console/v1/hooks/{hook_id}",
                    "/console/v1/hooks/{hook_id}/check",
                    "/console/v1/hooks/{hook_id}/enable",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &["cargo run -p palyra-cli -- hooks list --json"],
                Some("Hooks bind explicit internal events to plugin bindings and keep dispatch audit-visible."),
            ),
            capability_entry(
                "audit",
                "audit",
                "operations",
                "Audit event browsing",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["audit"],
                &["/console/v1/audit/events"],
                &["apps/web/src/App.test.tsx"],
                &[],
                None,
            ),
            capability_entry(
                "auth.profiles",
                "auth",
                "auth",
                "Auth profile CRUD",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["provider_auth", "secrets"],
                &["/console/v1/auth/profiles"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "auth.health",
                "auth",
                "auth",
                "Auth profile health and refresh metrics",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["provider_auth"],
                &["/console/v1/auth/health"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "auth.openai",
                "auth",
                "auth",
                "OpenAI provider auth contract surface",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["provider_auth", "deployment"],
                &[
                    "/console/v1/auth/providers/openai",
                    "/console/v1/auth/providers/openai/api-key",
                    "/console/v1/auth/providers/openai/bootstrap",
                    "/console/v1/auth/providers/openai/callback-state",
                    "/console/v1/auth/providers/openai/reconnect",
                    "/console/v1/auth/providers/openai/refresh",
                    "/console/v1/auth/providers/openai/revoke",
                    "/console/v1/auth/providers/openai/default-profile",
                    "/console/v1/auth/providers/anthropic",
                    "/console/v1/auth/providers/anthropic/api-key",
                    "/console/v1/auth/providers/anthropic/revoke",
                    "/console/v1/auth/providers/anthropic/default-profile",
                    "/console/v1/models/test-connection",
                    "/console/v1/models/discover",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "config.inspect",
                "config",
                "config",
                "Config inspect and validate",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["deployment", "secrets"],
                &["/console/v1/config/inspect", "/console/v1/config/validate"],
                &["crates/palyra-daemon/tests/admin_surface.rs", "crates/palyra-cli/tests/config_mutation.rs"],
                &[],
                None,
            ),
            capability_entry(
                "config.mutate",
                "config",
                "config",
                "Config mutate, migrate, and recover",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["deployment", "secrets"],
                &[
                    "/console/v1/config/mutate",
                    "/console/v1/config/migrate",
                    "/console/v1/config/recover",
                ],
                &["crates/palyra-daemon/tests/admin_surface.rs", "crates/palyra-cli/tests/config_mutation.rs"],
                &[],
                Some("Dashboard executes redacted inspect, validate, mutate, migrate, and recover flows without raw config hand edits."),
            ),
            capability_entry(
                "secrets",
                "secrets",
                "config",
                "Secret metadata, reveal, write, and delete",
                "palyrad",
                &["backend", "dashboard", "cli"],
                "direct_ui",
                &["secrets"],
                &[
                    "/console/v1/secrets",
                    "/console/v1/secrets/metadata",
                    "/console/v1/secrets/reveal",
                    "/console/v1/secrets/delete",
                ],
                &["crates/palyra-daemon/tests/gateway_grpc.rs", "crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                None,
            ),
            capability_entry(
                "pairing",
                "pairing",
                "access",
                "DM pairing codes and approval state",
                "palyrad",
                &["backend", "dashboard"],
                "direct_ui",
                &["channels", "approvals"],
                &["/console/v1/pairing", "/console/v1/pairing/codes"],
                &["crates/palyra-daemon/src/channel_router.rs"],
                &[],
                None,
            ),
            capability_entry(
                "gateway.access",
                "deployment",
                "access",
                "Gateway access and deployment posture summary",
                "palyrad",
                &["backend", "dashboard", "desktop", "cli"],
                "direct_ui",
                &["deployment"],
                &["/console/v1/deployment/posture"],
                &["apps/desktop/src-tauri/src/lib.rs"],
                &[],
                Some("Dashboard shows local or remote bind posture, TLS state, and remote exposure warnings."),
            ),
            capability_entry(
                "onboarding.posture",
                "onboarding",
                "access",
                "Canonical onboarding posture and resumable step state",
                "palyrad",
                &["backend", "dashboard", "desktop", "cli"],
                "direct_ui",
                &["deployment", "provider_auth", "channels"],
                &["/console/v1/onboarding/posture"],
                &["crates/palyra-daemon/tests/admin_surface.rs", "crates/palyra-cli/src/commands/onboarding.rs"],
                &[],
                Some("The onboarding orchestrator publishes shared step state, machine-readable blockers, and recommended next actions for quick-start and advanced setup flows."),
            ),
            capability_entry(
                "gateway.access.verify_remote",
                "deployment",
                "access",
                "Remote dashboard URL verification",
                "palyra-cli",
                &["cli", "dashboard"],
                "generated_cli",
                &["deployment"],
                &[],
                &["crates/palyra-cli/src/main.rs"],
                &[
                    "cargo run -p palyra-cli -- dashboard --verify-remote --json",
                ],
                Some("Remote URL verification stays CLI-driven because operators may need host-specific identity store arguments and pin diagnostics."),
            ),
            capability_entry(
                "gateway.access.tunnel",
                "deployment",
                "access",
                "SSH tunnel helper",
                "palyra-cli",
                &["cli", "dashboard"],
                "generated_cli",
                &["deployment"],
                &[],
                &["crates/palyra-cli/src/main.rs"],
                &[
                    "cargo run -p palyra-cli -- tunnel --ssh <user>@<host> --remote-port 7142 --local-port 7142",
                ],
                Some("Tunnel setup remains a CLI handoff because it depends on operator-specific SSH topology and host access."),
            ),
            capability_entry(
                "support.bundle",
                "support",
                "support",
                "Support bundle export jobs",
                "palyrad",
                &["backend", "dashboard", "desktop", "cli"],
                "direct_ui",
                &["support"],
                &["/console/v1/support-bundle/jobs"],
                &["apps/desktop/src-tauri/src/lib.rs", "crates/palyra-cli/src/main.rs"],
                &[],
                Some("Dashboard can queue and inspect export jobs while CLI export remains available for detached recovery workflows."),
            ),
            capability_entry(
                "runtime.doctor",
                "runtime",
                "operations",
                "Doctor JSON diagnostics export",
                "palyra-cli",
                &["cli", "dashboard"],
                "generated_cli",
                &["support", "deployment"],
                &[],
                &["crates/palyra-cli/src/main.rs"],
                &["cargo run -p palyra-cli -- doctor --json"],
                Some("Doctor output remains CLI-first so operators can export a deterministic JSON report outside the browser session."),
            ),
            capability_entry(
                "protocol.contracts",
                "protocol",
                "operations",
                "Protocol validation utilities",
                "scripts",
                &["cli", "dashboard"],
                "generated_cli",
                &["protocol"],
                &[],
                &["scripts/protocol/check-generated-stubs.sh", "scripts/protocol/check-generated-stubs.ps1"],
                &[
                    "bash scripts/protocol/check-generated-stubs.sh",
                    "pwsh scripts/protocol/check-generated-stubs.ps1",
                ],
                Some("Low-level protocol validation remains a CLI handoff for developer and release workflows."),
            ),
            capability_entry(
                "policy.explain",
                "policy",
                "operations",
                "Policy explain developer surface",
                "palyrad",
                &["backend", "internal"],
                "internal",
                &["policy"],
                &["/admin/v1/policy/explain"],
                &["crates/palyra-daemon/tests/admin_surface.rs"],
                &[],
                Some("Policy explain stays admin-only because it exposes low-level evaluation detail that is not yet normalized for operator use."),
            ),
        ],
        migration_notes: vec![
            control_plane::CapabilityMigrationNote {
                id: "m52-page-meta".to_owned(),
                message: "M52 adds typed contract/page/error metadata while preserving legacy response keys for existing dashboard consumers.".to_owned(),
            },
            control_plane::CapabilityMigrationNote {
                id: "m52-openai-contract".to_owned(),
                message: "OpenAI provider auth endpoints publish the control-plane contract in M52; interactive OAuth bootstrap/callback UX is completed in M54.".to_owned(),
            },
            control_plane::CapabilityMigrationNote {
                id: "m56-capability-exposure".to_owned(),
                message: "M56 expands capability catalog entries with dashboard section ownership and CLI handoff metadata so the dashboard can surface direct actions, read-only handoffs, and internal-only capabilities explicitly.".to_owned(),
            },
        ],
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn capability_entry(
    id: &str,
    domain: &str,
    dashboard_section: &str,
    title: &str,
    owner: &str,
    surfaces: &[&str],
    execution_mode: &str,
    mutation_classes: &[&str],
    contract_paths: &[&str],
    test_refs: &[&str],
    cli_handoff_commands: &[&str],
    notes: Option<&str>,
) -> control_plane::CapabilityEntry {
    control_plane::CapabilityEntry {
        id: id.to_owned(),
        domain: domain.to_owned(),
        dashboard_section: dashboard_section.to_owned(),
        title: title.to_owned(),
        owner: owner.to_owned(),
        surfaces: surfaces.iter().map(|value| (*value).to_owned()).collect(),
        execution_mode: execution_mode.to_owned(),
        dashboard_exposure: Some(match execution_mode {
            "generated_cli" => control_plane::CapabilityDashboardExposure::CliHandoff,
            "internal" => control_plane::CapabilityDashboardExposure::InternalOnly,
            _ => control_plane::CapabilityDashboardExposure::DirectAction,
        }),
        cli_handoff_commands: cli_handoff_commands
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        mutation_classes: mutation_classes.iter().map(|value| (*value).to_owned()).collect(),
        test_refs: test_refs.iter().map(|value| (*value).to_owned()).collect(),
        contract_paths: contract_paths.iter().map(|value| (*value).to_owned()).collect(),
        notes: notes.map(str::to_owned),
    }
}

pub(crate) async fn collect_console_browser_relay_failure_metrics(
    state: &AppState,
) -> (u64, Vec<String>) {
    let snapshot = match state.runtime.recent_journal_snapshot(256).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return (
                0,
                vec![sanitize_http_error_message(
                    format!("failed to query recent browser relay diagnostics: {error}").as_str(),
                )],
            );
        }
    };

    let mut failures = 0_u64;
    let mut messages = Vec::<String>::new();
    for event in snapshot.events {
        let Ok(payload) = serde_json::from_str::<Value>(event.payload_json.as_str()) else {
            continue;
        };
        if payload.get("event").and_then(Value::as_str) != Some("browser.relay.action") {
            continue;
        }
        let success = payload.get("success").and_then(Value::as_bool).unwrap_or(false);
        if success {
            continue;
        }
        failures = failures.saturating_add(1);
        if messages.len() >= 5 {
            continue;
        }
        if let Some(error_message) = payload.get("error").and_then(Value::as_str) {
            if !error_message.trim().is_empty() {
                messages.push(sanitize_http_error_message(error_message));
            }
        }
    }
    (failures, messages)
}

pub(crate) fn redact_console_diagnostics_value(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if redaction_key_is_sensitive(key.as_str()) {
                    *child = Value::String("<redacted>".to_owned());
                    continue;
                }
                redact_console_diagnostics_value(child, Some(key.as_str()));
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                redact_console_diagnostics_value(entry, key_context);
            }
        }
        Value::String(raw) => {
            if key_context.is_some_and(redaction_key_is_sensitive) {
                *raw = "<redacted>".to_owned();
                return;
            }
            if key_context
                .map(|key| {
                    let lowered = key.to_ascii_lowercase();
                    lowered.contains("url")
                        || lowered.contains("uri")
                        || lowered.contains("endpoint")
                        || lowered.contains("location")
                })
                .unwrap_or(false)
            {
                *raw = redact_url(raw.as_str());
                return;
            }
            if key_context
                .map(|key| {
                    let lowered = key.to_ascii_lowercase();
                    lowered.contains("error")
                        || lowered.contains("reason")
                        || lowered.contains("message")
                        || lowered.contains("detail")
                })
                .unwrap_or(false)
            {
                *raw = redact_auth_error(raw.as_str());
                *raw = redact_url_segments_in_text(raw.as_str());
            }
        }
        _ => {}
    }
}

fn build_console_profile_context(state: &AppState) -> control_plane::ConsoleProfileContext {
    let deployment = build_deployment_posture_summary(state);
    let remote_like = deployment.remote_bind_detected
        || deployment.bind_profile.eq_ignore_ascii_case("public_tls")
        || deployment.mode.eq_ignore_ascii_case("remote_vps");
    let strict_mode = remote_like || !deployment.warnings.is_empty();
    let label = match deployment.mode.as_str() {
        "local_desktop" => "Local desktop",
        "remote_vps" => "Remote VPS",
        other => other,
    };
    let environment = if remote_like { "production" } else { "local" };
    let color = if deployment.remote_bind_detected {
        "red"
    } else if strict_mode {
        "amber"
    } else {
        "green"
    };
    let risk_level = if deployment.remote_bind_detected {
        "high"
    } else if strict_mode {
        "elevated"
    } else {
        "low"
    };

    control_plane::ConsoleProfileContext {
        name: deployment.mode.clone(),
        label: label.to_owned(),
        environment: environment.to_owned(),
        color: color.to_owned(),
        risk_level: risk_level.to_owned(),
        strict_mode,
        mode: deployment.bind_profile,
    }
}

pub(crate) fn build_console_session_response(
    state: &AppState,
    session: &ConsoleSession,
    csrf_token: String,
) -> ConsoleSessionResponse {
    ConsoleSessionResponse {
        principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        profile: Some(build_console_profile_context(state)),
        csrf_token,
        issued_at_unix_ms: session.issued_at_unix_ms,
        expires_at_unix_ms: session.expires_at_unix_ms,
    }
}

pub(crate) fn next_console_session_expiry_unix_ms(now: i64) -> i64 {
    now.saturating_add(i64::try_from(CONSOLE_SESSION_TTL_SECONDS).unwrap_or(i64::MAX) * 1_000)
}

#[allow(clippy::result_large_err)]
pub(crate) fn authorize_console_session(
    state: &AppState,
    headers: &HeaderMap,
    require_csrf: bool,
) -> Result<ConsoleSession, Response> {
    let session_token = cookie_value(headers, CONSOLE_SESSION_COOKIE_NAME).ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "console session cookie is missing",
        ))
    })?;
    let session_token_hash_sha256 = sha256_hex(session_token.as_bytes());
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let mut sessions = lock_console_sessions(&state.console_sessions);
    sessions.retain(|_, session| session.expires_at_unix_ms > now);
    let session_key = find_hashed_secret_map_key(&sessions, session_token_hash_sha256.as_str())
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::permission_denied(
                "console session is missing or expired",
            ))
        })?;
    let session = sessions.get_mut(session_key.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "console session is missing or expired",
        ))
    })?;
    if require_csrf {
        let csrf_candidate = headers
            .get(CONSOLE_CSRF_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                runtime_status_response(tonic::Status::permission_denied(
                    "missing CSRF token for console request",
                ))
            })?;
        if !constant_time_eq_bytes(csrf_candidate.as_bytes(), session.csrf_token.as_bytes()) {
            return Err(runtime_status_response(tonic::Status::permission_denied(
                "CSRF token is invalid",
            )));
        }
    }
    session.expires_at_unix_ms = next_console_session_expiry_unix_ms(now);
    Ok(session.clone())
}

#[allow(clippy::result_large_err)]
pub(crate) fn refresh_console_session_cookie(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<HeaderValue>, Response> {
    let Some(session_token) = cookie_value(headers, CONSOLE_SESSION_COOKIE_NAME) else {
        return Ok(None);
    };
    let session_token_hash_sha256 = sha256_hex(session_token.as_bytes());
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let mut sessions = lock_console_sessions(&state.console_sessions);
    sessions.retain(|_, session| session.expires_at_unix_ms > now);
    let Some(session_key) =
        find_hashed_secret_map_key(&sessions, session_token_hash_sha256.as_str())
    else {
        return Ok(None);
    };
    let Some(session) = sessions.get_mut(session_key.as_str()) else {
        return Ok(None);
    };
    session.expires_at_unix_ms = next_console_session_expiry_unix_ms(now);
    build_console_session_cookie(session_token.as_str(), request_uses_tls(headers)).map(Some)
}

pub(crate) fn lock_console_sessions<'a>(
    sessions: &'a Arc<Mutex<HashMap<String, ConsoleSession>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, ConsoleSession>> {
    match sessions.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("console session map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

pub(crate) fn request_uses_tls(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

#[allow(clippy::result_large_err)]
pub(crate) fn build_console_session_cookie(
    session_id: &str,
    secure: bool,
) -> Result<HeaderValue, Response> {
    let mut cookie = format!(
        "{CONSOLE_SESSION_COOKIE_NAME}={session_id}; Max-Age={CONSOLE_SESSION_TTL_SECONDS}; Path=/; HttpOnly; SameSite=Lax"
    );
    if secure {
        cookie.push_str("; Secure");
    }
    HeaderValue::from_str(cookie.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode Set-Cookie header: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn clear_console_session_cookie(secure: bool) -> Result<HeaderValue, Response> {
    let mut cookie =
        format!("{CONSOLE_SESSION_COOKIE_NAME}=deleted; Max-Age=0; Path=/; HttpOnly; SameSite=Lax");
    if secure {
        cookie.push_str("; Secure");
    }
    HeaderValue::from_str(cookie.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode Set-Cookie header: {error}"
        )))
    })
}

pub(crate) fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let trimmed = part.trim();
        let mut pair = trimmed.splitn(2, '=');
        let key = pair.next()?.trim();
        let value = pair.next().unwrap_or("").trim();
        if key == name && !value.is_empty() {
            return Some(value.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod config_ref_health_tests {
    use super::config_ref_item_guidance;
    use palyra_control_plane as control_plane;

    fn sample_record(
        state: &str,
        source_kind: &str,
        reload_action: &str,
    ) -> control_plane::ConfiguredSecretRecord {
        control_plane::ConfiguredSecretRecord {
            secret_id: "model_provider.openai_api_key_secret_ref:fp-1".to_owned(),
            component: "model_provider".to_owned(),
            config_path: "model_provider.openai_api_key_secret_ref".to_owned(),
            status: state.to_owned(),
            resolution_scope: "startup".to_owned(),
            reload_action: reload_action.to_owned(),
            snapshot_generation: 1,
            source: control_plane::ConfiguredSecretSourceView {
                kind: source_kind.to_owned(),
                fingerprint: "fp-1".to_owned(),
                required: true,
                refresh_policy: "startup_only".to_owned(),
                snapshot_policy: "runtime_snapshot".to_owned(),
                description: "test".to_owned(),
                display_name: None,
                redaction_label: None,
                max_bytes: None,
                exec_timeout_ms: Some(500),
                trusted_dir_count: Some(1),
                inherited_env_count: Some(0),
                allow_symlinks: Some(false),
            },
            affected_components: vec!["model_provider".to_owned()],
            last_resolved_at_unix_ms: Some(1_700_000_000_000),
            last_error_kind: None,
            last_error: None,
            value_bytes: None,
        }
    }

    #[test]
    fn config_ref_item_guidance_recommends_missing_env_sources() {
        let record = sample_record("missing", "env", "blocked_while_runs_active");
        let (severity, advice) = config_ref_item_guidance(&record, 0, None);
        assert_eq!(severity, "blocking");
        assert_eq!(
            advice.as_deref(),
            Some("Set the missing environment variable or migrate this ref to vault.")
        );
    }

    #[test]
    fn config_ref_item_guidance_recommends_waiting_for_active_runs_on_stale_entries() {
        let record = sample_record("stale", "env", "blocked_while_runs_active");
        let plan = control_plane::ConfigReloadPlanEnvelope {
            contract: super::contract_descriptor(),
            plan_id: "plan-1".to_owned(),
            source_path: "./palyra.toml".to_owned(),
            generated_at_unix_ms: 1_700_000_000_000,
            active_runs: 3,
            requires_restart: false,
            hot_safe_applicable: false,
            summary: control_plane::ConfigReloadPlanSummary {
                hot_safe: 0,
                restart_required: 0,
                blocked_while_runs_active: 1,
                manual_review: 0,
            },
            steps: Vec::new(),
        };
        let (severity, advice) = config_ref_item_guidance(&record, plan.active_runs, Some(&plan));
        assert_eq!(severity, "warning");
        assert_eq!(
            advice.as_deref(),
            Some(
                "Wait for active runs to finish, then rerun the reload apply step for this config ref."
            )
        );
    }
}

#[cfg(test)]
mod support_bundle_root_tests {
    use super::resolve_support_bundle_root;
    use std::{
        ffi::OsString,
        sync::{Mutex, OnceLock},
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                unsafe {
                    std::env::set_var(self.key, previous);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn resolve_support_bundle_root_prefers_explicit_state_root_env() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let portable_state_root = std::env::temp_dir().join("palyra-portable-state");
        let portable_state_root_string = portable_state_root.to_string_lossy().into_owned();
        let _state_root =
            ScopedEnvVar::set("PALYRA_STATE_ROOT", portable_state_root_string.as_str());
        let support_root =
            resolve_support_bundle_root().expect("support bundle root should resolve");
        assert_eq!(support_root, portable_state_root.join("support-bundles"));
    }
}
