//! Host-owned objective judging and continuation orchestration.
//!
//! SQLite records are the replay authority. The file-backed objective registry
//! is updated as an idempotent operator-facing projection after each decision.

use std::sync::Arc;

use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::Status;
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::daemon_lifecycle::DaemonLifecyclePhase,
    gateway::GatewayRuntimeState,
    journal::{
        objective_continuation::{
            ObjectiveAttemptReserveRequest, ObjectiveContinuationAttemptRecord,
            ObjectiveContinuationDecision, ObjectiveJudgeDecisionRequest,
        },
        ObjectiveGuardDisposition, ObjectiveGuardEvaluation,
        OrchestratorBackgroundTaskCreateRequest, OrchestratorBackgroundTaskRecord,
        OrchestratorBackgroundTaskWorkerUpdateRequest, OrchestratorRunStatusSnapshot,
    },
    objective_judge::{ObjectiveJudgeInput, ObjectiveJudgeOutput, ObjectiveJudgeStatus},
    objectives::{
        ObjectiveAttemptRecord, ObjectiveFinalizationMode, ObjectiveLifecycleRecord,
        ObjectiveRecord, ObjectiveState, ObjectiveUpsert,
    },
};

const OBJECTIVE_JUDGE_BUDGET_TOKENS: u64 = 900;
const OBJECTIVE_CONTINUATION_DEFAULT_BUDGET_TOKENS: u64 = 4_000;
const OBJECTIVE_CONTINUATION_PRIORITY: i64 = -100;
const OBJECTIVE_JUDGE_PRIORITY: i64 = -90;
const OBJECTIVE_PARSE_RETRY_LIMIT: u64 = 3;
const OBJECTIVE_RECONCILE_LIMIT: usize = 256;

/// Bounded startup reconciliation outcome exposed to startup diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ObjectiveContinuationReconcileReport {
    pub(crate) scanned: u64,
    pub(crate) judge_tasks_repaired: u64,
    pub(crate) decisions_applied: u64,
    pub(crate) continuations_repaired: u64,
    pub(crate) paused: u64,
    pub(crate) errors: u64,
}

/// Enqueues the first judge for a terminal cron run bound to an active objective.
///
/// # Errors
/// Returns a runtime status when a bound run cannot be read or its durable
/// continuation intent cannot be reserved.
pub(crate) async fn schedule_after_cron_terminal(
    runtime: &Arc<GatewayRuntimeState>,
    job_id: &str,
    cron_run_id: &str,
) -> Result<bool, Status> {
    let config = runtime.routines_runtime_config()?;
    let objective = {
        let registry = Arc::clone(&config.objectives);
        let job_id = job_id.to_owned();
        tokio::task::spawn_blocking(move || {
            registry.list_objectives().map_err(objective_registry_status).map(|records| {
                records.into_iter().find(|record| {
                    record.state == ObjectiveState::Active
                        && record.automation.enabled
                        && record.automation.routine_id.as_deref() == Some(job_id.as_str())
                })
            })
        })
        .await
        .map_err(|_| Status::internal("objective registry lookup worker panicked"))??
    };
    let Some(objective) = objective else {
        return Ok(false);
    };
    let Some(cron_run) = runtime.cron_run(cron_run_id.to_owned()).await? else {
        return Err(Status::not_found("terminal cron run was not found"));
    };
    let Some(source_run_id) = cron_run.orchestrator_run_id else {
        return Ok(false);
    };
    let Some(source_run) = runtime.orchestrator_run_status_snapshot(source_run_id.clone()).await?
    else {
        return Err(Status::failed_precondition(
            "terminal cron run is missing its orchestrator run",
        ));
    };
    if !is_terminal_run_state(source_run.state.as_str()) {
        return Ok(false);
    }
    reserve_and_enqueue_judge(
        runtime,
        &objective,
        Some(job_id.to_owned()),
        &source_run,
        source_run.run_id.as_str(),
    )
    .await?;
    Ok(true)
}

/// Reconciles an ObjectiveJudge or objective-continuation task after terminal persistence.
///
/// # Errors
/// Returns a runtime status if the controller transition cannot be persisted.
pub(crate) async fn reconcile_terminal_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    if let Some(attempt) = objective_attempt_for_judge_task(runtime, task.task_id.clone()).await? {
        let decision = judge_decision_from_task(runtime, task, &attempt).await?;
        let settled = settle_judge_decision(runtime, decision).await?;
        apply_decision(runtime, settled).await?;
        return Ok(true);
    }
    let Some(attempt) =
        objective_attempt_for_continuation_task(runtime, task.task_id.clone()).await?
    else {
        return Ok(false);
    };
    if !is_terminal_task_state(task.state.as_str()) {
        return Ok(true);
    }
    let Some(source_run_id) = task.target_run_id.as_deref() else {
        pause_attempt(
            runtime,
            &attempt,
            "objective.continuation.run_missing",
            "Continuation finished without a durable child run.",
        )
        .await?;
        return Ok(true);
    };
    let Some(source_run) =
        runtime.orchestrator_run_status_snapshot(source_run_id.to_owned()).await?
    else {
        return Ok(true);
    };
    if !is_terminal_run_state(source_run.state.as_str()) {
        return Ok(true);
    }
    let objective = load_objective(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("objective continuation target no longer exists"))?;
    reserve_and_enqueue_judge(
        runtime,
        &objective,
        attempt.routine_id.clone(),
        &source_run,
        attempt.root_run_id.as_str(),
    )
    .await?;
    mark_attempt_applied(
        runtime,
        attempt.attempt_id.clone(),
        "settled",
        "objective.continuation.turn_completed",
    )
    .await?;
    Ok(true)
}

/// Rechecks user-preemption and lifecycle state after a continuation worker
/// claims a task but before it enters RunStream admission.
///
/// # Errors
/// Returns a runtime status when preflight state cannot be read or a rejected
/// task cannot be settled.
pub(crate) async fn admit_claimed_continuation_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    let Some(attempt) =
        objective_attempt_for_continuation_task(runtime, task.task_id.clone()).await?
    else {
        return Ok(true);
    };
    let lifecycle = runtime.daemon_lifecycle_snapshot()?;
    let user_preempted = has_active_user_input(runtime, attempt.session_id.as_str()).await?;
    let reason = if lifecycle.phase != DaemonLifecyclePhase::Running {
        Some((
            "objective.continuation.daemon_draining",
            "Daemon drain preempted the autonomous continuation.",
        ))
    } else if user_preempted {
        Some((
            "objective.continuation.user_preempted",
            "Queued user input preempted the autonomous continuation.",
        ))
    } else {
        None
    };
    let Some((reason_code, summary)) = reason else {
        return Ok(true);
    };
    runtime
        .update_orchestrator_background_task_from_worker(
            OrchestratorBackgroundTaskWorkerUpdateRequest {
                task_id: task.task_id.clone(),
                execution_generation: task.execution_generation,
                state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
                target_run_id: None,
                last_error: Some(Some(summary.to_owned())),
                result_json: Some(Some(
                    json!({
                        "status": "cancelled",
                        "task_id": task.task_id,
                        "reason_code": reason_code,
                    })
                    .to_string(),
                )),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
            },
        )
        .await?;
    pause_attempt(runtime, &attempt, reason_code, summary).await?;
    Ok(false)
}

/// Replays bounded objective intents after routines/objectives configuration.
///
/// # Errors
/// Only failure to load the durable candidate batch aborts startup. Individual
/// candidate failures are counted and logged so one damaged objective cannot
/// starve unrelated recovery.
pub(crate) async fn reconcile_startup(
    runtime: &Arc<GatewayRuntimeState>,
) -> Result<ObjectiveContinuationReconcileReport, Status> {
    let runtime_for_read = Arc::clone(runtime);
    let candidates = tokio::task::spawn_blocking(move || {
        runtime_for_read
            .journal_store
            .pending_objective_attempts()
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective continuation recovery worker panicked"))??;
    let mut report = ObjectiveContinuationReconcileReport::default();
    for attempt in candidates.into_iter().take(OBJECTIVE_RECONCILE_LIMIT) {
        report.scanned = report.scanned.saturating_add(1);
        let outcome = reconcile_attempt(runtime, &attempt).await;
        match outcome {
            Ok(ReconcileAction::JudgeTask) => {
                report.judge_tasks_repaired = report.judge_tasks_repaired.saturating_add(1);
            }
            Ok(ReconcileAction::Decision) => {
                report.decisions_applied = report.decisions_applied.saturating_add(1);
            }
            Ok(ReconcileAction::Continuation) => {
                report.continuations_repaired = report.continuations_repaired.saturating_add(1);
            }
            Ok(ReconcileAction::Paused) => {
                report.paused = report.paused.saturating_add(1);
            }
            Ok(ReconcileAction::Noop) => {}
            Err(error) => {
                report.errors = report.errors.saturating_add(1);
                warn!(
                    attempt_id = %attempt.attempt_id,
                    objective_id = %attempt.objective_id,
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "objective continuation startup reconciliation failed"
                );
            }
        }
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileAction {
    JudgeTask,
    Decision,
    Continuation,
    Paused,
    Noop,
}

async fn reconcile_attempt(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<ReconcileAction, Status> {
    match attempt.state.as_str() {
        "judge_enqueue_pending" => {
            ensure_judge_task(runtime, attempt).await?;
            mark_judge_enqueued(runtime, attempt.judge_task_id.clone()).await?;
            Ok(ReconcileAction::JudgeTask)
        }
        "judge_enqueued" => {
            let Some(task) =
                runtime.get_orchestrator_background_task(attempt.judge_task_id.clone()).await?
            else {
                ensure_judge_task(runtime, attempt).await?;
                return Ok(ReconcileAction::JudgeTask);
            };
            if is_terminal_task_state(task.state.as_str()) {
                reconcile_terminal_task(runtime, &task).await?;
                return Ok(ReconcileAction::Decision);
            }
            Ok(ReconcileAction::Noop)
        }
        "decision_pending" => {
            apply_decision(runtime, attempt.clone()).await?;
            Ok(match attempt.decision {
                ObjectiveContinuationDecision::Continue => ReconcileAction::Continuation,
                ObjectiveContinuationDecision::Done
                | ObjectiveContinuationDecision::Wait
                | ObjectiveContinuationDecision::Blocked
                | ObjectiveContinuationDecision::NeedsUser => ReconcileAction::Paused,
                ObjectiveContinuationDecision::Pending => ReconcileAction::Decision,
            })
        }
        "continuation_enqueue_pending" => {
            ensure_continuation_task(runtime, attempt).await?;
            mark_attempt_applied(
                runtime,
                attempt.attempt_id.clone(),
                "continuation_enqueued",
                "objective.continuation.task_enqueued",
            )
            .await?;
            Ok(ReconcileAction::Continuation)
        }
        "continuation_enqueued" => {
            let Some(task_id) = attempt.continuation_task_id.as_ref() else {
                return Err(Status::failed_precondition(
                    "enqueued objective continuation is missing its task identity",
                ));
            };
            let Some(task) = runtime.get_orchestrator_background_task(task_id.clone()).await?
            else {
                return Err(Status::failed_precondition(
                    "enqueued objective continuation task is missing",
                ));
            };
            if is_terminal_task_state(task.state.as_str()) {
                reconcile_terminal_task(runtime, &task).await?;
                return Ok(ReconcileAction::Decision);
            }
            Ok(ReconcileAction::Noop)
        }
        _ => Ok(ReconcileAction::Noop),
    }
}

async fn reserve_and_enqueue_judge(
    runtime: &Arc<GatewayRuntimeState>,
    objective: &ObjectiveRecord,
    routine_id: Option<String>,
    source_run: &OrchestratorRunStatusSnapshot,
    root_run_id: &str,
) -> Result<ObjectiveContinuationAttemptRecord, Status> {
    let session = runtime
        .orchestrator_session_by_id_snapshot(source_run.session_id.clone())
        .await?
        .ok_or_else(|| Status::failed_precondition("objective session no longer exists"))?;
    let mut evidence_refs = objective.linked_artifact_paths.clone();
    evidence_refs.extend(objective.linked_run_ids.clone());
    evidence_refs.push(format!("run:{}", source_run.run_id));
    evidence_refs.sort();
    evidence_refs.dedup();
    evidence_refs.truncate(128);
    let candidate_final = Some(format!(
        "Terminal objective run {} completed with state {}.",
        source_run.run_id, source_run.state
    ));
    let input = ObjectiveJudgeInput::from_objective(objective, candidate_final, evidence_refs);
    let judge_payload_json = serde_json::to_string(&input).map_err(|error| {
        Status::internal(format!("failed to encode objective judge input: {error}"))
    })?;
    let contract_sha256 = objective_contract_sha256(objective)?;
    let (_, source_generation) = runtime
        .persisted_runtime_generation_for_run(source_run.run_id.clone())
        .await?
        .ok_or_else(|| {
            Status::failed_precondition(
                "objective source run is missing persisted generation authority",
            )
        })?;
    let attempt_id = Ulid::new().to_string();
    let judge_task_id = Ulid::new().to_string();
    let request = ObjectiveAttemptReserveRequest {
        attempt_id,
        objective_id: objective.objective_id.clone(),
        routine_id,
        session_id: source_run.session_id.clone(),
        root_run_id: root_run_id.to_owned(),
        source_run_id: source_run.run_id.clone(),
        source_run_generation: source_generation.get(),
        judge_task_id,
        owner_principal: session.principal,
        device_id: session.device_id,
        channel: session.channel,
        judge_payload_json,
        contract_sha256,
        budget_tokens: objective
            .budget
            .max_tokens
            .unwrap_or(OBJECTIVE_JUDGE_BUDGET_TOKENS)
            .clamp(1, OBJECTIVE_JUDGE_BUDGET_TOKENS),
        workgraph_id: None,
    };
    let runtime_for_reserve = Arc::clone(runtime);
    let attempt = tokio::task::spawn_blocking(move || {
        runtime_for_reserve
            .journal_store
            .reserve_objective_attempt(&request)
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective attempt reservation worker panicked"))??;
    ensure_judge_task(runtime, &attempt).await?;
    mark_judge_enqueued(runtime, attempt.judge_task_id.clone()).await
}

async fn ensure_judge_task(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    if let Some(existing) =
        runtime.get_orchestrator_background_task(attempt.judge_task_id.clone()).await?
    {
        if existing.task_kind != AuxiliaryTaskKind::ObjectiveJudge.as_str()
            || existing.session_id != attempt.session_id
            || existing.payload_json.as_deref() != Some(attempt.judge_payload_json.as_str())
        {
            return Err(Status::failed_precondition(
                "objective judge task replay conflicts with durable attempt",
            ));
        }
        return Ok(());
    }
    runtime
        .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
            task_id: attempt.judge_task_id.clone(),
            task_kind: AuxiliaryTaskKind::ObjectiveJudge.as_str().to_owned(),
            session_id: attempt.session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: None,
            queued_input_id: None,
            owner_principal: attempt.owner_principal.clone(),
            device_id: attempt.device_id.clone(),
            channel: attempt.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: OBJECTIVE_JUDGE_PRIORITY,
            max_attempts: 1,
            budget_tokens: attempt.budget_tokens.max(1),
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: attempt.next_eligible_at_unix_ms,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: None,
            payload_json: Some(attempt.judge_payload_json.clone()),
        })
        .await?;
    Ok(())
}

async fn apply_decision(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    project_attempt(runtime, &attempt).await?;
    let guard = objective_guard_evaluation(runtime, attempt.attempt_id.clone()).await?.ok_or_else(
        || Status::failed_precondition("objective decision is missing its guard evaluation"),
    )?;
    if guard.disposition == ObjectiveGuardDisposition::Pause {
        return pause_guarded_attempt(
            runtime,
            &attempt,
            guard.reason_code.as_str(),
            objective_guard_pause_summary(&guard),
            &guard,
        )
        .await;
    }
    match attempt.decision {
        ObjectiveContinuationDecision::Continue => continue_after_decision(runtime, &attempt).await,
        ObjectiveContinuationDecision::Done => {
            finalize_after_decision(runtime, &attempt, &guard).await
        }
        ObjectiveContinuationDecision::Wait => {
            crate::application::wake_coordinator::register_objective_wait(runtime, &attempt).await
        }
        ObjectiveContinuationDecision::Blocked => {
            pause_attempt(
                runtime,
                &attempt,
                "objective.continuation.blocked",
                "Objective judge reported a blocking condition.",
            )
            .await
        }
        ObjectiveContinuationDecision::NeedsUser => {
            crate::application::wake_coordinator::register_objective_user_input(runtime, &attempt)
                .await?;
            pause_attempt(
                runtime,
                &attempt,
                "objective.continuation.needs_user",
                "Objective judge requires user input.",
            )
            .await
        }
        ObjectiveContinuationDecision::Pending => {
            Err(Status::failed_precondition("pending objective decision cannot be applied"))
        }
    }
}

async fn continue_after_decision(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    let lifecycle = runtime.daemon_lifecycle_snapshot()?;
    if lifecycle.phase != DaemonLifecyclePhase::Running {
        return pause_attempt(
            runtime,
            attempt,
            "objective.continuation.daemon_draining",
            "Daemon drain stopped autonomous continuation.",
        )
        .await;
    }
    let objective = load_objective(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("objective no longer exists"))?;
    let binding = objective_runtime_binding(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::failed_precondition("objective runtime binding is missing"))?;
    let current_contract_sha256 = objective_contract_sha256(&objective)?;
    if objective.state != ObjectiveState::Active
        || !objective.automation.enabled
        || binding.current_attempt_id != attempt.attempt_id
        || binding.session_id != attempt.session_id
        || binding.root_run_id != attempt.root_run_id
        || binding.current_run_generation != attempt.source_run_generation
        || current_contract_sha256 != attempt.contract_sha256
        || binding.contract_sha256 != attempt.contract_sha256
    {
        return pause_attempt(
            runtime,
            attempt,
            "objective.continuation.policy_changed",
            "Objective policy changed after the judge snapshot.",
        )
        .await;
    }
    let completed_turns = objective.attempt_history.len() as u32;
    let max_turns = objective.contract.max_turns.or(objective.budget.max_runs);
    if max_turns.is_some_and(|limit| completed_turns >= limit) {
        return pause_attempt(
            runtime,
            attempt,
            "objective.continuation.budget_exhausted",
            "Objective continuation turn budget is exhausted.",
        )
        .await;
    }
    if has_active_user_input(runtime, attempt.session_id.as_str()).await? {
        return pause_attempt(
            runtime,
            attempt,
            "objective.continuation.user_preempted",
            "Queued user input preempted autonomous continuation.",
        )
        .await;
    }
    crate::application::wake_coordinator::register_objective_wait(runtime, attempt).await
}

async fn ensure_continuation_task(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    let task_id = attempt.continuation_task_id.as_ref().ok_or_else(|| {
        Status::failed_precondition("objective continuation task identity is missing")
    })?;
    if let Some(existing) = runtime.get_orchestrator_background_task(task_id.clone()).await? {
        if existing.task_kind != AuxiliaryTaskKind::BackgroundPrompt.as_str()
            || existing.session_id != attempt.session_id
        {
            return Err(Status::failed_precondition(
                "objective continuation task replay conflicts with durable attempt",
            ));
        }
        return Ok(());
    }
    let objective = load_objective(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("objective no longer exists"))?;
    let prompt = attempt.next_action.clone().unwrap_or_else(|| {
        format!(
            "Continue objective '{}'. Re-evaluate its success contract before concluding.",
            objective.name
        )
    });
    let planned_run_id = Ulid::new().to_string();
    let budget_tokens = objective
        .budget
        .max_tokens
        .unwrap_or(OBJECTIVE_CONTINUATION_DEFAULT_BUDGET_TOKENS)
        .clamp(1, OBJECTIVE_CONTINUATION_DEFAULT_BUDGET_TOKENS);
    runtime
        .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
            task_id: task_id.clone(),
            task_kind: AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned(),
            session_id: attempt.session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: Some(planned_run_id),
            queued_input_id: None,
            owner_principal: attempt.owner_principal.clone(),
            device_id: attempt.device_id.clone(),
            channel: attempt.channel.clone(),
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: OBJECTIVE_CONTINUATION_PRIORITY,
            max_attempts: 1,
            budget_tokens,
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: attempt.next_eligible_at_unix_ms,
            expires_at_unix_ms: None,
            notification_target_json: None,
            input_text: Some(prompt),
            payload_json: Some(
                json!({
                    "parameter_delta": {
                        "objective_continuation": {
                            "schema_version": 1,
                            "objective_id": attempt.objective_id,
                            "attempt_id": attempt.attempt_id,
                            "root_run_id": attempt.root_run_id,
                            "source_run_id": attempt.source_run_id,
                            "source_run_generation": attempt.source_run_generation,
                            "contract_sha256": attempt.contract_sha256,
                            "reason_code": "objective.continuation.internal_turn",
                        }
                    }
                })
                .to_string(),
            ),
        })
        .await?;
    Ok(())
}

async fn pause_attempt(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
    reason_code: &str,
    summary: &str,
) -> Result<(), Status> {
    update_objective_projection(
        runtime,
        attempt,
        ObjectiveProjectionTransition::Paused { reason_code, summary, guard: None },
    )
    .await?;
    mark_attempt_applied(runtime, attempt.attempt_id.clone(), "settled", reason_code).await?;
    Ok(())
}

async fn pause_guarded_attempt(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
    reason_code: &str,
    summary: &str,
    guard: &ObjectiveGuardEvaluation,
) -> Result<(), Status> {
    update_objective_projection(
        runtime,
        attempt,
        ObjectiveProjectionTransition::Paused { reason_code, summary, guard: Some(guard) },
    )
    .await?;
    mark_attempt_applied(runtime, attempt.attempt_id.clone(), "settled", reason_code).await?;
    Ok(())
}

async fn finalize_after_decision(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
    guard: &ObjectiveGuardEvaluation,
) -> Result<(), Status> {
    let objective = load_objective(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("objective no longer exists"))?;
    match objective.contract.finalization_policy.mode {
        ObjectiveFinalizationMode::AutomaticWhenSatisfied => {
            complete_attempt(runtime, attempt, guard).await
        }
        ObjectiveFinalizationMode::ManualReview => {
            pause_guarded_attempt(
                runtime,
                attempt,
                "objective.continuation.manual_review_required",
                "Objective verification passed and awaits manual review.",
                guard,
            )
            .await
        }
        ObjectiveFinalizationMode::NeverAutomatic => {
            pause_guarded_attempt(
                runtime,
                attempt,
                "objective.continuation.automatic_finalization_disabled",
                "Objective verification passed, but its contract forbids automatic completion.",
                guard,
            )
            .await
        }
    }
}

async fn complete_attempt(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
    guard: &ObjectiveGuardEvaluation,
) -> Result<(), Status> {
    update_objective_projection(
        runtime,
        attempt,
        ObjectiveProjectionTransition::Completed {
            reason_code: guard.reason_code.as_str(),
            summary: "Objective completion passed persisted verification and runaway guards.",
            guard,
        },
    )
    .await?;
    mark_attempt_applied(
        runtime,
        attempt.attempt_id.clone(),
        "settled",
        guard.reason_code.as_str(),
    )
    .await?;
    Ok(())
}

async fn project_attempt(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    update_objective_projection(runtime, attempt, ObjectiveProjectionTransition::None).await
}

#[derive(Debug, Clone, Copy)]
enum ObjectiveProjectionTransition<'a> {
    None,
    Paused { reason_code: &'a str, summary: &'a str, guard: Option<&'a ObjectiveGuardEvaluation> },
    Completed { reason_code: &'a str, summary: &'a str, guard: &'a ObjectiveGuardEvaluation },
}

async fn update_objective_projection(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
    transition: ObjectiveProjectionTransition<'_>,
) -> Result<(), Status> {
    let config = runtime.routines_runtime_config()?;
    let registry = Arc::clone(&config.objectives);
    let attempt = attempt.clone();
    let transition = match transition {
        ObjectiveProjectionTransition::None => None,
        ObjectiveProjectionTransition::Paused { reason_code, summary, guard } => Some((
            ObjectiveState::Paused,
            "objective_continuation_paused".to_owned(),
            reason_code.to_owned(),
            summary.to_owned(),
            guard.map(|guard| guard.fingerprint.verification_evidence_json.clone()),
            guard.map(|guard| guard.fingerprint.missing_artifacts_json.clone()),
        )),
        ObjectiveProjectionTransition::Completed { reason_code, summary, guard } => Some((
            ObjectiveState::Completed,
            "objective_continuation_completed".to_owned(),
            reason_code.to_owned(),
            summary.to_owned(),
            Some(guard.fingerprint.verification_evidence_json.clone()),
            Some(guard.fingerprint.missing_artifacts_json.clone()),
        )),
    };
    tokio::task::spawn_blocking(move || {
        let Some(mut objective) = registry
            .get_objective(attempt.objective_id.as_str())
            .map_err(objective_registry_status)?
        else {
            return Err(Status::not_found("objective no longer exists"));
        };
        let evidence_refs =
            serde_json::from_str::<Vec<String>>(attempt.evidence_refs_json.as_str())
                .unwrap_or_default();
        let mut projected_attempt = ObjectiveAttemptRecord {
            attempt_id: attempt.attempt_id.clone(),
            run_id: Some(attempt.source_run_id.clone()),
            session_id: Some(attempt.session_id.clone()),
            status: attempt.decision.as_str().to_owned(),
            outcome_kind: Some(attempt.reason_code.clone()),
            summary: attempt.summary_text.clone(),
            learned: objective_outcome_evidence_summary(
                evidence_refs.as_slice(),
                transition.as_ref().and_then(|entry| entry.4.as_deref()),
                transition.as_ref().and_then(|entry| entry.5.as_deref()),
            ),
            recommended_next_step: attempt.next_action.clone(),
            created_at_unix_ms: attempt.created_at_unix_ms,
            completed_at_unix_ms: Some(attempt.updated_at_unix_ms),
        };
        if let Some(existing) = objective
            .attempt_history
            .iter_mut()
            .find(|record| record.attempt_id == projected_attempt.attempt_id)
        {
            *existing = projected_attempt.clone();
        } else {
            objective.attempt_history.push(projected_attempt.clone());
        }
        objective.last_attempt = Some(projected_attempt.clone());
        if !objective.linked_run_ids.contains(&attempt.source_run_id) {
            objective.linked_run_ids.push(attempt.source_run_id.clone());
        }
        objective.next_recommended_step = attempt.next_action.clone();
        if let Some((to_state, action, reason_code, summary, _, _)) = transition {
            projected_attempt.status = to_state.as_str().to_owned();
            projected_attempt.outcome_kind = Some(reason_code.clone());
            if let Some(existing) = objective
                .attempt_history
                .iter_mut()
                .find(|record| record.attempt_id == projected_attempt.attempt_id)
            {
                *existing = projected_attempt.clone();
            }
            objective.last_attempt = Some(projected_attempt);
            let from_state = objective.state;
            objective.state = to_state;
            objective.automation.enabled = false;
            if !objective.lifecycle_history.iter().any(|event| event.event_id == attempt.attempt_id)
            {
                objective.lifecycle_history.push(ObjectiveLifecycleRecord {
                    event_id: attempt.attempt_id.clone(),
                    action,
                    from_state: Some(from_state),
                    to_state,
                    reason: Some(format!("{reason_code}: {summary}")),
                    run_id: Some(attempt.source_run_id.clone()),
                    occurred_at_unix_ms: attempt.updated_at_unix_ms,
                });
            }
        }
        registry
            .upsert_objective(ObjectiveUpsert { record: objective })
            .map_err(objective_registry_status)?;
        Ok(())
    })
    .await
    .map_err(|_| Status::internal("objective projection worker panicked"))?
}

fn objective_outcome_evidence_summary(
    judge_evidence_refs: &[String],
    verification_evidence_json: Option<&str>,
    missing_artifacts_json: Option<&str>,
) -> Option<String> {
    let mut parts = Vec::new();
    if !judge_evidence_refs.is_empty() {
        parts.push(format!("Judge evidence: {}", judge_evidence_refs.join(", ")));
    }
    if let Some(verification) = verification_evidence_json.filter(|value| *value != "null") {
        parts.push(format!("Verification: {verification}"));
    }
    if let Some(missing) = missing_artifacts_json.filter(|value| *value != "[]") {
        parts.push(format!("Missing artifacts: {missing}"));
    }
    (!parts.is_empty()).then(|| truncate_utf8_bytes(parts.join("; "), 2_000))
}

fn truncate_utf8_bytes(mut value: String, maximum: usize) -> String {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum.saturating_sub(3);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str("...");
    value
}

async fn judge_decision_from_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<ObjectiveJudgeDecisionRequest, Status> {
    let output = task
        .result_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| value.get("objective_judge").cloned())
        .and_then(|value| serde_json::from_value::<ObjectiveJudgeOutput>(value).ok());
    let (decision, reason_code, summary, evidence_refs, next_action, retry_count, next_eligible) =
        match output {
            Some(output) => {
                let decision = match output.status {
                    ObjectiveJudgeStatus::Done => ObjectiveContinuationDecision::Done,
                    ObjectiveJudgeStatus::Continue => ObjectiveContinuationDecision::Continue,
                    ObjectiveJudgeStatus::Wait => ObjectiveContinuationDecision::Wait,
                    ObjectiveJudgeStatus::Blocked => ObjectiveContinuationDecision::Blocked,
                    ObjectiveJudgeStatus::NeedsUser => ObjectiveContinuationDecision::NeedsUser,
                };
                let retry_count =
                    if output.degraded { attempt.retry_count.saturating_add(1) } else { 0 };
                let decision = if output.degraded && retry_count >= OBJECTIVE_PARSE_RETRY_LIMIT {
                    ObjectiveContinuationDecision::Blocked
                } else {
                    decision
                };
                let backoff = output
                    .backoff_ms
                    .and_then(|value| i64::try_from(value).ok())
                    .map(|value| crate::gateway::current_unix_ms().saturating_add(value));
                (
                    decision,
                    output.reason_code,
                    output.summary,
                    output.evidence_refs,
                    output.next_action,
                    retry_count,
                    backoff,
                )
            }
            None => (
                ObjectiveContinuationDecision::Wait,
                "objective_judge_result_missing".to_owned(),
                "Objective judge result was unavailable.".to_owned(),
                Vec::new(),
                None,
                attempt.retry_count.saturating_add(1),
                Some(crate::gateway::current_unix_ms().saturating_add(30_000)),
            ),
        };
    let evidence_refs_json = serde_json::to_string(&evidence_refs)
        .map_err(|error| Status::internal(format!("failed to encode judge evidence: {error}")))?;
    let objective = load_objective(runtime, attempt.objective_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("objective no longer exists"))?;
    let guard = crate::application::objective_guards::build_objective_guard_request(
        runtime,
        &objective,
        attempt,
        decision,
        retry_count > 0,
        evidence_refs_json.as_str(),
    )
    .await?;
    Ok(ObjectiveJudgeDecisionRequest {
        judge_task_id: task.task_id.clone(),
        decision,
        reason_code,
        summary_text: summary,
        evidence_refs_json,
        next_action,
        retry_count,
        next_eligible_at_unix_ms: next_eligible,
        guard,
    })
}

async fn settle_judge_decision(
    runtime: &Arc<GatewayRuntimeState>,
    request: ObjectiveJudgeDecisionRequest,
) -> Result<ObjectiveContinuationAttemptRecord, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .settle_objective_judge_decision(&request)
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective judge settlement worker panicked"))?
}

async fn mark_judge_enqueued(
    runtime: &Arc<GatewayRuntimeState>,
    judge_task_id: String,
) -> Result<ObjectiveContinuationAttemptRecord, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .mark_objective_judge_enqueued(judge_task_id.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective judge enqueue marker worker panicked"))?
}

async fn mark_attempt_applied(
    runtime: &Arc<GatewayRuntimeState>,
    attempt_id: String,
    target_state: &'static str,
    reason_code: &str,
) -> Result<ObjectiveContinuationAttemptRecord, Status> {
    let runtime = Arc::clone(runtime);
    let reason_code = reason_code.to_owned();
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .mark_objective_attempt_applied(attempt_id.as_str(), target_state, reason_code.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective attempt transition worker panicked"))?
}

async fn objective_attempt_for_judge_task(
    runtime: &Arc<GatewayRuntimeState>,
    task_id: String,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .objective_attempt_for_judge_task(task_id.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective judge lookup worker panicked"))?
}

async fn objective_attempt_for_continuation_task(
    runtime: &Arc<GatewayRuntimeState>,
    task_id: String,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .objective_attempt_for_continuation_task(task_id.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective continuation lookup worker panicked"))?
}

async fn objective_runtime_binding(
    runtime: &Arc<GatewayRuntimeState>,
    objective_id: String,
) -> Result<Option<crate::journal::objective_continuation::ObjectiveRuntimeBindingRecord>, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .objective_runtime_binding(objective_id.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective runtime binding lookup worker panicked"))?
}

async fn objective_guard_evaluation(
    runtime: &Arc<GatewayRuntimeState>,
    attempt_id: String,
) -> Result<Option<ObjectiveGuardEvaluation>, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .objective_guard_evaluation_for_attempt(attempt_id.as_str())
            .map_err(objective_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective guard lookup worker panicked"))?
}

fn objective_guard_pause_summary(guard: &ObjectiveGuardEvaluation) -> &'static str {
    if guard.reason_code.starts_with("objective.guard.budget.") {
        "Objective continuation exhausted its durable cross-run budget."
    } else if guard.reason_code.starts_with("objective.guard.verification_") {
        "Objective completion is missing persisted verification evidence."
    } else {
        "Objective continuation paused after repeated non-progress evidence."
    }
}

async fn load_objective(
    runtime: &Arc<GatewayRuntimeState>,
    objective_id: String,
) -> Result<Option<ObjectiveRecord>, Status> {
    let config = runtime.routines_runtime_config()?;
    let registry = Arc::clone(&config.objectives);
    tokio::task::spawn_blocking(move || {
        registry.get_objective(objective_id.as_str()).map_err(objective_registry_status)
    })
    .await
    .map_err(|_| Status::internal("objective registry read worker panicked"))?
}

async fn has_active_user_input(
    runtime: &Arc<GatewayRuntimeState>,
    session_id: &str,
) -> Result<bool, Status> {
    Ok(runtime
        .list_orchestrator_queued_inputs(session_id.to_owned())
        .await?
        .iter()
        .any(|input| matches!(input.state.as_str(), "pending" | "claimed" | "deferred")))
}

fn objective_contract_sha256(objective: &ObjectiveRecord) -> Result<String, Status> {
    let encoded = serde_json::to_vec(&objective.contract).map_err(|error| {
        Status::internal(format!("failed to encode objective contract snapshot: {error}"))
    })?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn is_terminal_task_state(state: &str) -> bool {
    AuxiliaryTaskState::from_str(state).is_some_and(AuxiliaryTaskState::is_terminal)
}

fn is_terminal_run_state(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled" | "expired")
}

fn objective_journal_status(error: crate::journal::JournalError) -> Status {
    Status::internal(format!("objective continuation journal error: {error}"))
}

fn objective_registry_status(error: crate::objectives::ObjectiveRegistryError) -> Status {
    Status::internal(format!("objective registry error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objective_outcome_preserves_verification_and_missing_artifacts() {
        let summary = objective_outcome_evidence_summary(
            &["test:judge".to_owned()],
            Some(r#"{"status":"verified","evidence_refs":["test:runtime"]}"#),
            Some(r#"["artifact:missing"]"#),
        )
        .expect("objective outcome should contain evidence");

        assert!(summary.contains("test:judge"));
        assert!(summary.contains("\"status\":\"verified\""));
        assert!(summary.contains("artifact:missing"));
    }

    #[test]
    fn objective_outcome_evidence_is_utf8_safe_and_bounded() {
        let summary = objective_outcome_evidence_summary(
            &[format!("evidence:{}", "\u{017e}".repeat(2_000))],
            None,
            None,
        )
        .expect("objective outcome should contain evidence");

        assert!(summary.len() <= 2_000);
        assert!(summary.ends_with("..."));
    }
}
