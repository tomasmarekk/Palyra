//! Event-driven delivery for durable typed wait barriers.
//!
//! SQLite remains the replay authority. Tokio notifications reduce latency,
//! while one bounded fallback deadline covers receiver lag and restart gaps.

use std::{sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, TimeZone, Timelike, Utc};
use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState};
use serde::Deserialize;
use serde_json::json;
use tonic::Status;
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::daemon_lifecycle::DaemonLifecyclePhase,
    gateway::GatewayRuntimeState,
    journal::{
        objective_continuation::{
            ObjectiveContinuationAttemptRecord, ObjectiveContinuationTaskReserveOutcome,
        },
        wait_coordinator::{
            WaitBarrierCreateRequest, WaitBarrierKind, WaitBarrierV1, WakeDecision,
            WakeEventRequest, WakeIntentV1, WakeTaskReserveOutcome,
        },
        OrchestratorBackgroundTaskCreateRequest, OrchestratorBackgroundTaskRecord,
        OrchestratorBackgroundTaskWorkerUpdateRequest,
    },
};

const WAKE_RECOVERY_FALLBACK: Duration = Duration::from_secs(30);
const WAKE_TRANSIENT_DEFER_MS: i64 = 1_000;
const WAKE_PRIORITY: i64 = -110;
const OBJECTIVE_WAIT_DEFAULT_MS: i64 = 30_000;

/// One bounded reconciliation pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WakeCoordinatorReport {
    pub(crate) scanned: u64,
    pub(crate) delivered_only: u64,
    pub(crate) queued: u64,
    pub(crate) deferred: u64,
    pub(crate) cancelled: u64,
    pub(crate) errors: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveHoursUtc {
    start_hour_utc: u32,
    end_hour_utc: u32,
}

/// Runs the coordinator until the daemon lifecycle stops background subsystems.
pub(crate) fn spawn_wake_coordinator(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lifecycle = runtime.daemon_lifecycle.subscribe();
        loop {
            if lifecycle.borrow().phase.stops_subsystems() {
                break;
            }
            if let Err(error) = reconcile_wakes(&runtime).await {
                warn!(
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "wake coordinator reconciliation failed"
                );
            }
            let delay = next_coordinator_delay(&runtime).await.unwrap_or(WAKE_RECOVERY_FALLBACK);
            tokio::select! {
                () = runtime.orchestrator_run_notify.notified() => {}
                () = tokio::time::sleep(delay) => {}
                changed = lifecycle.changed() => {
                    if changed.is_err() || lifecycle.borrow().phase.stops_subsystems() {
                        break;
                    }
                }
            }
        }
    })
}

/// Reconciles due barriers and materializes at most one continuation per intent.
pub(crate) async fn reconcile_wakes(
    runtime: &Arc<GatewayRuntimeState>,
) -> Result<WakeCoordinatorReport, Status> {
    // The parent-continuation projection remains atomic. Its bounded
    // reconciliation shares this coordinator's earliest persisted deadline,
    // replacing one Tokio timer per suspended parent.
    runtime.reconcile_parent_suspensions().await?;
    let now = crate::gateway::current_unix_ms();
    let state = Arc::clone(runtime);
    let intents = tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .materialize_due_wait_barriers(now)
            .and_then(|_| state.journal_store.ready_wake_intents(now))
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake coordinator journal worker panicked"))??;
    let mut report = WakeCoordinatorReport { scanned: intents.len() as u64, ..Default::default() };
    for intent in intents {
        match deliver_intent(runtime, &intent, now).await {
            Ok(WakeDelivery::DeliveryOnly) => report.delivered_only += 1,
            Ok(WakeDelivery::Queued) => report.queued += 1,
            Ok(WakeDelivery::Deferred) => report.deferred += 1,
            Ok(WakeDelivery::Cancelled) => report.cancelled += 1,
            Ok(WakeDelivery::AlreadyMaterialized) => {}
            Err(error) => {
                report.errors += 1;
                warn!(
                    intent_id = %intent.intent_id,
                    status_code = ?error.code(),
                    status_message = %error.message(),
                    "wake intent delivery failed"
                );
            }
        }
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeDelivery {
    DeliveryOnly,
    Queued,
    Deferred,
    Cancelled,
    AlreadyMaterialized,
}

async fn deliver_intent(
    runtime: &Arc<GatewayRuntimeState>,
    intent: &WakeIntentV1,
    now_unix_ms: i64,
) -> Result<WakeDelivery, Status> {
    if let Some(task_id) = intent.continuation_task_id.as_deref() {
        if runtime.get_orchestrator_background_task(task_id.to_owned()).await?.is_some() {
            return Ok(WakeDelivery::AlreadyMaterialized);
        }
    }
    let barrier = wait_barrier(runtime, intent.intent_id.clone())
        .await?
        .ok_or_else(|| Status::failed_precondition("wake intent barrier is missing"))?;
    let desired_decision = match intent.decision {
        WakeDecision::Cancel | WakeDecision::DeliveryOnly => intent.decision,
        WakeDecision::Run | WakeDecision::Defer | WakeDecision::Coalesce => barrier.wake_decision,
    };
    match desired_decision {
        WakeDecision::Cancel => {
            settle_intent(
                runtime,
                intent.intent_id.clone(),
                "cancelled",
                WakeDecision::Cancel,
                "wake.cancelled",
            )
            .await?;
            return Ok(WakeDelivery::Cancelled);
        }
        WakeDecision::DeliveryOnly => {
            settle_intent(
                runtime,
                intent.intent_id.clone(),
                "delivered",
                WakeDecision::DeliveryOnly,
                "wake.delivery_only",
            )
            .await?;
            return Ok(WakeDelivery::DeliveryOnly);
        }
        WakeDecision::Defer => {
            defer_intent(
                runtime,
                intent.intent_id.clone(),
                "wake.source_requested_defer",
                now_unix_ms.saturating_add(WAKE_TRANSIENT_DEFER_MS),
            )
            .await?;
            return Ok(WakeDelivery::Deferred);
        }
        WakeDecision::Run | WakeDecision::Coalesce => {}
    }
    if runtime.daemon_lifecycle_snapshot()?.phase != DaemonLifecyclePhase::Running {
        defer_intent(
            runtime,
            intent.intent_id.clone(),
            "wake.daemon_draining",
            now_unix_ms.saturating_add(WAKE_RECOVERY_FALLBACK.as_millis() as i64),
        )
        .await?;
        return Ok(WakeDelivery::Deferred);
    }
    if let Some(next_active) = next_active_time(&barrier, now_unix_ms)? {
        defer_intent(runtime, intent.intent_id.clone(), "wake.outside_active_hours", next_active)
            .await?;
        return Ok(WakeDelivery::Deferred);
    }
    if barrier.budget_tokens == 0 {
        settle_intent(
            runtime,
            intent.intent_id.clone(),
            "cancelled",
            WakeDecision::Cancel,
            "wake.budget_exhausted",
        )
        .await?;
        return Ok(WakeDelivery::Cancelled);
    }
    if session_has_active_run(runtime, barrier.session_id.clone()).await? {
        defer_intent(
            runtime,
            intent.intent_id.clone(),
            "wake.session_busy",
            now_unix_ms.saturating_add(WAKE_TRANSIENT_DEFER_MS),
        )
        .await?;
        return Ok(WakeDelivery::Deferred);
    }
    let task_id = intent.continuation_task_id.clone().unwrap_or_else(|| Ulid::new().to_string());
    let reserved = reserve_wake_task(runtime, intent.intent_id.clone(), task_id.clone()).await?;
    let WakeTaskReserveOutcome::Reserved(reserved) = reserved else {
        return Ok(WakeDelivery::Cancelled);
    };
    let objective_attempt =
        reserve_objective_task_if_needed(runtime, &barrier, task_id.as_str()).await?;
    ensure_wake_task(runtime, &barrier, &reserved, objective_attempt.as_ref()).await?;
    if let Some(attempt) = objective_attempt {
        mark_objective_task_enqueued(runtime, attempt.attempt_id).await?;
    }
    runtime.orchestrator_run_notify.notify_waiters();
    Ok(WakeDelivery::Queued)
}

async fn ensure_wake_task(
    runtime: &Arc<GatewayRuntimeState>,
    barrier: &WaitBarrierV1,
    intent: &WakeIntentV1,
    objective_attempt: Option<&ObjectiveContinuationAttemptRecord>,
) -> Result<(), Status> {
    let task_id = intent.continuation_task_id.as_ref().ok_or_else(|| {
        Status::failed_precondition("reserved wake intent is missing its continuation task")
    })?;
    if runtime.get_orchestrator_background_task(task_id.clone()).await?.is_some() {
        return Ok(());
    }
    let state = Arc::clone(runtime);
    let session_id = barrier.session_id.clone();
    let session = tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .orchestrator_session_by_id_snapshot(session_id.as_str())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake session lookup worker panicked"))??
    .ok_or_else(|| Status::not_found("wake target session does not exist"))?;
    let prompt = barrier.continuation_prompt.clone().unwrap_or_else(|| {
        "Resume the suspended work. Re-check current state before taking action.".to_owned()
    });
    let payload = json!({
        "parameter_delta": {
            "wake_continuation": {
                "schema_version": 1,
                "barrier_id": barrier.barrier_id,
                "intent_id": intent.intent_id,
                "owner_kind": barrier.owner_kind,
                "owner_id": barrier.owner_id,
                "source_kind": intent.source_kind,
                "source_id": intent.source_id,
                "source_generation": intent.source_generation,
                "attempt_generation": intent.attempt_generation,
                "reason_code": intent.wake_reason,
                "objective_attempt_id": objective_attempt.map(|attempt| attempt.attempt_id.as_str()),
            }
        }
    });
    runtime
        .create_orchestrator_background_task(OrchestratorBackgroundTaskCreateRequest {
            task_id: task_id.clone(),
            task_kind: AuxiliaryTaskKind::BackgroundPrompt.as_str().to_owned(),
            session_id: barrier.session_id.clone(),
            child_session_id: None,
            parent_run_id: None,
            target_run_id: None,
            planned_child_run_id: Some(Ulid::new().to_string()),
            queued_input_id: None,
            owner_principal: session.principal,
            device_id: session.device_id,
            channel: session.channel,
            state: AuxiliaryTaskState::Queued.as_str().to_owned(),
            priority: WAKE_PRIORITY,
            max_attempts: 1,
            budget_tokens: barrier.budget_tokens,
            delegation: None,
            cancellation_context: None,
            not_before_unix_ms: None,
            expires_at_unix_ms: barrier.expires_at_unix_ms,
            notification_target_json: None,
            input_text: Some(prompt),
            payload_json: Some(payload.to_string()),
        })
        .await?;
    Ok(())
}

async fn reserve_objective_task_if_needed(
    runtime: &Arc<GatewayRuntimeState>,
    barrier: &WaitBarrierV1,
    task_id: &str,
) -> Result<Option<ObjectiveContinuationAttemptRecord>, Status> {
    if barrier.owner_kind != "objective_attempt" {
        return Ok(None);
    }
    let state = Arc::clone(runtime);
    let attempt_id = barrier.owner_id.clone();
    let task_id = task_id.to_owned();
    let outcome = tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .reserve_objective_continuation_task(
                attempt_id.as_str(),
                task_id.as_str(),
                "objective.continuation.wake_reserved",
            )
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective wake reservation worker panicked"))??;
    match outcome {
        ObjectiveContinuationTaskReserveOutcome::Reserved(attempt) => Ok(Some(attempt)),
        ObjectiveContinuationTaskReserveOutcome::UserPreempted(_) => {
            Err(Status::cancelled("user input preempted objective wake"))
        }
    }
}

async fn mark_objective_task_enqueued(
    runtime: &Arc<GatewayRuntimeState>,
    attempt_id: String,
) -> Result<(), Status> {
    let state = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .mark_objective_attempt_applied(
                attempt_id.as_str(),
                "continuation_enqueued",
                "objective.continuation.wake_enqueued",
            )
            .map(|_| ())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("objective wake transition worker panicked"))?
}

/// Registers an immediate continue or bounded objective wait on the shared contract.
pub(crate) async fn register_objective_wait(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    let now = crate::gateway::current_unix_ms();
    let wake_at_unix_ms = match attempt.decision {
        crate::journal::objective_continuation::ObjectiveContinuationDecision::Continue => now,
        crate::journal::objective_continuation::ObjectiveContinuationDecision::Wait => attempt
            .next_eligible_at_unix_ms
            .unwrap_or_else(|| now.saturating_add(OBJECTIVE_WAIT_DEFAULT_MS)),
        _ => {
            return Err(Status::failed_precondition(
                "only continue or wait can register an objective wake",
            ));
        }
    };
    let request = WaitBarrierCreateRequest {
        barrier_id: Ulid::new().to_string(),
        owner_kind: "objective_attempt".to_owned(),
        owner_id: attempt.attempt_id.clone(),
        session_id: attempt.session_id.clone(),
        root_run_id: Some(attempt.root_run_id.clone()),
        barrier_kind: WaitBarrierKind::TimeDeadline,
        source_kind: WaitBarrierKind::TimeDeadline.as_str().to_owned(),
        source_id: attempt.attempt_id.clone(),
        wake_decision: WakeDecision::Run,
        continuation_prompt: attempt.next_action.clone(),
        budget_tokens: attempt.budget_tokens.max(1),
        attempt_generation: attempt.source_run_generation,
        wake_at_unix_ms: Some(wake_at_unix_ms),
        expires_at_unix_ms: None,
        liveness_probe_json: json!({
            "schema_version": 1,
            "objective_id": attempt.objective_id,
            "attempt_id": attempt.attempt_id,
        })
        .to_string(),
        active_hours_json: None,
        stale_policy: "cancel".to_owned(),
        reason_code: "objective.continuation.wait_registered".to_owned(),
    };
    register_barrier(runtime, request).await?;
    Ok(())
}

/// Registers a delivery-only user-input barrier for a paused objective.
pub(crate) async fn register_objective_user_input(
    runtime: &Arc<GatewayRuntimeState>,
    attempt: &ObjectiveContinuationAttemptRecord,
) -> Result<(), Status> {
    register_barrier(
        runtime,
        WaitBarrierCreateRequest {
            barrier_id: Ulid::new().to_string(),
            owner_kind: "objective_attempt".to_owned(),
            owner_id: attempt.attempt_id.clone(),
            session_id: attempt.session_id.clone(),
            root_run_id: Some(attempt.root_run_id.clone()),
            barrier_kind: WaitBarrierKind::UserInput,
            source_kind: WaitBarrierKind::UserInput.as_str().to_owned(),
            source_id: attempt.session_id.clone(),
            wake_decision: WakeDecision::DeliveryOnly,
            continuation_prompt: None,
            budget_tokens: 0,
            attempt_generation: attempt.source_run_generation,
            wake_at_unix_ms: None,
            expires_at_unix_ms: None,
            liveness_probe_json: json!({
                "schema_version": 1,
                "objective_id": attempt.objective_id,
                "attempt_id": attempt.attempt_id,
            })
            .to_string(),
            active_hours_json: None,
            stale_policy: "cancel".to_owned(),
            reason_code: "objective.continuation.user_input_registered".to_owned(),
        },
    )
    .await?;
    Ok(())
}

/// Persists an authenticated external source event and notifies the coordinator.
pub(crate) async fn emit_wake_event(
    runtime: &Arc<GatewayRuntimeState>,
    event: WakeEventRequest,
) -> Result<usize, Status> {
    let state = Arc::clone(runtime);
    let intents = tokio::task::spawn_blocking(move || {
        state.journal_store.emit_wake_event(&event).map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake event journal worker panicked"))??;
    runtime.orchestrator_run_notify.notify_waiters();
    Ok(intents.len())
}

/// Rechecks user priority after a wake task claim and before RunStream admission.
pub(crate) async fn admit_claimed_wake_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    let Some(intent) = wake_intent_for_task(runtime, task.task_id.clone()).await? else {
        return Ok(true);
    };
    let lifecycle = runtime.daemon_lifecycle_snapshot()?;
    let preempted = session_has_active_queued_input(runtime, intent.session_id.clone()).await?;
    if lifecycle.phase == DaemonLifecyclePhase::Running && !preempted {
        return Ok(true);
    }
    let reason_code = if preempted { "wake.user_preempted" } else { "wake.daemon_draining" };
    runtime
        .update_orchestrator_background_task_from_worker(
            OrchestratorBackgroundTaskWorkerUpdateRequest {
                task_id: task.task_id.clone(),
                execution_generation: task.execution_generation,
                state: Some(AuxiliaryTaskState::Cancelled.as_str().to_owned()),
                target_run_id: None,
                last_error: Some(Some(reason_code.to_owned())),
                result_json: Some(Some(
                    json!({"status":"cancelled","reason_code":reason_code}).to_string(),
                )),
                started_at_unix_ms: None,
                completed_at_unix_ms: Some(Some(crate::gateway::current_unix_ms())),
            },
        )
        .await?;
    settle_intent(runtime, intent.intent_id, "cancelled", WakeDecision::Cancel, reason_code)
        .await?;
    Ok(false)
}

/// Settles the durable wake projection after its task reaches a terminal state.
pub(crate) async fn reconcile_terminal_wake_task(
    runtime: &Arc<GatewayRuntimeState>,
    task: &OrchestratorBackgroundTaskRecord,
) -> Result<bool, Status> {
    let Some(intent) = wake_intent_for_task(runtime, task.task_id.clone()).await? else {
        return Ok(false);
    };
    let state = match AuxiliaryTaskState::from_str(task.state.as_str()) {
        Some(AuxiliaryTaskState::Succeeded) => "delivered",
        Some(
            AuxiliaryTaskState::Failed
            | AuxiliaryTaskState::Cancelled
            | AuxiliaryTaskState::Expired,
        ) => "cancelled",
        _ => return Ok(true),
    };
    let decision = if state == "delivered" { WakeDecision::Run } else { WakeDecision::Cancel };
    settle_intent(runtime, intent.intent_id, state, decision, "wake.task_terminal").await?;
    Ok(true)
}

async fn register_barrier(
    runtime: &Arc<GatewayRuntimeState>,
    request: WaitBarrierCreateRequest,
) -> Result<WaitBarrierV1, Status> {
    let state = Arc::clone(runtime);
    let barrier = tokio::task::spawn_blocking(move || {
        state.journal_store.register_wait_barrier(&request).map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wait barrier journal worker panicked"))??;
    runtime.orchestrator_run_notify.notify_waiters();
    Ok(barrier)
}

async fn wait_barrier(
    runtime: &Arc<GatewayRuntimeState>,
    intent_id: String,
) -> Result<Option<WaitBarrierV1>, Status> {
    let state = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        state.journal_store.wait_barrier_for_intent(intent_id.as_str()).map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wait barrier lookup worker panicked"))?
}

async fn wake_intent_for_task(
    runtime: &Arc<GatewayRuntimeState>,
    task_id: String,
) -> Result<Option<WakeIntentV1>, Status> {
    let state = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        state.journal_store.wake_intent_for_task(task_id.as_str()).map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake task lookup worker panicked"))?
}

async fn reserve_wake_task(
    runtime: &Arc<GatewayRuntimeState>,
    intent_id: String,
    task_id: String,
) -> Result<WakeTaskReserveOutcome, Status> {
    let state = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .reserve_wake_task(intent_id.as_str(), task_id.as_str())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake task reservation worker panicked"))?
}

async fn settle_intent(
    runtime: &Arc<GatewayRuntimeState>,
    intent_id: String,
    state_name: &'static str,
    decision: WakeDecision,
    outcome: &'static str,
) -> Result<(), Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .settle_wake_intent(intent_id.as_str(), state_name, decision, outcome)
            .map(|_| ())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake settlement worker panicked"))?
}

async fn defer_intent(
    runtime: &Arc<GatewayRuntimeState>,
    intent_id: String,
    reason_code: &'static str,
    next_eligible_at_unix_ms: i64,
) -> Result<(), Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .defer_wake_intent(intent_id.as_str(), reason_code, next_eligible_at_unix_ms)
            .map(|_| ())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake defer worker panicked"))?
}

async fn session_has_active_run(
    runtime: &Arc<GatewayRuntimeState>,
    session_id: String,
) -> Result<bool, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .session_has_active_run(session_id.as_str())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake session-state worker panicked"))?
}

async fn session_has_active_queued_input(
    runtime: &Arc<GatewayRuntimeState>,
    session_id: String,
) -> Result<bool, Status> {
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        runtime
            .journal_store
            .session_has_active_queued_input(session_id.as_str())
            .map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake queued-input worker panicked"))?
}

async fn next_coordinator_delay(runtime: &Arc<GatewayRuntimeState>) -> Result<Duration, Status> {
    let state = Arc::clone(runtime);
    let deadline = tokio::task::spawn_blocking(move || {
        state.journal_store.next_wait_coordinator_deadline().map_err(wake_journal_status)
    })
    .await
    .map_err(|_| Status::internal("wake deadline worker panicked"))??;
    let Some(deadline) = deadline else {
        return Ok(WAKE_RECOVERY_FALLBACK);
    };
    let delay_ms = deadline.saturating_sub(crate::gateway::current_unix_ms()).max(0) as u64;
    Ok(Duration::from_millis(delay_ms).min(WAKE_RECOVERY_FALLBACK))
}

fn next_active_time(barrier: &WaitBarrierV1, now_unix_ms: i64) -> Result<Option<i64>, Status> {
    let Some(raw) = barrier.active_hours_json.as_deref() else {
        return Ok(None);
    };
    let hours = serde_json::from_str::<ActiveHoursUtc>(raw)
        .map_err(|error| Status::invalid_argument(format!("invalid active hours: {error}")))?;
    if hours.start_hour_utc > 23 || hours.end_hour_utc > 23 {
        return Err(Status::invalid_argument("active-hours values must be UTC hours 0..=23"));
    }
    if hours.start_hour_utc == hours.end_hour_utc {
        return Ok(None);
    }
    let now = Utc
        .timestamp_millis_opt(now_unix_ms)
        .single()
        .ok_or_else(|| Status::invalid_argument("active-hours timestamp is out of range"))?;
    let hour = now.hour();
    let active = if hours.start_hour_utc < hours.end_hour_utc {
        hour >= hours.start_hour_utc && hour < hours.end_hour_utc
    } else {
        hour >= hours.start_hour_utc || hour < hours.end_hour_utc
    };
    if active {
        return Ok(None);
    }
    let mut next = now
        .date_naive()
        .and_hms_opt(hours.start_hour_utc, 0, 0)
        .ok_or_else(|| Status::invalid_argument("active-hours boundary is invalid"))?
        .and_utc();
    if next <= now {
        next += ChronoDuration::days(1);
    }
    Ok(Some(next.timestamp_millis()))
}

fn wake_journal_status(error: crate::journal::JournalError) -> Status {
    Status::internal(format!("wake coordinator journal error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn barrier_with_active_hours(raw: &str) -> WaitBarrierV1 {
        WaitBarrierV1 {
            barrier_id: "barrier".to_owned(),
            owner_kind: "test".to_owned(),
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            root_run_id: None,
            barrier_kind: WaitBarrierKind::TimeDeadline,
            source_kind: "time_deadline".to_owned(),
            source_id: "source".to_owned(),
            state: "active".to_owned(),
            wake_decision: WakeDecision::Run,
            continuation_prompt: None,
            budget_tokens: 1,
            attempt_generation: 1,
            wake_at_unix_ms: None,
            expires_at_unix_ms: None,
            liveness_probe_json: "{}".to_owned(),
            active_hours_json: Some(raw.to_owned()),
            stale_policy: "cancel".to_owned(),
            reason_code: "test".to_owned(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    #[test]
    fn active_hours_defer_until_the_next_utc_boundary() {
        let barrier = barrier_with_active_hours(r#"{"start_hour_utc":8,"end_hour_utc":18}"#);
        let now = Utc.with_ymd_and_hms(2026, 7, 28, 7, 30, 0).single().unwrap();
        let next = next_active_time(&barrier, now.timestamp_millis())
            .expect("active hours should parse")
            .expect("outside hours should defer");
        assert_eq!(Utc.timestamp_millis_opt(next).single().unwrap().hour(), 8);
    }

    #[test]
    fn overnight_active_hours_include_late_and_early_hours() {
        let barrier = barrier_with_active_hours(r#"{"start_hour_utc":22,"end_hour_utc":6}"#);
        for hour in [23, 2] {
            let now = Utc.with_ymd_and_hms(2026, 7, 28, hour, 0, 0).single().unwrap();
            assert_eq!(next_active_time(&barrier, now.timestamp_millis()).unwrap(), None);
        }
    }
}
