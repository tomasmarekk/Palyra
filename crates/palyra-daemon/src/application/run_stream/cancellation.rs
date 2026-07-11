//! Shared cancel transition for run streams.
//!
//! Every cancellation path in the run-stream pipeline (cancel commands,
//! orchestrator cancel requests observed mid-turn, cancelled tool batches)
//! funnels through this module so the state machine, journal, heartbeat,
//! tape, and resource cleanup always agree on the terminal state.

use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Status;

use crate::{
    gateway::{cleanup_run_resources, GatewayRuntimeState, CANCELLED_REASON},
    orchestrator::{RunLifecycleState, RunStateMachine, RunTransition},
    self_healing::WorkHeartbeatKind,
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

use super::tape::send_final_status_with_tape;

/// Transitions an in-flight run stream to the cancelled terminal state.
///
/// Applies the `Cancel` state-machine transition, persists the cancelled
/// lifecycle state, clears the self-healing heartbeat, emits the terminal
/// status event (with tape append), and releases run-scoped resources.
///
/// # Errors
///
/// Returns `Status::internal` when the state machine rejects the `Cancel`
/// transition, or any journal error from persisting the cancelled state.
/// Failures while emitting the terminal status event are forwarded to the
/// client best-effort instead of being returned, so cleanup still runs.
#[allow(clippy::result_large_err)]
pub(crate) async fn transition_run_stream_to_cancelled(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    transition_run_stream_state_to_cancelled(runtime_state, run_state, run_id).await?;
    runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
    // The wire status is Failed (the proto has no dedicated cancelled kind);
    // the tape payload maps CANCELLED_REASON to the "cancelled" lifecycle so
    // replay can distinguish controlled cancellation from real failures.
    if let Err(error) = send_final_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        CANCELLED_REASON,
    )
    .await
    {
        // Best-effort: the client may already be gone; cleanup must still run.
        let _ = sender.send(Err(error)).await;
    }
    cleanup_run_resources(runtime_state, run_id, CANCELLED_REASON).await;
    Ok(())
}

/// Applies and persists the canonical cancelled transition without emitting terminal effects.
///
/// This lower-level boundary lets finalizers insert durable evidence before
/// the terminal status while ordinary cancellation paths retain the shared
/// status and cleanup behavior above.
///
/// # Errors
///
/// Returns `Status::internal` when the state machine rejects the transition,
/// or a journal error when the cancelled state cannot be persisted.
#[allow(clippy::result_large_err)]
pub(crate) async fn transition_run_stream_state_to_cancelled(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
) -> Result<(), Status> {
    run_state
        .transition(RunTransition::Cancel)
        .map_err(|error| Status::internal(error.to_string()))?;
    runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Cancelled,
            Some(CANCELLED_REASON.to_owned()),
        )
        .await?;
    Ok(())
}
