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
    application::run_stream::orchestration::{
        run_runtime_path_summary_payload, run_stream_harness_cancelled_tape_events,
        RunStreamHarnessLifecycle,
    },
    gateway::{cleanup_run_resources, current_unix_ms, GatewayRuntimeState, CANCELLED_REASON},
    journal::OrchestratorRunTerminalSettlementRequest,
    orchestrator::{RunLifecycleState, RunStateMachine},
    self_healing::WorkHeartbeatKind,
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

use super::{
    flow_control::RunStreamFlowControl,
    orchestration::RunStreamMessageProcessingOutcome,
    tape::{send_settled_final_status, status_tape_payload},
};

/// Records the first request-to-observation latency sample for one run.
pub(crate) fn record_run_interrupt_observation(
    runtime_state: &GatewayRuntimeState,
    flow_control: &RunStreamFlowControl,
) {
    if let Some(observation) = flow_control.take_interrupt_latency_observation(current_unix_ms()) {
        runtime_state.record_run_interrupt_latency(observation);
    }
}

/// Copies the durable cancellation reason and request timestamp into the live root authority.
#[allow(clippy::result_large_err)]
pub(crate) async fn request_persisted_run_interrupt(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    flow_control: &RunStreamFlowControl,
) -> Result<bool, Status> {
    let Some(snapshot) = runtime_state.orchestrator_run_status_snapshot(run_id.to_owned()).await?
    else {
        return Ok(false);
    };
    if !snapshot.cancel_requested {
        return Ok(false);
    }
    let reason = snapshot.cancel_reason.as_deref().unwrap_or(CANCELLED_REASON);
    flow_control.request_cancel_from_persisted_reason(reason, snapshot.updated_at_unix_ms);
    Ok(true)
}

/// Settles an in-flight run stream after cancellation was observed.
///
/// Returns the effective durable terminal state because an earlier `done` or
/// `failed` settlement may already be sticky. Cleanup and cancelled wire
/// delivery run only when this call owns a newly committed cancellation.
///
/// # Errors
///
/// Returns `Status::internal` when the state machine rejects the `Cancel`
/// transition, or any journal error from persisting the cancelled state. A
/// persistence failure retains the run heartbeat and cleanup authority because
/// generation invalidation is not known to have committed. Failures while
/// emitting the terminal status event are forwarded to the client best-effort
/// instead of being returned, so cleanup still runs after durable settlement.
#[allow(clippy::result_large_err)]
pub(crate) async fn transition_run_stream_to_cancelled(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    flow_control: &RunStreamFlowControl,
    tape_seq: &mut i64,
    harness_lifecycle: Option<&RunStreamHarnessLifecycle>,
) -> Result<RunLifecycleState, Status> {
    if !request_persisted_run_interrupt(runtime_state, run_id, flow_control).await? {
        flow_control.request_cancel_from_reason(CANCELLED_REASON);
    }
    record_run_interrupt_observation(runtime_state, flow_control);
    let cancelled_outcome = Ok(RunStreamMessageProcessingOutcome::Terminate);
    let terminal_summary_payload_json = Some(run_runtime_path_summary_payload(
        runtime_state,
        RunLifecycleState::Cancelled,
        &cancelled_outcome,
        Some("run_stream.cancel"),
    )?);
    let settlement_result = runtime_state
        .settle_orchestrator_run_terminal(OrchestratorRunTerminalSettlementRequest {
            run_id: run_id.to_owned(),
            requested_state: RunLifecycleState::Cancelled,
            reason_code: "runtime.terminal.cancelled".to_owned(),
            status_message: CANCELLED_REASON.to_owned(),
            actor: palyra_common::runtime_contracts::RuntimeActorRef {
                kind: palyra_common::runtime_contracts::RuntimeActorKind::System,
                id: "run_stream.cancel".to_owned(),
            },
            terminal_summary_payload_json,
            terminal_tape_events: run_stream_harness_cancelled_tape_events(harness_lifecycle),
            terminal_status_payload_json: status_tape_payload(
                common_v1::stream_status::StatusKind::Failed,
                CANCELLED_REASON,
            ),
        })
        .await;
    let settlement = match settlement_result {
        Ok(settlement) => settlement,
        Err(error) => {
            // Retain both the heartbeat and exact cleanup authority until a later
            // recovery attempt proves that the run generation is terminal.
            return Err(error);
        }
    };
    let transition = settlement
        .effective_state
        .terminal_transition()
        .ok_or_else(|| Status::internal("terminal settlement returned a nonterminal run state"))?;
    run_state.transition(transition).map_err(|error| Status::internal(error.to_string()))?;
    if settlement.changed {
        runtime_state.clear_self_healing_heartbeat(WorkHeartbeatKind::Run, run_id);
        cleanup_run_resources(runtime_state, run_id, CANCELLED_REASON).await;
        // The wire status is Failed (the proto has no dedicated cancelled kind);
        // the durable tape payload records the canonical cancelled lifecycle.
        if let Some(settled_tape_sequence) = settlement.tape_sequence {
            let delivery = flow_control.delivery()?;
            if let Err(error) = send_settled_final_status(
                sender,
                runtime_state,
                run_id,
                tape_seq,
                settled_tape_sequence,
                common_v1::stream_status::StatusKind::Failed,
                CANCELLED_REASON,
                &delivery,
            )
            .await
            {
                // Best-effort: the client may already be gone; durable settlement is complete.
                let _ = sender.try_send(Err(error));
            }
        }
    }
    Ok(settlement.effective_state)
}
