use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Status;

use crate::{
    gateway::{GatewayRuntimeState, CANCELLED_REASON},
    orchestrator::{RunLifecycleState, RunStateMachine, RunTransition},
    transport::grpc::proto::palyra::common::v1 as common_v1,
};

use super::tape::send_status_with_tape;

#[allow(clippy::result_large_err)]
pub(crate) async fn transition_run_stream_to_cancelled(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    run_id: &str,
    tape_seq: &mut i64,
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
    if let Err(error) = send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        CANCELLED_REASON,
    )
    .await
    {
        let _ = sender.send(Err(error)).await;
    }
    Ok(())
}
