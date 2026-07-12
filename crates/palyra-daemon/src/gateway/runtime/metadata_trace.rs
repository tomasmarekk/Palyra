//! Async gateway access to durable metadata-only run traces.

use std::sync::Arc;

use palyra_common::metadata_trace::MetadataTraceV1;
use tonic::Status;

use super::{map_orchestrator_store_error, GatewayRuntimeState};

impl GatewayRuntimeState {
    #[allow(clippy::result_large_err)]
    fn metadata_trace_snapshot_blocking(&self, run_id: &str) -> Result<MetadataTraceV1, Status> {
        self.journal_store
            .load_metadata_trace(run_id)
            .map_err(|error| map_orchestrator_store_error("load metadata trace", error))?
            .ok_or_else(|| {
                Status::failed_precondition(
                    "metadata trace is unavailable for a run created before schema version 1",
                )
            })
    }

    /// Loads and validates one run's bounded metadata trace on a blocking worker.
    ///
    /// # Errors
    /// Returns `not_found` for an unknown run, `failed_precondition` for a legacy
    /// run without trace storage, or the mapped durable validation/storage error.
    #[allow(clippy::result_large_err)]
    pub async fn metadata_trace_snapshot(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<MetadataTraceV1, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.metadata_trace_snapshot_blocking(run_id.as_str()))
            .await
            .map_err(|_| Status::internal("metadata trace snapshot worker panicked"))?
    }
}
