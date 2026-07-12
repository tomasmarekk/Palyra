//! Authorized console access to the always-on, metadata-only run trace.

use crate::*;

use super::chat::run_matches_console_context;

/// Returns the bounded metadata trace for one run in the authenticated console context.
///
/// # Errors
/// Returns an error when authorization fails, the run id is malformed or unknown,
/// the run belongs to another console context, or durable trace validation fails.
pub(crate) async fn console_chat_run_metadata_trace_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    let run = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    if !run_matches_console_context(&run, &session.context) {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "chat run does not belong to the authenticated console session context",
        )));
    }
    let metadata_trace =
        state.runtime.metadata_trace_snapshot(run_id).await.map_err(runtime_status_response)?;
    Ok(Json(json!({
        "metadata_trace": metadata_trace,
        "contract": contract_descriptor(),
    })))
}
