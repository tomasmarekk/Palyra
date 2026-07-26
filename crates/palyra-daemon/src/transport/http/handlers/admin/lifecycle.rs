//! Authenticated process lifecycle status and drain controls.

use crate::*;
use application::daemon_lifecycle::{
    DaemonDrainRequest, DaemonDrainTrigger, DaemonLifecycleSnapshot, DrainAdmissionPolicy,
};

const DEFAULT_ADMIN_DRAIN_TIMEOUT_MS: u64 = 30_000;
const MAX_ADMIN_DRAIN_TIMEOUT_MS: u64 = 300_000;

/// Optional controls for an authenticated admin drain request.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdminDaemonDrainRequest {
    /// Maximum time to let active runs reach a safe boundary.
    timeout_ms: Option<u64>,
    /// Whether new work must fail or may enter the existing durable queue.
    admission_policy: Option<DrainAdmissionPolicy>,
}

/// Exact drain epoch to cancel before checkpointing starts.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdminDaemonDrainCancelRequest {
    /// Drain epoch returned by the start endpoint.
    epoch: u64,
}

/// Returns the committed process lifecycle snapshot.
///
/// # Errors
/// Returns an HTTP error when authorization, context extraction, or snapshot
/// collection fails.
pub(crate) async fn admin_daemon_lifecycle_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DaemonLifecycleSnapshot>, Response> {
    authorize_admin(&state, &headers)?;
    let snapshot = state.runtime.daemon_lifecycle_snapshot().map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

/// Commits a drain boundary and starts the coordinator.
///
/// # Errors
/// Returns an HTTP error when authorization, validation, or durable lifecycle
/// transition fails.
pub(crate) async fn admin_daemon_lifecycle_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AdminDaemonDrainRequest>,
) -> Result<Json<DaemonLifecycleSnapshot>, Response> {
    let principal = authorize_admin(&state, &headers)?;
    let timeout_ms = payload
        .timeout_ms
        .unwrap_or(DEFAULT_ADMIN_DRAIN_TIMEOUT_MS)
        .min(MAX_ADMIN_DRAIN_TIMEOUT_MS);
    let timeout_ms = i64::try_from(timeout_ms).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "daemon drain timeout exceeds the supported range",
        ))
    })?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock for daemon drain: {error}"
        )))
    })?;
    let snapshot = state
        .runtime
        .spawn_daemon_drain(DaemonDrainRequest {
            trigger: DaemonDrainTrigger::Admin,
            reason_code: "daemon.lifecycle.admin_drain".to_owned(),
            requested_by: principal,
            deadline_unix_ms: now.saturating_add(timeout_ms),
            admission_policy: payload.admission_policy.unwrap_or(DrainAdmissionPolicy::RejectNew),
        })
        .map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

/// Cancels a drain before its durable checkpoint boundary.
///
/// # Errors
/// Returns an HTTP error when authorization, epoch validation, or the durable
/// cancellation transition fails.
pub(crate) async fn admin_daemon_lifecycle_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AdminDaemonDrainCancelRequest>,
) -> Result<Json<DaemonLifecycleSnapshot>, Response> {
    let principal = authorize_admin(&state, &headers)?;
    let snapshot = state
        .runtime
        .cancel_daemon_drain(payload.epoch, principal)
        .map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

#[allow(
    clippy::result_large_err,
    reason = "admin handlers share the concrete Axum response error type"
)]
fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<String, Response> {
    authorize_headers(headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    request_context_from_headers(headers).map(|context| context.principal).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })
}
