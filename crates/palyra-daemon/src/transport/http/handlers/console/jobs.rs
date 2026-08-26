//! Console HTTP handlers for long-running tool jobs (`/console/v1/jobs*`):
//! list, get, tail, lifecycle transitions (cancel/drain/resume), retry,
//! attach/release, and the sweep/recover maintenance endpoints.
//!
//! All access is scoped to the authenticated principal's jobs; every mutation
//! goes through [`authorize_console_job_mutation`], which requires the
//! console CSRF token (pinned by a const assertion in the tests module).
//!
//! Response JSON shapes are part of the `/console/v1` wire contract consumed
//! by `apps/web`; field names, status codes, and error strings must stay
//! byte-identical.

use crate::*;
use crate::{
    app::state::ConsoleSession,
    gateway::current_unix_ms,
    journal::{
        ToolJobAttachRequest, ToolJobRecord, ToolJobRetryRequest, ToolJobState,
        ToolJobTailReadRequest, ToolJobTransitionRequest, ToolJobsListFilter,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;

/// Query filters for `GET /console/v1/jobs`; terminal jobs are excluded
/// unless `include_terminal` is set.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleJobsListQuery {
    pub(crate) limit: Option<usize>,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) include_terminal: Option<bool>,
}

/// Window parameters for `GET /console/v1/jobs/{job_id}/tail`; negative
/// offsets are clamped to the start of the output.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleJobTailQuery {
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
    pub(crate) max_bytes: Option<usize>,
}

/// Body for job lifecycle actions (cancel/drain/resume/retry); the reason
/// defaults to an operation-specific `operator_*` audit tag, and the
/// idempotency key is honored by retry only.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleJobActionRequest {
    pub(crate) reason: Option<String>,
    pub(crate) idempotency_key: Option<String>,
}

/// Body for `POST /console/v1/jobs/sweep-expired`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleJobSweepRequest {
    pub(crate) limit: Option<usize>,
}

/// Body for `POST /console/v1/jobs/recover-stale`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleJobRecoverRequest {
    pub(crate) limit: Option<usize>,
}

/// Security posture pin: job mutations must always demand the console CSRF
/// token. A const assertion in the tests module fails the build if this is
/// ever flipped.
const REQUIRE_CSRF_FOR_JOB_MUTATION: bool = true;

/// Lists the principal's tool jobs (`GET /console/v1/jobs`).
///
/// # Errors
/// Returns an unauthorized response when console authorization fails, or a
/// mapped runtime error response when listing fails.
pub(crate) async fn console_jobs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleJobsListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let jobs = state
        .runtime
        .list_tool_jobs(ToolJobsListFilter {
            owner_principal: Some(session.context.principal.clone()),
            session_id: query.session_id,
            run_id: query.run_id,
            include_terminal: query.include_terminal.unwrap_or(false),
            limit: query.limit.unwrap_or(50),
        })
        .await
        .map_err(runtime_status_response)?;
    let next_cursor = jobs.last().map(|job| job.job_id.clone());
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "page": build_page_info(query.limit.unwrap_or(50), jobs.len(), next_cursor),
        "jobs": jobs,
    })))
}

/// Returns one owned tool job (`GET /console/v1/jobs/{job_id}`).
///
/// # Errors
/// Returns not-found for unknown jobs, permission-denied for jobs owned by
/// another principal, or a mapped runtime error response.
pub(crate) async fn console_job_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let job = load_authorized_job(&state, job_id, session.context.principal.as_str()).await?;
    Ok(Json(job_envelope(job)))
}

/// Reads a byte-bounded window of an owned job's output
/// (`GET /console/v1/jobs/{job_id}/tail`); ownership is enforced by the
/// runtime via the `owner_principal` filter.
///
/// # Errors
/// Returns invalid-argument for an empty job id, or a mapped runtime error
/// response (not-found, permission-denied).
pub(crate) async fn console_job_tail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Query(query): Query<ConsoleJobTailQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let page = state
        .runtime
        .tail_tool_job(ToolJobTailReadRequest {
            job_id: normalize_non_empty_field(job_id, "job_id")?,
            owner_principal: Some(session.context.principal.clone()),
            offset: query.offset.unwrap_or(0).max(0),
            limit: query.limit.unwrap_or(100),
            max_bytes: query.max_bytes.unwrap_or(16 * 1024),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "tail": page,
    })))
}

/// Requests cancellation of an owned job
/// (`POST /console/v1/jobs/{job_id}/cancel`).
///
/// # Errors
/// Returns unauthorized/CSRF failures, not-found or permission-denied for
/// missing/foreign jobs, or a mapped runtime transition error response.
pub(crate) async fn console_job_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleJobActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_authorized_job(
        state,
        headers,
        job_id,
        ToolJobState::Cancelling,
        payload.reason.unwrap_or_else(|| "operator_cancel".to_owned()),
        None,
    )
    .await
}

/// Requests a graceful drain of an owned job
/// (`POST /console/v1/jobs/{job_id}/drain`).
///
/// # Errors
/// Returns unauthorized/CSRF failures, not-found or permission-denied for
/// missing/foreign jobs, or a mapped runtime transition error response.
pub(crate) async fn console_job_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleJobActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_authorized_job(
        state,
        headers,
        job_id,
        ToolJobState::Draining,
        payload.reason.unwrap_or_else(|| "operator_drain".to_owned()),
        None,
    )
    .await
}

/// Requeues an owned job for execution
/// (`POST /console/v1/jobs/{job_id}/resume`).
///
/// # Errors
/// Returns unauthorized/CSRF failures, not-found or permission-denied for
/// missing/foreign jobs, or a mapped runtime transition error response.
pub(crate) async fn console_job_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleJobActionRequest>,
) -> Result<Json<Value>, Response> {
    transition_authorized_job(
        state,
        headers,
        job_id,
        ToolJobState::Queued,
        payload.reason.unwrap_or_else(|| "operator_resume".to_owned()),
        None,
    )
    .await
}

/// Retries an owned job, optionally idempotently via `idempotency_key`
/// (`POST /console/v1/jobs/{job_id}/retry`); ownership is enforced by the
/// runtime via the `owner_principal` filter.
///
/// # Errors
/// Returns unauthorized/CSRF failures, invalid-argument for an empty job id,
/// or a mapped runtime error response.
pub(crate) async fn console_job_retry_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleJobActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let job = state
        .runtime
        .retry_tool_job(ToolJobRetryRequest {
            job_id: normalize_non_empty_field(job_id, "job_id")?,
            owner_principal: Some(session.context.principal.clone()),
            idempotency_key: payload.idempotency_key,
            reason: payload.reason.unwrap_or_else(|| "operator_retry".to_owned()),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(job_envelope(job)))
}

/// Attaches the console to an owned job's output stream
/// (`POST /console/v1/jobs/{job_id}/attach`).
///
/// # Errors
/// Returns unauthorized/CSRF failures, invalid-argument for an empty job id,
/// or a mapped runtime error response.
pub(crate) async fn console_job_attach_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let job = state
        .runtime
        .attach_tool_job(ToolJobAttachRequest {
            job_id: normalize_non_empty_field(job_id, "job_id")?,
            owner_principal: Some(session.context.principal.clone()),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(job_envelope(job)))
}

/// Releases a previous attachment on an owned job
/// (`POST /console/v1/jobs/{job_id}/release`); ownership is checked
/// explicitly because the release call itself is not owner-filtered.
///
/// # Errors
/// Returns unauthorized/CSRF failures, not-found or permission-denied for
/// missing/foreign jobs, or a mapped runtime error response.
pub(crate) async fn console_job_release_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let job_id = normalize_non_empty_field(job_id, "job_id")?;
    let _job =
        load_authorized_job(&state, job_id.clone(), session.context.principal.as_str()).await?;
    let job =
        state.runtime.release_tool_job_attachment(job_id).await.map_err(runtime_status_response)?;
    Ok(Json(job_envelope(job)))
}

/// Sweeps jobs whose leases expired into a terminal state
/// (`POST /console/v1/jobs/sweep-expired`).
///
/// # Errors
/// Returns unauthorized/CSRF failures or a mapped runtime error response.
pub(crate) async fn console_jobs_sweep_expired_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleJobSweepRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let jobs = state
        .runtime
        .sweep_expired_tool_jobs(
            session.context.principal,
            current_unix_ms(),
            payload.limit.unwrap_or(100),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "jobs": jobs,
    })))
}

/// Recovers jobs whose heartbeat went stale, requeueing or failing them per
/// runtime policy (`POST /console/v1/jobs/recover-stale`).
///
/// # Errors
/// Returns unauthorized/CSRF failures or a mapped runtime error response.
pub(crate) async fn console_jobs_recover_stale_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleJobRecoverRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let jobs = state
        .runtime
        .recover_stale_tool_jobs(
            session.context.principal,
            current_unix_ms(),
            5 * 60 * 1_000,
            payload.limit.unwrap_or(100),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "jobs": jobs,
    })))
}

// Shared body of the cancel/drain/resume handlers: CSRF-authorized session,
// ownership check, then an unconditional (no expected_state) transition with
// a fresh heartbeat so the job does not immediately look stale.
async fn transition_authorized_job(
    state: AppState,
    headers: HeaderMap,
    job_id: String,
    next_state: ToolJobState,
    reason: String,
    last_error: Option<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_job_mutation(&state, &headers)?;
    let job_id = normalize_non_empty_field(job_id, "job_id")?;
    let _job =
        load_authorized_job(&state, job_id.clone(), session.context.principal.as_str()).await?;
    let job = state
        .runtime
        .transition_tool_job(ToolJobTransitionRequest {
            job_id,
            expected_state: None,
            next_state,
            reason,
            last_error,
            heartbeat_at_unix_ms: Some(current_unix_ms()),
            lease_expires_at_unix_ms: None,
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(job_envelope(job)))
}

// Single chokepoint for job mutations so the CSRF requirement cannot be
// dropped on one endpoint by accident.
#[allow(clippy::result_large_err)]
fn authorize_console_job_mutation(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<ConsoleSession, Response> {
    authorize_console_session(state, headers, REQUIRE_CSRF_FOR_JOB_MUTATION)
}

async fn load_authorized_job(
    state: &AppState,
    job_id: String,
    principal: &str,
) -> Result<ToolJobRecord, Response> {
    let job_id = normalize_non_empty_field(job_id, "job_id")?;
    let job = state
        .runtime
        .get_tool_job(job_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| runtime_status_response(Status::not_found("tool job not found")))?;
    if job.owner_principal != principal {
        return Err(runtime_status_response(Status::permission_denied(
            "tool job is outside the current console principal scope",
        )));
    }
    Ok(job)
}

fn job_envelope(job: ToolJobRecord) -> Value {
    json!({
        "contract": contract_descriptor(),
        "job": job,
    })
}

#[cfg(test)]
mod tests {
    use super::{ConsoleJobRecoverRequest, ConsoleJobSweepRequest, REQUIRE_CSRF_FOR_JOB_MUTATION};
    use serde_json::json;

    #[test]
    fn job_lifecycle_mutations_require_csrf() {
        const {
            assert!(
                REQUIRE_CSRF_FOR_JOB_MUTATION,
                "job reference and lifecycle mutations must require the console CSRF token",
            );
        }
    }

    #[test]
    fn job_maintenance_requests_reject_caller_controlled_clocks() {
        serde_json::from_value::<ConsoleJobSweepRequest>(json!({
            "now_unix_ms": i64::MAX,
            "limit": 1
        }))
        .expect_err("sweep clock must be server-owned");
        serde_json::from_value::<ConsoleJobRecoverRequest>(json!({
            "now_unix_ms": i64::MAX,
            "stale_after_ms": 1,
            "limit": 1
        }))
        .expect_err("recovery clock and threshold must be server-owned");
    }
}
