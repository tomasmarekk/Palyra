//! Console HTTP handlers for raw cron jobs (`/console/v1/cron/jobs*`): list,
//! create, enable/disable, run-now, and run history.
//!
//! Unlike the routines surface, these handlers operate on bare cron jobs.
//! Create and run-now are brokered through the gateway `CronServiceImpl` gRPC
//! service so they share its validation and policy path; the console session
//! identity is forwarded via [`apply_console_rpc_context`], which other
//! console handlers also reuse for gateway RPC dispatch.
//!
//! Response JSON shapes are part of the `/console/v1` wire contract consumed
//! by `apps/web`; field names, status codes, and error strings must stay
//! byte-identical.

use crate::journal::CronJobRecord;
use crate::*;

/// Lists the principal's cron jobs with cursor pagination
/// (`GET /console/v1/cron/jobs`).
///
/// # Errors
/// Returns an unauthorized response when console authorization fails, or a
/// mapped runtime error response when listing fails.
pub(crate) async fn console_cron_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleCronListQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = console_cron_page_limit(query.limit);
    let (jobs, next_after_job_id) = state
        .runtime
        .list_cron_jobs(
            query.after_job_id,
            Some(limit),
            query.enabled,
            Some(session.context.principal),
            session.context.channel,
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "jobs": jobs,
        "next_after_job_id": next_after_job_id,
        "page": build_page_info(limit, jobs.len(), next_after_job_id.clone()),
    })))
}

/// Creates a cron job owned by the caller via the gateway cron service
/// (`POST /console/v1/cron/jobs`).
///
/// # Errors
/// Returns invalid-argument for empty name/prompt or malformed schedules,
/// permission-denied when `owner_principal` differs from the session
/// principal, or a mapped gateway/runtime error response.
pub(crate) async fn console_cron_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleCronCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "name cannot be empty",
        )));
    }
    let prompt = payload.prompt.trim();
    if prompt.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "prompt cannot be empty",
        )));
    }
    let owner_principal = match payload.owner_principal.as_deref().map(str::trim) {
        Some("") | None => session.context.principal.clone(),
        Some(owner_principal) if owner_principal == session.context.principal => {
            owner_principal.to_owned()
        }
        Some(_) => {
            return Err(runtime_status_response(tonic::Status::permission_denied(
                "owner_principal must match authenticated session principal",
            )));
        }
    };
    let channel = payload
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| session.context.channel.clone())
        .unwrap_or_default();
    let session_key = payload.session_key.clone().and_then(trim_to_option).unwrap_or_default();
    let session_label = payload.session_label.clone().and_then(trim_to_option).unwrap_or_default();
    let schedule = build_console_schedule(payload.schedule_type.as_str(), &payload)?;
    let mut request = TonicRequest::new(cron_v1::CreateJobRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        name: name.to_owned(),
        prompt: prompt.to_owned(),
        owner_principal,
        channel,
        session_key,
        session_label,
        schedule: Some(schedule),
        enabled: payload.enabled.unwrap_or(true),
        concurrency_policy: cron_v1::ConcurrencyPolicy::Forbid as i32,
        retry_policy: Some(cron_v1::RetryPolicy { max_attempts: 1, backoff_ms: 1_000 }),
        misfire_policy: cron_v1::MisfirePolicy::Skip as i32,
        jitter_ms: payload.jitter_ms.unwrap_or(0),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_cron_service(&state);
    let response =
        <gateway::CronServiceImpl as cron_v1::cron_service_server::CronService>::create_job(
            &service, request,
        )
        .await
        .map_err(runtime_status_response)?;
    let job_id =
        response.into_inner().job.and_then(|job| job.job_id).map(|value| value.ulid).ok_or_else(
            || {
                runtime_status_response(tonic::Status::internal(
                    "cron create response did not include job_id",
                ))
            },
        )?;
    let job =
        state.runtime.cron_job(job_id.clone()).await.map_err(runtime_status_response)?.ok_or_else(
            || {
                runtime_status_response(tonic::Status::internal(format!(
                    "created cron job not found: {job_id}"
                )))
            },
        )?;
    Ok(Json(json!({ "job": job })))
}

/// Enables or disables an owned cron job, recomputing the next run timestamp
/// and waking the scheduler (`POST /console/v1/cron/jobs/{job_id}/enabled`).
///
/// # Errors
/// Returns invalid-argument for non-ULID job ids, not-found or
/// permission-denied for missing/foreign jobs, or a mapped runtime error
/// response.
pub(crate) async fn console_cron_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(payload): Json<ConsoleCronEnabledRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    let job = load_console_cron_job_for_owner(
        &state,
        job_id.as_str(),
        session.context.principal.as_str(),
    )
    .await?;
    let next_run_at_unix_ms = crate::cron::next_run_at_for_enabled_state(
        &job,
        payload.enabled,
        crate::gateway::current_unix_ms_status().map_err(runtime_status_response)?,
    )
    .map_err(runtime_status_response)?;
    let updated = state
        .runtime
        .update_cron_job(
            job_id.clone(),
            CronJobUpdatePatch {
                enabled: Some(payload.enabled),
                next_run_at_unix_ms: Some(next_run_at_unix_ms),
                ..CronJobUpdatePatch::default()
            },
        )
        .await
        .map_err(runtime_status_response)?;
    state.scheduler_wake.notify_one();
    Ok(Json(json!({ "job": updated })))
}

/// Triggers an immediate run of a cron job via the gateway cron service,
/// which enforces ownership (`POST /console/v1/cron/jobs/{job_id}/run-now`).
///
/// # Errors
/// Returns invalid-argument for non-ULID job ids, or a mapped
/// gateway/runtime error response (not-found, permission-denied, scheduler
/// failures).
pub(crate) async fn console_cron_run_now_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    let mut request = TonicRequest::new(cron_v1::RunJobNowRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        job_id: Some(common_v1::CanonicalId { ulid: job_id }),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_cron_service(&state);
    let response =
        <gateway::CronServiceImpl as cron_v1::cron_service_server::CronService>::run_job_now(
            &service, request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let status = cron_v1::JobRunStatus::try_from(response.status)
        .unwrap_or(cron_v1::JobRunStatus::Unspecified)
        .as_str_name()
        .to_ascii_lowercase();
    Ok(Json(json!({
        "run_id": response.run_id.map(|value| value.ulid),
        "status": status,
        "message": response.message,
    })))
}

/// Lists an owned cron job's run history with cursor pagination
/// (`GET /console/v1/cron/jobs/{job_id}/runs`).
///
/// # Errors
/// Returns invalid-argument for non-ULID job ids, not-found or
/// permission-denied for missing/foreign jobs, or a mapped runtime error
/// response.
pub(crate) async fn console_cron_runs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Query(query): Query<ConsoleCronRunsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = console_cron_page_limit(query.limit);
    validate_canonical_id(job_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("job_id must be a canonical ULID"))
    })?;
    ensure_console_cron_job_owner(&state, job_id.as_str(), session.context.principal.as_str())
        .await?;
    let (runs, next_after_run_id) = state
        .runtime
        .list_cron_runs(Some(job_id), query.after_run_id, Some(limit))
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "runs": runs,
        "next_after_run_id": next_after_run_id,
        "page": build_page_info(limit, runs.len(), next_after_run_id.clone()),
    })))
}

// Ownership probe used where the job record itself is not needed.
#[allow(clippy::result_large_err)]
async fn ensure_console_cron_job_owner(
    state: &AppState,
    job_id: &str,
    principal: &str,
) -> Result<(), Response> {
    load_console_cron_job_for_owner(state, job_id, principal).await?;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn load_console_cron_job_for_owner(
    state: &AppState,
    job_id: &str,
    principal: &str,
) -> Result<CronJobRecord, Response> {
    let job = state
        .runtime
        .cron_job(job_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "cron job not found: {job_id}"
            )))
        })?;
    if job.owner_principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "cron job owner mismatch for authenticated principal",
        )));
    }
    Ok(job)
}

// Builds the protobuf schedule from the console payload. Kept separate from
// the near-identical routines.rs builder because the two surfaces evolve
// their error envelopes independently.
#[allow(clippy::result_large_err)]
fn build_console_schedule(
    schedule_type_raw: &str,
    payload: &ConsoleCronCreateRequest,
) -> Result<cron_v1::Schedule, Response> {
    match schedule_type_raw.trim().to_ascii_lowercase().as_str() {
        "cron" => {
            let expression = payload
                .cron_expression
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "cron_expression is required for schedule_type=cron",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Cron as i32,
                spec: Some(cron_v1::schedule::Spec::Cron(cron_v1::CronSchedule {
                    expression: expression.to_owned(),
                })),
            })
        }
        "every" => {
            let interval_ms =
                payload.every_interval_ms.filter(|value| *value > 0).ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "every_interval_ms must be greater than zero for schedule_type=every",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::Every as i32,
                spec: Some(cron_v1::schedule::Spec::Every(cron_v1::EverySchedule { interval_ms })),
            })
        }
        "at" => {
            let timestamp = payload
                .at_timestamp_rfc3339
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "at_timestamp_rfc3339 is required for schedule_type=at",
                    ))
                })?;
            Ok(cron_v1::Schedule {
                r#type: cron_v1::ScheduleType::At as i32,
                spec: Some(cron_v1::schedule::Spec::At(cron_v1::AtSchedule {
                    timestamp_rfc3339: timestamp.to_owned(),
                })),
            })
        }
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "schedule_type must be one of cron|every|at",
        ))),
    }
}

fn build_console_cron_service(state: &AppState) -> gateway::CronServiceImpl {
    gateway::CronServiceImpl::new(
        Arc::clone(&state.runtime),
        state.auth.clone(),
        state.grpc_url.clone(),
        Arc::clone(&state.scheduler_wake),
        state.cron_timezone_mode,
    )
}

fn console_cron_page_limit(requested_limit: Option<usize>) -> usize {
    requested_limit.unwrap_or(100).clamp(1, 500)
}

/// Stamps a console session's principal, device, and channel onto outgoing
/// gateway RPC metadata. Convenience wrapper over
/// [`apply_console_request_context`].
///
/// # Errors
/// Returns failed-precondition when auth is required but no admin token is
/// configured, or an internal error response when a value cannot be encoded
/// as gRPC metadata.
#[allow(clippy::result_large_err)]
pub(crate) fn apply_console_rpc_context(
    state: &AppState,
    session: &ConsoleSession,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    apply_console_request_context(
        state,
        session.context.principal.as_str(),
        session.context.device_id.as_str(),
        session.context.channel.as_deref(),
        metadata,
    )
}

/// Attaches the daemon admin bearer token (when auth is enabled) plus the
/// caller's principal/device/channel headers to gateway RPC metadata, so
/// in-process gateway services authorize console calls like external ones.
///
/// # Errors
/// Returns failed-precondition when auth is required but no admin token is
/// configured, or an internal error response when a value cannot be encoded
/// as gRPC metadata.
#[allow(clippy::result_large_err)]
pub(crate) fn apply_console_request_context(
    state: &AppState,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    if state.auth.require_auth {
        let token = state.auth.admin_token.as_deref().ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "admin token is not configured for authenticated console RPC dispatch",
            ))
        })?;
        let bearer = MetadataValue::try_from(format!("Bearer {token}").as_str()).map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "failed to encode authorization metadata",
            ))
        })?;
        metadata.insert("authorization", bearer);
    }
    let principal = MetadataValue::try_from(principal).map_err(|_| {
        runtime_status_response(tonic::Status::internal("failed to encode principal metadata"))
    })?;
    metadata.insert(gateway::HEADER_PRINCIPAL, principal);
    let device_id = MetadataValue::try_from(device_id).map_err(|_| {
        runtime_status_response(tonic::Status::internal("failed to encode device metadata"))
    })?;
    metadata.insert(gateway::HEADER_DEVICE_ID, device_id);
    if let Some(channel) = channel {
        let channel = MetadataValue::try_from(channel).map_err(|_| {
            runtime_status_response(tonic::Status::internal("failed to encode channel metadata"))
        })?;
        metadata.insert(gateway::HEADER_CHANNEL, channel);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::console_cron_page_limit;

    #[test]
    fn console_cron_page_limit_matches_runtime_limit() {
        assert_eq!(console_cron_page_limit(None), 100);
        assert_eq!(console_cron_page_limit(Some(0)), 1);
        assert_eq!(console_cron_page_limit(Some(42)), 42);
        assert_eq!(console_cron_page_limit(Some(10_000)), 500);
    }
}
