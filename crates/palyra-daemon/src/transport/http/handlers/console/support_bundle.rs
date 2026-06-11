//! Console support-bundle job handlers.
//!
//! Support bundle creation is modeled as a queued job so the console can list,
//! poll, and retain recent bundle generation attempts.

use crate::*;

/// Lists support-bundle jobs.
///
/// # Errors
/// Returns an error response when console authorization fails.
pub(crate) async fn console_support_bundle_jobs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSupportBundleJobsQuery>,
) -> Result<Json<control_plane::SupportBundleJobListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(32).clamp(1, 256);
    let jobs = list_support_bundle_jobs(&state, query.after_job_id.as_deref(), limit);
    let next_cursor =
        if jobs.len() == limit { jobs.last().map(|job| job.job_id.clone()) } else { None };
    Ok(Json(control_plane::SupportBundleJobListEnvelope {
        contract: contract_descriptor(),
        page: build_page_info(limit, jobs.len(), next_cursor),
        jobs,
    }))
}

/// Creates a support-bundle generation job.
///
/// # Errors
/// Returns an error response when console authorization or job creation fails.
pub(crate) async fn console_support_bundle_job_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::SupportBundleCreateRequest>,
) -> Result<Json<control_plane::SupportBundleJobEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let job = create_support_bundle_job(&state, payload.retain_jobs)?;
    Ok(Json(control_plane::SupportBundleJobEnvelope { contract: contract_descriptor(), job }))
}

/// Returns one support-bundle job by id.
///
/// # Errors
/// Returns an error response when console authorization, job-id normalization,
/// or job lookup fails.
pub(crate) async fn console_support_bundle_job_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<control_plane::SupportBundleJobEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let job_id = normalize_non_empty_field(job_id, "job_id")?;
    let job = {
        let jobs = lock_support_bundle_jobs(&state.support_bundle_jobs);
        jobs.get(job_id.as_str()).cloned()
    }
    .ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found("support bundle job not found"))
    })?;
    Ok(Json(control_plane::SupportBundleJobEnvelope { contract: contract_descriptor(), job }))
}
