use crate::*;

pub(crate) async fn console_doctor_jobs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleDoctorJobsQuery>,
) -> Result<Json<control_plane::DoctorRecoveryJobListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(32).clamp(1, 256);
    let jobs = list_doctor_jobs(&state, query.after_job_id.as_deref(), limit);
    let next_cursor =
        if jobs.len() == limit { jobs.last().map(|job| job.job_id.clone()) } else { None };
    Ok(Json(control_plane::DoctorRecoveryJobListEnvelope {
        contract: contract_descriptor(),
        page: build_page_info(limit, jobs.len(), next_cursor),
        jobs,
    }))
}

pub(crate) async fn console_doctor_job_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::DoctorRecoveryCreateRequest>,
) -> Result<Json<control_plane::DoctorRecoveryJobEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let job = create_doctor_job(&state, &session.context, payload.clone())?;
    state
        .runtime
        .record_console_event(
            &session.context,
            "doctor_recovery_requested",
            json!({
                "job_id": job.job_id.as_str(),
                "dry_run": payload.dry_run,
                "repair": payload.repair,
                "force": payload.force,
                "rollback_run_present": payload
                    .rollback_run
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()),
                "idempotency_key_present": payload
                    .idempotency_key
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()),
                "command": job.command.clone(),
            }),
        )
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(control_plane::DoctorRecoveryJobEnvelope { contract: contract_descriptor(), job }))
}

pub(crate) async fn console_doctor_job_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<control_plane::DoctorRecoveryJobEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let job_id = normalize_non_empty_field(job_id, "job_id")?;
    let job = {
        let jobs = lock_doctor_jobs(&state.doctor_jobs);
        jobs.get(job_id.as_str()).cloned()
    }
    .ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found("doctor recovery job not found"))
    })?;
    Ok(Json(control_plane::DoctorRecoveryJobEnvelope { contract: contract_descriptor(), job }))
}
