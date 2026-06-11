//! Admin skill-status handlers.
//!
//! The endpoints here mutate skill execution status and always record a
//! corresponding operator event so quarantine and re-enable actions remain
//! auditable.

use crate::*;

/// Marks a skill version as quarantined.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// input normalization, clock access, persistence, or event recording fails.
pub(crate) async fn admin_skill_quarantine_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Quarantined,
            reason: payload.reason.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            }),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&context, "skill.quarantined", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

/// Re-enables a quarantined skill version after explicit operator override.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// override acknowledgement, input normalization, clock access, persistence,
/// or event recording fails.
pub(crate) async fn admin_skill_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    if !payload.override_enabled.unwrap_or(false) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "enable requires explicit override=true acknowledgment",
        )));
    }
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Active,
            reason: payload.reason.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            }),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&context, "skill.enabled", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

/// Converts a persisted skill status record into the HTTP response contract.
pub(crate) fn skill_status_response(record: SkillStatusRecord) -> SkillStatusResponse {
    SkillStatusResponse {
        skill_id: record.skill_id,
        version: record.version,
        status: record.status.as_str().to_owned(),
        reason: record.reason,
        detected_at_ms: record.detected_at_ms,
        operator_principal: record.operator_principal,
    }
}
