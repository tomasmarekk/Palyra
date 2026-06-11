//! Discord connector admin HTTP handlers.
//!
//! These endpoints keep Discord onboarding and account lifecycle operations
//! behind the same admin auth and audit counters as the rest of channel admin.

use crate::application::channels::providers::discord::{
    apply_discord_onboarding, build_discord_onboarding_preflight, perform_discord_account_logout,
    perform_discord_account_remove,
};
use crate::*;

/// Runs a Discord onboarding preflight without persisting connector changes.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// onboarding preflight validation fails.
pub(crate) async fn admin_discord_onboarding_probe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordOnboardingRequest>,
) -> Result<Json<DiscordOnboardingPreflightResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let response = build_discord_onboarding_preflight(&state, payload).await?;
    Ok(Json(response))
}

/// Applies Discord onboarding changes for a connector.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// onboarding application fails.
pub(crate) async fn admin_discord_onboarding_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordOnboardingRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let response = apply_discord_onboarding(&state, payload).await?;
    Ok(Json(response))
}

/// Logs out a Discord account selected by path parameter.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// account logout fails.
pub(crate) async fn admin_discord_account_logout_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Json(payload): Json<DiscordAccountLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let response = perform_discord_account_logout(&state, account_id, &payload)?;
    Ok(Json(response))
}

/// Logs out a Discord account selected by request body.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// account logout fails.
pub(crate) async fn admin_discord_account_logout_action_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordAccountLifecycleActionRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let request = DiscordAccountLifecycleRequest { keep_credential: payload.keep_credential };
    let response = perform_discord_account_logout(&state, payload.account_id, &request)?;
    Ok(Json(response))
}

/// Removes a Discord account selected by path parameter.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// account removal fails.
pub(crate) async fn admin_discord_account_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Json(payload): Json<DiscordAccountLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let response = perform_discord_account_remove(&state, account_id, &payload)?;
    Ok(Json(response))
}

/// Removes a Discord account selected by request body.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// account removal fails.
pub(crate) async fn admin_discord_account_remove_action_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordAccountLifecycleActionRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let request = DiscordAccountLifecycleRequest { keep_credential: payload.keep_credential };
    let response = perform_discord_account_remove(&state, payload.account_id, &request)?;
    Ok(Json(response))
}
