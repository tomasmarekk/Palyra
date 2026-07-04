//! Console automation suggestion and blueprint handlers.

#![allow(clippy::result_large_err)]

use anyhow::anyhow;

use crate::{automation::*, *};

#[derive(Debug, Deserialize)]
pub(crate) struct AutomationSuggestionsQuery {
    status: Option<String>,
    candidate_type: Option<String>,
}

/// Lists automation suggestions, optionally filtered by lifecycle status or candidate type.
///
/// # Errors
/// Returns an error response when console authorization, registry loading, or
/// query parsing fails.
pub(crate) async fn console_automation_suggestions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AutomationSuggestionsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let status = match query.status.as_deref() {
        Some(raw) => Some(
            AutomationSuggestionStatus::parse(raw)
                .ok_or_else(|| invalid_argument_response("unknown automation suggestion status"))?,
        ),
        None => None,
    };
    let candidate_type = match query.candidate_type.as_deref() {
        Some(raw) => Some(
            AutomationCandidateType::parse(raw)
                .ok_or_else(|| invalid_argument_response("unknown automation candidate type"))?,
        ),
        None => None,
    };
    let mut suggestions =
        load_automation_suggestions().map_err(|error| internal_console_error(anyhow!(error)))?;
    if let Some(status) = status {
        suggestions.retain(|entry| entry.status == status);
    }
    if let Some(candidate_type) = candidate_type {
        suggestions.retain(|entry| entry.candidate_type == candidate_type);
    }
    suggestions.sort_by_key(|entry| std::cmp::Reverse(entry.updated_at_unix_ms));
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "count": suggestions.len(),
        "suggestions": suggestions,
    })))
}

/// Returns one automation suggestion.
///
/// # Errors
/// Returns an error response when console authorization fails, the registry
/// cannot be loaded, or the suggestion id is unknown.
pub(crate) async fn console_automation_suggestion_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(suggestion_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let suggestion = load_automation_suggestions()
        .map_err(|error| internal_console_error(anyhow!(error)))?
        .into_iter()
        .find(|entry| entry.suggestion_id == suggestion_id)
        .ok_or_else(|| not_found_console_error(anyhow!("automation suggestion not found")))?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "suggestion": suggestion,
    })))
}

/// Creates an automation suggestion from a caller-provided spec.
///
/// # Errors
/// Returns an error response when authorization, input validation, or registry
/// persistence fails.
pub(crate) async fn console_automation_suggestion_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AutomationSuggestionInput>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let suggestion = create_automation_suggestion(payload).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid automation suggestion: {error}"
        )))
    })?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "suggestion": suggestion,
    })))
}

/// Accepts an automation suggestion and returns the review-preserving creation plan.
///
/// # Errors
/// Returns an error response when authorization, lifecycle validation, or
/// registry persistence fails.
pub(crate) async fn console_automation_suggestion_accept_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(suggestion_id): Path<String>,
    Json(payload): Json<AutomationSuggestionTransitionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let (suggestion, plan) = accept_automation_suggestion(suggestion_id.as_str(), payload)
        .map_err(|error| {
            runtime_status_response(tonic::Status::failed_precondition(format!(
                "failed to accept automation suggestion: {error}"
            )))
        })?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "suggestion": suggestion,
        "accept_plan": plan,
    })))
}

/// Dismisses an automation suggestion.
///
/// # Errors
/// Returns an error response when authorization, lifecycle validation, or
/// registry persistence fails.
pub(crate) async fn console_automation_suggestion_dismiss_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(suggestion_id): Path<String>,
    Json(payload): Json<AutomationSuggestionTransitionRequest>,
) -> Result<Json<Value>, Response> {
    transition_suggestion_response(
        &state,
        &headers,
        suggestion_id.as_str(),
        AutomationSuggestionStatus::Dismissed,
        payload,
    )
}

/// Snoozes an automation suggestion until the request's timestamp.
///
/// # Errors
/// Returns an error response when authorization, lifecycle validation, or
/// registry persistence fails.
pub(crate) async fn console_automation_suggestion_snooze_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(suggestion_id): Path<String>,
    Json(payload): Json<AutomationSuggestionTransitionRequest>,
) -> Result<Json<Value>, Response> {
    transition_suggestion_response(
        &state,
        &headers,
        suggestion_id.as_str(),
        AutomationSuggestionStatus::Snoozed,
        payload,
    )
}

/// Lists built-in automation blueprints.
///
/// # Errors
/// Returns an error response when console authorization fails.
pub(crate) async fn console_automation_blueprints_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let blueprints = automation_blueprints();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "count": blueprints.len(),
        "blueprints": blueprints,
    })))
}

/// Returns one built-in automation blueprint.
///
/// # Errors
/// Returns an error response when console authorization fails or the blueprint
/// id is unknown.
pub(crate) async fn console_automation_blueprint_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(blueprint_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let blueprint = automation_blueprint(blueprint_id.as_str())
        .ok_or_else(|| not_found_console_error(anyhow!("automation blueprint not found")))?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "blueprint": blueprint,
    })))
}

/// Creates a reviewable suggestion from a built-in automation blueprint.
///
/// # Errors
/// Returns an error response when authorization, parameter validation, or
/// registry persistence fails.
pub(crate) async fn console_automation_blueprint_create_suggestion_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AutomationBlueprintSuggestionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let suggestion = create_automation_suggestion_from_blueprint(payload).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to create automation suggestion from blueprint: {error}"
        )))
    })?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "suggestion": suggestion,
    })))
}

fn transition_suggestion_response(
    state: &AppState,
    headers: &HeaderMap,
    suggestion_id: &str,
    status: AutomationSuggestionStatus,
    payload: AutomationSuggestionTransitionRequest,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(state, headers, true)?;
    let suggestion =
        transition_automation_suggestion(suggestion_id, status, payload).map_err(|error| {
            runtime_status_response(tonic::Status::failed_precondition(format!(
                "failed to transition automation suggestion: {error}"
            )))
        })?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "schema_version": 1,
        "suggestion": suggestion,
    })))
}

fn invalid_argument_response(message: &'static str) -> Response {
    runtime_status_response(tonic::Status::invalid_argument(message))
}

fn internal_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}

fn not_found_console_error(error: anyhow::Error) -> Response {
    runtime_status_response(tonic::Status::not_found(sanitize_http_error_message(
        error.to_string().as_str(),
    )))
}
