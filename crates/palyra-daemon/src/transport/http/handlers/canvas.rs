//! HTTP handlers for signed canvas frames, runtime assets, bundle assets, and state.
//!
//! The query token and canvas id are validated before runtime access so bundle
//! assets never reach the canvas store with unchecked path or token input.

use axum::{
    extract::{Path, Query, State},
    http::{
        header::{CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE},
        HeaderMap, HeaderName, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use palyra_common::validate_canonical_id;

use crate::{
    app::state::AppState, gateway::CanvasAssetResponse, runtime_status_response,
    CanvasRuntimeQuery, CanvasStateQuery, CanvasTokenQuery, CANVAS_HTTP_MAX_CANVAS_ID_BYTES,
    CANVAS_HTTP_MAX_TOKEN_BYTES,
};

/// Validates the signed canvas token passed as a query string.
///
/// # Errors
/// Returns an error response when the token is blank or exceeds the HTTP byte
/// budget accepted by the canvas runtime.
#[expect(clippy::result_large_err, reason = "axum handlers return Response errors directly")]
pub(crate) fn validate_canvas_http_token_query(token: &str) -> Result<(), Response> {
    if token.trim().is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "canvas token query parameter cannot be empty",
        )));
    }
    if token.len() > CANVAS_HTTP_MAX_TOKEN_BYTES {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "canvas token query parameter exceeds byte limit ({} > {CANVAS_HTTP_MAX_TOKEN_BYTES})",
            token.len()
        ))));
    }
    Ok(())
}

/// Validates a canvas id before it is used for runtime lookup.
///
/// # Errors
/// Returns an error response when the id exceeds the path byte budget or is not
/// a canonical ULID.
#[expect(clippy::result_large_err, reason = "axum handlers return Response errors directly")]
pub(crate) fn validate_canvas_http_canvas_id(canvas_id: &str) -> Result<(), Response> {
    if canvas_id.len() > CANVAS_HTTP_MAX_CANVAS_ID_BYTES {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "canvas_id exceeds byte limit ({} > {CANVAS_HTTP_MAX_CANVAS_ID_BYTES})",
            canvas_id.len()
        ))));
    }
    validate_canonical_id(canvas_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "canvas_id must be a canonical ULID",
        ))
    })
}

/// Serves the signed HTML frame for a canvas instance.
///
/// # Errors
/// Returns an error response when id/token validation fails, when the runtime
/// rejects the frame request, or when the CSP header cannot be encoded.
pub(crate) async fn canvas_frame_handler(
    State(state): State<AppState>,
    Path(canvas_id): Path<String>,
    Query(query): Query<CanvasTokenQuery>,
) -> Result<Response, Response> {
    validate_canvas_http_canvas_id(canvas_id.as_str())?;
    validate_canvas_http_token_query(query.token.as_str())?;
    let frame = state
        .runtime
        .canvas_frame_document(canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    let mut response = frame.html.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    apply_canvas_security_headers(response.headers_mut(), frame.csp.as_str())?;
    Ok(response)
}

/// Serves the signed JavaScript runtime for a canvas instance.
///
/// # Errors
/// Returns an error response when id/token validation fails, when the runtime
/// rejects the request, or when response headers cannot be encoded.
pub(crate) async fn canvas_runtime_js_handler(
    State(state): State<AppState>,
    Query(query): Query<CanvasRuntimeQuery>,
) -> Result<Response, Response> {
    validate_canvas_http_canvas_id(query.canvas_id.as_str())?;
    validate_canvas_http_token_query(query.token.as_str())?;
    let asset = state
        .runtime
        .canvas_runtime_script(query.canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

/// Serves the signed stylesheet runtime for a canvas instance.
///
/// # Errors
/// Returns an error response when id/token validation fails, when the runtime
/// rejects the request, or when response headers cannot be encoded.
pub(crate) async fn canvas_runtime_css_handler(
    State(state): State<AppState>,
    Query(query): Query<CanvasRuntimeQuery>,
) -> Result<Response, Response> {
    validate_canvas_http_canvas_id(query.canvas_id.as_str())?;
    validate_canvas_http_token_query(query.token.as_str())?;
    let asset = state
        .runtime
        .canvas_runtime_stylesheet(query.canvas_id.as_str(), query.token.as_str())
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

/// Serves one signed canvas bundle asset by canvas id and asset path.
///
/// # Errors
/// Returns an error response when id/token validation fails, when the runtime
/// rejects the asset path, or when response headers cannot be encoded.
pub(crate) async fn canvas_bundle_asset_handler(
    State(state): State<AppState>,
    Path((canvas_id, asset_path)): Path<(String, String)>,
    Query(query): Query<CanvasTokenQuery>,
) -> Result<Response, Response> {
    validate_canvas_http_canvas_id(canvas_id.as_str())?;
    validate_canvas_http_token_query(query.token.as_str())?;
    let normalized_asset_path = asset_path.trim_start_matches('/').to_owned();
    let asset = state
        .runtime
        .canvas_bundle_asset(
            canvas_id.as_str(),
            normalized_asset_path.as_str(),
            query.token.as_str(),
        )
        .map_err(runtime_status_response)?;
    canvas_asset_response(asset)
}

/// Returns the latest canvas state after an optional version cursor.
///
/// # Errors
/// Returns an error response when id/token validation fails or the runtime
/// rejects the state lookup.
pub(crate) async fn canvas_state_handler(
    State(state): State<AppState>,
    Path(canvas_id): Path<String>,
    Query(query): Query<CanvasStateQuery>,
) -> Result<Response, Response> {
    validate_canvas_http_canvas_id(canvas_id.as_str())?;
    validate_canvas_http_token_query(query.token.as_str())?;
    let payload = state
        .runtime
        .canvas_state(canvas_id.as_str(), query.token.as_str(), query.after_version)
        .map_err(runtime_status_response)?;
    if let Some(payload) = payload {
        let mut response = Json(payload).into_response();
        response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response.headers_mut().insert(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        );
        Ok(response)
    } else {
        Ok(StatusCode::NO_CONTENT.into_response())
    }
}

#[expect(clippy::result_large_err, reason = "axum handlers return Response errors directly")]
fn canvas_asset_response(asset: CanvasAssetResponse) -> Result<Response, Response> {
    let mut response = asset.body.into_response();
    let content_type = HeaderValue::from_str(asset.content_type.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode canvas content-type header: {error}"
        )))
    })?;
    response.headers_mut().insert(CONTENT_TYPE, content_type);
    apply_canvas_security_headers(response.headers_mut(), asset.csp.as_str())?;
    Ok(response)
}

#[expect(clippy::result_large_err, reason = "axum handlers return Response errors directly")]
fn apply_canvas_security_headers(headers: &mut HeaderMap, csp: &str) -> Result<(), Response> {
    let csp_header = HeaderValue::from_str(csp).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode canvas csp header: {error}"
        )))
    })?;
    headers.insert(CONTENT_SECURITY_POLICY, csp_header);
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(())
}
