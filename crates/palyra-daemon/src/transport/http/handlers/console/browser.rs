//! Console browser-control handlers for the `/console/v1/browser/*` routes.
//!
//! Every handler authorizes the console session, validates identifiers, then
//! proxies the operation to `palyra-browserd` over gRPC (`browser_v1`) and
//! re-shapes the response into `control_plane` envelopes consumed by
//! `apps/web` (the JSON field names are wire contract). Mutating handlers
//! additionally write a `browser.*` console audit event with session/tab/
//! profile identifiers redacted. The browser-extension relay flow (token
//! minting plus token-authenticated relay actions) also lives here; relay
//! tokens are stored hashed in [`AppState::relay_tokens`] and compared in
//! constant time.

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use crate::*;

/// gRPC metadata key carrying the console caller's principal to browserd for
/// per-principal scoping of session-derived data (downloads, network log).
const BROWSER_CALLER_PRINCIPAL_HEADER: &str = "x-palyra-principal";

/// Maps an authorized console request into browserd's private-target flag.
///
/// URL shape and deployment mode are deliberately absent: localhost and
/// `local_desktop` must not create an implicit private-network exception.
fn console_browser_private_target_flag(requested: Option<bool>) -> bool {
    requested.unwrap_or(false)
}

/// `GET /console/v1/browser/profiles` — lists browser profiles for the
/// resolved principal, including which profile is active.
///
/// # Errors
/// Returns an error response when console authorization fails, the principal
/// is invalid, or the browserd RPC fails.
pub(crate) async fn console_browser_profiles_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleBrowserProfilesQuery>,
) -> Result<Json<control_plane::BrowserProfileListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let principal = resolve_console_browser_principal(
        query.principal.as_deref(),
        session.context.principal.as_str(),
    )?;
    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ListProfilesRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.list_profiles(request).await.map_err(runtime_status_response)?.into_inner();
    let profiles =
        response.profiles.into_iter().map(control_plane_browser_profile).collect::<Vec<_>>();
    Ok(Json(control_plane::BrowserProfileListEnvelope {
        contract: contract_descriptor(),
        principal,
        active_profile_id: maybe_canonical_id(response.active_profile_id),
        page: build_page_info(profiles.len().max(1), profiles.len(), None),
        profiles,
    }))
}

/// `POST /console/v1/browser/profiles/create` — creates a browser profile
/// and records a `browser.profile.created` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the name is
/// empty, the browserd RPC fails, or the audit event cannot be recorded.
pub(crate) async fn console_browser_profile_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleBrowserCreateProfileRequest>,
) -> Result<Json<control_plane::BrowserProfileEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let principal = resolve_console_browser_principal(
        payload.principal.as_deref(),
        session.context.principal.as_str(),
    )?;
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "profile name cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::CreateProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        name: name.to_owned(),
        theme_color: payload.theme_color.as_deref().map(str::trim).unwrap_or_default().to_owned(),
        persistence_enabled: payload.persistence_enabled.unwrap_or(false),
        private_profile: payload.private_profile.unwrap_or(false),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.create_profile(request).await.map_err(runtime_status_response)?.into_inner();
    let profile = response.profile.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "browser create_profile response is missing profile payload",
        ))
    })?;
    let profile = control_plane_browser_profile(profile);

    record_browser_console_event(
        &state,
        &session.context,
        "browser.profile.created",
        json!({
            "principal": principal,
            "profile_id": profile.profile_id,
            "name": profile.name,
            "persistence_enabled": profile.persistence_enabled,
            "private_profile": profile.private_profile,
        }),
    )
    .await?;

    Ok(Json(control_plane::BrowserProfileEnvelope { contract: contract_descriptor(), profile }))
}

/// `POST /console/v1/browser/profiles/{profile_id}/rename` — renames a
/// profile and records a `browser.profile.renamed` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the profile id
/// or name is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_profile_rename_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
    Json(payload): Json<ConsoleBrowserRenameProfileRequest>,
) -> Result<Json<control_plane::BrowserProfileEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(profile_id.as_str(), "profile_id")?;
    let principal = resolve_console_browser_principal(
        payload.principal.as_deref(),
        session.context.principal.as_str(),
    )?;
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "profile name cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::RenameProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        profile_id: Some(common_v1::CanonicalId { ulid: profile_id.clone() }),
        name: name.to_owned(),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.rename_profile(request).await.map_err(runtime_status_response)?.into_inner();
    let profile = response.profile.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "browser rename_profile response is missing profile payload",
        ))
    })?;
    let profile = control_plane_browser_profile(profile);

    record_browser_console_event(
        &state,
        &session.context,
        "browser.profile.renamed",
        json!({
            "principal": principal,
            "profile_id": profile.profile_id,
            "name": profile.name,
        }),
    )
    .await?;

    Ok(Json(control_plane::BrowserProfileEnvelope { contract: contract_descriptor(), profile }))
}

/// `POST /console/v1/browser/profiles/{profile_id}/delete` — deletes a
/// profile and records a `browser.profile.deleted` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the profile id
/// is invalid, the browserd RPC fails, or the audit event cannot be recorded.
pub(crate) async fn console_browser_profile_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
    Json(payload): Json<ConsoleBrowserProfileScopeRequest>,
) -> Result<Json<control_plane::BrowserProfileDeleteEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(profile_id.as_str(), "profile_id")?;
    let principal = resolve_console_browser_principal(
        payload.principal.as_deref(),
        session.context.principal.as_str(),
    )?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::DeleteProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        profile_id: Some(common_v1::CanonicalId { ulid: profile_id.clone() }),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.delete_profile(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserProfileDeleteEnvelope {
        contract: contract_descriptor(),
        principal: principal.clone(),
        profile_id: profile_id.clone(),
        deleted: response.deleted,
        active_profile_id: maybe_canonical_id(response.active_profile_id),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.profile.deleted",
        json!({
            "principal": principal,
            "profile_id": profile_id,
            "deleted": envelope.deleted,
            "active_profile_id": envelope.active_profile_id,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/profiles/{profile_id}/activate` — marks a
/// profile active and records a `browser.profile.activated` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the profile id
/// is invalid, the browserd RPC fails, or the audit event cannot be recorded.
pub(crate) async fn console_browser_profile_activate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
    Json(payload): Json<ConsoleBrowserProfileScopeRequest>,
) -> Result<Json<control_plane::BrowserProfileEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(profile_id.as_str(), "profile_id")?;
    let principal = resolve_console_browser_principal(
        payload.principal.as_deref(),
        session.context.principal.as_str(),
    )?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::SetActiveProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        profile_id: Some(common_v1::CanonicalId { ulid: profile_id.clone() }),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.set_active_profile(request).await.map_err(runtime_status_response)?.into_inner();
    let profile = response.profile.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "browser set_active_profile response is missing profile payload",
        ))
    })?;
    let profile = control_plane_browser_profile(profile);

    record_browser_console_event(
        &state,
        &session.context,
        "browser.profile.activated",
        json!({
            "principal": principal,
            "profile_id": profile.profile_id,
            "name": profile.name,
        }),
    )
    .await?;

    Ok(Json(control_plane::BrowserProfileEnvelope { contract: contract_descriptor(), profile }))
}

/// `GET /console/v1/browser/sessions` — lists live browser sessions for the
/// resolved principal (most recent first, limit clamped to 1..=250).
///
/// # Errors
/// Returns an error response when console authorization fails, the principal
/// is invalid, or the browserd RPC fails.
pub(crate) async fn console_browser_sessions_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleBrowserSessionsQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let principal = resolve_console_browser_principal(
        query.principal.as_deref(),
        session.context.principal.as_str(),
    )?;
    let limit = query.limit.unwrap_or(50).clamp(1, 250);

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ListSessionsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        limit,
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.list_sessions(request).await.map_err(runtime_status_response)?.into_inner();
    let sessions = response.sessions.iter().map(session_summary_to_value).collect::<Vec<_>>();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "principal": principal,
        "truncated": response.truncated,
        "error": response.error,
        "page": build_page_info(limit as usize, sessions.len(), None),
        "sessions": sessions,
    })))
}

/// `POST /console/v1/browser/sessions` — creates a browser session with the
/// requested budget/persistence options and records a
/// `browser.session.created` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the principal
/// or profile id is invalid, the browserd RPC fails, or the audit event
/// cannot be recorded.
pub(crate) async fn console_browser_session_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleBrowserCreateSessionRequest>,
) -> Result<Json<control_plane::BrowserSessionCreateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let principal = resolve_console_browser_principal(
        payload.principal.as_deref(),
        session.context.principal.as_str(),
    )?;
    let channel = normalize_optional_console_browser_channel(payload.channel.as_deref())
        .or_else(|| session.context.channel.clone());
    let action_allowed_domains = payload
        .action_allowed_domains
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::CreateSessionRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        principal: principal.clone(),
        idle_ttl_ms: payload.idle_ttl_ms.unwrap_or(0),
        budget: payload.budget.as_ref().map(console_browser_session_budget_to_proto),
        allow_private_targets: console_browser_private_target_flag(payload.allow_private_targets),
        allow_downloads: payload.allow_downloads.unwrap_or(false),
        action_allowed_domains,
        persistence_enabled: payload.persistence_enabled.unwrap_or(false),
        persistence_id: payload
            .persistence_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_owned(),
        channel: channel.clone().unwrap_or_default(),
        profile_id: optional_console_browser_canonical_id(
            payload.profile_id.as_deref(),
            "profile_id",
        )?,
        private_profile: payload.private_profile.unwrap_or(false),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.create_session(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserSessionCreateEnvelope {
        contract: contract_descriptor(),
        principal: principal.clone(),
        channel: channel.clone(),
        session_id: maybe_canonical_id(response.session_id),
        created_at_unix_ms: response.created_at_unix_ms,
        effective_budget: response.effective_budget.map(control_plane_browser_session_budget),
        downloads_enabled: response.downloads_enabled,
        action_allowed_domains: response.action_allowed_domains.clone(),
        persistence_enabled: response.persistence_enabled,
        persistence_id: response.persistence_id.clone(),
        state_restored: response.state_restored,
        profile_id: maybe_canonical_id(response.profile_id),
        private_profile: response.private_profile,
    };
    if let Some(session_id) = envelope.session_id.as_deref() {
        state.runtime.forget_closed_browser_session(session_id);
    }

    record_browser_console_event(
        &state,
        &session.context,
        "browser.session.created",
        json!({
            "principal": principal,
            "channel": channel,
            "downloads_enabled": envelope.downloads_enabled,
            "persistence_enabled": envelope.persistence_enabled,
            "state_restored": envelope.state_restored,
            "private_profile": envelope.private_profile,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `GET /console/v1/browser/sessions/{session_id}` — returns the session
/// summary, effective budget, and tab list.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_session_show_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::GetSessionRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.get_session(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "session_id": session_id,
        "success": response.success,
        "session": response.session.as_ref().map(session_detail_to_value).unwrap_or(Value::Null),
        "error": response.error,
    })))
}

/// `GET /console/v1/browser/sessions/{session_id}/inspect` — deep-inspects a
/// session (cookies, storage, action/network/console logs, page snapshot)
/// with per-section opt-ins and byte caps from the query string.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_session_inspect_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserInspectSessionQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::InspectSessionRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        include_cookies: query.include_cookies.unwrap_or(false),
        include_storage: query.include_storage.unwrap_or(false),
        include_action_log: query.include_action_log.unwrap_or(true),
        include_network_log: query.include_network_log.unwrap_or(true),
        include_page_snapshot: query.include_page_snapshot.unwrap_or(true),
        max_cookie_bytes: query.max_cookie_bytes.unwrap_or(0),
        max_storage_bytes: query.max_storage_bytes.unwrap_or(0),
        max_action_log_entries: query.max_action_log_entries.unwrap_or(0),
        max_network_log_entries: query.max_network_log_entries.unwrap_or(0),
        max_network_log_bytes: query.max_network_log_bytes.unwrap_or(0),
        max_dom_snapshot_bytes: query.max_dom_snapshot_bytes.unwrap_or(0),
        max_visible_text_bytes: query.max_visible_text_bytes.unwrap_or(0),
        include_console_log: query.include_console_log.unwrap_or(true),
        include_page_diagnostics: query.include_page_diagnostics.unwrap_or(true),
        max_console_log_entries: query.max_console_log_entries.unwrap_or(0),
        max_console_log_bytes: query.max_console_log_bytes.unwrap_or(0),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.inspect_session(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "session_id": session_id,
        "success": response.success,
        "session": response.session.as_ref().map(session_detail_to_value).unwrap_or(Value::Null),
        "cookies": response.cookies.iter().map(cookie_domain_to_value).collect::<Vec<_>>(),
        "storage": response.storage.iter().map(storage_origin_to_value).collect::<Vec<_>>(),
        "action_log": response.action_log.iter().map(browser_action_log_entry_to_value).collect::<Vec<_>>(),
        "network_log": response.network_log.iter().map(browser_network_log_entry_to_value).collect::<Vec<_>>(),
        "dom_snapshot": response.dom_snapshot,
        "visible_text": response.visible_text,
        "page_url": response.page_url,
        "cookies_truncated": response.cookies_truncated,
        "storage_truncated": response.storage_truncated,
        "action_log_truncated": response.action_log_truncated,
        "network_log_truncated": response.network_log_truncated,
        "dom_truncated": response.dom_truncated,
        "visible_text_truncated": response.visible_text_truncated,
        "console_log": response.console_log.iter().map(browser_console_entry_to_value).collect::<Vec<_>>(),
        "console_log_truncated": response.console_log_truncated,
        "page_diagnostics": response.page_diagnostics.as_ref().map(browser_page_diagnostics_to_value).unwrap_or(Value::Null),
        "error": response.error,
    })))
}

/// `POST /console/v1/browser/sessions/{session_id}/close` — closes a session,
/// remembers it as closed for friendlier later errors, and records a
/// `browser.session.closed` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_session_close_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::BrowserSessionCloseEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::CloseSessionRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.close_session(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserSessionCloseEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        closed: response.closed,
        reason: response.reason.clone(),
    };
    if envelope.closed {
        state.runtime.record_closed_browser_session(session_id.as_str());
    }

    record_browser_console_event(
        &state,
        &session.context,
        "browser.session.closed",
        json!({
            "session_id": session_id,
            "closed": envelope.closed,
            "reason": envelope.reason,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/navigate` — navigates the
/// active tab and records a `browser.action.navigate` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or URL is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_navigate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserNavigateRequest>,
) -> Result<Json<control_plane::BrowserNavigateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let url = payload.url.trim();
    if url.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "url cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::NavigateRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        url: url.to_owned(),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        allow_redirects: payload.allow_redirects.unwrap_or(true),
        max_redirects: payload.max_redirects.unwrap_or(3),
        allow_private_targets: console_browser_private_target_flag(payload.allow_private_targets),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.navigate(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserNavigateEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        final_url: response.final_url.clone(),
        status_code: response.status_code,
        title: response.title.clone(),
        body_bytes: response.body_bytes,
        latency_ms: response.latency_ms,
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.navigate",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "status_code": envelope.status_code,
            "body_bytes": envelope.body_bytes,
            "latency_ms": envelope.latency_ms,
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/click` — clicks the first
/// element matching the selector and records a `browser.action.click` audit
/// event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or selector is invalid, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_click_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserClickRequest>,
) -> Result<Json<control_plane::BrowserClickEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let selector = payload.selector.trim();
    if selector.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "selector cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ClickRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        selector: selector.to_owned(),
        max_retries: payload.max_retries.unwrap_or(0),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.click(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserClickEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        error: response.error.clone(),
        action_log: action_log.clone(),
        artifact: response.artifact.map(control_plane_browser_download_artifact),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.click",
        json!({
            "session_id": session_id,
            "selector": selector,
            "success": envelope.success,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/type` — types text into
/// the element matching the selector and records a `browser.action.type`
/// audit event (typed byte count only, never the text itself).
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or selector is invalid, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_type_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserTypeRequest>,
) -> Result<Json<control_plane::BrowserTypeEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let selector = payload.selector.trim();
    if selector.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "selector cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::TypeRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        selector: selector.to_owned(),
        text: payload.text,
        clear_existing: payload.clear_existing.unwrap_or(false),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.r#type(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserTypeEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        typed_bytes: response.typed_bytes,
        error: response.error.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.type",
        json!({
            "session_id": session_id,
            "selector": selector,
            "success": envelope.success,
            "typed_bytes": envelope.typed_bytes,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/press` — presses a key in
/// the active tab and records a `browser.action.press` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or key is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_press_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserPressRequest>,
) -> Result<Json<control_plane::BrowserPressEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let key = payload.key.trim();
    if key.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "key cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::PressRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        key: key.to_owned(),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.press(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserPressEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        key: response.key.clone(),
        error: response.error.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.press",
        json!({
            "session_id": session_id,
            "key": key,
            "success": envelope.success,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/select` — selects a value
/// in a `<select>` element and records a `browser.action.select` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session
/// id, selector, or value is invalid, the browserd RPC fails, or the audit
/// event cannot be recorded.
pub(crate) async fn console_browser_select_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserSelectRequest>,
) -> Result<Json<control_plane::BrowserSelectEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let selector = payload.selector.trim();
    if selector.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "selector cannot be empty",
        )));
    }
    let value = payload.value.trim();
    if value.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "value cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::SelectRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        selector: selector.to_owned(),
        value: value.to_owned(),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.select(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserSelectEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        selected_value: response.selected_value.clone(),
        error: response.error.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.select",
        json!({
            "session_id": session_id,
            "selector": selector,
            "value": value,
            "success": envelope.success,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/highlight` — visually
/// highlights the element matching the selector and records a
/// `browser.action.highlight` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or selector is invalid, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_highlight_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserHighlightRequest>,
) -> Result<Json<control_plane::BrowserHighlightEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let selector = payload.selector.trim();
    if selector.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "selector cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::HighlightRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        selector: selector.to_owned(),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        duration_ms: payload.duration_ms.unwrap_or(1_500),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.highlight(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserHighlightEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        selector: response.selector.clone(),
        error: response.error.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.highlight",
        json!({
            "session_id": session_id,
            "selector": selector,
            "success": envelope.success,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/scroll` — scrolls the
/// active tab by the requested deltas and records a `browser.action.scroll`
/// audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_scroll_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserScrollRequest>,
) -> Result<Json<control_plane::BrowserScrollEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ScrollRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        delta_x: payload.delta_x.unwrap_or(0),
        delta_y: payload.delta_y.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.scroll(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserScrollEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        scroll_x: response.scroll_x,
        scroll_y: response.scroll_y,
        error: response.error.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.scroll",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "scroll_x": envelope.scroll_x,
            "scroll_y": envelope.scroll_y,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/wait-for` — polls until a
/// selector and/or text appears and records a `browser.action.wait_for`
/// audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_wait_for_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserWaitForRequest>,
) -> Result<Json<control_plane::BrowserWaitForEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let selector = payload.selector.as_deref().map(str::trim).unwrap_or_default().to_owned();
    let text = payload.text.as_deref().map(str::trim).unwrap_or_default().to_owned();

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::WaitForRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        selector,
        text,
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        poll_interval_ms: payload.poll_interval_ms.unwrap_or(0),
        capture_failure_screenshot: payload.capture_failure_screenshot.unwrap_or(true),
        max_failure_screenshot_bytes: clamp_console_browser_max_screenshot_bytes(
            &state,
            payload.max_failure_screenshot_bytes,
        ),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.wait_for(request).await.map_err(runtime_status_response)?.into_inner();
    let action_log = response.action_log.map(control_plane_browser_action_log);
    let envelope = control_plane::BrowserWaitForEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        waited_ms: response.waited_ms,
        error: response.error.clone(),
        matched_selector: response.matched_selector.clone(),
        matched_text: response.matched_text.clone(),
        action_log: action_log.clone(),
        failure_screenshot_mime_type: non_empty_string(response.failure_screenshot_mime_type),
        failure_screenshot_base64: encode_optional_base64(
            response.failure_screenshot_bytes.as_slice(),
        ),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.action.wait_for",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "waited_ms": envelope.waited_ms,
            "matched_selector": envelope.matched_selector,
            "error": envelope.error,
            "action_id": action_log.as_ref().map(|value| value.action_id.clone()),
            "attempts": action_log.as_ref().map(|value| value.attempts),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `GET /console/v1/browser/sessions/{session_id}/title` — returns the active
/// tab's title, truncated to the configured byte budget.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_title_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserTitleQuery>,
) -> Result<Json<control_plane::BrowserTitleEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::GetTitleRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        max_title_bytes: clamp_console_browser_max_title_bytes(&state, query.max_title_bytes),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.get_title(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserTitleEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        title: response.title,
        error: response.error,
    }))
}

/// `GET /console/v1/browser/sessions/{session_id}/screenshot` — captures a
/// screenshot of the active tab as base64 (default `png`), capped by the
/// configured screenshot byte budget.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_screenshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserScreenshotQuery>,
) -> Result<Json<control_plane::BrowserScreenshotEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ScreenshotRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        max_bytes: clamp_console_browser_max_screenshot_bytes(&state, query.max_bytes),
        format: query
            .format
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("png")
            .to_owned(),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.screenshot(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserScreenshotEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        mime_type: non_empty_string(response.mime_type),
        image_base64: encode_optional_base64(response.image_bytes.as_slice()),
        error: response.error,
    }))
}

/// `GET /console/v1/browser/sessions/{session_id}/pdf` — exports the active
/// tab as a PDF (inline base64 plus an optional download artifact record).
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_pdf_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserPdfQuery>,
) -> Result<Json<control_plane::BrowserPdfEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ExportPdfRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        max_bytes: query.max_bytes.unwrap_or(0),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.export_pdf(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserPdfEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        mime_type: non_empty_string(response.mime_type),
        size_bytes: response.size_bytes,
        sha256: non_empty_string(response.sha256),
        artifact: response.artifact.map(control_plane_browser_download_artifact),
        pdf_base64: encode_optional_base64(response.pdf_bytes.as_slice()),
        error: response.error,
    }))
}

/// `GET /console/v1/browser/sessions/{session_id}/observe` — returns DOM
/// snapshot, accessibility tree, and visible text for the active tab, each
/// individually opt-out and byte-capped.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_observe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserObserveQuery>,
) -> Result<Json<control_plane::BrowserObserveEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ObserveRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        include_dom_snapshot: query.include_dom_snapshot.unwrap_or(true),
        include_accessibility_tree: query.include_accessibility_tree.unwrap_or(true),
        include_visible_text: console_browser_observe_include_visible_text(
            query.include_visible_text,
        ),
        max_dom_snapshot_bytes: query.max_dom_snapshot_bytes.unwrap_or(0),
        max_accessibility_tree_bytes: query.max_accessibility_tree_bytes.unwrap_or(0),
        max_visible_text_bytes: query.max_visible_text_bytes.unwrap_or(0),
        capture_selectors: Vec::new(),
        computed_style_properties: Vec::new(),
        max_capture_text_bytes: 0,
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.observe(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserObserveEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        dom_snapshot: response.dom_snapshot,
        accessibility_tree: response.accessibility_tree,
        visible_text: response.visible_text,
        dom_truncated: response.dom_truncated,
        accessibility_tree_truncated: response.accessibility_tree_truncated,
        visible_text_truncated: response.visible_text_truncated,
        page_url: response.page_url,
        error: response.error,
    }))
}

/// `GET /console/v1/browser/sessions/{session_id}/network-log` — returns the
/// session's captured network log entries; the caller principal is forwarded
/// so browserd can enforce per-principal access to the log.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the principal cannot be encoded as metadata, or
/// the browserd RPC fails.
pub(crate) async fn console_browser_network_log_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserNetworkLogQuery>,
) -> Result<Json<control_plane::BrowserNetworkLogEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let limit = query.limit.unwrap_or(50).clamp(1, 250);

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::NetworkLogRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        limit,
        include_headers: query.include_headers.unwrap_or(false),
        max_payload_bytes: query.max_payload_bytes.unwrap_or(0),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.network_log(request).await.map_err(runtime_status_response)?.into_inner();
    let entries = response
        .entries
        .into_iter()
        .map(control_plane_browser_network_log_entry)
        .collect::<Vec<_>>();
    Ok(Json(control_plane::BrowserNetworkLogEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        truncated: response.truncated,
        error: response.error,
        page: build_page_info(limit as usize, entries.len(), None),
        entries,
    }))
}

/// `GET /console/v1/browser/sessions/{session_id}/console` — returns the
/// page's console log entries filtered by minimum severity, optionally with
/// aggregate page diagnostics.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_console_log_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserConsoleLogQuery>,
) -> Result<Json<control_plane::BrowserConsoleLogEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let limit = query.limit.unwrap_or(50).clamp(1, 250);

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ConsoleLogRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        limit,
        minimum_severity: browser_diagnostic_severity_to_proto(query.minimum_severity),
        include_page_diagnostics: query.include_page_diagnostics.unwrap_or(false),
        max_payload_bytes: query.max_payload_bytes.unwrap_or(0),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.console_log(request).await.map_err(runtime_status_response)?.into_inner();
    let entries =
        response.entries.into_iter().map(control_plane_browser_console_entry).collect::<Vec<_>>();
    let entry_count = entries.len();
    Ok(Json(control_plane::BrowserConsoleLogEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        entries,
        truncated: response.truncated,
        page_diagnostics: response.page_diagnostics.map(control_plane_browser_page_diagnostics),
        error: response.error,
        page: build_page_info(limit as usize, entry_count, None),
    }))
}

/// `POST /console/v1/browser/sessions/{session_id}/reset-state` — clears the
/// selected session state (cookies, storage, tabs, permissions) and records
/// a `browser.state.reset` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_reset_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserResetStateRequest>,
) -> Result<Json<control_plane::BrowserResetStateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ResetStateRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        clear_cookies: payload.clear_cookies.unwrap_or(false),
        clear_storage: payload.clear_storage.unwrap_or(false),
        reset_tabs: payload.reset_tabs.unwrap_or(false),
        reset_permissions: payload.reset_permissions.unwrap_or(false),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.reset_state(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserResetStateEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        cookies_cleared: response.cookies_cleared,
        storage_entries_cleared: response.storage_entries_cleared,
        tabs_closed: response.tabs_closed,
        permissions: response.permissions.map(control_plane_browser_permissions),
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.state.reset",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "cookies_cleared": envelope.cookies_cleared,
            "storage_entries_cleared": envelope.storage_entries_cleared,
            "tabs_closed": envelope.tabs_closed,
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `GET /console/v1/browser/sessions/{session_id}/tabs` — lists the session's
/// open tabs and the active tab id.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_tabs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::BrowserTabListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ListTabsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.list_tabs(request).await.map_err(runtime_status_response)?.into_inner();
    let tabs = response.tabs.into_iter().map(control_plane_browser_tab).collect::<Vec<_>>();
    Ok(Json(control_plane::BrowserTabListEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        active_tab_id: maybe_canonical_id(response.active_tab_id),
        error: response.error,
        page: build_page_info(tabs.len().max(1), tabs.len(), None),
        tabs,
    }))
}

/// `POST /console/v1/browser/sessions/{session_id}/tabs/open` — opens a new
/// tab at the requested URL and records a `browser.tab.opened` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or URL is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_tab_open_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserOpenTabRequest>,
) -> Result<Json<control_plane::BrowserOpenTabEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let url = payload.url.trim();
    if url.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "url cannot be empty",
        )));
    }

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::OpenTabRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        url: url.to_owned(),
        activate: payload.activate.unwrap_or(true),
        timeout_ms: payload.timeout_ms.unwrap_or(0),
        allow_redirects: payload.allow_redirects.unwrap_or(true),
        max_redirects: payload.max_redirects.unwrap_or(3),
        allow_private_targets: console_browser_private_target_flag(payload.allow_private_targets),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.open_tab(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserOpenTabEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        tab: response.tab.map(control_plane_browser_tab),
        navigated: response.navigated,
        status_code: response.status_code,
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.tab.opened",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "tab_id": envelope.tab.as_ref().and_then(|value| value.tab_id.clone()),
            "navigated": envelope.navigated,
            "status_code": envelope.status_code,
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/tabs/switch` — activates
/// another tab and records a `browser.tab.switched` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or tab id is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_tab_switch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserTabMutationRequest>,
) -> Result<Json<control_plane::BrowserSwitchTabEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let tab_id = required_console_browser_canonical_id(payload.tab_id.as_str(), "tab_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::SwitchTabRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        tab_id: Some(common_v1::CanonicalId { ulid: tab_id.clone() }),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.switch_tab(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserSwitchTabEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        active_tab: response.active_tab.map(control_plane_browser_tab),
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.tab.switched",
        json!({
            "session_id": session_id,
            "tab_id": tab_id,
            "success": envelope.success,
            "active_tab_id": envelope.active_tab.as_ref().and_then(|value| value.tab_id.clone()),
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `POST /console/v1/browser/sessions/{session_id}/tabs/close` — closes a tab
/// (the active one when no `tab_id` is given) and records a
/// `browser.tab.closed` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// or tab id is invalid, the browserd RPC fails, or the audit event cannot be
/// recorded.
pub(crate) async fn console_browser_tab_close_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserTabCloseRequest>,
) -> Result<Json<control_plane::BrowserCloseTabEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;
    let tab_id = optional_console_browser_canonical_id(payload.tab_id.as_deref(), "tab_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::CloseTabRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        tab_id,
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client.close_tab(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserCloseTabEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        closed_tab_id: maybe_canonical_id(response.closed_tab_id),
        active_tab: response.active_tab.map(control_plane_browser_tab),
        tabs_remaining: response.tabs_remaining,
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.tab.closed",
        json!({
            "session_id": session_id,
            "requested_tab_id": payload.tab_id,
            "success": envelope.success,
            "closed_tab_id": envelope.closed_tab_id,
            "active_tab_id": envelope.active_tab.as_ref().and_then(|value| value.tab_id.clone()),
            "tabs_remaining": envelope.tabs_remaining,
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `GET /console/v1/browser/sessions/{session_id}/permissions` — returns the
/// session's camera/microphone/location permission settings.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, or the browserd RPC fails.
pub(crate) async fn console_browser_permissions_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::BrowserPermissionsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::GetPermissionsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.get_permissions(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserPermissionsEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        permissions: response.permissions.map(control_plane_browser_permissions),
        error: response.error,
    }))
}

/// `POST /console/v1/browser/sessions/{session_id}/permissions` — updates the
/// session's permission settings (or resets them to defaults) and records a
/// `browser.permissions.set` audit event.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is not a canonical ULID, the browserd RPC fails, or the audit event cannot
/// be recorded.
pub(crate) async fn console_browser_permissions_set_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Json(payload): Json<ConsoleBrowserSetPermissionsRequest>,
) -> Result<Json<control_plane::BrowserPermissionsEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::SetPermissionsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        camera: browser_permission_setting_to_proto(payload.camera),
        microphone: browser_permission_setting_to_proto(payload.microphone),
        location: browser_permission_setting_to_proto(payload.location),
        reset_to_default: payload.reset_to_default.unwrap_or(false),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response =
        client.set_permissions(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserPermissionsEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        success: response.success,
        permissions: response.permissions.map(control_plane_browser_permissions),
        error: response.error.clone(),
    };

    record_browser_console_event(
        &state,
        &session.context,
        "browser.permissions.set",
        json!({
            "session_id": session_id,
            "success": envelope.success,
            "reset_to_default": payload.reset_to_default.unwrap_or(false),
            "permissions": envelope.permissions,
            "error": envelope.error,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

/// `GET /console/v1/browser/downloads` — lists download artifacts for a
/// session; the caller principal is forwarded so browserd can enforce
/// per-principal access to artifacts.
///
/// # Errors
/// Returns an error response when console authorization fails, the session id
/// is invalid, the principal cannot be encoded as metadata, or the browserd
/// RPC fails.
pub(crate) async fn console_browser_downloads_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleBrowserDownloadsQuery>,
) -> Result<Json<control_plane::BrowserDownloadArtifactListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let session_id =
        required_console_browser_canonical_id(query.session_id.as_str(), "session_id")?;
    let limit = query.limit.unwrap_or(50).clamp(1, 250);

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ListDownloadArtifactsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        limit,
        quarantined_only: query.quarantined_only.unwrap_or(false),
    });
    apply_browser_service_session_auth(
        &state,
        session.context.principal.as_str(),
        request.metadata_mut(),
    )?;
    let response = client
        .list_download_artifacts(request)
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let artifacts = response
        .artifacts
        .into_iter()
        .map(control_plane_browser_download_artifact)
        .collect::<Vec<_>>();
    Ok(Json(control_plane::BrowserDownloadArtifactListEnvelope {
        contract: contract_descriptor(),
        session_id,
        truncated: response.truncated,
        error: response.error,
        page: build_page_info(limit as usize, artifacts.len(), None),
        artifacts,
    }))
}

/// `POST /console/v1/browser/relay/tokens` — mints a short-lived bearer token
/// that lets the browser extension perform a scoped set of relay actions
/// against one session, and records a `browser.relay.token.minted` audit
/// event (hash only, never the token).
///
/// The plaintext token appears exactly once, in this response; the daemon
/// keeps only its SHA-256 hash keyed in [`AppState::relay_tokens`].
///
/// # Errors
/// Returns an error response when console authorization fails, the session or
/// extension id is invalid, the system clock cannot be read, or the audit
/// event cannot be recorded.
pub(crate) async fn console_browser_relay_token_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleBrowserRelayTokenRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let session_id =
        required_console_browser_canonical_id(payload.session_id.as_str(), "session_id")?;
    let extension_id = normalize_browser_extension_id(payload.extension_id.as_str())?;
    let ttl_ms = clamp_console_relay_token_ttl_ms(payload.ttl_ms);
    let issued_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let expires_at_unix_ms =
        issued_at_unix_ms.saturating_add(i64::try_from(ttl_ms).unwrap_or(i64::MAX));
    let relay_token = mint_console_relay_token();
    let token_hash_sha256 = sha256_hex(relay_token.as_bytes());
    let record = ConsoleRelayToken {
        token_hash_sha256: token_hash_sha256.clone(),
        principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        session_id: session_id.clone(),
        extension_id: extension_id.clone(),
        issued_at_unix_ms,
        expires_at_unix_ms,
    };
    {
        // Prune both before and after the insert: the first pass clears
        // expired entries, the second re-enforces the size cap with the new
        // token counted (it may evict the earliest-expiring record).
        let mut relay_tokens = lock_relay_tokens(&state.relay_tokens);
        prune_console_relay_tokens(&mut relay_tokens, issued_at_unix_ms);
        relay_tokens.insert(token_hash_sha256.clone(), record.clone());
        prune_console_relay_tokens(&mut relay_tokens, issued_at_unix_ms);
    }

    state
        .runtime
        .record_console_event(
            &session.context,
            "browser.relay.token.minted",
            json!({
                "session_id": record.session_id,
                "extension_id": record.extension_id,
                "issued_at_unix_ms": record.issued_at_unix_ms,
                "expires_at_unix_ms": record.expires_at_unix_ms,
                "token_hash_sha256": record.token_hash_sha256,
            }),
        )
        .await
        .map_err(runtime_status_response)?;

    Ok(Json(json!({
        "relay_token": relay_token,
        "session_id": record.session_id,
        "extension_id": record.extension_id,
        "issued_at_unix_ms": record.issued_at_unix_ms,
        "expires_at_unix_ms": record.expires_at_unix_ms,
        "token_ttl_ms": ttl_ms,
        "warning": "Relay token grants scoped browser extension actions; keep it short-lived and private.",
    })))
}

/// `POST /console/v1/browser/relay/actions` — executes a relay action
/// (open_tab, capture_selection, send_page_snapshot) authenticated by a
/// bearer relay token instead of a console session, and records a
/// `browser.relay.action` audit event under the principal that minted the
/// token.
///
/// The token must match both the requested `session_id` and `extension_id`;
/// either mismatch is a permission error so a leaked token cannot be replayed
/// against another session or extension.
///
/// # Errors
/// Returns an error response when the relay token is missing/expired/
/// mismatched, the action or payload is invalid, the browserd RPC fails, or
/// the audit event cannot be recorded.
pub(crate) async fn console_browser_relay_action_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleBrowserRelayActionRequest>,
) -> Result<Json<Value>, Response> {
    let relay_token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(extract_bearer_token)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::permission_denied(
                "relay action requires bearer relay token",
            ))
        })?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let relay_token_hash_sha256 = sha256_hex(relay_token.as_bytes());
    let record = {
        let mut relay_tokens = lock_relay_tokens(&state.relay_tokens);
        prune_console_relay_tokens(&mut relay_tokens, now);
        let relay_token_key =
            find_hashed_secret_map_key(&relay_tokens, relay_token_hash_sha256.as_str())
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::permission_denied(
                        "relay token is missing, invalid, or expired",
                    ))
                })?;
        relay_tokens.get(relay_token_key.as_str()).cloned().ok_or_else(|| {
            runtime_status_response(tonic::Status::permission_denied(
                "relay token is missing, invalid, or expired",
            ))
        })?
    };

    let session_id =
        required_console_browser_canonical_id(payload.session_id.as_str(), "session_id")?;
    if session_id != record.session_id {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "relay token is not valid for the requested session_id",
        )));
    }
    let extension_id = normalize_browser_extension_id(payload.extension_id.as_str())?;
    if extension_id != record.extension_id {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "relay token is not valid for the requested extension_id",
        )));
    }

    let action = parse_console_relay_action_kind(payload.action.as_str())?;
    let relay_payload = match action {
        browser_v1::RelayActionKind::OpenTab => {
            let open_tab = payload.open_tab.ok_or_else(|| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "open_tab payload is required for action=open_tab",
                ))
            })?;
            let url = open_tab.url.trim();
            if url.is_empty() {
                return Err(runtime_status_response(tonic::Status::invalid_argument(
                    "open_tab.url cannot be empty",
                )));
            }
            Some(browser_v1::relay_action_request::Payload::OpenTab(
                browser_v1::RelayOpenTabPayload {
                    url: url.to_owned(),
                    activate: open_tab.activate.unwrap_or(true),
                    timeout_ms: open_tab.timeout_ms.unwrap_or(0),
                },
            ))
        }
        browser_v1::RelayActionKind::CaptureSelection => {
            let capture = payload.capture_selection.ok_or_else(|| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "capture_selection payload is required for action=capture_selection",
                ))
            })?;
            let selector = capture.selector.trim();
            if selector.is_empty() {
                return Err(runtime_status_response(tonic::Status::invalid_argument(
                    "capture_selection.selector cannot be empty",
                )));
            }
            Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                browser_v1::RelayCaptureSelectionPayload {
                    selector: selector.to_owned(),
                    max_selection_bytes: capture.max_selection_bytes.unwrap_or(0),
                },
            ))
        }
        browser_v1::RelayActionKind::SendPageSnapshot => {
            let snapshot =
                payload.page_snapshot.unwrap_or(ConsoleBrowserRelayPageSnapshotPayload {
                    include_dom_snapshot: Some(true),
                    include_visible_text: Some(true),
                    max_dom_snapshot_bytes: Some(16 * 1_024),
                    max_visible_text_bytes: Some(8 * 1_024),
                });
            Some(browser_v1::relay_action_request::Payload::PageSnapshot(
                browser_v1::RelayPageSnapshotPayload {
                    include_dom_snapshot: snapshot.include_dom_snapshot.unwrap_or(true),
                    include_visible_text: snapshot.include_visible_text.unwrap_or(true),
                    max_dom_snapshot_bytes: snapshot.max_dom_snapshot_bytes.unwrap_or(0),
                    max_visible_text_bytes: snapshot.max_visible_text_bytes.unwrap_or(0),
                },
            ))
        }
        // Unreachable in practice: parse_console_relay_action_kind rejects
        // unknown labels before this match.
        browser_v1::RelayActionKind::Unspecified => None,
    };

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::RelayActionRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        extension_id: extension_id.clone(),
        action: action as i32,
        payload: relay_payload,
        max_payload_bytes: payload
            .max_payload_bytes
            .unwrap_or(CONSOLE_MAX_RELAY_ACTION_PAYLOAD_BYTES)
            .clamp(1, CONSOLE_MAX_RELAY_ACTION_PAYLOAD_BYTES),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.relay_action(request).await.map_err(runtime_status_response)?.into_inner();

    let result = match response.result {
        Some(browser_v1::relay_action_response::Result::OpenedTab(tab)) => {
            json!({ "opened_tab": console_browser_tab_to_json(tab) })
        }
        Some(browser_v1::relay_action_response::Result::Selection(selection)) => json!({
            "selection": {
                "selector": selection.selector,
                "selected_text": selection.selected_text,
                "truncated": selection.truncated,
            }
        }),
        Some(browser_v1::relay_action_response::Result::Snapshot(snapshot)) => json!({
            "snapshot": {
                "dom_snapshot": snapshot.dom_snapshot,
                "visible_text": snapshot.visible_text,
                "dom_truncated": snapshot.dom_truncated,
                "visible_text_truncated": snapshot.visible_text_truncated,
                "page_url": snapshot.page_url,
            }
        }),
        None => Value::Null,
    };

    let audit_context = gateway::RequestContext {
        principal: record.principal.clone(),
        device_id: record.device_id.clone(),
        channel: record.channel.clone(),
    };
    state
        .runtime
        .record_console_event(
            &audit_context,
            "browser.relay.action",
            json!({
                "session_id": record.session_id,
                "extension_id": record.extension_id,
                "action": relay_action_kind_label(response.action),
                "success": response.success,
                "error": response.error,
                "token_hash_sha256": record.token_hash_sha256,
            }),
        )
        .await
        .map_err(runtime_status_response)?;

    Ok(Json(json!({
        "success": response.success,
        "action": relay_action_kind_label(response.action),
        "error": response.error,
        "result": result,
    })))
}

/// Picks the browser principal: an explicit non-empty request value wins,
/// otherwise the authenticated console session's principal is used.
///
/// # Errors
/// Returns an invalid-argument response when the resolved principal is empty
/// or longer than 128 bytes.
#[allow(clippy::result_large_err)]
fn resolve_console_browser_principal(
    requested: Option<&str>,
    fallback: &str,
) -> Result<String, Response> {
    let value =
        requested.map(str::trim).filter(|value| !value.is_empty()).unwrap_or(fallback).trim();
    if value.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "principal cannot be empty",
        )));
    }
    if value.len() > 128 {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "principal exceeds max bytes (128)",
        )));
    }
    Ok(value.to_owned())
}

fn normalize_optional_console_browser_channel(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|candidate| !candidate.is_empty()).map(str::to_owned)
}

/// Validates that `raw` (after trimming) is a canonical ULID.
///
/// # Errors
/// Returns an invalid-argument response naming `field_name` otherwise.
#[allow(clippy::result_large_err)]
fn validate_console_browser_canonical_id(raw: &str, field_name: &str) -> Result<(), Response> {
    validate_canonical_id(raw.trim()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field_name} must be a canonical ULID",
        )))
    })
}

/// Trims and validates a mandatory canonical-ULID field.
///
/// # Errors
/// Returns an invalid-argument response when the value is empty or not a
/// canonical ULID.
#[allow(clippy::result_large_err)]
fn required_console_browser_canonical_id(raw: &str, field_name: &str) -> Result<String, Response> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field_name} cannot be empty",
        ))));
    }
    validate_console_browser_canonical_id(value, field_name)?;
    Ok(value.to_owned())
}

/// Converts an optional canonical-ULID field into proto form; absent or blank
/// values become `None`.
///
/// # Errors
/// Returns an invalid-argument response when a non-blank value is not a
/// canonical ULID.
#[allow(clippy::result_large_err)]
fn optional_console_browser_canonical_id(
    raw: Option<&str>,
    field_name: &str,
) -> Result<Option<common_v1::CanonicalId>, Response> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    validate_console_browser_canonical_id(value, field_name)?;
    Ok(Some(common_v1::CanonicalId { ulid: value.to_owned() }))
}

fn clamp_console_browser_max_title_bytes(state: &AppState, value: Option<u64>) -> u64 {
    let browser_service_config = state.runtime.browser_service_config_snapshot();
    value
        .unwrap_or(browser_service_config.max_title_bytes as u64)
        .clamp(1, browser_service_config.max_title_bytes as u64)
}

fn clamp_console_browser_max_screenshot_bytes(state: &AppState, value: Option<u64>) -> u64 {
    let browser_service_config = state.runtime.browser_service_config_snapshot();
    value
        .unwrap_or(browser_service_config.max_screenshot_bytes as u64)
        .clamp(1, browser_service_config.max_screenshot_bytes as u64)
}

fn console_browser_session_budget_to_proto(
    budget: &control_plane::BrowserSessionBudget,
) -> browser_v1::SessionBudget {
    browser_v1::SessionBudget {
        max_navigation_timeout_ms: budget.max_navigation_timeout_ms.unwrap_or(0),
        max_session_lifetime_ms: budget.max_session_lifetime_ms.unwrap_or(0),
        max_screenshot_bytes: budget.max_screenshot_bytes.unwrap_or(0),
        max_response_bytes: budget.max_response_bytes.unwrap_or(0),
        max_action_timeout_ms: budget.max_action_timeout_ms.unwrap_or(0),
        max_type_input_bytes: budget.max_type_input_bytes.unwrap_or(0),
        max_actions_per_session: budget.max_actions_per_session.unwrap_or(0),
        max_actions_per_window: budget.max_actions_per_window.unwrap_or(0),
        action_rate_window_ms: budget.action_rate_window_ms.unwrap_or(0),
        max_action_log_entries: budget.max_action_log_entries.unwrap_or(0),
        max_observe_snapshot_bytes: budget.max_observe_snapshot_bytes.unwrap_or(0),
        max_visible_text_bytes: budget.max_visible_text_bytes.unwrap_or(0),
        max_network_log_entries: budget.max_network_log_entries.unwrap_or(0),
        max_network_log_bytes: budget.max_network_log_bytes.unwrap_or(0),
    }
}

fn control_plane_browser_session_budget(
    budget: browser_v1::SessionBudget,
) -> control_plane::BrowserSessionBudget {
    control_plane::BrowserSessionBudget {
        max_navigation_timeout_ms: Some(budget.max_navigation_timeout_ms),
        max_session_lifetime_ms: Some(budget.max_session_lifetime_ms),
        max_screenshot_bytes: Some(budget.max_screenshot_bytes),
        max_response_bytes: Some(budget.max_response_bytes),
        max_action_timeout_ms: Some(budget.max_action_timeout_ms),
        max_type_input_bytes: Some(budget.max_type_input_bytes),
        max_actions_per_session: Some(budget.max_actions_per_session),
        max_actions_per_window: Some(budget.max_actions_per_window),
        action_rate_window_ms: Some(budget.action_rate_window_ms),
        max_action_log_entries: Some(budget.max_action_log_entries),
        max_observe_snapshot_bytes: Some(budget.max_observe_snapshot_bytes),
        max_visible_text_bytes: Some(budget.max_visible_text_bytes),
        max_network_log_entries: Some(budget.max_network_log_entries),
        max_network_log_bytes: Some(budget.max_network_log_bytes),
    }
}

fn maybe_canonical_id(value: Option<common_v1::CanonicalId>) -> Option<String> {
    value.map(|candidate| candidate.ulid)
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn encode_optional_base64(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        None
    } else {
        Some(BASE64_STANDARD.encode(bytes))
    }
}

fn browser_permission_setting_to_proto(
    value: Option<control_plane::BrowserPermissionSetting>,
) -> i32 {
    match value.unwrap_or(control_plane::BrowserPermissionSetting::Unspecified) {
        control_plane::BrowserPermissionSetting::Unspecified => 0,
        control_plane::BrowserPermissionSetting::Deny => 1,
        control_plane::BrowserPermissionSetting::Allow => 2,
    }
}

fn control_plane_browser_permission_setting(value: i32) -> control_plane::BrowserPermissionSetting {
    match value {
        1 => control_plane::BrowserPermissionSetting::Deny,
        2 => control_plane::BrowserPermissionSetting::Allow,
        _ => control_plane::BrowserPermissionSetting::Unspecified,
    }
}

fn control_plane_browser_permissions(
    permissions: browser_v1::SessionPermissions,
) -> control_plane::BrowserSessionPermissions {
    control_plane::BrowserSessionPermissions {
        camera: control_plane_browser_permission_setting(permissions.camera),
        microphone: control_plane_browser_permission_setting(permissions.microphone),
        location: control_plane_browser_permission_setting(permissions.location),
    }
}

fn control_plane_browser_action_log(
    entry: browser_v1::BrowserActionLogEntry,
) -> control_plane::BrowserActionLogEntry {
    control_plane::BrowserActionLogEntry {
        action_id: entry.action_id,
        action_name: entry.action_name,
        selector: entry.selector,
        success: entry.success,
        outcome: entry.outcome,
        error: entry.error,
        started_at_unix_ms: entry.started_at_unix_ms,
        completed_at_unix_ms: entry.completed_at_unix_ms,
        attempts: entry.attempts,
        page_url: entry.page_url,
    }
}

fn control_plane_browser_network_log_entry(
    entry: browser_v1::NetworkLogEntry,
) -> control_plane::BrowserNetworkLogEntry {
    let mut headers = entry
        .headers
        .into_iter()
        .map(|header| control_plane::BrowserNetworkLogHeader {
            name: header.name,
            value: header.value,
        })
        .collect::<Vec<_>>();
    // Sorted so the envelope is deterministic regardless of capture order.
    headers.sort_by(|left, right| left.name.cmp(&right.name));
    control_plane::BrowserNetworkLogEntry {
        request_url: entry.request_url,
        status_code: entry.status_code,
        timing_bucket: entry.timing_bucket,
        latency_ms: entry.latency_ms,
        captured_at_unix_ms: entry.captured_at_unix_ms,
        headers,
    }
}

fn browser_diagnostic_severity_to_proto(
    value: Option<control_plane::BrowserDiagnosticSeverity>,
) -> i32 {
    match value {
        None => browser_v1::BrowserDiagnosticSeverity::Unspecified as i32,
        Some(control_plane::BrowserDiagnosticSeverity::Debug) => {
            browser_v1::BrowserDiagnosticSeverity::Debug as i32
        }
        Some(control_plane::BrowserDiagnosticSeverity::Info) => {
            browser_v1::BrowserDiagnosticSeverity::Info as i32
        }
        Some(control_plane::BrowserDiagnosticSeverity::Warn) => {
            browser_v1::BrowserDiagnosticSeverity::Warn as i32
        }
        Some(control_plane::BrowserDiagnosticSeverity::Error) => {
            browser_v1::BrowserDiagnosticSeverity::Error as i32
        }
    }
}

fn control_plane_browser_console_severity(value: i32) -> control_plane::BrowserDiagnosticSeverity {
    match browser_v1::BrowserDiagnosticSeverity::try_from(value)
        .unwrap_or(browser_v1::BrowserDiagnosticSeverity::Unspecified)
    {
        browser_v1::BrowserDiagnosticSeverity::Debug => {
            control_plane::BrowserDiagnosticSeverity::Debug
        }
        browser_v1::BrowserDiagnosticSeverity::Warn => {
            control_plane::BrowserDiagnosticSeverity::Warn
        }
        browser_v1::BrowserDiagnosticSeverity::Error => {
            control_plane::BrowserDiagnosticSeverity::Error
        }
        browser_v1::BrowserDiagnosticSeverity::Info
        | browser_v1::BrowserDiagnosticSeverity::Unspecified => {
            control_plane::BrowserDiagnosticSeverity::Info
        }
    }
}

fn control_plane_browser_console_entry(
    entry: browser_v1::BrowserConsoleEntry,
) -> control_plane::BrowserConsoleEntry {
    control_plane::BrowserConsoleEntry {
        severity: control_plane_browser_console_severity(entry.severity),
        kind: entry.kind,
        message: entry.message,
        captured_at_unix_ms: entry.captured_at_unix_ms,
        source: entry.source,
        stack_trace: entry.stack_trace,
        page_url: entry.page_url,
    }
}

fn control_plane_browser_page_diagnostics(
    diagnostics: browser_v1::BrowserPageDiagnostics,
) -> control_plane::BrowserPageDiagnostics {
    control_plane::BrowserPageDiagnostics {
        page_url: diagnostics.page_url,
        page_title: diagnostics.page_title,
        console_entry_count: diagnostics.console_entry_count,
        warning_count: diagnostics.warning_count,
        error_count: diagnostics.error_count,
        last_event_unix_ms: diagnostics.last_event_unix_ms,
    }
}

fn control_plane_browser_profile(
    profile: browser_v1::BrowserProfile,
) -> control_plane::BrowserProfileRecord {
    control_plane::BrowserProfileRecord {
        profile_id: maybe_canonical_id(profile.profile_id),
        principal: profile.principal,
        name: profile.name,
        theme_color: profile.theme_color,
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
        last_used_unix_ms: profile.last_used_unix_ms,
        persistence_enabled: profile.persistence_enabled,
        private_profile: profile.private_profile,
        active: profile.active,
    }
}

fn control_plane_browser_tab(tab: browser_v1::BrowserTab) -> control_plane::BrowserTabRecord {
    control_plane::BrowserTabRecord {
        tab_id: maybe_canonical_id(tab.tab_id),
        url: tab.url,
        title: tab.title,
        active: tab.active,
    }
}

fn control_plane_browser_download_artifact(
    artifact: browser_v1::DownloadArtifact,
) -> control_plane::BrowserDownloadArtifactRecord {
    control_plane::BrowserDownloadArtifactRecord {
        artifact_id: maybe_canonical_id(artifact.artifact_id),
        session_id: maybe_canonical_id(artifact.session_id),
        profile_id: maybe_canonical_id(artifact.profile_id),
        source_url: artifact.source_url,
        file_name: artifact.file_name,
        mime_type: artifact.mime_type,
        size_bytes: artifact.size_bytes,
        sha256: artifact.sha256,
        created_at_unix_ms: artifact.created_at_unix_ms,
        quarantined: artifact.quarantined,
        quarantine_reason: artifact.quarantine_reason,
    }
}

fn session_summary_to_value(summary: &browser_v1::BrowserSessionSummary) -> Value {
    json!({
        "session_id": maybe_canonical_id(summary.session_id.clone()),
        "principal": summary.principal,
        "channel": summary.channel,
        "created_at_unix_ms": summary.created_at_unix_ms,
        "last_active_unix_ms": summary.last_active_unix_ms,
        "idle_ttl_ms": summary.idle_ttl_ms,
        "age_ms": summary.age_ms,
        "idle_for_ms": summary.idle_for_ms,
        "action_count": summary.action_count,
        "action_log_entries": summary.action_log_entries,
        "tab_count": summary.tab_count,
        "active_tab_id": maybe_canonical_id(summary.active_tab_id.clone()),
        "active_tab_url": summary.active_tab_url,
        "active_tab_title": summary.active_tab_title,
        "allow_private_targets": summary.allow_private_targets,
        "downloads_enabled": summary.downloads_enabled,
        "persistence_enabled": summary.persistence_enabled,
        "persistence_id": summary.persistence_id,
        "state_restored": summary.state_restored,
        "profile_id": maybe_canonical_id(summary.profile_id.clone()),
        "private_profile": summary.private_profile,
        "action_allowed_domains": summary.action_allowed_domains,
        "permissions": summary
            .permissions
            .as_ref()
            .map(session_permissions_to_value)
            .unwrap_or(Value::Null),
    })
}

fn session_detail_to_value(detail: &browser_v1::BrowserSessionDetail) -> Value {
    json!({
        "summary": detail.summary.as_ref().map(session_summary_to_value).unwrap_or(Value::Null),
        "effective_budget": detail
            .effective_budget
            .as_ref()
            .map(session_budget_to_value)
            .unwrap_or(Value::Null),
        "tabs": detail.tabs.iter().map(browser_tab_to_value).collect::<Vec<_>>(),
    })
}

fn session_budget_to_value(budget: &browser_v1::SessionBudget) -> Value {
    json!({
        "max_navigation_timeout_ms": budget.max_navigation_timeout_ms,
        "max_session_lifetime_ms": budget.max_session_lifetime_ms,
        "max_screenshot_bytes": budget.max_screenshot_bytes,
        "max_response_bytes": budget.max_response_bytes,
        "max_action_timeout_ms": budget.max_action_timeout_ms,
        "max_type_input_bytes": budget.max_type_input_bytes,
        "max_actions_per_session": budget.max_actions_per_session,
        "max_actions_per_window": budget.max_actions_per_window,
        "action_rate_window_ms": budget.action_rate_window_ms,
        "max_action_log_entries": budget.max_action_log_entries,
        "max_observe_snapshot_bytes": budget.max_observe_snapshot_bytes,
        "max_visible_text_bytes": budget.max_visible_text_bytes,
        "max_network_log_entries": budget.max_network_log_entries,
        "max_network_log_bytes": budget.max_network_log_bytes,
    })
}

fn session_permissions_to_value(permissions: &browser_v1::SessionPermissions) -> Value {
    json!({
        "camera": browser_permission_setting_text(permissions.camera),
        "microphone": browser_permission_setting_text(permissions.microphone),
        "location": browser_permission_setting_text(permissions.location),
    })
}

fn browser_permission_setting_text(value: i32) -> &'static str {
    match browser_v1::PermissionSetting::try_from(value)
        .unwrap_or(browser_v1::PermissionSetting::Unspecified)
    {
        browser_v1::PermissionSetting::Allow => "allow",
        browser_v1::PermissionSetting::Deny => "deny",
        browser_v1::PermissionSetting::Unspecified => "unspecified",
    }
}

fn browser_tab_to_value(tab: &browser_v1::BrowserTab) -> Value {
    json!({
        "tab_id": maybe_canonical_id(tab.tab_id.clone()),
        "url": tab.url,
        "title": tab.title,
        "active": tab.active,
    })
}

fn cookie_domain_to_value(value: &browser_v1::SessionCookieDomain) -> Value {
    json!({
        "domain": value.domain,
        "cookies": value.cookies.iter().map(|cookie| {
            json!({
                "name": cookie.name,
                "value": cookie.value,
            })
        }).collect::<Vec<_>>(),
    })
}

fn storage_origin_to_value(value: &browser_v1::SessionStorageOrigin) -> Value {
    json!({
        "origin": value.origin,
        "entries": value.entries.iter().map(|entry| {
            json!({
                "key": entry.key,
                "value": entry.value,
            })
        }).collect::<Vec<_>>(),
    })
}

fn browser_action_log_entry_to_value(entry: &browser_v1::BrowserActionLogEntry) -> Value {
    json!({
        "action_id": entry.action_id,
        "action_name": entry.action_name,
        "selector": entry.selector,
        "success": entry.success,
        "outcome": entry.outcome,
        "error": entry.error,
        "started_at_unix_ms": entry.started_at_unix_ms,
        "completed_at_unix_ms": entry.completed_at_unix_ms,
        "attempts": entry.attempts,
        "page_url": entry.page_url,
    })
}

fn browser_network_log_entry_to_value(entry: &browser_v1::NetworkLogEntry) -> Value {
    json!({
        "request_url": entry.request_url,
        "status_code": entry.status_code,
        "timing_bucket": entry.timing_bucket,
        "latency_ms": entry.latency_ms,
        "captured_at_unix_ms": entry.captured_at_unix_ms,
        "headers": entry.headers.iter().map(|header| {
            json!({
                "name": header.name,
                "value": header.value,
            })
        }).collect::<Vec<_>>(),
    })
}

fn browser_console_entry_to_value(entry: &browser_v1::BrowserConsoleEntry) -> Value {
    json!({
        "severity": browser_console_severity_text(entry.severity),
        "kind": entry.kind,
        "message": entry.message,
        "captured_at_unix_ms": entry.captured_at_unix_ms,
        "source": entry.source,
        "stack_trace": entry.stack_trace,
        "page_url": entry.page_url,
    })
}

fn browser_console_severity_text(value: i32) -> &'static str {
    match browser_v1::BrowserDiagnosticSeverity::try_from(value)
        .unwrap_or(browser_v1::BrowserDiagnosticSeverity::Unspecified)
    {
        browser_v1::BrowserDiagnosticSeverity::Debug => "debug",
        browser_v1::BrowserDiagnosticSeverity::Info => "info",
        browser_v1::BrowserDiagnosticSeverity::Warn => "warn",
        browser_v1::BrowserDiagnosticSeverity::Error => "error",
        browser_v1::BrowserDiagnosticSeverity::Unspecified => "unspecified",
    }
}

fn browser_page_diagnostics_to_value(value: &browser_v1::BrowserPageDiagnostics) -> Value {
    json!({
        "page_url": value.page_url,
        "page_title": value.page_title,
        "console_entry_count": value.console_entry_count,
        "warning_count": value.warning_count,
        "error_count": value.error_count,
        "last_event_unix_ms": value.last_event_unix_ms,
    })
}

/// Records a `browser.*` console audit event after redacting identifier
/// fields from the details payload.
///
/// Failure to record is propagated to the caller, so mutating browser
/// handlers fail closed: an action whose audit trail cannot be written is
/// reported as an error even though browserd already performed it.
///
/// # Errors
/// Returns the mapped runtime status response when the journal write fails.
async fn record_browser_console_event(
    state: &AppState,
    context: &gateway::RequestContext,
    event: &str,
    mut details: Value,
) -> Result<(), Response> {
    redact_browser_console_event_details(&mut details, None);
    state
        .runtime
        .record_console_event(context, event, details)
        .await
        .map_err(runtime_status_response)
}

/// Shortens an identifier to a `head***tail` form for audit payloads.
fn redact_browser_console_identifier(value: &str) -> String {
    gateway::redact_session_id(value)
}

/// Returns `true` for JSON keys whose string values are browser identifiers.
///
/// Session/tab/profile/artifact ids double as capability handles on the
/// `/console/v1/browser` surface, so audit events store them redacted to keep
/// the journal from becoming a replayable handle inventory.
fn browser_console_identifier_key(key: &str) -> bool {
    matches!(
        key,
        "session_id"
            | "active_tab_id"
            | "tab_id"
            | "closed_tab_id"
            | "profile_id"
            | "active_profile_id"
            | "artifact_id"
            | "action_id"
    )
}

/// Recursively redacts identifier-keyed string values inside an audit details
/// payload. Array elements inherit the key of the field that contains them.
fn redact_browser_console_event_details(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                redact_browser_console_event_details(entry, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_browser_console_event_details(item, key_context);
            }
        }
        Value::String(text)
            if key_context.is_some_and(browser_console_identifier_key)
                && !text.trim().is_empty() =>
        {
            *text = redact_browser_console_identifier(text.as_str());
        }
        _ => {}
    }
}

/// Trims and validates a browser extension id (ASCII alphanumerics plus
/// `.`, `-`, `_`, length-capped).
///
/// # Errors
/// Returns an invalid-argument response when the id is empty, too long, or
/// contains unsupported characters.
#[allow(clippy::result_large_err)]
fn normalize_browser_extension_id(raw: &str) -> Result<String, Response> {
    let extension_id = raw.trim();
    if extension_id.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "extension_id cannot be empty",
        )));
    }
    if extension_id.len() > CONSOLE_MAX_RELAY_EXTENSION_ID_BYTES {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "extension_id exceeds max bytes ({CONSOLE_MAX_RELAY_EXTENSION_ID_BYTES})",
        ))));
    }
    if !extension_id
        .bytes()
        .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'-' | b'_'))
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "extension_id contains unsupported characters",
        )));
    }
    Ok(extension_id.to_owned())
}

/// Clamps a requested relay-token TTL into the supported window, defaulting
/// when absent.
pub(crate) fn clamp_console_relay_token_ttl_ms(value: Option<u64>) -> u64 {
    value
        .unwrap_or(CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS)
        .clamp(CONSOLE_RELAY_TOKEN_MIN_TTL_MS, CONSOLE_RELAY_TOKEN_MAX_TTL_MS)
}

/// Mints a 256-bit random secret encoded as URL-safe base64 without padding.
/// Shared by console session, CSRF, handoff, and relay token minting.
pub(crate) fn mint_console_secret_token() -> String {
    let token_bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes)
}

/// Mints a browser-extension relay token (same entropy/encoding as every
/// other console secret).
pub(crate) fn mint_console_relay_token() -> String {
    mint_console_secret_token()
}

/// Locks the relay-token map, recovering from lock poisoning instead of
/// panicking: the map only holds expiring token records, so serving requests
/// with whatever state the panicked thread left behind is strictly better
/// than taking the whole console surface down.
fn lock_relay_tokens<'a>(
    tokens: &'a Arc<Mutex<HashMap<String, ConsoleRelayToken>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, ConsoleRelayToken>> {
    match tokens.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("relay token map lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

/// Compares two byte slices in time independent of where they first differ.
///
/// Used for secret-hash comparisons so an attacker cannot binary-search a
/// token byte-by-byte from response latency. A length mismatch still returns
/// `false` only after scanning `max(len)` bytes.
pub(crate) fn constant_time_eq_bytes(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

/// Finds the map key equal to `candidate_hash` without early exit.
///
/// INTENTIONAL: this is a full linear scan instead of `HashMap::get` so the
/// lookup cost does not reveal whether (or where) a candidate hash exists in
/// the map. Hash keys are unique, so at most one key can match.
pub(crate) fn find_hashed_secret_map_key<T>(
    values: &HashMap<String, T>,
    candidate_hash: &str,
) -> Option<String> {
    let mut matched: Option<String> = None;
    for token_hash in values.keys() {
        if constant_time_eq_bytes(token_hash.as_bytes(), candidate_hash.as_bytes()) {
            matched = Some(token_hash.clone());
        }
    }
    matched
}

/// Drops expired relay tokens, then evicts earliest-expiring tokens until the
/// map fits the `CONSOLE_MAX_RELAY_TOKENS` cap (bounds memory under token
/// minting abuse).
pub(crate) fn prune_console_relay_tokens(
    tokens: &mut HashMap<String, ConsoleRelayToken>,
    now_unix_ms: i64,
) {
    tokens.retain(|_, value| value.expires_at_unix_ms > now_unix_ms);
    while tokens.len() > CONSOLE_MAX_RELAY_TOKENS {
        let removable = tokens
            .iter()
            .min_by(|left, right| left.1.expires_at_unix_ms.cmp(&right.1.expires_at_unix_ms))
            .map(|(token, _)| token.clone());
        if let Some(token) = removable {
            tokens.remove(token.as_str());
        } else {
            break;
        }
    }
}

/// Extracts the token from a case-insensitive `Bearer <token>` authorization
/// value; returns `None` for any other shape or an empty token.
fn extract_bearer_token(raw_authorization: &str) -> Option<String> {
    let trimmed = raw_authorization.trim();
    let prefix = "bearer ";
    if trimmed.len() <= prefix.len() || !trimmed[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return None;
    }
    let token = trimmed[prefix.len()..].trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

/// Parses the relay action discriminator from its wire label.
///
/// # Errors
/// Returns an invalid-argument response listing the supported actions when
/// the label is unknown.
#[allow(clippy::result_large_err)]
fn parse_console_relay_action_kind(raw: &str) -> Result<browser_v1::RelayActionKind, Response> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "open_tab" => Ok(browser_v1::RelayActionKind::OpenTab),
        "capture_selection" => Ok(browser_v1::RelayActionKind::CaptureSelection),
        "send_page_snapshot" => Ok(browser_v1::RelayActionKind::SendPageSnapshot),
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "action must be one of open_tab|capture_selection|send_page_snapshot",
        ))),
    }
}

/// Maps a proto relay-action discriminator back to its stable wire label.
fn relay_action_kind_label(raw: i32) -> &'static str {
    match browser_v1::RelayActionKind::try_from(raw)
        .unwrap_or(browser_v1::RelayActionKind::Unspecified)
    {
        browser_v1::RelayActionKind::OpenTab => "open_tab",
        browser_v1::RelayActionKind::CaptureSelection => "capture_selection",
        browser_v1::RelayActionKind::SendPageSnapshot => "send_page_snapshot",
        browser_v1::RelayActionKind::Unspecified => "unspecified",
    }
}

/// Connects a gRPC client to the configured browserd endpoint with the
/// configured connect/request timeouts. Also used by self-healing probes.
///
/// # Errors
/// Returns a failed-precondition response when the browser service is
/// disabled, an invalid-argument response for a malformed endpoint, and an
/// unavailable response when the connection cannot be established.
pub(crate) async fn build_console_browser_client(
    state: &AppState,
) -> Result<
    browser_v1::browser_service_client::BrowserServiceClient<tonic::transport::Channel>,
    Response,
> {
    let browser_service_config = state.runtime.browser_service_config_snapshot();
    if !browser_service_config.enabled {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "browser service is disabled (tool_call.browser_service.enabled=false)",
        )));
    }
    let endpoint = tonic::transport::Endpoint::from_shared(browser_service_config.endpoint.clone())
        .map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "invalid browser service endpoint '{}': {error}",
                browser_service_config.endpoint
            )))
        })?
        .connect_timeout(std::time::Duration::from_millis(
            browser_service_config.connect_timeout_ms,
        ))
        .timeout(std::time::Duration::from_millis(browser_service_config.request_timeout_ms));
    let channel = endpoint.connect().await.map_err(|error| {
        runtime_status_response(tonic::Status::unavailable(format!(
            "failed to connect to browser service '{}': {error}",
            browser_service_config.endpoint
        )))
    })?;
    Ok(browser_v1::browser_service_client::BrowserServiceClient::new(channel))
}

/// Attaches the configured browserd bearer token to outgoing gRPC metadata;
/// a no-op when no auth token is configured.
///
/// # Errors
/// Returns an internal error response when the token cannot be encoded as
/// metadata.
#[allow(clippy::result_large_err)]
pub(crate) fn apply_browser_service_auth(
    state: &AppState,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    let browser_service_config = state.runtime.browser_service_config_snapshot();
    if let Some(token) = browser_service_config.auth_token.as_deref() {
        let bearer = MetadataValue::try_from(format!("Bearer {token}").as_str()).map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "failed to encode browser service authorization metadata",
            ))
        })?;
        metadata.insert("authorization", bearer);
    }
    Ok(())
}

/// Attaches service authentication and the authorized console principal to a
/// browserd request that can observe or mutate principal-owned state.
#[allow(clippy::result_large_err)]
fn apply_browser_service_session_auth(
    state: &AppState,
    principal: &str,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    apply_browser_service_auth(state, metadata)?;
    apply_browser_caller_principal_metadata(principal, metadata)
}

/// Forwards the console caller's principal to browserd via
/// [`BROWSER_CALLER_PRINCIPAL_HEADER`] so principal-scoped reads and
/// destructive session mutations can be enforced server-side.
///
/// # Errors
/// Returns an unauthenticated response for a blank principal and an
/// invalid-argument response when it cannot be encoded as metadata.
#[allow(clippy::result_large_err)]
pub(crate) fn apply_browser_caller_principal_metadata(
    principal: &str,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    let principal = principal.trim();
    if principal.is_empty() {
        return Err(runtime_status_response(tonic::Status::unauthenticated(
            "missing caller principal",
        )));
    }
    let value = tonic::metadata::MetadataValue::try_from(principal).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "failed to encode browser caller principal metadata",
        ))
    })?;
    metadata.insert(BROWSER_CALLER_PRINCIPAL_HEADER, value);
    Ok(())
}

fn console_browser_tab_to_json(tab: browser_v1::BrowserTab) -> Value {
    serde_json::to_value(control_plane_browser_tab(tab)).unwrap_or(Value::Null)
}

fn console_browser_observe_include_visible_text(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_console_browser_canonical_id_rejects_empty_values() {
        let response = required_console_browser_canonical_id("   ", "session_id")
            .expect_err("empty session_id should be rejected");
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn console_browser_private_targets_require_explicit_opt_in() {
        assert!(!console_browser_private_target_flag(None));
        assert!(!console_browser_private_target_flag(Some(false)));
        assert!(console_browser_private_target_flag(Some(true)));
    }

    #[test]
    fn browser_network_log_metadata_includes_caller_principal() {
        let mut metadata = tonic::metadata::MetadataMap::new();

        apply_browser_caller_principal_metadata(" user:local ", &mut metadata)
            .expect("principal metadata should attach");

        assert_eq!(
            metadata.get(BROWSER_CALLER_PRINCIPAL_HEADER).and_then(|value| value.to_str().ok()),
            Some("user:local")
        );
    }

    #[test]
    fn control_plane_browser_permission_setting_maps_proto_values() {
        assert_eq!(
            control_plane_browser_permission_setting(0),
            control_plane::BrowserPermissionSetting::Unspecified
        );
        assert_eq!(
            control_plane_browser_permission_setting(1),
            control_plane::BrowserPermissionSetting::Deny
        );
        assert_eq!(
            control_plane_browser_permission_setting(2),
            control_plane::BrowserPermissionSetting::Allow
        );
    }

    #[test]
    fn control_plane_browser_download_artifact_preserves_session_id() {
        let artifact = control_plane_browser_download_artifact(browser_v1::DownloadArtifact {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            artifact_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            }),
            session_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
            }),
            profile_id: Some(common_v1::CanonicalId {
                ulid: "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned(),
            }),
            source_url: "https://example.test/file".to_owned(),
            file_name: "file.txt".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: 42,
            sha256: "abc123".to_owned(),
            created_at_unix_ms: 7,
            quarantined: false,
            quarantine_reason: String::new(),
        });

        assert_eq!(artifact.session_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FB0"));
        assert_eq!(artifact.profile_id.as_deref(), Some("01ARZ3NDEKTSV4RRFFQ69G5FB1"));
    }

    #[test]
    fn console_observe_includes_visible_text_by_default() {
        assert!(console_browser_observe_include_visible_text(None));
        assert!(console_browser_observe_include_visible_text(Some(true)));
        assert!(!console_browser_observe_include_visible_text(Some(false)));
    }
}
