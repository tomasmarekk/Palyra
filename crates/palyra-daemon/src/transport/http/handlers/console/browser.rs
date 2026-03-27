use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use crate::*;

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
        allow_private_targets: payload.allow_private_targets.unwrap_or(false),
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

    record_browser_console_event(
        &state,
        &session.context,
        "browser.session.created",
        json!({
            "principal": principal,
            "channel": channel,
            "session_id": envelope.session_id,
            "downloads_enabled": envelope.downloads_enabled,
            "persistence_enabled": envelope.persistence_enabled,
            "state_restored": envelope.state_restored,
            "profile_id": envelope.profile_id,
            "private_profile": envelope.private_profile,
        }),
    )
    .await?;

    Ok(Json(envelope))
}

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
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response =
        client.close_session(request).await.map_err(runtime_status_response)?.into_inner();
    let envelope = control_plane::BrowserSessionCloseEnvelope {
        contract: contract_descriptor(),
        session_id: session_id.clone(),
        closed: response.closed,
        reason: response.reason.clone(),
    };

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
        allow_private_targets: payload.allow_private_targets.unwrap_or(false),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
            "artifact_id": envelope
                .artifact
                .as_ref()
                .and_then(|value| value.artifact_id.clone()),
        }),
    )
    .await?;

    Ok(Json(envelope))
}

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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_title_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserTitleQuery>,
) -> Result<Json<control_plane::BrowserTitleEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::GetTitleRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        max_title_bytes: clamp_console_browser_max_title_bytes(&state, query.max_title_bytes),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
    let response = client.get_title(request).await.map_err(runtime_status_response)?.into_inner();
    Ok(Json(control_plane::BrowserTitleEnvelope {
        contract: contract_descriptor(),
        session_id,
        success: response.success,
        title: response.title,
        error: response.error,
    }))
}

pub(crate) async fn console_browser_screenshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserScreenshotQuery>,
) -> Result<Json<control_plane::BrowserScreenshotEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_observe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserObserveQuery>,
) -> Result<Json<control_plane::BrowserObserveEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ObserveRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
        include_dom_snapshot: query.include_dom_snapshot.unwrap_or(true),
        include_accessibility_tree: query.include_accessibility_tree.unwrap_or(true),
        include_visible_text: query.include_visible_text.unwrap_or(false),
        max_dom_snapshot_bytes: query.max_dom_snapshot_bytes.unwrap_or(0),
        max_accessibility_tree_bytes: query.max_accessibility_tree_bytes.unwrap_or(0),
        max_visible_text_bytes: query.max_visible_text_bytes.unwrap_or(0),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_network_log_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(query): Query<ConsoleBrowserNetworkLogQuery>,
) -> Result<Json<control_plane::BrowserNetworkLogEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_tabs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::BrowserTabListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::ListTabsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
        allow_private_targets: payload.allow_private_targets.unwrap_or(false),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_permissions_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<control_plane::BrowserPermissionsEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_console_browser_canonical_id(session_id.as_str(), "session_id")?;

    let mut client = build_console_browser_client(&state).await?;
    let mut request = TonicRequest::new(browser_v1::GetPermissionsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.clone() }),
    });
    apply_browser_service_auth(&state, request.metadata_mut())?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

pub(crate) async fn console_browser_downloads_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleBrowserDownloadsQuery>,
) -> Result<Json<control_plane::BrowserDownloadArtifactListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
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
    apply_browser_service_auth(&state, request.metadata_mut())?;
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

#[allow(clippy::result_large_err)]
fn validate_console_browser_canonical_id(raw: &str, field_name: &str) -> Result<(), Response> {
    validate_canonical_id(raw.trim()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field_name} must be a canonical ULID",
        )))
    })
}

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
    value
        .unwrap_or(state.browser_service_config.max_title_bytes as u64)
        .clamp(1, state.browser_service_config.max_title_bytes as u64)
}

fn clamp_console_browser_max_screenshot_bytes(state: &AppState, value: Option<u64>) -> u64 {
    value
        .unwrap_or(state.browser_service_config.max_screenshot_bytes as u64)
        .clamp(1, state.browser_service_config.max_screenshot_bytes as u64)
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

fn redact_browser_console_identifier(value: &str) -> String {
    gateway::redact_session_id(value)
}

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
        Value::String(text) => {
            if key_context.is_some_and(browser_console_identifier_key) && !text.trim().is_empty() {
                *text = redact_browser_console_identifier(text.as_str());
            }
        }
        _ => {}
    }
}

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

pub(crate) fn clamp_console_relay_token_ttl_ms(value: Option<u64>) -> u64 {
    value
        .unwrap_or(CONSOLE_RELAY_TOKEN_DEFAULT_TTL_MS)
        .clamp(CONSOLE_RELAY_TOKEN_MIN_TTL_MS, CONSOLE_RELAY_TOKEN_MAX_TTL_MS)
}

pub(crate) fn mint_console_secret_token() -> String {
    let token_bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_bytes)
}

pub(crate) fn mint_console_relay_token() -> String {
    mint_console_secret_token()
}

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

pub(crate) async fn build_console_browser_client(
    state: &AppState,
) -> Result<
    browser_v1::browser_service_client::BrowserServiceClient<tonic::transport::Channel>,
    Response,
> {
    if !state.browser_service_config.enabled {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "browser service is disabled (tool_call.browser_service.enabled=false)",
        )));
    }
    let endpoint =
        tonic::transport::Endpoint::from_shared(state.browser_service_config.endpoint.clone())
            .map_err(|error| {
                runtime_status_response(tonic::Status::invalid_argument(format!(
                    "invalid browser service endpoint '{}': {error}",
                    state.browser_service_config.endpoint
                )))
            })?
            .connect_timeout(std::time::Duration::from_millis(
                state.browser_service_config.connect_timeout_ms,
            ))
            .timeout(std::time::Duration::from_millis(
                state.browser_service_config.request_timeout_ms,
            ));
    let channel = endpoint.connect().await.map_err(|error| {
        runtime_status_response(tonic::Status::unavailable(format!(
            "failed to connect to browser service '{}': {error}",
            state.browser_service_config.endpoint
        )))
    })?;
    Ok(browser_v1::browser_service_client::BrowserServiceClient::new(channel))
}

#[allow(clippy::result_large_err)]
pub(crate) fn apply_browser_service_auth(
    state: &AppState,
    metadata: &mut tonic::metadata::MetadataMap,
) -> Result<(), Response> {
    if let Some(token) = state.browser_service_config.auth_token.as_deref() {
        let bearer = MetadataValue::try_from(format!("Bearer {token}").as_str()).map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "failed to encode browser service authorization metadata",
            ))
        })?;
        metadata.insert("authorization", bearer);
    }
    Ok(())
}

fn console_browser_tab_to_json(tab: browser_v1::BrowserTab) -> Value {
    serde_json::to_value(control_plane_browser_tab(tab)).unwrap_or(Value::Null)
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
}
