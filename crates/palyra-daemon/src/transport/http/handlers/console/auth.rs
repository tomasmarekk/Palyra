use crate::app::state::ConsoleBrowserHandoff;
use crate::*;

const CONSOLE_BROWSER_HANDOFF_TTL_MS: i64 = 60_000;
const DEFAULT_CONSOLE_BROWSER_REDIRECT_PATH: &str = "/#/control/overview";
const CONSOLE_BROWSER_HANDOFF_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleBrowserBootstrapQuery {
    token: String,
}

pub(crate) async fn console_login_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleLoginRequest>,
) -> Result<(HeaderMap, Json<ConsoleSessionResponse>), Response> {
    let requested_principal = payload.principal.trim();
    let device_id = payload.device_id.trim();
    if requested_principal.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "principal cannot be empty",
        )));
    }
    if !requested_principal.starts_with("admin:") {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "web console login requires an admin:* principal",
        )));
    }
    if device_id.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "device_id cannot be empty",
        )));
    }
    if state.auth.require_auth && state.auth.bound_principal.is_none() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "console login requires configured admin.bound_principal when auth is enabled",
        )));
    }
    let principal = state.auth.bound_principal.as_deref().unwrap_or(requested_principal);

    let mut auth_headers = HeaderMap::new();
    if let Some(token) = payload.admin_token.as_deref() {
        let token = token.trim();
        if token.is_empty() {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "admin_token cannot be empty when provided",
            )));
        }
        let authorization =
            HeaderValue::from_str(format!("Bearer {token}").as_str()).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "admin_token contains unsupported characters",
                ))
            })?;
        auth_headers.insert(AUTHORIZATION, authorization);
    }
    let principal_header = HeaderValue::from_str(principal).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "principal contains unsupported characters",
        ))
    })?;
    let device_header = HeaderValue::from_str(device_id).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "device_id contains unsupported characters",
        ))
    })?;
    auth_headers.insert(gateway::HEADER_PRINCIPAL, principal_header);
    auth_headers.insert(gateway::HEADER_DEVICE_ID, device_header);
    if let Some(channel) = payload.channel.as_deref() {
        let channel = channel.trim();
        if !channel.is_empty() {
            let channel_header = HeaderValue::from_str(channel).map_err(|_| {
                runtime_status_response(tonic::Status::invalid_argument(
                    "channel contains unsupported characters",
                ))
            })?;
            auth_headers.insert(gateway::HEADER_CHANNEL, channel_header);
        }
    }

    authorize_headers(&auth_headers, &state.auth).map_err(auth_error_response)?;
    let context = request_context_from_headers(&auth_headers).map_err(auth_error_response)?;
    if !context.principal.starts_with("admin:") {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "web console login requires an admin:* principal",
        )));
    }

    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let (session_token, session) = issue_console_session(&state, context, now);

    let secure_cookie = request_uses_tls(&headers);
    let mut response_headers = HeaderMap::new();
    response_headers
        .insert(SET_COOKIE, build_console_session_cookie(session_token.as_str(), secure_cookie)?);
    Ok((
        response_headers,
        Json(build_console_session_response(&session, session.csrf_token.clone())),
    ))
}

pub(crate) async fn console_logout_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<Value>), Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    {
        let mut sessions = lock_console_sessions(&state.console_sessions);
        sessions.remove(session.session_token_hash_sha256.as_str());
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(SET_COOKIE, clear_console_session_cookie(request_uses_tls(&headers))?);
    Ok((response_headers, Json(json!({ "signed_out": true }))))
}

pub(crate) async fn console_browser_handoff_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ConsoleBrowserHandoffRequest>,
) -> Result<Json<control_plane::ConsoleBrowserHandoffEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let redirect_path = normalize_console_browser_redirect_path(payload.redirect_path.as_deref())?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let expires_at_unix_ms = now.saturating_add(CONSOLE_BROWSER_HANDOFF_TTL_MS);
    let handoff_token = mint_console_secret_token();
    let handoff = ConsoleBrowserHandoff {
        token_hash_sha256: sha256_hex(handoff_token.as_bytes()),
        context: session.context.clone(),
        redirect_path,
        expires_at_unix_ms,
    };

    {
        let mut handoffs = lock_console_browser_handoffs(&state.console_browser_handoffs);
        handoffs.retain(|_, existing| existing.expires_at_unix_ms > now);
        handoffs.insert(handoff.token_hash_sha256.clone(), handoff);
    }

    let handoff_url = format!(
        "http://{CONSOLE_BROWSER_HANDOFF_HOST}:{}/console/v1/auth/browser-handoff/consume?token={handoff_token}",
        state.deployment.admin_port
    );
    Ok(Json(control_plane::ConsoleBrowserHandoffEnvelope { handoff_url, expires_at_unix_ms }))
}

pub(crate) async fn console_browser_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleBrowserBootstrapQuery>,
) -> Result<Response, Response> {
    let token = query.token.trim();
    if token.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "browser handoff token is required",
        )));
    }
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let handoff = {
        let mut handoffs = lock_console_browser_handoffs(&state.console_browser_handoffs);
        handoffs.retain(|_, existing| existing.expires_at_unix_ms > now);
        handoffs.remove(sha256_hex(token.as_bytes()).as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::permission_denied(
                "browser handoff token is invalid or expired",
            ))
        })?
    };
    let (session_token, _session) = issue_console_session(&state, handoff.context, now);
    let mut response_headers = HeaderMap::new();
    response_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response_headers.insert(
        SET_COOKIE,
        build_console_session_cookie(session_token.as_str(), request_uses_tls(&headers))?,
    );
    response_headers.insert(
        "location",
        HeaderValue::from_str(handoff.redirect_path.as_str()).map_err(|_| {
            runtime_status_response(tonic::Status::invalid_argument(
                "browser handoff redirect path contains unsupported characters",
            ))
        })?,
    );
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::SEE_OTHER;
    *response.headers_mut() = response_headers;
    Ok(response)
}

pub(crate) async fn console_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ConsoleSessionResponse>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(build_console_session_response(&session, session.csrf_token.clone())))
}

pub(crate) async fn console_capability_catalog_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::CapabilityCatalog>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(build_capability_catalog()?))
}

pub(crate) async fn console_deployment_posture_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::DeploymentPostureSummary>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(build_deployment_posture_summary(&state)))
}

pub(crate) async fn console_auth_profiles_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuthProfilesQuery>,
) -> Result<Json<control_plane::AuthProfileListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let mut request =
        TonicRequest::new(gateway::proto::palyra::auth::v1::ListAuthProfilesRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            after_profile_id: query.after_profile_id.unwrap_or_default(),
            limit,
            provider_kind: parse_console_auth_provider_kind(query.provider_kind.as_deref()) as i32,
            provider_custom_name: query.provider_custom_name.unwrap_or_default(),
            scope_kind: parse_console_auth_scope_kind(query.scope_kind.as_deref()) as i32,
            scope_agent_id: query.scope_agent_id.unwrap_or_default(),
        });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_auth_service(&state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::list_profiles(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let profiles = response
        .profiles
        .iter()
        .map(control_plane_auth_profile_from_proto)
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(Json(control_plane::AuthProfileListEnvelope {
        contract: contract_descriptor(),
        page: build_page_info(
            usize::try_from(limit).unwrap_or(usize::MAX),
            profiles.len(),
            trim_to_option(response.next_after_profile_id),
        ),
        profiles,
    }))
}

pub(crate) async fn console_auth_profile_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<Json<control_plane::AuthProfileEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let profile_id = normalize_non_empty_field(profile_id, "profile_id")?;
    let mut request = TonicRequest::new(gateway::proto::palyra::auth::v1::GetAuthProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        profile_id,
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_auth_service(&state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::get_profile(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let profile = response.profile.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "auth get response did not include profile",
        ))
    })?;
    Ok(Json(control_plane::AuthProfileEnvelope {
        contract: contract_descriptor(),
        profile: control_plane_auth_profile_from_proto(&profile)?,
    }))
}

pub(crate) async fn console_auth_profile_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(profile): Json<control_plane::AuthProfileView>,
) -> Result<Json<control_plane::AuthProfileEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let mut request = TonicRequest::new(gateway::proto::palyra::auth::v1::SetAuthProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        profile: Some(control_plane_auth_profile_to_proto(&profile)?),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_auth_service(&state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::set_profile(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let profile = response.profile.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "auth set response did not include profile",
        ))
    })?;
    Ok(Json(control_plane::AuthProfileEnvelope {
        contract: contract_descriptor(),
        profile: control_plane_auth_profile_from_proto(&profile)?,
    }))
}

pub(crate) async fn console_auth_profile_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<Json<control_plane::AuthProfileDeleteEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let profile_id = normalize_non_empty_field(profile_id, "profile_id")?;
    let existing_profile =
        state.auth_runtime.registry().get_profile(profile_id.as_str()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to load auth profile before delete: {error}"
            )))
        })?;
    let mut request =
        TonicRequest::new(gateway::proto::palyra::auth::v1::DeleteAuthProfileRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            profile_id: profile_id.clone(),
        });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_auth_service(&state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::delete_profile(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    if response.deleted
        && existing_profile
            .as_ref()
            .is_some_and(|profile| profile.provider.kind == AuthProviderKind::Openai)
    {
        let _ = clear_model_provider_auth_profile_selection_if_matches(
            &state,
            &session.context,
            profile_id.as_str(),
        )
        .await?;
    }
    Ok(Json(control_plane::AuthProfileDeleteEnvelope {
        contract: contract_descriptor(),
        profile_id,
        deleted: response.deleted,
    }))
}

pub(crate) async fn console_auth_health_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuthHealthQuery>,
) -> Result<Json<control_plane::AuthHealthEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let mut request = TonicRequest::new(gateway::proto::palyra::auth::v1::GetAuthHealthRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        agent_id: query.agent_id.unwrap_or_default(),
        include_profiles: query.include_profiles.unwrap_or(false),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_auth_service(&state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::get_health(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    Ok(Json(control_plane::AuthHealthEnvelope {
        contract: contract_descriptor(),
        summary: auth_health_summary_json(response.summary.as_ref()),
        expiry_distribution: auth_expiry_distribution_json(response.expiry_distribution.as_ref()),
        profiles: response.profiles.iter().map(auth_profile_health_json).collect(),
        refresh_metrics: auth_refresh_metrics_json(response.refresh_metrics.as_ref()),
    }))
}

pub(crate) async fn console_openai_provider_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::ProviderAuthStateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let profiles = list_console_auth_profiles(
        &state,
        &session,
        gateway::proto::palyra::auth::v1::AuthProviderKind::Openai,
    )
    .await?;
    let configured_path = std::env::var("PALYRA_CONFIG").ok();
    let (document, _, _) = load_console_config_snapshot(configured_path.as_deref(), true)?;
    Ok(Json(build_openai_provider_state(&document, profiles)))
}

pub(crate) async fn console_openai_provider_api_key_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::OpenAiApiKeyUpsertRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match connect_openai_api_key(&state, &session.context, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.api_key_connect",
                response.status(),
                auth_correlation_from_context(
                    &session.context,
                    profile_id.as_deref(),
                    None,
                    None,
                    None,
                ),
                false,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::OpenAiOAuthBootstrapRequest>,
) -> Result<Json<control_plane::OpenAiOAuthBootstrapEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match start_openai_oauth_attempt_from_request(&state, &session.context, &headers, payload).await
    {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.oauth_bootstrap",
                response.status(),
                auth_correlation_from_context(
                    &session.context,
                    profile_id.as_deref(),
                    None,
                    None,
                    None,
                ),
                false,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_callback_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleOpenAiCallbackStateQuery>,
) -> Result<Json<control_plane::OpenAiOAuthCallbackStateEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    load_openai_oauth_callback_state(&state, query.attempt_id.as_str()).await.map(Json)
}

pub(crate) async fn console_openai_provider_callback_handler(
    State(state): State<AppState>,
    Query(query): Query<ConsoleOpenAiCallbackQuery>,
) -> Result<Html<String>, Response> {
    state.observability.record_provider_auth_attempt();
    let attempt_id = query.state.clone();
    match complete_openai_oauth_callback(&state, query).await {
        Ok(page) => Ok(Html(page)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.oauth_callback",
                response.status(),
                ObservabilityCorrelationSnapshot {
                    auth_profile_id: Some(attempt_id),
                    ..ObservabilityCorrelationSnapshot::default()
                },
                false,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_reconnect_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::OpenAiOAuthBootstrapEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match reconnect_openai_oauth_attempt(&state, &session.context, &headers, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.oauth_reconnect",
                response.status(),
                auth_correlation_from_context(
                    &session.context,
                    profile_id.as_deref(),
                    None,
                    None,
                    None,
                ),
                false,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match refresh_openai_oauth_profile(&state, &session.context, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.oauth_refresh",
                response.status(),
                auth_correlation_from_context(
                    &session.context,
                    profile_id.as_deref(),
                    None,
                    None,
                    None,
                ),
                true,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_revoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match revoke_openai_auth_profile(&state, &session.context, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.oauth_revoke",
                response.status(),
                auth_correlation_from_context(
                    &session.context,
                    profile_id.as_deref(),
                    None,
                    None,
                    None,
                ),
                false,
            );
            Err(response)
        }
    }
}

pub(crate) async fn console_openai_provider_default_profile_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    select_default_openai_auth_profile(&state, &session.context, payload).await.map(Json)
}

fn issue_console_session(
    state: &AppState,
    context: gateway::RequestContext,
    now: i64,
) -> (String, ConsoleSession) {
    let expires_at_unix_ms = next_console_session_expiry_unix_ms(now);
    let session_token = mint_console_secret_token();
    let csrf_token = mint_console_secret_token();
    let session = ConsoleSession {
        session_token_hash_sha256: sha256_hex(session_token.as_bytes()),
        csrf_token,
        context,
        issued_at_unix_ms: now,
        expires_at_unix_ms,
    };

    {
        let mut sessions = lock_console_sessions(&state.console_sessions);
        sessions.retain(|_, existing| existing.expires_at_unix_ms > now);
        if sessions.len() >= CONSOLE_MAX_ACTIVE_SESSIONS {
            let mut oldest: Option<(String, i64)> = None;
            for (session_hash, existing) in sessions.iter() {
                if oldest
                    .as_ref()
                    .is_none_or(|(_, issued_at)| existing.issued_at_unix_ms < *issued_at)
                {
                    oldest = Some((session_hash.clone(), existing.issued_at_unix_ms));
                }
            }
            if let Some((session_hash, _)) = oldest {
                sessions.remove(session_hash.as_str());
            }
        }
        sessions.insert(session.session_token_hash_sha256.clone(), session.clone());
    }

    (session_token, session)
}

#[allow(clippy::result_large_err)]
fn normalize_console_browser_redirect_path(candidate: Option<&str>) -> Result<String, Response> {
    let redirect_path = candidate
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_CONSOLE_BROWSER_REDIRECT_PATH);
    if !redirect_path.starts_with('/') || redirect_path.starts_with("//") {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "browser handoff redirect path must stay same-origin",
        )));
    }
    if redirect_path.contains('\\') || redirect_path.contains('\r') || redirect_path.contains('\n')
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "browser handoff redirect path contains unsupported characters",
        )));
    }
    Ok(redirect_path.to_owned())
}

fn lock_console_browser_handoffs<'a>(
    handoffs: &'a Arc<Mutex<HashMap<String, ConsoleBrowserHandoff>>>,
) -> std::sync::MutexGuard<'a, HashMap<String, ConsoleBrowserHandoff>> {
    match handoffs.lock() {
        Ok(guard) => guard,
        Err(error) => error.into_inner(),
    }
}
