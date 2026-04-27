use crate::app::state::ConsoleBrowserHandoff;
use crate::*;
use reqwest::Url;

const CONSOLE_BROWSER_HANDOFF_TTL_MS: i64 = 60_000;
const DEFAULT_CONSOLE_BROWSER_REDIRECT_PATH: &str = "/#/control/overview";
const CONSOLE_BROWSER_HANDOFF_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleBrowserBootstrapQuery {
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleBrowserBootstrapRequest {
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
        Json(build_console_session_response(&state, &session, session.csrf_token.clone())),
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
    Ok(Json(create_console_browser_handoff(
        &state,
        &session.context,
        payload.redirect_path.as_deref(),
    )?))
}

#[allow(clippy::result_large_err)]
pub(crate) fn create_console_browser_handoff(
    state: &AppState,
    context: &gateway::RequestContext,
    redirect_path: Option<&str>,
) -> Result<control_plane::ConsoleBrowserHandoffEnvelope, Response> {
    let redirect_path = normalize_console_browser_redirect_path(redirect_path)?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let expires_at_unix_ms = now.saturating_add(CONSOLE_BROWSER_HANDOFF_TTL_MS);
    let handoff_token = mint_console_secret_token();
    let handoff = ConsoleBrowserHandoff {
        token_hash_sha256: sha256_hex(handoff_token.as_bytes()),
        context: context.clone(),
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
    Ok(control_plane::ConsoleBrowserHandoffEnvelope { handoff_url, expires_at_unix_ms })
}

pub(crate) async fn console_browser_bootstrap_handler(
    State(state): State<AppState>,
    Query(query): Query<ConsoleBrowserBootstrapQuery>,
) -> Result<Response, Response> {
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let handoff = load_console_browser_handoff(&state, query.token.trim(), now)?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response_headers.insert(
        "location",
        HeaderValue::from_str(
            build_console_browser_bootstrap_redirect(
                handoff.redirect_path.as_str(),
                query.token.trim(),
            )?
            .as_str(),
        )
        .map_err(|_| {
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

pub(crate) async fn console_browser_session_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleBrowserBootstrapRequest>,
) -> Result<(HeaderMap, Json<ConsoleSessionResponse>), Response> {
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let handoff = consume_console_browser_handoff(&state, payload.token.trim(), now)?;
    let (session_token, session) = issue_console_session(&state, handoff.context, now);
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        SET_COOKIE,
        build_console_session_cookie(session_token.as_str(), request_uses_tls(&headers))?,
    );
    Ok((
        response_headers,
        Json(build_console_session_response(&state, &session, session.csrf_token.clone())),
    ))
}

pub(crate) async fn console_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ConsoleSessionResponse>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    Ok(Json(build_console_session_response(&state, &session, session.csrf_token.clone())))
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
        && existing_profile.as_ref().is_some_and(|profile| {
            matches!(profile.provider.kind, AuthProviderKind::Openai | AuthProviderKind::Anthropic)
                || (matches!(profile.provider.kind, AuthProviderKind::Custom)
                    && profile
                        .provider
                        .custom_name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case("minimax")))
        })
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

pub(crate) async fn console_auth_doctor_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuthRuntimeQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    crate::application::service_authorization::authorize_auth_profile_action(
        session.context.principal.as_str(),
        "auth.profile.doctor",
        "auth:doctor",
    )
    .map_err(runtime_status_response)?;
    let agent_id = normalize_optional_query_value(query.agent_id.as_deref(), "agent_id")?;
    let auth_runtime = Arc::clone(&state.auth_runtime);
    let vault = Arc::clone(&state.vault);
    let agent_id_for_worker = agent_id.clone();
    let records = tokio::task::spawn_blocking(move || {
        auth_runtime
            .registry()
            .runtime_records_for_agent(vault.as_ref(), agent_id_for_worker.as_deref())
            .map_err(crate::application::auth::map_auth_profile_error)
    })
    .await
    .map_err(|_| runtime_status_response(tonic::Status::internal("auth doctor worker panicked")))?
    .map_err(runtime_status_response)?;
    let warning_count = records.iter().filter(|record| record.doctor_hint.is_some()).count();
    let error_count = records
        .iter()
        .filter(|record| {
            record
                .doctor_hint
                .as_ref()
                .is_some_and(|hint| hint.severity == palyra_auth::AuthProfileDoctorSeverity::Error)
        })
        .count();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "generated_at_unix_ms": crate::gateway::current_unix_ms(),
        "status": if error_count > 0 { "error" } else if warning_count > 0 { "warning" } else { "ok" },
        "summary": {
            "profile_count": records.len(),
            "warning_count": warning_count,
            "error_count": error_count,
        },
        "profiles": records,
    })))
}

pub(crate) async fn console_auth_audit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuthRuntimeQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    crate::application::service_authorization::authorize_auth_profile_action(
        session.context.principal.as_str(),
        "auth.profile.audit",
        "auth:audit",
    )
    .map_err(runtime_status_response)?;
    let agent_id = normalize_optional_query_value(query.agent_id.as_deref(), "agent_id")?;
    let provider = parse_runtime_auth_provider(
        query.provider_kind.as_deref(),
        query.provider_custom_name.as_deref(),
    )?;
    let auth_runtime = Arc::clone(&state.auth_runtime);
    let vault = Arc::clone(&state.vault);
    let agent_id_for_worker = agent_id.clone();
    let provider_for_worker = provider.clone();
    let (records, order) = tokio::task::spawn_blocking(move || {
        let records = auth_runtime
            .registry()
            .runtime_records_for_agent_readonly(vault.as_ref(), agent_id_for_worker.as_deref())
            .map_err(crate::application::auth::map_auth_profile_error)?;
        let order = auth_runtime
            .registry()
            .profile_order(provider_for_worker.as_ref(), agent_id_for_worker.as_deref())
            .map_err(crate::application::auth::map_auth_profile_error)?;
        Ok::<_, tonic::Status>((records, order))
    })
    .await
    .map_err(|_| runtime_status_response(tonic::Status::internal("auth audit worker panicked")))?
    .map_err(runtime_status_response)?;
    let snapshot =
        state.runtime.recent_journal_snapshot(256).await.map_err(runtime_status_response)?;
    let events = snapshot
        .events
        .into_iter()
        .filter_map(|event| auth_audit_event_json(&event))
        .collect::<Vec<_>>();
    let returned_events = events.len();
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "generated_at_unix_ms": crate::gateway::current_unix_ms(),
        "hash_chain_enabled": snapshot.hash_chain_enabled,
        "total_events": snapshot.total_events,
        "runtime_records": records,
        "profile_order": order,
        "events": events,
        "page": build_page_info(256, returned_events, None),
    })))
}

pub(crate) async fn console_auth_selection_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleAuthSelectionExplainRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    crate::application::service_authorization::authorize_auth_profile_action(
        session.context.principal.as_str(),
        "auth.profile.selection.explain",
        "auth:selection",
    )
    .map_err(runtime_status_response)?;
    let provider = parse_runtime_auth_provider(
        payload.provider_kind.as_deref(),
        payload.provider_custom_name.as_deref(),
    )?;
    let allowed_credential_types = payload
        .allowed_credential_types
        .iter()
        .map(|value| parse_runtime_auth_credential_type(value.as_str()))
        .collect::<Result<Vec<_>, Response>>()?;
    let agent_id = normalize_optional_query_value(payload.agent_id.as_deref(), "agent_id")?;
    let request = palyra_auth::AuthProfileSelectionRequest {
        provider,
        agent_id,
        explicit_profile_order: payload.explicit_profile_order,
        allowed_credential_types,
        policy_denied_profile_ids: payload.policy_denied_profile_ids,
    };
    let auth_runtime = Arc::clone(&state.auth_runtime);
    let vault = Arc::clone(&state.vault);
    let result = tokio::task::spawn_blocking(move || {
        auth_runtime
            .registry()
            .select_auth_profile(vault.as_ref(), request)
            .map_err(crate::application::auth::map_auth_profile_error)
    })
    .await
    .map_err(|_| {
        runtime_status_response(tonic::Status::internal("auth selection worker panicked"))
    })?
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "selection": result,
    })))
}

pub(crate) async fn console_auth_profile_cooldown_clear_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let profile_id = normalize_non_empty_field(profile_id, "profile_id")?;
    crate::application::service_authorization::authorize_auth_profile_action(
        session.context.principal.as_str(),
        "auth.profile.cooldown.clear",
        format!("auth:profile:{profile_id}").as_str(),
    )
    .map_err(runtime_status_response)?;
    let auth_runtime = Arc::clone(&state.auth_runtime);
    let profile_id_for_worker = profile_id.clone();
    let record = tokio::task::spawn_blocking(move || {
        auth_runtime
            .registry()
            .clear_profile_cooldown(profile_id_for_worker.as_str())
            .map_err(crate::application::auth::map_auth_profile_error)
    })
    .await
    .map_err(|_| runtime_status_response(tonic::Status::internal("auth cooldown worker panicked")))?
    .map_err(runtime_status_response)?;
    crate::application::auth::record_auth_runtime_operation_journal_event(
        &state.runtime,
        &session.context,
        "auth.profile.cooldown_cleared",
        json!({ "profile_id": profile_id.clone() }),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "profile_id": profile_id,
        "runtime": record,
    })))
}

pub(crate) async fn console_auth_profile_order_set_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleAuthProfileOrderSetRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    if payload.profile_ids.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "profile_ids must include at least one profile",
        )));
    }
    let agent_id = normalize_optional_query_value(payload.agent_id.as_deref(), "agent_id")?;
    let provider = parse_runtime_auth_provider(
        payload.provider_kind.as_deref(),
        payload.provider_custom_name.as_deref(),
    )?;
    let resource = format!(
        "auth:profile-order:{}:{}",
        agent_id.as_deref().unwrap_or("global"),
        provider
            .as_ref()
            .map(palyra_auth::AuthProvider::canonical_key)
            .unwrap_or_else(|| "any".to_owned())
    );
    crate::application::service_authorization::authorize_auth_profile_action(
        session.context.principal.as_str(),
        "auth.profile.order.set",
        resource.as_str(),
    )
    .map_err(runtime_status_response)?;
    let auth_runtime = Arc::clone(&state.auth_runtime);
    let provider_for_worker = provider.clone();
    let agent_id_for_worker = agent_id.clone();
    let profile_ids_for_worker = payload.profile_ids.clone();
    let order = tokio::task::spawn_blocking(move || {
        auth_runtime
            .registry()
            .set_profile_order(
                provider_for_worker,
                agent_id_for_worker.as_deref(),
                profile_ids_for_worker,
            )
            .map_err(crate::application::auth::map_auth_profile_error)
    })
    .await
    .map_err(|_| runtime_status_response(tonic::Status::internal("auth order worker panicked")))?
    .map_err(runtime_status_response)?;
    crate::application::auth::record_auth_runtime_operation_journal_event(
        &state.runtime,
        &session.context,
        "auth.profile.order_set",
        json!({
            "scope": order.scope.clone(),
            "provider": order.provider.clone(),
            "profile_ids": order.profile_ids.clone(),
        }),
    )
    .await
    .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "order": order,
    })))
}

#[allow(clippy::result_large_err)]
fn parse_runtime_auth_provider(
    kind: Option<&str>,
    custom_name: Option<&str>,
) -> Result<Option<palyra_auth::AuthProvider>, Response> {
    let Some(kind) = kind.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let provider = match kind.to_ascii_lowercase().as_str() {
        "openai" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Openai),
        "anthropic" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Anthropic),
        "telegram" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Telegram),
        "slack" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Slack),
        "discord" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Discord),
        "webhook" => palyra_auth::AuthProvider::known(palyra_auth::AuthProviderKind::Webhook),
        "custom" => {
            let name =
                custom_name.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
                    runtime_status_response(tonic::Status::invalid_argument(
                        "provider_custom_name is required when provider_kind is custom",
                    ))
                })?;
            palyra_auth::AuthProvider {
                kind: palyra_auth::AuthProviderKind::Custom,
                custom_name: Some(name.to_owned()),
            }
        }
        _ => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "provider_kind is unsupported",
            )));
        }
    };
    Ok(Some(provider))
}

#[allow(clippy::result_large_err)]
fn parse_runtime_auth_credential_type(
    value: &str,
) -> Result<palyra_auth::AuthCredentialType, Response> {
    match value.trim().to_ascii_lowercase().as_str() {
        "api_key" | "api-key" => Ok(palyra_auth::AuthCredentialType::ApiKey),
        "oauth" | "oauth_access_token" | "oauth-access-token" => {
            Ok(palyra_auth::AuthCredentialType::Oauth)
        }
        _ => Err(runtime_status_response(tonic::Status::invalid_argument(
            "allowed_credential_types contains unsupported credential type",
        ))),
    }
}

#[allow(clippy::result_large_err)]
fn normalize_optional_query_value(
    value: Option<&str>,
    field: &'static str,
) -> Result<Option<String>, Response> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.contains(['\r', '\n', '\t']) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field} contains unsupported control characters"
        ))));
    }
    Ok(Some(value.to_owned()))
}

fn auth_audit_event_json(event: &crate::journal::JournalEventRecord) -> Option<Value> {
    if !event.payload_json.to_ascii_lowercase().contains("auth.") {
        return None;
    }
    let payload = serde_json::from_str::<Value>(event.payload_json.as_str()).unwrap_or_else(|_| {
        json!({
            "raw": crate::model_provider::sanitize_remote_error(event.payload_json.as_str()),
        })
    });
    Some(json!({
        "event_id": event.event_id,
        "kind": event.kind,
        "actor": event.actor,
        "timestamp_unix_ms": event.timestamp_unix_ms,
        "principal": event.principal,
        "channel": event.channel,
        "redacted": event.redacted,
        "payload": payload,
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

pub(crate) async fn console_anthropic_provider_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::ProviderAuthStateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let profiles = list_console_auth_profiles(
        &state,
        &session,
        gateway::proto::palyra::auth::v1::AuthProviderKind::Anthropic,
    )
    .await?;
    let configured_path = std::env::var("PALYRA_CONFIG").ok();
    let (document, _, _) = load_console_config_snapshot(configured_path.as_deref(), true)?;
    Ok(Json(build_provider_state(&document, profiles, ModelProviderAuthProviderKind::Anthropic)))
}

pub(crate) async fn console_minimax_provider_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::ProviderAuthStateEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let profiles = list_console_custom_auth_profiles(&state, &session, "minimax").await?;
    let configured_path = std::env::var("PALYRA_CONFIG").ok();
    let (document, _, _) = load_console_config_snapshot(configured_path.as_deref(), true)?;
    Ok(Json(build_provider_state(&document, profiles, ModelProviderAuthProviderKind::Minimax)))
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

pub(crate) async fn console_anthropic_provider_api_key_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderApiKeyUpsertRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match connect_anthropic_api_key(&state, &session.context, payload).await {
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

pub(crate) async fn console_minimax_provider_api_key_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderApiKeyUpsertRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match connect_minimax_api_key(&state, &session.context, payload).await {
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

pub(crate) async fn console_minimax_provider_bootstrap_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::OpenAiOAuthBootstrapRequest>,
) -> Result<Json<control_plane::OpenAiOAuthBootstrapEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match start_minimax_oauth_attempt_from_request(&state, &session.context, payload).await {
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

pub(crate) async fn console_minimax_provider_callback_state_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleOpenAiCallbackStateQuery>,
) -> Result<Json<control_plane::OpenAiOAuthCallbackStateEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    load_minimax_oauth_callback_state(&state, query.attempt_id.as_str()).await.map(Json)
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

pub(crate) async fn console_minimax_provider_reconnect_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::OpenAiOAuthBootstrapEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match reconnect_minimax_oauth_attempt(&state, &session.context, payload).await {
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

pub(crate) async fn console_minimax_provider_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match refresh_minimax_oauth_profile(&state, &session.context, payload).await {
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

pub(crate) async fn console_anthropic_provider_revoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match revoke_anthropic_auth_profile(&state, &session.context, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.api_key_revoke",
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

pub(crate) async fn console_minimax_provider_revoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    state.observability.record_provider_auth_attempt();
    let profile_id = payload.profile_id.clone();
    match revoke_minimax_auth_profile(&state, &session.context, payload).await {
        Ok(envelope) => Ok(Json(envelope)),
        Err(response) => {
            record_provider_auth_failure(
                &state,
                "provider_auth.api_key_revoke",
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

pub(crate) async fn console_anthropic_provider_default_profile_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    select_default_anthropic_auth_profile(&state, &session.context, payload).await.map(Json)
}

pub(crate) async fn console_minimax_provider_default_profile_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::ProviderAuthActionRequest>,
) -> Result<Json<control_plane::ProviderAuthActionEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    select_default_minimax_auth_profile(&state, &session.context, payload).await.map(Json)
}

pub(crate) fn issue_console_session(
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
pub(crate) fn consume_console_browser_handoff(
    state: &AppState,
    token: &str,
    now: i64,
) -> Result<ConsoleBrowserHandoff, Response> {
    let token_hash_sha256 = validate_console_browser_handoff_token(token)?;
    let mut handoffs = lock_console_browser_handoffs(&state.console_browser_handoffs);
    handoffs.retain(|_, existing| existing.expires_at_unix_ms > now);
    handoffs.remove(token_hash_sha256.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "browser handoff token is invalid or expired",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn load_console_browser_handoff(
    state: &AppState,
    token: &str,
    now: i64,
) -> Result<ConsoleBrowserHandoff, Response> {
    let token_hash_sha256 = validate_console_browser_handoff_token(token)?;
    let mut handoffs = lock_console_browser_handoffs(&state.console_browser_handoffs);
    handoffs.retain(|_, existing| existing.expires_at_unix_ms > now);
    handoffs.get(token_hash_sha256.as_str()).cloned().ok_or_else(|| {
        runtime_status_response(tonic::Status::permission_denied(
            "browser handoff token is invalid or expired",
        ))
    })
}

#[allow(clippy::result_large_err)]
fn validate_console_browser_handoff_token(token: &str) -> Result<String, Response> {
    if token.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "browser handoff token is required",
        )));
    }
    Ok(sha256_hex(token.as_bytes()))
}

#[allow(clippy::result_large_err)]
fn build_console_browser_bootstrap_redirect(
    redirect_path: &str,
    token: &str,
) -> Result<String, Response> {
    let redirect_target = format!("http://{CONSOLE_BROWSER_HANDOFF_HOST}{redirect_path}");
    let mut redirect = Url::parse(redirect_target.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "browser handoff redirect path contains unsupported characters",
        ))
    })?;
    redirect.query_pairs_mut().append_pair("desktop_handoff_token", token);
    let mut location = redirect.path().to_owned();
    if let Some(query) = redirect.query() {
        location.push('?');
        location.push_str(query);
    }
    if let Some(fragment) = redirect.fragment() {
        location.push('#');
        location.push_str(fragment);
    }
    Ok(location)
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
