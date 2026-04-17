use crate::*;

pub(crate) async fn console_secrets_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSecretsListQuery>,
) -> Result<Json<control_plane::SecretMetadataList>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let scope = query.scope.trim().to_owned();
    if scope.is_empty() {
        return Err(validation_error_response("scope", "required", "scope is required"));
    }
    let mut request = TonicRequest::new(gateway::proto::palyra::gateway::v1::ListSecretsRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        scope: scope.clone(),
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_vault_service(&state);
    let response =
        <gateway::VaultServiceImpl as gateway::proto::palyra::gateway::v1::vault_service_server::VaultService>::list_secrets(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let secrets =
        response.secrets.iter().map(control_plane_secret_metadata_from_proto).collect::<Vec<_>>();
    Ok(Json(control_plane::SecretMetadataList {
        contract: contract_descriptor(),
        scope,
        page: build_page_info(secrets.len().max(1), secrets.len(), None),
        secrets,
    }))
}

pub(crate) async fn console_secret_metadata_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSecretMetadataQuery>,
) -> Result<Json<control_plane::SecretMetadataEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let metadata =
        secret_metadata_from_runtime(&state, &session, query.scope.trim(), query.key.trim())
            .await?;
    Ok(Json(control_plane::SecretMetadataEnvelope {
        contract: contract_descriptor(),
        secret: metadata,
    }))
}

pub(crate) async fn console_secret_set_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::SecretSetRequest>,
) -> Result<Json<control_plane::SecretMetadataEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let value = BASE64_STANDARD.decode(payload.value_base64.as_bytes()).map_err(|error| {
        validation_error_response(
            "value_base64",
            "invalid_base64",
            format!("value_base64 must decode from base64: {error}").as_str(),
        )
    })?;
    let mut request = TonicRequest::new(gateway::proto::palyra::gateway::v1::PutSecretRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        scope: payload.scope,
        key: payload.key,
        value,
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_vault_service(&state);
    let response =
        <gateway::VaultServiceImpl as gateway::proto::palyra::gateway::v1::vault_service_server::VaultService>::put_secret(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    let secret = response.secret.ok_or_else(|| {
        runtime_status_response(tonic::Status::internal(
            "vault put response did not include metadata",
        ))
    })?;
    Ok(Json(control_plane::SecretMetadataEnvelope {
        contract: contract_descriptor(),
        secret: control_plane_secret_metadata_from_proto(&secret),
    }))
}

pub(crate) async fn console_secret_reveal_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::SecretRevealRequest>,
) -> Result<Json<control_plane::SecretRevealEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    if !payload.reveal {
        return Err(validation_error_response(
            "reveal",
            "required_true",
            "reveal must be true for secret reveal requests",
        ));
    }
    let value = gateway::reveal_vault_secret_for_console(
        &state.runtime,
        &session.context,
        payload.scope.as_str(),
        payload.key.as_str(),
    )
    .await
    .map_err(runtime_status_response)?;
    let value_utf8 = String::from_utf8(value.clone()).ok().filter(|value| !value.trim().is_empty());
    Ok(Json(control_plane::SecretRevealEnvelope {
        contract: contract_descriptor(),
        scope: payload.scope,
        key: payload.key,
        value_bytes: u32::try_from(value.len()).unwrap_or(u32::MAX),
        value_base64: BASE64_STANDARD.encode(value),
        value_utf8,
    }))
}

pub(crate) async fn console_secret_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::SecretDeleteRequest>,
) -> Result<Json<control_plane::SecretMetadataEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let metadata = secret_metadata_from_runtime(
        &state,
        &session,
        payload.scope.as_str(),
        payload.key.as_str(),
    )
    .await?;
    let mut request = TonicRequest::new(gateway::proto::palyra::gateway::v1::DeleteSecretRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        scope: payload.scope,
        key: payload.key,
    });
    apply_console_rpc_context(&state, &session, request.metadata_mut())?;
    let service = build_console_vault_service(&state);
    let response =
        <gateway::VaultServiceImpl as gateway::proto::palyra::gateway::v1::vault_service_server::VaultService>::delete_secret(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    if !response.deleted {
        return Err(runtime_status_response(tonic::Status::not_found("secret did not exist")));
    }
    Ok(Json(control_plane::SecretMetadataEnvelope {
        contract: contract_descriptor(),
        secret: metadata,
    }))
}

pub(crate) async fn console_configured_secrets_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::ConfiguredSecretListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let snapshot = configured_secrets_snapshot(&state);
    Ok(Json(control_plane::ConfiguredSecretListEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: snapshot.generated_at_unix_ms,
        snapshot_generation: snapshot.snapshot_generation,
        page: build_page_info(snapshot.secrets.len().max(1), snapshot.secrets.len(), None),
        secrets: snapshot.secrets,
    }))
}

pub(crate) async fn console_configured_secret_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<control_plane::ConfiguredSecretQuery>,
) -> Result<Json<control_plane::ConfiguredSecretEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let snapshot = configured_secrets_snapshot(&state);
    let secret =
        snapshot.secrets.into_iter().find(|entry| entry.secret_id == query.secret_id).ok_or_else(
            || runtime_status_response(tonic::Status::not_found("configured secret not found")),
        )?;
    Ok(Json(control_plane::ConfiguredSecretEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms: snapshot.generated_at_unix_ms,
        snapshot_generation: snapshot.snapshot_generation,
        secret,
    }))
}

pub(crate) fn configured_secrets_snapshot(
    state: &AppState,
) -> crate::app::state::ConfiguredSecretsState {
    let snapshot =
        state.configured_secrets.lock().unwrap_or_else(|error| error.into_inner()).clone();
    let latest_plan =
        state.reload_state.lock().unwrap_or_else(|error| error.into_inner()).latest_plan.clone();
    if let Some(plan) = latest_plan {
        let secrets = snapshot
            .secrets
            .into_iter()
            .map(|mut secret| {
                if secret.status == "healthy"
                    && plan.steps.iter().any(|step| step.config_path == secret.config_path)
                {
                    secret.status = "stale".to_owned();
                    secret.last_error = Some(
                        "runtime snapshot is older than the latest reload plan for this secret"
                            .to_owned(),
                    );
                }
                secret
            })
            .collect::<Vec<_>>();
        crate::app::state::ConfiguredSecretsState { secrets, ..snapshot }
    } else {
        snapshot
    }
}
