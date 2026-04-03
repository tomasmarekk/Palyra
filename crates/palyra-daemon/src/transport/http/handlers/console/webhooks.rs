use crate::*;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleWebhooksListQuery {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsoleWebhookDispatchRequest {
    event: String,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    dedupe_key: Option<String>,
}

pub(crate) async fn console_webhooks_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleWebhooksListQuery>,
) -> Result<Json<control_plane::WebhookIntegrationListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let integrations = state
        .webhooks
        .list_views(
            webhooks::WebhookIntegrationListFilter {
                provider: query.provider,
                enabled: query.enabled,
            },
            state.vault.as_ref(),
        )
        .map_err(webhook_registry_error_response)?;
    let returned = integrations.len();
    Ok(Json(control_plane::WebhookIntegrationListEnvelope {
        contract: contract_descriptor(),
        integrations,
        page: build_page_info(100, returned, None),
    }))
}

pub(crate) async fn console_webhook_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration_id): Path<String>,
) -> Result<Json<control_plane::WebhookIntegrationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let integration = state
        .webhooks
        .get_view(integration_id.as_str(), state.vault.as_ref())
        .map_err(webhook_registry_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("webhook integration not found"))
        })?;
    Ok(Json(control_plane::WebhookIntegrationEnvelope {
        contract: contract_descriptor(),
        integration,
    }))
}

pub(crate) async fn console_webhook_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<control_plane::WebhookIntegrationUpsertRequest>,
) -> Result<Json<control_plane::WebhookIntegrationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let integration = state
        .webhooks
        .set_integration(
            webhooks::WebhookIntegrationSetRequest {
                integration_id: payload.integration_id,
                provider: payload.provider,
                display_name: payload.display_name,
                secret_vault_ref: payload.secret_vault_ref,
                allowed_events: payload.allowed_events,
                allowed_sources: payload.allowed_sources,
                enabled: payload.enabled.unwrap_or(true),
                signature_required: payload.signature_required.unwrap_or(true),
                max_payload_bytes: payload.max_payload_bytes.unwrap_or(64 * 1024),
            },
            state.vault.as_ref(),
        )
        .map_err(webhook_registry_error_response)?;
    Ok(Json(control_plane::WebhookIntegrationEnvelope {
        contract: contract_descriptor(),
        integration,
    }))
}

pub(crate) async fn console_webhook_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration_id): Path<String>,
    Json(payload): Json<control_plane::WebhookIntegrationEnabledRequest>,
) -> Result<Json<control_plane::WebhookIntegrationEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let integration = state
        .webhooks
        .set_enabled(integration_id.as_str(), payload.enabled, state.vault.as_ref())
        .map_err(webhook_registry_error_response)?;
    Ok(Json(control_plane::WebhookIntegrationEnvelope {
        contract: contract_descriptor(),
        integration,
    }))
}

pub(crate) async fn console_webhook_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration_id): Path<String>,
) -> Result<Json<control_plane::WebhookIntegrationDeleteEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let normalized = integration_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(validation_error_response(
            "integration_id",
            "required",
            "integration_id is required",
        ));
    }
    let deleted = state
        .webhooks
        .delete_integration(normalized.as_str())
        .map_err(webhook_registry_error_response)?;
    Ok(Json(control_plane::WebhookIntegrationDeleteEnvelope {
        contract: contract_descriptor(),
        integration_id: normalized,
        deleted,
    }))
}

pub(crate) async fn console_webhook_test_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration_id): Path<String>,
    Json(payload): Json<control_plane::WebhookIntegrationTestRequest>,
) -> Result<Json<control_plane::WebhookIntegrationTestEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let body = BASE64_STANDARD.decode(payload.payload_base64.as_bytes()).map_err(|error| {
        validation_error_response(
            "payload_base64",
            "invalid_base64",
            format!("payload_base64 must decode from base64: {error}").as_str(),
        )
    })?;
    let outcome = state
        .webhooks
        .test_integration(integration_id.as_str(), body.as_slice(), state.vault.as_ref())
        .map_err(webhook_registry_error_response)?;
    Ok(Json(control_plane::WebhookIntegrationTestEnvelope {
        contract: contract_descriptor(),
        integration: outcome.integration,
        result: outcome.result,
    }))
}

pub(crate) async fn console_webhook_dispatch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(integration_id): Path<String>,
    Json(payload): Json<ConsoleWebhookDispatchRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let integration = state
        .webhooks
        .get_view(integration_id.as_str(), state.vault.as_ref())
        .map_err(webhook_registry_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("webhook integration not found"))
        })?;
    let event = payload.event.trim();
    if event.is_empty() {
        return Err(validation_error_response(
            "event",
            "required",
            "event is required for webhook dispatch",
        ));
    }
    let dispatches = super::routines::dispatch_webhook_event_routines(
        &state,
        session.context.principal.as_str(),
        integration.integration_id.as_str(),
        integration.provider.as_str(),
        event,
        json!({
            "integration_id": integration.integration_id,
            "provider": integration.provider,
            "event": event,
            "source": payload.source,
            "payload": payload.payload.unwrap_or_else(|| json!({})),
        }),
        payload.dedupe_key,
    )
    .await?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "integration": integration,
        "dispatches": dispatches,
    })))
}

fn webhook_registry_error_response(error: webhooks::WebhookRegistryError) -> Response {
    match error {
        webhooks::WebhookRegistryError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        webhooks::WebhookRegistryError::IntegrationNotFound(integration_id) => {
            runtime_status_response(tonic::Status::not_found(format!(
                "webhook integration not found: {integration_id}"
            )))
        }
        other => runtime_status_response(tonic::Status::internal(other.to_string())),
    }
}
