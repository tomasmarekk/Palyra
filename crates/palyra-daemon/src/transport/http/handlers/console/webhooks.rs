//! Console webhook integration handlers.
//!
//! Webhook routes manage integration metadata and trigger routine dispatch
//! tests while keeping vault-backed secret material behind registry views.

use crate::*;

/// Query parameters for filtering webhook integrations.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsoleWebhooksListQuery {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

/// Request body for dispatching a webhook event into routine matching.
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

/// Lists configured webhook integrations.
///
/// # Errors
/// Returns an error response when console authorization or registry listing
/// fails.
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

/// Returns one webhook integration by id.
///
/// # Errors
/// Returns an error response when console authorization or registry lookup
/// fails.
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

/// Creates or updates a webhook integration.
///
/// # Errors
/// Returns an error response when console authorization, registry validation,
/// or persistence fails.
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

/// Enables or disables one webhook integration.
///
/// # Errors
/// Returns an error response when console authorization or registry mutation
/// fails.
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

/// Deletes one webhook integration.
///
/// # Errors
/// Returns an error response when console authorization, id validation, or
/// registry deletion fails.
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

/// Sends a test payload through one webhook integration.
///
/// # Errors
/// Returns an error response when console authorization, payload decoding, or
/// test delivery fails.
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

/// Dispatches a webhook event into matching routines.
///
/// # Errors
/// Returns an error response when console authorization, registry lookup,
/// event validation, or routine dispatch fails.
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
    let wake_identity = payload.dedupe_key.as_deref().unwrap_or(event);
    let wake_digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
        format!(
            "{}:{}:{}:{}",
            integration.integration_id, integration.provider, event, wake_identity
        )
        .as_bytes(),
    ));
    crate::application::wake_coordinator::emit_wake_event(
        &state.runtime,
        crate::journal::wait_coordinator::WakeEventRequest {
            source_event_id: format!("wake:webhook:{}", &wake_digest[..48]),
            source_kind: crate::journal::wait_coordinator::WaitBarrierKind::Webhook
                .as_str()
                .to_owned(),
            source_id: integration.integration_id.clone(),
            source_generation: 1,
            reason_code: "wake.webhook.accepted".to_owned(),
            evidence_json: json!({
                "schema_version": 1,
                "integration_id": integration.integration_id,
                "provider": integration.provider,
                "event_sha256": hex::encode(
                    <sha2::Sha256 as sha2::Digest>::digest(event.as_bytes())
                ),
                "dedupe_sha256": wake_digest,
            })
            .to_string(),
            occurred_at_unix_ms: crate::gateway::current_unix_ms(),
        },
    )
    .await
    .map_err(runtime_status_response)?;
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
