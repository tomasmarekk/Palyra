use std::env;

use super::*;

const OPENAI_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const OPENAI_DEFAULT_CONFIG_BACKUPS: usize = 5;
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

#[allow(clippy::result_large_err)]
pub(crate) async fn connect_openai_api_key(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::OpenAiApiKeyUpsertRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_name = normalize_openai_profile_name(payload.profile_name.as_str())?;
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_openai_profile_id(profile_name.as_str()));
    let scope = normalize_openai_profile_scope(Some(payload.scope))?;
    let api_key = normalize_required_openai_text(payload.api_key.as_str(), "api_key")?;
    let (document, _, _) = load_console_config_snapshot(None, true)?;
    let validation_base_url = load_openai_validation_base_url(Some(&document));
    validate_openai_bearer_token(
        validation_base_url.as_str(),
        api_key.as_str(),
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    .map_err(|error| map_openai_validation_error("api_key", error))?;

    let api_key_vault_ref = store_openai_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "api_key",
        api_key.as_bytes(),
    )?;
    let profile = control_plane::AuthProfileView {
        profile_id: profile_id.clone(),
        provider: control_plane::AuthProfileProvider {
            kind: "openai".to_owned(),
            custom_name: None,
        },
        profile_name,
        scope: scope.clone(),
        credential: control_plane::AuthCredentialView::ApiKey { api_key_vault_ref },
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    persist_openai_auth_profile(state, context, profile).await?;
    if payload.set_default {
        persist_model_provider_auth_profile_selection(state, context, profile_id.as_str()).await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "OpenAI API key profile saved and selected as the default auth profile.")
    } else {
        ("saved", "OpenAI API key profile saved.")
    };
    Ok(openai_provider_action_envelope("api_key", state_name, message, Some(profile_id)))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn start_openai_oauth_attempt_from_request(
    state: &AppState,
    context: &RequestContext,
    headers: &HeaderMap,
    payload: control_plane::OpenAiOAuthBootstrapRequest,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let profile_name = payload
        .profile_name
        .as_deref()
        .map(normalize_openai_profile_name)
        .transpose()?
        .unwrap_or_else(|| "OpenAI".to_owned());
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_openai_profile_id(profile_name.as_str()));
    let scope = normalize_openai_profile_scope(payload.scope)?;
    let client_id = normalize_required_openai_text(
        payload.client_id.as_deref().unwrap_or_default(),
        "client_id",
    )?;
    let client_secret = payload
        .client_secret
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_default();
    let scopes = normalize_scopes(&payload.scopes);
    start_openai_oauth_attempt(
        state,
        context,
        headers,
        profile_id,
        profile_name,
        scope,
        client_id,
        client_secret,
        scopes,
        payload.set_default,
    )
}

#[allow(clippy::result_large_err)]
pub(crate) async fn load_openai_oauth_callback_state(
    state: &AppState,
    attempt_id: &str,
) -> Result<control_plane::OpenAiOAuthCallbackStateEnvelope, Response> {
    let attempt_id = normalize_openai_identifier(attempt_id, "attempt_id")?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let mut attempts = lock_openai_oauth_attempts(state)?;
    cleanup_openai_oauth_attempts(&mut attempts, now);
    let attempt = attempts.get_mut(attempt_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "OpenAI OAuth attempt not found: {attempt_id}"
        )))
    })?;
    if matches!(attempt.state, OpenAiOAuthAttemptStateRecord::Pending { .. })
        && attempt.expires_at_unix_ms <= now
    {
        attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
            message: "OpenAI OAuth attempt expired before the callback completed.".to_owned(),
            completed_at_unix_ms: now,
        };
    }
    Ok(openai_oauth_callback_state_envelope(attempt))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn complete_openai_oauth_callback(
    state: &AppState,
    query: ConsoleOpenAiCallbackQuery,
) -> Result<String, Response> {
    let attempt_id = normalize_openai_identifier(query.state.as_str(), "state")?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let attempt = {
        let mut attempts = lock_openai_oauth_attempts(state)?;
        cleanup_openai_oauth_attempts(&mut attempts, now);
        let attempt = attempts.get_mut(attempt_id.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "OpenAI OAuth attempt not found: {attempt_id}"
            )))
        })?;
        if let Some((title, body)) = callback_terminal_page(attempt, now) {
            let payload = callback_payload_json(attempt, Some(body.as_str()), None);
            return Ok(render_callback_page(title.as_str(), body.as_str(), Some(payload.as_str())));
        }
        attempt.clone()
    };

    if let Some(error) = query.error.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        let description = query
            .error_description
            .as_deref()
            .map(sanitize_http_error_message)
            .unwrap_or_else(|| "OpenAI OAuth authorization was denied.".to_owned());
        return fail_openai_oauth_attempt(
            state,
            attempt_id.as_str(),
            format!("{error}: {description}"),
            now,
        );
    }

    if attempt.expires_at_unix_ms <= now {
        return fail_openai_oauth_attempt(
            state,
            attempt_id.as_str(),
            "OpenAI OAuth attempt expired before the callback completed.".to_owned(),
            now,
        );
    }

    let code = query
        .code
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| validation_error_response("code", "required", "code is required"))?;
    let token_result = match exchange_authorization_code(
        attempt.token_endpoint.as_str(),
        attempt.redirect_uri.as_str(),
        attempt.client_id.as_str(),
        attempt.client_secret.as_str(),
        attempt.code_verifier.as_str(),
        code,
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return fail_openai_oauth_attempt(
                state,
                attempt_id.as_str(),
                sanitize_http_error_message(error.to_string().as_str()),
                now,
            );
        }
    };

    let (document, _, _) = load_console_config_snapshot(None, true)?;
    let validation_base_url = load_openai_validation_base_url(Some(&document));
    if let Err(error) = validate_openai_bearer_token(
        validation_base_url.as_str(),
        token_result.access_token.as_str(),
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    {
        return fail_openai_oauth_attempt(
            state,
            attempt_id.as_str(),
            callback_validation_failure_message(error),
            now,
        );
    }

    let access_token_vault_ref = store_openai_secret(
        state.vault.as_ref(),
        &attempt.scope,
        attempt.profile_id.as_str(),
        "oauth_access_token",
        token_result.access_token.as_bytes(),
    )?;
    let refresh_token_vault_ref = store_openai_secret(
        state.vault.as_ref(),
        &attempt.scope,
        attempt.profile_id.as_str(),
        "oauth_refresh_token",
        token_result.refresh_token.as_bytes(),
    )?;
    let client_secret_vault_ref = if attempt.client_secret.trim().is_empty() {
        None
    } else {
        Some(store_openai_secret(
            state.vault.as_ref(),
            &attempt.scope,
            attempt.profile_id.as_str(),
            "oauth_client_secret",
            attempt.client_secret.as_bytes(),
        )?)
    };
    let expires_at_unix_ms = token_result
        .expires_in_seconds
        .map(|seconds| now.saturating_add((seconds as i64).saturating_mul(1_000)));
    let refresh_state = serde_json::to_value(OAuthRefreshState::default()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize OAuth refresh state: {error}"
        )))
    })?;
    let profile = control_plane::AuthProfileView {
        profile_id: attempt.profile_id.clone(),
        provider: control_plane::AuthProfileProvider {
            kind: "openai".to_owned(),
            custom_name: None,
        },
        profile_name: attempt.profile_name.clone(),
        scope: attempt.scope.clone(),
        credential: control_plane::AuthCredentialView::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint: attempt.token_endpoint.clone(),
            client_id: Some(attempt.client_id.clone()),
            client_secret_vault_ref,
            scopes: attempt.scopes.clone(),
            expires_at_unix_ms,
            refresh_state,
        },
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    let context = request_context_from_console_action(&attempt.context);
    persist_openai_auth_profile(state, &context, profile).await?;
    if attempt.set_default {
        persist_model_provider_auth_profile_selection(state, &context, attempt.profile_id.as_str())
            .await?;
    }

    let message = if attempt.set_default {
        "OpenAI OAuth profile connected and selected as the default auth profile."
    } else {
        "OpenAI OAuth profile connected."
    };
    let payload_json = {
        let mut attempts = lock_openai_oauth_attempts(state)?;
        if let Some(stored) = attempts.get_mut(attempt_id.as_str()) {
            stored.state = OpenAiOAuthAttemptStateRecord::Succeeded {
                profile_id: attempt.profile_id.clone(),
                message: message.to_owned(),
                completed_at_unix_ms: now,
            };
            callback_payload_json(stored, None, Some(now))
        } else {
            json!({
                "type": OPENAI_OAUTH_CALLBACK_EVENT_TYPE,
                "attempt_id": attempt_id,
                "state": "succeeded",
                "message": message,
                "profile_id": attempt.profile_id,
                "completed_at_unix_ms": now,
            })
            .to_string()
        }
    };
    Ok(render_callback_page("OpenAI Connected", message, Some(payload_json.as_str())))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn reconnect_openai_oauth_attempt(
    state: &AppState,
    context: &RequestContext,
    headers: &HeaderMap,
    payload: control_plane::ProviderAuthActionRequest,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let profile_id = payload
        .profile_id
        .as_deref()
        .ok_or_else(|| {
            validation_error_response(
                "profile_id",
                "required",
                "profile_id is required for provider reconnect",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let profile = load_openai_auth_profile_record(state, profile_id.as_str())?;
    let (client_id, client_secret, scopes) = match &profile.credential {
        AuthCredential::Oauth { client_id, client_secret_vault_ref, scopes, .. } => {
            let client_id = client_id.as_deref().ok_or_else(|| {
                runtime_status_response(tonic::Status::failed_precondition(
                    "OpenAI OAuth reconnect requires a stored client_id",
                ))
            })?;
            let client_secret = client_secret_vault_ref
                .as_deref()
                .map(|vault_ref| {
                    load_vault_secret_utf8(state.vault.as_ref(), vault_ref, "client_secret")
                })
                .transpose()?
                .unwrap_or_default();
            (client_id.to_owned(), client_secret, scopes.clone())
        }
        AuthCredential::ApiKey { .. } => {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "profile is not an OpenAI OAuth auth profile",
            )))
        }
    };
    start_openai_oauth_attempt(
        state,
        context,
        headers,
        profile.profile_id.clone(),
        profile.profile_name.clone(),
        auth_scope_to_control_plane(&profile.scope),
        client_id,
        client_secret,
        scopes,
        false,
    )
}

#[allow(clippy::result_large_err)]
pub(crate) async fn refresh_openai_oauth_profile(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderAuthActionRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_id = payload
        .profile_id
        .as_deref()
        .ok_or_else(|| {
            validation_error_response(
                "profile_id",
                "required",
                "profile_id is required for provider refresh",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let outcome = state
        .auth_runtime
        .refresh_oauth_profile(profile_id.clone(), Arc::clone(&state.vault))
        .await
        .map_err(runtime_status_response)?;
    record_auth_refresh_journal_event(&state.runtime, context, &outcome)
        .await
        .map_err(runtime_status_response)?;
    let (state_name, message) = match outcome.kind {
        OAuthRefreshOutcomeKind::Succeeded => ("refreshed", outcome.reason.as_str()),
        OAuthRefreshOutcomeKind::SkippedNotDue => ("not_due", outcome.reason.as_str()),
        OAuthRefreshOutcomeKind::SkippedCooldown => ("cooldown", outcome.reason.as_str()),
        OAuthRefreshOutcomeKind::SkippedNotOauth => ("not_oauth", outcome.reason.as_str()),
        OAuthRefreshOutcomeKind::Failed => ("failed", outcome.reason.as_str()),
    };
    Ok(openai_provider_action_envelope(
        "refresh",
        state_name,
        sanitize_http_error_message(message).as_str(),
        Some(outcome.profile_id),
    ))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn revoke_openai_auth_profile(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderAuthActionRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_id = payload
        .profile_id
        .as_deref()
        .ok_or_else(|| {
            validation_error_response(
                "profile_id",
                "required",
                "profile_id is required for provider revoke",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let profile = load_openai_auth_profile_record(state, profile_id.as_str())?;
    let mut remote_revocation = false;
    let message = match &profile.credential {
        AuthCredential::Oauth {
            refresh_token_vault_ref,
            client_id,
            client_secret_vault_ref,
            ..
        } => {
            let client_id = client_id.as_deref().ok_or_else(|| {
                runtime_status_response(tonic::Status::failed_precondition(
                    "OpenAI OAuth revoke requires a stored client_id",
                ))
            })?;
            let refresh_token = load_vault_secret_utf8(
                state.vault.as_ref(),
                refresh_token_vault_ref.as_str(),
                "refresh_token",
            )?;
            let client_secret = client_secret_vault_ref
                .as_deref()
                .map(|vault_ref| load_vault_secret_utf8(state.vault.as_ref(), vault_ref, "client_secret"))
                .transpose()?
                .unwrap_or_default();
            let revocation_endpoint = oauth_endpoint_config_from_env().revocation_endpoint;
            revoke_openai_token(
                revocation_endpoint.as_str(),
                client_id,
                client_secret.as_str(),
                refresh_token.as_str(),
                OPENAI_HTTP_TIMEOUT,
            )
            .await
            .map_err(|error| {
                runtime_status_response(tonic::Status::unavailable(format!(
                    "OpenAI OAuth revocation failed: {}",
                    sanitize_http_error_message(error.to_string().as_str())
                )))
            })?;
            remote_revocation = true;
            "OpenAI OAuth profile revoked.".to_owned()
        }
        AuthCredential::ApiKey { .. } => {
            "OpenAI API key profile removed locally. Revoke the API key in the OpenAI console if provider-side invalidation is required.".to_owned()
        }
    };
    let deleted =
        delete_auth_profile_via_console_service(state, context, profile_id.as_str()).await?;
    if !deleted {
        return Ok(openai_provider_action_envelope(
            "revoke",
            "not_found",
            "OpenAI auth profile no longer exists.",
            Some(profile_id),
        ));
    }
    let default_cleared =
        clear_model_provider_auth_profile_selection_if_matches(state, context, profile_id.as_str())
            .await?;
    append_console_auth_journal_event(
        state,
        context,
        json!({
            "event": "auth.profile.revoked",
            "profile_id": profile_id,
            "provider": "openai",
            "credential_type": match profile.credential.credential_type() {
                palyra_auth::AuthCredentialType::ApiKey => "api_key",
                palyra_auth::AuthCredentialType::Oauth => "oauth",
            },
            "remote_revocation": remote_revocation,
            "default_cleared": default_cleared,
        }),
    )
    .await?;
    Ok(openai_provider_action_envelope("revoke", "revoked", message.as_str(), Some(profile_id)))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn select_default_openai_auth_profile(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderAuthActionRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_id = payload
        .profile_id
        .as_deref()
        .ok_or_else(|| {
            validation_error_response(
                "profile_id",
                "required",
                "profile_id is required for default profile selection",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let _profile = load_openai_auth_profile_record(state, profile_id.as_str())?;
    persist_model_provider_auth_profile_selection(state, context, profile_id.as_str()).await?;
    Ok(openai_provider_action_envelope(
        "default_profile",
        "selected",
        "OpenAI default auth profile updated.",
        Some(profile_id),
    ))
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::result_large_err)]
fn start_openai_oauth_attempt(
    state: &AppState,
    context: &RequestContext,
    headers: &HeaderMap,
    profile_id: String,
    profile_name: String,
    scope: control_plane::AuthProfileScope,
    client_id: String,
    client_secret: String,
    scopes: Vec<String>,
    set_default: bool,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let expires_at_unix_ms = now.saturating_add(OPENAI_OAUTH_ATTEMPT_TTL_MS);
    let attempt_id = Ulid::new().to_string().to_ascii_lowercase();
    let endpoint_config = oauth_endpoint_config_from_env();
    let redirect_uri = build_openai_oauth_callback_url(headers)?;
    let code_verifier = generate_pkce_verifier();
    let code_challenge = pkce_challenge(code_verifier.as_str());
    let authorization_url = build_authorization_url(
        endpoint_config.authorization_endpoint.as_str(),
        client_id.as_str(),
        redirect_uri.as_str(),
        &scopes,
        code_challenge.as_str(),
        attempt_id.as_str(),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to build OpenAI OAuth authorization URL: {error}"
        )))
    })?;
    let attempt = OpenAiOAuthAttempt {
        attempt_id: attempt_id.clone(),
        expires_at_unix_ms,
        redirect_uri,
        profile_id: profile_id.clone(),
        profile_name,
        scope,
        client_id,
        client_secret,
        scopes,
        token_endpoint: endpoint_config.token_endpoint,
        code_verifier,
        set_default,
        context: ConsoleActionContext {
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        },
        state: OpenAiOAuthAttemptStateRecord::Pending {
            message: "Awaiting OpenAI OAuth callback.".to_owned(),
        },
    };
    let mut attempts = lock_openai_oauth_attempts(state)?;
    cleanup_openai_oauth_attempts(&mut attempts, now);
    attempts.insert(attempt_id.clone(), attempt);
    Ok(control_plane::OpenAiOAuthBootstrapEnvelope {
        contract: contract_descriptor(),
        provider: "openai".to_owned(),
        attempt_id,
        authorization_url,
        expires_at_unix_ms,
        profile_id: Some(profile_id),
        message: "OpenAI OAuth authorization URL issued.".to_owned(),
    })
}

fn default_openai_profile_scope() -> control_plane::AuthProfileScope {
    control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None }
}

fn request_context_from_console_action(context: &ConsoleActionContext) -> RequestContext {
    RequestContext {
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel: context.channel.clone(),
    }
}

#[allow(clippy::result_large_err)]
fn normalize_openai_identifier(raw: &str, field: &'static str) -> Result<String, Response> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(validation_error_response(field, "required", &format!("{field} is required")));
    }
    if trimmed.len() > 128 {
        return Err(validation_error_response(
            field,
            "too_long",
            &format!("{field} exceeds max length (128)"),
        ));
    }
    let normalized = trimmed.to_ascii_lowercase();
    if !normalized.chars().all(|ch| {
        ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_' || ch == '.'
    }) {
        return Err(validation_error_response(
            field,
            "invalid_format",
            &format!("{field} contains unsupported characters"),
        ));
    }
    Ok(normalized)
}

#[allow(clippy::result_large_err)]
fn normalize_openai_profile_name(raw: &str) -> Result<String, Response> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(validation_error_response(
            "profile_name",
            "required",
            "profile_name is required",
        ));
    }
    if trimmed.len() > 256 {
        return Err(validation_error_response(
            "profile_name",
            "too_long",
            "profile_name exceeds max length (256)",
        ));
    }
    Ok(trimmed.to_owned())
}

#[allow(clippy::result_large_err)]
fn normalize_required_openai_text(raw: &str, field: &'static str) -> Result<String, Response> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(validation_error_response(field, "required", &format!("{field} is required")));
    }
    Ok(trimmed.to_owned())
}

#[allow(clippy::result_large_err)]
fn normalize_openai_profile_scope(
    scope: Option<control_plane::AuthProfileScope>,
) -> Result<control_plane::AuthProfileScope, Response> {
    let scope = scope.unwrap_or_else(default_openai_profile_scope);
    match scope.kind.trim().to_ascii_lowercase().as_str() {
        "global" => {
            Ok(control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None })
        }
        "agent" => Ok(control_plane::AuthProfileScope {
            kind: "agent".to_owned(),
            agent_id: Some(normalize_openai_identifier(
                scope.agent_id.as_deref().unwrap_or_default(),
                "scope.agent_id",
            )?),
        }),
        _ => Err(validation_error_response(
            "scope.kind",
            "unsupported",
            "scope.kind must be global or agent",
        )),
    }
}

#[allow(clippy::result_large_err)]
fn vault_scope_for_openai_profile_scope(
    scope: &control_plane::AuthProfileScope,
) -> Result<VaultScope, Response> {
    match scope.kind.as_str() {
        "global" => Ok(VaultScope::Global),
        "agent" => Ok(VaultScope::Principal {
            principal_id: scope.agent_id.clone().ok_or_else(|| {
                validation_error_response(
                    "scope.agent_id",
                    "required",
                    "scope.agent_id is required",
                )
            })?,
        }),
        _ => Err(validation_error_response(
            "scope.kind",
            "unsupported",
            "scope.kind must be global or agent",
        )),
    }
}

fn openai_secret_key(profile_id: &str, suffix: &str) -> String {
    let digest = sha256_hex(profile_id.as_bytes());
    format!("auth_openai_{}_{}", &digest[..16], suffix)
}

fn openai_secret_vault_ref(scope: &VaultScope, key: &str) -> String {
    format!("{scope}/{key}")
}

#[allow(clippy::result_large_err)]
fn store_openai_secret(
    vault: &Vault,
    scope: &control_plane::AuthProfileScope,
    profile_id: &str,
    suffix: &str,
    value: &[u8],
) -> Result<String, Response> {
    let vault_scope = vault_scope_for_openai_profile_scope(scope)?;
    let key = openai_secret_key(profile_id, suffix);
    vault.put_secret(&vault_scope, key.as_str(), value).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to store OpenAI secret {}: {error}",
            openai_secret_vault_ref(&vault_scope, key.as_str())
        )))
    })?;
    Ok(openai_secret_vault_ref(&vault_scope, key.as_str()))
}

#[allow(clippy::result_large_err)]
fn load_vault_secret_utf8(vault: &Vault, vault_ref: &str, field: &str) -> Result<String, Response> {
    let vault_ref = VaultRef::parse(vault_ref).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "invalid vault ref for {field}: {error}"
        )))
    })?;
    let bytes = vault.get_secret(&vault_ref.scope, vault_ref.key.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "failed to load {field} from vault: {error}"
        )))
    })?;
    String::from_utf8(bytes).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "{field} secret is not valid UTF-8: {error}"
        )))
    })
}

fn generate_openai_profile_id(profile_name: &str) -> String {
    let slug = profile_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let base = if slug.is_empty() { "openai".to_owned() } else { slug };
    let suffix = Ulid::new().to_string().to_ascii_lowercase();
    format!("{base}-{suffix}")
}

fn load_openai_validation_base_url(document: Option<&toml::Value>) -> String {
    env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL")
        .ok()
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .or_else(|| document.and_then(openai_validation_base_url_from_document))
        .unwrap_or_else(|| OPENAI_DEFAULT_BASE_URL.to_owned())
}

fn openai_validation_base_url_from_document(document: &toml::Value) -> Option<String> {
    get_value_at_path(document, "model_provider.openai_base_url")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[allow(clippy::result_large_err)]
fn resolve_console_config_mutation_path(path: Option<&str>) -> Result<String, Response> {
    if let Some(resolved) = resolve_console_config_path(path, false)? {
        return Ok(resolved);
    }
    default_config_search_paths()
        .into_iter()
        .next()
        .map(|candidate| candidate.to_string_lossy().into_owned())
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::internal(
                "no default daemon config path is available",
            ))
        })
}

#[allow(clippy::result_large_err)]
fn ensure_console_config_parent_dir(path: &FsPath) -> Result<(), Response> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to create config parent directory {}: {error}",
                parent.display()
            )))
        })?;
    }
    Ok(())
}

fn auth_profile_error_response(error: AuthProfileError) -> Response {
    match error {
        AuthProfileError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        AuthProfileError::ProfileNotFound(profile_id) => runtime_status_response(
            tonic::Status::not_found(format!("auth profile not found: {profile_id}")),
        ),
        AuthProfileError::RegistryLimitExceeded => runtime_status_response(
            tonic::Status::resource_exhausted("auth profile registry exceeds maximum entries"),
        ),
        other => runtime_status_response(tonic::Status::internal(format!(
            "auth profile operation failed: {other}"
        ))),
    }
}

fn auth_scope_to_control_plane(scope: &AuthProfileScope) -> control_plane::AuthProfileScope {
    match scope {
        AuthProfileScope::Global => {
            control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None }
        }
        AuthProfileScope::Agent { agent_id } => control_plane::AuthProfileScope {
            kind: "agent".to_owned(),
            agent_id: Some(agent_id.clone()),
        },
    }
}

#[allow(clippy::result_large_err)]
fn load_openai_auth_profile_record(
    state: &AppState,
    profile_id: &str,
) -> Result<AuthProfileRecord, Response> {
    let record = state
        .auth_runtime
        .registry()
        .get_profile(profile_id)
        .map_err(auth_profile_error_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "auth profile not found: {profile_id}"
            )))
        })?;
    if record.provider.kind != AuthProviderKind::Openai {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "auth profile does not belong to the OpenAI provider",
        )));
    }
    Ok(record)
}

#[allow(clippy::result_large_err)]
async fn persist_openai_auth_profile(
    state: &AppState,
    context: &RequestContext,
    profile: control_plane::AuthProfileView,
) -> Result<control_plane::AuthProfileView, Response> {
    let mut request = TonicRequest::new(gateway::proto::palyra::auth::v1::SetAuthProfileRequest {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        profile: Some(control_plane_auth_profile_to_proto(&profile)?),
    });
    apply_console_request_context(
        state,
        context.principal.as_str(),
        context.device_id.as_str(),
        context.channel.as_deref(),
        request.metadata_mut(),
    )?;
    let service = build_console_auth_service(state);
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
    control_plane_auth_profile_from_proto(&profile)
}

#[allow(clippy::result_large_err)]
async fn delete_auth_profile_via_console_service(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
) -> Result<bool, Response> {
    let mut request =
        TonicRequest::new(gateway::proto::palyra::auth::v1::DeleteAuthProfileRequest {
            v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            profile_id: profile_id.to_owned(),
        });
    apply_console_request_context(
        state,
        context.principal.as_str(),
        context.device_id.as_str(),
        context.channel.as_deref(),
        request.metadata_mut(),
    )?;
    let service = build_console_auth_service(state);
    let response =
        <gateway::AuthServiceImpl as gateway::proto::palyra::auth::v1::auth_service_server::AuthService>::delete_profile(
            &service,
            request,
        )
        .await
        .map_err(runtime_status_response)?
        .into_inner();
    Ok(response.deleted)
}

#[allow(clippy::result_large_err)]
async fn append_console_auth_journal_event(
    state: &AppState,
    context: &RequestContext,
    payload: Value,
) -> Result<(), Response> {
    state
        .runtime
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn persist_model_provider_auth_profile_selection(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
) -> Result<(), Response> {
    let path = resolve_console_config_mutation_path(None)?;
    let path_ref = FsPath::new(path.as_str());
    ensure_console_config_parent_dir(path_ref)?;
    let (mut document, _) = load_console_document_for_mutation(path_ref)?;
    set_value_at_path(
        &mut document,
        "model_provider.auth_profile_id",
        toml::Value::String(profile_id.to_owned()),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to set model_provider.auth_profile_id: {error}"
        )))
    })?;
    let _ = unset_value_at_path(&mut document, "model_provider.openai_api_key");
    let _ = unset_value_at_path(&mut document, "model_provider.openai_api_key_vault_ref");
    validate_daemon_compatible_document(&document)?;
    write_document_with_backups(path_ref, &document, OPENAI_DEFAULT_CONFIG_BACKUPS).map_err(
        |error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to persist config {}: {error}",
                path_ref.display()
            )))
        },
    )?;
    append_console_auth_journal_event(
        state,
        context,
        json!({
            "event": "auth.profile.default_selected",
            "profile_id": profile_id,
            "provider": "openai",
            "source_path": path,
        }),
    )
    .await
}

#[allow(clippy::result_large_err)]
pub(crate) async fn clear_model_provider_auth_profile_selection_if_matches(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
) -> Result<bool, Response> {
    let Some(path) = resolve_console_config_path(None, false)? else {
        return Ok(false);
    };
    let selected = read_console_config_profile_id(path.as_str())?;
    if selected.as_deref() != Some(profile_id) {
        return Ok(false);
    }
    let path_ref = FsPath::new(path.as_str());
    let (mut document, _) = load_console_document_for_mutation(path_ref)?;
    let _ = unset_value_at_path(&mut document, "model_provider.auth_profile_id");
    validate_daemon_compatible_document(&document)?;
    write_document_with_backups(path_ref, &document, OPENAI_DEFAULT_CONFIG_BACKUPS).map_err(
        |error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to persist config {}: {error}",
                path_ref.display()
            )))
        },
    )?;
    append_console_auth_journal_event(
        state,
        context,
        json!({
            "event": "auth.profile.default_cleared",
            "profile_id": profile_id,
            "provider": "openai",
            "source_path": path,
        }),
    )
    .await?;
    Ok(true)
}

fn map_openai_validation_error(field: &str, error: OpenAiCredentialValidationError) -> Response {
    match error {
        OpenAiCredentialValidationError::InvalidCredential => validation_error_response(
            field,
            "invalid_credential",
            "OpenAI credential is invalid or does not have the required access.",
        ),
        OpenAiCredentialValidationError::RateLimited => runtime_status_response(
            tonic::Status::resource_exhausted("OpenAI credential validation was rate limited"),
        ),
        OpenAiCredentialValidationError::ProviderUnavailable => runtime_status_response(
            tonic::Status::unavailable("OpenAI credential validation is temporarily unavailable"),
        ),
        OpenAiCredentialValidationError::Unexpected(message) => {
            runtime_status_response(tonic::Status::internal(format!(
                "OpenAI credential validation failed: {}",
                sanitize_http_error_message(message.as_str())
            )))
        }
    }
}

#[allow(clippy::result_large_err)]
fn openai_callback_origin(headers: &HeaderMap) -> Result<String, Response> {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "request host is required for OpenAI OAuth callback URLs",
            ))
        })?;
    let scheme = if request_uses_tls(headers) { "https" } else { "http" };
    Ok(format!("{scheme}://{host}"))
}

#[allow(clippy::result_large_err)]
fn build_openai_oauth_callback_url(headers: &HeaderMap) -> Result<String, Response> {
    Ok(format!("{}/console/v1/auth/providers/openai/callback", openai_callback_origin(headers)?))
}

#[allow(clippy::result_large_err)]
fn lock_openai_oauth_attempts(
    state: &AppState,
) -> Result<std::sync::MutexGuard<'_, HashMap<String, OpenAiOAuthAttempt>>, Response> {
    state.openai_oauth_attempts.lock().map_err(|_| {
        runtime_status_response(tonic::Status::internal(
            "OpenAI OAuth attempt registry lock is poisoned",
        ))
    })
}

fn cleanup_openai_oauth_attempts(attempts: &mut HashMap<String, OpenAiOAuthAttempt>, now: i64) {
    attempts.retain(|_, attempt| match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Pending { .. } => {
            attempt.expires_at_unix_ms.saturating_add(OPENAI_OAUTH_ATTEMPT_TTL_MS) >= now
        }
        OpenAiOAuthAttemptStateRecord::Succeeded { completed_at_unix_ms, .. }
        | OpenAiOAuthAttemptStateRecord::Failed { completed_at_unix_ms, .. } => {
            completed_at_unix_ms.saturating_add(OPENAI_OAUTH_ATTEMPT_TTL_MS) >= now
        }
    });
}

fn openai_oauth_callback_state_envelope(
    attempt: &OpenAiOAuthAttempt,
) -> control_plane::OpenAiOAuthCallbackStateEnvelope {
    match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Pending { message } => {
            control_plane::OpenAiOAuthCallbackStateEnvelope {
                contract: contract_descriptor(),
                provider: "openai".to_owned(),
                attempt_id: attempt.attempt_id.clone(),
                state: "pending".to_owned(),
                message: message.clone(),
                profile_id: Some(attempt.profile_id.clone()),
                completed_at_unix_ms: None,
                expires_at_unix_ms: Some(attempt.expires_at_unix_ms),
            }
        }
        OpenAiOAuthAttemptStateRecord::Succeeded { profile_id, message, completed_at_unix_ms } => {
            control_plane::OpenAiOAuthCallbackStateEnvelope {
                contract: contract_descriptor(),
                provider: "openai".to_owned(),
                attempt_id: attempt.attempt_id.clone(),
                state: "succeeded".to_owned(),
                message: message.clone(),
                profile_id: Some(profile_id.clone()),
                completed_at_unix_ms: Some(*completed_at_unix_ms),
                expires_at_unix_ms: None,
            }
        }
        OpenAiOAuthAttemptStateRecord::Failed { message, completed_at_unix_ms } => {
            control_plane::OpenAiOAuthCallbackStateEnvelope {
                contract: contract_descriptor(),
                provider: "openai".to_owned(),
                attempt_id: attempt.attempt_id.clone(),
                state: "failed".to_owned(),
                message: message.clone(),
                profile_id: Some(attempt.profile_id.clone()),
                completed_at_unix_ms: Some(*completed_at_unix_ms),
                expires_at_unix_ms: None,
            }
        }
    }
}

fn callback_validation_failure_message(error: OpenAiCredentialValidationError) -> String {
    match error {
        OpenAiCredentialValidationError::InvalidCredential => {
            "OpenAI returned an access token that failed validation.".to_owned()
        }
        OpenAiCredentialValidationError::RateLimited => {
            "OpenAI rate limited credential validation for the OAuth callback.".to_owned()
        }
        OpenAiCredentialValidationError::ProviderUnavailable => {
            "OpenAI credential validation is temporarily unavailable.".to_owned()
        }
        OpenAiCredentialValidationError::Unexpected(message) => sanitize_http_error_message(
            format!("OpenAI credential validation failed: {message}").as_str(),
        ),
    }
}

fn callback_payload_json(
    attempt: &OpenAiOAuthAttempt,
    override_message: Option<&str>,
    override_completed_at_unix_ms: Option<i64>,
) -> String {
    let envelope = openai_oauth_callback_state_envelope(attempt);
    json!({
        "type": OPENAI_OAUTH_CALLBACK_EVENT_TYPE,
        "attempt_id": envelope.attempt_id,
        "state": envelope.state,
        "message": override_message.unwrap_or(envelope.message.as_str()),
        "profile_id": envelope.profile_id,
        "completed_at_unix_ms": override_completed_at_unix_ms.or(envelope.completed_at_unix_ms),
    })
    .to_string()
}

fn callback_terminal_page(attempt: &mut OpenAiOAuthAttempt, now: i64) -> Option<(String, String)> {
    match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Succeeded { message, .. } => {
            Some(("OpenAI Connected".to_owned(), message.clone()))
        }
        OpenAiOAuthAttemptStateRecord::Failed { message, .. } => {
            Some(("OpenAI Connection Failed".to_owned(), message.clone()))
        }
        OpenAiOAuthAttemptStateRecord::Pending { .. } if attempt.expires_at_unix_ms <= now => {
            let message = "OpenAI OAuth attempt expired before the callback completed.".to_owned();
            attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
                message: message.clone(),
                completed_at_unix_ms: now,
            };
            Some(("OpenAI Connection Failed".to_owned(), message))
        }
        OpenAiOAuthAttemptStateRecord::Pending { .. } => None,
    }
}

#[allow(clippy::result_large_err)]
fn fail_openai_oauth_attempt(
    state: &AppState,
    attempt_id: &str,
    message: String,
    completed_at_unix_ms: i64,
) -> Result<String, Response> {
    let mut attempts = lock_openai_oauth_attempts(state)?;
    let attempt = attempts.get_mut(attempt_id).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "OpenAI OAuth attempt not found: {attempt_id}"
        )))
    })?;
    attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
        message: sanitize_http_error_message(message.as_str()),
        completed_at_unix_ms,
    };
    let payload = callback_payload_json(attempt, None, Some(completed_at_unix_ms));
    let body = openai_oauth_callback_state_envelope(attempt).message;
    Ok(render_callback_page("OpenAI Connection Failed", body.as_str(), Some(payload.as_str())))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;

    #[test]
    fn normalize_openai_profile_scope_defaults_to_global() {
        let scope =
            normalize_openai_profile_scope(None).expect("missing scope should default to global");
        assert_eq!(scope.kind, "global");
        assert_eq!(scope.agent_id, None);
    }

    #[test]
    fn normalize_openai_identifier_rejects_unsupported_characters() {
        let response = normalize_openai_identifier("openai default", "profile_id")
            .expect_err("spaces should be rejected");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn generate_openai_profile_id_emits_safe_slug() {
        let profile_id = generate_openai_profile_id("OpenAI Team Default");
        assert!(profile_id.starts_with("openai-team-default-"));
        assert!(profile_id.len() > "openai-team-default-".len());
        assert!(profile_id.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_' || ch == '.'
        }));
    }

    #[test]
    fn openai_callback_state_envelope_reports_terminal_state() {
        let attempt = OpenAiOAuthAttempt {
            attempt_id: "attempt-1".to_owned(),
            expires_at_unix_ms: 10,
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            profile_id: "openai-default".to_owned(),
            profile_name: "OpenAI Default".to_owned(),
            scope: default_openai_profile_scope(),
            client_id: "client".to_owned(),
            client_secret: String::new(),
            scopes: vec!["openid".to_owned()],
            token_endpoint: "https://auth0.openai.com/oauth/token".to_owned(),
            code_verifier: "verifier".to_owned(),
            set_default: false,
            context: ConsoleActionContext {
                principal: "admin:test".to_owned(),
                device_id: "device".to_owned(),
                channel: None,
            },
            state: OpenAiOAuthAttemptStateRecord::Succeeded {
                profile_id: "openai-default".to_owned(),
                message: "connected".to_owned(),
                completed_at_unix_ms: 11,
            },
        };
        let envelope = openai_oauth_callback_state_envelope(&attempt);
        assert_eq!(envelope.state, "succeeded");
        assert_eq!(envelope.profile_id.as_deref(), Some("openai-default"));
        assert_eq!(envelope.completed_at_unix_ms, Some(11));
        assert_eq!(envelope.expires_at_unix_ms, None);
    }

    #[test]
    fn callback_terminal_page_marks_expired_pending_attempt_failed() {
        let mut attempt = OpenAiOAuthAttempt {
            attempt_id: "attempt-2".to_owned(),
            expires_at_unix_ms: 100,
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            profile_id: "openai-default".to_owned(),
            profile_name: "OpenAI Default".to_owned(),
            scope: default_openai_profile_scope(),
            client_id: "client".to_owned(),
            client_secret: String::new(),
            scopes: vec!["openid".to_owned()],
            token_endpoint: "https://auth0.openai.com/oauth/token".to_owned(),
            code_verifier: "verifier".to_owned(),
            set_default: false,
            context: ConsoleActionContext {
                principal: "admin:test".to_owned(),
                device_id: "device".to_owned(),
                channel: None,
            },
            state: OpenAiOAuthAttemptStateRecord::Pending {
                message: "Awaiting OpenAI OAuth callback.".to_owned(),
            },
        };
        let rendered = callback_terminal_page(&mut attempt, 101)
            .expect("expired pending attempt should render a terminal page");
        assert_eq!(rendered.0, "OpenAI Connection Failed");
        assert!(rendered.1.contains("expired"));
        assert!(matches!(
            attempt.state,
            OpenAiOAuthAttemptStateRecord::Failed { completed_at_unix_ms: 101, .. }
        ));
    }

    #[test]
    fn map_openai_validation_error_preserves_http_semantics() {
        assert_eq!(
            map_openai_validation_error(
                "api_key",
                OpenAiCredentialValidationError::InvalidCredential,
            )
            .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            map_openai_validation_error("api_key", OpenAiCredentialValidationError::RateLimited)
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            map_openai_validation_error(
                "api_key",
                OpenAiCredentialValidationError::ProviderUnavailable,
            )
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
