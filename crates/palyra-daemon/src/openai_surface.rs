//! Console provider-auth handlers for OpenAI, Anthropic, MiniMax, and xAI:
//! API-key connect, OAuth flows (PKCE authorization-code and ChatGPT/Codex
//! device login for OpenAI, device user-code polling for MiniMax), refresh,
//! revoke, and default-profile selection.
//!
//! Credentials are validated against the live provider before anything is
//! persisted; raw secrets go only into the vault (profiles carry vault refs),
//! profiles into the auth registry via the in-process console `AuthService`,
//! and the default selection into the daemon config file. In-flight OAuth
//! attempts for all providers share the in-memory
//! `AppState::openai_oauth_attempts` map and expire after
//! `OPENAI_OAUTH_ATTEMPT_TTL_MS`. Builds on the primitives in `openai_auth`;
//! compiled as a crate-root submodule, so `use super::*` pulls in the lib.rs
//! imports.

use std::env;

use super::*;
use crate::openai_model_discovery::{
    discover_explicit_tool_capable_openai_compatible_model_id,
    discover_preferred_openai_chatgpt_codex_model_id,
    discover_preferred_openai_compatible_model_id,
};
use palyra_common::daemon_config_schema::FileModelProviderConfig;
use palyra_model_providers::{
    preserved_unresolved_default_chat_model_for_provider, ANTHROPIC_API_VERSION,
    OPENAI_CHATGPT_OAUTH_CLIENT_ID, OPENAI_CODEX_BACKEND_BASE_URL as OPENAI_CHATGPT_CODEX_BASE_URL,
};

const OPENAI_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const ANTHROPIC_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const ANTHROPIC_VALIDATION_RETRY_ATTEMPTS: usize = 3;
const ANTHROPIC_VALIDATION_RETRY_DELAY: Duration = Duration::from_millis(100);
const OPENAI_DEFAULT_CONFIG_BACKUPS: usize = 5;
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const OPENAI_CHATGPT_DEVICE_CODE_ENDPOINT: &str =
    "https://auth.openai.com/api/accounts/deviceauth/usercode";
const OPENAI_CHATGPT_DEVICE_TOKEN_ENDPOINT: &str =
    "https://auth.openai.com/api/accounts/deviceauth/token";
const OPENAI_CHATGPT_DEVICE_VERIFICATION_URL: &str = "https://auth.openai.com/codex/device";
const OPENAI_CHATGPT_TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const OPENAI_CHATGPT_DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";
const OPENAI_CHATGPT_DEVICE_ATTEMPT_TTL_MS: i64 = 15 * 60 * 1_000;
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const ANTHROPIC_OAUTH_ALLOWED_TOKEN_HOSTS: &[&str] =
    &["console.anthropic.com", "platform.claude.com"];
const MINIMAX_DEFAULT_BASE_URL: &str = "https://api.minimax.io/anthropic";
const MINIMAX_PROVIDER_CUSTOM_NAME: &str = "minimax";
const MINIMAX_OAUTH_DEFAULT_BASE_URL: &str = "https://api.minimax.io";
const MINIMAX_OAUTH_DEFAULT_CLIENT_ID: &str = "78257093-7e40-4613-99e0-527b14b39113";
const MINIMAX_OAUTH_DEFAULT_SCOPES: &[&str] = &["group_id", "profile", "model.completion"];
const MINIMAX_OAUTH_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:user_code";
const MINIMAX_RESOURCE_URL_ALLOWED_DOMAINS: &[&str] = &["minimax.io", "minimaxi.com"];
const XAI_DEFAULT_BASE_URL: &str = "https://api.x.ai/v1";
const XAI_MODEL_DISCOVERY_BASE_URL_ENV: &str = "PALYRA_MODEL_PROVIDER_XAI_BASE_URL";
const XAI_PROVIDER_CUSTOM_NAME: &str = "xai";
const XAI_OAUTH_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const XAI_OAUTH_ALLOWED_ROOT_HOST: &str = "x.ai";
const XAI_OAUTH_ALLOWED_TOKEN_HOST_SUFFIX: &str = ".x.ai";
const GOOGLE_GEMINI_OPENAI_BASE_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/openai";
const OPENROUTER_DEFAULT_BASE_URL: &str = "https://openrouter.ai/api/v1";
const OPENAI_OAUTH_CALLBACK_PATH: &str = "console/v1/auth/providers/openai/callback";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenAiProviderSelectionRuntime {
    OpenAiCompatible,
    ChatGptCodex,
}

impl OpenAiProviderSelectionRuntime {
    fn from_profile(profile: &AuthProfileRecord) -> Self {
        match &profile.credential {
            AuthCredential::Oauth { client_id: Some(client_id), .. } => {
                Self::from_client_id(client_id)
            }
            AuthCredential::Oauth { client_id: None, .. } | AuthCredential::ApiKey { .. } => {
                Self::OpenAiCompatible
            }
        }
    }

    fn from_client_id(client_id: &str) -> Self {
        if client_id == OPENAI_CHATGPT_OAUTH_CLIENT_ID {
            Self::ChatGptCodex
        } else {
            Self::OpenAiCompatible
        }
    }
}

/// Validates an OpenAI API key against the provider's models endpoint, then
/// stores it as an auth profile (key in the vault, profile via the console
/// auth service), optionally selecting it as the default provider profile.
///
/// # Errors
/// Returns a validation error response for malformed fields or a rejected
/// key, and an internal/unavailable error response when validation transport,
/// vault storage, profile persistence, or config persistence fails.
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
    let (document, _, _) = load_openai_console_config_snapshot()?;
    let validation_base_url = load_openai_validation_base_url(Some(&document));
    validate_openai_bearer_token(
        validation_base_url.as_str(),
        api_key.as_str(),
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    .map_err(|error| map_openai_validation_error("api_key", error))?;
    let discovered_model_id = discover_explicit_tool_capable_openai_compatible_model_id(
        validation_base_url.as_str(),
        api_key.as_str(),
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    .ok()
    .flatten();

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
        persist_model_provider_auth_profile_selection_with_openai_runtime(
            state,
            context,
            profile_id.as_str(),
            ModelProviderAuthProviderKind::Openai,
            OpenAiProviderSelectionRuntime::OpenAiCompatible,
            discovered_model_id.as_deref(),
        )
        .await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "OpenAI API key profile saved and selected as the default auth profile.")
    } else {
        ("saved", "OpenAI API key profile saved.")
    };
    Ok(openai_provider_action_envelope("api_key", state_name, message, Some(profile_id)))
}

/// Validates an Anthropic API key against the provider's models endpoint,
/// then stores it as an auth profile, optionally selecting it as the default
/// provider profile.
///
/// # Errors
/// Returns a validation error response for malformed fields or a rejected
/// key, and an internal/unavailable error response when validation transport,
/// vault storage, profile persistence, or config persistence fails.
#[allow(clippy::result_large_err)]
pub(crate) async fn connect_anthropic_api_key(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderApiKeyUpsertRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_name = normalize_openai_profile_name(payload.profile_name.as_str())?;
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_provider_profile_id("anthropic", profile_name.as_str()));
    let scope = normalize_openai_profile_scope(Some(payload.scope))?;
    let api_key = normalize_required_openai_text(payload.api_key.as_str(), "api_key")?;
    let (document, _, _) = load_openai_console_config_snapshot()?;
    let validation_base_url = load_anthropic_validation_base_url(Some(&document));
    validate_anthropic_api_key(
        validation_base_url.as_str(),
        api_key.as_str(),
        ANTHROPIC_HTTP_TIMEOUT,
    )
    .await
    .map_err(|error| map_anthropic_validation_error("api_key", error))?;

    let api_key_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "anthropic",
        "api_key",
        api_key.as_bytes(),
    )?;
    let profile = control_plane::AuthProfileView {
        profile_id: profile_id.clone(),
        provider: control_plane::AuthProfileProvider {
            kind: "anthropic".to_owned(),
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
        persist_model_provider_auth_profile_selection(
            state,
            context,
            profile_id.as_str(),
            ModelProviderAuthProviderKind::Anthropic,
        )
        .await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "Anthropic API key profile saved and selected as the default auth profile.")
    } else {
        ("saved", "Anthropic API key profile saved.")
    };
    Ok(provider_action_envelope("anthropic", "api_key", state_name, message, Some(profile_id)))
}

/// Stores Anthropic OAuth tokens as a vault-backed auth profile, optionally
/// selecting it as the default model-provider profile.
///
/// # Errors
/// Returns a validation error response for malformed fields or an unsupported
/// token endpoint, and an internal/unavailable error response when vault,
/// profile, or config persistence fails.
#[allow(clippy::result_large_err)]
pub(crate) async fn connect_anthropic_oauth_tokens(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderOAuthTokenUpsertRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_name = normalize_openai_profile_name(payload.profile_name.as_str())?;
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_provider_profile_id("anthropic", profile_name.as_str()));
    let scope = normalize_openai_profile_scope(Some(payload.scope))?;
    let access_token =
        normalize_required_openai_text(payload.access_token.as_str(), "access_token")?;
    let refresh_token =
        normalize_required_openai_text(payload.refresh_token.as_str(), "refresh_token")?;
    let token_endpoint = normalize_anthropic_oauth_token_endpoint(payload.token_endpoint.as_str())?;
    let client_id = payload
        .client_id
        .as_deref()
        .and_then(normalize_optional_text)
        .map(str::to_owned)
        .unwrap_or_else(|| ANTHROPIC_OAUTH_CLIENT_ID.to_owned());
    let access_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "anthropic",
        "oauth_access_token",
        access_token.as_bytes(),
    )?;
    let refresh_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "anthropic",
        "oauth_refresh_token",
        refresh_token.as_bytes(),
    )?;
    let refresh_state = serde_json::to_value(OAuthRefreshState::default()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize OAuth refresh state: {error}"
        )))
    })?;
    let profile = control_plane::AuthProfileView {
        profile_id: profile_id.clone(),
        provider: control_plane::AuthProfileProvider {
            kind: "anthropic".to_owned(),
            custom_name: None,
        },
        profile_name,
        scope: scope.clone(),
        credential: control_plane::AuthCredentialView::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id: Some(client_id),
            client_secret_vault_ref: None,
            scopes: Vec::new(),
            expires_at_unix_ms: payload.expires_at_unix_ms,
            refresh_state,
        },
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    persist_openai_auth_profile(state, context, profile).await?;
    if payload.set_default {
        persist_model_provider_auth_profile_selection(
            state,
            context,
            profile_id.as_str(),
            ModelProviderAuthProviderKind::Anthropic,
        )
        .await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "Anthropic OAuth profile saved and selected as the default auth profile.")
    } else {
        ("saved", "Anthropic OAuth profile saved.")
    };
    Ok(provider_action_envelope("anthropic", "oauth", state_name, message, Some(profile_id)))
}

/// Stores xAI OAuth tokens as a vault-backed auth profile, optionally
/// selecting it as the default OpenAI-compatible model-provider profile.
///
/// # Errors
/// Returns a validation error response for malformed fields or an unsupported
/// token endpoint, and an internal/unavailable error response when vault,
/// profile, or config persistence fails.
#[allow(clippy::result_large_err)]
pub(crate) async fn connect_xai_oauth_tokens(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderOAuthTokenUpsertRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_name = normalize_openai_profile_name(payload.profile_name.as_str())?;
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_provider_profile_id("xai", profile_name.as_str()));
    let scope = normalize_openai_profile_scope(Some(payload.scope))?;
    let access_token =
        normalize_required_openai_text(payload.access_token.as_str(), "access_token")?;
    let refresh_token =
        normalize_required_openai_text(payload.refresh_token.as_str(), "refresh_token")?;
    let token_endpoint = normalize_xai_oauth_token_endpoint(payload.token_endpoint.as_str())?;
    let scopes = normalize_provider_oauth_scopes(payload.scopes.as_slice());
    let discovery_base_url = xai_model_discovery_base_url();
    let discovered_model_id = discover_preferred_openai_compatible_model_id(
        discovery_base_url.as_str(),
        access_token.as_str(),
        OPENAI_HTTP_TIMEOUT,
    )
    .await
    .ok()
    .flatten();
    let client_id = payload
        .client_id
        .as_deref()
        .and_then(normalize_optional_text)
        .map(str::to_owned)
        .unwrap_or_else(|| XAI_OAUTH_CLIENT_ID.to_owned());
    let access_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "xai",
        "oauth_access_token",
        access_token.as_bytes(),
    )?;
    let refresh_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "xai",
        "oauth_refresh_token",
        refresh_token.as_bytes(),
    )?;
    let refresh_state = serde_json::to_value(OAuthRefreshState::default()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize OAuth refresh state: {error}"
        )))
    })?;
    let profile = control_plane::AuthProfileView {
        profile_id: profile_id.clone(),
        provider: xai_auth_profile_provider(),
        profile_name,
        scope: scope.clone(),
        credential: control_plane::AuthCredentialView::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id: Some(client_id),
            client_secret_vault_ref: None,
            scopes,
            expires_at_unix_ms: payload.expires_at_unix_ms,
            refresh_state,
        },
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    persist_openai_auth_profile(state, context, profile).await?;
    if payload.set_default {
        persist_model_provider_auth_profile_selection_with_openai_runtime(
            state,
            context,
            profile_id.as_str(),
            ModelProviderAuthProviderKind::Xai,
            OpenAiProviderSelectionRuntime::OpenAiCompatible,
            discovered_model_id.as_deref(),
        )
        .await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "xAI OAuth profile saved and selected as the default auth profile.")
    } else {
        ("saved", "xAI OAuth profile saved.")
    };
    Ok(provider_action_envelope("xai", "oauth", state_name, message, Some(profile_id)))
}

/// Validates a MiniMax API key with a minimal completion probe, then stores
/// it as an auth profile (MiniMax profiles use the custom-provider kind),
/// optionally selecting it as the default provider profile.
///
/// # Errors
/// Returns a validation error response for malformed fields or a rejected
/// key, and an internal/unavailable error response when validation transport,
/// vault storage, profile persistence, or config persistence fails.
#[allow(clippy::result_large_err)]
pub(crate) async fn connect_minimax_api_key(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::ProviderApiKeyUpsertRequest,
) -> Result<control_plane::ProviderAuthActionEnvelope, Response> {
    let profile_name = normalize_openai_profile_name(payload.profile_name.as_str())?;
    let profile_id = payload
        .profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_provider_profile_id("minimax", profile_name.as_str()));
    let scope = normalize_openai_profile_scope(Some(payload.scope))?;
    let api_key = normalize_required_openai_text(payload.api_key.as_str(), "api_key")?;
    let validation_base_url = load_minimax_validation_base_url(None);
    let discovered_model_id = discover_minimax_api_key_model_id(
        validation_base_url.as_str(),
        api_key.as_str(),
        ANTHROPIC_HTTP_TIMEOUT,
    )
    .await
    .map_err(|error| map_minimax_validation_error("api_key", error))?;

    let api_key_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &scope,
        profile_id.as_str(),
        "minimax",
        "api_key",
        api_key.as_bytes(),
    )?;
    let profile = control_plane::AuthProfileView {
        profile_id: profile_id.clone(),
        provider: minimax_auth_profile_provider(),
        profile_name,
        scope: scope.clone(),
        credential: control_plane::AuthCredentialView::ApiKey { api_key_vault_ref },
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
    };
    persist_openai_auth_profile(state, context, profile).await?;
    if payload.set_default {
        persist_model_provider_auth_profile_selection_with_discovered_model(
            state,
            context,
            profile_id.as_str(),
            ModelProviderAuthProviderKind::Minimax,
            Some(discovered_model_id.as_str()),
        )
        .await?;
    }

    let (state_name, message) = if payload.set_default {
        ("selected", "MiniMax API key profile saved and selected as the default auth profile.")
    } else {
        ("saved", "MiniMax API key profile saved.")
    };
    Ok(provider_action_envelope("minimax", "api_key", state_name, message, Some(profile_id)))
}

/// Normalizes an OAuth bootstrap request and starts a PKCE authorization-code
/// attempt for OpenAI, returning the authorization URL the user must open.
///
/// # Errors
/// Returns a validation error response for malformed identifiers and an
/// unavailable/internal error response when either OAuth bootstrap path cannot
/// be reached or configured.
#[allow(clippy::result_large_err)]
pub(crate) async fn start_openai_oauth_attempt_from_request(
    state: &AppState,
    context: &RequestContext,
    headers: &HeaderMap,
    payload: control_plane::OpenAiOAuthBootstrapRequest,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let control_plane::OpenAiOAuthBootstrapRequest {
        profile_id: payload_profile_id,
        profile_name: payload_profile_name,
        scope: payload_scope,
        client_id: payload_client_id,
        client_secret: payload_client_secret,
        scopes: payload_scopes,
        set_default,
    } = payload;
    let uses_public_chatgpt_device_flow =
        payload_client_id.as_deref().and_then(normalize_optional_text).is_none();
    let profile_name = payload_profile_name
        .as_deref()
        .map(normalize_openai_profile_name)
        .transpose()?
        .unwrap_or_else(|| {
            if uses_public_chatgpt_device_flow {
                "ChatGPT Login".to_owned()
            } else {
                "OpenAI".to_owned()
            }
        });
    let profile_id = payload_profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_openai_profile_id(profile_name.as_str()));
    let scope = normalize_openai_profile_scope(payload_scope)?;
    let scopes = normalize_scopes(&payload_scopes);
    if uses_public_chatgpt_device_flow {
        return start_openai_chatgpt_device_oauth_attempt(
            state,
            context,
            profile_id,
            profile_name,
            scope,
            scopes,
            set_default,
        )
        .await;
    }
    let client_id = normalize_required_openai_text(
        payload_client_id.as_deref().unwrap_or_default(),
        "client_id",
    )?;
    let client_secret = normalize_optional_openai_secret(payload_client_secret);
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
        set_default,
    )
}

/// Normalizes an OAuth bootstrap request and starts a MiniMax device
/// user-code attempt, returning the verification URL and user code.
///
/// The client id falls back from the request to `PALYRA_MINIMAX_OAUTH_CLIENT_ID`
/// and finally to the built-in public MiniMax client id; `client_secret` is
/// ignored because the device flow uses PKCE only.
///
/// # Errors
/// Returns a validation error response for malformed identifiers and an
/// unavailable/internal error response when the MiniMax code endpoint cannot
/// be reached or the endpoint config is invalid.
#[allow(clippy::result_large_err)]
pub(crate) async fn start_minimax_oauth_attempt_from_request(
    state: &AppState,
    context: &RequestContext,
    payload: control_plane::OpenAiOAuthBootstrapRequest,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let control_plane::OpenAiOAuthBootstrapRequest {
        profile_id: payload_profile_id,
        profile_name: payload_profile_name,
        scope: payload_scope,
        client_id: payload_client_id,
        client_secret: _,
        scopes: payload_scopes,
        set_default,
    } = payload;
    let profile_name = payload_profile_name
        .as_deref()
        .map(normalize_openai_profile_name)
        .transpose()?
        .unwrap_or_else(|| "MiniMax".to_owned());
    let profile_id = payload_profile_id
        .as_deref()
        .map(|value| normalize_openai_identifier(value, "profile_id"))
        .transpose()?
        .unwrap_or_else(|| generate_provider_profile_id("minimax", profile_name.as_str()));
    let scope = normalize_openai_profile_scope(payload_scope)?;
    let client_id = payload_client_id
        .as_deref()
        .and_then(normalize_optional_text)
        .map(str::to_owned)
        .or_else(|| env::var("PALYRA_MINIMAX_OAUTH_CLIENT_ID").ok())
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
        .unwrap_or_else(|| MINIMAX_OAUTH_DEFAULT_CLIENT_ID.to_owned());
    let scopes = normalize_minimax_scopes(&payload_scopes);
    start_minimax_oauth_attempt(
        state,
        context,
        profile_id,
        profile_name,
        scope,
        client_id,
        scopes,
        set_default,
    )
    .await
}

/// Returns the current state of a MiniMax OAuth attempt, driving one poll of
/// the MiniMax token endpoint first when the attempt is pending and its poll
/// interval has elapsed (the device flow has no callback, so polling this
/// endpoint is what advances the attempt).
///
/// # Errors
/// Returns not-found for unknown or purged attempt ids, failed-precondition
/// when the attempt belongs to another provider, and propagates vault,
/// profile-persistence, and config errors from a successful poll.
#[allow(clippy::result_large_err)]
pub(crate) async fn load_minimax_oauth_callback_state(
    state: &AppState,
    attempt_id: &str,
) -> Result<control_plane::OpenAiOAuthCallbackStateEnvelope, Response> {
    let attempt_id = normalize_openai_identifier(attempt_id, "attempt_id")?;
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let attempt_to_poll = {
        let mut attempts = lock_openai_oauth_attempts(state)?;
        cleanup_openai_oauth_attempts(&mut attempts, now);
        let attempt = attempts.get_mut(attempt_id.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "MiniMax OAuth attempt not found: {attempt_id}"
            )))
        })?;
        if attempt.provider != ModelProviderAuthProviderKind::Minimax {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "OAuth attempt does not belong to MiniMax",
            )));
        }
        if matches!(attempt.state, OpenAiOAuthAttemptStateRecord::Pending { .. })
            && attempt.expires_at_unix_ms <= now
        {
            attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
                message: "MiniMax OAuth attempt expired before authorization completed.".to_owned(),
                completed_at_unix_ms: now,
            };
        }
        if matches!(attempt.state, OpenAiOAuthAttemptStateRecord::Pending { .. })
            && attempt.next_poll_after_unix_ms <= now
        {
            // Bump the next-poll deadline while still under the lock so
            // concurrent state requests cannot stampede the MiniMax token
            // endpoint with parallel polls for the same attempt.
            attempt.next_poll_after_unix_ms =
                now.saturating_add(attempt.poll_interval_ms.try_into().unwrap_or(i64::MAX));
            Some(attempt.clone())
        } else {
            None
        }
    };

    // The attempt map is behind a std Mutex, so the guard is dropped before
    // the async poll and the result is applied under a fresh lock.
    if let Some(attempt) = attempt_to_poll {
        apply_minimax_oauth_poll_result(state, attempt, now).await?;
    }

    let attempts = lock_openai_oauth_attempts(state)?;
    let attempt = attempts.get(attempt_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "MiniMax OAuth attempt not found: {attempt_id}"
        )))
    })?;
    Ok(oauth_callback_state_envelope(attempt))
}

/// Starts a fresh MiniMax device-flow attempt that reuses the client id and
/// scopes stored on an existing MiniMax OAuth profile.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition when the profile does not exist
/// or is not a MiniMax OAuth profile, and propagates attempt-start failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn reconnect_minimax_oauth_attempt(
    state: &AppState,
    context: &RequestContext,
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
    let profile = load_minimax_auth_profile_record(state, profile_id.as_str())?;
    let (client_id, scopes) = match &profile.credential {
        AuthCredential::Oauth { client_id, scopes, .. } => (
            client_id.clone().unwrap_or_else(|| MINIMAX_OAUTH_DEFAULT_CLIENT_ID.to_owned()),
            scopes.clone(),
        ),
        AuthCredential::ApiKey { .. } => {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "profile is not a MiniMax OAuth auth profile",
            )))
        }
    };
    start_minimax_oauth_attempt(
        state,
        context,
        profile.profile_id,
        profile.profile_name,
        auth_scope_to_control_plane(&profile.scope),
        client_id,
        normalize_minimax_scopes(&scopes),
        false,
    )
    .await
}

/// Triggers an OAuth token refresh for a MiniMax profile through the auth
/// runtime and journals the outcome; skip outcomes (not due, cooldown, not
/// OAuth) are reported in the envelope state rather than as errors.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-MiniMax
/// profiles, and propagates auth-runtime and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn refresh_minimax_oauth_profile(
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
    let _profile = load_minimax_auth_profile_record(state, profile_id.as_str())?;
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
    Ok(provider_action_envelope(
        "minimax",
        "refresh",
        state_name,
        sanitize_http_error_message(message).as_str(),
        Some(outcome.profile_id),
    ))
}

/// Returns the current state of an OpenAI OAuth attempt for UI polling,
/// flipping a pending attempt to failed once its deadline has passed. ChatGPT
/// device-login attempts are also polled here because they have no callback.
///
/// # Errors
/// Returns not-found for unknown or purged attempt ids and
/// failed-precondition when the attempt belongs to another provider.
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
    let attempt_to_poll = {
        let mut attempts = lock_openai_oauth_attempts(state)?;
        cleanup_openai_oauth_attempts(&mut attempts, now);
        let attempt = attempts.get_mut(attempt_id.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "OpenAI OAuth attempt not found: {attempt_id}"
            )))
        })?;
        if attempt.provider != ModelProviderAuthProviderKind::Openai {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "OAuth attempt does not belong to OpenAI",
            )));
        }
        if matches!(
            attempt.state,
            OpenAiOAuthAttemptStateRecord::Pending { .. }
                | OpenAiOAuthAttemptStateRecord::Completing { .. }
        ) && attempt.expires_at_unix_ms <= now
        {
            attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
                message: "OpenAI OAuth attempt expired before authorization completed.".to_owned(),
                completed_at_unix_ms: now,
            };
        }
        if attempt.device_auth_id.is_some()
            && matches!(attempt.state, OpenAiOAuthAttemptStateRecord::Pending { .. })
            && attempt.next_poll_after_unix_ms <= now
        {
            attempt.next_poll_after_unix_ms =
                now.saturating_add(attempt.poll_interval_ms.try_into().unwrap_or(i64::MAX));
            Some(attempt.clone())
        } else {
            None
        }
    };

    if let Some(attempt) = attempt_to_poll {
        apply_openai_chatgpt_device_poll_result(state, attempt, now).await?;
    }

    let attempts = lock_openai_oauth_attempts(state)?;
    let attempt = attempts.get(attempt_id.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(format!(
            "OpenAI OAuth attempt not found: {attempt_id}"
        )))
    })?;
    Ok(oauth_callback_state_envelope(attempt))
}

/// Handles the browser redirect of the OpenAI authorization-code flow:
/// exchanges the code (with the stored PKCE verifier), validates the issued
/// access token, stores the tokens in the vault, persists the auth profile,
/// and returns the HTML callback page for the user's browser.
///
/// Flow failures (denied authorization, expired attempt, failed exchange or
/// validation) are reported by marking the attempt failed and returning a
/// renderable failure page, not as error responses.
///
/// # Errors
/// Returns not-found for unknown or purged attempt ids, failed-precondition
/// when the attempt belongs to another provider, a validation error response
/// when `code` is absent, and internal error responses for vault, profile, or
/// config persistence failures after a successful exchange.
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
    let callback_error = query.error.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let code = query.code.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let attempt = {
        let mut attempts = lock_openai_oauth_attempts(state)?;
        cleanup_openai_oauth_attempts(&mut attempts, now);
        let attempt = attempts.get_mut(attempt_id.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "OpenAI OAuth attempt not found: {attempt_id}"
            )))
        })?;
        if attempt.provider != ModelProviderAuthProviderKind::Openai {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "OAuth attempt does not belong to OpenAI",
            )));
        }
        if let Some((title, body)) = callback_terminal_page(attempt, now) {
            let payload = callback_payload_json(attempt, Some(body.as_str()), None);
            return Ok(render_callback_page(title.as_str(), body.as_str(), Some(payload.as_str())));
        }
        if callback_error.is_none() && code.is_none() {
            return Err(validation_error_response("code", "required", "code is required"));
        }
        if callback_error.is_none() {
            attempt.state = OpenAiOAuthAttemptStateRecord::Completing {
                message: "OpenAI OAuth callback is being completed.".to_owned(),
            };
        }
        attempt.clone()
    };

    if let Some(error) = callback_error {
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

    let code = code.expect("callback code was validated before reserving the OAuth attempt");
    let token_result = match exchange_authorization_code(
        &attempt.token_endpoint,
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

    // Validate the issued token before any secret is persisted so a broken
    // grant never lands in the vault or the auth registry.
    let (document, _, _) = load_openai_console_config_snapshot()?;
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

    // `expires_in` is a short relative duration; the `as` cast cannot
    // truncate realistic values, and saturating math caps pathological ones.
    let expires_at_unix_ms = token_result
        .expires_in_seconds
        .map(|seconds| now.saturating_add((seconds as i64).saturating_mul(1_000)));
    let message = persist_openai_oauth_success(
        state,
        &attempt,
        token_result.access_token.as_str(),
        token_result.refresh_token.as_str(),
        expires_at_unix_ms,
    )
    .await?;
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
            // TTL cleanup can purge the attempt while the exchange was in
            // flight; the profile was still persisted above, so synthesize
            // the success payload for the opener window.
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
    Ok(render_callback_page("OpenAI Connected", message.as_str(), Some(payload_json.as_str())))
}

/// Starts a fresh OpenAI authorization-code attempt that reuses the client
/// id, client secret (loaded from the vault), and scopes stored on an
/// existing OpenAI OAuth profile.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition when the profile does not exist,
/// is not an OAuth profile, or lacks a stored client id, and propagates
/// vault-read and attempt-start failures.
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

/// Triggers an OAuth token refresh for an OpenAI profile through the auth
/// runtime and journals the outcome; skip outcomes (not due, cooldown, not
/// OAuth) are reported in the envelope state rather than as errors.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed and propagates auth-runtime and journaling failures.
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

/// Revokes an OpenAI auth profile: OAuth profiles have their refresh token
/// revoked at the provider first, API-key profiles are removed locally only
/// (the message tells the operator to revoke the key in the OpenAI console).
/// Deletes the profile, clears it as the configured default if selected, and
/// journals the revocation.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-OpenAI
/// profiles, unavailable when remote revocation fails, and propagates vault,
/// deletion, config, and journaling failures.
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
    // Remote revocation runs before local deletion: a failed revoke leaves
    // the profile intact for retry, while the inverse order could orphan
    // live provider tokens with no local record of them.
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
            let revocation_endpoint = oauth_endpoint_config_from_env()
                .map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to load OpenAI OAuth endpoint config: {error}"
                    )))
                })?
                .revocation_endpoint;
            revoke_openai_token(
                &revocation_endpoint,
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

/// Marks an existing OpenAI auth profile as the default model-provider
/// profile by rewriting the daemon config.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-OpenAI
/// profiles, and propagates config persistence failures.
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
    let profile = load_openai_auth_profile_record(state, profile_id.as_str())?;
    persist_model_provider_auth_profile_selection_with_openai_runtime(
        state,
        context,
        profile_id.as_str(),
        ModelProviderAuthProviderKind::Openai,
        OpenAiProviderSelectionRuntime::from_profile(&profile),
        None,
    )
    .await?;
    Ok(openai_provider_action_envelope(
        "default_profile",
        "selected",
        "OpenAI default auth profile updated.",
        Some(profile_id),
    ))
}

/// Removes an Anthropic auth profile locally (Anthropic API keys have no
/// remote revocation endpoint), clears it as the configured default if
/// selected, and journals the revocation.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-Anthropic
/// profiles, and propagates deletion, config, and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn revoke_anthropic_auth_profile(
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
                "profile_id is required for Anthropic revoke",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let profile = load_auth_profile_record_for_provider(
        state,
        profile_id.as_str(),
        AuthProviderKind::Anthropic,
    )?;
    let deleted =
        delete_auth_profile_via_console_service(state, context, profile_id.as_str()).await?;
    if !deleted {
        return Ok(provider_action_envelope(
            "anthropic",
            "revoke",
            "not_found",
            "Anthropic auth profile was already removed.",
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
            "provider": "anthropic",
            "profile_id": profile.profile_id,
            "credential_type": match profile.credential.credential_type() {
                palyra_auth::AuthCredentialType::ApiKey => "api_key",
                palyra_auth::AuthCredentialType::Oauth => "oauth",
            },
            "remote_revocation": false,
            "default_cleared": default_cleared,
        }),
    )
    .await?;
    Ok(provider_action_envelope(
        "anthropic",
        "revoke",
        "revoked",
        "Anthropic auth profile revoked.",
        Some(profile_id),
    ))
}

/// Marks an existing Anthropic auth profile as the default model-provider
/// profile by rewriting the daemon config.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-Anthropic
/// profiles, and propagates config persistence failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn select_default_anthropic_auth_profile(
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
    let _profile = load_auth_profile_record_for_provider(
        state,
        profile_id.as_str(),
        AuthProviderKind::Anthropic,
    )?;
    persist_model_provider_auth_profile_selection(
        state,
        context,
        profile_id.as_str(),
        ModelProviderAuthProviderKind::Anthropic,
    )
    .await?;
    Ok(provider_action_envelope(
        "anthropic",
        "default_profile",
        "selected",
        "Anthropic default auth profile updated.",
        Some(profile_id),
    ))
}

/// Triggers an OAuth token refresh for an Anthropic profile through the auth
/// runtime and journals the outcome; skip outcomes are returned as envelope
/// states rather than errors.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-Anthropic
/// profiles, and propagates auth-runtime and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn refresh_anthropic_oauth_profile(
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
    let _profile = load_auth_profile_record_for_provider(
        state,
        profile_id.as_str(),
        AuthProviderKind::Anthropic,
    )?;
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
    Ok(provider_action_envelope(
        "anthropic",
        "refresh",
        state_name,
        sanitize_http_error_message(message).as_str(),
        Some(outcome.profile_id),
    ))
}

/// Removes an xAI auth profile locally, clears it as the configured default
/// if selected, and journals the revocation.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-xAI profiles,
/// and propagates deletion, config, and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn revoke_xai_auth_profile(
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
                "profile_id is required for xAI revoke",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let profile = load_xai_auth_profile_record(state, profile_id.as_str())?;
    let deleted =
        delete_auth_profile_via_console_service(state, context, profile_id.as_str()).await?;
    if !deleted {
        return Ok(provider_action_envelope(
            "xai",
            "revoke",
            "not_found",
            "xAI auth profile was already removed.",
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
            "provider": "xai",
            "profile_id": profile.profile_id,
            "credential_type": match profile.credential.credential_type() {
                palyra_auth::AuthCredentialType::ApiKey => "api_key",
                palyra_auth::AuthCredentialType::Oauth => "oauth",
            },
            "remote_revocation": false,
            "default_cleared": default_cleared,
        }),
    )
    .await?;
    Ok(provider_action_envelope(
        "xai",
        "revoke",
        "revoked",
        "xAI auth profile revoked.",
        Some(profile_id),
    ))
}

/// Marks an existing xAI auth profile as the default model-provider profile.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-xAI profiles,
/// and propagates config persistence failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn select_default_xai_auth_profile(
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
    let _profile = load_xai_auth_profile_record(state, profile_id.as_str())?;
    persist_model_provider_auth_profile_selection(
        state,
        context,
        profile_id.as_str(),
        ModelProviderAuthProviderKind::Xai,
    )
    .await?;
    Ok(provider_action_envelope(
        "xai",
        "default_profile",
        "selected",
        "xAI default auth profile updated.",
        Some(profile_id),
    ))
}

/// Triggers an OAuth token refresh for an xAI profile through the auth
/// runtime and journals the outcome; skip outcomes are returned as envelope
/// states rather than errors.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-xAI profiles,
/// and propagates auth-runtime and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn refresh_xai_oauth_profile(
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
    let _profile = load_xai_auth_profile_record(state, profile_id.as_str())?;
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
    Ok(provider_action_envelope(
        "xai",
        "refresh",
        state_name,
        sanitize_http_error_message(message).as_str(),
        Some(outcome.profile_id),
    ))
}

/// Removes a MiniMax auth profile locally (no remote revocation is attempted
/// for either credential type), clears it as the configured default if
/// selected, and journals the revocation.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-MiniMax
/// profiles, and propagates deletion, config, and journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn revoke_minimax_auth_profile(
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
                "profile_id is required for MiniMax revoke",
            )
        })
        .and_then(|value| normalize_openai_identifier(value, "profile_id"))?;
    let profile = load_minimax_auth_profile_record(state, profile_id.as_str())?;
    let deleted =
        delete_auth_profile_via_console_service(state, context, profile_id.as_str()).await?;
    if !deleted {
        return Ok(provider_action_envelope(
            "minimax",
            "revoke",
            "not_found",
            "MiniMax auth profile was already removed.",
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
            "provider": "minimax",
            "profile_id": profile.profile_id,
            "credential_type": match profile.credential.credential_type() {
                palyra_auth::AuthCredentialType::ApiKey => "api_key",
                palyra_auth::AuthCredentialType::Oauth => "oauth",
            },
            "remote_revocation": false,
            "default_cleared": default_cleared,
        }),
    )
    .await?;
    Ok(provider_action_envelope(
        "minimax",
        "revoke",
        "revoked",
        "MiniMax auth profile revoked.",
        Some(profile_id),
    ))
}

/// Marks an existing MiniMax auth profile as the default model-provider
/// profile by rewriting the daemon config.
///
/// # Errors
/// Returns a validation error response when `profile_id` is missing or
/// malformed, not-found/failed-precondition for unknown or non-MiniMax
/// profiles, and propagates config persistence failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn select_default_minimax_auth_profile(
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
    let _profile = load_minimax_auth_profile_record(state, profile_id.as_str())?;
    persist_model_provider_auth_profile_selection(
        state,
        context,
        profile_id.as_str(),
        ModelProviderAuthProviderKind::Minimax,
    )
    .await?;
    Ok(provider_action_envelope(
        "minimax",
        "default_profile",
        "selected",
        "MiniMax default auth profile updated.",
        Some(profile_id),
    ))
}

/// Registers a pending OpenAI authorization-code attempt (fresh PKCE
/// verifier, attempt-id-as-state) in the shared attempt map and returns the
/// envelope with the authorization URL.
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
    let endpoint_config = oauth_endpoint_config_from_env().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to load OpenAI OAuth endpoint config: {error}"
        )))
    })?;
    let (document, _, _) = load_openai_console_config_snapshot()?;
    let redirect_uri = build_openai_oauth_callback_url(Some(&document), headers)?;
    let code_verifier = generate_pkce_verifier();
    let code_challenge = pkce_challenge(code_verifier.as_str());
    let authorization_url = build_authorization_url(
        &endpoint_config.authorization_endpoint,
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
        provider: ModelProviderAuthProviderKind::Openai,
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
        device_user_code: None,
        device_auth_id: None,
        poll_interval_ms: 0,
        next_poll_after_unix_ms: 0,
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

#[derive(Debug, Clone, Deserialize)]
struct OpenAiChatGptDeviceCodeResponse {
    user_code: String,
    device_auth_id: String,
    #[serde(default)]
    interval: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenAiChatGptDeviceTokenResponse {
    #[serde(default)]
    authorization_code: Option<String>,
    #[serde(default)]
    code_verifier: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

enum OpenAiChatGptDevicePollOutcome {
    Pending(String),
    Succeeded { authorization_code: String, code_verifier: String },
    Failed(String),
}

/// Starts the public ChatGPT/Codex device flow used by Codex-style clients.
///
/// The built-in client id is a public native-app id; there is intentionally no
/// client secret, matching the competitor CLIs and avoiding user-side app setup.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::result_large_err)]
async fn start_openai_chatgpt_device_oauth_attempt(
    state: &AppState,
    context: &RequestContext,
    profile_id: String,
    profile_name: String,
    scope: control_plane::AuthProfileScope,
    scopes: Vec<String>,
    set_default: bool,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let attempt_id = Ulid::new().to_string().to_ascii_lowercase();
    let code_endpoint = Url::parse(OPENAI_CHATGPT_DEVICE_CODE_ENDPOINT).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "invalid ChatGPT device code endpoint: {error}"
        )))
    })?;
    let token_endpoint = Url::parse(OPENAI_CHATGPT_TOKEN_ENDPOINT).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "invalid ChatGPT OAuth token endpoint: {error}"
        )))
    })?;
    let code_response =
        request_openai_chatgpt_device_code(&code_endpoint, OPENAI_CHATGPT_OAUTH_CLIENT_ID)
            .await
            .map_err(|error| {
                runtime_status_response(tonic::Status::unavailable(format!(
                    "OpenAI ChatGPT device-code request failed: {}",
                    sanitize_http_error_message(error.to_string().as_str())
                )))
            })?;
    let poll_interval_ms = normalize_openai_chatgpt_poll_interval_ms(code_response.interval);
    let expires_at_unix_ms = now.saturating_add(OPENAI_CHATGPT_DEVICE_ATTEMPT_TTL_MS);
    let message = format!(
        "Open {OPENAI_CHATGPT_DEVICE_VERIFICATION_URL} and enter code {}.",
        code_response.user_code
    );
    let attempt = OpenAiOAuthAttempt {
        provider: ModelProviderAuthProviderKind::Openai,
        attempt_id: attempt_id.clone(),
        expires_at_unix_ms,
        redirect_uri: OPENAI_CHATGPT_DEVICE_REDIRECT_URI.to_owned(),
        profile_id: profile_id.clone(),
        profile_name,
        scope,
        client_id: OPENAI_CHATGPT_OAUTH_CLIENT_ID.to_owned(),
        client_secret: String::new(),
        scopes,
        token_endpoint,
        code_verifier: String::new(),
        device_user_code: Some(code_response.user_code.clone()),
        device_auth_id: Some(code_response.device_auth_id),
        poll_interval_ms,
        next_poll_after_unix_ms: now.saturating_add(poll_interval_ms.try_into().unwrap_or(0)),
        set_default,
        context: ConsoleActionContext {
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        },
        state: OpenAiOAuthAttemptStateRecord::Pending { message: message.clone() },
    };
    let mut attempts = lock_openai_oauth_attempts(state)?;
    cleanup_openai_oauth_attempts(&mut attempts, now);
    attempts.insert(attempt_id.clone(), attempt);
    Ok(control_plane::OpenAiOAuthBootstrapEnvelope {
        contract: contract_descriptor(),
        provider: "openai".to_owned(),
        attempt_id,
        authorization_url: OPENAI_CHATGPT_DEVICE_VERIFICATION_URL.to_owned(),
        expires_at_unix_ms,
        profile_id: Some(profile_id),
        message,
    })
}

async fn apply_openai_chatgpt_device_poll_result(
    state: &AppState,
    attempt: OpenAiOAuthAttempt,
    now: i64,
) -> Result<(), Response> {
    let device_auth_id = attempt.device_auth_id.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "OpenAI ChatGPT device attempt is missing a device_auth_id",
        ))
    })?;
    let user_code = attempt.device_user_code.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "OpenAI ChatGPT device attempt is missing a user_code",
        ))
    })?;
    let token_poll_endpoint =
        Url::parse(OPENAI_CHATGPT_DEVICE_TOKEN_ENDPOINT).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "invalid ChatGPT device token endpoint: {error}"
            )))
        })?;
    let outcome = poll_openai_chatgpt_device_token(
        &token_poll_endpoint,
        device_auth_id.as_str(),
        user_code.as_str(),
    )
    .await
    .unwrap_or_else(|error| {
        OpenAiChatGptDevicePollOutcome::Pending(format!(
            "OpenAI ChatGPT authorization is still pending: {}",
            sanitize_http_error_message(error.to_string().as_str())
        ))
    });
    match outcome {
        OpenAiChatGptDevicePollOutcome::Pending(message) => {
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state = OpenAiOAuthAttemptStateRecord::Pending { message };
            }
            Ok(())
        }
        OpenAiChatGptDevicePollOutcome::Failed(message) => {
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state =
                    OpenAiOAuthAttemptStateRecord::Failed { message, completed_at_unix_ms: now };
            }
            Ok(())
        }
        OpenAiChatGptDevicePollOutcome::Succeeded { authorization_code, code_verifier } => {
            let token_result = match exchange_authorization_code(
                &attempt.token_endpoint,
                attempt.redirect_uri.as_str(),
                attempt.client_id.as_str(),
                attempt.client_secret.as_str(),
                code_verifier.as_str(),
                authorization_code.as_str(),
                OPENAI_HTTP_TIMEOUT,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    let mut attempts = lock_openai_oauth_attempts(state)?;
                    if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                        stored.state = OpenAiOAuthAttemptStateRecord::Failed {
                            message: sanitize_http_error_message(error.to_string().as_str()),
                            completed_at_unix_ms: now,
                        };
                    }
                    return Ok(());
                }
            };
            let expires_at_unix_ms = token_result
                .expires_in_seconds
                .map(|seconds| now.saturating_add((seconds as i64).saturating_mul(1_000)));
            // ChatGPT/Codex subscription tokens are accepted by the Codex auth
            // issuer, not by the public /v1/models API-key validation probe.
            let message = persist_openai_oauth_success(
                state,
                &attempt,
                token_result.access_token.as_str(),
                token_result.refresh_token.as_str(),
                expires_at_unix_ms,
            )
            .await?;
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state = OpenAiOAuthAttemptStateRecord::Succeeded {
                    profile_id: attempt.profile_id,
                    message,
                    completed_at_unix_ms: now,
                };
            }
            Ok(())
        }
    }
}

/// Requests a ChatGPT/Codex device user code from OpenAI.
///
/// # Errors
/// Returns an error on transport failure, a non-success status, invalid JSON,
/// or a response missing the user-visible code or polling handle.
async fn request_openai_chatgpt_device_code(
    code_endpoint: &Url,
    client_id: &str,
) -> Result<OpenAiChatGptDeviceCodeResponse> {
    let client = ReqwestClient::builder()
        .timeout(OPENAI_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build OpenAI ChatGPT device-code client")?;
    let response =
        client.post(code_endpoint.clone()).json(&json!({ "client_id": client_id })).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "OpenAI ChatGPT device-code endpoint returned HTTP {}: {}",
            status.as_u16(),
            crate::model_provider::sanitize_remote_error(body.as_str())
        );
    }
    let payload: OpenAiChatGptDeviceCodeResponse = serde_json::from_str(body.as_str())
        .context("OpenAI ChatGPT device-code response was invalid JSON")?;
    if payload.user_code.trim().is_empty() || payload.device_auth_id.trim().is_empty() {
        anyhow::bail!(
            "OpenAI ChatGPT device-code response did not include user_code and device_auth_id"
        );
    }
    Ok(payload)
}

/// Polls OpenAI's device-auth token endpoint once.
///
/// # Errors
/// Returns an error only for transport or malformed successful JSON; provider
/// pending/failure statuses are classified in the return value.
async fn poll_openai_chatgpt_device_token(
    token_endpoint: &Url,
    device_auth_id: &str,
    user_code: &str,
) -> Result<OpenAiChatGptDevicePollOutcome> {
    let client = ReqwestClient::builder()
        .timeout(OPENAI_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build OpenAI ChatGPT device-token client")?;
    let response = client
        .post(token_endpoint.clone())
        .json(&json!({ "device_auth_id": device_auth_id, "user_code": user_code }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if matches!(status.as_u16(), 403 | 404) {
        return Ok(OpenAiChatGptDevicePollOutcome::Pending(
            "Awaiting OpenAI ChatGPT sign-in.".to_owned(),
        ));
    }
    if !status.is_success() {
        return Ok(OpenAiChatGptDevicePollOutcome::Failed(format!(
            "OpenAI ChatGPT device-token endpoint returned HTTP {}: {}",
            status.as_u16(),
            crate::model_provider::sanitize_remote_error(body.as_str())
        )));
    }
    let payload: OpenAiChatGptDeviceTokenResponse = serde_json::from_str(body.as_str())
        .context("OpenAI ChatGPT device-token response was invalid JSON")?;
    if let Some(error) = payload.error.as_deref().and_then(normalize_optional_text) {
        let description = payload
            .error_description
            .as_deref()
            .and_then(normalize_optional_text)
            .unwrap_or("OpenAI ChatGPT authorization is still pending.");
        let message = format!("{error}: {description}");
        let lower = message.to_ascii_lowercase();
        if lower.contains("pending") || lower.contains("slow_down") {
            return Ok(OpenAiChatGptDevicePollOutcome::Pending(sanitize_http_error_message(
                message.as_str(),
            )));
        }
        return Ok(OpenAiChatGptDevicePollOutcome::Failed(sanitize_http_error_message(
            message.as_str(),
        )));
    }
    let authorization_code = payload
        .authorization_code
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned));
    let code_verifier = payload
        .code_verifier
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned));
    match (authorization_code, code_verifier) {
        (Some(authorization_code), Some(code_verifier)) => {
            Ok(OpenAiChatGptDevicePollOutcome::Succeeded { authorization_code, code_verifier })
        }
        _ => Ok(OpenAiChatGptDevicePollOutcome::Pending(
            "Awaiting OpenAI ChatGPT sign-in.".to_owned(),
        )),
    }
}

fn normalize_openai_chatgpt_poll_interval_ms(interval_seconds: Option<serde_json::Value>) -> u64 {
    let seconds = match interval_seconds {
        Some(serde_json::Value::Number(value)) => value.as_u64(),
        Some(serde_json::Value::String(value)) => value.trim().parse::<u64>().ok(),
        _ => None,
    }
    .unwrap_or(5);
    seconds.clamp(3, 60).saturating_mul(1_000)
}

async fn persist_openai_oauth_success(
    state: &AppState,
    attempt: &OpenAiOAuthAttempt,
    access_token: &str,
    refresh_token: &str,
    expires_at_unix_ms: Option<i64>,
) -> Result<String, Response> {
    let access_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &attempt.scope,
        attempt.profile_id.as_str(),
        "openai",
        "oauth_access_token",
        access_token.as_bytes(),
    )?;
    let refresh_token_vault_ref = store_provider_secret(
        state.vault.as_ref(),
        &attempt.scope,
        attempt.profile_id.as_str(),
        "openai",
        "oauth_refresh_token",
        refresh_token.as_bytes(),
    )?;
    let client_secret_vault_ref = if attempt.client_secret.trim().is_empty() {
        None
    } else {
        Some(store_provider_secret(
            state.vault.as_ref(),
            &attempt.scope,
            attempt.profile_id.as_str(),
            "openai",
            "oauth_client_secret",
            attempt.client_secret.as_bytes(),
        )?)
    };
    let refresh_state = serde_json::to_value(OAuthRefreshState::default()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize OAuth refresh state: {error}"
        )))
    })?;
    let openai_runtime = OpenAiProviderSelectionRuntime::from_client_id(attempt.client_id.as_str());
    let discovered_model_id =
        discover_preferred_openai_model_id_for_runtime(openai_runtime, access_token)
            .await
            .ok()
            .flatten();
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
            token_endpoint: attempt.token_endpoint.to_string(),
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
        persist_model_provider_auth_profile_selection_with_openai_runtime(
            state,
            &context,
            attempt.profile_id.as_str(),
            ModelProviderAuthProviderKind::Openai,
            openai_runtime,
            discovered_model_id.as_deref(),
        )
        .await?;
    }
    Ok(if attempt.set_default {
        "OpenAI OAuth profile connected and selected as the default auth profile.".to_owned()
    } else {
        "OpenAI OAuth profile connected.".to_owned()
    })
}

#[derive(Debug, Clone)]
struct MinimaxOAuthEndpointConfig {
    code_endpoint: Url,
    token_endpoint: Url,
}

/// Wire shape of the MiniMax device-flow code response; `expired_in` and
/// `interval` come in OpenClaw-compatible mixed units (see the normalize
/// helpers below).
#[derive(Debug, Clone, Deserialize)]
struct MinimaxOAuthCodeResponse {
    user_code: String,
    verification_uri: String,
    expired_in: i64,
    #[serde(default)]
    interval: Option<u64>,
    #[serde(default)]
    state: Option<String>,
}

/// Wire shape of the MiniMax device-flow token poll response: every field is
/// optional because pending, success, and failure replies populate different
/// subsets.
#[derive(Debug, Clone, Deserialize)]
struct MinimaxOAuthTokenResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expired_in: Option<u64>,
    #[serde(default)]
    resource_url: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Classified result of one MiniMax token poll; messages are already
/// sanitized for client display.
enum MinimaxOAuthPollOutcome {
    Pending(String),
    Succeeded {
        access_token: String,
        refresh_token: String,
        expires_at_unix_ms: Option<i64>,
        resource_url: Option<String>,
    },
    Failed(String),
}

/// Requests a MiniMax device user code and registers the pending attempt in
/// the shared attempt map; the user authorizes by entering the code in the
/// MiniMax portal while the daemon polls the token endpoint.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::result_large_err)]
async fn start_minimax_oauth_attempt(
    state: &AppState,
    context: &RequestContext,
    profile_id: String,
    profile_name: String,
    scope: control_plane::AuthProfileScope,
    client_id: String,
    scopes: Vec<String>,
    set_default: bool,
) -> Result<control_plane::OpenAiOAuthBootstrapEnvelope, Response> {
    let now = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let attempt_id = Ulid::new().to_string().to_ascii_lowercase();
    let endpoint_config = minimax_oauth_endpoint_config_from_env().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to load MiniMax OAuth endpoint config: {error}"
        )))
    })?;
    let code_verifier = generate_pkce_verifier();
    let code_challenge = pkce_challenge(code_verifier.as_str());
    let code_response = request_minimax_user_code(
        &endpoint_config.code_endpoint,
        client_id.as_str(),
        &scopes,
        code_challenge.as_str(),
        attempt_id.as_str(),
    )
    .await
    .map_err(|error| {
        runtime_status_response(tonic::Status::unavailable(format!(
            "MiniMax OAuth code request failed: {}",
            sanitize_http_error_message(error.to_string().as_str())
        )))
    })?;
    let expires_at_unix_ms = normalize_minimax_expired_in_to_unix_ms(code_response.expired_in, now);
    let poll_interval_ms = normalize_minimax_poll_interval_ms(code_response.interval);
    let attempt = OpenAiOAuthAttempt {
        provider: ModelProviderAuthProviderKind::Minimax,
        attempt_id: attempt_id.clone(),
        expires_at_unix_ms,
        redirect_uri: String::new(),
        profile_id: profile_id.clone(),
        profile_name,
        scope,
        client_id,
        client_secret: String::new(),
        scopes,
        token_endpoint: endpoint_config.token_endpoint,
        code_verifier,
        device_user_code: Some(code_response.user_code.clone()),
        device_auth_id: None,
        poll_interval_ms,
        next_poll_after_unix_ms: now.saturating_add(poll_interval_ms.try_into().unwrap_or(0)),
        set_default,
        context: ConsoleActionContext {
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        },
        state: OpenAiOAuthAttemptStateRecord::Pending {
            message: format!(
                "Awaiting MiniMax OAuth authorization. Enter user code {} in the MiniMax portal.",
                code_response.user_code
            ),
        },
    };
    let mut attempts = lock_openai_oauth_attempts(state)?;
    cleanup_openai_oauth_attempts(&mut attempts, now);
    attempts.insert(attempt_id.clone(), attempt);
    Ok(control_plane::OpenAiOAuthBootstrapEnvelope {
        contract: contract_descriptor(),
        provider: "minimax".to_owned(),
        attempt_id,
        authorization_url: code_response.verification_uri,
        expires_at_unix_ms,
        profile_id: Some(profile_id),
        message: format!(
            "MiniMax OAuth user code issued. Enter code {} in the MiniMax portal.",
            code_response.user_code
        ),
    })
}

/// Polls the MiniMax token endpoint once for `attempt` and applies the
/// outcome to the stored attempt: pending updates the message, failure marks
/// the attempt failed, and success stores tokens in the vault, persists the
/// auth profile, and optionally adopts the returned resource URL as the
/// configured base URL.
async fn apply_minimax_oauth_poll_result(
    state: &AppState,
    attempt: OpenAiOAuthAttempt,
    now: i64,
) -> Result<(), Response> {
    let user_code = attempt.device_user_code.clone().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "MiniMax OAuth attempt is missing a user_code",
        ))
    })?;
    // Transport-level poll errors are mapped to Pending rather than Failed:
    // a transient network hiccup must not kill an attempt the user may still
    // be in the middle of authorizing.
    let outcome = poll_minimax_oauth_token(
        &attempt.token_endpoint,
        attempt.client_id.as_str(),
        user_code.as_str(),
        attempt.code_verifier.as_str(),
        now,
    )
    .await
    .unwrap_or_else(|error| {
        MinimaxOAuthPollOutcome::Pending(format!(
            "MiniMax OAuth authorization is still pending: {}",
            sanitize_http_error_message(error.to_string().as_str())
        ))
    });
    match outcome {
        MinimaxOAuthPollOutcome::Pending(message) => {
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state = OpenAiOAuthAttemptStateRecord::Pending { message };
            }
            Ok(())
        }
        MinimaxOAuthPollOutcome::Failed(message) => {
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state =
                    OpenAiOAuthAttemptStateRecord::Failed { message, completed_at_unix_ms: now };
            }
            Ok(())
        }
        MinimaxOAuthPollOutcome::Succeeded {
            access_token,
            refresh_token,
            expires_at_unix_ms,
            resource_url,
        } => {
            let context = request_context_from_console_action(&attempt.context);
            let access_token_vault_ref = store_provider_secret(
                state.vault.as_ref(),
                &attempt.scope,
                attempt.profile_id.as_str(),
                "minimax",
                "oauth_access_token",
                access_token.as_bytes(),
            )?;
            let refresh_token_vault_ref = store_provider_secret(
                state.vault.as_ref(),
                &attempt.scope,
                attempt.profile_id.as_str(),
                "minimax",
                "oauth_refresh_token",
                refresh_token.as_bytes(),
            )?;
            let refresh_state =
                serde_json::to_value(OAuthRefreshState::default()).map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to serialize OAuth refresh state: {error}"
                    )))
                })?;
            let profile = control_plane::AuthProfileView {
                profile_id: attempt.profile_id.clone(),
                provider: minimax_auth_profile_provider(),
                profile_name: attempt.profile_name.clone(),
                scope: attempt.scope.clone(),
                credential: control_plane::AuthCredentialView::Oauth {
                    access_token_vault_ref,
                    refresh_token_vault_ref,
                    token_endpoint: attempt.token_endpoint.to_string(),
                    client_id: Some(attempt.client_id.clone()),
                    client_secret_vault_ref: None,
                    scopes: attempt.scopes.clone(),
                    expires_at_unix_ms,
                    refresh_state,
                },
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
            };
            persist_openai_auth_profile(state, &context, profile).await?;
            let normalized_resource_base_url =
                resource_url.as_deref().and_then(normalize_minimax_resource_url);
            let model_discovery_base_url =
                normalized_resource_base_url.as_deref().unwrap_or(MINIMAX_DEFAULT_BASE_URL);
            let discovered_model_id = discover_minimax_api_key_model_id(
                model_discovery_base_url,
                access_token.as_str(),
                ANTHROPIC_HTTP_TIMEOUT,
            )
            .await
            .ok();
            if attempt.set_default {
                persist_model_provider_auth_profile_selection_with_discovered_model(
                    state,
                    &context,
                    attempt.profile_id.as_str(),
                    ModelProviderAuthProviderKind::Minimax,
                    discovered_model_id.as_deref(),
                )
                .await?;
            }
            if let Some(base_url) = normalized_resource_base_url {
                persist_minimax_resource_base_url(state, &context, base_url.as_str()).await?;
            }
            let message = if attempt.set_default {
                "MiniMax OAuth profile connected and selected as the default auth profile."
            } else {
                "MiniMax OAuth profile connected."
            };
            let mut attempts = lock_openai_oauth_attempts(state)?;
            if let Some(stored) = attempts.get_mut(attempt.attempt_id.as_str()) {
                stored.state = OpenAiOAuthAttemptStateRecord::Succeeded {
                    profile_id: attempt.profile_id,
                    message: message.to_owned(),
                    completed_at_unix_ms: now,
                };
            }
            Ok(())
        }
    }
}

/// Trims the requested scopes, substituting the MiniMax defaults when nothing
/// non-empty remains; unlike OpenAI scopes these are case-preserving.
fn normalize_minimax_scopes(scopes: &[String]) -> Vec<String> {
    let normalized = scopes
        .iter()
        .filter_map(|scope| normalize_optional_text(scope))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if normalized.is_empty() {
        return MINIMAX_OAUTH_DEFAULT_SCOPES.iter().map(|scope| (*scope).to_owned()).collect();
    }
    normalized
}

fn normalize_provider_oauth_scopes(scopes: &[String]) -> Vec<String> {
    scopes.iter().filter_map(|scope| normalize_optional_text(scope)).map(str::to_owned).collect()
}

/// Resolves the MiniMax OAuth endpoints from `PALYRA_MINIMAX_OAUTH_BASE_URL`
/// (falling back to the production base URL), enforcing https and rejecting
/// embedded credentials, query, and fragment.
///
/// # Errors
/// Returns an error when the configured base URL fails any of those checks.
fn minimax_oauth_endpoint_config_from_env() -> Result<MinimaxOAuthEndpointConfig> {
    let base_url = env::var("PALYRA_MINIMAX_OAUTH_BASE_URL")
        .ok()
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
        .unwrap_or_else(|| MINIMAX_OAUTH_DEFAULT_BASE_URL.to_owned());
    let base_url = Url::parse(base_url.trim()).context("MiniMax OAuth base URL must be valid")?;
    if base_url.scheme() != "https" {
        anyhow::bail!("MiniMax OAuth base URL must use https");
    }
    if base_url.host_str().is_none() {
        anyhow::bail!("MiniMax OAuth base URL must include a host");
    }
    if !base_url.username().is_empty()
        || base_url.password().is_some()
        || base_url.query().is_some()
        || base_url.fragment().is_some()
    {
        anyhow::bail!("MiniMax OAuth base URL must not include credentials, query, or fragment");
    }
    Ok(MinimaxOAuthEndpointConfig {
        code_endpoint: base_url.join("/oauth/code")?,
        token_endpoint: base_url.join("/oauth/token")?,
    })
}

/// Requests a device user code from the MiniMax code endpoint, verifying the
/// echoed `state` (when present) matches the attempt id to keep the response
/// bound to this attempt.
///
/// # Errors
/// Returns an error on transport failure, a non-success status, invalid JSON,
/// a mismatched `state`, or a payload missing the user code or URI.
async fn request_minimax_user_code(
    code_endpoint: &Url,
    client_id: &str,
    scopes: &[String],
    code_challenge: &str,
    state: &str,
) -> Result<MinimaxOAuthCodeResponse> {
    let client = ReqwestClient::builder()
        .timeout(OPENAI_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build MiniMax OAuth code client")?;
    let form_fields = [
        ("response_type", "code".to_owned()),
        ("client_id", client_id.to_owned()),
        ("scope", scopes.join(" ")),
        ("code_challenge", code_challenge.to_owned()),
        ("code_challenge_method", "S256".to_owned()),
        ("state", state.to_owned()),
    ];
    let response = client.post(code_endpoint.clone()).form(&form_fields).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "MiniMax OAuth code endpoint returned HTTP {}: {}",
            status.as_u16(),
            crate::model_provider::sanitize_remote_error(body.as_str())
        );
    }
    let payload: MinimaxOAuthCodeResponse = serde_json::from_str(body.as_str())
        .context("MiniMax OAuth code response was invalid JSON")?;
    if let Some(returned_state) =
        payload.state.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        if returned_state != state {
            anyhow::bail!("MiniMax OAuth code response state did not match the request");
        }
    }
    if payload.user_code.trim().is_empty() || payload.verification_uri.trim().is_empty() {
        anyhow::bail!("MiniMax OAuth code response did not include user_code and verification_uri");
    }
    Ok(payload)
}

/// Performs one MiniMax device-flow token poll and classifies the reply.
///
/// MiniMax does not use the standard RFC 8628 error codes consistently, so
/// pending-vs-failed is decided by substring matching on the error text
/// ("pending"/"slow_down" means keep polling); anything else on a success
/// status without tokens is treated as a terminal failure.
///
/// # Errors
/// Returns an error only for transport failures, invalid JSON, or a success
/// payload missing one of the two tokens; provider-side rejections come back
/// as [`MinimaxOAuthPollOutcome::Failed`] instead.
async fn poll_minimax_oauth_token(
    token_endpoint: &Url,
    client_id: &str,
    user_code: &str,
    code_verifier: &str,
    now_unix_ms: i64,
) -> Result<MinimaxOAuthPollOutcome> {
    let client = ReqwestClient::builder()
        .timeout(OPENAI_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build MiniMax OAuth token client")?;
    let form_fields = [
        ("grant_type", MINIMAX_OAUTH_GRANT_TYPE.to_owned()),
        ("client_id", client_id.to_owned()),
        ("user_code", user_code.to_owned()),
        ("code_verifier", code_verifier.to_owned()),
    ];
    let response = client.post(token_endpoint.clone()).form(&form_fields).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let sanitized = crate::model_provider::sanitize_remote_error(body.as_str());
    if !status.is_success() {
        let lower = body.to_ascii_lowercase();
        if lower.contains("authorization_pending") || lower.contains("pending") {
            return Ok(MinimaxOAuthPollOutcome::Pending(
                "Awaiting MiniMax OAuth authorization.".to_owned(),
            ));
        }
        return Ok(MinimaxOAuthPollOutcome::Failed(format!(
            "MiniMax OAuth token endpoint returned HTTP {}: {}",
            status.as_u16(),
            sanitized
        )));
    }
    let payload: MinimaxOAuthTokenResponse = serde_json::from_str(body.as_str())
        .context("MiniMax OAuth token response was invalid JSON")?;
    let status_text = payload.status.as_deref().unwrap_or_default().to_ascii_lowercase();
    if status_text == "success" || payload.access_token.is_some() {
        let access_token = payload
            .access_token
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .ok_or_else(|| anyhow::anyhow!("MiniMax OAuth token response missing access_token"))?;
        let refresh_token = payload
            .refresh_token
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .ok_or_else(|| anyhow::anyhow!("MiniMax OAuth token response missing refresh_token"))?;
        return Ok(MinimaxOAuthPollOutcome::Succeeded {
            access_token,
            refresh_token,
            expires_at_unix_ms: payload
                .expired_in
                .and_then(|value| i64::try_from(value).ok())
                .map(|value| normalize_minimax_expired_in_to_unix_ms(value, now_unix_ms)),
            resource_url: payload.resource_url,
        });
    }
    let message = payload
        .error_description
        .or(payload.message)
        .or(payload.error)
        .unwrap_or_else(|| "Awaiting MiniMax OAuth authorization.".to_owned());
    let lower = message.to_ascii_lowercase();
    if lower.contains("pending") || lower.contains("slow_down") {
        Ok(MinimaxOAuthPollOutcome::Pending(sanitize_http_error_message(message.as_str())))
    } else {
        Ok(MinimaxOAuthPollOutcome::Failed(sanitize_http_error_message(message.as_str())))
    }
}

// MiniMax/OpenClaw clients send `expired_in` in three formats; the magnitude
// disambiguates: >= 1e12 is an absolute unix-millisecond timestamp, >= 1e9 an
// absolute unix-second timestamp, anything smaller a standard OAuth relative
// duration in seconds.
fn normalize_minimax_expired_in_to_unix_ms(expired_in: i64, now_unix_ms: i64) -> i64 {
    let normalized = expired_in.max(1);
    if normalized >= 1_000_000_000_000 {
        return normalized;
    }
    if normalized >= 1_000_000_000 {
        return normalized.saturating_mul(1_000);
    }
    now_unix_ms.saturating_add(normalized.saturating_mul(1_000))
}

// Same mixed-unit compatibility as `expired_in`: values below 100 are
// RFC 8628-style seconds, larger values are already milliseconds (OpenClaw).
fn normalize_minimax_poll_interval_ms(interval: Option<u64>) -> u64 {
    let raw = interval.unwrap_or(2_000).max(1);
    if raw < 100 {
        return raw.saturating_mul(1_000);
    }
    raw
}

/// Validates the provider-supplied `resource_url` and normalizes it onto the
/// anthropic-compatible path, returning `None` for anything suspicious.
///
/// The value ends up persisted as the daemon's model-provider base URL, so it
/// is treated as untrusted input: https only, host restricted to the trusted
/// MiniMax domain allowlist, and no credentials, query, or fragment.
fn normalize_minimax_resource_url(raw: &str) -> Option<String> {
    let trimmed = normalize_optional_text(raw)?;
    let mut url = Url::parse(trimmed).ok()?;
    if url.scheme() != "https" || !minimax_resource_url_host_allowed(&url) {
        return None;
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let path = url.path().trim_end_matches('/').to_owned();
    if path.ends_with("/anthropic") {
        return Some(url.to_string().trim_end_matches('/').to_owned());
    }
    url.set_path(format!("{path}/anthropic").as_str());
    Some(url.to_string().trim_end_matches('/').to_owned())
}

fn minimax_resource_url_host_allowed(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    // The suffix match requires a '.' boundary so lookalike registrations
    // (e.g. a domain merely ending in an allowed name) cannot pass.
    MINIMAX_RESOURCE_URL_ALLOWED_DOMAINS.iter().any(|domain| {
        host == *domain || host.strip_suffix(domain).is_some_and(|prefix| prefix.ends_with('.'))
    })
}

/// Persists a normalized MiniMax resource URL as
/// `model_provider.anthropic_base_url` in the daemon config, re-running the
/// network policy check (private targets need an explicit opt-in) before the
/// write, and journals the update.
#[allow(clippy::result_large_err)]
async fn persist_minimax_resource_base_url(
    state: &AppState,
    context: &RequestContext,
    base_url: &str,
) -> Result<(), Response> {
    let path = resolve_console_config_mutation_path(None)?;
    let path_ref = FsPath::new(path.as_str());
    ensure_console_config_parent_dir(path_ref)?;
    let (mut document, _) = load_console_document_for_mutation(path_ref)?;
    let allow_private_base_url =
        get_value_at_path(&document, "model_provider.allow_private_base_url")
            .ok()
            .flatten()
            .and_then(toml::Value::as_bool)
            .unwrap_or(false);
    crate::model_provider::validate_openai_base_url_network_policy(
        base_url,
        allow_private_base_url,
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "MiniMax OAuth resource_url rejected: {error}"
        )))
    })?;
    set_string_value_at_path(&mut document, "model_provider.anthropic_base_url", base_url)?;
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
            "event": "auth.profile.minimax_resource_url_updated",
            "provider": "minimax",
            "source_path": path,
        }),
    )
    .await
}

fn default_openai_profile_scope() -> control_plane::AuthProfileScope {
    control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None }
}

fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

// MiniMax has no first-class kind in the auth-profile contract, so its
// profiles are stored as a custom provider with a fixed custom name; lookups
// must match on that pair (see load_minimax_auth_profile_record).
fn minimax_auth_profile_provider() -> control_plane::AuthProfileProvider {
    control_plane::AuthProfileProvider {
        kind: "custom".to_owned(),
        custom_name: Some(MINIMAX_PROVIDER_CUSTOM_NAME.to_owned()),
    }
}

fn xai_auth_profile_provider() -> control_plane::AuthProfileProvider {
    control_plane::AuthProfileProvider {
        kind: "custom".to_owned(),
        custom_name: Some(XAI_PROVIDER_CUSTOM_NAME.to_owned()),
    }
}

#[cfg(test)]
mod minimax_tests {
    use super::{
        normalize_minimax_expired_in_to_unix_ms, normalize_minimax_poll_interval_ms,
        normalize_minimax_resource_url,
    };

    #[test]
    fn minimax_expired_in_normalizes_openclaw_timestamp_and_standard_durations() {
        let now = 1_700_000_000_000_i64;
        assert_eq!(
            normalize_minimax_expired_in_to_unix_ms(1_700_000_060_000, now),
            1_700_000_060_000,
            "MiniMax/OpenClaw absolute millisecond timestamps should be preserved"
        );
        assert_eq!(
            normalize_minimax_expired_in_to_unix_ms(1_700_000_060, now),
            1_700_000_060_000,
            "absolute second timestamps should be converted to milliseconds"
        );
        assert_eq!(
            normalize_minimax_expired_in_to_unix_ms(60, now),
            1_700_000_060_000,
            "standard OAuth duration seconds should be added to the current time"
        );
    }

    #[test]
    fn minimax_resource_url_normalizes_to_anthropic_base_url() {
        assert_eq!(
            normalize_minimax_resource_url("https://tenant.minimax.io"),
            Some("https://tenant.minimax.io/anthropic".to_owned())
        );
        assert_eq!(
            normalize_minimax_resource_url("https://tenant.minimax.io/anthropic"),
            Some("https://tenant.minimax.io/anthropic".to_owned())
        );
        assert_eq!(
            normalize_minimax_resource_url("https://api.minimaxi.com"),
            Some("https://api.minimaxi.com/anthropic".to_owned()),
            "China MiniMax endpoint remains supported"
        );
        assert_eq!(
            normalize_minimax_resource_url("https://tenant.minimax.io/path?token=secret"),
            None,
            "resource URLs with query secrets must be rejected"
        );
        assert_eq!(
            normalize_minimax_resource_url("https://attacker.example/collect"),
            None,
            "resource URLs outside trusted MiniMax domains must be rejected"
        );
        assert_eq!(
            normalize_minimax_resource_url("https://evilminimax.io/collect"),
            None,
            "lookalike domains must not match the MiniMax suffix allowlist"
        );
        assert_eq!(
            normalize_minimax_resource_url("https://127.0.0.1:8443/internal"),
            None,
            "resource URLs targeting private/local hosts must be rejected"
        );
    }

    #[test]
    fn minimax_poll_interval_accepts_openclaw_milliseconds_and_standard_seconds() {
        assert_eq!(normalize_minimax_poll_interval_ms(None), 2_000);
        assert_eq!(normalize_minimax_poll_interval_ms(Some(2_000)), 2_000);
        assert_eq!(normalize_minimax_poll_interval_ms(Some(2)), 2_000);
        assert_eq!(normalize_minimax_poll_interval_ms(Some(500)), 500);
    }
}

fn request_context_from_console_action(context: &ConsoleActionContext) -> RequestContext {
    RequestContext {
        principal: context.principal.clone(),
        device_id: context.device_id.clone(),
        channel: context.channel.clone(),
    }
}

/// Normalizes a caller-supplied identifier (profile id, attempt id, agent id)
/// to lowercase and validates the `[a-z0-9._-]{1,128}` charset; identifiers
/// feed vault key derivation and config values, so the alphabet is strict.
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

fn normalize_optional_openai_secret(raw: Option<String>) -> String {
    raw.unwrap_or_default().trim().to_owned()
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

// The profile id is hashed into the vault key so key length stays bounded
// and the key format is independent of the (user-influenced) id format.
fn provider_secret_key(provider_slug: &str, profile_id: &str, suffix: &str) -> String {
    let digest = sha256_hex(profile_id.as_bytes());
    format!("auth_{provider_slug}_{}_{}", &digest[..16], suffix)
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
    store_provider_secret(vault, scope, profile_id, "openai", suffix, value)
}

/// Stores one provider secret in the vault under a derived key and returns
/// the vault ref (`scope/key`) that auth profiles carry instead of the value.
///
/// # Errors
/// Returns a validation error response for an unsupported scope and an
/// internal error response when the vault write fails.
#[allow(clippy::result_large_err)]
fn store_provider_secret(
    vault: &Vault,
    scope: &control_plane::AuthProfileScope,
    profile_id: &str,
    provider_slug: &str,
    suffix: &str,
    value: &[u8],
) -> Result<String, Response> {
    let vault_scope = vault_scope_for_openai_profile_scope(scope)?;
    let key = provider_secret_key(provider_slug, profile_id, suffix);
    vault.put_secret(&vault_scope, key.as_str(), value).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to store provider secret {}: {error}",
            openai_secret_vault_ref(&vault_scope, key.as_str())
        )))
    })?;
    Ok(openai_secret_vault_ref(&vault_scope, key.as_str()))
}

/// Loads a vault secret by ref and decodes it as UTF-8; `field` only labels
/// error messages, the secret value itself never reaches them.
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
    generate_provider_profile_id("openai", profile_name)
}

/// Derives a profile id from the display name: an ASCII slug plus a ULID
/// suffix so repeated connects with the same name stay unique.
fn generate_provider_profile_id(provider_slug: &str, profile_name: &str) -> String {
    let slug = profile_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let base = if slug.is_empty() { provider_slug.to_owned() } else { slug };
    let suffix = Ulid::new().to_string().to_ascii_lowercase();
    format!("{base}-{suffix}")
}

// OpenAI bearer-token validation intentionally ignores the current
// model-provider document because that document may still point at a
// ChatGPT/Codex OAuth backend from a previous auth method. Use an explicit env
// override for tests or private OpenAI-compatible endpoints; otherwise validate
// against OpenAI's public API.
fn load_openai_validation_base_url(document: Option<&toml::Value>) -> String {
    let env_override = env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL").ok();
    load_openai_validation_base_url_with_env(env_override.as_deref(), document)
}

fn openai_compatible_runtime_base_url() -> String {
    let env_override = env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL").ok();
    load_openai_validation_base_url_with_env(env_override.as_deref(), None)
}

fn load_openai_validation_base_url_with_env(
    env_override: Option<&str>,
    _document: Option<&toml::Value>,
) -> String {
    env_override
        .and_then(|value| normalize_optional_text(value).map(ToOwned::to_owned))
        .unwrap_or_else(|| OPENAI_DEFAULT_BASE_URL.to_owned())
}

fn load_anthropic_validation_base_url(document: Option<&toml::Value>) -> String {
    let env_override = env::var("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL").ok();
    load_anthropic_validation_base_url_with_env(env_override.as_deref(), document)
}

fn load_anthropic_validation_base_url_with_env(
    env_override: Option<&str>,
    document: Option<&toml::Value>,
) -> String {
    env_override
        .and_then(|value| normalize_optional_text(value).map(ToOwned::to_owned))
        .or_else(|| document.and_then(anthropic_validation_base_url_from_document))
        .unwrap_or_else(|| ANTHROPIC_DEFAULT_BASE_URL.to_owned())
}

fn xai_model_discovery_base_url() -> String {
    env::var(XAI_MODEL_DISCOVERY_BASE_URL_ENV)
        .ok()
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
        .unwrap_or_else(|| XAI_DEFAULT_BASE_URL.to_owned())
}

async fn discover_preferred_openai_model_id_for_runtime(
    runtime: OpenAiProviderSelectionRuntime,
    bearer_token: &str,
) -> Result<Option<String>, OpenAiCredentialValidationError> {
    match runtime {
        OpenAiProviderSelectionRuntime::ChatGptCodex => {
            discover_preferred_openai_chatgpt_codex_model_id(bearer_token, OPENAI_HTTP_TIMEOUT)
                .await
        }
        OpenAiProviderSelectionRuntime::OpenAiCompatible => {
            let (document, _, _) = load_openai_console_config_snapshot().map_err(|response| {
                OpenAiCredentialValidationError::Unexpected(format!(
                    "failed to load config for OpenAI model discovery: {}",
                    response.status()
                ))
            })?;
            let base_url = load_openai_validation_base_url(Some(&document));
            discover_explicit_tool_capable_openai_compatible_model_id(
                base_url.as_str(),
                bearer_token,
                OPENAI_HTTP_TIMEOUT,
            )
            .await
        }
    }
}

#[allow(clippy::result_large_err)]
fn normalize_anthropic_oauth_token_endpoint(raw: &str) -> Result<String, Response> {
    let trimmed = normalize_required_openai_text(raw, "token_endpoint")?;
    let parsed = Url::parse(trimmed.as_str()).map_err(|_| {
        validation_error_response(
            "token_endpoint",
            "invalid",
            "token_endpoint must be a valid absolute URL",
        )
    })?;
    if parsed.scheme() != "https" {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "Anthropic OAuth token_endpoint must use https",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "Anthropic OAuth token_endpoint must not contain embedded credentials",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "Anthropic OAuth token_endpoint must not contain query or fragment components",
        ));
    }
    let host = parsed.host_str().unwrap_or_default();
    if !ANTHROPIC_OAUTH_ALLOWED_TOKEN_HOSTS.iter().any(|allowed| host.eq_ignore_ascii_case(allowed))
    {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "Anthropic OAuth token_endpoint host is not trusted",
        ));
    }
    Ok(parsed.to_string())
}

#[allow(clippy::result_large_err)]
fn normalize_xai_oauth_token_endpoint(raw: &str) -> Result<String, Response> {
    let trimmed = normalize_required_openai_text(raw, "token_endpoint")?;
    let parsed = Url::parse(trimmed.as_str()).map_err(|_| {
        validation_error_response(
            "token_endpoint",
            "invalid",
            "token_endpoint must be a valid absolute URL",
        )
    })?;
    if parsed.scheme() != "https" {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "xAI OAuth token_endpoint must use https",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "xAI OAuth token_endpoint must not contain embedded credentials",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "xAI OAuth token_endpoint must not contain query or fragment components",
        ));
    }
    let host = parsed.host_str().unwrap_or_default();
    if !host.eq_ignore_ascii_case(XAI_OAUTH_ALLOWED_ROOT_HOST)
        && !host.to_ascii_lowercase().ends_with(XAI_OAUTH_ALLOWED_TOKEN_HOST_SUFFIX)
    {
        return Err(validation_error_response(
            "token_endpoint",
            "unsupported",
            "xAI OAuth token_endpoint host is not trusted",
        ));
    }
    Ok(parsed.to_string())
}

fn load_minimax_validation_base_url(document: Option<&toml::Value>) -> String {
    env::var("PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL")
        .ok()
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .or_else(|| document.and_then(minimax_validation_base_url_from_document))
        .unwrap_or_else(|| MINIMAX_DEFAULT_BASE_URL.to_owned())
}

#[allow(clippy::result_large_err)]
fn load_openai_console_config_snapshot(
) -> Result<(toml::Value, ConfigMigrationInfo, String), Response> {
    let configured_path = configured_openai_console_config_path(None);
    load_console_config_snapshot(configured_path.as_deref(), true)
}

fn configured_openai_console_config_path(path: Option<&str>) -> Option<String> {
    path.map(str::to_owned).or_else(|| env::var("PALYRA_CONFIG").ok())
}

#[allow(clippy::result_large_err)]
fn resolve_openai_console_config_path(
    path: Option<&str>,
    require_existing: bool,
) -> Result<Option<String>, Response> {
    let configured_path = configured_openai_console_config_path(path);
    resolve_console_config_path(configured_path.as_deref(), require_existing)
}

fn anthropic_validation_base_url_from_document(document: &toml::Value) -> Option<String> {
    get_value_at_path(document, "model_provider.anthropic_base_url")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

// MiniMax shares the anthropic_base_url config key, so that key is only
// trusted as a MiniMax URL when auth_provider_kind says minimax; otherwise it
// would probe MiniMax keys against an Anthropic endpoint.
fn minimax_validation_base_url_from_document(document: &toml::Value) -> Option<String> {
    let configured_auth_provider = get_value_at_path(document, "model_provider.auth_provider_kind")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| value.eq_ignore_ascii_case(MINIMAX_PROVIDER_CUSTOM_NAME));
    configured_auth_provider.and_then(|_| anthropic_validation_base_url_from_document(document))
}

fn openai_callback_base_url_from_document(document: &toml::Value) -> Option<String> {
    get_value_at_path(document, "gateway_access.remote_base_url")
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Resolves the config path used for mutations, falling back to the first
/// default search location when no config file exists yet (selecting a
/// default profile may create the file).
#[allow(clippy::result_large_err)]
fn resolve_console_config_mutation_path(path: Option<&str>) -> Result<String, Response> {
    if let Some(resolved) = resolve_openai_console_config_path(path, false)? {
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
    load_auth_profile_record_for_provider(state, profile_id, AuthProviderKind::Openai)
}

/// Loads an auth profile and verifies it is a MiniMax profile (custom
/// provider kind with the MiniMax custom name, matched case-insensitively).
#[allow(clippy::result_large_err)]
fn load_minimax_auth_profile_record(
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
    let is_minimax = matches!(record.provider.kind, AuthProviderKind::Custom)
        && record
            .provider
            .custom_name
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(MINIMAX_PROVIDER_CUSTOM_NAME));
    if !is_minimax {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "auth profile does not belong to MiniMax",
        )));
    }
    Ok(record)
}

/// Loads an auth profile and verifies it is an xAI profile (custom provider
/// kind with the xAI custom name, matched case-insensitively).
#[allow(clippy::result_large_err)]
fn load_xai_auth_profile_record(
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
    let is_xai = matches!(record.provider.kind, AuthProviderKind::Custom)
        && record
            .provider
            .custom_name
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(XAI_PROVIDER_CUSTOM_NAME));
    if !is_xai {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "auth profile does not belong to xAI",
        )));
    }
    Ok(record)
}

#[allow(clippy::result_large_err)]
fn load_auth_profile_record_for_provider(
    state: &AppState,
    profile_id: &str,
    expected_provider: AuthProviderKind,
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
    if record.provider.kind != expected_provider {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "auth profile does not belong to the expected provider",
        )));
    }
    Ok(record)
}

// Profile writes and deletes are routed through the in-process console
// AuthService (not the registry directly) so they hit the same validation,
// authorization context, and persistence path as external console calls.
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

/// Appends an auth audit event to the journal; auth actions are not tied to
/// a chat run, so fresh ULIDs stand in for the session/run identifiers.
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

/// Rewrites the daemon config so `profile_id` becomes the active
/// model-provider auth profile: sets the auth profile/provider keys, switches
/// `model_provider.kind`, seeds provider base-URL/model defaults, removes
/// legacy plaintext key fields, and journals the selection.
///
/// # Errors
/// Returns an internal error response when the config path cannot be
/// resolved, read, validated, or written, and propagates journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn persist_model_provider_auth_profile_selection(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
    provider_kind: ModelProviderAuthProviderKind,
) -> Result<(), Response> {
    persist_model_provider_auth_profile_selection_with_discovered_model(
        state,
        context,
        profile_id,
        provider_kind,
        None,
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn persist_model_provider_auth_profile_selection_with_discovered_model(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
    provider_kind: ModelProviderAuthProviderKind,
    discovered_model_id: Option<&str>,
) -> Result<(), Response> {
    persist_model_provider_auth_profile_selection_with_openai_runtime(
        state,
        context,
        profile_id,
        provider_kind,
        OpenAiProviderSelectionRuntime::OpenAiCompatible,
        discovered_model_id,
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn persist_model_provider_auth_profile_selection_with_openai_runtime(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
    provider_kind: ModelProviderAuthProviderKind,
    openai_runtime: OpenAiProviderSelectionRuntime,
    discovered_model_id: Option<&str>,
) -> Result<(), Response> {
    let path = resolve_console_config_mutation_path(None)?;
    let path_ref = FsPath::new(path.as_str());
    ensure_console_config_parent_dir(path_ref)?;
    let (mut document, _) = load_console_document_for_mutation(path_ref)?;
    let previous_auth_provider_kind = model_provider_auth_provider_kind_from_document(&document);
    let discovered_model_present = discovered_model_id.and_then(normalize_optional_text).is_some();
    // MiniMax rides the anthropic-compatible provider runtime, so both map to
    // the same model_provider.kind; auth_provider_kind keeps them distinct.
    let provider_kind_value = match provider_kind {
        ModelProviderAuthProviderKind::Openai
        | ModelProviderAuthProviderKind::Xai
        | ModelProviderAuthProviderKind::GoogleGemini
        | ModelProviderAuthProviderKind::GoogleGeminiCli
        | ModelProviderAuthProviderKind::Openrouter => "openai_compatible",
        ModelProviderAuthProviderKind::Anthropic | ModelProviderAuthProviderKind::Minimax => {
            "anthropic"
        }
    };
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
    set_value_at_path(
        &mut document,
        "model_provider.auth_provider_kind",
        toml::Value::String(provider_kind.as_str().to_owned()),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to set model_provider.auth_provider_kind: {error}"
        )))
    })?;
    set_value_at_path(
        &mut document,
        "model_provider.kind",
        toml::Value::String(provider_kind_value.to_owned()),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to set model_provider.kind: {error}"
        )))
    })?;
    // Endpoint defaults are provider wiring; model selection is intentionally
    // not seeded here. A model chosen for one provider family must not leak
    // into another family, and new model choices come from live discovery.
    match provider_kind {
        ModelProviderAuthProviderKind::Openai => {
            apply_openai_provider_selection_defaults(
                &mut document,
                previous_auth_provider_kind,
                openai_runtime,
                discovered_model_id,
            )?;
        }
        ModelProviderAuthProviderKind::Anthropic => {
            ensure_string_value_at_path(
                &mut document,
                "model_provider.anthropic_base_url",
                ANTHROPIC_DEFAULT_BASE_URL,
            )?;
            apply_discovered_or_clear_text_model_selection(
                &mut document,
                "model_provider.anthropic_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    &mut document,
                    PendingModelRegistryProvider {
                        provider_id: "anthropic-primary",
                        display_name: "Anthropic",
                        kind: "anthropic",
                        base_url: ANTHROPIC_DEFAULT_BASE_URL,
                        auth_provider_kind: "anthropic",
                    },
                )?;
            }
        }
        ModelProviderAuthProviderKind::Minimax => {
            set_string_value_at_path(
                &mut document,
                "model_provider.anthropic_base_url",
                MINIMAX_DEFAULT_BASE_URL,
            )?;
            apply_discovered_or_clear_text_model_selection(
                &mut document,
                "model_provider.anthropic_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    &mut document,
                    PendingModelRegistryProvider {
                        provider_id: "minimax-primary",
                        display_name: "MiniMax",
                        kind: "anthropic",
                        base_url: MINIMAX_DEFAULT_BASE_URL,
                        auth_provider_kind: MINIMAX_PROVIDER_CUSTOM_NAME,
                    },
                )?;
            }
        }
        ModelProviderAuthProviderKind::Xai => {
            set_string_value_at_path(
                &mut document,
                "model_provider.openai_base_url",
                XAI_DEFAULT_BASE_URL,
            )?;
            apply_discovered_or_clear_text_model_selection(
                &mut document,
                "model_provider.openai_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    &mut document,
                    PendingModelRegistryProvider {
                        provider_id: "xai-primary",
                        display_name: "xAI (Grok)",
                        kind: "openai_compatible",
                        base_url: XAI_DEFAULT_BASE_URL,
                        auth_provider_kind: XAI_PROVIDER_CUSTOM_NAME,
                    },
                )?;
            }
        }
        ModelProviderAuthProviderKind::GoogleGemini
        | ModelProviderAuthProviderKind::GoogleGeminiCli => {
            set_string_value_at_path(
                &mut document,
                "model_provider.openai_base_url",
                GOOGLE_GEMINI_OPENAI_BASE_URL,
            )?;
            apply_discovered_or_clear_text_model_selection(
                &mut document,
                "model_provider.openai_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    &mut document,
                    PendingModelRegistryProvider {
                        provider_id: "google-gemini-primary",
                        display_name: "Google Gemini",
                        kind: "openai_compatible",
                        base_url: GOOGLE_GEMINI_OPENAI_BASE_URL,
                        auth_provider_kind: provider_kind.as_str(),
                    },
                )?;
            }
        }
        ModelProviderAuthProviderKind::Openrouter => {
            set_string_value_at_path(
                &mut document,
                "model_provider.openai_base_url",
                OPENROUTER_DEFAULT_BASE_URL,
            )?;
            apply_discovered_or_clear_text_model_selection(
                &mut document,
                "model_provider.openai_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    &mut document,
                    PendingModelRegistryProvider {
                        provider_id: "openrouter-primary",
                        display_name: "OpenRouter",
                        kind: "openai_compatible",
                        base_url: OPENROUTER_DEFAULT_BASE_URL,
                        auth_provider_kind: provider_kind.as_str(),
                    },
                )?;
            }
        }
    }
    // Once an auth profile is selected, credentials come from the vault via
    // the profile; legacy inline key fields are dropped so the config file
    // can never carry a raw key alongside the selection.
    let _ = unset_value_at_path(&mut document, "model_provider.openai_api_key");
    let _ = unset_value_at_path(&mut document, "model_provider.openai_api_key_vault_ref");
    let _ = unset_value_at_path(&mut document, "model_provider.anthropic_api_key");
    let _ = unset_value_at_path(&mut document, "model_provider.anthropic_api_key_vault_ref");
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
            "provider": provider_kind.as_str(),
            "source_path": path,
        }),
    )
    .await
}

/// Clears the configured default auth profile when it points at
/// `profile_id` (used after that profile is revoked or deleted); returns
/// whether anything was cleared. A missing config file is a no-op.
///
/// # Errors
/// Returns an internal error response when the config cannot be read,
/// validated, or written, and propagates journaling failures.
#[allow(clippy::result_large_err)]
pub(crate) async fn clear_model_provider_auth_profile_selection_if_matches(
    state: &AppState,
    context: &RequestContext,
    profile_id: &str,
) -> Result<bool, Response> {
    let Some(path) = resolve_openai_console_config_path(None, false)? else {
        return Ok(false);
    };
    let selected = read_console_config_profile_id(path.as_str())?;
    if selected.as_deref() != Some(profile_id) {
        return Ok(false);
    }
    let path_ref = FsPath::new(path.as_str());
    let (mut document, _) = load_console_document_for_mutation(path_ref)?;
    let _ = unset_value_at_path(&mut document, "model_provider.auth_profile_id");
    let _ = unset_value_at_path(&mut document, "model_provider.auth_provider_kind");
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
            "source_path": path,
        }),
    )
    .await?;
    Ok(true)
}

#[allow(clippy::result_large_err)]
fn apply_openai_provider_selection_defaults(
    document: &mut toml::Value,
    previous_auth_provider_kind: Option<ModelProviderAuthProviderKind>,
    runtime: OpenAiProviderSelectionRuntime,
    discovered_model_id: Option<&str>,
) -> Result<(), Response> {
    let discovered_model_present = discovered_model_id.and_then(normalize_optional_text).is_some();
    let openai_compatible_base_url = openai_compatible_runtime_base_url();
    match runtime {
        OpenAiProviderSelectionRuntime::ChatGptCodex => {
            set_string_value_at_path(
                document,
                "model_provider.openai_base_url",
                OPENAI_CHATGPT_CODEX_BASE_URL,
            )?;
            let provider = PendingModelRegistryProvider {
                provider_id: "openai-primary",
                display_name: "ChatGPT Login",
                kind: "openai_compatible",
                base_url: OPENAI_CHATGPT_CODEX_BASE_URL,
                auth_provider_kind: "openai",
            };
            let model_provider_config = file_model_provider_config_from_document(document)?;
            if let Some(model_id) = preserved_unresolved_default_chat_model_for_provider(
                &model_provider_config,
                provider.provider_id,
                discovered_model_present,
            ) {
                // Operator-selected registry defaults come from `models set --allow-custom`;
                // keep them when ChatGPT live discovery is unavailable.
                preserve_provider_registry_default_model(document, provider, model_id.as_str())?;
            } else {
                apply_discovered_or_clear_text_model_selection(
                    document,
                    "model_provider.openai_model",
                    discovered_model_id,
                    true,
                )?;
                if !discovered_model_present {
                    write_pending_model_registry(document, provider)?;
                }
            }
        }
        OpenAiProviderSelectionRuntime::OpenAiCompatible
            if should_reset_openai_compatible_defaults(document, previous_auth_provider_kind) =>
        {
            set_string_value_at_path(
                document,
                "model_provider.openai_base_url",
                openai_compatible_base_url.as_str(),
            )?;
            apply_discovered_or_clear_text_model_selection(
                document,
                "model_provider.openai_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    document,
                    PendingModelRegistryProvider {
                        provider_id: "openai-primary",
                        display_name: "OpenAI",
                        kind: "openai_compatible",
                        base_url: openai_compatible_base_url.as_str(),
                        auth_provider_kind: "openai",
                    },
                )?;
            }
        }
        OpenAiProviderSelectionRuntime::OpenAiCompatible => {
            ensure_string_value_at_path(
                document,
                "model_provider.openai_base_url",
                openai_compatible_base_url.as_str(),
            )?;
            apply_discovered_or_clear_text_model_selection(
                document,
                "model_provider.openai_model",
                discovered_model_id,
                true,
            )?;
            if !discovered_model_present {
                write_pending_model_registry(
                    document,
                    PendingModelRegistryProvider {
                        provider_id: "openai-primary",
                        display_name: "OpenAI",
                        kind: "openai_compatible",
                        base_url: openai_compatible_base_url.as_str(),
                        auth_provider_kind: "openai",
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn should_reset_openai_compatible_defaults(
    document: &toml::Value,
    previous_auth_provider_kind: Option<ModelProviderAuthProviderKind>,
) -> bool {
    previous_auth_provider_kind.is_some_and(|kind| kind != ModelProviderAuthProviderKind::Openai)
        || openai_base_url_matches_stale_provider_default(document)
}

fn openai_base_url_matches_stale_provider_default(document: &toml::Value) -> bool {
    let Some(base_url) = document_string_value_at_path(document, "model_provider.openai_base_url")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    [
        XAI_DEFAULT_BASE_URL,
        GOOGLE_GEMINI_OPENAI_BASE_URL,
        OPENROUTER_DEFAULT_BASE_URL,
        OPENAI_CHATGPT_CODEX_BASE_URL,
    ]
    .iter()
    .any(|known| base_url.eq_ignore_ascii_case(known))
}

fn model_provider_auth_provider_kind_from_document(
    document: &toml::Value,
) -> Option<ModelProviderAuthProviderKind> {
    document_string_value_at_path(document, "model_provider.auth_provider_kind")
        .and_then(|value| ModelProviderAuthProviderKind::parse(value).ok())
}

fn document_string_value_at_path<'a>(document: &'a toml::Value, path: &str) -> Option<&'a str> {
    get_value_at_path(document, path).ok().flatten().and_then(toml::Value::as_str)
}

#[allow(clippy::result_large_err)]
fn file_model_provider_config_from_document(
    document: &toml::Value,
) -> Result<FileModelProviderConfig, Response> {
    let Some(model_provider) = get_value_at_path(document, "model_provider").ok().flatten() else {
        return Ok(FileModelProviderConfig::default());
    };
    model_provider.clone().try_into().map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "invalid model_provider config schema: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
fn apply_discovered_or_clear_text_model_selection(
    document: &mut toml::Value,
    model_path: &str,
    discovered_model_id: Option<&str>,
    clear_when_missing: bool,
) -> Result<(), Response> {
    if let Some(model_id) = discovered_model_id.and_then(normalize_optional_text).map(str::to_owned)
    {
        clear_pending_model_registry(document)?;
        unset_model_value_at_path(document, "model_provider.default_chat_model_id")?;
        return set_string_value_at_path(document, model_path, model_id.as_str());
    }
    if clear_when_missing {
        clear_text_model_selection_at_path(document, model_path)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct PendingModelRegistryProvider<'a> {
    provider_id: &'a str,
    display_name: &'a str,
    kind: &'a str,
    base_url: &'a str,
    auth_provider_kind: &'a str,
}

#[allow(clippy::result_large_err)]
fn set_single_model_registry_provider(
    document: &mut toml::Value,
    provider: PendingModelRegistryProvider<'_>,
) -> Result<(), Response> {
    let mut table = toml::map::Map::new();
    table.insert("provider_id".to_owned(), toml::Value::String(provider.provider_id.to_owned()));
    table.insert("display_name".to_owned(), toml::Value::String(provider.display_name.to_owned()));
    table.insert("kind".to_owned(), toml::Value::String(provider.kind.to_owned()));
    table.insert("base_url".to_owned(), toml::Value::String(provider.base_url.to_owned()));
    table.insert(
        "auth_provider_kind".to_owned(),
        toml::Value::String(provider.auth_provider_kind.to_owned()),
    );
    table.insert("enabled".to_owned(), toml::Value::Boolean(true));
    set_value_at_path(
        document,
        "model_provider.providers",
        toml::Value::Array(vec![toml::Value::Table(table)]),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to set model_provider.providers: {error}"
        )))
    })
}

#[allow(clippy::result_large_err)]
fn write_pending_model_registry(
    document: &mut toml::Value,
    provider: PendingModelRegistryProvider<'_>,
) -> Result<(), Response> {
    set_single_model_registry_provider(document, provider)?;
    unset_model_value_at_path(document, "model_provider.models")?;
    unset_model_value_at_path(document, "model_provider.default_chat_model_id")
}

#[allow(clippy::result_large_err)]
fn preserve_provider_registry_default_model(
    document: &mut toml::Value,
    provider: PendingModelRegistryProvider<'_>,
    model_id: &str,
) -> Result<(), Response> {
    set_single_model_registry_provider(document, provider)?;
    unset_model_value_at_path(document, "model_provider.models")?;
    set_string_value_at_path(document, "model_provider.default_chat_model_id", model_id)?;
    unset_model_value_at_path(document, "model_provider.openai_model")
}

#[allow(clippy::result_large_err)]
fn clear_pending_model_registry(document: &mut toml::Value) -> Result<(), Response> {
    unset_model_value_at_path(document, "model_provider.providers")?;
    unset_model_value_at_path(document, "model_provider.models")
}

#[allow(clippy::result_large_err)]
fn clear_text_model_selection_at_path(
    document: &mut toml::Value,
    model_path: &str,
) -> Result<(), Response> {
    unset_model_value_at_path(document, model_path)?;
    unset_model_value_at_path(document, "model_provider.default_chat_model_id")
}

#[allow(clippy::result_large_err)]
fn unset_model_value_at_path(document: &mut toml::Value, path: &str) -> Result<(), Response> {
    unset_value_at_path(document, path).map(|_| ()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to unset {path}: {error}"
        )))
    })
}

/// Sets `path` to `default_value` only when it is currently missing or blank
/// (the keep-operator-overrides counterpart of [`set_string_value_at_path`]).
#[allow(clippy::result_large_err)]
fn ensure_string_value_at_path(
    document: &mut toml::Value,
    path: &str,
    default_value: &str,
) -> Result<(), Response> {
    let existing = get_value_at_path(document, path)
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if existing.is_some() {
        return Ok(());
    }
    set_value_at_path(document, path, toml::Value::String(default_value.to_owned())).map_err(
        |error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "failed to set {path}: {error}"
            )))
        },
    )
}

#[allow(clippy::result_large_err)]
fn set_string_value_at_path(
    document: &mut toml::Value,
    path: &str,
    value: &str,
) -> Result<(), Response> {
    set_value_at_path(document, path, toml::Value::String(value.to_owned())).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to set {path}: {error}"
        )))
    })
}

/// Validation outcome classification shared by the Anthropic and MiniMax
/// probes; `Unexpected` may carry a raw provider body, which the `map_*`
/// response builders sanitize before it can reach a client or log.
enum AnthropicCredentialValidationError {
    InvalidCredential,
    RateLimited,
    ProviderUnavailable,
    Unexpected(String),
}

/// Probes `GET {base}/v1/models` with the Anthropic headers to confirm the
/// key is accepted; only `ProviderUnavailable` outcomes (5xx, timeouts) are
/// retried, since other statuses are deterministic answers.
#[allow(clippy::result_large_err)]
async fn validate_anthropic_api_key(
    base_url: &str,
    api_key: &str,
    timeout: Duration,
) -> Result<(), AnthropicCredentialValidationError> {
    let base = base_url.trim().trim_end_matches('/');
    let endpoint =
        if base.ends_with("/v1") { format!("{base}/models") } else { format!("{base}/v1/models") };
    let client = ReqwestClient::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| AnthropicCredentialValidationError::Unexpected(error.to_string()))?;

    for attempt in 1..=ANTHROPIC_VALIDATION_RETRY_ATTEMPTS {
        let response = client
            .get(endpoint.as_str())
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_API_VERSION)
            .send()
            .await;

        let outcome = match response {
            Ok(response) => match response.status() {
                StatusCode::OK => Ok(()),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    Err(AnthropicCredentialValidationError::InvalidCredential)
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    Err(AnthropicCredentialValidationError::RateLimited)
                }
                status if status.is_server_error() => {
                    Err(AnthropicCredentialValidationError::ProviderUnavailable)
                }
                status => {
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<anthropic error body unavailable>".to_owned());
                    Err(AnthropicCredentialValidationError::Unexpected(format!(
                        "Anthropic endpoint returned HTTP {status}: {body}"
                    )))
                }
            },
            Err(error) => {
                if error.is_timeout() {
                    Err(AnthropicCredentialValidationError::ProviderUnavailable)
                } else {
                    Err(AnthropicCredentialValidationError::Unexpected(error.to_string()))
                }
            }
        };

        match outcome {
            Ok(()) => return Ok(()),
            Err(AnthropicCredentialValidationError::ProviderUnavailable)
                if attempt < ANTHROPIC_VALIDATION_RETRY_ATTEMPTS =>
            {
                tokio::time::sleep(ANTHROPIC_VALIDATION_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }

    Err(AnthropicCredentialValidationError::ProviderUnavailable)
}

/// Validates a MiniMax key by reading the provider-advertised model list and
/// returns the preferred chat model id. The caller persists only this
/// discovered id; no local model name is used as a fallback.
#[allow(clippy::result_large_err)]
async fn discover_minimax_api_key_model_id(
    base_url: &str,
    api_key: &str,
    timeout: Duration,
) -> Result<String, AnthropicCredentialValidationError> {
    for attempt in 1..=ANTHROPIC_VALIDATION_RETRY_ATTEMPTS {
        let outcome =
            discover_preferred_openai_compatible_model_id(base_url, api_key, timeout).await;
        match outcome {
            Ok(Some(model_id)) => return Ok(model_id),
            Ok(None) => {
                return Err(AnthropicCredentialValidationError::Unexpected(
                    "MiniMax model discovery returned no selectable models".to_owned(),
                ))
            }
            Err(OpenAiCredentialValidationError::ProviderUnavailable)
                if attempt < ANTHROPIC_VALIDATION_RETRY_ATTEMPTS =>
            {
                tokio::time::sleep(ANTHROPIC_VALIDATION_RETRY_DELAY).await;
            }
            Err(error) => return Err(map_minimax_discovery_error(error)),
        }
    }

    Err(AnthropicCredentialValidationError::ProviderUnavailable)
}

fn map_minimax_discovery_error(
    error: OpenAiCredentialValidationError,
) -> AnthropicCredentialValidationError {
    match error {
        OpenAiCredentialValidationError::InvalidCredential => {
            AnthropicCredentialValidationError::InvalidCredential
        }
        OpenAiCredentialValidationError::RateLimited => {
            AnthropicCredentialValidationError::RateLimited
        }
        OpenAiCredentialValidationError::ProviderUnavailable => {
            AnthropicCredentialValidationError::ProviderUnavailable
        }
        OpenAiCredentialValidationError::Unexpected(message) => {
            AnthropicCredentialValidationError::Unexpected(message)
        }
    }
}

fn map_anthropic_validation_error(
    field: &str,
    error: AnthropicCredentialValidationError,
) -> Response {
    match error {
        AnthropicCredentialValidationError::InvalidCredential => validation_error_response(
            field,
            "invalid_credential",
            "Anthropic credential is invalid or does not have the required access.",
        ),
        AnthropicCredentialValidationError::RateLimited => runtime_status_response(
            tonic::Status::resource_exhausted("Anthropic credential validation was rate limited"),
        ),
        AnthropicCredentialValidationError::ProviderUnavailable => {
            runtime_status_response(tonic::Status::unavailable(
                "Anthropic credential validation is temporarily unavailable",
            ))
        }
        AnthropicCredentialValidationError::Unexpected(message) => {
            runtime_status_response(tonic::Status::internal(format!(
                "Anthropic credential validation failed: {}",
                sanitize_http_error_message(message.as_str())
            )))
        }
    }
}

fn map_minimax_validation_error(
    field: &str,
    error: AnthropicCredentialValidationError,
) -> Response {
    match error {
        AnthropicCredentialValidationError::InvalidCredential => validation_error_response(
            field,
            "invalid_credential",
            "MiniMax credential is invalid or does not have the required access.",
        ),
        AnthropicCredentialValidationError::RateLimited => runtime_status_response(
            tonic::Status::resource_exhausted("MiniMax credential validation was rate limited"),
        ),
        AnthropicCredentialValidationError::ProviderUnavailable => runtime_status_response(
            tonic::Status::unavailable("MiniMax credential validation is temporarily unavailable"),
        ),
        AnthropicCredentialValidationError::Unexpected(message) => {
            runtime_status_response(tonic::Status::internal(format!(
                "MiniMax credential validation failed: {}",
                sanitize_http_error_message(message.as_str())
            )))
        }
    }
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

/// Validates a configured callback base URL (https, host present, no
/// credentials/query/fragment) and normalizes it to a trailing slash so
/// `Url::join` keeps the full path.
#[allow(clippy::result_large_err)]
fn normalize_openai_callback_base_url(raw: &str, source_name: &str) -> Result<Url, Response> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} cannot be empty"
        ))));
    }
    let mut parsed = Url::parse(trimmed).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} must be a valid absolute URL: {error}"
        )))
    })?;
    if parsed.scheme() != "https" {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} must use https://"
        ))));
    }
    if parsed.host_str().is_none() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} must include a host"
        ))));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} must not include embedded credentials"
        ))));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "{source_name} must not include query or fragment"
        ))));
    }
    if !parsed.path().ends_with('/') {
        parsed.set_path(format!("{}/", parsed.path()).as_str());
    }
    Ok(parsed)
}

/// Derives an `http://` callback origin from the request's Host header,
/// accepting only bare loopback origins.
///
/// The redirect URI is security-sensitive (the provider sends the
/// authorization code there), so a Host header is never trusted beyond
/// loopback: any forwarded-* header means a proxy is in front and the
/// operator must configure `gateway_access.remote_base_url` explicitly
/// instead of letting a spoofable header pick the callback host.
#[allow(clippy::result_large_err)]
fn openai_loopback_callback_base_url(headers: &HeaderMap) -> Result<Url, Response> {
    if headers.contains_key("x-forwarded-host")
        || headers.contains_key("x-forwarded-proto")
        || headers.contains_key("x-forwarded-port")
    {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "gateway_access.remote_base_url is required when forwarded host headers are present",
        )));
    }
    let host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::failed_precondition(
                "host header is required for OpenAI OAuth callback URLs",
            ))
        })?;
    let parsed = Url::parse(format!("http://{host}").as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "host header is not a valid loopback origin: {error}"
        )))
    })?;
    let Some(host_name) = parsed.host_str() else {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "host header must include a loopback host",
        )));
    };
    let loopback_host = host_name.eq_ignore_ascii_case("localhost")
        || host_name.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if !loopback_host {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "gateway_access.remote_base_url is required for non-loopback OpenAI OAuth callback hosts",
        )));
    }
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "host header must describe a bare loopback origin",
        )));
    }
    Ok(parsed)
}

/// Builds the OAuth redirect URI: the configured
/// `gateway_access.remote_base_url` wins, otherwise the loopback origin from
/// the Host header is used for local flows.
#[allow(clippy::result_large_err)]
fn build_openai_oauth_callback_url(
    document: Option<&toml::Value>,
    headers: &HeaderMap,
) -> Result<String, Response> {
    let base_url =
        if let Some(remote_base_url) = document.and_then(openai_callback_base_url_from_document) {
            normalize_openai_callback_base_url(
                remote_base_url.as_str(),
                "gateway_access.remote_base_url",
            )?
        } else {
            openai_loopback_callback_base_url(headers)?
        };
    base_url.join(OPENAI_OAUTH_CALLBACK_PATH).map(|url| url.to_string()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to build OpenAI OAuth callback URL: {error}"
        )))
    })
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

/// Prunes stale attempts from the shared map. Both pending and terminal
/// attempts stay addressable for one extra TTL window (past expiry or
/// completion respectively) so a polling UI can still read the final state
/// before the entry disappears.
fn cleanup_openai_oauth_attempts(attempts: &mut HashMap<String, OpenAiOAuthAttempt>, now: i64) {
    attempts.retain(|_, attempt| match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Pending { .. }
        | OpenAiOAuthAttemptStateRecord::Completing { .. } => {
            attempt.expires_at_unix_ms.saturating_add(OPENAI_OAUTH_ATTEMPT_TTL_MS) >= now
        }
        OpenAiOAuthAttemptStateRecord::Succeeded { completed_at_unix_ms, .. }
        | OpenAiOAuthAttemptStateRecord::Failed { completed_at_unix_ms, .. } => {
            completed_at_unix_ms.saturating_add(OPENAI_OAUTH_ATTEMPT_TTL_MS) >= now
        }
    });
}

fn oauth_callback_state_envelope(
    attempt: &OpenAiOAuthAttempt,
) -> control_plane::OpenAiOAuthCallbackStateEnvelope {
    let provider = attempt.provider.as_str().to_owned();
    match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Pending { message }
        | OpenAiOAuthAttemptStateRecord::Completing { message, .. } => {
            control_plane::OpenAiOAuthCallbackStateEnvelope {
                contract: contract_descriptor(),
                provider,
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
                provider,
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
                provider,
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

/// Serializes the JSON payload the rendered callback page posts to
/// `window.opener` so the console UI can finish the flow without polling.
fn callback_payload_json(
    attempt: &OpenAiOAuthAttempt,
    override_message: Option<&str>,
    override_completed_at_unix_ms: Option<i64>,
) -> String {
    let envelope = oauth_callback_state_envelope(attempt);
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

/// Returns the (title, body) for the callback page when the attempt is
/// already terminal, flipping an expired pending attempt to failed as a side
/// effect; `None` means the attempt is still live and the callback should
/// proceed with the code exchange.
fn callback_terminal_page(attempt: &mut OpenAiOAuthAttempt, now: i64) -> Option<(String, String)> {
    match &attempt.state {
        OpenAiOAuthAttemptStateRecord::Succeeded { message, .. } => {
            Some(("OpenAI Connected".to_owned(), message.clone()))
        }
        OpenAiOAuthAttemptStateRecord::Failed { message, .. } => {
            Some(("OpenAI Connection Failed".to_owned(), message.clone()))
        }
        OpenAiOAuthAttemptStateRecord::Pending { .. }
        | OpenAiOAuthAttemptStateRecord::Completing { .. }
            if attempt.expires_at_unix_ms <= now =>
        {
            let message = "OpenAI OAuth attempt expired before the callback completed.".to_owned();
            attempt.state = OpenAiOAuthAttemptStateRecord::Failed {
                message: message.clone(),
                completed_at_unix_ms: now,
            };
            Some(("OpenAI Connection Failed".to_owned(), message))
        }
        OpenAiOAuthAttemptStateRecord::Completing { message, .. } => {
            Some(("OpenAI Connection In Progress".to_owned(), message.clone()))
        }
        OpenAiOAuthAttemptStateRecord::Pending { .. } => None,
    }
}

/// Marks an attempt failed with a sanitized message and renders the failure
/// page for the browser; used for every recoverable callback failure so the
/// user always gets a page instead of a bare error response.
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
    let body = oauth_callback_state_envelope(attempt).message;
    Ok(render_callback_page("OpenAI Connection Failed", body.as_str(), Some(payload.as_str())))
}

#[cfg(test)]
mod tests {
    use axum::http::{header::HOST, HeaderMap, HeaderValue, StatusCode};

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
    fn normalize_anthropic_oauth_token_endpoint_accepts_trusted_hosts() {
        let console = normalize_anthropic_oauth_token_endpoint(
            "https://console.anthropic.com/v1/oauth/token",
        )
        .expect("console Anthropic token endpoint should be trusted");
        let platform =
            normalize_anthropic_oauth_token_endpoint("https://platform.claude.com/v1/oauth/token")
                .expect("platform Claude token endpoint should be trusted");

        assert_eq!(console, "https://console.anthropic.com/v1/oauth/token");
        assert_eq!(platform, "https://platform.claude.com/v1/oauth/token");
    }

    #[test]
    fn normalize_anthropic_oauth_token_endpoint_rejects_untrusted_targets() {
        let hostile = normalize_anthropic_oauth_token_endpoint("https://attacker.example/token")
            .expect_err("untrusted host should be rejected");
        let embedded_credentials = normalize_anthropic_oauth_token_endpoint(
            "https://user:secret@console.anthropic.com/v1/oauth/token",
        )
        .expect_err("embedded credentials should be rejected");

        assert_eq!(hostile.status(), StatusCode::BAD_REQUEST);
        assert_eq!(embedded_credentials.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn normalize_xai_oauth_token_endpoint_accepts_trusted_hosts() {
        let root = normalize_xai_oauth_token_endpoint("https://x.ai/oauth/token")
            .expect("xAI root host should be trusted");
        let auth = normalize_xai_oauth_token_endpoint("https://auth.x.ai/oauth/token")
            .expect("xAI auth host should be trusted");
        let accounts = normalize_xai_oauth_token_endpoint("https://accounts.x.ai/oauth/token")
            .expect("xAI subdomains should be trusted");

        assert_eq!(root, "https://x.ai/oauth/token");
        assert_eq!(auth, "https://auth.x.ai/oauth/token");
        assert_eq!(accounts, "https://accounts.x.ai/oauth/token");
    }

    #[test]
    fn normalize_xai_oauth_token_endpoint_rejects_untrusted_targets() {
        let hostile = normalize_xai_oauth_token_endpoint("https://attacker.example/token")
            .expect_err("untrusted host should be rejected");
        let lookalike = normalize_xai_oauth_token_endpoint("https://evilx.ai/token")
            .expect_err("lookalike host should be rejected");
        let embedded_credentials =
            normalize_xai_oauth_token_endpoint("https://user:secret@auth.x.ai/oauth/token")
                .expect_err("embedded credentials should be rejected");

        assert_eq!(hostile.status(), StatusCode::BAD_REQUEST);
        assert_eq!(lookalike.status(), StatusCode::BAD_REQUEST);
        assert_eq!(embedded_credentials.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn build_openai_oauth_callback_url_prefers_configured_remote_base_url() {
        let document = toml::from_str::<toml::Value>(
            r#"
            [gateway_access]
            remote_base_url = "https://console.example.test/palyra"
            "#,
        )
        .expect("gateway_access config should parse");
        let callback_url = build_openai_oauth_callback_url(Some(&document), &HeaderMap::new())
            .expect("configured remote base URL should build callback URL");
        assert_eq!(
            callback_url,
            "https://console.example.test/palyra/console/v1/auth/providers/openai/callback"
        );
    }

    #[test]
    fn build_openai_oauth_callback_url_accepts_loopback_host_without_forwarding() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("127.0.0.1:7142"));
        let callback_url = build_openai_oauth_callback_url(None, &headers)
            .expect("loopback host should remain valid for local OAuth flows");
        assert_eq!(callback_url, "http://127.0.0.1:7142/console/v1/auth/providers/openai/callback");
    }

    #[test]
    fn build_openai_oauth_callback_url_rejects_forwarded_host_without_trusted_base_url() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("127.0.0.1:7142"));
        headers.insert("x-forwarded-host", HeaderValue::from_static("evil.example"));
        let response = build_openai_oauth_callback_url(None, &headers)
            .expect_err("forwarded host should require configured remote base URL");
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    }

    #[test]
    fn anthropic_validation_base_url_prefers_configured_document_without_env() {
        let document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            anthropic_base_url = "https://enterprise-anthropic.example.test/anthropic"
            "#,
        )
        .expect("model provider config should parse");

        assert_eq!(
            load_anthropic_validation_base_url_with_env(None, Some(&document)),
            "https://enterprise-anthropic.example.test/anthropic"
        );
        assert_eq!(
            load_anthropic_validation_base_url_with_env(
                Some("https://env-anthropic.example.test"),
                Some(&document),
            ),
            "https://env-anthropic.example.test"
        );
    }

    #[test]
    fn openai_callback_state_envelope_reports_terminal_state() {
        let attempt = OpenAiOAuthAttempt {
            provider: ModelProviderAuthProviderKind::Openai,
            attempt_id: "attempt-1".to_owned(),
            expires_at_unix_ms: 10,
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            profile_id: "openai-default".to_owned(),
            profile_name: "OpenAI Default".to_owned(),
            scope: default_openai_profile_scope(),
            client_id: "client".to_owned(),
            client_secret: String::new(),
            scopes: vec!["openid".to_owned()],
            token_endpoint: Url::parse("https://auth.openai.com/oauth/token")
                .expect("token endpoint URL should parse"),
            code_verifier: "verifier".to_owned(),
            device_user_code: None,
            device_auth_id: None,
            poll_interval_ms: 0,
            next_poll_after_unix_ms: 0,
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
        let envelope = oauth_callback_state_envelope(&attempt);
        assert_eq!(envelope.state, "succeeded");
        assert_eq!(envelope.profile_id.as_deref(), Some("openai-default"));
        assert_eq!(envelope.completed_at_unix_ms, Some(11));
        assert_eq!(envelope.expires_at_unix_ms, None);
    }

    #[test]
    fn callback_terminal_page_marks_expired_pending_attempt_failed() {
        let mut attempt = OpenAiOAuthAttempt {
            provider: ModelProviderAuthProviderKind::Openai,
            attempt_id: "attempt-2".to_owned(),
            expires_at_unix_ms: 100,
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            profile_id: "openai-default".to_owned(),
            profile_name: "OpenAI Default".to_owned(),
            scope: default_openai_profile_scope(),
            client_id: "client".to_owned(),
            client_secret: String::new(),
            scopes: vec!["openid".to_owned()],
            token_endpoint: Url::parse("https://auth.openai.com/oauth/token")
                .expect("token endpoint URL should parse"),
            code_verifier: "verifier".to_owned(),
            device_user_code: None,
            device_auth_id: None,
            poll_interval_ms: 0,
            next_poll_after_unix_ms: 0,
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
    fn callback_terminal_page_reports_completing_attempt_as_in_progress() {
        let mut attempt = test_openai_oauth_attempt(
            OpenAiOAuthAttemptStateRecord::Completing {
                message: "OpenAI OAuth callback is being completed.".to_owned(),
            },
            100,
        );

        let rendered = callback_terminal_page(&mut attempt, 50)
            .expect("completing attempt should render an in-progress page");
        assert_eq!(rendered.0, "OpenAI Connection In Progress");
        assert!(rendered.1.contains("being completed"));
        assert!(matches!(attempt.state, OpenAiOAuthAttemptStateRecord::Completing { .. }));
        let envelope = oauth_callback_state_envelope(&attempt);
        assert_eq!(envelope.state, "pending");
        assert_eq!(envelope.expires_at_unix_ms, Some(100));
    }

    #[test]
    fn callback_terminal_page_marks_expired_completing_attempt_failed() {
        let mut attempt = test_openai_oauth_attempt(
            OpenAiOAuthAttemptStateRecord::Completing {
                message: "OpenAI OAuth callback is being completed.".to_owned(),
            },
            100,
        );

        let rendered = callback_terminal_page(&mut attempt, 101)
            .expect("expired completing attempt should render a failed page");
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

    #[test]
    fn openai_validation_base_url_ignores_stale_chatgpt_codex_config() {
        let document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "openai"
            openai_base_url = "https://chatgpt.com/backend-api/codex"
            "#,
        )
        .expect("model provider config should parse");

        assert_eq!(
            load_openai_validation_base_url_with_env(None, Some(&document)),
            OPENAI_DEFAULT_BASE_URL
        );
        assert_eq!(
            load_openai_validation_base_url_with_env(
                Some("http://127.0.0.1:9911/v1"),
                Some(&document)
            ),
            "http://127.0.0.1:9911/v1"
        );
    }

    #[test]
    fn normalize_openai_chatgpt_poll_interval_accepts_number_and_string() {
        assert_eq!(normalize_openai_chatgpt_poll_interval_ms(Some(json!(2))), 3_000);
        assert_eq!(normalize_openai_chatgpt_poll_interval_ms(Some(json!("7"))), 7_000);
        assert_eq!(normalize_openai_chatgpt_poll_interval_ms(Some(json!(120))), 60_000);
        assert_eq!(normalize_openai_chatgpt_poll_interval_ms(None), 5_000);
    }

    #[test]
    fn openai_selection_defaults_force_codex_for_chatgpt_runtime() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "xai"
            openai_base_url = "https://api.x.ai/v1"
            openai_model = "stale-provider-model"
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Xai),
            OpenAiProviderSelectionRuntime::ChatGptCodex,
            None,
        )
        .expect("ChatGPT selection defaults should apply");

        assert_eq!(
            document_string_value_at_path(&document, "model_provider.openai_base_url"),
            Some(OPENAI_CHATGPT_CODEX_BASE_URL)
        );
        assert_eq!(document_string_value_at_path(&document, "model_provider.openai_model"), None);
    }

    #[test]
    fn openai_selection_defaults_reset_non_openai_provider_endpoint() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "openai"
            openai_base_url = "https://api.x.ai/v1"
            openai_model = "stale-provider-model"
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Openai),
            OpenAiProviderSelectionRuntime::OpenAiCompatible,
            None,
        )
        .expect("OpenAI compatible selection defaults should apply");

        assert_eq!(
            document_string_value_at_path(&document, "model_provider.openai_base_url"),
            Some(OPENAI_DEFAULT_BASE_URL)
        );
        assert_eq!(document_string_value_at_path(&document, "model_provider.openai_model"), None);
    }

    #[test]
    fn openai_selection_defaults_reset_chatgpt_codex_endpoint_for_openai_compatible_profile() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "openai"
            openai_base_url = "https://chatgpt.com/backend-api/codex"
            openai_model = "stale-chatgpt-model"
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Openai),
            OpenAiProviderSelectionRuntime::OpenAiCompatible,
            None,
        )
        .expect("OpenAI compatible selection defaults should apply");

        assert_eq!(
            document_string_value_at_path(&document, "model_provider.openai_base_url"),
            Some(OPENAI_DEFAULT_BASE_URL)
        );
        assert_eq!(document_string_value_at_path(&document, "model_provider.openai_model"), None);
    }

    #[test]
    fn openai_selection_defaults_clear_same_provider_stale_model_without_discovery() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "openai"
            openai_base_url = "https://api.openai.com/v1"
            openai_model = "stale-provider-model"
            default_chat_model_id = "stale-provider-model"
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Openai),
            OpenAiProviderSelectionRuntime::OpenAiCompatible,
            None,
        )
        .expect("OpenAI compatible selection defaults should apply");

        assert_eq!(document_string_value_at_path(&document, "model_provider.openai_model"), None);
        assert_eq!(
            document_string_value_at_path(&document, "model_provider.default_chat_model_id"),
            None
        );
        assert!(
            get_value_at_path(&document, "model_provider.providers")
                .ok()
                .flatten()
                .and_then(toml::Value::as_array)
                .is_some_and(|providers| !providers.is_empty()),
            "missing discovery should leave an explicit pending provider registry"
        );
    }

    #[test]
    fn chatgpt_selection_defaults_preserve_registry_default_model_without_discovery() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            kind = "openai_compatible"
            auth_provider_kind = "openai"
            openai_base_url = "https://chatgpt.com/backend-api/codex"
            openai_model = "stale-provider-model"
            default_chat_model_id = "gpt-5.5"

            [[model_provider.providers]]
            provider_id = "openai-primary"
            display_name = "ChatGPT Login"
            kind = "openai_compatible"
            base_url = "https://chatgpt.com/backend-api/codex"
            auth_provider_kind = "openai"
            enabled = true
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Openai),
            OpenAiProviderSelectionRuntime::ChatGptCodex,
            None,
        )
        .expect("ChatGPT selection defaults should preserve custom registry default");

        assert_eq!(
            document_string_value_at_path(&document, "model_provider.openai_base_url"),
            Some(OPENAI_CHATGPT_CODEX_BASE_URL)
        );
        assert_eq!(document_string_value_at_path(&document, "model_provider.openai_model"), None);
        assert_eq!(
            document_string_value_at_path(&document, "model_provider.default_chat_model_id"),
            Some("gpt-5.5")
        );
        assert!(
            get_value_at_path(&document, "model_provider.providers")
                .ok()
                .flatten()
                .and_then(toml::Value::as_array)
                .is_some_and(|providers| providers.len() == 1),
            "selection should keep an explicit ChatGPT provider registry"
        );
    }

    #[test]
    fn openai_selection_defaults_store_discovered_provider_model() {
        let mut document = toml::from_str::<toml::Value>(
            r#"
            [model_provider]
            auth_provider_kind = "xai"
            openai_base_url = "https://api.x.ai/v1"
            openai_model = "stale-provider-model"
            default_chat_model_id = "stale-provider-model"
            "#,
        )
        .expect("model provider config should parse");

        apply_openai_provider_selection_defaults(
            &mut document,
            Some(ModelProviderAuthProviderKind::Xai),
            OpenAiProviderSelectionRuntime::ChatGptCodex,
            Some("provider-discovered-model"),
        )
        .expect("discovered model selection should apply");

        assert_eq!(
            document_string_value_at_path(&document, "model_provider.openai_model"),
            Some("provider-discovered-model")
        );
        assert_eq!(
            document_string_value_at_path(&document, "model_provider.default_chat_model_id"),
            None
        );
    }

    fn test_openai_oauth_attempt(
        state: OpenAiOAuthAttemptStateRecord,
        expires_at_unix_ms: i64,
    ) -> OpenAiOAuthAttempt {
        OpenAiOAuthAttempt {
            provider: ModelProviderAuthProviderKind::Openai,
            attempt_id: "attempt-test".to_owned(),
            expires_at_unix_ms,
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            profile_id: "openai-default".to_owned(),
            profile_name: "OpenAI Default".to_owned(),
            scope: default_openai_profile_scope(),
            client_id: "client".to_owned(),
            client_secret: String::new(),
            scopes: vec!["openid".to_owned()],
            token_endpoint: Url::parse("https://auth.openai.com/oauth/token")
                .expect("token endpoint URL should parse"),
            code_verifier: "verifier".to_owned(),
            device_user_code: None,
            device_auth_id: None,
            poll_interval_ms: 0,
            next_poll_after_unix_ms: 0,
            set_default: false,
            context: ConsoleActionContext {
                principal: "admin:test".to_owned(),
                device_id: "device".to_owned(),
                channel: None,
            },
            state,
        }
    }
}
