//! OpenAI provider-auth bridge for the desktop app.
//!
//! The desktop UI uses this module to summarize provider-auth state, connect
//! API-key profiles, launch OAuth handoff, and trigger profile lifecycle
//! actions through the console control-plane API.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context, Result};
use palyra_control_plane::{self as control_plane, AuthCredentialView};
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};

use super::snapshot::{
    build_control_plane_client, ensure_console_session_with_cache,
    ensure_console_session_with_cached_csrf, loopback_url, sanitize_log_line, ActionResult,
};
use super::supervisor::ConsoleSessionCache;
use super::{normalize_optional_text, ControlCenter, RuntimeConfig};

const OPENAI_PROVIDER: &str = "openai";

/// Captured control-plane inputs required for OpenAI provider-auth calls.
#[derive(Debug, Clone)]
pub(crate) struct OpenAiControlPlaneInputs {
    pub(crate) runtime: RuntimeConfig,
    pub(crate) admin_token: String,
    pub(crate) http_client: Client,
    pub(crate) console_session_cache: Arc<Mutex<Option<ConsoleSessionCache>>>,
}

impl OpenAiControlPlaneInputs {
    /// Captures OpenAI control-plane inputs from the running control center.
    pub(crate) fn capture(control_center: &ControlCenter) -> Self {
        Self {
            runtime: control_center.runtime.clone(),
            admin_token: control_center.admin_token.clone(),
            http_client: control_center.http_client.clone(),
            console_session_cache: Arc::clone(&control_center.console_session_cache),
        }
    }
}

/// Provider-auth scope selected by the desktop UI.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenAiScopeInput {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) agent_id: Option<String>,
}

/// Request body for connecting or rotating an OpenAI API-key profile.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenAiApiKeyConnectRequest {
    #[serde(default)]
    pub(crate) profile_id: Option<String>,
    #[serde(default)]
    pub(crate) profile_name: String,
    #[serde(default)]
    pub(crate) scope: Option<OpenAiScopeInput>,
    #[serde(default)]
    pub(crate) api_key: String,
    #[serde(default)]
    pub(crate) set_default: bool,
}

/// Request body for starting OpenAI OAuth bootstrap.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenAiOAuthBootstrapRequest {
    #[serde(default)]
    pub(crate) profile_id: Option<String>,
    #[serde(default)]
    pub(crate) profile_name: Option<String>,
    #[serde(default)]
    pub(crate) scope: Option<OpenAiScopeInput>,
    #[serde(default)]
    pub(crate) client_id: Option<String>,
    #[serde(default)]
    pub(crate) client_secret: Option<String>,
    #[serde(default)]
    pub(crate) scopes_text: String,
    #[serde(default)]
    pub(crate) set_default: bool,
}

/// Request body for actions that target one OpenAI auth profile.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenAiProfileActionRequest {
    pub(crate) profile_id: String,
}

/// Request body for polling an OpenAI OAuth callback attempt.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenAiOAuthCallbackStateRequest {
    pub(crate) attempt_id: String,
}

/// Aggregate health counters for OpenAI auth profiles.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct OpenAiProfileHealthSummary {
    pub(crate) total: u64,
    pub(crate) ok: u64,
    pub(crate) expiring: u64,
    pub(crate) expired: u64,
    pub(crate) missing: u64,
    pub(crate) static_count: u64,
}

/// Provider refresh metrics for OpenAI OAuth profiles.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct OpenAiRefreshMetricsSnapshot {
    pub(crate) attempts: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
}

/// Refresh state for one OpenAI OAuth profile.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpenAiProfileRefreshStateSnapshot {
    pub(crate) failure_count: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) last_attempt_unix_ms: Option<i64>,
    pub(crate) last_success_unix_ms: Option<i64>,
    pub(crate) next_allowed_refresh_unix_ms: Option<i64>,
}

/// Desktop-safe OpenAI profile view.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpenAiProfileSnapshot {
    pub(crate) profile_id: String,
    pub(crate) profile_name: String,
    pub(crate) scope_kind: String,
    pub(crate) scope_label: String,
    pub(crate) agent_id: Option<String>,
    pub(crate) credential_type: String,
    pub(crate) health_state: String,
    pub(crate) health_reason: String,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) is_default: bool,
    pub(crate) scopes: Vec<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) refresh_state: Option<OpenAiProfileRefreshStateSnapshot>,
    pub(crate) can_reconnect: bool,
    pub(crate) can_refresh: bool,
    pub(crate) can_revoke: bool,
    pub(crate) can_set_default: bool,
    pub(crate) can_rotate_api_key: bool,
}

/// Full OpenAI provider-auth status snapshot returned to the desktop UI.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpenAiAuthStatusSnapshot {
    pub(crate) available: bool,
    pub(crate) badge_status: String,
    pub(crate) provider: String,
    pub(crate) provider_state: String,
    pub(crate) note: Option<String>,
    pub(crate) default_profile_id: Option<String>,
    pub(crate) oauth_supported: bool,
    pub(crate) bootstrap_supported: bool,
    pub(crate) callback_supported: bool,
    pub(crate) reconnect_supported: bool,
    pub(crate) revoke_supported: bool,
    pub(crate) default_selection_supported: bool,
    pub(crate) summary: OpenAiProfileHealthSummary,
    pub(crate) refresh_metrics: OpenAiRefreshMetricsSnapshot,
    pub(crate) profiles: Vec<OpenAiProfileSnapshot>,
}

impl OpenAiAuthStatusSnapshot {
    /// Builds an unavailable provider-auth snapshot with a sanitized message.
    pub(crate) fn unavailable(message: String) -> Self {
        Self {
            available: false,
            badge_status: "unknown".to_owned(),
            provider: OPENAI_PROVIDER.to_owned(),
            provider_state: "unavailable".to_owned(),
            note: Some(message),
            default_profile_id: None,
            oauth_supported: false,
            bootstrap_supported: false,
            callback_supported: false,
            reconnect_supported: false,
            revoke_supported: false,
            default_selection_supported: false,
            summary: OpenAiProfileHealthSummary::default(),
            refresh_metrics: OpenAiRefreshMetricsSnapshot::default(),
            profiles: Vec::new(),
        }
    }
}

/// Result returned after starting or reconnecting OpenAI OAuth.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpenAiOAuthLaunchResult {
    pub(crate) ok: bool,
    pub(crate) message: String,
    pub(crate) attempt_id: String,
    pub(crate) authorization_url: String,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) profile_id: Option<String>,
    pub(crate) browser_opened: bool,
}

impl OpenAiOAuthLaunchResult {
    /// Converts a control-plane OAuth bootstrap envelope into a desktop result.
    pub(crate) fn from_envelope(envelope: control_plane::OpenAiOAuthBootstrapEnvelope) -> Self {
        Self {
            ok: true,
            message: envelope.message,
            attempt_id: envelope.attempt_id,
            authorization_url: envelope.authorization_url,
            expires_at_unix_ms: envelope.expires_at_unix_ms,
            profile_id: envelope.profile_id,
            browser_opened: false,
        }
    }

    /// Marks that the desktop successfully opened the system browser.
    pub(crate) fn mark_browser_opened(mut self) -> Self {
        self.browser_opened = true;
        self.message = format!("{} Default browser handoff opened.", self.message);
        self
    }

    /// Appends a browser-open warning without failing OAuth bootstrap.
    pub(crate) fn mark_browser_pending(mut self, warning: &str) -> Self {
        self.message = format!("{} {warning}", self.message);
        self
    }
}

/// Poll result for an OpenAI OAuth callback attempt.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OpenAiOAuthCallbackStateSnapshot {
    pub(crate) provider: String,
    pub(crate) attempt_id: String,
    pub(crate) state: String,
    pub(crate) message: String,
    pub(crate) profile_id: Option<String>,
    pub(crate) completed_at_unix_ms: Option<i64>,
    pub(crate) expires_at_unix_ms: Option<i64>,
    pub(crate) is_terminal: bool,
}

impl OpenAiOAuthCallbackStateSnapshot {
    fn from_envelope(envelope: control_plane::OpenAiOAuthCallbackStateEnvelope) -> Self {
        let is_terminal = matches!(envelope.state.as_str(), "succeeded" | "failed");
        Self {
            provider: envelope.provider,
            attempt_id: envelope.attempt_id,
            state: envelope.state,
            message: envelope.message,
            profile_id: envelope.profile_id,
            completed_at_unix_ms: envelope.completed_at_unix_ms,
            expires_at_unix_ms: envelope.expires_at_unix_ms,
            is_terminal,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AuthHealthProfileValue {
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    profile_name: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    credential_type: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RefreshMetricsValue {
    #[serde(default)]
    attempts: u64,
    #[serde(default)]
    successes: u64,
    #[serde(default)]
    failures: u64,
    #[serde(default)]
    by_provider: Vec<ProviderRefreshMetricsValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ProviderRefreshMetricsValue {
    #[serde(default)]
    provider: String,
    #[serde(default)]
    attempts: u64,
    #[serde(default)]
    successes: u64,
    #[serde(default)]
    failures: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct OAuthRefreshStateValue {
    #[serde(default)]
    failure_count: u64,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    last_attempt_unix_ms: Option<i64>,
    #[serde(default)]
    last_success_unix_ms: Option<i64>,
    #[serde(default)]
    next_allowed_refresh_unix_ms: Option<i64>,
}

/// Loads OpenAI provider-auth status for the desktop UI.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, provider state lookup, auth health lookup, or profile listing
/// fails.
pub(crate) async fn load_openai_auth_status(
    inputs: OpenAiControlPlaneInputs,
) -> Result<OpenAiAuthStatusSnapshot> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    ensure_console_session_with_cache(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;

    let provider_state = control_plane
        .get_openai_provider_state()
        .await
        .map_err(|error| control_plane_error("fetch OpenAI provider state", error))?;
    let auth_health = control_plane
        .get_auth_health(true, None)
        .await
        .map_err(|error| control_plane_error("fetch OpenAI auth health", error))?;
    let profiles = control_plane
        .list_auth_profiles("provider_kind=openai&limit=100")
        .await
        .map_err(|error| control_plane_error("list OpenAI auth profiles", error))?;

    let mut health_by_id = auth_health
        .profiles
        .iter()
        .filter_map(parse_auth_health_profile)
        .filter(|profile| profile.provider.eq_ignore_ascii_case(OPENAI_PROVIDER))
        .map(|profile| (profile.profile_id.clone(), profile))
        .collect::<BTreeMap<_, _>>();

    let mut profile_snapshots = profiles
        .profiles
        .into_iter()
        .map(|profile| {
            let health = health_by_id.remove(profile.profile_id.as_str());
            build_profile_snapshot(
                profile,
                health,
                provider_state.default_profile_id.as_deref(),
                provider_state.reconnect_supported,
                provider_state.revoke_supported,
                provider_state.default_selection_supported,
            )
        })
        .collect::<Vec<_>>();

    for health in health_by_id.into_values() {
        profile_snapshots.push(build_orphan_profile_snapshot(
            health,
            provider_state.default_profile_id.as_deref(),
            provider_state.reconnect_supported,
            provider_state.revoke_supported,
            provider_state.default_selection_supported,
        ));
    }

    profile_snapshots.sort_by(|left, right| {
        right
            .is_default
            .cmp(&left.is_default)
            .then_with(|| right.updated_at_unix_ms.cmp(&left.updated_at_unix_ms))
            .then_with(|| left.profile_name.cmp(&right.profile_name))
    });

    let summary = summarize_profiles(profile_snapshots.as_slice());
    let refresh_metrics = parse_openai_refresh_metrics(&auth_health.refresh_metrics);
    let badge_status =
        derive_badge_status(provider_state.state.as_str(), profile_snapshots.as_slice(), &summary);

    Ok(OpenAiAuthStatusSnapshot {
        available: true,
        badge_status,
        provider: provider_state.provider,
        provider_state: provider_state.state,
        note: provider_state.note.map(|value| sanitize_log_line(value.as_str())),
        default_profile_id: provider_state.default_profile_id,
        oauth_supported: provider_state.oauth_supported,
        bootstrap_supported: provider_state.bootstrap_supported,
        callback_supported: provider_state.callback_supported,
        reconnect_supported: provider_state.reconnect_supported,
        revoke_supported: provider_state.revoke_supported,
        default_selection_supported: provider_state.default_selection_supported,
        summary,
        refresh_metrics,
        profiles: profile_snapshots,
    })
}

/// Connects or rotates an OpenAI API-key profile.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, scope normalization, or provider connection fails.
pub(crate) async fn connect_openai_api_key(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiApiKeyConnectRequest,
) -> Result<ActionResult> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    ensure_console_session_with_cache(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;

    let profile_name =
        normalize_optional_text(request.profile_name.as_str()).unwrap_or("OpenAI").to_owned();
    let response = control_plane
        .connect_openai_api_key(&control_plane::OpenAiApiKeyUpsertRequest {
            profile_id: normalize_optional_owned(request.profile_id),
            profile_name,
            scope: normalize_scope(request.scope)?,
            api_key: request.api_key,
            set_default: request.set_default,
        })
        .await
        .map_err(|error| control_plane_error("connect OpenAI API key", error))?;

    Ok(ActionResult { ok: true, message: response.message })
}

/// Starts OpenAI OAuth bootstrap and returns the authorization handoff payload.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, required OAuth credentials, scope normalization, or bootstrap
/// request execution fails.
pub(crate) async fn start_openai_oauth_bootstrap(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiOAuthBootstrapRequest,
) -> Result<OpenAiOAuthLaunchResult> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    ensure_console_session_with_cache(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;

    let client_id = normalize_optional_owned(request.client_id)
        .ok_or_else(|| anyhow!("OpenAI OAuth client_id is required"))?;
    let client_secret = normalize_optional_owned(request.client_secret)
        .ok_or_else(|| anyhow!("OpenAI OAuth client_secret is required"))?;

    let response = control_plane
        .start_openai_oauth_bootstrap(&control_plane::OpenAiOAuthBootstrapRequest {
            profile_id: normalize_optional_owned(request.profile_id),
            profile_name: normalize_optional_owned(request.profile_name),
            scope: Some(normalize_scope(request.scope)?),
            client_id: Some(client_id),
            client_secret: Some(client_secret),
            scopes: parse_scope_list(request.scopes_text.as_str()),
            set_default: request.set_default,
        })
        .await
        .map_err(|error| control_plane_error("start OpenAI OAuth bootstrap", error))?;

    Ok(OpenAiOAuthLaunchResult::from_envelope(response))
}

/// Starts an OpenAI OAuth reconnect flow for an existing profile.
///
/// # Errors
/// Returns an error when console session bootstrap, profile-id normalization,
/// HTTP dispatch, or response decoding fails.
pub(crate) async fn reconnect_openai_oauth(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiProfileActionRequest,
) -> Result<OpenAiOAuthLaunchResult> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    let csrf_token = ensure_console_session_with_cached_csrf(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;
    let profile_id = normalize_profile_id(request.profile_id.as_str())?;

    let response = post_console_json::<control_plane::OpenAiOAuthBootstrapEnvelope, _>(
        &inputs.http_client,
        &inputs.runtime,
        "/console/v1/auth/providers/openai/reconnect",
        csrf_token.as_str(),
        &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
    )
    .await?;

    Ok(OpenAiOAuthLaunchResult::from_envelope(response))
}

/// Loads callback state for an OpenAI OAuth bootstrap attempt.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, or callback-state lookup fails.
pub(crate) async fn get_openai_oauth_callback_state(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiOAuthCallbackStateRequest,
) -> Result<OpenAiOAuthCallbackStateSnapshot> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    ensure_console_session_with_cache(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;

    let envelope =
        control_plane
            .get_openai_oauth_callback_state(request.attempt_id.trim())
            .await
            .map_err(|error| control_plane_error("fetch OpenAI OAuth callback state", error))?;
    Ok(OpenAiOAuthCallbackStateSnapshot::from_envelope(envelope))
}

/// Refreshes one OpenAI OAuth profile.
///
/// # Errors
/// Returns an error when console session bootstrap, profile-id normalization,
/// or provider action execution fails.
pub(crate) async fn refresh_openai_profile(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiProfileActionRequest,
) -> Result<ActionResult> {
    run_provider_action(inputs, "refresh", request).await
}

/// Revokes one OpenAI auth profile.
///
/// # Errors
/// Returns an error when console session bootstrap, profile-id normalization,
/// or provider action execution fails.
pub(crate) async fn revoke_openai_profile(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiProfileActionRequest,
) -> Result<ActionResult> {
    run_provider_action(inputs, "revoke", request).await
}

/// Sets one OpenAI profile as the default profile.
///
/// # Errors
/// Returns an error when console session bootstrap, profile-id normalization,
/// or provider action execution fails.
pub(crate) async fn set_openai_default_profile(
    inputs: OpenAiControlPlaneInputs,
    request: OpenAiProfileActionRequest,
) -> Result<ActionResult> {
    run_provider_action(inputs, "default-profile", request).await
}

/// Opens a validated OpenAI browser handoff URL with the supplied opener.
///
/// # Errors
/// Returns an error when the URL is not absolute HTTP(S) or the opener fails.
pub(crate) fn open_external_browser<E>(
    raw_url: &str,
    opener: impl FnOnce(&str) -> std::result::Result<(), E>,
) -> Result<()>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let parsed = Url::parse(raw_url)
        .with_context(|| "desktop browser handoff requires a valid absolute URL".to_owned())?;
    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        bail!("desktop browser handoff only supports http:// and https:// URLs");
    }
    opener(parsed.as_str()).map_err(|error| {
        anyhow!(
            "failed to open OpenAI browser handoff: {}",
            sanitize_log_line(error.to_string().as_str())
        )
    })
}

async fn run_provider_action(
    inputs: OpenAiControlPlaneInputs,
    action: &str,
    request: OpenAiProfileActionRequest,
) -> Result<ActionResult> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    ensure_console_session_with_cache(
        &mut control_plane,
        inputs.admin_token.as_str(),
        inputs.console_session_cache.as_ref(),
    )
    .await?;
    let profile_id = normalize_profile_id(request.profile_id.as_str())?;

    let response = control_plane
        .run_openai_provider_action(
            action,
            &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
        )
        .await
        .map_err(|error| {
            control_plane_error(format!("run OpenAI provider action '{action}'").as_str(), error)
        })?;

    Ok(ActionResult { ok: true, message: response.message })
}

fn build_profile_snapshot(
    profile: control_plane::AuthProfileView,
    health: Option<AuthHealthProfileValue>,
    default_profile_id: Option<&str>,
    reconnect_supported: bool,
    revoke_supported: bool,
    default_selection_supported: bool,
) -> OpenAiProfileSnapshot {
    let is_default = default_profile_id.is_some_and(|value| value == profile.profile_id);
    let (credential_type, scopes, client_id, refresh_state) = match profile.credential {
        AuthCredentialView::ApiKey { .. } => ("api_key".to_owned(), Vec::new(), None, None),
        AuthCredentialView::Oauth { client_id, scopes, refresh_state, .. } => {
            ("oauth".to_owned(), scopes, client_id, Some(parse_refresh_state(refresh_state)))
        }
    };
    let health_state = health
        .as_ref()
        .map(|value| normalize_health_state(value.state.as_str()))
        .unwrap_or_else(|| "unknown".to_owned());
    let health_reason = health
        .as_ref()
        .map(|value| sanitize_log_line(value.reason.as_str()))
        .unwrap_or_else(|| "No health report received yet.".to_owned());
    let expires_at_unix_ms = health.and_then(|value| value.expires_at_unix_ms);
    let scope_label = format_scope_label(&profile.scope.kind, profile.scope.agent_id.as_deref());

    OpenAiProfileSnapshot {
        profile_id: profile.profile_id,
        profile_name: profile.profile_name,
        scope_kind: profile.scope.kind,
        scope_label,
        agent_id: profile.scope.agent_id,
        credential_type: credential_type.clone(),
        health_state,
        health_reason,
        expires_at_unix_ms,
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
        is_default,
        scopes,
        client_id,
        refresh_state,
        can_reconnect: credential_type == "oauth" && reconnect_supported,
        can_refresh: credential_type == "oauth",
        can_revoke: revoke_supported,
        can_set_default: default_selection_supported && !is_default,
        can_rotate_api_key: credential_type == "api_key",
    }
}

fn build_orphan_profile_snapshot(
    health: AuthHealthProfileValue,
    default_profile_id: Option<&str>,
    reconnect_supported: bool,
    revoke_supported: bool,
    default_selection_supported: bool,
) -> OpenAiProfileSnapshot {
    let is_default = default_profile_id.is_some_and(|value| value == health.profile_id);
    OpenAiProfileSnapshot {
        profile_id: health.profile_id.clone(),
        profile_name: if health.profile_name.trim().is_empty() {
            health.profile_id.clone()
        } else {
            health.profile_name.clone()
        },
        scope_kind: health.scope.clone(),
        scope_label: health.scope.clone(),
        agent_id: None,
        credential_type: health.credential_type.clone(),
        health_state: normalize_health_state(health.state.as_str()),
        health_reason: sanitize_log_line(health.reason.as_str()),
        expires_at_unix_ms: health.expires_at_unix_ms,
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
        is_default,
        scopes: Vec::new(),
        client_id: None,
        refresh_state: None,
        can_reconnect: health.credential_type.eq_ignore_ascii_case("oauth") && reconnect_supported,
        can_refresh: health.credential_type.eq_ignore_ascii_case("oauth"),
        can_revoke: revoke_supported,
        can_set_default: default_selection_supported && !is_default,
        can_rotate_api_key: health.credential_type.eq_ignore_ascii_case("api_key"),
    }
}

fn summarize_profiles(profiles: &[OpenAiProfileSnapshot]) -> OpenAiProfileHealthSummary {
    let mut summary = OpenAiProfileHealthSummary {
        total: profiles.len() as u64,
        ..OpenAiProfileHealthSummary::default()
    };
    for profile in profiles {
        match profile.health_state.as_str() {
            "ok" => summary.ok += 1,
            "expiring" => summary.expiring += 1,
            "expired" => summary.expired += 1,
            "missing" => summary.missing += 1,
            "static" => summary.static_count += 1,
            _ => {}
        }
    }
    summary
}

fn derive_badge_status(
    provider_state: &str,
    profiles: &[OpenAiProfileSnapshot],
    summary: &OpenAiProfileHealthSummary,
) -> String {
    if profiles.is_empty() {
        return if provider_state.trim().eq_ignore_ascii_case("connected") {
            "degraded".to_owned()
        } else {
            "unknown".to_owned()
        };
    }
    if summary.expired > 0 || summary.missing > 0 {
        return "degraded".to_owned();
    }
    if summary.expiring > 0 {
        return "degraded".to_owned();
    }
    if profiles.iter().all(|profile| matches!(profile.health_state.as_str(), "ok" | "static")) {
        return "healthy".to_owned();
    }
    "unknown".to_owned()
}

fn parse_auth_health_profile(value: &Value) -> Option<AuthHealthProfileValue> {
    let parsed = serde_json::from_value::<AuthHealthProfileValue>(value.clone()).ok()?;
    if parsed.profile_id.trim().is_empty() {
        None
    } else {
        Some(parsed)
    }
}

fn parse_openai_refresh_metrics(value: &Value) -> OpenAiRefreshMetricsSnapshot {
    let parsed = serde_json::from_value::<RefreshMetricsValue>(value.clone()).unwrap_or_default();
    let by_provider = parsed
        .by_provider
        .into_iter()
        .find(|entry| entry.provider.eq_ignore_ascii_case(OPENAI_PROVIDER));

    match by_provider {
        Some(entry) => OpenAiRefreshMetricsSnapshot {
            attempts: entry.attempts,
            successes: entry.successes,
            failures: entry.failures,
        },
        None => OpenAiRefreshMetricsSnapshot {
            attempts: parsed.attempts,
            successes: parsed.successes,
            failures: parsed.failures,
        },
    }
}

fn parse_refresh_state(value: Value) -> OpenAiProfileRefreshStateSnapshot {
    let parsed = serde_json::from_value::<OAuthRefreshStateValue>(value).unwrap_or_default();
    OpenAiProfileRefreshStateSnapshot {
        failure_count: parsed.failure_count,
        last_error: parsed.last_error.map(|value| sanitize_log_line(value.as_str())),
        last_attempt_unix_ms: parsed.last_attempt_unix_ms.filter(|value| *value > 0),
        last_success_unix_ms: parsed.last_success_unix_ms.filter(|value| *value > 0),
        next_allowed_refresh_unix_ms: parsed
            .next_allowed_refresh_unix_ms
            .filter(|value| *value > 0),
    }
}

fn normalize_scope(scope: Option<OpenAiScopeInput>) -> Result<control_plane::AuthProfileScope> {
    let scope = scope.unwrap_or(OpenAiScopeInput { kind: "global".to_owned(), agent_id: None });
    match scope.kind.trim().to_ascii_lowercase().as_str() {
        "global" | "" => {
            Ok(control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None })
        }
        "agent" => {
            let agent_id = normalize_optional_owned(scope.agent_id)
                .ok_or_else(|| anyhow!("OpenAI agent scope requires an agent_id"))?;
            Ok(control_plane::AuthProfileScope {
                kind: "agent".to_owned(),
                agent_id: Some(agent_id),
            })
        }
        other => bail!("unsupported OpenAI auth scope '{other}'"),
    }
}

fn parse_scope_list(raw: &str) -> Vec<String> {
    let mut scopes = Vec::new();
    for token in raw.split(|ch: char| ch == ',' || ch.is_whitespace()) {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        if scopes.iter().any(|existing: &String| existing == trimmed) {
            continue;
        }
        scopes.push(trimmed.to_owned());
    }
    scopes
}

fn normalize_optional_owned(raw: Option<String>) -> Option<String> {
    raw.and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
}

fn normalize_profile_id(raw: &str) -> Result<String> {
    normalize_optional_text(raw)
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("OpenAI profile_id is required"))
}

fn normalize_health_state(raw: &str) -> String {
    let lowered = raw.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        "unknown".to_owned()
    } else {
        lowered
    }
}

fn format_scope_label(kind: &str, agent_id: Option<&str>) -> String {
    match kind.trim().to_ascii_lowercase().as_str() {
        "agent" => match agent_id.and_then(normalize_optional_text) {
            Some(agent_id) => format!("agent:{agent_id}"),
            None => "agent".to_owned(),
        },
        "global" => "global".to_owned(),
        _ => kind.to_owned(),
    }
}

fn control_plane_error(
    operation: &str,
    error: control_plane::ControlPlaneClientError,
) -> anyhow::Error {
    anyhow!("{operation} failed: {}", sanitize_log_line(error.to_string().as_str()))
}

async fn post_console_json<T, B>(
    http_client: &Client,
    runtime: &RuntimeConfig,
    path: &str,
    csrf_token: &str,
    body: &B,
) -> Result<T>
where
    T: DeserializeOwned,
    B: Serialize + ?Sized,
{
    let url = loopback_url(runtime.gateway_admin_port, path)?;
    let response = http_client
        .post(url)
        .header("x-palyra-csrf-token", csrf_token)
        .json(body)
        .send()
        .await
        .with_context(|| format!("console POST request to {path} failed"))?;
    decode_console_response(path, response).await
}

async fn decode_console_response<T>(path: &str, response: reqwest::Response) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "console request {} failed with HTTP {}: {}",
            path,
            status.as_u16(),
            sanitize_log_line(body.as_str())
        );
    }
    response
        .json::<T>()
        .await
        .with_context(|| format!("failed to decode console response from {path}"))
}
