//! Discord onboarding control-plane client for the desktop app.
//!
//! The desktop command layer calls these helpers to authenticate a console
//! session, POST onboarding requests, and sanitize returned status details
//! before they reach the UI.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::snapshot::{
    build_control_plane_client, ensure_console_session_with_csrf, loopback_url, sanitize_log_line,
};
use crate::{normalize_optional_text, ControlCenter, RuntimeConfig};

/// Captured control-plane inputs required for Discord onboarding requests.
#[derive(Debug, Clone)]
pub(crate) struct DiscordControlPlaneInputs {
    pub(crate) runtime: RuntimeConfig,
    pub(crate) admin_token: String,
    pub(crate) http_client: Client,
}

impl DiscordControlPlaneInputs {
    /// Captures Discord control-plane inputs from the running control center.
    pub(crate) fn capture(control_center: &ControlCenter) -> Self {
        Self {
            runtime: control_center.runtime.clone(),
            admin_token: control_center.admin_token.clone(),
            http_client: control_center.http_client.clone(),
        }
    }
}

/// Desktop request body for Discord onboarding preflight and apply actions.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiscordOnboardingRequest {
    #[serde(default)]
    pub(crate) account_id: Option<String>,
    #[serde(default)]
    pub(crate) token: String,
    #[serde(default)]
    pub(crate) mode: Option<String>,
    #[serde(default)]
    pub(crate) inbound_scope: Option<String>,
    #[serde(default)]
    pub(crate) allow_from: Vec<String>,
    #[serde(default)]
    pub(crate) deny_from: Vec<String>,
    #[serde(default)]
    pub(crate) require_mention: Option<bool>,
    #[serde(default)]
    pub(crate) mention_patterns: Vec<String>,
    #[serde(default)]
    pub(crate) concurrency_limit: Option<u64>,
    #[serde(default)]
    pub(crate) direct_message_policy: Option<String>,
    #[serde(default)]
    pub(crate) broadcast_strategy: Option<String>,
    #[serde(default)]
    pub(crate) confirm_open_guild_channels: Option<bool>,
    #[serde(default)]
    pub(crate) verify_channel_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DiscordOnboardingConsoleRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inbound_scope: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    allow_from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    deny_from: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    require_mention: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mention_patterns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    concurrency_limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_message_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    broadcast_strategy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirm_open_guild_channels: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verify_channel_id: Option<String>,
}

impl From<DiscordOnboardingRequest> for DiscordOnboardingConsoleRequest {
    fn from(value: DiscordOnboardingRequest) -> Self {
        Self {
            account_id: value.account_id,
            token: value.token,
            mode: value.mode,
            inbound_scope: value.inbound_scope,
            allow_from: value.allow_from,
            deny_from: value.deny_from,
            require_mention: value.require_mention,
            mention_patterns: value.mention_patterns,
            concurrency_limit: value.concurrency_limit,
            direct_message_policy: value.direct_message_policy,
            broadcast_strategy: value.broadcast_strategy,
            confirm_open_guild_channels: value.confirm_open_guild_channels,
            verify_channel_id: value.verify_channel_id,
        }
    }
}

/// Desktop request body for sending a Discord verification test.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiscordVerificationRequest {
    pub(crate) connector_id: String,
    pub(crate) target: String,
    #[serde(default)]
    pub(crate) text: Option<String>,
}

/// Sanitized Discord onboarding preflight result returned to the desktop UI.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordOnboardingPreflightSnapshot {
    pub(crate) connector_id: String,
    pub(crate) account_id: String,
    pub(crate) bot_id: Option<String>,
    pub(crate) bot_username: Option<String>,
    pub(crate) invite_url_template: Option<String>,
    pub(crate) inbound_alive: bool,
    pub(crate) warnings: Vec<String>,
    pub(crate) policy_warnings: Vec<String>,
    pub(crate) required_permissions: Vec<String>,
    pub(crate) security_defaults: Vec<String>,
}

/// Sanitized Discord onboarding apply result returned to the desktop UI.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordOnboardingApplySnapshot {
    pub(crate) connector_id: String,
    pub(crate) config_path: Option<String>,
    pub(crate) config_created: bool,
    pub(crate) connector_enabled: bool,
    pub(crate) inbound_alive: bool,
    pub(crate) inbound_monitor_warnings: Vec<String>,
    pub(crate) readiness: Option<String>,
    pub(crate) liveness: Option<String>,
    pub(crate) token_vault_ref: Option<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) policy_warnings: Vec<String>,
}

/// Result of dispatching a Discord verification test message.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiscordVerificationResult {
    pub(crate) connector_id: String,
    pub(crate) target: String,
    pub(crate) delivered: Option<u64>,
    pub(crate) message: String,
}

/// Runs Discord onboarding preflight through the console API.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, HTTP dispatch, or response parsing fails.
pub(crate) async fn run_discord_onboarding_preflight(
    inputs: DiscordControlPlaneInputs,
    request: DiscordOnboardingRequest,
) -> Result<DiscordOnboardingPreflightSnapshot> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    let csrf_token =
        ensure_console_session_with_csrf(&mut control_plane, inputs.admin_token.as_str()).await?;
    let console_request = DiscordOnboardingConsoleRequest::from(request);
    let payload = post_console_json::<Value, _>(
        &inputs.http_client,
        &inputs.runtime,
        "/console/v1/channels/discord/onboarding/probe",
        csrf_token.as_str(),
        &console_request,
    )
    .await?;
    parse_preflight_snapshot(payload)
}

/// Applies Discord onboarding through the console API.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, HTTP dispatch, or response parsing fails.
pub(crate) async fn apply_discord_onboarding(
    inputs: DiscordControlPlaneInputs,
    request: DiscordOnboardingRequest,
) -> Result<DiscordOnboardingApplySnapshot> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    let csrf_token =
        ensure_console_session_with_csrf(&mut control_plane, inputs.admin_token.as_str()).await?;
    let console_request = DiscordOnboardingConsoleRequest::from(request);
    let payload = post_console_json::<Value, _>(
        &inputs.http_client,
        &inputs.runtime,
        "/console/v1/channels/discord/onboarding/apply",
        csrf_token.as_str(),
        &console_request,
    )
    .await?;
    parse_apply_snapshot(payload)
}

/// Sends a Discord connector verification message through the console API.
///
/// # Errors
/// Returns an error when control-plane client construction, console session
/// bootstrap, request validation, HTTP dispatch, or response parsing fails.
pub(crate) async fn verify_discord_connector(
    inputs: DiscordControlPlaneInputs,
    request: DiscordVerificationRequest,
) -> Result<DiscordVerificationResult> {
    let mut control_plane =
        build_control_plane_client(inputs.http_client.clone(), &inputs.runtime)?;
    let csrf_token =
        ensure_console_session_with_csrf(&mut control_plane, inputs.admin_token.as_str()).await?;
    let connector_id = normalize_optional_text(request.connector_id.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("Discord connector_id is required"))?;
    let target = normalize_optional_text(request.target.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("Discord verification target is required"))?;

    let payload = post_console_json::<Value, _>(
        &inputs.http_client,
        &inputs.runtime,
        format!(
            "/console/v1/channels/{}/test-send",
            connector_id.replace(':', "%3A")
        )
        .as_str(),
        csrf_token.as_str(),
        &serde_json::json!({
            "target": target,
            "text": request.text.and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned)),
            "confirm": true
        }),
    )
    .await?;
    parse_verification_result(connector_id, payload)
}

fn parse_preflight_snapshot(payload: Value) -> Result<DiscordOnboardingPreflightSnapshot> {
    let connector_id = read_string(payload.get("connector_id"))
        .ok_or_else(|| anyhow!("Discord preflight response is missing connector_id"))?;
    let account_id = read_string(payload.get("account_id"))
        .ok_or_else(|| anyhow!("Discord preflight response is missing account_id"))?;
    let bot = payload.get("bot").and_then(Value::as_object);

    Ok(DiscordOnboardingPreflightSnapshot {
        connector_id,
        account_id,
        bot_id: bot.and_then(|value| read_string(value.get("id"))),
        bot_username: bot.and_then(|value| read_string(value.get("username"))),
        invite_url_template: read_string(payload.get("invite_url_template")),
        inbound_alive: payload.get("inbound_alive").and_then(Value::as_bool).unwrap_or(false),
        warnings: read_string_array(payload.get("warnings")),
        policy_warnings: read_string_array(payload.get("policy_warnings")),
        required_permissions: read_string_array(payload.get("required_permissions")),
        security_defaults: read_string_array(payload.get("security_defaults")),
    })
}

fn parse_apply_snapshot(payload: Value) -> Result<DiscordOnboardingApplySnapshot> {
    let preflight = payload
        .get("preflight")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Discord apply response is missing preflight"))?;
    let applied = payload
        .get("applied")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Discord apply response is missing applied"))?;
    let status = payload.get("status").and_then(Value::as_object);

    Ok(DiscordOnboardingApplySnapshot {
        connector_id: read_string(preflight.get("connector_id"))
            .ok_or_else(|| anyhow!("Discord apply response is missing connector_id"))?,
        config_path: read_string(applied.get("config_path")),
        config_created: applied.get("config_created").and_then(Value::as_bool).unwrap_or(false),
        connector_enabled: applied
            .get("connector_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        inbound_alive: payload.get("inbound_alive").and_then(Value::as_bool).unwrap_or(false),
        inbound_monitor_warnings: read_string_array(payload.get("inbound_monitor_warnings")),
        readiness: status.and_then(|value| read_string(value.get("readiness"))),
        liveness: status.and_then(|value| read_string(value.get("liveness"))),
        token_vault_ref: read_string(applied.get("token_vault_ref")),
        warnings: read_string_array(preflight.get("warnings")),
        policy_warnings: read_string_array(preflight.get("policy_warnings")),
    })
}

fn parse_verification_result(
    connector_id: String,
    payload: Value,
) -> Result<DiscordVerificationResult> {
    let dispatch = payload
        .get("dispatch")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Discord verification response is missing dispatch"))?;
    let target = read_string(dispatch.get("target"))
        .ok_or_else(|| anyhow!("Discord verification response is missing target"))?;
    let delivered = dispatch.get("delivered").and_then(Value::as_u64);
    let message = match delivered {
        Some(delivered) => format!("Discord verification dispatched (delivered={delivered})."),
        None => "Discord verification dispatched.".to_owned(),
    };

    Ok(DiscordVerificationResult { connector_id, target, delivered, message })
}

fn read_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .and_then(normalize_optional_text)
        .map(|value| sanitize_log_line(value).to_owned())
}

fn read_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .filter_map(normalize_optional_text)
                .map(|item| sanitize_log_line(item).to_owned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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
