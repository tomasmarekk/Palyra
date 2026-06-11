//! HTTP transport layer and credential resolution for the Discord adapter.
//!
//! Defines the [`DiscordTransport`] seam the adapter calls (faked in tests, reqwest-backed in
//! production), URL builders for the REST routes in use, rate-limit header parsing, and
//! environment-based bot-token resolution.

use std::{collections::HashMap, env, time::Duration};

use async_trait::async_trait;
use palyra_common::redaction::redact_auth_error;
use reqwest::{multipart, redirect::Policy, Client, Url};
use serde_json::Value;

use crate::{storage::ConnectorInstanceRecord, supervisor::ConnectorAdapterError};

use super::{
    outbound::{percent_encode_component, DiscordMultipartAttachment},
    runtime::RateLimitSnapshot,
};

fn account_id_from_connector_id(connector_id: &str) -> String {
    connector_id
        .strip_prefix("discord:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_owned()
}

fn sanitize_env_suffix(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_uppercase() } else { '_' })
        .collect()
}

/// Returns the last four characters of the token for runtime diagnostics.
///
/// Only this suffix is ever surfaced (runtime snapshots, console); the full token must never
/// leave the credential resolver.
pub(super) fn token_suffix(token: &str) -> Option<String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() <= 4 {
        return Some(trimmed.to_owned());
    }
    Some(chars[chars.len().saturating_sub(4)..].iter().collect())
}

/// Adapter-level wrapper around [`super::super::normalize_discord_target`] that rewrites the
/// semantic error into the adapter's outbound-facing wording.
///
/// # Errors
/// Returns [`ConnectorAdapterError::Backend`] when the target is blank or contains
/// unsupported characters.
pub(super) fn normalize_discord_target(raw: &str) -> Result<String, ConnectorAdapterError> {
    super::super::normalize_discord_target(raw).map_err(|error| {
        let message = match error {
            super::super::DiscordSemanticsError::EmptyTarget => {
                "discord target conversation id cannot be empty".to_owned()
            }
            super::super::DiscordSemanticsError::InvalidTarget => {
                "discord target conversation id contains unsupported characters".to_owned()
            }
            other => other.to_string(),
        };
        ConnectorAdapterError::Backend(message)
    })
}

pub(super) fn build_users_me_url(api_base: &Url) -> Result<Url, ConnectorAdapterError> {
    let candidate = format!("{}/users/@me", api_base.as_str().trim_end_matches('/'));
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

pub(super) fn build_messages_url(
    api_base: &Url,
    channel_id: &str,
) -> Result<Url, ConnectorAdapterError> {
    let candidate =
        format!("{}/channels/{}/messages", api_base.as_str().trim_end_matches('/'), channel_id);
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

pub(super) fn build_channel_url(
    api_base: &Url,
    channel_id: &str,
) -> Result<Url, ConnectorAdapterError> {
    let candidate = format!("{}/channels/{}", api_base.as_str().trim_end_matches('/'), channel_id);
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

pub(super) fn build_message_url(
    api_base: &Url,
    channel_id: &str,
    message_id: &str,
) -> Result<Url, ConnectorAdapterError> {
    let candidate = format!(
        "{}/channels/{}/messages/{}",
        api_base.as_str().trim_end_matches('/'),
        channel_id,
        message_id
    );
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

pub(super) fn build_reaction_url(
    api_base: &Url,
    channel_id: &str,
    message_id: &str,
    emoji: &str,
) -> Result<Url, ConnectorAdapterError> {
    let emoji_component = percent_encode_component(emoji.trim());
    let candidate = format!(
        "{}/channels/{}/messages/{}/reactions/{}/@me",
        api_base.as_str().trim_end_matches('/'),
        channel_id,
        message_id,
        emoji_component
    );
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

/// Extracts Discord's rate-limit signals from response headers and body.
///
/// The body `retry_after` wins over the `retry-after` header because Discord reports
/// sub-second precision only in the body; the global flag may arrive via either channel.
pub(super) fn parse_rate_limit_snapshot(response: &DiscordTransportResponse) -> RateLimitSnapshot {
    let retry_after_ms_from_body = parse_retry_after_ms_from_body(response.body.as_str());
    let retry_after_ms_from_header =
        parse_f64_header_ms(response.headers.get("retry-after").map(String::as_str));
    let reset_after_ms =
        parse_f64_header_ms(response.headers.get("x-ratelimit-reset-after").map(String::as_str));
    let remaining =
        parse_u64_header(response.headers.get("x-ratelimit-remaining").map(String::as_str));
    let bucket_id = response
        .headers
        .get("x-ratelimit-bucket")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let global_from_header = response
        .headers
        .get("x-ratelimit-global")
        .map(String::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let (global_from_body, _) = parse_global_flag_from_body(response.body.as_str());

    RateLimitSnapshot {
        retry_after_ms: retry_after_ms_from_body.or(retry_after_ms_from_header),
        reset_after_ms,
        remaining,
        bucket_id,
        global: global_from_header || global_from_body,
    }
}

fn parse_u64_header(value: Option<&str>) -> Option<u64> {
    value.and_then(|raw| raw.trim().parse::<u64>().ok())
}

fn parse_f64_header_ms(value: Option<&str>) -> Option<u64> {
    value.and_then(|raw| raw.trim().parse::<f64>().ok()).and_then(seconds_to_ms)
}

fn parse_retry_after_ms_from_body(raw_body: &str) -> Option<u64> {
    serde_json::from_str::<Value>(raw_body)
        .ok()
        .and_then(|payload| payload.get("retry_after").and_then(Value::as_f64))
        .and_then(seconds_to_ms)
}

fn parse_global_flag_from_body(raw_body: &str) -> (bool, Option<String>) {
    let Ok(payload) = serde_json::from_str::<Value>(raw_body) else {
        return (false, None);
    };
    let global = payload.get("global").and_then(Value::as_bool).unwrap_or(false);
    let message = payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    (global, message)
}

fn seconds_to_ms(seconds: f64) -> Option<u64> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let millis = (seconds * 1_000.0).ceil();
    if !millis.is_finite() || millis < 0.0 {
        return None;
    }
    // Float-to-int `as` saturates; after the finite/non-negative checks above the only edge
    // left is an absurdly large wait, which clamping to u64::MAX handles correctly.
    Some(millis as u64)
}

/// Summarizes a Discord error response body into a single redacted line for diagnostics.
pub(super) fn parse_discord_error_summary(raw_body: &str) -> Option<String> {
    let trimmed = raw_body.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = serde_json::from_str::<Value>(trimmed).ok();
    if let Some(parsed) = parsed {
        let message = parsed
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "unknown error".to_owned());
        let retry_after = parsed.get("retry_after").and_then(Value::as_f64).and_then(seconds_to_ms);
        if let Some(retry_after) = retry_after {
            return Some(format!("{message} (retry_after_ms={retry_after})"));
        }
        return Some(message);
    }
    Some(redact_auth_error(trimmed))
}

fn map_reqwest_error(error: reqwest::Error) -> ConnectorAdapterError {
    ConnectorAdapterError::Backend(redact_auth_error(
        format!("discord HTTP transport error: {error}").as_str(),
    ))
}

async fn response_to_transport(
    response: reqwest::Response,
) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (name.as_str().to_ascii_lowercase(), value.to_str().unwrap_or_default().to_owned())
        })
        .collect::<HashMap<_, _>>();
    let body = response.text().await.map_err(map_reqwest_error)?;
    Ok(DiscordTransportResponse { status, headers, body })
}

/// Resolved bot credential plus a non-sensitive label describing where it came from.
#[derive(Debug, Clone)]
pub struct DiscordCredential {
    /// The bot token; must never be logged or surfaced beyond the transport layer.
    pub token: String,
    /// Diagnostic label of the resolution source (e.g. vault reference or env variable).
    pub source: String,
}

/// Resolves the bot credential for a Discord connector instance.
#[async_trait]
pub trait DiscordCredentialResolver: Send + Sync {
    /// Resolves the credential for `instance`.
    ///
    /// # Errors
    /// Returns [`ConnectorAdapterError::Backend`] when no usable credential can be found;
    /// the message must already be redaction-safe.
    async fn resolve_credential(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<DiscordCredential, ConnectorAdapterError>;
}

/// Default resolver reading bot tokens from `PALYRA_DISCORD_TOKEN*` environment variables.
///
/// Resolution order: vault-reference-specific variable, then account-specific variable, then
/// the global `PALYRA_DISCORD_TOKEN` — most specific wins so multi-account setups can
/// override the shared default.
#[derive(Debug, Default)]
pub struct EnvDiscordCredentialResolver;

#[async_trait]
impl DiscordCredentialResolver for EnvDiscordCredentialResolver {
    async fn resolve_credential(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<DiscordCredential, ConnectorAdapterError> {
        if let Some(token_vault_ref) = instance.token_vault_ref.as_deref() {
            let vault_env =
                format!("PALYRA_DISCORD_TOKEN_REF_{}", sanitize_env_suffix(token_vault_ref));
            if let Ok(token) = env::var(vault_env.as_str()) {
                let trimmed = token.trim();
                if !trimmed.is_empty() {
                    return Ok(DiscordCredential {
                        token: trimmed.to_owned(),
                        source: "vault_ref_env".to_owned(),
                    });
                }
            }
        }

        let account_id = account_id_from_connector_id(instance.connector_id.as_str());
        let account_env =
            format!("PALYRA_DISCORD_TOKEN_{}", sanitize_env_suffix(account_id.as_str()));
        if let Ok(token) = env::var(account_env.as_str()) {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(DiscordCredential {
                    token: trimmed.to_owned(),
                    source: format!("env:{account_env}"),
                });
            }
        }

        if let Ok(token) = env::var("PALYRA_DISCORD_TOKEN") {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(DiscordCredential {
                    token: trimmed.to_owned(),
                    source: "env:PALYRA_DISCORD_TOKEN".to_owned(),
                });
            }
        }

        Err(ConnectorAdapterError::Backend(format!(
            "discord credential missing for connector {}",
            instance.connector_id
        )))
    }
}

/// Raw HTTP response surface the adapter needs: status, lowercased headers, and body text.
pub(super) struct DiscordTransportResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

/// HTTP verbs the adapter issues against the Discord REST API.
///
/// Implementations return `Ok` for any HTTP status; `Err` is reserved for transport-level
/// failures (connect, timeout, body read), which callers classify as transient.
#[async_trait]
pub(super) trait DiscordTransport: Send + Sync {
    async fn get(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;

    async fn post_json(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;

    async fn post_multipart(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        files: &[DiscordMultipartAttachment],
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;

    async fn put(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;

    async fn patch_json(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;

    async fn delete(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;
}

/// Production transport backed by a shared reqwest client.
#[derive(Clone)]
pub struct ReqwestDiscordTransport {
    client: Client,
}

impl Default for ReqwestDiscordTransport {
    fn default() -> Self {
        // Redirects are disabled so a redirecting response can never re-route a request
        // (with its bot token) outside the egress-validated target.
        let client = Client::builder()
            .redirect(Policy::none())
            .build()
            .expect("default reqwest client should initialize");
        Self { client }
    }
}

#[async_trait]
impl DiscordTransport for ReqwestDiscordTransport {
    async fn get(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let response = self
            .client
            .get(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }

    async fn post_json(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let response = self
            .client
            .post(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .json(payload)
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }

    async fn post_multipart(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        files: &[DiscordMultipartAttachment],
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let mut form = multipart::Form::new().text("payload_json", payload.to_string());
        for (index, file) in files.iter().enumerate() {
            let part = multipart::Part::bytes(file.bytes.clone())
                .file_name(file.filename.clone())
                .mime_str(file.content_type.as_str())
                .map_err(|error| {
                    ConnectorAdapterError::Backend(format!(
                        "discord multipart content type is invalid: {error}"
                    ))
                })?;
            form = form.part(format!("files[{index}]"), part);
        }
        let response = self
            .client
            .post(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .multipart(form)
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }

    async fn put(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let response = self
            .client
            .put(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }

    async fn patch_json(
        &self,
        url: &Url,
        token: &str,
        payload: &Value,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let response = self
            .client
            .patch(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .json(payload)
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }

    async fn delete(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        let response = self
            .client
            .delete(url.clone())
            .header("Authorization", format!("Bot {token}"))
            .header("User-Agent", "palyra-discord-connector/1.0")
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .send()
            .await
            .map_err(map_reqwest_error)?;
        response_to_transport(response).await
    }
}
