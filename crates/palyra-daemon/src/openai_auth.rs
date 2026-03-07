use std::{env, time::Duration};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::model_provider::sanitize_remote_error;

pub(crate) const OPENAI_OAUTH_ATTEMPT_TTL_MS: i64 = 10 * 60 * 1_000;
pub(crate) const OPENAI_OAUTH_AUDIENCE: &str = "https://api.openai.com/v1";
pub(crate) const OPENAI_OAUTH_CALLBACK_EVENT_TYPE: &str = "palyra-openai-oauth-complete";
pub(crate) const OPENAI_OAUTH_DEFAULT_SCOPES: &[&str] =
    &["openid", "profile", "email", "offline_access"];

const ENV_OPENAI_AUTHORIZATION_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_AUTHORIZATION_ENDPOINT";
const ENV_OPENAI_TOKEN_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_TOKEN_ENDPOINT";
const ENV_OPENAI_REVOCATION_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_REVOCATION_ENDPOINT";
const OPENAI_AUTHORIZATION_ENDPOINT: &str = "https://auth.openai.com/authorize";
const OPENAI_TOKEN_ENDPOINT: &str = "https://auth0.openai.com/oauth/token";
const OPENAI_REVOCATION_ENDPOINT: &str = "https://auth0.openai.com/oauth/revoke";
const MODELS_PATH: &str = "models";

#[derive(Debug, Clone)]
pub(crate) struct OpenAiOAuthEndpointConfig {
    pub(crate) authorization_endpoint: String,
    pub(crate) token_endpoint: String,
    pub(crate) revocation_endpoint: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OAuthTokenExchangeResult {
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) expires_in_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenAiCredentialValidationError {
    InvalidCredential,
    RateLimited,
    ProviderUnavailable,
    Unexpected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenAiOAuthAttemptStateRecord {
    Pending { message: String },
    Succeeded { profile_id: String, message: String, completed_at_unix_ms: i64 },
    Failed { message: String, completed_at_unix_ms: i64 },
}

#[derive(Debug, Deserialize)]
struct OAuthTokenExchangePayload {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

pub(crate) fn oauth_endpoint_config_from_env() -> OpenAiOAuthEndpointConfig {
    OpenAiOAuthEndpointConfig {
        authorization_endpoint: env::var(ENV_OPENAI_AUTHORIZATION_ENDPOINT)
            .ok()
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .unwrap_or_else(|| OPENAI_AUTHORIZATION_ENDPOINT.to_owned()),
        token_endpoint: env::var(ENV_OPENAI_TOKEN_ENDPOINT)
            .ok()
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .unwrap_or_else(|| OPENAI_TOKEN_ENDPOINT.to_owned()),
        revocation_endpoint: env::var(ENV_OPENAI_REVOCATION_ENDPOINT)
            .ok()
            .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
            .unwrap_or_else(|| OPENAI_REVOCATION_ENDPOINT.to_owned()),
    }
}

pub(crate) fn normalize_scopes(scopes: &[String]) -> Vec<String> {
    let normalized = scopes
        .iter()
        .filter_map(|scope| normalize_optional_text(scope))
        .map(|scope| scope.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if normalized.is_empty() {
        return OPENAI_OAUTH_DEFAULT_SCOPES.iter().map(|scope| (*scope).to_owned()).collect();
    }
    normalized
}

pub(crate) fn generate_pkce_verifier() -> String {
    let bytes: [u8; 32] = rand::random();
    URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

pub(crate) fn build_authorization_url(
    endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    scopes: &[String],
    code_challenge: &str,
    state: &str,
) -> Result<String> {
    let mut url = Url::parse(endpoint)
        .with_context(|| format!("invalid OpenAI authorization endpoint: {endpoint}"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("response_type", "code");
        pairs.append_pair("client_id", client_id);
        pairs.append_pair("redirect_uri", redirect_uri);
        pairs.append_pair("scope", scopes.join(" ").as_str());
        pairs.append_pair("code_challenge", code_challenge);
        pairs.append_pair("code_challenge_method", "S256");
        pairs.append_pair("audience", OPENAI_OAUTH_AUDIENCE);
        pairs.append_pair("state", state);
    }
    Ok(url.to_string())
}

pub(crate) async fn exchange_authorization_code(
    token_endpoint: &str,
    redirect_uri: &str,
    client_id: &str,
    client_secret: &str,
    code_verifier: &str,
    code: &str,
    timeout: Duration,
) -> Result<OAuthTokenExchangeResult> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build OpenAI OAuth token client")?;
    let mut form_fields = vec![
        ("grant_type", "authorization_code".to_owned()),
        ("client_id", client_id.to_owned()),
        ("redirect_uri", redirect_uri.to_owned()),
        ("code_verifier", code_verifier.to_owned()),
        ("code", code.to_owned()),
    ];
    if !client_secret.trim().is_empty() {
        form_fields.push(("client_secret", client_secret.to_owned()));
    }
    let response =
        client.post(token_endpoint).form(&form_fields).send().await.with_context(|| {
            format!("OpenAI OAuth token exchange request failed for {token_endpoint}")
        })?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let sanitized = sanitize_remote_error(body.as_str());
        return Err(anyhow!(
            "OpenAI OAuth token exchange failed with status {}: {}",
            status.as_u16(),
            sanitized
        ));
    }
    let payload: OAuthTokenExchangePayload = serde_json::from_str(body.as_str())
        .context("OpenAI OAuth token response was not valid JSON")?;
    let refresh_token = payload
        .refresh_token
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
        .ok_or_else(|| anyhow!("OpenAI OAuth token response did not include a refresh_token"))?;
    if payload.access_token.trim().is_empty() {
        return Err(anyhow!("OpenAI OAuth token response did not include an access_token"));
    }
    Ok(OAuthTokenExchangeResult {
        access_token: payload.access_token,
        refresh_token,
        expires_in_seconds: payload.expires_in,
    })
}

pub(crate) async fn validate_openai_bearer_token(
    base_url: &str,
    bearer_token: &str,
    timeout: Duration,
) -> Result<(), OpenAiCredentialValidationError> {
    let endpoint = openai_models_endpoint(base_url)
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    let response = client
        .get(endpoint)
        .bearer_auth(bearer_token)
        .send()
        .await
        .map_err(|error| OpenAiCredentialValidationError::Unexpected(error.to_string()))?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    let sanitized = sanitize_remote_error(body.as_str());
    match status.as_u16() {
        401 | 403 => Err(OpenAiCredentialValidationError::InvalidCredential),
        429 => Err(OpenAiCredentialValidationError::RateLimited),
        500 | 502 | 503 | 504 => Err(OpenAiCredentialValidationError::ProviderUnavailable),
        _ => Err(OpenAiCredentialValidationError::Unexpected(format!(
            "validation endpoint returned status {}: {}",
            status.as_u16(),
            sanitized
        ))),
    }
}

pub(crate) async fn revoke_openai_token(
    revocation_endpoint: &str,
    client_id: &str,
    client_secret: &str,
    token: &str,
    timeout: Duration,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build OpenAI OAuth revocation client")?;
    let mut form_fields = vec![("client_id", client_id.to_owned()), ("token", token.to_owned())];
    if !client_secret.trim().is_empty() {
        form_fields.push(("client_secret", client_secret.to_owned()));
    }
    let response =
        client.post(revocation_endpoint).form(&form_fields).send().await.with_context(|| {
            format!("OpenAI OAuth revocation request failed for {revocation_endpoint}")
        })?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let sanitized = sanitize_remote_error(body.as_str());
        return Err(anyhow!(
            "OpenAI OAuth revocation failed with status {}: {}",
            status.as_u16(),
            sanitized
        ));
    }
    Ok(())
}

pub(crate) fn render_callback_page(title: &str, body: &str, payload_json: Option<&str>) -> String {
    let escaped_title = html_escape(title);
    let escaped_body = html_escape(body);
    let post_message_script = payload_json.map_or_else(String::new, |payload| {
        format!(
            "if (window.opener && !window.opener.closed) {{ try {{ window.opener.postMessage({payload}, window.location.origin); window.close(); }} catch (_error) {{ }} }}"
        )
    });
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{escaped_title}</title><style>body{{font-family:ui-sans-serif,system-ui,sans-serif;background:#f7f4ec;color:#1f2933;margin:0;padding:32px}}main{{max-width:560px;margin:0 auto;background:#fff;border:1px solid #d8d1c2;border-radius:16px;padding:24px;box-shadow:0 20px 60px rgba(31,41,51,.08)}}h1{{margin-top:0;font-size:1.4rem}}p{{line-height:1.5}}code{{background:#f2ede2;padding:2px 6px;border-radius:6px}}</style></head><body><main><h1>{escaped_title}</h1><p>{escaped_body}</p><p>You can return to Palyra now.</p></main><script>{post_message_script}</script></body></html>"
    )
}

fn html_escape(raw: &str) -> String {
    raw.chars()
        .map(|ch| match ch {
            '&' => "&amp;".to_owned(),
            '<' => "&lt;".to_owned(),
            '>' => "&gt;".to_owned(),
            '"' => "&quot;".to_owned(),
            '\'' => "&#39;".to_owned(),
            _ => ch.to_string(),
        })
        .collect()
}

fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn openai_models_endpoint(base_url: &str) -> Result<Url> {
    let mut normalized = base_url.trim().to_owned();
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    let base = Url::parse(normalized.as_str())
        .with_context(|| format!("invalid OpenAI validation base URL: {base_url}"))?;
    base.join(MODELS_PATH)
        .with_context(|| format!("invalid OpenAI validation models URL for {base_url}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_models_endpoint_preserves_versioned_base_path() {
        let endpoint = openai_models_endpoint("https://api.openai.com/v1")
            .expect("versioned base URL should build a models endpoint");
        assert_eq!(endpoint.as_str(), "https://api.openai.com/v1/models");
    }

    #[test]
    fn openai_models_endpoint_handles_trailing_slash() {
        let endpoint = openai_models_endpoint("https://example.test/custom/")
            .expect("base URL with trailing slash should build a models endpoint");
        assert_eq!(endpoint.as_str(), "https://example.test/custom/models");
    }
}
