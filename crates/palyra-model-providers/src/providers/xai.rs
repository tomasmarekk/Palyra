//! xAI identity for OpenAI-compatible transport.

pub(crate) const PROVIDER_ID: &str = "xai-primary";
pub(crate) const DISPLAY_NAME: &str = "xAI (Grok)";
/// xAI's OpenAI-compatible API-key endpoint.
pub const API_BASE_URL: &str = "https://api.x.ai/v1";
/// Grok OAuth proxy endpoint used by some compatible harnesses for live catalog discovery.
pub const GROK_OAUTH_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";
/// Static xAI chat default used when live discovery is unavailable.
pub const DEFAULT_CHAT_MODEL_ID: &str = "grok-4.3";
pub const OAUTH_ISSUER: &str = "https://auth.x.ai";
pub const OAUTH_DISCOVERY_URL: &str = "https://auth.x.ai/.well-known/openid-configuration";
pub const OAUTH_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
pub const OAUTH_SCOPE: &str = "openid profile email offline_access grok-cli:access api:access";
pub const OAUTH_REDIRECT_URI: &str = "http://127.0.0.1:56121/callback";
pub const OAUTH_CALLBACK_HOST: &str = "127.0.0.1";
pub const OAUTH_CALLBACK_PORT: u16 = 56_121;
pub const OAUTH_CALLBACK_PATH: &str = "/callback";
pub const OAUTH_CALLBACK_CORS_ORIGIN_ALLOWLIST: &[&str] = &["auth.x.ai", "accounts.x.ai"];

/// Normalizes xAI OAuth discovery endpoints and rejects token exfiltration targets.
///
/// # Errors
/// Returns an error when `raw` is blank, not HTTPS, includes credentials,
/// carries query/fragment data, or is not hosted on `x.ai` / `*.x.ai`.
pub fn normalize_oauth_endpoint(raw: &str, field: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("xAI OAuth discovery is missing {field}");
    }
    let parsed = reqwest::Url::parse(trimmed)
        .map_err(|error| anyhow::anyhow!("invalid xAI {field}: {error}"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("xAI {field} must use https");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("xAI {field} must not contain embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("xAI {field} must not contain query or fragment components");
    }
    let host = parsed.host_str().unwrap_or_default();
    if !is_trusted_oauth_host(host) {
        anyhow::bail!("xAI {field} host is not trusted");
    }
    Ok(parsed.to_string())
}

/// Returns whether an OAuth discovery/token host belongs to xAI.
#[must_use]
pub fn is_trusted_oauth_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("x.ai") || host.to_ascii_lowercase().ends_with(".x.ai")
}
