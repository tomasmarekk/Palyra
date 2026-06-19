//! OpenAI OAuth and credential-validation primitives.
//!
//! Provides PKCE material, authorization-URL construction, authorization-code
//! exchange, bearer-token validation, token revocation, OAuth endpoint
//! resolution from env, and the HTML callback page served to the user's
//! browser. Consumed by `openai_surface` (console provider-auth handlers).
//!
//! Every remote response body passes through `sanitize_remote_error` before
//! it can reach an error message, so raw provider payloads (which may echo
//! credentials) never leak into logs or client-visible errors.

use std::{env, time::Duration};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::model_provider::sanitize_remote_error;

/// How long an OAuth attempt stays addressable after creation or completion.
pub(crate) const OPENAI_OAUTH_ATTEMPT_TTL_MS: i64 = 10 * 60 * 1_000;
/// `audience` sent in the authorization request; pins issued tokens to the OpenAI API.
pub(crate) const OPENAI_OAUTH_AUDIENCE: &str = "https://api.openai.com/v1";
/// `type` discriminator of the `postMessage` payload emitted by the rendered callback page.
pub(crate) const OPENAI_OAUTH_CALLBACK_EVENT_TYPE: &str = "palyra-openai-oauth-complete";
/// Scopes requested when the caller does not supply any non-empty scope.
pub(crate) const OPENAI_OAUTH_DEFAULT_SCOPES: &[&str] =
    &["openid", "profile", "email", "offline_access"];
const OPENAI_VALIDATION_RETRY_ATTEMPTS: usize = 5;
const OPENAI_VALIDATION_RETRY_DELAY: Duration = Duration::from_millis(100);
const OPENAI_REVOCATION_RETRY_ATTEMPTS: usize = 3;
const OPENAI_REVOCATION_RETRY_DELAY: Duration = Duration::from_millis(100);

const ENV_OPENAI_AUTHORIZATION_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_AUTHORIZATION_ENDPOINT";
const ENV_OPENAI_TOKEN_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_TOKEN_ENDPOINT";
const ENV_OPENAI_REVOCATION_ENDPOINT: &str = "PALYRA_OPENAI_OAUTH_REVOCATION_ENDPOINT";
const OPENAI_AUTHORIZATION_ENDPOINT: &str = "https://auth.openai.com/oauth/authorize";
const OPENAI_TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const OPENAI_REVOCATION_ENDPOINT: &str = "https://auth0.openai.com/oauth/revoke";
const MODELS_PATH: &str = "models";

/// Resolved OAuth endpoint set, each URL validated for scheme, host, and
/// absence of embedded credentials, query, or fragment.
#[derive(Debug, Clone)]
pub(crate) struct OpenAiOAuthEndpointConfig {
    pub(crate) authorization_endpoint: Url,
    pub(crate) token_endpoint: Url,
    pub(crate) revocation_endpoint: Url,
}

/// Tokens returned by a successful authorization-code exchange.
///
/// `refresh_token` is mandatory: the daemon refreshes unattended, so a grant
/// without one is rejected during the exchange.
#[derive(Debug, Clone)]
pub(crate) struct OAuthTokenExchangeResult {
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) expires_in_seconds: Option<u64>,
}

/// Outcome classification for OpenAI bearer-token validation, mapped from the
/// HTTP status of the models-endpoint probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenAiCredentialValidationError {
    /// 401/403: the credential is rejected by the provider.
    InvalidCredential,
    /// 429: the validation probe itself was rate limited.
    RateLimited,
    /// 5xx or repeated transport failures.
    ProviderUnavailable,
    /// Any other status or setup failure; carries a sanitized description.
    Unexpected(String),
}

/// Lifecycle state of an in-memory OAuth attempt as exposed to polling
/// clients via the callback-state endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenAiOAuthAttemptStateRecord {
    Pending { message: String },
    Completing { message: String },
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

/// Resolves the OAuth endpoint set from `PALYRA_OPENAI_OAUTH_*` env overrides,
/// falling back to the production OpenAI endpoints.
///
/// # Errors
/// Returns an error if any configured endpoint is empty, relative, carries
/// embedded credentials/query/fragment, or uses `http` on a non-loopback host.
pub(crate) fn oauth_endpoint_config_from_env() -> Result<OpenAiOAuthEndpointConfig> {
    Ok(OpenAiOAuthEndpointConfig {
        authorization_endpoint: load_openai_oauth_endpoint_from_env(
            ENV_OPENAI_AUTHORIZATION_ENDPOINT,
            OPENAI_AUTHORIZATION_ENDPOINT,
            "authorization endpoint",
        )?,
        token_endpoint: load_openai_oauth_endpoint_from_env(
            ENV_OPENAI_TOKEN_ENDPOINT,
            OPENAI_TOKEN_ENDPOINT,
            "token endpoint",
        )?,
        revocation_endpoint: load_openai_oauth_endpoint_from_env(
            ENV_OPENAI_REVOCATION_ENDPOINT,
            OPENAI_REVOCATION_ENDPOINT,
            "revocation endpoint",
        )?,
    })
}

/// Trims and lowercases the requested scopes, substituting
/// [`OPENAI_OAUTH_DEFAULT_SCOPES`] when nothing non-empty remains.
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

/// Generates a PKCE code verifier: 32 random bytes, base64url-encoded without
/// padding per RFC 7636 section 4.1 (43 characters, within the 43-128 range).
pub(crate) fn generate_pkce_verifier() -> String {
    let bytes: [u8; 32] = rand::random();
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Derives the `S256` PKCE code challenge for `verifier` (RFC 7636 section 4.2).
pub(crate) fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

/// Builds the user-facing authorization URL for the PKCE code flow.
///
/// `state` carries the attempt id and is round-tripped by the provider so the
/// callback can be matched to its pending attempt.
///
/// # Errors
/// Currently infallible; the `Result` is kept so URL-construction failures
/// can surface later without a signature change.
pub(crate) fn build_authorization_url(
    endpoint: &Url,
    client_id: &str,
    redirect_uri: &str,
    scopes: &[String],
    code_challenge: &str,
    state: &str,
) -> Result<String> {
    let mut url = endpoint.clone();
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

/// Exchanges an authorization code (plus PKCE verifier) for access and
/// refresh tokens at `token_endpoint`.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built, the request fails,
/// the provider responds with a non-success status (body sanitized before it
/// reaches the message), the response is not valid JSON, or the payload lacks
/// a usable `access_token` or `refresh_token`.
pub(crate) async fn exchange_authorization_code(
    token_endpoint: &Url,
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
    // Public PKCE clients have no client_secret; only confidential clients
    // send one, so an empty secret is omitted rather than sent blank.
    if !client_secret.trim().is_empty() {
        form_fields.push(("client_secret", client_secret.to_owned()));
    }
    let response =
        client.post(token_endpoint.clone()).form(&form_fields).send().await.with_context(|| {
            format!(
                "OpenAI OAuth token exchange request failed for host {}",
                token_endpoint.host_str().unwrap_or("<unknown>")
            )
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

/// Probes `GET {base_url}/models` with `bearer_token` to confirm the
/// credential is accepted by the provider.
///
/// Only transport-level failures are retried (the provider was never
/// reached); HTTP statuses are treated as deterministic answers and mapped
/// immediately.
///
/// # Errors
/// Returns [`OpenAiCredentialValidationError::InvalidCredential`] on 401/403,
/// `RateLimited` on 429, `ProviderUnavailable` on 5xx or exhausted transport
/// retries, and `Unexpected` for any other status or setup failure.
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

    for attempt_index in 0..OPENAI_VALIDATION_RETRY_ATTEMPTS {
        let response = client.get(endpoint.clone()).bearer_auth(bearer_token).send().await;
        match response {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    return Ok(());
                }
                let body = response.text().await.unwrap_or_default();
                let sanitized = sanitize_remote_error(body.as_str());
                return match status.as_u16() {
                    401 | 403 => Err(OpenAiCredentialValidationError::InvalidCredential),
                    429 => Err(OpenAiCredentialValidationError::RateLimited),
                    500 | 502 | 503 | 504 => {
                        Err(OpenAiCredentialValidationError::ProviderUnavailable)
                    }
                    _ => Err(OpenAiCredentialValidationError::Unexpected(format!(
                        "validation endpoint returned status {}: {}",
                        status.as_u16(),
                        sanitized
                    ))),
                };
            }
            Err(_error) => {
                if attempt_index + 1 < OPENAI_VALIDATION_RETRY_ATTEMPTS {
                    tokio::time::sleep(OPENAI_VALIDATION_RETRY_DELAY).await;
                    continue;
                }
                return Err(OpenAiCredentialValidationError::ProviderUnavailable);
            }
        }
    }

    Err(OpenAiCredentialValidationError::ProviderUnavailable)
}

/// Revokes `token` (RFC 7009) at `revocation_endpoint`, retrying transient
/// 5xx responses and transport failures a bounded number of times.
///
/// # Errors
/// Returns an error when the HTTP client cannot be built, the provider
/// responds with a persistent non-success status (body sanitized), or the
/// retry budget is exhausted without reaching the endpoint.
pub(crate) async fn revoke_openai_token(
    revocation_endpoint: &Url,
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

    for attempt_index in 0..OPENAI_REVOCATION_RETRY_ATTEMPTS {
        let response = client.post(revocation_endpoint.clone()).form(&form_fields).send().await;
        match response {
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                if status.is_success() {
                    return Ok(());
                }

                let sanitized = sanitize_remote_error(body.as_str());
                if status.is_server_error() && attempt_index + 1 < OPENAI_REVOCATION_RETRY_ATTEMPTS
                {
                    tokio::time::sleep(OPENAI_REVOCATION_RETRY_DELAY).await;
                    continue;
                }

                return Err(anyhow!(
                    "OpenAI OAuth revocation failed with status {}: {}",
                    status.as_u16(),
                    sanitized
                ));
            }
            Err(error) => {
                if attempt_index + 1 < OPENAI_REVOCATION_RETRY_ATTEMPTS {
                    tokio::time::sleep(OPENAI_REVOCATION_RETRY_DELAY).await;
                    continue;
                }

                return Err(error).with_context(|| {
                    format!(
                        "OpenAI OAuth revocation request failed for host {}",
                        revocation_endpoint.host_str().unwrap_or("<unknown>")
                    )
                });
            }
        }
    }

    Err(anyhow!("OpenAI OAuth revocation failed after exhausting retries"))
}

/// Renders the self-contained HTML page returned to the browser after an
/// OAuth callback or device-flow poll.
///
/// When `payload_json` is provided, the page posts it to `window.opener`
/// (same-origin only) so the console UI can finish the flow, then closes
/// itself.
pub(crate) fn render_callback_page(title: &str, body: &str, payload_json: Option<&str>) -> String {
    let escaped_title = html_escape(title);
    let escaped_body = html_escape(body);
    let post_message_script = payload_json.map_or_else(String::new, |payload| {
        let safe_payload = escape_json_for_script_tag(payload);
        format!(
            "if (window.opener && !window.opener.closed) {{ try {{ window.opener.postMessage({safe_payload}, window.location.origin); window.close(); }} catch (_error) {{ }} }}"
        )
    });
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{escaped_title}</title><style>body{{font-family:ui-sans-serif,system-ui,sans-serif;background:#f7f4ec;color:#1f2933;margin:0;padding:32px}}main{{max-width:560px;margin:0 auto;background:#fff;border:1px solid #d8d1c2;border-radius:16px;padding:24px;box-shadow:0 20px 60px rgba(31,41,51,.08)}}h1{{margin-top:0;font-size:1.4rem}}p{{line-height:1.5}}code{{background:#f2ede2;padding:2px 6px;border-radius:6px}}</style></head><body><main><h1>{escaped_title}</h1><p>{escaped_body}</p><p>You can return to Palyra now.</p></main><script>{post_message_script}</script></body></html>"
    )
}

// The payload is inlined inside a <script> element, so `<`, `>`, and `&` must
// become \u escapes to block `</script>` breakout, and U+2028/U+2029 must be
// escaped because they are line terminators in JavaScript source but not in
// JSON.
fn escape_json_for_script_tag(raw_json: &str) -> String {
    raw_json
        .chars()
        .map(|ch| match ch {
            '&' => "\\u0026".to_owned(),
            '<' => "\\u003c".to_owned(),
            '>' => "\\u003e".to_owned(),
            '\u{2028}' => "\\u2028".to_owned(),
            '\u{2029}' => "\\u2029".to_owned(),
            _ => ch.to_string(),
        })
        .collect()
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

fn load_openai_oauth_endpoint_from_env(
    env_name: &str,
    default_value: &str,
    label: &str,
) -> Result<Url> {
    let raw = env::var(env_name)
        .ok()
        .and_then(|value| normalize_optional_text(value.as_str()).map(str::to_owned))
        .unwrap_or_else(|| default_value.to_owned());
    parse_openai_oauth_endpoint(raw.as_str(), label)
        .with_context(|| format!("invalid OpenAI OAuth {label} from {env_name}"))
}

fn parse_openai_oauth_endpoint(raw: &str, label: &str) -> Result<Url> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("OpenAI OAuth {label} cannot be empty");
    }
    let parsed = Url::parse(trimmed)
        .with_context(|| format!("OpenAI OAuth {label} must be a valid absolute URL"))?;
    let host =
        parsed.host_str().ok_or_else(|| anyhow!("OpenAI OAuth {label} must include a host"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("OpenAI OAuth {label} must not include embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("OpenAI OAuth {label} must not include query or fragment");
    }
    // Plain http is tolerated only for loopback hosts (RFC 8252 native-app
    // guidance), which keeps local test servers usable without weakening the
    // https requirement for real endpoints.
    let loopback_http_allowed = host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_http_allowed) {
        anyhow::bail!(
            "OpenAI OAuth {label} must use https (http is only allowed for loopback hosts)"
        );
    }
    Ok(parsed)
}

fn openai_models_endpoint(base_url: &str) -> Result<Url> {
    let mut normalized = base_url.trim().to_owned();
    // Url::join drops the last path segment of a base without a trailing
    // slash, which would turn ".../v1" + "models" into ".../models".
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
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
    };

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

    #[test]
    fn parse_openai_oauth_endpoint_rejects_query_and_fragment() {
        let query_error = parse_openai_oauth_endpoint(
            "https://auth.openai.com/authorize?client_secret=secret",
            "authorization endpoint",
        )
        .expect_err("query-bearing authorization endpoint must be rejected");
        let fragment_error = parse_openai_oauth_endpoint(
            "https://auth.openai.com/authorize#secret",
            "authorization endpoint",
        )
        .expect_err("fragment-bearing authorization endpoint must be rejected");
        assert!(query_error.to_string().contains("query or fragment"));
        assert!(fragment_error.to_string().contains("query or fragment"));
    }

    #[test]
    fn parse_openai_oauth_endpoint_rejects_embedded_credentials() {
        let error = parse_openai_oauth_endpoint(
            "https://user:secret@auth.openai.com/oauth/token",
            "token endpoint",
        )
        .expect_err("credential-bearing token endpoint must be rejected");
        assert!(error.to_string().contains("embedded credentials"));
    }

    #[test]
    fn render_callback_page_escapes_script_breakout_sequences_in_payload_json() {
        let html = render_callback_page(
            "OpenAI OAuth callback",
            "You can return to Palyra now.",
            Some(r#"{"message":"bad </script><script>alert(1)</script>"}"#),
        );

        assert!(!html.contains("</script><script>alert(1)</script>"));
        assert!(
            html.contains(r#"\u003c/script\u003e\u003cscript\u003ealert(1)\u003c/script\u003e"#),
            "callback payload should remain script-safe: {html}"
        );
    }

    #[tokio::test]
    async fn revoke_openai_token_retries_transient_server_errors() {
        let server = RevocationMockServer::new(vec!["503 Service Unavailable", "200 OK"]);
        let endpoint = Url::parse(format!("{}/oauth/revoke", server.base_url()).as_str())
            .expect("mock revocation endpoint should parse");

        revoke_openai_token(
            &endpoint,
            "client-live-123",
            "client-secret-live",
            "refresh-token-live",
            Duration::from_secs(2),
        )
        .await
        .expect("revocation should retry a transient 503");

        let requests = server.request_bodies();
        assert_eq!(requests.len(), 2, "revocation should retry exactly once");
        assert!(
            requests.iter().all(|body| {
                body.contains("client_id=client-live-123")
                    && body.contains("client_secret=client-secret-live")
                    && body.contains("token=refresh-token-live")
            }),
            "revocation retry should preserve the same request body: {requests:?}"
        );
    }

    struct RevocationMockServer {
        address: String,
        request_bodies: Arc<Mutex<Vec<String>>>,
        worker: Option<JoinHandle<()>>,
    }

    impl RevocationMockServer {
        fn new(statuses: Vec<&'static str>) -> Self {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("revocation mock listener should bind");
            let address = listener
                .local_addr()
                .expect("revocation mock listener should expose a local address");
            let request_bodies = Arc::new(Mutex::new(Vec::new()));
            let request_bodies_for_thread = Arc::clone(&request_bodies);
            let worker = thread::spawn(move || {
                for status in statuses {
                    let (mut stream, _) =
                        listener.accept().expect("revocation mock should accept a request");
                    let body = read_http_body(&mut stream)
                        .expect("revocation mock should read the request body");
                    request_bodies_for_thread
                        .lock()
                        .expect("revocation mock request log should lock")
                        .push(body);
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                    )
                    .expect("revocation mock should write the HTTP response");
                    stream.flush().expect("revocation mock response should flush");
                }
            });
            Self { address: address.to_string(), request_bodies, worker: Some(worker) }
        }

        fn base_url(&self) -> String {
            format!("http://{}", self.address)
        }

        fn request_bodies(&self) -> Vec<String> {
            self.request_bodies.lock().expect("revocation mock request log should lock").clone()
        }
    }

    impl Drop for RevocationMockServer {
        fn drop(&mut self) {
            if let Some(worker) = self.worker.take() {
                worker.join().expect("revocation mock worker should stop cleanly");
            }
        }
    }

    fn read_http_body(stream: &mut TcpStream) -> Result<String> {
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .context("revocation mock request should include a request line")?;
        if request_line.is_empty() {
            anyhow::bail!("revocation mock request is missing a request line");
        }

        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).context("revocation mock should read a header line")?;
            let line = line.trim_end_matches(&['\r', '\n'][..]);
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.trim().eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse::<usize>().with_context(|| {
                        format!("invalid revocation mock content-length header: {value}")
                    })?;
                }
            }
        }

        let mut body = vec![0u8; content_length];
        reader
            .read_exact(&mut body)
            .context("revocation mock should read the full request body")?;
        String::from_utf8(body).context("revocation mock body should be valid UTF-8")
    }
}
