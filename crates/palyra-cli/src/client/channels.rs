//! Blocking HTTP transport for the daemon channels admin endpoints.
//!
//! Resolves connection identity (profile context first, env/defaults second),
//! attaches the identity headers, and turns non-success responses into bounded,
//! human-readable error messages.

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;
use std::io::Read;

use crate::{app, env, DEFAULT_CHANNEL, DEFAULT_DAEMON_URL, DEFAULT_DEVICE_ID};

// Error bodies are operator-facing diagnostics, not payloads: cap both the
// bytes buffered from the wire and the characters surfaced in the message so a
// misbehaving endpoint cannot bloat CLI errors.
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
const MAX_ERROR_MESSAGE_CHARS: usize = 512;

/// Resolved connection target and identity headers for one channels request.
#[derive(Debug, Clone)]
pub(crate) struct ChannelRequestContext {
    pub base_url: String,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub trace_id: Option<String>,
}

/// Resolves the channels request context from the CLI root context when one is
/// active, otherwise directly from env vars and built-in defaults.
///
/// # Errors
/// Returns an error when the root context fails to resolve the HTTP connection.
pub(crate) fn resolve_request_context(
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
) -> Result<ChannelRequestContext> {
    if let Some(root_context) = app::current_root_context() {
        let connection = root_context.resolve_http_connection(
            app::ConnectionOverrides {
                daemon_url: url,
                token,
                principal: normalize_default_override(
                    principal,
                    app::ConnectionDefaults::USER.principal,
                ),
                device_id: normalize_default_override(device_id, DEFAULT_DEVICE_ID),
                channel,
                ..Default::default()
            },
            app::ConnectionDefaults::USER,
        )?;
        return Ok(ChannelRequestContext {
            base_url: connection.base_url,
            token: connection.token,
            principal: connection.principal,
            device_id: connection.device_id,
            channel: Some(connection.channel),
            trace_id: Some(connection.trace_id),
        });
    }

    Ok(ChannelRequestContext {
        base_url: url
            .or_else(|| env::var("PALYRA_DAEMON_URL").ok())
            .unwrap_or_else(|| DEFAULT_DAEMON_URL.to_owned()),
        token: token.or_else(|| env::var("PALYRA_ADMIN_TOKEN").ok()),
        principal,
        device_id,
        channel: channel.or_else(|| Some(DEFAULT_CHANNEL.to_owned())),
        trace_id: None,
    })
}

// Clap always supplies the built-in default, so an unchanged value must be
// treated as "no override" or it would shadow profile-configured identity.
fn normalize_default_override(value: String, default_value: &str) -> Option<String> {
    if value == default_value {
        None
    } else {
        Some(value)
    }
}

/// Builds the blocking HTTP client used for channels admin requests.
///
/// # Errors
/// Returns an error when the client cannot be constructed.
pub(crate) fn build_client() -> Result<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .context("failed to build channels HTTP client")
}

/// Sends a prepared channels request with identity headers attached and parses
/// the JSON response.
///
/// # Errors
/// Returns an error when the request fails, the endpoint responds with a
/// non-success status (including the bounded response message), or the success
/// payload is not valid JSON.
pub(crate) fn send_request(
    request: reqwest::blocking::RequestBuilder,
    context: ChannelRequestContext,
    error_context: &'static str,
) -> Result<Value> {
    let mut request = request
        .header("x-palyra-principal", context.principal)
        .header("x-palyra-device-id", context.device_id);
    if let Some(token) = context.token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(channel) = context.channel {
        request = request.header("x-palyra-channel", channel);
    }
    if let Some(trace_id) = context.trace_id {
        request = request.header("x-palyra-trace-id", trace_id);
    }
    let response = request.send().context(error_context)?;
    let status = response.status();
    if !status.is_success() {
        let fallback = status.to_string();
        let message = channel_error_message_from_response(response, fallback.as_str());
        anyhow::bail!(
            "channels endpoint returned non-success status: HTTP {}: {}",
            status.as_u16(),
            message
        );
    }
    response.json().context("failed to parse channels endpoint JSON payload")
}

struct LimitedErrorBody {
    bytes: Vec<u8>,
    truncated: bool,
}

fn channel_error_message_from_response(
    response: reqwest::blocking::Response,
    fallback: &str,
) -> String {
    let Some(body) = read_limited_error_body(response) else {
        return fallback.to_owned();
    };
    let message = String::from_utf8_lossy(body.bytes.as_slice());
    channel_error_message(message.as_ref(), fallback, body.truncated)
}

fn read_limited_error_body<R: Read>(reader: R) -> Option<LimitedErrorBody> {
    // Read one byte past the cap so truncation can be detected without
    // buffering an unbounded body.
    let mut limited = reader.take((MAX_ERROR_BODY_BYTES as u64).saturating_add(1));
    let mut bytes = Vec::new();
    limited.read_to_end(&mut bytes).ok()?;
    let truncated = bytes.len() > MAX_ERROR_BODY_BYTES;
    if truncated {
        bytes.truncate(MAX_ERROR_BODY_BYTES);
    }
    Some(LimitedErrorBody { bytes, truncated })
}

// Prefer the structured "error"/"message" fields the control plane emits;
// fall back to the raw (trimmed, capped) body, then to the HTTP status text.
fn channel_error_message(body: &str, fallback: &str, body_truncated: bool) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return fallback.to_owned();
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(message) = value.get("error").and_then(Value::as_str) {
            return truncate_error_message(message, body_truncated);
        }
        if let Some(message) = value.get("message").and_then(Value::as_str) {
            return truncate_error_message(message, body_truncated);
        }
    }
    truncate_error_message(trimmed, body_truncated)
}

fn truncate_error_message(message: &str, body_truncated: bool) -> String {
    // A truncated wire body always gets an ellipsis, even when the extracted
    // message is short, so operators know the diagnostic is incomplete.
    if !body_truncated && message.chars().count() <= MAX_ERROR_MESSAGE_CHARS {
        return message.to_owned();
    }
    let mut truncated = message.chars().take(MAX_ERROR_MESSAGE_CHARS).collect::<String>();
    truncated.push_str("...");
    truncated
}

#[cfg(test)]
mod tests {
    use super::{
        channel_error_message, read_limited_error_body, MAX_ERROR_BODY_BYTES,
        MAX_ERROR_MESSAGE_CHARS,
    };
    use std::io::Cursor;

    #[test]
    fn channel_error_message_prefers_control_plane_error_body() {
        let message = channel_error_message(
            r#"{"error":"connector 'echo:default' is internal_test_only; run `palyra message capabilities echo:default`"}"#,
            "412 Precondition Failed",
            false,
        );

        assert!(message.contains("internal_test_only"));
        assert!(message.contains("message capabilities echo:default"));
    }

    #[test]
    fn channel_error_message_caps_json_error_body_text() {
        let oversized_error = "x".repeat(MAX_ERROR_MESSAGE_CHARS + 64);
        let body = serde_json::json!({ "error": oversized_error }).to_string();

        let message = channel_error_message(body.as_str(), "500 Internal Server Error", false);

        assert_eq!(message.chars().count(), MAX_ERROR_MESSAGE_CHARS + 3);
        assert!(message.ends_with("..."));
    }

    #[test]
    fn channel_error_body_reader_caps_buffered_bytes() {
        let body = vec![b'a'; MAX_ERROR_BODY_BYTES + 1024];

        let limited = read_limited_error_body(Cursor::new(body))
            .expect("bounded error body reader should succeed");

        assert_eq!(limited.bytes.len(), MAX_ERROR_BODY_BYTES);
        assert!(limited.truncated);
    }
}
