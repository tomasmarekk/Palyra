use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::{app, env, DEFAULT_CHANNEL, DEFAULT_DAEMON_URL, DEFAULT_DEVICE_ID};

#[derive(Debug, Clone)]
pub(crate) struct ChannelRequestContext {
    pub base_url: String,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub trace_id: Option<String>,
}

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

fn normalize_default_override(value: String, default_value: &str) -> Option<String> {
    if value == default_value {
        None
    } else {
        Some(value)
    }
}

pub(crate) fn build_client() -> Result<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .context("failed to build channels HTTP client")
}

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
        let message = response
            .text()
            .map(|body| channel_error_message(body.as_str(), fallback.as_str()))
            .unwrap_or(fallback);
        anyhow::bail!(
            "channels endpoint returned non-success status: HTTP {}: {}",
            status.as_u16(),
            message
        );
    }
    response.json().context("failed to parse channels endpoint JSON payload")
}

fn channel_error_message(body: &str, fallback: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return fallback.to_owned();
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(message) = value.get("error").and_then(Value::as_str) {
            return message.to_owned();
        }
        if let Some(message) = value.get("message").and_then(Value::as_str) {
            return message.to_owned();
        }
    }
    const MAX_ERROR_BODY_CHARS: usize = 512;
    if trimmed.chars().count() <= MAX_ERROR_BODY_CHARS {
        return trimmed.to_owned();
    }
    let mut truncated = trimmed.chars().take(MAX_ERROR_BODY_CHARS).collect::<String>();
    truncated.push_str("...");
    truncated
}

#[cfg(test)]
mod tests {
    use super::channel_error_message;

    #[test]
    fn channel_error_message_prefers_control_plane_error_body() {
        let message = channel_error_message(
            r#"{"error":"connector 'echo:default' is internal_test_only; run `palyra message capabilities echo:default`"}"#,
            "412 Precondition Failed",
        );

        assert!(message.contains("internal_test_only"));
        assert!(message.contains("message capabilities echo:default"));
    }
}
