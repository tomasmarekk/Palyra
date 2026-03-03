use std::{
    collections::{HashMap, VecDeque},
    env,
    hash::{Hash, Hasher},
    net::ToSocketAddrs,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use palyra_common::redaction::redact_auth_error;
use reqwest::{redirect::Policy, Client, Url};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ulid::Ulid;

use crate::{
    net::ConnectorNetGuard,
    protocol::{
        AttachmentKind, AttachmentRef, ConnectorKind, DeliveryOutcome, InboundMessageEvent,
        OutboundMessageRequest, RetryClass,
    },
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

const DISCORD_DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_DEFAULT_TIMEOUT_MS: u64 = 15_000;
const DISCORD_MAX_MESSAGE_CHARS: usize = 2_000;
const DISCORD_MAX_MESSAGE_LINES: usize = 17;
const DISCORD_GATEWAY_VERSION: &str = "10";
const DISCORD_GATEWAY_ENCODING: &str = "json";
const DISCORD_GATEWAY_INTENTS: u64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);
const DISCORD_GATEWAY_HEARTBEAT_FALLBACK_MS: u64 = 45_000;
const DISCORD_GATEWAY_MONITOR_MIN_BACKOFF_MS: u64 = 1_000;
const DISCORD_GATEWAY_MONITOR_MAX_BACKOFF_MS: u64 = 60_000;
const DISCORD_GATEWAY_MONITOR_JITTER_MAX_MS: u64 = 500;
const DISCORD_INBOUND_BUFFER_CAPACITY: usize = 512;
const IDENTITY_CACHE_TTL_MS: i64 = 5 * 60 * 1_000;
const MAX_DELIVERY_CACHE: usize = 4_096;
const MAX_ROUTE_LIMIT_CACHE: usize = 256;
const DEFAULT_MIN_RATE_LIMIT_RETRY_MS: u64 = 250;
const DISCORD_FALLBACK_ALLOWLIST: [&str; 8] = [
    "discord.com",
    "*.discord.com",
    "discordapp.com",
    "*.discordapp.com",
    "discord.gg",
    "*.discord.gg",
    "discordapp.net",
    "*.discordapp.net",
];

#[derive(Debug, Clone)]
pub struct DiscordAdapterConfig {
    pub api_base_url: Url,
    pub request_timeout_ms: u64,
    pub max_chunk_chars: usize,
    pub max_chunk_lines: usize,
    pub enable_auto_reactions: bool,
    pub enable_inbound_gateway: bool,
    pub inbound_buffer_capacity: usize,
}

#[async_trait]
impl ConnectorAdapter for DiscordConnectorAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind::Discord
    }

    fn split_outbound(
        &self,
        _instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<Vec<OutboundMessageRequest>, ConnectorAdapterError> {
        let rendered_text =
            with_attachment_context(request.text.as_str(), request.attachments.as_slice());
        let chunks = chunk_discord_text(
            rendered_text.as_str(),
            self.config.max_chunk_chars,
            self.config.max_chunk_lines,
        );
        if chunks.is_empty() {
            return Err(ConnectorAdapterError::Backend(
                "discord outbound payload became empty after chunking".to_owned(),
            ));
        }
        if chunks.len() == 1 {
            let mut single = request.clone();
            single.text = rendered_text;
            return Ok(vec![single]);
        }

        let mut split = Vec::with_capacity(chunks.len());
        for (index, chunk) in chunks.into_iter().enumerate() {
            let mut next = request.clone();
            next.text = chunk;
            if index > 0 {
                next.envelope_id = format!("{}:chunk{index}", request.envelope_id);
            }
            split.push(next);
        }
        Ok(split)
    }

    fn runtime_snapshot(&self, _instance: &ConnectorInstanceRecord) -> Option<Value> {
        let state = self.lock_state().ok()?;
        let mut routes = state
            .route_limits
            .iter()
            .map(|(route, window)| {
                json!({
                    "route": route,
                    "bucket_id": window.bucket_id,
                    "blocked_until_unix_ms": window.blocked_until_unix_ms,
                })
            })
            .collect::<Vec<_>>();
        routes.sort_by(|left, right| {
            left.get("route")
                .and_then(Value::as_str)
                .cmp(&right.get("route").and_then(Value::as_str))
        });

        Some(json!({
            "credential": {
                "source": state.credential_source,
                "token_suffix": state.token_suffix,
            },
            "bot_identity": state.bot_identity.as_ref().map(|identity| {
                json!({
                    "id": identity.id,
                    "username": identity.username,
                })
            }),
            "last_error": state.last_error,
            "global_rate_limit_until_unix_ms": state.global_blocked_until_unix_ms,
            "route_rate_limits": routes,
            "idempotency_cache_size": state.delivered_native_ids.len(),
            "inbound": {
                "last_inbound_unix_ms": state.last_inbound_unix_ms,
                "gateway_connected": state.gateway_connected,
                "last_connect_unix_ms": state.gateway_last_connect_unix_ms,
                "last_disconnect_unix_ms": state.gateway_last_disconnect_unix_ms,
                "last_event_type": state.gateway_last_event_type,
            },
        }))
    }

    async fn poll_inbound(
        &self,
        instance: &ConnectorInstanceRecord,
        limit: usize,
    ) -> Result<Vec<InboundMessageEvent>, ConnectorAdapterError> {
        if !self.config.enable_inbound_gateway {
            return Ok(Vec::new());
        }
        let max_events = limit.max(1);
        self.ensure_inbound_monitor(instance).await?;
        self.drain_inbound_events(instance.connector_id.as_str(), max_events)
    }

    async fn send_outbound(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &OutboundMessageRequest,
    ) -> Result<DeliveryOutcome, ConnectorAdapterError> {
        if let Some(native_message_id) = self.cached_delivery(request.envelope_id.as_str())? {
            return Ok(DeliveryOutcome::Delivered { native_message_id });
        }

        let conversation_id = match normalize_discord_target(request.conversation_id.as_str()) {
            Ok(value) => value,
            Err(error) => {
                self.record_last_error(error.to_string().as_str());
                return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
            }
        };

        let target_channel_id = match request
            .reply_thread_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(thread_id) => match normalize_discord_target(thread_id) {
                Ok(value) => value,
                Err(error) => {
                    self.record_last_error(error.to_string().as_str());
                    return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
                }
            },
            None => conversation_id.clone(),
        };

        let route_key = format!("discord:post:/channels/{target_channel_id}/messages");
        let now_unix_ms = unix_ms_now();
        if let Some(retry_after_ms) =
            self.preflight_retry_after_ms(route_key.as_str(), now_unix_ms)?
        {
            let reason =
                "discord outbound deferred due to local route/global rate-limit budget".to_owned();
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason,
                retry_after_ms: Some(retry_after_ms),
            });
        }

        let credential = match self.credential_resolver.resolve_credential(instance).await {
            Ok(credential) => {
                self.record_credential_metadata(&credential);
                credential
            }
            Err(error) => {
                let reason = redact_auth_error(error.to_string().as_str());
                self.record_last_error(reason.as_str());
                return Ok(DeliveryOutcome::PermanentFailure { reason });
            }
        };

        let guard = match self.build_net_guard(instance) {
            Ok(guard) => guard,
            Err(error) => {
                self.record_last_error(error.to_string().as_str());
                return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
            }
        };

        if let Err(error) = self.validate_url_target(&guard, &self.config.api_base_url) {
            self.record_last_error(error.to_string().as_str());
            return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
        }

        if let Some(outcome) = self.ensure_bot_identity(&guard, &credential).await? {
            return Ok(outcome);
        }

        let message_url =
            match build_messages_url(&self.config.api_base_url, target_channel_id.as_str()) {
                Ok(url) => url,
                Err(error) => {
                    self.record_last_error(error.to_string().as_str());
                    return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
                }
            };
        if let Err(error) = self.validate_url_target(&guard, &message_url) {
            self.record_last_error(error.to_string().as_str());
            return Ok(DeliveryOutcome::PermanentFailure { reason: error.to_string() });
        }

        let payload = build_discord_message_payload(request);
        let response = match self
            .transport
            .post_json(
                &message_url,
                credential.token.as_str(),
                &payload,
                self.config.request_timeout_ms,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let reason = redact_auth_error(error.to_string().as_str());
                self.record_last_error(reason.as_str());
                return Ok(DeliveryOutcome::Retry {
                    class: RetryClass::TransientNetwork,
                    reason,
                    retry_after_ms: None,
                });
            }
        };

        let now_unix_ms = unix_ms_now();
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, now_unix_ms)?;

        if response.status == 429 {
            let retry_after_ms = snapshot
                .retry_after_ms
                .or(snapshot.reset_after_ms)
                .unwrap_or(1_000)
                .max(DEFAULT_MIN_RATE_LIMIT_RETRY_MS);
            let reason = format!(
                "discord rate-limited outbound send: {}",
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "retry later".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason,
                retry_after_ms: Some(retry_after_ms),
            });
        }

        if response.status == 401 || response.status == 403 {
            let reason = format!(
                "discord authentication failed during outbound send (status={}): {}",
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unauthorized".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            });
        }

        if response.status >= 500 {
            let reason = format!(
                "discord upstream transient error during outbound send (status={})",
                response.status
            );
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::TransientNetwork,
                reason,
                retry_after_ms: None,
            });
        }

        if !(200..300).contains(&response.status) {
            let reason = format!(
                "discord outbound send failed (status={}): {}",
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            });
        }

        let native_message_id = parse_discord_message_id(response.body.as_str())
            .unwrap_or_else(|| fallback_native_message_id(request));
        self.remember_delivery(request.envelope_id.as_str(), native_message_id.as_str())?;
        self.clear_last_error();

        if self.config.enable_auto_reactions {
            if let Some(auto_reaction) = request.auto_reaction.as_deref() {
                self.send_auto_reaction(
                    &guard,
                    &credential,
                    target_channel_id.as_str(),
                    native_message_id.as_str(),
                    auto_reaction,
                )
                .await;
            }
        }

        Ok(DeliveryOutcome::Delivered { native_message_id })
    }
}

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

fn token_suffix(token: &str) -> Option<String> {
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

fn normalize_discord_target(raw: &str) -> Result<String, ConnectorAdapterError> {
    let trimmed = raw.trim();
    let normalized = trimmed
        .strip_prefix("channel:")
        .or_else(|| trimmed.strip_prefix("thread:"))
        .map(str::trim)
        .unwrap_or(trimmed);
    if normalized.is_empty() {
        return Err(ConnectorAdapterError::Backend(
            "discord target conversation id cannot be empty".to_owned(),
        ));
    }
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.' | '/'))
    {
        return Err(ConnectorAdapterError::Backend(
            "discord target conversation id contains unsupported characters".to_owned(),
        ));
    }
    Ok(normalized.to_owned())
}

fn build_users_me_url(api_base: &Url) -> Result<Url, ConnectorAdapterError> {
    let candidate = format!("{}/users/@me", api_base.as_str().trim_end_matches('/'));
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

fn build_messages_url(api_base: &Url, channel_id: &str) -> Result<Url, ConnectorAdapterError> {
    let candidate =
        format!("{}/channels/{}/messages", api_base.as_str().trim_end_matches('/'), channel_id);
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

fn build_reaction_url(
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

fn percent_encode_component(raw: &str) -> String {
    let mut encoded = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(format!("{byte:02X}").as_str());
        }
    }
    encoded
}

fn build_discord_message_payload(request: &OutboundMessageRequest) -> Value {
    let mut payload = json!({
        "content": request.text,
    });
    if let Some(in_reply_to_message_id) =
        request.in_reply_to_message_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        payload["message_reference"] = json!({
            "message_id": in_reply_to_message_id,
            "fail_if_not_exists": false,
        });
    }
    payload
}

fn parse_discord_message_id(raw_body: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw_body)
        .ok()
        .and_then(|payload| payload.get("id").and_then(Value::as_str).map(ToOwned::to_owned))
}

fn fallback_native_message_id(request: &OutboundMessageRequest) -> String {
    let fingerprint = json!({
        "envelope_id": request.envelope_id,
        "connector_id": request.connector_id,
        "conversation_id": request.conversation_id,
        "text": request.text,
        "thread_id": request.reply_thread_id,
    });
    format!("discord-{:016x}", deterministic_hash(&fingerprint.to_string()))
}

fn deterministic_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn parse_rate_limit_snapshot(response: &DiscordTransportResponse) -> RateLimitSnapshot {
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
    let Some(payload) = serde_json::from_str::<Value>(raw_body).ok() else {
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
    Some(millis as u64)
}

fn parse_discord_error_summary(raw_body: &str) -> Option<String> {
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

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
struct OpenFence {
    indent: String,
    marker: char,
    marker_len: usize,
    open_line: String,
}

fn chunk_discord_text(text: &str, max_chars: usize, max_lines: usize) -> Vec<String> {
    let body = text.trim_end_matches('\r');
    if body.trim().is_empty() {
        return Vec::new();
    }

    let max_chars = max_chars.max(1);
    let max_lines = max_lines.max(1);
    if char_len(body) <= max_chars && count_lines(body) <= max_lines {
        return vec![body.to_owned()];
    }

    let lines = body.split('\n').collect::<Vec<_>>();
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_lines = 0_usize;
    let mut open_fence: Option<OpenFence> = None;

    for line in lines {
        let fence_info = parse_fence_line(line);
        let was_inside_fence = open_fence.is_some();
        let mut next_open_fence = open_fence.clone();
        if let Some(fence_info) = fence_info {
            if open_fence.is_none() {
                next_open_fence = Some(fence_info);
            } else if let Some(existing) = open_fence.as_ref() {
                if existing.marker == fence_info.marker
                    && fence_info.marker_len >= existing.marker_len
                {
                    next_open_fence = None;
                }
            }
        }

        let reserve_chars = next_open_fence
            .as_ref()
            .map(|fence| char_len(close_fence_line(fence).as_str()) + 1)
            .unwrap_or(0);
        let reserve_lines = if next_open_fence.is_some() { 1 } else { 0 };

        let char_limit = max_chars.saturating_sub(reserve_chars).max(1);
        let line_limit = max_lines.saturating_sub(reserve_lines).max(1);
        let prefix_len = if current.is_empty() { 0 } else { char_len(current.as_str()) + 1 };
        let segment_limit = char_limit.saturating_sub(prefix_len).max(1);
        let segments = split_long_line(line, segment_limit, was_inside_fence);

        for (segment_index, segment) in segments.iter().enumerate() {
            let is_continuation = segment_index > 0;
            let delimiter = if is_continuation || current.is_empty() { "" } else { "\n" };
            let addition = format!("{delimiter}{segment}");
            let next_length = char_len(current.as_str()) + char_len(addition.as_str());
            let next_line_count = current_lines + if is_continuation { 0 } else { 1 };
            if (next_length > char_limit || next_line_count > line_limit) && !current.is_empty() {
                flush_chunk(&mut current, &mut current_lines, &open_fence, &mut chunks);
            }

            if current.is_empty() {
                current.push_str(segment);
                current_lines = 1;
            } else {
                current.push_str(delimiter);
                current.push_str(segment);
                if !is_continuation {
                    current_lines = current_lines.saturating_add(1);
                }
            }
        }

        open_fence = next_open_fence;
    }

    if !current.is_empty() {
        let payload = ensure_balanced_fences(close_fence_if_needed(current.as_str(), &open_fence));
        if !payload.trim().is_empty() {
            chunks.push(payload);
        }
    }

    chunks
}

fn flush_chunk(
    current: &mut String,
    current_lines: &mut usize,
    open_fence: &Option<OpenFence>,
    chunks: &mut Vec<String>,
) {
    if current.is_empty() {
        return;
    }
    let payload = ensure_balanced_fences(close_fence_if_needed(current.as_str(), open_fence));
    if !payload.trim().is_empty() {
        chunks.push(payload);
    }
    current.clear();
    *current_lines = 0;
    if let Some(open_fence) = open_fence {
        current.push_str(open_fence.open_line.as_str());
        *current_lines = 1;
    }
}

fn parse_fence_line(line: &str) -> Option<OpenFence> {
    let mut chars = line.chars().peekable();
    let mut indent = String::new();
    while indent.len() < 3 {
        let Some(next) = chars.peek().copied() else {
            break;
        };
        if next == ' ' {
            indent.push(next);
            chars.next();
            continue;
        }
        break;
    }
    let marker = chars.peek().copied()?;
    if marker != '`' && marker != '~' {
        return None;
    }
    let mut marker_len = 0_usize;
    while let Some(next) = chars.peek().copied() {
        if next == marker {
            marker_len = marker_len.saturating_add(1);
            chars.next();
        } else {
            break;
        }
    }
    if marker_len < 3 {
        return None;
    }
    Some(OpenFence { indent, marker, marker_len, open_line: line.to_owned() })
}

fn close_fence_line(open_fence: &OpenFence) -> String {
    format!("{}{}", open_fence.indent, open_fence.marker.to_string().repeat(open_fence.marker_len))
}

fn close_fence_if_needed(text: &str, open_fence: &Option<OpenFence>) -> String {
    let Some(open_fence) = open_fence else {
        return text.to_owned();
    };
    let close_line = close_fence_line(open_fence);
    if text.is_empty() {
        return close_line;
    }
    if text.ends_with('\n') {
        format!("{text}{close_line}")
    } else {
        format!("{text}\n{close_line}")
    }
}

fn ensure_balanced_fences(chunk: String) -> String {
    let mut open_fence: Option<OpenFence> = None;
    for line in chunk.split('\n') {
        let Some(fence_info) = parse_fence_line(line) else {
            continue;
        };
        if open_fence.is_none() {
            open_fence = Some(fence_info);
            continue;
        }
        if let Some(existing) = open_fence.as_ref() {
            if existing.marker == fence_info.marker && fence_info.marker_len >= existing.marker_len
            {
                open_fence = None;
            }
        }
    }
    let Some(open_fence) = open_fence else {
        return chunk;
    };
    let close_line = close_fence_line(&open_fence);
    if chunk.ends_with('\n') {
        format!("{chunk}{close_line}")
    } else {
        format!("{chunk}\n{close_line}")
    }
}

fn split_long_line(line: &str, limit: usize, preserve_whitespace: bool) -> Vec<String> {
    let limit = limit.max(1);
    if char_len(line) <= limit {
        return vec![line.to_owned()];
    }

    let chars = line.chars().collect::<Vec<_>>();
    let mut out = Vec::new();
    let mut start = 0_usize;
    while start < chars.len() {
        let remaining = chars.len().saturating_sub(start);
        if remaining <= limit {
            out.push(chars[start..].iter().collect::<String>());
            break;
        }
        let window_end = start.saturating_add(limit);
        if preserve_whitespace {
            out.push(chars[start..window_end].iter().collect::<String>());
            start = window_end;
            continue;
        }

        let mut split_index = None;
        for index in (start..window_end).rev() {
            if chars[index].is_whitespace() {
                split_index = Some(index);
                break;
            }
        }
        let split_at = split_index.filter(|index| *index > start).unwrap_or(window_end);
        out.push(chars[start..split_at].iter().collect::<String>());
        start = split_at;
    }
    out
}

fn count_lines(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    text.split('\n').count()
}

fn char_len(text: &str) -> usize {
    text.chars().count()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use super::{
        chunk_discord_text, deterministic_inbound_envelope_id, normalize_discord_message_create,
        parse_fence_line, DiscordAdapterConfig, DiscordConnectorAdapter, DiscordCredential,
        DiscordCredentialResolver, DiscordTransport, DiscordTransportResponse, OpenFence,
    };
    use crate::{
        protocol::{
            AttachmentKind, AttachmentRef, ConnectorKind, DeliveryOutcome, OutboundMessageRequest,
            RetryClass,
        },
        storage::ConnectorInstanceRecord,
        supervisor::ConnectorAdapter,
    };
    use async_trait::async_trait;
    use reqwest::Url;
    use serde_json::{json, Value};

    #[derive(Debug, Default)]
    struct StaticCredentialResolver {
        credential: Option<DiscordCredential>,
    }

    #[async_trait]
    impl DiscordCredentialResolver for StaticCredentialResolver {
        async fn resolve_credential(
            &self,
            _instance: &ConnectorInstanceRecord,
        ) -> Result<DiscordCredential, crate::supervisor::ConnectorAdapterError> {
            self.credential.clone().ok_or_else(|| {
                crate::supervisor::ConnectorAdapterError::Backend("missing credential".to_owned())
            })
        }
    }

    #[derive(Debug, Clone)]
    struct CapturedCall {
        method: String,
        url: String,
        payload: Option<Value>,
    }

    #[derive(Default)]
    struct FakeTransport {
        get_responses: Mutex<
            VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>,
        >,
        post_responses: Mutex<
            VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>,
        >,
        put_responses: Mutex<
            VecDeque<Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>>,
        >,
        captured: Mutex<Vec<CapturedCall>>,
    }

    impl FakeTransport {
        fn push_get_response(
            &self,
            response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
        ) {
            self.get_responses
                .lock()
                .expect("get response lock should not be poisoned")
                .push_back(response);
        }

        fn push_post_response(
            &self,
            response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
        ) {
            self.post_responses
                .lock()
                .expect("post response lock should not be poisoned")
                .push_back(response);
        }

        fn push_put_response(
            &self,
            response: Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError>,
        ) {
            self.put_responses
                .lock()
                .expect("put response lock should not be poisoned")
                .push_back(response);
        }

        fn captured(&self) -> Vec<CapturedCall> {
            self.captured.lock().expect("captured lock should not be poisoned").clone()
        }
    }

    #[async_trait]
    impl DiscordTransport for FakeTransport {
        async fn get(
            &self,
            url: &Url,
            _token: &str,
            _timeout_ms: u64,
        ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
            self.captured.lock().expect("captured lock should not be poisoned").push(
                CapturedCall { method: "GET".to_owned(), url: url.to_string(), payload: None },
            );
            self.get_responses
                .lock()
                .expect("get response lock should not be poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(DiscordTransportResponse {
                        status: 200,
                        headers: Default::default(),
                        body: "{}".to_owned(),
                    })
                })
        }

        async fn post_json(
            &self,
            url: &Url,
            _token: &str,
            payload: &Value,
            _timeout_ms: u64,
        ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
            self.captured.lock().expect("captured lock should not be poisoned").push(
                CapturedCall {
                    method: "POST".to_owned(),
                    url: url.to_string(),
                    payload: Some(payload.clone()),
                },
            );
            self.post_responses
                .lock()
                .expect("post response lock should not be poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(DiscordTransportResponse {
                        status: 200,
                        headers: Default::default(),
                        body: "{\"id\":\"native-default\"}".to_owned(),
                    })
                })
        }

        async fn put(
            &self,
            url: &Url,
            _token: &str,
            _timeout_ms: u64,
        ) -> Result<DiscordTransportResponse, crate::supervisor::ConnectorAdapterError> {
            self.captured.lock().expect("captured lock should not be poisoned").push(
                CapturedCall { method: "PUT".to_owned(), url: url.to_string(), payload: None },
            );
            self.put_responses
                .lock()
                .expect("put response lock should not be poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(DiscordTransportResponse {
                        status: 204,
                        headers: Default::default(),
                        body: String::new(),
                    })
                })
        }
    }

    fn sample_instance() -> ConnectorInstanceRecord {
        ConnectorInstanceRecord {
            connector_id: "discord:default".to_owned(),
            kind: ConnectorKind::Discord,
            principal: "channel:discord:default".to_owned(),
            auth_profile_ref: Some("discord.default".to_owned()),
            token_vault_ref: Some("global/discord_bot_token".to_owned()),
            egress_allowlist: vec!["discord.com".to_owned(), "*.discord.com".to_owned()],
            enabled: true,
            readiness: crate::protocol::ConnectorReadiness::Ready,
            liveness: crate::protocol::ConnectorLiveness::Running,
            restart_count: 0,
            last_error: None,
            last_inbound_unix_ms: None,
            last_outbound_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    fn sample_request(text: &str) -> OutboundMessageRequest {
        OutboundMessageRequest {
            envelope_id: "env-1".to_owned(),
            connector_id: "discord:default".to_owned(),
            conversation_id: "1234567890".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: text.to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        }
    }

    fn adapter_with_fake_transport(transport: Arc<FakeTransport>) -> DiscordConnectorAdapter {
        let config = DiscordAdapterConfig {
            request_timeout_ms: 1_000,
            max_chunk_chars: 120,
            max_chunk_lines: 8,
            enable_auto_reactions: true,
            ..DiscordAdapterConfig::default()
        };
        DiscordConnectorAdapter::with_dependencies(
            config,
            transport,
            Arc::new(StaticCredentialResolver {
                credential: Some(DiscordCredential {
                    token: "super-secret-token".to_owned(),
                    source: "test-resolver".to_owned(),
                }),
            }),
        )
    }

    fn ok_identity_response() -> DiscordTransportResponse {
        DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"bot-1\",\"username\":\"palyra\"}".to_owned(),
        }
    }

    #[tokio::test]
    async fn send_outbound_returns_retry_on_429_with_retry_after() {
        let transport = Arc::new(FakeTransport::default());
        transport.push_get_response(Ok(ok_identity_response()));
        transport.push_post_response(Ok(DiscordTransportResponse {
            status: 429,
            headers: [("retry-after".to_owned(), "1.25".to_owned())].into_iter().collect(),
            body: "{\"message\":\"too many requests\",\"retry_after\":1.25,\"global\":true}"
                .to_owned(),
        }));
        let adapter = adapter_with_fake_transport(Arc::clone(&transport));

        let outcome = adapter
            .send_outbound(&sample_instance(), &sample_request("hello"))
            .await
            .expect("send should produce delivery outcome");
        match outcome {
            DeliveryOutcome::Retry { class, retry_after_ms, .. } => {
                assert_eq!(class, RetryClass::RateLimit);
                assert_eq!(retry_after_ms, Some(1_250));
            }
            other => panic!("expected retry outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_outbound_is_idempotent_for_same_envelope() {
        let transport = Arc::new(FakeTransport::default());
        transport.push_get_response(Ok(ok_identity_response()));
        transport.push_post_response(Ok(DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"native-1\"}".to_owned(),
        }));
        let adapter = adapter_with_fake_transport(Arc::clone(&transport));
        let request = sample_request("hello");

        let first = adapter
            .send_outbound(&sample_instance(), &request)
            .await
            .expect("first send should return outcome");
        let second = adapter
            .send_outbound(&sample_instance(), &request)
            .await
            .expect("second send should return outcome");

        let DeliveryOutcome::Delivered { native_message_id: first_id } = first else {
            panic!("first outcome should be delivered");
        };
        let DeliveryOutcome::Delivered { native_message_id: second_id } = second else {
            panic!("second outcome should be delivered");
        };
        assert_eq!(first_id, "native-1");
        assert_eq!(second_id, "native-1");

        let posts = transport
            .captured()
            .into_iter()
            .filter(|entry| entry.method == "POST")
            .collect::<Vec<_>>();
        assert_eq!(posts.len(), 1, "duplicate envelope should not trigger extra POST");
    }

    #[tokio::test]
    async fn send_outbound_sets_thread_reference_and_auto_reaction() {
        let transport = Arc::new(FakeTransport::default());
        transport.push_get_response(Ok(ok_identity_response()));
        transport.push_post_response(Ok(DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"native-2\"}".to_owned(),
        }));
        transport.push_put_response(Ok(DiscordTransportResponse {
            status: 204,
            headers: Default::default(),
            body: String::new(),
        }));
        let adapter = adapter_with_fake_transport(Arc::clone(&transport));
        let mut request = sample_request("hello thread");
        request.reply_thread_id = Some("thread-123".to_owned());
        request.in_reply_to_message_id = Some("origin-42".to_owned());
        request.auto_reaction = Some("✅".to_owned());

        let outcome = adapter
            .send_outbound(&sample_instance(), &request)
            .await
            .expect("thread send should succeed");
        assert!(matches!(outcome, DeliveryOutcome::Delivered { .. }));

        let captured = transport.captured();
        let message_post =
            captured.iter().find(|entry| entry.method == "POST").expect("expected POST call");
        assert!(
            message_post.url.contains("/channels/thread-123/messages"),
            "thread replies must target the thread channel route"
        );
        assert!(
            !message_post.url.contains("thread_id="),
            "discord bot token send should not use thread_id query parameter"
        );
        let payload = message_post.payload.as_ref().expect("POST call should include payload");
        assert_eq!(
            payload
                .get("message_reference")
                .and_then(|value| value.get("message_id"))
                .and_then(Value::as_str),
            Some("origin-42"),
            "message reference should be propagated into payload"
        );

        let reaction_put = captured
            .iter()
            .find(|entry| entry.method == "PUT")
            .expect("expected PUT reaction call");
        assert!(
            reaction_put.url.contains("/reactions/"),
            "auto reaction should hit reaction endpoint"
        );
    }

    #[test]
    fn split_outbound_chunks_large_payload_and_keeps_envelope_deterministic() {
        let transport = Arc::new(FakeTransport::default());
        let adapter = adapter_with_fake_transport(transport);
        let text = (0..100).map(|index| format!("line-{index}")).collect::<Vec<_>>().join("\n");
        let request = sample_request(text.as_str());
        let chunks =
            adapter.split_outbound(&sample_instance(), &request).expect("split should succeed");
        assert!(chunks.len() > 1);
        assert_eq!(chunks[0].envelope_id, "env-1");
        assert!(
            chunks[1].envelope_id.starts_with("env-1:chunk"),
            "secondary chunks should use deterministic chunk suffix"
        );
        for chunk in chunks {
            assert!(
                chunk.text.chars().count() <= 120,
                "chunk text should respect max char configuration"
            );
        }
    }

    #[test]
    fn split_outbound_embeds_attachment_metadata_context() {
        let transport = Arc::new(FakeTransport::default());
        let adapter = adapter_with_fake_transport(transport);
        let mut request = sample_request("reply with attachment context");
        request.attachments = vec![AttachmentRef {
            kind: AttachmentKind::Image,
            url: Some("u".to_owned()),
            artifact_ref: None,
            filename: Some("a".to_owned()),
            content_type: Some("i".to_owned()),
            size_bytes: Some(1),
        }];

        let chunks = adapter
            .split_outbound(&sample_instance(), &request)
            .expect("split should succeed with attachment metadata");
        assert_eq!(chunks.len(), 1, "short payload should stay in one chunk");
        let rendered = &chunks[0].text;
        assert!(
            rendered.contains("[attachment-metadata]"),
            "attachment metadata marker should be appended to outbound text"
        );
        assert!(
            rendered.contains("kind=image, filename=a, content_type=i, size_bytes=1, source=u"),
            "attachment metadata should preserve key fields for operator visibility"
        );
    }

    #[test]
    fn chunker_preserves_balanced_fences() {
        let code =
            (0..40).map(|index| format!("console.log({index});")).collect::<Vec<_>>().join("\n");
        let message = format!("Here is code:\\n```js\\n{code}\\n```\\nDone.");
        let chunks = chunk_discord_text(message.as_str(), 80, 8);
        assert!(chunks.len() > 1);
        for chunk in chunks {
            let mut open_fence: Option<OpenFence> = None;
            for line in chunk.lines() {
                let Some(fence_info) = parse_fence_line(line) else {
                    continue;
                };
                if open_fence.is_none() {
                    open_fence = Some(fence_info);
                    continue;
                }
                if let Some(existing) = open_fence.as_ref() {
                    if existing.marker == fence_info.marker
                        && fence_info.marker_len >= existing.marker_len
                    {
                        open_fence = None;
                    }
                }
            }
            assert!(open_fence.is_none(), "fenced blocks should stay balanced in chunk: {chunk}");
        }
    }

    #[tokio::test]
    async fn runtime_snapshot_redacts_token_material() {
        let transport = Arc::new(FakeTransport::default());
        transport.push_get_response(Ok(ok_identity_response()));
        transport.push_post_response(Ok(DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"native-3\"}".to_owned(),
        }));
        let adapter = adapter_with_fake_transport(Arc::clone(&transport));
        adapter
            .send_outbound(&sample_instance(), &sample_request("snapshot"))
            .await
            .expect("send should succeed");
        let snapshot = adapter
            .runtime_snapshot(&sample_instance())
            .expect("runtime snapshot should be available");
        assert_eq!(
            snapshot
                .get("credential")
                .and_then(|value| value.get("token_suffix"))
                .and_then(Value::as_str),
            Some("oken"),
            "snapshot should expose only token suffix"
        );
        assert_eq!(
            snapshot
                .get("bot_identity")
                .and_then(|value| value.get("username"))
                .and_then(Value::as_str),
            Some("palyra"),
            "snapshot should include validated bot identity metadata"
        );
    }

    #[test]
    fn inbound_envelope_id_is_stable_for_same_message() {
        let first = deterministic_inbound_envelope_id("discord:default", "1234567890");
        let second = deterministic_inbound_envelope_id("discord:default", "1234567890");
        let third = deterministic_inbound_envelope_id("discord:default", "1234567891");
        assert_eq!(first, second, "same connector/message pair must produce stable envelope ids");
        assert_ne!(first, third, "different messages must produce different envelope ids");
    }

    #[test]
    fn normalize_message_create_maps_thread_and_attachments() {
        let payload = json!({
            "id": "175928847299117063",
            "channel_id": "thread-123",
            "guild_id": "guild-1",
            "content": "<@bot-1> check this",
            "author": {
                "id": "user-7",
                "username": "operator",
                "global_name": "Operator"
            },
            "member": {
                "nick": "Op"
            },
            "message_reference": {
                "channel_id": "parent-777"
            },
            "attachments": [
                {
                    "url": "https://cdn.discordapp.net/attachments/abc/screenshot.png",
                    "filename": "screenshot.png",
                    "size": 4096,
                    "content_type": "image/png"
                }
            ]
        });

        let normalized =
            normalize_discord_message_create("discord:default", &payload, Some("bot-1"))
                .expect("payload should map to inbound event");
        assert_eq!(normalized.connector_id, "discord:default");
        assert_eq!(normalized.conversation_id, "thread-123");
        assert_eq!(normalized.adapter_message_id.as_deref(), Some("175928847299117063"));
        assert_eq!(normalized.adapter_thread_id.as_deref(), Some("thread-123"));
        assert_eq!(normalized.thread_id.as_deref(), Some("thread-123"));
        assert_eq!(normalized.sender_id, "user-7");
        assert_eq!(normalized.sender_display.as_deref(), Some("Op"));
        assert!(!normalized.is_direct_message, "guild message must not be marked as DM");
        assert_eq!(normalized.attachments.len(), 1);
        assert_eq!(normalized.attachments[0].kind, crate::protocol::AttachmentKind::Image);
    }

    #[test]
    fn normalize_message_create_ignores_self_messages() {
        let payload = json!({
            "id": "175928847299117064",
            "channel_id": "dm-42",
            "content": "hello",
            "author": {
                "id": "bot-1",
                "username": "palyra"
            },
            "attachments": []
        });
        let normalized =
            normalize_discord_message_create("discord:default", &payload, Some("bot-1"));
        assert!(normalized.is_none(), "connector must ignore its own bot messages");
    }

    #[test]
    fn build_net_guard_default_allowlist_covers_gateway_and_cdn_hosts() {
        let adapter = DiscordConnectorAdapter::new();
        let mut instance = sample_instance();
        instance.egress_allowlist.clear();
        let guard = adapter
            .build_net_guard(&instance)
            .expect("fallback allowlist should build a valid net guard");
        for host in
            ["discord.com", "gateway.discord.gg", "cdn.discordapp.net", "media.discordapp.com"]
        {
            guard.validate_target(host, &[]).unwrap_or_else(|error| {
                panic!("host '{host}' should pass fallback allowlist: {error}")
            });
        }
    }
}

impl Default for DiscordAdapterConfig {
    fn default() -> Self {
        Self {
            api_base_url: Url::parse(DISCORD_DEFAULT_API_BASE)
                .expect("default Discord API URL should be valid"),
            request_timeout_ms: DISCORD_DEFAULT_TIMEOUT_MS,
            max_chunk_chars: DISCORD_MAX_MESSAGE_CHARS,
            max_chunk_lines: DISCORD_MAX_MESSAGE_LINES,
            enable_auto_reactions: true,
            enable_inbound_gateway: true,
            inbound_buffer_capacity: DISCORD_INBOUND_BUFFER_CAPACITY,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscordCredential {
    pub token: String,
    pub source: String,
}

#[async_trait]
pub trait DiscordCredentialResolver: Send + Sync {
    async fn resolve_credential(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<DiscordCredential, ConnectorAdapterError>;
}

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

#[derive(Debug, Clone)]
pub struct DiscordTransportResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

#[async_trait]
pub trait DiscordTransport: Send + Sync {
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

    async fn put(
        &self,
        url: &Url,
        token: &str,
        timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError>;
}

#[derive(Clone)]
pub struct ReqwestDiscordTransport {
    client: Client,
}

impl Default for ReqwestDiscordTransport {
    fn default() -> Self {
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
}

#[derive(Debug, Clone)]
struct DiscordBotIdentity {
    id: String,
    username: String,
}

#[derive(Debug, Clone, Default)]
struct RouteRateLimitWindow {
    bucket_id: Option<String>,
    blocked_until_unix_ms: i64,
}

#[derive(Debug, Clone, Default)]
struct DiscordRuntimeState {
    delivered_native_ids: HashMap<String, String>,
    delivered_order: VecDeque<String>,
    route_limits: HashMap<String, RouteRateLimitWindow>,
    route_limit_order: VecDeque<String>,
    global_blocked_until_unix_ms: i64,
    credential_source: Option<String>,
    token_suffix: Option<String>,
    bot_identity: Option<DiscordBotIdentity>,
    bot_identity_checked_at_unix_ms: Option<i64>,
    last_inbound_unix_ms: Option<i64>,
    gateway_connected: bool,
    gateway_last_connect_unix_ms: Option<i64>,
    gateway_last_disconnect_unix_ms: Option<i64>,
    gateway_last_event_type: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RateLimitSnapshot {
    retry_after_ms: Option<u64>,
    reset_after_ms: Option<u64>,
    remaining: Option<u64>,
    bucket_id: Option<String>,
    global: bool,
}

#[derive(Debug, Default)]
struct DiscordGatewayResumeState {
    session_id: Option<String>,
    seq: Option<i64>,
    bot_user_id: Option<String>,
}

struct DiscordInboundMonitorHandle {
    receiver: mpsc::Receiver<InboundMessageEvent>,
}

#[derive(Clone)]
struct DiscordGatewayMonitorContext {
    connector_id: String,
    instance: ConnectorInstanceRecord,
    config: DiscordAdapterConfig,
    transport: Arc<dyn DiscordTransport>,
    credential_resolver: Arc<dyn DiscordCredentialResolver>,
    runtime_state: Arc<Mutex<DiscordRuntimeState>>,
    sender: mpsc::Sender<InboundMessageEvent>,
}

pub struct DiscordConnectorAdapter {
    config: DiscordAdapterConfig,
    transport: Arc<dyn DiscordTransport>,
    credential_resolver: Arc<dyn DiscordCredentialResolver>,
    state: Arc<Mutex<DiscordRuntimeState>>,
    inbound_monitors: Mutex<HashMap<String, DiscordInboundMonitorHandle>>,
}

impl std::fmt::Debug for DiscordConnectorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscordConnectorAdapter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Default for DiscordConnectorAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscordConnectorAdapter {
    #[must_use]
    pub fn new() -> Self {
        Self::with_dependencies(
            DiscordAdapterConfig::default(),
            Arc::new(ReqwestDiscordTransport::default()),
            Arc::new(EnvDiscordCredentialResolver),
        )
    }

    #[must_use]
    pub fn with_credential_resolver(
        credential_resolver: Arc<dyn DiscordCredentialResolver>,
    ) -> Self {
        Self::with_dependencies(
            DiscordAdapterConfig::default(),
            Arc::new(ReqwestDiscordTransport::default()),
            credential_resolver,
        )
    }

    #[must_use]
    pub fn with_dependencies(
        config: DiscordAdapterConfig,
        transport: Arc<dyn DiscordTransport>,
        credential_resolver: Arc<dyn DiscordCredentialResolver>,
    ) -> Self {
        Self {
            config,
            transport,
            credential_resolver,
            state: Arc::new(Mutex::new(DiscordRuntimeState::default())),
            inbound_monitors: Mutex::new(HashMap::new()),
        }
    }

    async fn ensure_inbound_monitor(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<(), ConnectorAdapterError> {
        let connector_id = instance.connector_id.clone();
        {
            let monitors = self.inbound_monitors.lock().map_err(|_| {
                ConnectorAdapterError::Backend(
                    "discord inbound monitor registry lock poisoned".to_owned(),
                )
            })?;
            if monitors.contains_key(connector_id.as_str()) {
                return Ok(());
            }
        }

        let (sender, receiver) = mpsc::channel(self.config.inbound_buffer_capacity.max(1));
        let context = DiscordGatewayMonitorContext {
            connector_id: connector_id.clone(),
            instance: instance.clone(),
            config: self.config.clone(),
            transport: Arc::clone(&self.transport),
            credential_resolver: Arc::clone(&self.credential_resolver),
            runtime_state: Arc::clone(&self.state),
            sender,
        };
        tokio::spawn(async move {
            run_discord_gateway_monitor(context).await;
        });

        let mut monitors = self.inbound_monitors.lock().map_err(|_| {
            ConnectorAdapterError::Backend(
                "discord inbound monitor registry lock poisoned".to_owned(),
            )
        })?;
        monitors.insert(connector_id, DiscordInboundMonitorHandle { receiver });
        Ok(())
    }

    fn drain_inbound_events(
        &self,
        connector_id: &str,
        max_events: usize,
    ) -> Result<Vec<InboundMessageEvent>, ConnectorAdapterError> {
        let mut monitors = self.inbound_monitors.lock().map_err(|_| {
            ConnectorAdapterError::Backend(
                "discord inbound monitor registry lock poisoned".to_owned(),
            )
        })?;
        let Some(handle) = monitors.get_mut(connector_id) else {
            return Ok(Vec::new());
        };

        let mut events = Vec::new();
        let limit = max_events.max(1);
        while events.len() < limit {
            match handle.receiver.try_recv() {
                Ok(event) => events.push(event),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    monitors.remove(connector_id);
                    break;
                }
            }
        }
        Ok(events)
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, DiscordRuntimeState>, ConnectorAdapterError> {
        self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("discord runtime state lock poisoned".to_owned())
        })
    }

    fn build_net_guard(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<ConnectorNetGuard, ConnectorAdapterError> {
        build_discord_net_guard(instance)
    }

    fn validate_url_target(
        &self,
        guard: &ConnectorNetGuard,
        url: &Url,
    ) -> Result<(), ConnectorAdapterError> {
        validate_discord_url_target(guard, url)
    }

    fn cached_delivery(&self, envelope_id: &str) -> Result<Option<String>, ConnectorAdapterError> {
        let state = self.lock_state()?;
        Ok(state.delivered_native_ids.get(envelope_id).cloned())
    }

    fn remember_delivery(
        &self,
        envelope_id: &str,
        native_message_id: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let mut state = self.lock_state()?;
        if !state.delivered_native_ids.contains_key(envelope_id) {
            state.delivered_order.push_back(envelope_id.to_owned());
        }
        state.delivered_native_ids.insert(envelope_id.to_owned(), native_message_id.to_owned());
        while state.delivered_order.len() > MAX_DELIVERY_CACHE {
            if let Some(stale) = state.delivered_order.pop_front() {
                state.delivered_native_ids.remove(stale.as_str());
            }
        }
        Ok(())
    }

    fn record_last_error(&self, message: &str) {
        let sanitized = redact_auth_error(message);
        if let Ok(mut state) = self.lock_state() {
            state.last_error = Some(sanitized);
        }
    }

    fn clear_last_error(&self) {
        if let Ok(mut state) = self.lock_state() {
            state.last_error = None;
        }
    }

    fn record_credential_metadata(&self, credential: &DiscordCredential) {
        if let Ok(mut state) = self.lock_state() {
            state.credential_source = Some(credential.source.clone());
            state.token_suffix = token_suffix(credential.token.as_str());
        }
    }

    fn preflight_retry_after_ms(
        &self,
        route_key: &str,
        now_unix_ms: i64,
    ) -> Result<Option<u64>, ConnectorAdapterError> {
        let mut state = self.lock_state()?;
        if state.global_blocked_until_unix_ms > now_unix_ms {
            let wait = state.global_blocked_until_unix_ms.saturating_sub(now_unix_ms);
            return Ok(Some(wait.max(1).try_into().unwrap_or(u64::MAX)));
        }
        if let Some(window) = state.route_limits.get(route_key) {
            if window.blocked_until_unix_ms > now_unix_ms {
                let wait = window.blocked_until_unix_ms.saturating_sub(now_unix_ms);
                return Ok(Some(wait.max(1).try_into().unwrap_or(u64::MAX)));
            }
            state.route_limits.remove(route_key);
            state.route_limit_order.retain(|entry| entry != route_key);
        }
        Ok(None)
    }

    fn apply_rate_limit_snapshot(
        &self,
        route_key: &str,
        snapshot: &RateLimitSnapshot,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorAdapterError> {
        let mut state = self.lock_state()?;

        let delay_ms = snapshot
            .retry_after_ms
            .or(snapshot.reset_after_ms)
            .map(|value| value.max(DEFAULT_MIN_RATE_LIMIT_RETRY_MS));
        if snapshot.global {
            if let Some(delay_ms) = delay_ms {
                let until = now_unix_ms.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
                state.global_blocked_until_unix_ms = state.global_blocked_until_unix_ms.max(until);
            }
        }

        let should_track_route = snapshot.bucket_id.is_some()
            || snapshot.retry_after_ms.is_some()
            || snapshot.reset_after_ms.is_some()
            || snapshot.remaining == Some(0);
        if !should_track_route {
            return Ok(());
        }

        let blocked_until = delay_ms
            .map(|value| now_unix_ms.saturating_add(i64::try_from(value).unwrap_or(i64::MAX)))
            .unwrap_or(now_unix_ms);

        let entry = state
            .route_limits
            .entry(route_key.to_owned())
            .or_insert_with(RouteRateLimitWindow::default);
        if let Some(bucket_id) = snapshot.bucket_id.clone() {
            entry.bucket_id = Some(bucket_id);
        }
        entry.blocked_until_unix_ms = entry.blocked_until_unix_ms.max(blocked_until);

        if !state.route_limit_order.iter().any(|item| item == route_key) {
            state.route_limit_order.push_back(route_key.to_owned());
        }
        while state.route_limit_order.len() > MAX_ROUTE_LIMIT_CACHE {
            if let Some(stale) = state.route_limit_order.pop_front() {
                state.route_limits.remove(stale.as_str());
            }
        }

        Ok(())
    }

    async fn ensure_bot_identity(
        &self,
        guard: &ConnectorNetGuard,
        credential: &DiscordCredential,
    ) -> Result<Option<DeliveryOutcome>, ConnectorAdapterError> {
        let now_unix_ms = unix_ms_now();
        {
            let state = self.lock_state()?;
            if state.bot_identity.is_some() {
                if let Some(checked_at) = state.bot_identity_checked_at_unix_ms {
                    if checked_at.saturating_add(IDENTITY_CACHE_TTL_MS) > now_unix_ms {
                        return Ok(None);
                    }
                }
            }
        }

        let route_key = "discord:get:/users/@me";
        if let Some(retry_after_ms) = self.preflight_retry_after_ms(route_key, now_unix_ms)? {
            return Ok(Some(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason: "discord identity lookup deferred by local rate-limit budget".to_owned(),
                retry_after_ms: Some(retry_after_ms),
            }));
        }

        let url = build_users_me_url(&self.config.api_base_url)?;
        self.validate_url_target(guard, &url)?;
        let response = match self
            .transport
            .get(&url, credential.token.as_str(), self.config.request_timeout_ms)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let reason = redact_auth_error(error.to_string().as_str());
                self.record_last_error(reason.as_str());
                return Ok(Some(DeliveryOutcome::Retry {
                    class: RetryClass::TransientNetwork,
                    reason,
                    retry_after_ms: None,
                }));
            }
        };

        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key, &snapshot, now_unix_ms)?;

        if response.status == 429 {
            let retry_after_ms = snapshot
                .retry_after_ms
                .or(snapshot.reset_after_ms)
                .unwrap_or(1_000)
                .max(DEFAULT_MIN_RATE_LIMIT_RETRY_MS);
            let reason = format!(
                "discord identity lookup rate-limited: {}",
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "retry later".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(Some(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason,
                retry_after_ms: Some(retry_after_ms),
            }));
        }

        if response.status == 401 || response.status == 403 {
            let reason = format!(
                "discord identity authentication failed (status={}): {}",
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unauthorized".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(Some(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            }));
        }

        if response.status >= 500 {
            let reason =
                format!("discord identity lookup failed with upstream status {}", response.status);
            self.record_last_error(reason.as_str());
            return Ok(Some(DeliveryOutcome::Retry {
                class: RetryClass::TransientNetwork,
                reason,
                retry_after_ms: None,
            }));
        }

        if !(200..300).contains(&response.status) {
            let reason = format!(
                "discord identity lookup failed (status={}): {}",
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            );
            self.record_last_error(reason.as_str());
            return Ok(Some(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            }));
        }

        let parsed = serde_json::from_str::<Value>(response.body.as_str()).ok();
        let id = parsed
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let username = parsed
            .as_ref()
            .and_then(|value| value.get("username"))
            .and_then(Value::as_str)
            .unwrap_or("discord-bot")
            .to_owned();

        let mut state = self.lock_state()?;
        state.bot_identity = Some(DiscordBotIdentity { id, username });
        state.bot_identity_checked_at_unix_ms = Some(now_unix_ms);
        Ok(None)
    }

    async fn send_auto_reaction(
        &self,
        guard: &ConnectorNetGuard,
        credential: &DiscordCredential,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) {
        let reaction_url =
            match build_reaction_url(&self.config.api_base_url, channel_id, message_id, emoji) {
                Ok(url) => url,
                Err(error) => {
                    self.record_last_error(error.to_string().as_str());
                    return;
                }
            };

        if let Err(error) = self.validate_url_target(guard, &reaction_url) {
            self.record_last_error(error.to_string().as_str());
            return;
        }

        let route_key = format!("discord:put:/channels/{channel_id}/messages/reactions");
        let now_unix_ms = unix_ms_now();
        if self.preflight_retry_after_ms(route_key.as_str(), now_unix_ms).ok().flatten().is_some() {
            return;
        }

        let response = match self
            .transport
            .put(&reaction_url, credential.token.as_str(), self.config.request_timeout_ms)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.record_last_error(error.to_string().as_str());
                return;
            }
        };

        let snapshot = parse_rate_limit_snapshot(&response);
        if let Err(error) =
            self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, now_unix_ms)
        {
            self.record_last_error(error.to_string().as_str());
            return;
        }

        if !(200..300).contains(&response.status) && response.status != 204 {
            let reason = format!(
                "discord reaction failed (status={}): {}",
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "reaction rejected".to_owned())
            );
            self.record_last_error(reason.as_str());
        }
    }
}

async fn run_discord_gateway_monitor(context: DiscordGatewayMonitorContext) {
    let mut resume_state = DiscordGatewayResumeState::default();
    let mut attempts = 0_u32;

    loop {
        let result = run_discord_gateway_session(&context, &mut resume_state).await;
        let now_unix_ms = unix_ms_now();
        if let Ok(mut state) = context.runtime_state.lock() {
            state.gateway_connected = false;
            state.gateway_last_disconnect_unix_ms = Some(now_unix_ms);
        }

        match result {
            Ok(()) => {
                attempts = 0;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => {
                attempts = attempts.saturating_add(1);
                let message = redact_auth_error(error.to_string().as_str());
                if let Ok(mut state) = context.runtime_state.lock() {
                    state.last_error = Some(message);
                }
                let delay_ms = monitor_backoff_ms(attempts);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

async fn run_discord_gateway_session(
    context: &DiscordGatewayMonitorContext,
    resume_state: &mut DiscordGatewayResumeState,
) -> Result<(), ConnectorAdapterError> {
    let credential = context.credential_resolver.resolve_credential(&context.instance).await?;
    let guard = build_discord_net_guard(&context.instance)?;

    let gateway_probe_url = build_gateway_bot_url(&context.config.api_base_url)?;
    validate_discord_url_target(&guard, &gateway_probe_url)?;
    let gateway_probe = context
        .transport
        .get(&gateway_probe_url, credential.token.as_str(), context.config.request_timeout_ms)
        .await?;
    if gateway_probe.status == 401 || gateway_probe.status == 403 {
        return Err(ConnectorAdapterError::Backend(redact_auth_error(
            format!(
                "discord gateway lookup unauthorized (status={}): {}",
                gateway_probe.status,
                parse_discord_error_summary(gateway_probe.body.as_str())
                    .unwrap_or_else(|| "unauthorized".to_owned())
            )
            .as_str(),
        )));
    }
    if !(200..300).contains(&gateway_probe.status) {
        return Err(ConnectorAdapterError::Backend(format!(
            "discord gateway lookup failed (status={}): {}",
            gateway_probe.status,
            parse_discord_error_summary(gateway_probe.body.as_str())
                .unwrap_or_else(|| "unexpected response".to_owned())
        )));
    }
    let gateway_url = parse_gateway_ws_url(gateway_probe.body.as_str())?;
    let gateway_url = normalize_gateway_ws_url(gateway_url)?;
    validate_discord_url_target(&guard, &gateway_url)?;

    let connect_result = tokio::time::timeout(
        Duration::from_millis(context.config.request_timeout_ms.max(1)),
        connect_async(gateway_url.as_str()),
    )
    .await
    .map_err(|_| ConnectorAdapterError::Backend("discord gateway connection timed out".to_owned()))?
    .map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord gateway connection failed: {error}"))
    })?;
    let (socket, _response) = connect_result;
    let (mut sink, mut stream) = socket.split();

    let now_unix_ms = unix_ms_now();
    if let Ok(mut state) = context.runtime_state.lock() {
        state.gateway_connected = true;
        state.gateway_last_connect_unix_ms = Some(now_unix_ms);
        state.last_error = None;
    }

    let mut heartbeat = interval(Duration::from_millis(DISCORD_GATEWAY_HEARTBEAT_FALLBACK_MS));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut hello_received = false;

    loop {
        tokio::select! {
            _ = heartbeat.tick(), if hello_received => {
                let heartbeat_payload = resume_state.seq.map(Value::from).unwrap_or(Value::Null);
                send_gateway_op(&mut sink, 1, heartbeat_payload).await?;
            }
            maybe_message = stream.next() => {
                let Some(message) = maybe_message else {
                    return Err(ConnectorAdapterError::Backend(
                        "discord gateway closed the websocket stream".to_owned(),
                    ));
                };
                let message = message.map_err(|error| {
                    ConnectorAdapterError::Backend(format!("discord gateway read failed: {error}"))
                })?;
                let raw_text = match message {
                    Message::Text(payload) => payload.to_string(),
                    Message::Binary(payload) => String::from_utf8_lossy(payload.as_ref()).to_string(),
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await.map_err(|error| {
                            ConnectorAdapterError::Backend(format!("discord gateway pong failed: {error}"))
                        })?;
                        continue;
                    }
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|value| value.reason.to_string())
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or_else(|| "no close reason".to_owned());
                        return Err(ConnectorAdapterError::Backend(format!(
                            "discord gateway closed websocket: {reason}"
                        )));
                    }
                    Message::Pong(_) | Message::Frame(_) => continue,
                };

                let envelope = parse_gateway_envelope(raw_text.as_str())?;
                if let Some(seq) = envelope.seq {
                    resume_state.seq = Some(seq);
                }
                if let Some(event_type) = envelope.event_type.as_deref() {
                    if let Ok(mut state) = context.runtime_state.lock() {
                        state.gateway_last_event_type = Some(event_type.to_owned());
                    }
                }

                match envelope.op {
                    10 => {
                        let heartbeat_interval_ms = envelope
                            .data
                            .get("heartbeat_interval")
                            .and_then(Value::as_u64)
                            .unwrap_or(DISCORD_GATEWAY_HEARTBEAT_FALLBACK_MS)
                            .max(250);
                        heartbeat = interval(Duration::from_millis(heartbeat_interval_ms));
                        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        hello_received = true;

                        if let (Some(session_id), Some(seq)) =
                            (resume_state.session_id.clone(), resume_state.seq)
                        {
                            send_gateway_op(
                                &mut sink,
                                6,
                                json!({
                                    "token": credential.token,
                                    "session_id": session_id,
                                    "seq": seq,
                                }),
                            )
                            .await?;
                        } else {
                            send_gateway_op(
                                &mut sink,
                                2,
                                json!({
                                    "token": credential.token,
                                    "intents": DISCORD_GATEWAY_INTENTS,
                                    "properties": {
                                        "os": std::env::consts::OS,
                                        "browser": "palyra",
                                        "device": "palyra",
                                    },
                                }),
                            )
                            .await?;
                        }
                    }
                    11 => {}
                    1 => {
                        let heartbeat_payload = resume_state.seq.map(Value::from).unwrap_or(Value::Null);
                        send_gateway_op(&mut sink, 1, heartbeat_payload).await?;
                    }
                    7 => {
                        return Err(ConnectorAdapterError::Backend(
                            "discord gateway requested reconnect".to_owned(),
                        ));
                    }
                    9 => {
                        let resumable = envelope.data.as_bool().unwrap_or(false);
                        if !resumable {
                            resume_state.session_id = None;
                            resume_state.seq = None;
                        }
                        return Err(ConnectorAdapterError::Backend(
                            "discord gateway invalidated session".to_owned(),
                        ));
                    }
                    0 => {
                        let event_type = envelope.event_type.unwrap_or_default();
                        match event_type.as_str() {
                            "READY" => {
                                resume_state.session_id = envelope
                                    .data
                                    .get("session_id")
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned);
                                resume_state.bot_user_id = envelope
                                    .data
                                    .get("user")
                                    .and_then(|value| value.get("id"))
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned);
                            }
                            "MESSAGE_CREATE" => {
                                let inbound = normalize_discord_message_create(
                                    context.connector_id.as_str(),
                                    &envelope.data,
                                    resume_state.bot_user_id.as_deref(),
                                );
                                if let Some(event) = inbound {
                                    if let Ok(mut state) = context.runtime_state.lock() {
                                        state.last_inbound_unix_ms = Some(event.received_at_unix_ms);
                                    }
                                    match context.sender.try_send(event) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            if let Ok(mut state) = context.runtime_state.lock() {
                                                state.last_error = Some(
                                                    "discord inbound queue full; dropping event".to_owned(),
                                                );
                                            }
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            return Err(ConnectorAdapterError::Backend(
                                                "discord inbound queue closed".to_owned(),
                                            ));
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn send_gateway_op<S>(sink: &mut S, op: i64, data: Value) -> Result<(), ConnectorAdapterError>
where
    S: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let payload = json!({
        "op": op,
        "d": data,
    });
    sink.send(Message::Text(payload.to_string().into())).await.map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord gateway write failed: {error}"))
    })
}

fn with_attachment_context(text: &str, attachments: &[AttachmentRef]) -> String {
    let Some(summary) = render_attachment_context(attachments) else {
        return text.to_owned();
    };
    let trimmed = text.trim_end();
    if trimmed.is_empty() {
        summary
    } else {
        format!("{trimmed}\n\n{summary}")
    }
}

fn render_attachment_context(attachments: &[AttachmentRef]) -> Option<String> {
    if attachments.is_empty() {
        return None;
    }
    let mut lines = Vec::with_capacity(attachments.len().saturating_add(1));
    lines.push("[attachment-metadata]".to_owned());
    for (index, attachment) in attachments.iter().enumerate() {
        lines.push(format!("- {}: {}", index.saturating_add(1), summarize_attachment(attachment)));
    }
    Some(lines.join("\n"))
}

fn summarize_attachment(attachment: &AttachmentRef) -> String {
    let kind = match attachment.kind {
        AttachmentKind::Image => "image",
        AttachmentKind::File => "file",
    };
    let source = attachment
        .url
        .as_deref()
        .or(attachment.artifact_ref.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let filename = attachment
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let content_type = attachment
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let size = attachment
        .size_bytes
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "kind={kind}, filename={filename}, content_type={content_type}, size_bytes={size}, source={source}"
    )
}

#[derive(Debug, Clone)]
struct DiscordGatewayEnvelope {
    op: i64,
    data: Value,
    seq: Option<i64>,
    event_type: Option<String>,
}

fn parse_gateway_envelope(raw: &str) -> Result<DiscordGatewayEnvelope, ConnectorAdapterError> {
    let payload = serde_json::from_str::<Value>(raw).map_err(|error| {
        ConnectorAdapterError::Backend(format!(
            "discord gateway payload is not valid JSON: {error}"
        ))
    })?;
    let op = payload.get("op").and_then(Value::as_i64).ok_or_else(|| {
        ConnectorAdapterError::Backend("discord gateway payload missing op code".to_owned())
    })?;
    Ok(DiscordGatewayEnvelope {
        op,
        data: payload.get("d").cloned().unwrap_or(Value::Null),
        seq: payload.get("s").and_then(Value::as_i64),
        event_type: payload
            .get("t")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    })
}

fn build_discord_net_guard(
    instance: &ConnectorInstanceRecord,
) -> Result<ConnectorNetGuard, ConnectorAdapterError> {
    let allowlist = if instance.egress_allowlist.is_empty() {
        DISCORD_FALLBACK_ALLOWLIST.iter().map(|entry| (*entry).to_owned()).collect::<Vec<_>>()
    } else {
        instance.egress_allowlist.clone()
    };
    ConnectorNetGuard::new(allowlist.as_slice()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord egress allowlist rejected: {error}"))
    })
}

fn validate_discord_url_target(
    guard: &ConnectorNetGuard,
    url: &Url,
) -> Result<(), ConnectorAdapterError> {
    let Some(host) = url.host_str() else {
        return Err(ConnectorAdapterError::Backend(
            "discord request URL is missing host".to_owned(),
        ));
    };
    let default_port =
        if url.scheme().eq_ignore_ascii_case("ws") || url.scheme().eq_ignore_ascii_case("http") {
            80_u16
        } else {
            443_u16
        };
    let port = url.port().unwrap_or(default_port);
    let mut resolved = Vec::new();
    if let Ok(addrs) = (host, port).to_socket_addrs() {
        resolved.extend(addrs.map(|entry| entry.ip()));
        resolved.sort_unstable();
        resolved.dedup();
    }
    guard
        .validate_target(host, resolved.as_slice())
        .map_err(|error| ConnectorAdapterError::Backend(format!("discord egress denied: {error}")))
}

fn build_gateway_bot_url(api_base: &Url) -> Result<Url, ConnectorAdapterError> {
    let candidate = format!("{}/gateway/bot", api_base.as_str().trim_end_matches('/'));
    Url::parse(candidate.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord API URL invalid: {error}"))
    })
}

fn parse_gateway_ws_url(raw_body: &str) -> Result<Url, ConnectorAdapterError> {
    let payload = serde_json::from_str::<Value>(raw_body).map_err(|error| {
        ConnectorAdapterError::Backend(format!(
            "discord gateway metadata payload is not valid JSON: {error}"
        ))
    })?;
    let raw_url = payload
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ConnectorAdapterError::Backend(
                "discord gateway metadata missing 'url' field".to_owned(),
            )
        })?;
    Url::parse(raw_url).map_err(|error| {
        ConnectorAdapterError::Backend(format!("discord gateway URL is invalid: {error}"))
    })
}

fn normalize_gateway_ws_url(mut url: Url) -> Result<Url, ConnectorAdapterError> {
    let normalized_scheme = match url.scheme() {
        "https" => "wss".to_owned(),
        "http" => "ws".to_owned(),
        "wss" | "ws" => url.scheme().to_owned(),
        _ => {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord gateway URL uses unsupported scheme '{}'",
                url.scheme()
            )));
        }
    };
    if url.scheme() != normalized_scheme.as_str() {
        url.set_scheme(normalized_scheme.as_str()).map_err(|_| {
            ConnectorAdapterError::Backend(
                "failed to normalize discord gateway URL scheme".to_owned(),
            )
        })?;
    }
    {
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.append_pair("v", DISCORD_GATEWAY_VERSION);
        pairs.append_pair("encoding", DISCORD_GATEWAY_ENCODING);
    }
    Ok(url)
}

fn monitor_backoff_ms(attempts: u32) -> u64 {
    let exponent = attempts.min(6);
    let base = DISCORD_GATEWAY_MONITOR_MIN_BACKOFF_MS
        .saturating_mul(1_u64 << exponent)
        .min(DISCORD_GATEWAY_MONITOR_MAX_BACKOFF_MS);
    base.saturating_add(monitor_jitter_ms()).min(DISCORD_GATEWAY_MONITOR_MAX_BACKOFF_MS)
}

fn monitor_jitter_ms() -> u64 {
    if DISCORD_GATEWAY_MONITOR_JITTER_MAX_MS <= 1 {
        return 0;
    }
    unix_ms_now().unsigned_abs() % DISCORD_GATEWAY_MONITOR_JITTER_MAX_MS
}

fn deterministic_inbound_envelope_id(connector_id: &str, message_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(connector_id.as_bytes());
    hasher.update(b":");
    hasher.update(message_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ulid::from_bytes(bytes).to_string()
}

fn normalize_discord_message_create(
    connector_id: &str,
    payload: &Value,
    bot_user_id: Option<&str>,
) -> Option<InboundMessageEvent> {
    let message_id = payload.get("id").and_then(Value::as_str).map(str::trim)?;
    if message_id.is_empty() {
        return None;
    }
    let channel_id = payload.get("channel_id").and_then(Value::as_str).map(str::trim)?;
    if channel_id.is_empty() {
        return None;
    }
    let author = payload.get("author")?;
    let sender_id = author.get("id").and_then(Value::as_str).map(str::trim)?;
    if sender_id.is_empty() {
        return None;
    }
    if bot_user_id.is_some_and(|value| value.eq_ignore_ascii_case(sender_id)) {
        return None;
    }

    let attachments = parse_discord_attachments(payload.get("attachments"));
    let body_text = payload
        .get("content")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();
    if body_text.is_empty() && attachments.is_empty() {
        return None;
    }
    let normalized_body = if body_text.is_empty() { "[attachment]".to_owned() } else { body_text };
    let is_direct_message = payload.get("guild_id").and_then(Value::as_str).is_none();
    let thread_id = parse_discord_thread_id(payload, channel_id);
    let received_at_unix_ms =
        parse_discord_snowflake_unix_ms(message_id).unwrap_or_else(unix_ms_now);
    let sender_display = resolve_discord_sender_display(payload);
    Some(InboundMessageEvent {
        envelope_id: deterministic_inbound_envelope_id(connector_id, message_id),
        connector_id: connector_id.to_owned(),
        conversation_id: channel_id.to_owned(),
        thread_id: thread_id.clone(),
        sender_id: sender_id.to_owned(),
        sender_display,
        body: normalized_body,
        adapter_message_id: Some(message_id.to_owned()),
        adapter_thread_id: thread_id,
        received_at_unix_ms,
        is_direct_message,
        requested_broadcast: false,
        attachments,
    })
}

fn resolve_discord_sender_display(payload: &Value) -> Option<String> {
    payload
        .get("member")
        .and_then(|member| member.get("nick"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            payload
                .get("author")
                .and_then(|author| author.get("global_name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            payload
                .get("author")
                .and_then(|author| author.get("username"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn parse_discord_thread_id(payload: &Value, channel_id: &str) -> Option<String> {
    if payload.get("thread").and_then(|value| value.get("id")).and_then(Value::as_str).is_some() {
        return Some(channel_id.to_owned());
    }
    let parent_channel_id = payload
        .get("message_reference")
        .and_then(|value| value.get("channel_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(parent_channel_id) = parent_channel_id {
        if !parent_channel_id.eq_ignore_ascii_case(channel_id) {
            return Some(channel_id.to_owned());
        }
    }
    None
}

fn parse_discord_attachments(raw: Option<&Value>) -> Vec<AttachmentRef> {
    let Some(entries) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let url = entry
                .get("url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let filename = entry
                .get("filename")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let content_type = entry
                .get("content_type")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let size_bytes = entry.get("size").and_then(Value::as_u64);
            if url.is_none() && filename.is_none() && content_type.is_none() && size_bytes.is_none()
            {
                return None;
            }
            let kind = if content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().starts_with("image/"))
                .unwrap_or(false)
                || filename.as_deref().map(|value| value.to_ascii_lowercase()).is_some_and(
                    |value| {
                        value.ends_with(".png")
                            || value.ends_with(".jpg")
                            || value.ends_with(".jpeg")
                            || value.ends_with(".gif")
                            || value.ends_with(".webp")
                            || value.ends_with(".bmp")
                            || value.ends_with(".svg")
                    },
                ) {
                AttachmentKind::Image
            } else {
                AttachmentKind::File
            };
            Some(AttachmentRef {
                kind,
                url,
                artifact_ref: None,
                filename,
                content_type,
                size_bytes,
            })
        })
        .collect()
}

fn parse_discord_snowflake_unix_ms(raw: &str) -> Option<i64> {
    let parsed = raw.trim().parse::<u64>().ok()?;
    let discord_epoch_ms = 1_420_070_400_000_u64;
    let unix_ms = (parsed >> 22).saturating_add(discord_epoch_ms);
    i64::try_from(unix_ms).ok()
}
