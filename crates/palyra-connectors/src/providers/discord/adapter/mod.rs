//! Discord connector adapter: outbound REST delivery plus gateway-based inbound intake.
//!
//! Expected provider failures (bad target, missing credential, rate limits, upstream errors)
//! are returned as classified [`DeliveryOutcome`] values rather than `Err`, so the supervisor
//! can schedule retries; `Err` is reserved for adapter-internal faults such as poisoned locks.

mod admin;
mod gateway;
mod outbound;
mod records;
mod runtime;
mod transport;

#[cfg(test)]
mod admin_operation_tests;
#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use crate::net::ConnectorNetGuard;
use admin::{
    denied_mutation_result, denied_operation_preflight, effective_channel_id,
    handle_mutation_message_response, search_message_matches, DiscordMessagePageRequest,
};
use async_trait::async_trait;
#[cfg(test)]
use gateway::{
    decode_gateway_binary_payload, deterministic_inbound_envelope_id, handle_gateway_envelope,
    normalize_gateway_ws_url, run_discord_gateway_transport_loop, validate_discord_url_target,
    validate_discord_url_target_with_resolver, DiscordGatewayEnvelope,
    DiscordGatewayMonitorContext,
};
use outbound::{
    build_discord_message_payload, chunk_discord_text, collect_discord_upload_files,
    fallback_native_message_id, parse_discord_message_id, with_attachment_context,
};
#[cfg(test)]
use outbound::{parse_fence_line, DiscordMultipartAttachment, OpenFence};
use palyra_common::redaction::redact_auth_error;
#[cfg(test)]
use records::{normalize_discord_message_create, parse_discord_attachments};
use reqwest::Url;
#[cfg(test)]
use runtime::{DiscordGatewayInflater, DiscordGatewayResumeState};
use runtime::{DiscordInboundMonitorHandle, DiscordRuntimeState};
use serde_json::{json, Value};
#[cfg(test)]
use transport::DiscordTransportResponse;
use transport::{
    build_message_url, build_messages_url, normalize_discord_target, parse_discord_error_summary,
    parse_rate_limit_snapshot, DiscordTransport, ReqwestDiscordTransport,
};
pub use transport::{DiscordCredential, DiscordCredentialResolver, EnvDiscordCredentialResolver};

use super::permissions::DiscordMessageOperation;
use crate::{
    protocol::{
        ConnectorKind, ConnectorMessageDeleteRequest, ConnectorMessageEditRequest,
        ConnectorMessageMutationDiff, ConnectorMessageMutationResult,
        ConnectorMessageMutationStatus, ConnectorMessageReactionRequest,
        ConnectorMessageReadRequest, ConnectorMessageReadResult, ConnectorMessageSearchRequest,
        ConnectorMessageSearchResult, DeliveryOutcome, InboundMessageEvent, OutboundMessageRequest,
        RetryClass,
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
const DISCORD_GATEWAY_COMPRESSION: &str = "zlib-stream";
// Gateway intents: GUILDS | GUILD_MESSAGES | DIRECT_MESSAGES | MESSAGE_CONTENT — the minimum
// needed to receive readable channel and DM messages; presence/member intents stay off.
const DISCORD_GATEWAY_INTENTS: u64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);
const DISCORD_GATEWAY_HEARTBEAT_FALLBACK_MS: u64 = 45_000;
const DISCORD_GATEWAY_MONITOR_MIN_BACKOFF_MS: u64 = 1_000;
const DISCORD_GATEWAY_MONITOR_MAX_BACKOFF_MS: u64 = 60_000;
const DISCORD_GATEWAY_MONITOR_JITTER_MAX_MS: u64 = 500;
const DISCORD_GATEWAY_ZLIB_SYNC_FLUSH_SUFFIX: [u8; 4] = [0x00, 0x00, 0xFF, 0xFF];
const DISCORD_GATEWAY_MAX_COMPRESSED_FRAME_BYTES: usize = 512 * 1024;
const DISCORD_GATEWAY_MAX_DECOMPRESSED_FRAME_BYTES: usize = 2 * 1024 * 1024;
const DISCORD_GATEWAY_DECOMPRESS_CHUNK_BYTES: usize = 8 * 1024;
const DISCORD_INBOUND_BUFFER_CAPACITY: usize = 512;
const IDENTITY_CACHE_TTL_MS: i64 = 5 * 60 * 1_000;
const MAX_DELIVERY_CACHE: usize = 4_096;
const MAX_ROUTE_LIMIT_CACHE: usize = 256;
const DEFAULT_MIN_RATE_LIMIT_RETRY_MS: u64 = 250;
const DISCORD_MAX_UPLOAD_BYTES: usize = 4 * 1024 * 1024;
const DISCORD_UPLOAD_ALLOWED_CONTENT_TYPES: &[&str] =
    &["image/png", "image/jpeg", "image/webp", "image/gif", "text/plain", "application/json"];

/// Tunable runtime configuration for the Discord adapter.
///
/// Defaults target the public Discord v10 API; tests override the base URL and feature
/// toggles to run against fake transports.
#[derive(Debug, Clone)]
pub struct DiscordAdapterConfig {
    /// Base URL for Discord REST calls (default: public v10 API).
    pub api_base_url: Url,
    /// Per-request timeout applied to every REST call.
    pub request_timeout_ms: u64,
    /// Maximum characters per outbound message chunk.
    pub max_chunk_chars: usize,
    /// Maximum lines per outbound message chunk.
    pub max_chunk_lines: usize,
    /// Whether delivered messages get the requested acknowledgement reaction.
    pub enable_auto_reactions: bool,
    /// Whether the inbound gateway monitor may be started by `poll_inbound`.
    pub enable_inbound_gateway: bool,
    /// Capacity of the bounded inbound event queue per connector.
    pub inbound_buffer_capacity: usize,
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

/// Current wall-clock time in unix milliseconds; clamps instead of failing on clock skew.
fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or_default()
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
        let rendered_text = with_attachment_context(
            request.text.as_str(),
            request
                .attachments
                .iter()
                .filter(|attachment| !attachment.upload_requested)
                .cloned()
                .collect::<Vec<_>>()
                .as_slice(),
        );
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
            // Only the first chunk keeps the original envelope id, attachments, and
            // structured payloads; continuation chunks get derived ids so idempotency
            // tracking stays per-chunk and side payloads are not delivered twice.
            if index > 0 {
                next.envelope_id = format!("{}:chunk{index}", request.envelope_id);
                next.attachments.clear();
                next.structured_json = None;
                next.a2ui_update = None;
            }
            split.push(next);
        }
        Ok(split)
    }

    fn runtime_snapshot(&self, _instance: &ConnectorInstanceRecord) -> Option<Value> {
        let state = self.lock_state().ok()?;
        let now_unix_ms = unix_ms_now();
        let mut routes = state
            .route_limits
            .iter()
            .map(|(route, window)| {
                json!({
                    "route": route,
                    "bucket_id": window.bucket_id,
                    "blocked_until_unix_ms": window.blocked_until_unix_ms,
                    "retry_after_ms": if window.blocked_until_unix_ms > now_unix_ms {
                        Some(window.blocked_until_unix_ms.saturating_sub(now_unix_ms))
                    } else {
                        None::<i64>
                    },
                    "attempts_total": window.attempts_total,
                    "delivered_total": window.delivered_total,
                    "local_deferrals_total": window.local_deferrals_total,
                    "upstream_rate_limits_total": window.upstream_rate_limits_total,
                    "transient_failures_total": window.transient_failures_total,
                    "last_retry_after_ms": window.last_retry_after_ms,
                    "last_status": window.last_status,
                    "last_error": window.last_error,
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
            "global_retry_after_ms": if state.global_blocked_until_unix_ms > now_unix_ms {
                Some(state.global_blocked_until_unix_ms.saturating_sub(now_unix_ms))
            } else {
                None::<i64>
            },
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

    fn stop_runtime(&self, connector_id: &str) -> Result<(), ConnectorAdapterError> {
        let _stopped = self.stop_inbound_monitor(connector_id)?;
        Ok(())
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
        if let Some(native_message_id) =
            self.cached_delivery(instance.connector_id.as_str(), request.envelope_id.as_str())?
        {
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
            self.record_route_local_deferral(route_key.as_str(), retry_after_ms, reason.as_str())?;
            self.record_last_error(reason.as_str());
            return Ok(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason,
                retry_after_ms: Some(retry_after_ms),
            });
        }
        self.record_route_attempt(route_key.as_str())?;

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

        let upload_files = match collect_discord_upload_files(request) {
            Ok(files) => files,
            Err(reason) => {
                self.record_last_error(reason.as_str());
                return Ok(DeliveryOutcome::PermanentFailure { reason });
            }
        };
        let payload = build_discord_message_payload(request, upload_files.as_slice());
        let response = match if upload_files.is_empty() {
            self.transport
                .post_json(
                    &message_url,
                    credential.token.as_str(),
                    &payload,
                    self.config.request_timeout_ms,
                )
                .await
        } else {
            self.transport
                .post_multipart(
                    &message_url,
                    credential.token.as_str(),
                    &payload,
                    upload_files.as_slice(),
                    self.config.request_timeout_ms,
                )
                .await
        } {
            Ok(response) => response,
            Err(error) => {
                let reason = redact_auth_error(error.to_string().as_str());
                self.record_last_error(reason.as_str());
                self.record_route_transient_failure(route_key.as_str(), reason.as_str())?;
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
            self.record_route_upstream_rate_limit(
                route_key.as_str(),
                retry_after_ms,
                reason.as_str(),
            )?;
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
            self.record_route_failure_status(route_key.as_str(), response.status, reason.as_str())?;
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
            self.record_route_transient_failure(route_key.as_str(), reason.as_str())?;
            self.record_route_failure_status(route_key.as_str(), response.status, reason.as_str())?;
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
            self.record_route_failure_status(route_key.as_str(), response.status, reason.as_str())?;
            return Ok(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            });
        }

        let native_message_id = parse_discord_message_id(response.body.as_str())
            .unwrap_or_else(|| fallback_native_message_id(request));
        self.remember_delivery(
            instance.connector_id.as_str(),
            request.envelope_id.as_str(),
            native_message_id.as_str(),
        )?;
        self.clear_last_error();
        self.record_route_delivery(route_key.as_str(), response.status)?;

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

    async fn read_messages(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReadRequest,
    ) -> Result<ConnectorMessageReadResult, ConnectorAdapterError> {
        let target_channel_id = effective_channel_id(&request.target);
        let context = match self
            .prepare_admin_context(instance, DiscordMessageOperation::Read, &request.target)
            .await?
        {
            Some(context) => context,
            None => {
                return Ok(ConnectorMessageReadResult {
                    preflight: denied_operation_preflight(
                        DiscordMessageOperation::Read,
                        "discord target channel is unavailable or permissions are insufficient"
                            .to_owned(),
                    ),
                    target: request.target.clone(),
                    exact_message_id: request.message_id.clone(),
                    messages: Vec::new(),
                    next_before_message_id: None,
                    next_after_message_id: None,
                });
            }
        };
        let preflight = context.preflight();
        if let Some(message_id) = request.message_id.as_deref() {
            let Some(message) = self
                .fetch_single_message(
                    &context,
                    &request.target,
                    message_id,
                    DiscordMessageOperation::Read,
                )
                .await?
            else {
                return Ok(ConnectorMessageReadResult {
                    preflight,
                    target: request.target.clone(),
                    exact_message_id: Some(message_id.to_owned()),
                    messages: Vec::new(),
                    next_before_message_id: None,
                    next_after_message_id: None,
                });
            };
            return Ok(ConnectorMessageReadResult {
                preflight,
                target: request.target.clone(),
                exact_message_id: Some(message_id.to_owned()),
                messages: vec![message],
                next_before_message_id: None,
                next_after_message_id: None,
            });
        }

        let messages = self
            .fetch_message_page(
                &context,
                DiscordMessagePageRequest {
                    channel_id: target_channel_id.as_str(),
                    before_message_id: request.before_message_id.as_deref(),
                    after_message_id: request.after_message_id.as_deref(),
                    around_message_id: request.around_message_id.as_deref(),
                    limit: request.limit,
                    operation: DiscordMessageOperation::Read,
                },
            )
            .await?;
        let next_before_message_id =
            messages.last().map(|message| message.locator.message_id.clone());
        let next_after_message_id =
            messages.first().map(|message| message.locator.message_id.clone());
        Ok(ConnectorMessageReadResult {
            preflight,
            target: request.target.clone(),
            exact_message_id: None,
            messages,
            next_before_message_id,
            next_after_message_id,
        })
    }

    async fn search_messages(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageSearchRequest,
    ) -> Result<ConnectorMessageSearchResult, ConnectorAdapterError> {
        let target_channel_id = effective_channel_id(&request.target);
        let context = match self
            .prepare_admin_context(instance, DiscordMessageOperation::Search, &request.target)
            .await?
        {
            Some(context) => {
                if !context.preflight_allowed {
                    return Ok(ConnectorMessageSearchResult {
                        preflight: context.preflight(),
                        target: request.target.clone(),
                        query: request.query.clone(),
                        author_id: request.author_id.clone(),
                        has_attachments: request.has_attachments,
                        matches: Vec::new(),
                        next_before_message_id: None,
                    });
                }
                context
            }
            None => {
                return Ok(ConnectorMessageSearchResult {
                    preflight: denied_operation_preflight(
                        DiscordMessageOperation::Search,
                        "discord target channel is unavailable or permissions are insufficient"
                            .to_owned(),
                    ),
                    target: request.target.clone(),
                    query: request.query.clone(),
                    author_id: request.author_id.clone(),
                    has_attachments: request.has_attachments,
                    matches: Vec::new(),
                    next_before_message_id: None,
                });
            }
        };
        let scanned_messages = self
            .fetch_message_page(
                &context,
                DiscordMessagePageRequest {
                    channel_id: target_channel_id.as_str(),
                    before_message_id: request.before_message_id.as_deref(),
                    after_message_id: None,
                    around_message_id: None,
                    limit: request.limit,
                    operation: DiscordMessageOperation::Search,
                },
            )
            .await?;
        let matches = scanned_messages
            .iter()
            .filter(|message| search_message_matches(message, request))
            .cloned()
            .collect::<Vec<_>>();
        let next_before_message_id =
            scanned_messages.last().map(|message| message.locator.message_id.clone());
        Ok(ConnectorMessageSearchResult {
            preflight: context.preflight(),
            target: request.target.clone(),
            query: request.query.clone(),
            author_id: request.author_id.clone(),
            has_attachments: request.has_attachments,
            matches,
            next_before_message_id,
        })
    }

    async fn edit_message(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageEditRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let context = match self
            .prepare_admin_context(instance, DiscordMessageOperation::Edit, &request.locator.target)
            .await?
        {
            Some(context) if context.preflight_allowed => context,
            Some(context) => {
                return Ok(denied_mutation_result(
                    DiscordMessageOperation::Edit,
                    request.locator.clone(),
                    context
                        .preflight_reason
                        .unwrap_or_else(|| "discord edit preflight denied".to_owned()),
                ));
            }
            None => {
                return Ok(denied_mutation_result(
                    DiscordMessageOperation::Edit,
                    request.locator.clone(),
                    "discord target channel is unavailable or permissions are insufficient"
                        .to_owned(),
                ));
            }
        };
        let Some(before) = self
            .fetch_single_message(
                &context,
                &request.locator.target,
                request.locator.message_id.as_str(),
                DiscordMessageOperation::Edit,
            )
            .await?
        else {
            return Ok(denied_mutation_result(
                DiscordMessageOperation::Edit,
                request.locator.clone(),
                "discord message is missing or stale".to_owned(),
            ));
        };
        if !before.is_connector_authored {
            return Ok(denied_mutation_result(
                DiscordMessageOperation::Edit,
                request.locator.clone(),
                "discord edit is only supported for connector-authored messages".to_owned(),
            ));
        }
        let message_url = build_message_url(
            &self.config.api_base_url,
            effective_channel_id(&request.locator.target).as_str(),
            request.locator.message_id.as_str(),
        )?;
        let response = self
            .patch_discord_json(
                &context,
                format!(
                    "discord:patch:/channels/{}/messages/{}",
                    effective_channel_id(&request.locator.target),
                    request.locator.message_id
                )
                .as_str(),
                &message_url,
                &json!({ "content": request.body }),
            )
            .await?;
        let Some(after) = handle_mutation_message_response(
            &context,
            DiscordMessageOperation::Edit,
            &request.locator,
            response,
        )?
        else {
            return Ok(denied_mutation_result(
                DiscordMessageOperation::Edit,
                request.locator.clone(),
                "discord edit was rejected by the platform".to_owned(),
            ));
        };
        Ok(ConnectorMessageMutationResult {
            preflight: context.preflight(),
            locator: request.locator.clone(),
            status: ConnectorMessageMutationStatus::Updated,
            reason: None,
            message: Some(after.clone()),
            diff: Some(ConnectorMessageMutationDiff {
                before_body: Some(before.body),
                after_body: Some(after.body),
            }),
        })
    }

    async fn delete_message(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageDeleteRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let context = match self
            .prepare_admin_context(
                instance,
                DiscordMessageOperation::Delete,
                &request.locator.target,
            )
            .await?
        {
            Some(context) if context.preflight_allowed => context,
            Some(context) => {
                return Ok(denied_mutation_result(
                    DiscordMessageOperation::Delete,
                    request.locator.clone(),
                    context
                        .preflight_reason
                        .unwrap_or_else(|| "discord delete preflight denied".to_owned()),
                ));
            }
            None => {
                return Ok(denied_mutation_result(
                    DiscordMessageOperation::Delete,
                    request.locator.clone(),
                    "discord target channel is unavailable or permissions are insufficient"
                        .to_owned(),
                ));
            }
        };
        let before = self
            .fetch_single_message(
                &context,
                &request.locator.target,
                request.locator.message_id.as_str(),
                DiscordMessageOperation::Delete,
            )
            .await?;
        let Some(before) = before else {
            return Ok(denied_mutation_result(
                DiscordMessageOperation::Delete,
                request.locator.clone(),
                "discord message is missing or stale".to_owned(),
            ));
        };
        let message_url = build_message_url(
            &self.config.api_base_url,
            effective_channel_id(&request.locator.target).as_str(),
            request.locator.message_id.as_str(),
        )?;
        let status = self
            .delete_discord_request(
                &context,
                format!(
                    "discord:delete:/channels/{}/messages/{}",
                    effective_channel_id(&request.locator.target),
                    request.locator.message_id
                )
                .as_str(),
                &message_url,
                DiscordMessageOperation::Delete,
                &request.locator,
            )
            .await?;
        if !status {
            return Ok(denied_mutation_result(
                DiscordMessageOperation::Delete,
                request.locator.clone(),
                "discord delete was rejected by the platform".to_owned(),
            ));
        }
        Ok(ConnectorMessageMutationResult {
            preflight: context.preflight(),
            locator: request.locator.clone(),
            status: ConnectorMessageMutationStatus::Deleted,
            reason: request.reason.clone(),
            message: Some(before.clone()),
            diff: Some(ConnectorMessageMutationDiff {
                before_body: Some(before.body),
                after_body: None,
            }),
        })
    }

    async fn add_reaction(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        self.apply_reaction_mutation(instance, request, true).await
    }

    async fn remove_reaction(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        self.apply_reaction_mutation(instance, request, false).await
    }
}

/// Supported Discord adapter handling outbound REST delivery, message administration, and
/// gateway-based inbound intake for all Discord connector instances.
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
    /// Creates an adapter with the default config, live HTTP transport, and
    /// environment-based credential resolution.
    #[must_use]
    pub fn new() -> Self {
        Self::with_dependencies(
            DiscordAdapterConfig::default(),
            Arc::new(ReqwestDiscordTransport::default()),
            Arc::new(EnvDiscordCredentialResolver),
        )
    }

    /// Creates an adapter with default config and transport but a custom credential resolver
    /// (e.g. vault-backed resolution in the daemon).
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
    fn with_dependencies(
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
}
