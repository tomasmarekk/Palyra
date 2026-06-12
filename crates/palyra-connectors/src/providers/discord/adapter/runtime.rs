//! Shared mutable runtime state for the Discord adapter.
//!
//! Owns the bounded delivery idempotency cache, per-route and global rate-limit windows,
//! cached bot identity, gateway monitor lifecycle, and best-effort auto-reactions. All state
//! lives behind one `std::sync::Mutex` and locks are never held across `.await`.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use flate2::Decompress;
use palyra_common::redaction::redact_auth_error;
use reqwest::Url;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    net::ConnectorNetGuard,
    protocol::{DeliveryOutcome, InboundMessageEvent, RetryClass},
    storage::ConnectorInstanceRecord,
    supervisor::ConnectorAdapterError,
};

use super::{
    gateway::{
        build_discord_net_guard, run_discord_gateway_monitor, validate_discord_url_target,
        DiscordGatewayMonitorContext,
    },
    transport::{
        build_reaction_url, build_users_me_url, parse_discord_error_summary,
        parse_rate_limit_snapshot, token_suffix, DiscordCredential,
    },
    unix_ms_now, DiscordConnectorAdapter, DEFAULT_MIN_RATE_LIMIT_RETRY_MS, IDENTITY_CACHE_TTL_MS,
    MAX_DELIVERY_CACHE, MAX_ROUTE_LIMIT_CACHE,
};

/// Cached identity of the authenticated bot user (`/users/@me`).
#[derive(Debug, Clone)]
pub(super) struct DiscordBotIdentity {
    pub(super) id: String,
    pub(super) username: String,
}

/// Rate-limit budget and delivery counters for one REST route key.
#[derive(Debug, Clone, Default)]
pub(super) struct RouteRateLimitWindow {
    pub(super) bucket_id: Option<String>,
    pub(super) blocked_until_unix_ms: i64,
    pub(super) attempts_total: u64,
    pub(super) delivered_total: u64,
    pub(super) local_deferrals_total: u64,
    pub(super) upstream_rate_limits_total: u64,
    pub(super) transient_failures_total: u64,
    pub(super) last_retry_after_ms: Option<u64>,
    pub(super) last_status: Option<u16>,
    pub(super) last_error: Option<String>,
}

/// Aggregate adapter state shared between outbound sends, admin operations, the gateway
/// monitor, and runtime snapshots.
#[derive(Debug, Clone, Default)]
pub(super) struct DiscordRuntimeState {
    pub(super) delivered_native_ids: HashMap<DeliveryCacheKey, String>,
    pub(super) delivered_order: VecDeque<DeliveryCacheKey>,
    pub(super) route_limits: HashMap<String, RouteRateLimitWindow>,
    pub(super) route_limit_order: VecDeque<String>,
    pub(super) global_blocked_until_unix_ms: i64,
    pub(super) credential_source: Option<String>,
    pub(super) token_suffix: Option<String>,
    pub(super) bot_identity: Option<DiscordBotIdentity>,
    pub(super) bot_identity_checked_at_unix_ms: Option<i64>,
    pub(super) last_inbound_unix_ms: Option<i64>,
    pub(super) gateway_connected: bool,
    pub(super) gateway_last_connect_unix_ms: Option<i64>,
    pub(super) gateway_last_disconnect_unix_ms: Option<i64>,
    pub(super) gateway_last_event_type: Option<String>,
    pub(super) last_error: Option<String>,
}

/// Idempotency cache key: one entry per (connector, outbound envelope) pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DeliveryCacheKey {
    connector_id: String,
    envelope_id: String,
}

impl DeliveryCacheKey {
    fn new(connector_id: &str, envelope_id: &str) -> Self {
        Self { connector_id: connector_id.to_owned(), envelope_id: envelope_id.to_owned() }
    }
}

/// Rate-limit signals parsed from one REST response (headers plus body fields).
#[derive(Debug, Clone, Default)]
pub(super) struct RateLimitSnapshot {
    pub(super) retry_after_ms: Option<u64>,
    pub(super) reset_after_ms: Option<u64>,
    pub(super) remaining: Option<u64>,
    pub(super) bucket_id: Option<String>,
    pub(super) global: bool,
}

/// Gateway session continuity carried across reconnects to enable RESUME.
#[derive(Debug, Default)]
pub(super) struct DiscordGatewayResumeState {
    pub(super) session_id: Option<String>,
    pub(super) seq: Option<i64>,
    pub(super) bot_user_id: Option<String>,
}

/// Connection-scoped zlib-stream decompression context (see `decode_gateway_binary_payload`).
#[derive(Debug)]
pub(super) struct DiscordGatewayInflater {
    pub(super) compressed_buffer: Vec<u8>,
    pub(super) decompressor: Decompress,
}

impl Default for DiscordGatewayInflater {
    fn default() -> Self {
        Self { compressed_buffer: Vec::new(), decompressor: Decompress::new(true) }
    }
}

/// Owning handle for one connector's gateway monitor task and its inbound event queue.
pub(super) struct DiscordInboundMonitorHandle {
    pub(super) receiver: mpsc::Receiver<InboundMessageEvent>,
    pub(super) task: tokio::task::JoinHandle<()>,
}

impl DiscordConnectorAdapter {
    /// Starts the gateway monitor task for `instance` if one is not already registered.
    ///
    /// The monitor is spawned outside the registry lock, then registration re-checks under
    /// the lock and aborts the redundant task if another caller won the race.
    pub(super) async fn ensure_inbound_monitor(
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
        let task = tokio::spawn(async move {
            run_discord_gateway_monitor(context).await;
        });

        self.register_inbound_monitor_handle(
            connector_id,
            DiscordInboundMonitorHandle { receiver, task },
        )
    }

    /// Registers a spawned monitor handle, aborting it when a monitor is already registered.
    pub(super) fn register_inbound_monitor_handle(
        &self,
        connector_id: String,
        handle: DiscordInboundMonitorHandle,
    ) -> Result<(), ConnectorAdapterError> {
        let mut monitors = self.inbound_monitors.lock().map_err(|_| {
            ConnectorAdapterError::Backend(
                "discord inbound monitor registry lock poisoned".to_owned(),
            )
        })?;
        if monitors.contains_key(connector_id.as_str()) {
            handle.task.abort();
            return Ok(());
        }
        monitors.insert(connector_id, handle);
        Ok(())
    }

    /// Aborts and deregisters the monitor for `connector_id`; returns whether one existed.
    pub(super) fn stop_inbound_monitor(
        &self,
        connector_id: &str,
    ) -> Result<bool, ConnectorAdapterError> {
        let mut monitors = self.inbound_monitors.lock().map_err(|_| {
            ConnectorAdapterError::Backend(
                "discord inbound monitor registry lock poisoned".to_owned(),
            )
        })?;
        let Some(handle) = monitors.remove(connector_id) else {
            return Ok(false);
        };
        handle.task.abort();
        Ok(true)
    }

    /// Drains up to `max_events` queued inbound events without blocking; a disconnected
    /// queue means the monitor died, so its registration is removed for a later restart.
    pub(super) fn drain_inbound_events(
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

    pub(super) fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, DiscordRuntimeState>, ConnectorAdapterError> {
        self.state.lock().map_err(|_| {
            ConnectorAdapterError::Backend("discord runtime state lock poisoned".to_owned())
        })
    }

    pub(super) fn build_net_guard(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<ConnectorNetGuard, ConnectorAdapterError> {
        build_discord_net_guard(instance)
    }

    pub(super) fn validate_url_target(
        &self,
        guard: &ConnectorNetGuard,
        url: &Url,
    ) -> Result<(), ConnectorAdapterError> {
        validate_discord_url_target(guard, url)
    }

    pub(super) fn cached_delivery(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<Option<String>, ConnectorAdapterError> {
        let state = self.lock_state()?;
        let key = DeliveryCacheKey::new(connector_id, envelope_id);
        Ok(state.delivered_native_ids.get(&key).cloned())
    }

    /// Records a delivered envelope in the idempotency cache, evicting oldest-inserted
    /// entries beyond the cap so memory stays bounded under sustained traffic.
    pub(super) fn remember_delivery(
        &self,
        connector_id: &str,
        envelope_id: &str,
        native_message_id: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let mut state = self.lock_state()?;
        let key = DeliveryCacheKey::new(connector_id, envelope_id);
        if !state.delivered_native_ids.contains_key(&key) {
            state.delivered_order.push_back(key.clone());
        }
        state.delivered_native_ids.insert(key, native_message_id.to_owned());
        while state.delivered_order.len() > MAX_DELIVERY_CACHE {
            if let Some(stale) = state.delivered_order.pop_front() {
                state.delivered_native_ids.remove(&stale);
            }
        }
        Ok(())
    }

    /// Mutates the route window for `route_key`, maintaining the bounded insertion-order
    /// index used to evict the oldest routes.
    fn with_route_window_mut<F>(
        &self,
        route_key: &str,
        callback: F,
    ) -> Result<(), ConnectorAdapterError>
    where
        F: FnOnce(&mut RouteRateLimitWindow),
    {
        let mut state = self.lock_state()?;
        let entry = state.route_limits.entry(route_key.to_owned()).or_default();
        callback(entry);
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

    pub(super) fn record_route_attempt(
        &self,
        route_key: &str,
    ) -> Result<(), ConnectorAdapterError> {
        self.with_route_window_mut(route_key, |window| {
            window.attempts_total = window.attempts_total.saturating_add(1);
        })
    }

    pub(super) fn record_route_delivery(
        &self,
        route_key: &str,
        status: u16,
    ) -> Result<(), ConnectorAdapterError> {
        self.with_route_window_mut(route_key, |window| {
            window.delivered_total = window.delivered_total.saturating_add(1);
            window.last_status = Some(status);
            window.last_error = None;
        })
    }

    pub(super) fn record_route_local_deferral(
        &self,
        route_key: &str,
        retry_after_ms: u64,
        reason: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let sanitized = redact_auth_error(reason);
        self.with_route_window_mut(route_key, |window| {
            window.local_deferrals_total = window.local_deferrals_total.saturating_add(1);
            window.last_retry_after_ms = Some(retry_after_ms);
            window.last_error = Some(sanitized);
        })
    }

    pub(super) fn record_route_transient_failure(
        &self,
        route_key: &str,
        reason: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let sanitized = redact_auth_error(reason);
        self.with_route_window_mut(route_key, |window| {
            window.transient_failures_total = window.transient_failures_total.saturating_add(1);
            window.last_error = Some(sanitized);
        })
    }

    pub(super) fn record_route_upstream_rate_limit(
        &self,
        route_key: &str,
        retry_after_ms: u64,
        reason: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let sanitized = redact_auth_error(reason);
        self.with_route_window_mut(route_key, |window| {
            window.upstream_rate_limits_total = window.upstream_rate_limits_total.saturating_add(1);
            window.last_retry_after_ms = Some(retry_after_ms);
            window.last_status = Some(429);
            window.last_error = Some(sanitized);
        })
    }

    pub(super) fn record_route_failure_status(
        &self,
        route_key: &str,
        status: u16,
        reason: &str,
    ) -> Result<(), ConnectorAdapterError> {
        let sanitized = redact_auth_error(reason);
        self.with_route_window_mut(route_key, |window| {
            window.last_status = Some(status);
            window.last_error = Some(sanitized);
        })
    }

    pub(super) fn record_last_error(&self, message: &str) {
        let sanitized = redact_auth_error(message);
        if let Ok(mut state) = self.lock_state() {
            state.last_error = Some(sanitized);
        }
    }

    pub(super) fn clear_last_error(&self) {
        if let Ok(mut state) = self.lock_state() {
            state.last_error = None;
        }
    }

    pub(super) fn record_credential_metadata(&self, credential: &DiscordCredential) {
        if let Ok(mut state) = self.lock_state() {
            state.credential_source = Some(credential.source.clone());
            state.token_suffix = token_suffix(credential.token.as_str());
        }
    }

    /// Checks the local rate-limit budget before a request: returns the wait in ms when the
    /// global window or this route's window is still blocked, recording the deferral; an
    /// expired route window is dropped on the way through.
    pub(super) fn preflight_retry_after_ms(
        &self,
        route_key: &str,
        now_unix_ms: i64,
    ) -> Result<Option<u64>, ConnectorAdapterError> {
        let mut state = self.lock_state()?;
        if state.global_blocked_until_unix_ms > now_unix_ms {
            let wait = state.global_blocked_until_unix_ms.saturating_sub(now_unix_ms);
            let wait_ms = wait.max(1).try_into().unwrap_or(u64::MAX);
            let entry = state.route_limits.entry(route_key.to_owned()).or_default();
            entry.local_deferrals_total = entry.local_deferrals_total.saturating_add(1);
            entry.last_retry_after_ms = Some(wait_ms);
            entry.last_error = Some(
                "discord outbound deferred due to local route/global rate-limit budget".to_owned(),
            );
            if !state.route_limit_order.iter().any(|item| item == route_key) {
                state.route_limit_order.push_back(route_key.to_owned());
            }
            while state.route_limit_order.len() > MAX_ROUTE_LIMIT_CACHE {
                if let Some(stale) = state.route_limit_order.pop_front() {
                    state.route_limits.remove(stale.as_str());
                }
            }
            return Ok(Some(wait_ms));
        }
        let route_blocked_until =
            state.route_limits.get(route_key).map(|window| window.blocked_until_unix_ms);
        if let Some(blocked_until_unix_ms) = route_blocked_until {
            if blocked_until_unix_ms > now_unix_ms {
                let wait = blocked_until_unix_ms.saturating_sub(now_unix_ms);
                let wait_ms = wait.max(1).try_into().unwrap_or(u64::MAX);
                let entry = state.route_limits.entry(route_key.to_owned()).or_default();
                entry.local_deferrals_total = entry.local_deferrals_total.saturating_add(1);
                entry.last_retry_after_ms = Some(wait_ms);
                entry.last_error = Some(
                    "discord outbound deferred due to local route/global rate-limit budget"
                        .to_owned(),
                );
                if !state.route_limit_order.iter().any(|item| item == route_key) {
                    state.route_limit_order.push_back(route_key.to_owned());
                }
                while state.route_limit_order.len() > MAX_ROUTE_LIMIT_CACHE {
                    if let Some(stale) = state.route_limit_order.pop_front() {
                        state.route_limits.remove(stale.as_str());
                    }
                }
                return Ok(Some(wait_ms));
            }
            state.route_limits.remove(route_key);
            state.route_limit_order.retain(|entry| entry != route_key);
        }
        Ok(None)
    }

    /// Folds a response's rate-limit signals into the global and per-route windows.
    ///
    /// Block windows only ever extend (`max`), never shrink, so an out-of-order response
    /// cannot reopen a budget another response already closed.
    pub(super) fn apply_rate_limit_snapshot(
        &self,
        route_key: &str,
        snapshot: &RateLimitSnapshot,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorAdapterError> {
        let mut state = self.lock_state()?;

        let delay_ms = snapshot
            .retry_after_ms
            .or(snapshot.reset_after_ms)
            .or_else(|| {
                if snapshot.remaining == Some(0) || snapshot.global {
                    Some(DEFAULT_MIN_RATE_LIMIT_RETRY_MS)
                } else {
                    None
                }
            })
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

        let entry = state.route_limits.entry(route_key.to_owned()).or_default();
        if let Some(bucket_id) = snapshot.bucket_id.clone() {
            entry.bucket_id = Some(bucket_id);
        }
        entry.blocked_until_unix_ms = entry.blocked_until_unix_ms.max(blocked_until);
        entry.last_retry_after_ms =
            snapshot.retry_after_ms.or(snapshot.reset_after_ms).or_else(|| {
                if blocked_until > now_unix_ms {
                    Some(blocked_until.saturating_sub(now_unix_ms).try_into().unwrap_or(u64::MAX))
                } else {
                    None
                }
            });

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

    /// Ensures a fresh bot identity is cached, refreshing via `/users/@me` past the TTL.
    ///
    /// Returns `Ok(None)` when the identity is usable, and `Ok(Some(outcome))` when the
    /// lookup could not complete — the caller forwards that outcome (retry or permanent
    /// failure) for the in-flight delivery instead of proceeding unauthenticated.
    pub(super) async fn ensure_bot_identity(
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
            self.record_route_local_deferral(
                route_key,
                retry_after_ms,
                "discord identity lookup deferred by local rate-limit budget",
            )?;
            return Ok(Some(DeliveryOutcome::Retry {
                class: RetryClass::RateLimit,
                reason: "discord identity lookup deferred by local rate-limit budget".to_owned(),
                retry_after_ms: Some(retry_after_ms),
            }));
        }
        self.record_route_attempt(route_key)?;

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
                self.record_route_transient_failure(route_key, reason.as_str())?;
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
            self.record_route_upstream_rate_limit(route_key, retry_after_ms, reason.as_str())?;
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
            self.record_route_failure_status(route_key, response.status, reason.as_str())?;
            return Ok(Some(DeliveryOutcome::PermanentFailure {
                reason: redact_auth_error(reason.as_str()),
            }));
        }

        if response.status >= 500 {
            let reason =
                format!("discord identity lookup failed with upstream status {}", response.status);
            self.record_last_error(reason.as_str());
            self.record_route_transient_failure(route_key, reason.as_str())?;
            self.record_route_failure_status(route_key, response.status, reason.as_str())?;
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
            self.record_route_failure_status(route_key, response.status, reason.as_str())?;
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
        drop(state);
        self.record_route_delivery(route_key, response.status)?;
        Ok(None)
    }

    /// Best-effort acknowledgement reaction after a successful delivery.
    ///
    /// Deliberately infallible: the message is already delivered, so reaction failures are
    /// only recorded as `last_error` and must never turn the delivery into a retry.
    pub(super) async fn send_auto_reaction(
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
        if let Some(retry_after_ms) =
            self.preflight_retry_after_ms(route_key.as_str(), now_unix_ms).ok().flatten()
        {
            let _ = self.record_route_local_deferral(
                route_key.as_str(),
                retry_after_ms,
                "discord reaction deferred by local route/global rate-limit budget",
            );
            return;
        }
        if self.record_route_attempt(route_key.as_str()).is_err() {
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
                let _ = self
                    .record_route_transient_failure(route_key.as_str(), error.to_string().as_str());
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
            let _ = self.record_route_failure_status(
                route_key.as_str(),
                response.status,
                reason.as_str(),
            );
        } else {
            let _ = self.record_route_delivery(route_key.as_str(), response.status);
        }
    }

    /// Resolves the cached bot identity, returning `None` (with `last_error` recorded) when
    /// the identity lookup is deferred or failed; admin flows then deny their preflight.
    pub(super) async fn resolve_bot_identity(
        &self,
        guard: &ConnectorNetGuard,
        credential: &DiscordCredential,
    ) -> Result<Option<DiscordBotIdentity>, ConnectorAdapterError> {
        if let Some(outcome) = self.ensure_bot_identity(guard, credential).await? {
            let reason = match outcome {
                DeliveryOutcome::Delivered { .. } => None,
                DeliveryOutcome::Retry { reason, .. }
                | DeliveryOutcome::PermanentFailure { reason } => Some(reason),
            };
            self.record_last_error(
                reason
                    .as_deref()
                    .unwrap_or("discord bot identity lookup returned an unexpected state"),
            );
            return Ok(None);
        }
        let state = self.lock_state()?;
        Ok(state.bot_identity.clone())
    }
}
