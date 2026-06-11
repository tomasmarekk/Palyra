//! Debounce-based coalescing of rapid-fire inbound channel messages.
//!
//! Buckets messages by a normalized scope key (principal, session, channel,
//! conversation, thread, sender) and merges everything that arrives within
//! the policy debounce window into one provider turn. Commands, urgent stop
//! requests, and media bypass coalescing. Policy comes from
//! `channel_router::InboundCoalescingPolicy`; `route_message` drives the
//! production flush path.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::channel_router::InboundCoalescingPolicy;

const INBOUND_COALESCING_SCHEMA_VERSION: u32 = 1;
const MAX_SAFE_TEXT_PREVIEW_CHARS: usize = 240;

/// Normalized identity of one coalescing bucket; messages merge only when
/// every component matches. Empty required components normalize to
/// "unknown" so malformed requests still bucket deterministically.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct InboundCoalescingKey {
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) session_id: String,
    pub(crate) policy_scope: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_identity: Option<String>,
}

impl InboundCoalescingKey {
    #[must_use]
    fn from_request(request: &InboundCoalescingRequest) -> Self {
        Self {
            principal: normalize_required_component(request.principal.as_str(), "unknown"),
            device_id: normalize_required_component(request.device_id.as_str(), "unknown"),
            session_id: normalize_required_component(request.session_id.as_str(), "unknown"),
            policy_scope: normalize_required_component(request.policy_scope.as_str(), "unknown"),
            binding_id: normalize_optional_component(request.binding_id.clone()),
            channel: normalize_required_component(request.channel.as_str(), "unknown"),
            conversation_id: normalize_optional_component(request.conversation_id.clone()),
            thread_id: normalize_optional_component(request.thread_id.clone()),
            sender_identity: normalize_optional_component(request.sender_identity.clone()),
        }
    }
}

/// Per-message audit trail kept alongside the merged text so the original
/// message order and sizes stay reconstructible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InboundMessageProvenance {
    pub(crate) message_id: String,
    pub(crate) received_at_unix_ms: i64,
    pub(crate) order: u32,
    pub(crate) text_bytes: usize,
    pub(crate) has_media: bool,
}

/// Result of merging one bucket: trimmed message texts joined with blank
/// lines plus the provenance of every contributing message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InboundCoalescedInput {
    pub(crate) key: InboundCoalescingKey,
    pub(crate) text: String,
    pub(crate) provenance: Vec<InboundMessageProvenance>,
    pub(crate) first_received_at_unix_ms: i64,
    pub(crate) last_received_at_unix_ms: i64,
}

impl InboundCoalescedInput {
    /// Renders a journal-safe snapshot; the text is reduced to a redacted,
    /// length-capped preview.
    #[must_use]
    pub(crate) fn safe_snapshot_json(&self) -> Value {
        json!({
            "schema_version": INBOUND_COALESCING_SCHEMA_VERSION,
            "key": self.key,
            "text_preview": safe_preview(self.text.as_str()),
            "text_bytes": self.text.len(),
            "message_count": self.provenance.len(),
            "provenance": self.provenance,
            "first_received_at_unix_ms": self.first_received_at_unix_ms,
            "last_received_at_unix_ms": self.last_received_at_unix_ms,
        })
    }
}

/// What the coalescer decided for a submitted message: routed around the
/// debounce (`Bypassed`), buffered (`Pending`), or merged and released
/// (`Ready`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InboundCoalescingDecisionKind {
    Bypassed,
    Pending,
    Ready,
}

impl InboundCoalescingDecisionKind {
    /// Stable snake_case label used in journal snapshots.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Bypassed => "bypassed",
            Self::Pending => "pending",
            Self::Ready => "ready",
        }
    }
}

/// Outcome of one submission: the decision kind, a stable reason label,
/// the coalesced input when released, and tracker occupancy for telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InboundCoalescingDecision {
    pub(crate) kind: InboundCoalescingDecisionKind,
    pub(crate) reason: String,
    pub(crate) key: InboundCoalescingKey,
    pub(crate) coalesced: Option<InboundCoalescedInput>,
    pub(crate) ready_at_unix_ms: Option<i64>,
    pub(crate) tracked_key_count: usize,
}

impl InboundCoalescingDecision {
    /// Renders a journal-safe snapshot including the policy that produced
    /// this decision.
    #[must_use]
    pub(crate) fn safe_snapshot_json(&self, policy: &InboundCoalescingPolicy) -> Value {
        json!({
            "schema_version": INBOUND_COALESCING_SCHEMA_VERSION,
            "decision": self.kind.as_str(),
            "reason": self.reason,
            "policy": policy_snapshot_json(policy),
            "key": self.key,
            "coalesced": self.coalesced.as_ref().map(InboundCoalescedInput::safe_snapshot_json),
            "ready_at_unix_ms": self.ready_at_unix_ms,
            "tracked_key_count": self.tracked_key_count,
        })
    }
}

/// One inbound message offered to the coalescer, carrying both the scope
/// identity and the bypass signals (`is_command`, `urgent_stop`,
/// `has_media`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InboundCoalescingRequest {
    pub(crate) message_id: String,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) session_id: String,
    pub(crate) policy_scope: String,
    pub(crate) binding_id: Option<String>,
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_identity: Option<String>,
    pub(crate) text: String,
    pub(crate) received_at_unix_ms: i64,
    pub(crate) has_media: bool,
    pub(crate) is_command: bool,
    pub(crate) urgent_stop: bool,
}

/// Coalescer failure with a stable machine-readable code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InboundCoalescingError {
    code: &'static str,
    message: String,
}

impl InboundCoalescingError {
    /// Stable error code of the form `inbound_coalescing/<cause>`.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        self.code
    }

    /// Human-readable message with auth material redacted.
    #[must_use]
    pub(crate) fn safe_message(&self) -> String {
        redact_auth_error(self.message.as_str())
    }
}

/// Thread-safe coalescer shared across inbound handlers; clones share the
/// same pending-bucket state. `BTreeMap` keeps bucket iteration order
/// deterministic for drain and snapshot output.
#[derive(Debug, Clone)]
pub(crate) struct InboundCoalescer {
    policy: InboundCoalescingPolicy,
    pending: Arc<Mutex<BTreeMap<InboundCoalescingKey, PendingCoalescingBucket>>>,
}

impl InboundCoalescer {
    /// Creates an empty coalescer governed by `policy`.
    #[must_use]
    pub(crate) fn new(policy: InboundCoalescingPolicy) -> Self {
        Self { policy, pending: Arc::new(Mutex::new(BTreeMap::new())) }
    }

    /// Returns the policy this coalescer was built with.
    #[must_use]
    pub(crate) const fn policy(&self) -> &InboundCoalescingPolicy {
        &self.policy
    }

    // Deferred debounce path (buffer until the window elapses, then drain).
    // Production currently routes through submit_for_immediate_route only,
    // so this and drain_ready stay exercised by tests until a scheduler
    // flushes pending buckets asynchronously.
    #[cfg(test)]
    fn submit(
        &self,
        request: InboundCoalescingRequest,
    ) -> Result<InboundCoalescingDecision, InboundCoalescingError> {
        let key = request.key();
        if !self.policy.active() {
            return Ok(self.bypass(request, key, "policy_disabled"));
        }
        if self.policy.bypass_commands && (request.is_command || request.urgent_stop) {
            return Ok(self.bypass(request, key, "command_bypass"));
        }
        if self.policy.bypass_media && request.has_media {
            return Ok(self.bypass(request, key, "media_bypass"));
        }

        let mut guard = self.pending.lock().map_err(|_| InboundCoalescingError {
            code: "inbound_coalescing/state_unavailable",
            message: "inbound coalescer state lock is unavailable".to_owned(),
        })?;
        if !guard.contains_key(&key) && guard.len() >= self.policy.max_tracked_keys {
            return Err(InboundCoalescingError {
                code: "inbound_coalescing/max_tracked_keys_exceeded",
                message: format!(
                    "inbound coalescer is tracking {} keys, max_tracked_keys={}",
                    guard.len(),
                    self.policy.max_tracked_keys
                ),
            });
        }

        let received_at_unix_ms = request.received_at_unix_ms;
        let ready_at_unix_ms = {
            let bucket = guard.entry(key.clone()).or_insert_with(|| PendingCoalescingBucket {
                key: key.clone(),
                messages: Vec::new(),
                first_received_at_unix_ms: received_at_unix_ms,
                ready_at_unix_ms: received_at_unix_ms
                    .saturating_add(self.policy.debounce_ms as i64),
            });
            // push() slides ready_at forward: every new message restarts the
            // debounce window from its own arrival time.
            bucket.push(request, self.policy.debounce_ms);
            bucket.ready_at_unix_ms
        };
        let tracked_key_count = guard.len();
        if received_at_unix_ms >= ready_at_unix_ms {
            let bucket = guard
                .remove(&key)
                .expect("bucket inserted above cannot vanish while the lock is held");
            return Ok(InboundCoalescingDecision {
                kind: InboundCoalescingDecisionKind::Ready,
                reason: "debounce_window_elapsed".to_owned(),
                key,
                coalesced: Some(bucket.coalesced()),
                ready_at_unix_ms: Some(ready_at_unix_ms),
                tracked_key_count: guard.len(),
            });
        }

        Ok(InboundCoalescingDecision {
            kind: InboundCoalescingDecisionKind::Pending,
            reason: "debounce_window_open".to_owned(),
            key,
            coalesced: None,
            ready_at_unix_ms: Some(ready_at_unix_ms),
            tracked_key_count,
        })
    }

    /// Submits a message and flushes its bucket in the same call, merging
    /// it with anything already pending for the same key.
    ///
    /// This is the synchronous route_message path: the decision is always
    /// `Bypassed` or `Ready`, never `Pending`.
    ///
    /// # Errors
    /// Returns `inbound_coalescing/state_unavailable` when the state lock is
    /// poisoned and `inbound_coalescing/max_tracked_keys_exceeded` when a
    /// new key would exceed the policy key limit.
    pub(crate) fn submit_for_immediate_route(
        &self,
        request: InboundCoalescingRequest,
    ) -> Result<InboundCoalescingDecision, InboundCoalescingError> {
        let key = request.key();
        if !self.policy.active() {
            return Ok(self.bypass(request, key, "policy_disabled"));
        }
        if self.policy.bypass_commands && (request.is_command || request.urgent_stop) {
            return Ok(self.bypass(request, key, "command_bypass"));
        }
        if self.policy.bypass_media && request.has_media {
            return Ok(self.bypass(request, key, "media_bypass"));
        }

        let mut guard = self.pending.lock().map_err(|_| InboundCoalescingError {
            code: "inbound_coalescing/state_unavailable",
            message: "inbound coalescer state lock is unavailable".to_owned(),
        })?;
        if !guard.contains_key(&key) && guard.len() >= self.policy.max_tracked_keys {
            return Err(InboundCoalescingError {
                code: "inbound_coalescing/max_tracked_keys_exceeded",
                message: format!(
                    "inbound coalescer is tracking {} keys, max_tracked_keys={}",
                    guard.len(),
                    self.policy.max_tracked_keys
                ),
            });
        }
        let received_at_unix_ms = request.received_at_unix_ms;
        let bucket = guard.entry(key.clone()).or_insert_with(|| PendingCoalescingBucket {
            key: key.clone(),
            messages: Vec::new(),
            first_received_at_unix_ms: received_at_unix_ms,
            ready_at_unix_ms: received_at_unix_ms.saturating_add(self.policy.debounce_ms as i64),
        });
        bucket.push(request, self.policy.debounce_ms);
        let ready_at_unix_ms = bucket.ready_at_unix_ms;
        let bucket =
            guard.remove(&key).expect("bucket inserted above cannot vanish while the lock is held");
        let coalesced = bucket.coalesced();
        Ok(InboundCoalescingDecision {
            kind: InboundCoalescingDecisionKind::Ready,
            reason: "route_message_immediate_flush".to_owned(),
            key,
            coalesced: Some(coalesced),
            ready_at_unix_ms: Some(ready_at_unix_ms),
            tracked_key_count: guard.len(),
        })
    }

    /// Releases every bucket whose debounce window elapsed by `now_unix_ms`.
    ///
    /// # Errors
    /// Returns `inbound_coalescing/state_unavailable` when the state lock is
    /// poisoned.
    #[allow(dead_code)]
    pub(crate) fn drain_ready(
        &self,
        now_unix_ms: i64,
    ) -> Result<Vec<InboundCoalescingDecision>, InboundCoalescingError> {
        let mut guard = self.pending.lock().map_err(|_| InboundCoalescingError {
            code: "inbound_coalescing/state_unavailable",
            message: "inbound coalescer state lock is unavailable".to_owned(),
        })?;
        let ready_keys = guard
            .iter()
            .filter(|(_, bucket)| now_unix_ms >= bucket.ready_at_unix_ms)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let mut decisions = Vec::with_capacity(ready_keys.len());
        for key in ready_keys {
            let Some(bucket) = guard.remove(&key) else {
                continue;
            };
            decisions.push(InboundCoalescingDecision {
                kind: InboundCoalescingDecisionKind::Ready,
                reason: "debounce_window_elapsed".to_owned(),
                key,
                coalesced: Some(bucket.coalesced()),
                ready_at_unix_ms: Some(now_unix_ms),
                tracked_key_count: guard.len(),
            });
        }
        Ok(decisions)
    }

    // Wraps a single message as an already-coalesced decision without ever
    // touching the pending map, so bypassed traffic cannot stall behind a
    // bucket opened by earlier messages.
    fn bypass(
        &self,
        request: InboundCoalescingRequest,
        key: InboundCoalescingKey,
        reason: &str,
    ) -> InboundCoalescingDecision {
        let received_at_unix_ms = request.received_at_unix_ms;
        let mut bucket = PendingCoalescingBucket {
            key: key.clone(),
            messages: Vec::new(),
            first_received_at_unix_ms: received_at_unix_ms,
            ready_at_unix_ms: received_at_unix_ms,
        };
        bucket.push(request, 0);
        InboundCoalescingDecision {
            kind: InboundCoalescingDecisionKind::Bypassed,
            reason: reason.to_owned(),
            key,
            coalesced: Some(bucket.coalesced()),
            ready_at_unix_ms: Some(received_at_unix_ms),
            tracked_key_count: self.pending.lock().map_or(0, |guard| guard.len()),
        }
    }
}

impl InboundCoalescingRequest {
    #[must_use]
    fn key(&self) -> InboundCoalescingKey {
        InboundCoalescingKey::from_request(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingCoalescingMessage {
    text: String,
    provenance: InboundMessageProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingCoalescingBucket {
    key: InboundCoalescingKey,
    messages: Vec<PendingCoalescingMessage>,
    first_received_at_unix_ms: i64,
    ready_at_unix_ms: i64,
}

impl PendingCoalescingBucket {
    fn push(&mut self, request: InboundCoalescingRequest, debounce_ms: u64) {
        let order = u32::try_from(self.messages.len()).unwrap_or(u32::MAX);
        // Sliding window: each message restarts the debounce from its own
        // arrival. The as-cast is safe for any realistic config value.
        self.ready_at_unix_ms = request.received_at_unix_ms.saturating_add(debounce_ms as i64);
        self.messages.push(PendingCoalescingMessage {
            provenance: InboundMessageProvenance {
                message_id: normalize_required_component(request.message_id.as_str(), "unknown"),
                received_at_unix_ms: request.received_at_unix_ms,
                order,
                text_bytes: request.text.len(),
                has_media: request.has_media,
            },
            text: request.text,
        });
    }

    fn coalesced(self) -> InboundCoalescedInput {
        let last_received_at_unix_ms =
            self.messages.last().map_or(self.first_received_at_unix_ms, |message| {
                message.provenance.received_at_unix_ms
            });
        let text = self
            .messages
            .iter()
            .map(|message| message.text.trim())
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        let provenance = self.messages.into_iter().map(|message| message.provenance).collect();
        InboundCoalescedInput {
            key: self.key,
            text,
            provenance,
            first_received_at_unix_ms: self.first_received_at_unix_ms,
            last_received_at_unix_ms,
        }
    }
}

#[must_use]
fn policy_snapshot_json(policy: &InboundCoalescingPolicy) -> Value {
    json!({
        "enabled": policy.enabled,
        "debounce_ms": policy.debounce_ms,
        "max_tracked_keys": policy.max_tracked_keys,
        "bypass_commands": policy.bypass_commands,
        "bypass_media": policy.bypass_media,
    })
}

fn normalize_required_component(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn normalize_optional_component(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

// Redaction must run before truncation so a cut never re-exposes a secret
// that the redactor would otherwise have matched.
fn safe_preview(value: &str) -> String {
    let redacted = redact_url_segments_in_text(&redact_auth_error(value));
    let mut output: String = redacted.chars().take(MAX_SAFE_TEXT_PREVIEW_CHARS).collect();
    if redacted.chars().count() > MAX_SAFE_TEXT_PREVIEW_CHARS {
        output.push_str("...");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> InboundCoalescingPolicy {
        InboundCoalescingPolicy {
            enabled: true,
            debounce_ms: 100,
            max_tracked_keys: 8,
            bypass_commands: true,
            bypass_media: true,
        }
    }

    fn request(message_id: &str, text: &str, received_at_unix_ms: i64) -> InboundCoalescingRequest {
        InboundCoalescingRequest {
            message_id: message_id.to_owned(),
            principal: "user:ops".to_owned(),
            device_id: "device-1".to_owned(),
            session_id: "session-1".to_owned(),
            policy_scope: "channel:discord:ops".to_owned(),
            binding_id: Some("binding-1".to_owned()),
            channel: "discord:ops".to_owned(),
            conversation_id: Some("channel-1".to_owned()),
            thread_id: Some("thread-1".to_owned()),
            sender_identity: Some("discord:user:1".to_owned()),
            text: text.to_owned(),
            received_at_unix_ms,
            has_media: false,
            is_command: false,
            urgent_stop: false,
        }
    }

    #[test]
    fn debounce_merges_same_sender_key_preserving_order_and_provenance() {
        let coalescer = InboundCoalescer::new(policy());
        let first = coalescer.submit(request("m-1", "first", 1_000)).expect("first submit");
        assert_eq!(first.kind, InboundCoalescingDecisionKind::Pending);

        let second = coalescer.submit(request("m-2", "second", 1_050)).expect("second submit");
        assert_eq!(second.kind, InboundCoalescingDecisionKind::Pending);
        assert!(coalescer.drain_ready(1_149).expect("drain").is_empty());

        let drained = coalescer.drain_ready(1_150).expect("drain ready");
        assert_eq!(drained.len(), 1);
        let coalesced = drained[0].coalesced.as_ref().expect("coalesced input");
        assert_eq!(coalesced.text, "first\n\nsecond");
        assert_eq!(
            coalesced
                .provenance
                .iter()
                .map(|item| (item.message_id.as_str(), item.received_at_unix_ms, item.order))
                .collect::<Vec<_>>(),
            vec![("m-1", 1_000, 0), ("m-2", 1_050, 1)]
        );
    }

    #[test]
    fn different_thread_or_sender_stays_separate() {
        let coalescer = InboundCoalescer::new(policy());
        let mut same_sender = request("m-1", "first", 1_000);
        same_sender.thread_id = Some("thread-1".to_owned());
        let mut other_thread = request("m-2", "second", 1_010);
        other_thread.thread_id = Some("thread-2".to_owned());
        let mut other_sender = request("m-3", "third", 1_020);
        other_sender.sender_identity = Some("discord:user:2".to_owned());

        coalescer.submit(same_sender).expect("same sender submit");
        coalescer.submit(other_thread).expect("other thread submit");
        coalescer.submit(other_sender).expect("other sender submit");

        let drained = coalescer.drain_ready(1_200).expect("drain ready");
        assert_eq!(drained.len(), 3);
        assert!(drained.iter().all(|decision| {
            decision.coalesced.as_ref().is_some_and(|input| input.provenance.len() == 1)
        }));
    }

    #[test]
    fn different_principal_or_session_stays_separate() {
        let coalescer = InboundCoalescer::new(policy());
        let first = request("m-1", "first", 1_000);
        let mut other_principal = request("m-2", "second", 1_010);
        other_principal.principal = "user:other".to_owned();
        other_principal.session_id = "session-2".to_owned();
        other_principal.binding_id = Some("binding-2".to_owned());

        coalescer.submit(first).expect("first submit");
        coalescer.submit(other_principal).expect("other principal submit");

        let drained = coalescer.drain_ready(1_200).expect("drain ready");
        assert_eq!(drained.len(), 2);
        assert!(drained.iter().all(|decision| {
            decision.coalesced.as_ref().is_some_and(|input| input.provenance.len() == 1)
        }));
    }

    #[test]
    fn immediate_route_flush_does_not_share_pending_bucket() {
        let coalescer = InboundCoalescer::new(policy());
        let first = coalescer
            .submit_for_immediate_route(request("m-1", "first", 1_000))
            .expect("first immediate submit");
        let second = coalescer
            .submit_for_immediate_route(request("m-2", "second", 1_010))
            .expect("second immediate submit");

        assert_eq!(first.kind, InboundCoalescingDecisionKind::Ready);
        assert_eq!(second.kind, InboundCoalescingDecisionKind::Ready);
        assert_eq!(first.coalesced.as_ref().expect("first coalesced").text, "first");
        assert_eq!(second.coalesced.as_ref().expect("second coalesced").text, "second");
        assert_eq!(first.tracked_key_count, 0);
        assert_eq!(second.tracked_key_count, 0);
    }

    #[test]
    fn commands_media_and_urgent_stop_bypass_debounce() {
        let coalescer = InboundCoalescer::new(policy());
        let mut command = request("m-1", "/palyra status", 1_000);
        command.is_command = true;
        let mut media = request("m-2", "see screenshot", 1_010);
        media.has_media = true;
        let mut urgent_stop = request("m-3", "/palyra stop", 1_020);
        urgent_stop.urgent_stop = true;

        for request in [command, media, urgent_stop] {
            let decision = coalescer.submit(request).expect("bypass submit");
            assert_eq!(decision.kind, InboundCoalescingDecisionKind::Bypassed);
            assert_eq!(decision.coalesced.as_ref().expect("bypass input").provenance.len(), 1);
        }
    }

    #[test]
    fn max_tracked_keys_exceeded_fails_loudly() {
        let coalescer =
            InboundCoalescer::new(InboundCoalescingPolicy { max_tracked_keys: 1, ..policy() });
        coalescer.submit(request("m-1", "first", 1_000)).expect("first submit");
        let mut second = request("m-2", "second", 1_010);
        second.thread_id = Some("thread-2".to_owned());

        let error = coalescer.submit(second).expect_err("second key should exceed limit");
        assert_eq!(error.code(), "inbound_coalescing/max_tracked_keys_exceeded");
    }

    #[test]
    fn safe_snapshot_redacts_sensitive_text_preview() {
        let coalescer = InboundCoalescer::new(InboundCoalescingPolicy::default());
        let decision = coalescer
            .submit(request(
                "m-1",
                "callback https://example.test/cb?token=secret authorization=Bearer SECRET",
                1_000,
            ))
            .expect("policy disabled bypass");
        let snapshot = decision.safe_snapshot_json(coalescer.policy());
        let rendered = snapshot.to_string();
        assert!(rendered.contains("token=<redacted>"));
        assert!(rendered.contains("authorization=<redacted>"));
        assert!(!rendered.contains("SECRET"));
        assert!(!rendered.contains("token=secret"));
    }
}
