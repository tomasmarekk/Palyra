//! Channel-turn domain contracts for inbound channel lifecycle audit.
//!
//! `ChannelRouter` stays authoritative for route policy and concurrency.
//! This module gives that existing path a stable, replay-safe lifecycle
//! vocabulary: an inbound envelope, a pure admission decision, dispatch and
//! delivery outcomes, and bounded journal projections.

use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const CHANNEL_TURN_SCHEMA_VERSION: u32 = 1;
const MAX_SAFE_TEXT_PREVIEW_CHARS: usize = 240;
const MAX_CHANNEL_TURN_METADATA_BYTES: usize = 256;
const DEFAULT_CHANNEL_HISTORY_MAX_RECORDS: usize = 512;
const DEFAULT_CHANNEL_BOT_LOOP_THRESHOLD: u32 = 2;
const DEFAULT_CHANNEL_BOT_LOOP_WINDOW_MS: u64 = 30_000;
const DEFAULT_CHANNEL_BOT_LOOP_COOLDOWN_MS: u64 = 60_000;
const DEFAULT_CHANNEL_BOT_LOOP_MAX_PAIRS: usize = 1_024;

pub(crate) const CHANNEL_TURN_RECEIVED_EVENT: &str = "channel.turn.received";
pub(crate) const CHANNEL_TURN_ADMISSION_DECIDED_EVENT: &str = "channel.turn.admission_decided";
pub(crate) const CHANNEL_TURN_ADMITTED_EVENT: &str = "channel.turn.admitted";
pub(crate) const CHANNEL_TURN_DISPATCHED_EVENT: &str = "channel.turn.dispatched";
pub(crate) const CHANNEL_TURN_DELIVERED_EVENT: &str = "channel.turn.delivered";
pub(crate) const CHANNEL_TURN_DROPPED_EVENT: &str = "channel.turn.dropped";
pub(crate) const CHANNEL_HISTORY_RECORDED_EVENT: &str = "channel.history.recorded";
pub(crate) const CHANNEL_HISTORY_SKIPPED_EVENT: &str = "channel.history.skipped";
pub(crate) const CHANNEL_BOT_LOOP_OBSERVED_EVENT: &str = "channel.bot_loop.observed";
pub(crate) const CHANNEL_BOT_LOOP_DROPPED_EVENT: &str = "channel.bot_loop.dropped";

/// User or connector identity that submitted a channel turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnSender {
    pub(crate) handle: Option<String>,
    pub(crate) display: Option<String>,
    pub(crate) verified: bool,
    pub(crate) gateway_principal: String,
    pub(crate) gateway_device_id: String,
}

/// Destination scope for a channel turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnReceiver {
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) direct_message: bool,
}

/// Redacted message metadata safe for journal and operator diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnMessage {
    pub(crate) text_preview: String,
    pub(crate) text_bytes: usize,
    pub(crate) attachment_count: usize,
    pub(crate) has_media: bool,
    pub(crate) requested_broadcast: bool,
}

/// Stable routing hints captured before route execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnRouteHints {
    pub(crate) adapter_message_id: Option<String>,
    pub(crate) retry_attempt: u32,
    pub(crate) max_payload_bytes: u64,
    pub(crate) json_mode_requested: bool,
    pub(crate) route_config_hash: String,
}

/// Normalized inbound channel event used by the channel-turn kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnEnvelope {
    pub(crate) schema_version: u32,
    pub(crate) correlation_id: String,
    pub(crate) envelope_id: String,
    pub(crate) sender: ChannelTurnSender,
    pub(crate) receiver: ChannelTurnReceiver,
    pub(crate) message: ChannelTurnMessage,
    pub(crate) route_hints: ChannelTurnRouteHints,
    pub(crate) received_at_unix_ms: i64,
    pub(crate) redaction_level: String,
}

/// Inputs required to build a redacted channel-turn envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelTurnEnvelopeInput {
    pub(crate) envelope_id: String,
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_handle: Option<String>,
    pub(crate) sender_display: Option<String>,
    pub(crate) sender_verified: bool,
    pub(crate) gateway_principal: String,
    pub(crate) gateway_device_id: String,
    pub(crate) text: String,
    pub(crate) max_payload_bytes: u64,
    pub(crate) is_direct_message: bool,
    pub(crate) requested_broadcast: bool,
    pub(crate) adapter_message_id: Option<String>,
    pub(crate) retry_attempt: u32,
    pub(crate) attachment_count: usize,
    pub(crate) json_mode_requested: bool,
    pub(crate) route_config_hash: String,
    pub(crate) received_at_unix_ms: i64,
}

impl ChannelTurnEnvelope {
    /// Builds a journal-safe envelope from route-message boundary data.
    #[must_use]
    pub(crate) fn from_input(input: ChannelTurnEnvelopeInput) -> Self {
        let envelope_id = normalize_bounded_required(input.envelope_id);
        let correlation_id = format!("channel_turn:{envelope_id}");
        let has_media = input.attachment_count > 0;
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            correlation_id,
            envelope_id,
            sender: ChannelTurnSender {
                handle: normalize_bounded_optional(input.sender_handle),
                display: normalize_bounded_optional(input.sender_display),
                verified: input.sender_verified,
                gateway_principal: normalize_bounded_required(input.gateway_principal),
                gateway_device_id: normalize_bounded_required(input.gateway_device_id),
            },
            receiver: ChannelTurnReceiver {
                channel: normalize_bounded_required(input.channel),
                conversation_id: normalize_bounded_optional(input.conversation_id),
                thread_id: normalize_bounded_optional(input.thread_id),
                direct_message: input.is_direct_message,
            },
            message: ChannelTurnMessage {
                text_preview: safe_text_preview(input.text.as_str()),
                text_bytes: input.text.len(),
                attachment_count: input.attachment_count,
                has_media,
                requested_broadcast: input.requested_broadcast,
            },
            route_hints: ChannelTurnRouteHints {
                adapter_message_id: normalize_bounded_optional(input.adapter_message_id),
                retry_attempt: input.retry_attempt,
                max_payload_bytes: input.max_payload_bytes,
                json_mode_requested: input.json_mode_requested,
                route_config_hash: normalize_bounded_required(input.route_config_hash),
            },
            received_at_unix_ms: input.received_at_unix_ms,
            redaction_level: "redacted_text_preview".to_owned(),
        }
    }
}

/// Whether the inbound turn had a model-addressing signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelTurnMentionState {
    Matched,
    NotMatched,
    DirectMessage,
    Unknown,
}

impl ChannelTurnMentionState {
    /// Stable label for audit payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::NotMatched => "not_matched",
            Self::DirectMessage => "direct_message",
            Self::Unknown => "unknown",
        }
    }
}

/// Bot identity facts used by admission to avoid self-triggered loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnBotFacts {
    pub(crate) sender_is_self: bool,
    pub(crate) sender_is_bot: bool,
}

/// Provider-agnostic pair key used to detect tight channel bot loops.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct BotPairKey {
    pub(crate) connector_id: String,
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_id: String,
    pub(crate) receiver_id: String,
}

impl BotPairKey {
    /// Builds a key from normalized channel routing identities.
    #[must_use]
    pub(crate) fn new(
        connector_id: impl Into<String>,
        channel: impl Into<String>,
        conversation_id: Option<String>,
        thread_id: Option<String>,
        sender_id: impl Into<String>,
        receiver_id: impl Into<String>,
    ) -> Option<Self> {
        Some(Self {
            connector_id: normalize_required_identity(connector_id.into())?,
            channel: normalize_required_identity(channel.into())?,
            conversation_id: normalize_optional(conversation_id),
            thread_id: normalize_optional(thread_id),
            sender_id: normalize_required_identity(sender_id.into())?,
            receiver_id: normalize_required_identity(receiver_id.into())?,
        })
    }

    #[must_use]
    fn key_hash_sha256(&self) -> String {
        let mut canonical = String::new();
        append_key_field(&mut canonical, self.connector_id.as_str());
        append_key_field(&mut canonical, self.channel.as_str());
        append_key_option(&mut canonical, self.conversation_id.as_deref());
        append_key_option(&mut canonical, self.thread_id.as_deref());
        append_key_field(&mut canonical, self.sender_id.as_str());
        append_key_field(&mut canonical, self.receiver_id.as_str());
        format!("sha256:{}", crate::sha256_hex(canonical.as_bytes()))
    }
}

/// Conservative bot-loop guard policy for the production channel-turn path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelBotLoopGuardConfig {
    pub(crate) threshold: u32,
    pub(crate) window_ms: u64,
    pub(crate) cooldown_ms: u64,
    pub(crate) max_tracked_pairs: usize,
}

impl Default for ChannelBotLoopGuardConfig {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_CHANNEL_BOT_LOOP_THRESHOLD,
            window_ms: DEFAULT_CHANNEL_BOT_LOOP_WINDOW_MS,
            cooldown_ms: DEFAULT_CHANNEL_BOT_LOOP_COOLDOWN_MS,
            max_tracked_pairs: DEFAULT_CHANNEL_BOT_LOOP_MAX_PAIRS,
        }
    }
}

impl ChannelBotLoopGuardConfig {
    #[must_use]
    fn normalized(self) -> Self {
        Self {
            threshold: self.threshold.max(1),
            window_ms: self.window_ms.max(1),
            cooldown_ms: self.cooldown_ms.max(1),
            max_tracked_pairs: self.max_tracked_pairs.max(1),
        }
    }
}

/// Stable outcome labels emitted by [`ChannelBotLoopGuard`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BotLoopDecisionKind {
    Bypassed,
    Observed,
    Dropped,
}

impl BotLoopDecisionKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Bypassed => "bypassed",
            Self::Observed => "observed",
            Self::Dropped => "dropped",
        }
    }
}

/// Audit-safe decision emitted by the provider-agnostic bot loop guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BotLoopDecision {
    pub(crate) schema_version: u32,
    pub(crate) kind: BotLoopDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) key_hash_sha256: Option<String>,
    pub(crate) occurrence_count: u32,
    pub(crate) threshold: u32,
    pub(crate) window_ms: u64,
    pub(crate) cooldown_ms: u64,
    pub(crate) cooldown_until_unix_ms: Option<i64>,
    pub(crate) redaction_level: String,
}

impl BotLoopDecision {
    #[must_use]
    pub(crate) fn bypassed(reason_code: impl Into<String>) -> Self {
        let config = ChannelBotLoopGuardConfig::default().normalized();
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: BotLoopDecisionKind::Bypassed,
            reason_code: reason_code.into(),
            key_hash_sha256: None,
            occurrence_count: 0,
            threshold: config.threshold,
            window_ms: config.window_ms,
            cooldown_ms: config.cooldown_ms,
            cooldown_until_unix_ms: None,
            redaction_level: "metadata_only".to_owned(),
        }
    }

    #[must_use]
    fn observed(
        key: &BotPairKey,
        bucket: &BotLoopBucket,
        config: ChannelBotLoopGuardConfig,
    ) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: BotLoopDecisionKind::Observed,
            reason_code: "channel.bot_loop.observed".to_owned(),
            key_hash_sha256: Some(key.key_hash_sha256()),
            occurrence_count: bucket.occurrence_count,
            threshold: config.threshold,
            window_ms: config.window_ms,
            cooldown_ms: config.cooldown_ms,
            cooldown_until_unix_ms: bucket.cooldown_until_unix_ms,
            redaction_level: "metadata_only".to_owned(),
        }
    }

    #[must_use]
    fn dropped(
        key: &BotPairKey,
        bucket: &BotLoopBucket,
        config: ChannelBotLoopGuardConfig,
    ) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: BotLoopDecisionKind::Dropped,
            reason_code: "channel.bot_loop.dropped.bot_loop_protection".to_owned(),
            key_hash_sha256: Some(key.key_hash_sha256()),
            occurrence_count: bucket.occurrence_count,
            threshold: config.threshold,
            window_ms: config.window_ms,
            cooldown_ms: config.cooldown_ms,
            cooldown_until_unix_ms: bucket.cooldown_until_unix_ms,
            redaction_level: "metadata_only".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BotLoopBucket {
    first_seen_unix_ms: i64,
    last_seen_unix_ms: i64,
    occurrence_count: u32,
    cooldown_until_unix_ms: Option<i64>,
}

impl BotLoopBucket {
    fn new(now_unix_ms: i64) -> Self {
        Self {
            first_seen_unix_ms: now_unix_ms,
            last_seen_unix_ms: now_unix_ms,
            occurrence_count: 0,
            cooldown_until_unix_ms: None,
        }
    }

    fn reset_window(&mut self, now_unix_ms: i64) {
        self.first_seen_unix_ms = now_unix_ms;
        self.last_seen_unix_ms = now_unix_ms;
        self.occurrence_count = 0;
        self.cooldown_until_unix_ms = None;
    }
}

#[derive(Debug, Default)]
struct ChannelBotLoopGuardState {
    buckets: HashMap<BotPairKey, BotLoopBucket>,
}

/// Bounded in-memory detector for repeated channel bot pair traffic.
///
/// The guard is intentionally narrow: it only decides whether the current
/// channel turn should be admitted. Durable audit comes from
/// `channel.bot_loop.*` journal events emitted by the caller.
#[derive(Debug)]
pub(crate) struct ChannelBotLoopGuard {
    config: ChannelBotLoopGuardConfig,
    state: Mutex<ChannelBotLoopGuardState>,
}

impl Default for ChannelBotLoopGuard {
    fn default() -> Self {
        Self::new(ChannelBotLoopGuardConfig::default())
    }
}

impl ChannelBotLoopGuard {
    /// Creates an empty guard using normalized policy limits.
    #[must_use]
    pub(crate) fn new(config: ChannelBotLoopGuardConfig) -> Self {
        Self { config: config.normalized(), state: Mutex::new(ChannelBotLoopGuardState::default()) }
    }

    /// Observes one provider-agnostic bot pair and returns an audit-safe decision.
    pub(crate) fn observe(&self, key: BotPairKey, now_unix_ms: i64) -> BotLoopDecision {
        let now_unix_ms = now_unix_ms.max(0);
        let mut guard = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let bucket =
            guard.buckets.entry(key.clone()).or_insert_with(|| BotLoopBucket::new(now_unix_ms));

        if let Some(cooldown_until_unix_ms) = bucket.cooldown_until_unix_ms {
            if now_unix_ms < cooldown_until_unix_ms {
                bucket.last_seen_unix_ms = now_unix_ms;
                return BotLoopDecision::dropped(&key, bucket, self.config);
            }
            bucket.reset_window(now_unix_ms);
        }

        if elapsed_ms(bucket.first_seen_unix_ms, now_unix_ms) > self.config.window_ms {
            bucket.reset_window(now_unix_ms);
        }
        bucket.occurrence_count = bucket.occurrence_count.saturating_add(1);
        bucket.last_seen_unix_ms = now_unix_ms;
        if bucket.occurrence_count > self.config.threshold {
            bucket.cooldown_until_unix_ms =
                Some(unix_ms_saturating_add(now_unix_ms, self.config.cooldown_ms));
            let decision = BotLoopDecision::dropped(&key, bucket, self.config);
            enforce_bot_loop_pair_limit(&mut guard, self.config.max_tracked_pairs);
            return decision;
        }
        let decision = BotLoopDecision::observed(&key, bucket, self.config);
        enforce_bot_loop_pair_limit(&mut guard, self.config.max_tracked_pairs);
        decision
    }
}

/// Effective route policy facts supplied by the existing channel router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnPolicyFacts {
    pub(crate) channel_enabled: bool,
    pub(crate) route_allowed: bool,
}

/// Conversation-binding facts relevant to admission diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnBindingFacts {
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_kind: Option<String>,
    pub(crate) binding_present: bool,
}

/// Media facts that admission can inspect without opening attachment bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnMediaFacts {
    pub(crate) attachment_count: usize,
    pub(crate) has_media: bool,
}

/// Existing router outcome normalized for the channel-turn decision layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelTurnRouterOutcomeKind {
    Routed,
    Queued,
    Rejected,
}

impl ChannelTurnRouterOutcomeKind {
    /// Stable label for audit payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Routed => "routed",
            Self::Queued => "queued",
            Self::Rejected => "rejected",
        }
    }
}

/// Side-effect-free input for channel-turn admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnAdmissionInput {
    pub(crate) mention: ChannelTurnMentionState,
    pub(crate) bot: ChannelTurnBotFacts,
    pub(crate) bot_loop: BotLoopDecision,
    pub(crate) policy: ChannelTurnPolicyFacts,
    pub(crate) binding: ChannelTurnBindingFacts,
    pub(crate) media: ChannelTurnMediaFacts,
    pub(crate) router_outcome: ChannelTurnRouterOutcomeKind,
    pub(crate) router_reason: Option<String>,
    pub(crate) queued_for_retry: bool,
    pub(crate) is_channel_command: bool,
    pub(crate) urgent_command: bool,
    pub(crate) ambient_context_enabled: bool,
}

/// High-level admission result for one channel turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelTurnAdmissionKind {
    Dispatch,
    ObserveOnly,
    Drop,
    HandledNoRun,
}

impl ChannelTurnAdmissionKind {
    /// Stable label for audit payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatch => "dispatch",
            Self::ObserveOnly => "observe_only",
            Self::Drop => "drop",
            Self::HandledNoRun => "handled_no_run",
        }
    }
}

/// Stable reason codes for admission decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelAdmissionReason {
    DispatchMention,
    DispatchDirectMessage,
    DispatchRouterAccepted,
    ObserveAmbient,
    HandledCommand,
    HandledQueued,
    DropBotLoop,
    DropBotLoopProtection,
    DropPolicyDenied,
    DropRouterRejected,
    DropNoRoute,
}

impl ChannelAdmissionReason {
    /// Stable reason code persisted in journal payloads.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DispatchMention => "channel.admission.dispatch.mention",
            Self::DispatchDirectMessage => "channel.admission.dispatch.direct_message",
            Self::DispatchRouterAccepted => "channel.admission.dispatch.router_accepted",
            Self::ObserveAmbient => "channel.admission.observe.ambient",
            Self::HandledCommand => "channel.admission.handled.command",
            Self::HandledQueued => "channel.admission.handled.queued",
            Self::DropBotLoop => "channel.admission.drop.bot_loop",
            Self::DropBotLoopProtection => "channel.admission.drop.bot_loop_protection",
            Self::DropPolicyDenied => "channel.admission.drop.policy_denied",
            Self::DropRouterRejected => "channel.admission.drop.router_rejected",
            Self::DropNoRoute => "channel.admission.drop.no_route",
        }
    }

    #[must_use]
    const fn kind(self) -> ChannelTurnAdmissionKind {
        match self {
            Self::DispatchMention | Self::DispatchDirectMessage | Self::DispatchRouterAccepted => {
                ChannelTurnAdmissionKind::Dispatch
            }
            Self::ObserveAmbient => ChannelTurnAdmissionKind::ObserveOnly,
            Self::HandledCommand | Self::HandledQueued => ChannelTurnAdmissionKind::HandledNoRun,
            Self::DropBotLoop
            | Self::DropBotLoopProtection
            | Self::DropPolicyDenied
            | Self::DropRouterRejected
            | Self::DropNoRoute => ChannelTurnAdmissionKind::Drop,
        }
    }
}

/// Pure admission decision safe to persist and replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnAdmission {
    pub(crate) schema_version: u32,
    pub(crate) kind: ChannelTurnAdmissionKind,
    pub(crate) reason: ChannelAdmissionReason,
    pub(crate) reason_code: String,
    pub(crate) model_request_permitted: bool,
    pub(crate) durable_history_permitted: bool,
    pub(crate) visible_delivery_expected: bool,
    pub(crate) redaction_level: String,
}

impl ChannelTurnAdmission {
    #[must_use]
    fn new(reason: ChannelAdmissionReason) -> Self {
        let kind = reason.kind();
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind,
            reason,
            reason_code: reason.as_str().to_owned(),
            model_request_permitted: kind == ChannelTurnAdmissionKind::Dispatch,
            durable_history_permitted: kind != ChannelTurnAdmissionKind::Drop,
            visible_delivery_expected: matches!(
                kind,
                ChannelTurnAdmissionKind::Dispatch | ChannelTurnAdmissionKind::HandledNoRun
            ),
            redaction_level: "metadata_only".to_owned(),
        }
    }
}

/// Computes admission without journal writes, route dispatch, or provider I/O.
#[must_use]
pub(crate) fn decide_channel_turn_admission(
    input: &ChannelTurnAdmissionInput,
) -> ChannelTurnAdmission {
    if input.bot_loop.kind == BotLoopDecisionKind::Dropped {
        return ChannelTurnAdmission::new(ChannelAdmissionReason::DropBotLoopProtection);
    }
    if input.bot.sender_is_self {
        return ChannelTurnAdmission::new(ChannelAdmissionReason::DropBotLoop);
    }
    if !input.policy.channel_enabled || !input.policy.route_allowed {
        return ChannelTurnAdmission::new(ChannelAdmissionReason::DropPolicyDenied);
    }
    if input.queued_for_retry || input.router_outcome == ChannelTurnRouterOutcomeKind::Queued {
        return ChannelTurnAdmission::new(ChannelAdmissionReason::HandledQueued);
    }
    if input.is_channel_command || input.urgent_command {
        return ChannelTurnAdmission::new(ChannelAdmissionReason::HandledCommand);
    }
    if input.router_outcome == ChannelTurnRouterOutcomeKind::Rejected {
        if input.ambient_context_enabled
            && input.router_reason.as_deref() == Some("no_matching_mention_or_dm_policy")
            && matches!(
                input.mention,
                ChannelTurnMentionState::NotMatched | ChannelTurnMentionState::Unknown
            )
        {
            return ChannelTurnAdmission::new(ChannelAdmissionReason::ObserveAmbient);
        }
        return ChannelTurnAdmission::new(ChannelAdmissionReason::DropRouterRejected);
    }
    match input.mention {
        ChannelTurnMentionState::DirectMessage => {
            ChannelTurnAdmission::new(ChannelAdmissionReason::DispatchDirectMessage)
        }
        ChannelTurnMentionState::Matched => {
            ChannelTurnAdmission::new(ChannelAdmissionReason::DispatchMention)
        }
        ChannelTurnMentionState::Unknown
            if input.router_outcome == ChannelTurnRouterOutcomeKind::Routed =>
        {
            ChannelTurnAdmission::new(ChannelAdmissionReason::DispatchRouterAccepted)
        }
        ChannelTurnMentionState::NotMatched | ChannelTurnMentionState::Unknown
            if input.ambient_context_enabled =>
        {
            ChannelTurnAdmission::new(ChannelAdmissionReason::ObserveAmbient)
        }
        ChannelTurnMentionState::NotMatched | ChannelTurnMentionState::Unknown => {
            ChannelTurnAdmission::new(ChannelAdmissionReason::DropNoRoute)
        }
    }
}

/// Dispatch state after admission is applied to the route-message adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelTurnDispatchKind {
    Dispatched,
    NotDispatched,
    Queued,
}

impl ChannelTurnDispatchKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::NotDispatched => "not_dispatched",
            Self::Queued => "queued",
        }
    }
}

/// Result of trying to dispatch a channel turn to the model/runtime path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnDispatchOutcome {
    pub(crate) schema_version: u32,
    pub(crate) kind: ChannelTurnDispatchKind,
    pub(crate) reason_code: String,
    pub(crate) route_key: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) model_request_started: bool,
}

impl ChannelTurnDispatchOutcome {
    /// Builds a dispatched outcome for an accepted route-message run.
    #[must_use]
    pub(crate) fn dispatched(
        route_key: String,
        session_id: Option<String>,
        run_id: Option<String>,
    ) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: ChannelTurnDispatchKind::Dispatched,
            reason_code: "channel.dispatch.route_message".to_owned(),
            route_key: Some(route_key),
            session_id,
            run_id,
            model_request_started: true,
        }
    }

    /// Builds a no-dispatch outcome for handled commands, drops, and queued turns.
    #[must_use]
    pub(crate) fn not_dispatched(reason_code: impl Into<String>) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: ChannelTurnDispatchKind::NotDispatched,
            reason_code: reason_code.into(),
            route_key: None,
            session_id: None,
            run_id: None,
            model_request_started: false,
        }
    }

    /// Builds a no-provider outcome for a turn queued by the channel router.
    #[must_use]
    pub(crate) fn queued(reason_code: impl Into<String>) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: ChannelTurnDispatchKind::Queued,
            reason_code: reason_code.into(),
            route_key: None,
            session_id: None,
            run_id: None,
            model_request_started: false,
        }
    }
}

/// Channel-visible delivery state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelTurnDeliveryKind {
    VisibleOutput,
    NoVisibleOutput,
    DeferredRetry,
    Dropped,
    Failed,
}

impl ChannelTurnDeliveryKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::VisibleOutput => "visible_output",
            Self::NoVisibleOutput => "no_visible_output",
            Self::DeferredRetry => "deferred_retry",
            Self::Dropped => "dropped",
            Self::Failed => "failed",
        }
    }
}

/// Result of rendering or suppressing channel-visible output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnDeliveryOutcome {
    pub(crate) schema_version: u32,
    pub(crate) kind: ChannelTurnDeliveryKind,
    pub(crate) reason_code: String,
    pub(crate) visible_to_channel: bool,
    pub(crate) output_count: usize,
    pub(crate) retry_scheduled: bool,
    pub(crate) redaction_level: String,
}

impl ChannelTurnDeliveryOutcome {
    /// Builds a delivery outcome from route-message response semantics.
    #[must_use]
    pub(crate) fn from_route_response(
        accepted: bool,
        queued_for_retry: bool,
        output_count: usize,
        decision_reason: &str,
    ) -> Self {
        let kind = if queued_for_retry {
            ChannelTurnDeliveryKind::DeferredRetry
        } else if !accepted {
            ChannelTurnDeliveryKind::Dropped
        } else if output_count > 0 {
            ChannelTurnDeliveryKind::VisibleOutput
        } else {
            ChannelTurnDeliveryKind::NoVisibleOutput
        };
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind,
            reason_code: delivery_reason_code(kind, decision_reason),
            visible_to_channel: kind == ChannelTurnDeliveryKind::VisibleOutput,
            output_count,
            retry_scheduled: queued_for_retry,
            redaction_level: "metadata_only".to_owned(),
        }
    }
}

/// One bounded, journal-safe channel-turn projection record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnJournalRecord {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) correlation_id: String,
    pub(crate) envelope_id: String,
    pub(crate) channel: String,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) reason_code: String,
    pub(crate) redaction_level: String,
    pub(crate) payload: Value,
}

impl ChannelTurnJournalRecord {
    /// Creates a journal-safe record with stable common fields.
    #[must_use]
    pub(crate) fn new(
        event_type: &str,
        envelope: &ChannelTurnEnvelope,
        session_id: Option<&str>,
        run_id: Option<&str>,
        reason_code: &str,
        payload: Value,
    ) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            event_type: event_type.to_owned(),
            correlation_id: envelope.correlation_id.clone(),
            envelope_id: envelope.envelope_id.clone(),
            channel: envelope.receiver.channel.clone(),
            session_id: session_id.map(str::to_owned),
            run_id: run_id.map(str::to_owned),
            reason_code: reason_code.to_owned(),
            redaction_level: "metadata_or_redacted_preview".to_owned(),
            payload,
        }
    }

    /// Converts the record into the payload stored by message-router journal.
    #[must_use]
    pub(crate) fn payload_json(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "event_type": self.event_type,
            "correlation_id": self.correlation_id,
            "envelope_id": self.envelope_id,
            "channel": self.channel,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "reason_code": self.reason_code,
            "redaction_level": self.redaction_level,
            "payload": self.payload,
        })
    }
}

/// Bounded in-memory projection for recent channel-turn events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelTurnHistory {
    max_records: usize,
    records: VecDeque<ChannelTurnJournalRecord>,
}

impl ChannelTurnHistory {
    /// Creates a bounded history. Zero capacity is normalized to one record.
    #[must_use]
    pub(crate) fn new(max_records: usize) -> Self {
        Self { max_records: max_records.max(1), records: VecDeque::new() }
    }

    /// Adds `record`, evicting the oldest records to maintain the bound.
    pub(crate) fn push(&mut self, record: ChannelTurnJournalRecord) {
        while self.records.len() >= self.max_records {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    /// Returns records in deterministic oldest-to-newest order.
    #[must_use]
    pub(crate) fn records(&self) -> Vec<ChannelTurnJournalRecord> {
        self.records.iter().cloned().collect()
    }

    /// Renders a journal-safe read model snapshot.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn safe_snapshot_json(&self) -> Value {
        json!({
            "schema_version": CHANNEL_TURN_SCHEMA_VERSION,
            "max_records": self.max_records,
            "record_count": self.records.len(),
            "records": self.records.iter().map(ChannelTurnJournalRecord::payload_json).collect::<Vec<_>>(),
        })
    }
}

/// Scope used to retain nearby channel turns without crossing channel,
/// conversation, thread, or sender boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelHistoryScope {
    pub(crate) channel: String,
    pub(crate) conversation_id: Option<String>,
    pub(crate) thread_id: Option<String>,
    pub(crate) sender_handle: Option<String>,
}

impl ChannelHistoryScope {
    #[must_use]
    fn from_envelope(envelope: &ChannelTurnEnvelope) -> Option<Self> {
        Some(Self {
            channel: envelope.receiver.channel.clone(),
            conversation_id: envelope.receiver.conversation_id.clone(),
            thread_id: envelope.receiver.thread_id.clone(),
            sender_handle: Some(envelope.sender.handle.clone()?),
        })
    }

    #[must_use]
    fn matches_envelope(&self, envelope: &ChannelTurnEnvelope) -> bool {
        Self::from_envelope(envelope).as_ref() == Some(self)
    }
}

/// One redacted channel turn retained for later ambient-context assembly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelHistoryEntry {
    pub(crate) schema_version: u32,
    pub(crate) sequence: u64,
    pub(crate) scope: ChannelHistoryScope,
    pub(crate) envelope: ChannelTurnEnvelope,
    pub(crate) admission_kind: ChannelTurnAdmissionKind,
    pub(crate) admission_reason_code: String,
    pub(crate) stored_at_unix_ms: i64,
    pub(crate) redaction_level: String,
}

/// One redacted observe-only turn selected for model-visible ambient context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelHistoryAmbientContextEntry {
    pub(crate) sequence: u64,
    pub(crate) envelope_id: String,
    pub(crate) sender_handle: Option<String>,
    pub(crate) received_at_unix_ms: i64,
    pub(crate) stored_at_unix_ms: i64,
    pub(crate) admission_reason_code: String,
    pub(crate) text_preview: String,
    pub(crate) source_refs: Vec<String>,
}

impl ChannelHistoryAmbientContextEntry {
    #[must_use]
    fn from_record(record: &ChannelHistoryEntry) -> Self {
        Self {
            sequence: record.sequence,
            envelope_id: record.envelope.envelope_id.clone(),
            sender_handle: record.envelope.sender.handle.clone(),
            received_at_unix_ms: record.envelope.received_at_unix_ms,
            stored_at_unix_ms: record.stored_at_unix_ms,
            admission_reason_code: record.admission_reason_code.clone(),
            text_preview: record.envelope.message.text_preview.clone(),
            source_refs: vec![
                format!("channel_history:sequence:{}", record.sequence),
                format!("channel_envelope:{}", record.envelope.envelope_id),
            ],
        }
    }
}

/// Bounded, redacted observe-only history selected for context assembly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelHistoryAmbientContext {
    pub(crate) schema_version: u32,
    pub(crate) scope: ChannelHistoryScope,
    pub(crate) entries: Vec<ChannelHistoryAmbientContextEntry>,
    pub(crate) reason_code: String,
    pub(crate) redaction_level: String,
}

/// Whether the bounded channel history store retained a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChannelHistoryDecisionKind {
    Recorded,
    Skipped,
}

impl ChannelHistoryDecisionKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::Skipped => "skipped",
        }
    }
}

/// Audit-safe result of attempting to retain a channel turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelHistoryDecision {
    pub(crate) schema_version: u32,
    pub(crate) kind: ChannelHistoryDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) sequence: Option<u64>,
    pub(crate) record_count: usize,
    pub(crate) max_records: usize,
    pub(crate) evicted_count: usize,
    pub(crate) redaction_level: String,
}

impl ChannelHistoryDecision {
    fn recorded(
        sequence: u64,
        record_count: usize,
        max_records: usize,
        evicted_count: usize,
    ) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: ChannelHistoryDecisionKind::Recorded,
            reason_code: "channel.history.recorded".to_owned(),
            sequence: Some(sequence),
            record_count,
            max_records,
            evicted_count,
            redaction_level: "redacted_text_preview".to_owned(),
        }
    }

    fn skipped(reason_code: &str, record_count: usize, max_records: usize) -> Self {
        Self {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            kind: ChannelHistoryDecisionKind::Skipped,
            reason_code: reason_code.to_owned(),
            sequence: None,
            record_count,
            max_records,
            evicted_count: 0,
            redaction_level: "metadata_only".to_owned(),
        }
    }
}

#[derive(Debug, Default)]
struct ChannelHistoryStoreState {
    next_sequence: u64,
    records: VecDeque<ChannelHistoryEntry>,
}

/// Bounded in-memory history of redacted channel turns.
///
/// The store intentionally keeps only `ChannelTurnEnvelope` previews and
/// admission metadata. Context assembly can read this for ambient context
/// without giving unmentioned channel chatter instruction authority or
/// persisting raw text.
#[derive(Debug)]
pub(crate) struct ChannelHistoryStore {
    max_records: usize,
    state: Mutex<ChannelHistoryStoreState>,
}

impl Default for ChannelHistoryStore {
    fn default() -> Self {
        Self::new(DEFAULT_CHANNEL_HISTORY_MAX_RECORDS)
    }
}

impl ChannelHistoryStore {
    /// Creates an empty bounded history store. Zero capacity is normalized
    /// to one record so the eviction invariant stays simple.
    #[must_use]
    pub(crate) fn new(max_records: usize) -> Self {
        Self {
            max_records: max_records.max(1),
            state: Mutex::new(ChannelHistoryStoreState::default()),
        }
    }

    /// Records `envelope` when admission permits durable history.
    ///
    /// Drop decisions are skipped to avoid retaining policy-denied or bot-loop
    /// traffic for future model context.
    pub(crate) fn record(
        &self,
        envelope: &ChannelTurnEnvelope,
        admission: &ChannelTurnAdmission,
        stored_at_unix_ms: i64,
    ) -> ChannelHistoryDecision {
        let mut guard = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !admission.durable_history_permitted {
            return ChannelHistoryDecision::skipped(
                "channel.history.skipped.durable_history_denied",
                guard.records.len(),
                self.max_records,
            );
        }
        let Some(scope) = ChannelHistoryScope::from_envelope(envelope) else {
            return ChannelHistoryDecision::skipped(
                "channel.history.skipped.missing_stable_sender_handle",
                guard.records.len(),
                self.max_records,
            );
        };

        let sequence = guard.next_sequence;
        guard.next_sequence = guard.next_sequence.saturating_add(1);
        let mut evicted_count = 0;
        while guard.records.len() >= self.max_records {
            guard.records.pop_front();
            evicted_count += 1;
        }
        guard.records.push_back(ChannelHistoryEntry {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            sequence,
            scope,
            envelope: envelope.clone(),
            admission_kind: admission.kind,
            admission_reason_code: admission.reason_code.clone(),
            stored_at_unix_ms,
            redaction_level: "redacted_text_preview".to_owned(),
        });
        ChannelHistoryDecision::recorded(
            sequence,
            guard.records.len(),
            self.max_records,
            evicted_count,
        )
    }

    /// Selects recent observe-only turns from the same channel scope.
    ///
    /// The current envelope is excluded so a dispatch turn never re-injects
    /// its own text through the ambient path.
    #[must_use]
    pub(crate) fn ambient_observe_only_context(
        &self,
        current_envelope: &ChannelTurnEnvelope,
        max_entries: usize,
    ) -> Option<ChannelHistoryAmbientContext> {
        let scope = ChannelHistoryScope::from_envelope(current_envelope)?;
        let limit = max_entries.max(1);
        let guard = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut entries = guard
            .records
            .iter()
            .rev()
            .filter(|record| record.admission_kind == ChannelTurnAdmissionKind::ObserveOnly)
            .filter(|record| record.scope.matches_envelope(current_envelope))
            .filter(|record| record.envelope.envelope_id != current_envelope.envelope_id)
            .take(limit)
            .map(ChannelHistoryAmbientContextEntry::from_record)
            .collect::<Vec<_>>();
        entries.reverse();
        (!entries.is_empty()).then(|| ChannelHistoryAmbientContext {
            schema_version: CHANNEL_TURN_SCHEMA_VERSION,
            scope,
            entries,
            reason_code: "channel.history.ambient_context.selected".to_owned(),
            redaction_level: "redacted_text_preview".to_owned(),
        })
    }

    #[cfg(test)]
    fn records(&self) -> Vec<ChannelHistoryEntry> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .records
            .iter()
            .cloned()
            .collect()
    }
}

/// Builds the `channel.history.*` projection payload.
#[must_use]
pub(crate) fn channel_turn_history_record(
    envelope: &ChannelTurnEnvelope,
    decision: &ChannelHistoryDecision,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> ChannelTurnJournalRecord {
    let event_type = match decision.kind {
        ChannelHistoryDecisionKind::Recorded => CHANNEL_HISTORY_RECORDED_EVENT,
        ChannelHistoryDecisionKind::Skipped => CHANNEL_HISTORY_SKIPPED_EVENT,
    };
    ChannelTurnJournalRecord::new(
        event_type,
        envelope,
        session_id,
        run_id,
        decision.reason_code.as_str(),
        json!({
            "history": decision,
            "history_kind": decision.kind.as_str(),
        }),
    )
}

/// Builds the `channel.bot_loop.*` projection payload when the guard observed a pair.
#[must_use]
pub(crate) fn channel_bot_loop_record(
    envelope: &ChannelTurnEnvelope,
    decision: &BotLoopDecision,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> Option<ChannelTurnJournalRecord> {
    let event_type = match decision.kind {
        BotLoopDecisionKind::Bypassed => return None,
        BotLoopDecisionKind::Observed => CHANNEL_BOT_LOOP_OBSERVED_EVENT,
        BotLoopDecisionKind::Dropped => CHANNEL_BOT_LOOP_DROPPED_EVENT,
    };
    Some(ChannelTurnJournalRecord::new(
        event_type,
        envelope,
        session_id,
        run_id,
        decision.reason_code.as_str(),
        json!({
            "bot_loop": decision,
            "bot_loop_kind": decision.kind.as_str(),
        }),
    ))
}

/// Builds the `channel.turn.received` projection payload.
#[must_use]
pub(crate) fn channel_turn_received_record(
    envelope: &ChannelTurnEnvelope,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> ChannelTurnJournalRecord {
    ChannelTurnJournalRecord::new(
        CHANNEL_TURN_RECEIVED_EVENT,
        envelope,
        session_id,
        run_id,
        "channel.turn.received",
        json!({ "envelope": envelope }),
    )
}

/// Builds the `channel.turn.admission_decided` projection payload.
#[must_use]
pub(crate) fn channel_turn_admission_record(
    envelope: &ChannelTurnEnvelope,
    admission: &ChannelTurnAdmission,
    input: &ChannelTurnAdmissionInput,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> ChannelTurnJournalRecord {
    ChannelTurnJournalRecord::new(
        CHANNEL_TURN_ADMISSION_DECIDED_EVENT,
        envelope,
        session_id,
        run_id,
        admission.reason_code.as_str(),
        json!({
            "admission": admission,
            "admission_kind": admission.kind.as_str(),
            "input": {
                "mention": input.mention.as_str(),
                "bot": input.bot,
                "bot_loop": input.bot_loop,
                "bot_loop_kind": input.bot_loop.kind.as_str(),
                "policy": input.policy,
                "binding": input.binding,
                "media": input.media,
                "router_outcome": input.router_outcome.as_str(),
                "router_reason": input.router_reason,
                "queued_for_retry": input.queued_for_retry,
                "is_channel_command": input.is_channel_command,
                "urgent_command": input.urgent_command,
                "ambient_context_enabled": input.ambient_context_enabled,
            }
        }),
    )
}

/// Builds an admitted or dropped projection from the admission decision.
#[must_use]
pub(crate) fn channel_turn_admission_terminal_record(
    envelope: &ChannelTurnEnvelope,
    admission: &ChannelTurnAdmission,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> ChannelTurnJournalRecord {
    let event_type = if admission.kind == ChannelTurnAdmissionKind::Drop {
        CHANNEL_TURN_DROPPED_EVENT
    } else {
        CHANNEL_TURN_ADMITTED_EVENT
    };
    ChannelTurnJournalRecord::new(
        event_type,
        envelope,
        session_id,
        run_id,
        admission.reason_code.as_str(),
        json!({ "admission": admission, "admission_kind": admission.kind.as_str() }),
    )
}

/// Builds the `channel.turn.dispatched` projection payload.
#[must_use]
pub(crate) fn channel_turn_dispatched_record(
    envelope: &ChannelTurnEnvelope,
    dispatch: &ChannelTurnDispatchOutcome,
) -> ChannelTurnJournalRecord {
    ChannelTurnJournalRecord::new(
        CHANNEL_TURN_DISPATCHED_EVENT,
        envelope,
        dispatch.session_id.as_deref(),
        dispatch.run_id.as_deref(),
        dispatch.reason_code.as_str(),
        json!({
            "dispatch": {
                "schema_version": dispatch.schema_version,
                "kind": dispatch.kind.as_str(),
                "reason_code": dispatch.reason_code,
                "route_key": dispatch.route_key,
                "session_id": dispatch.session_id,
                "run_id": dispatch.run_id,
                "model_request_started": dispatch.model_request_started,
            }
        }),
    )
}

/// Builds the `channel.turn.delivered` projection payload.
#[must_use]
pub(crate) fn channel_turn_delivered_record(
    envelope: &ChannelTurnEnvelope,
    delivery: &ChannelTurnDeliveryOutcome,
    session_id: Option<&str>,
    run_id: Option<&str>,
) -> ChannelTurnJournalRecord {
    ChannelTurnJournalRecord::new(
        CHANNEL_TURN_DELIVERED_EVENT,
        envelope,
        session_id,
        run_id,
        delivery.reason_code.as_str(),
        json!({
            "delivery": {
                "schema_version": delivery.schema_version,
                "kind": delivery.kind.as_str(),
                "reason_code": delivery.reason_code,
                "visible_to_channel": delivery.visible_to_channel,
                "output_count": delivery.output_count,
                "retry_scheduled": delivery.retry_scheduled,
                "redaction_level": delivery.redaction_level,
            }
        }),
    )
}

fn delivery_reason_code(kind: ChannelTurnDeliveryKind, decision_reason: &str) -> String {
    let suffix = normalize_reason_fragment(decision_reason);
    match kind {
        ChannelTurnDeliveryKind::VisibleOutput => "channel.delivery.visible_output".to_owned(),
        ChannelTurnDeliveryKind::NoVisibleOutput => "channel.delivery.no_visible_output".to_owned(),
        ChannelTurnDeliveryKind::DeferredRetry => {
            format!("channel.delivery.deferred_retry.{suffix}")
        }
        ChannelTurnDeliveryKind::Dropped => format!("channel.delivery.dropped.{suffix}"),
        ChannelTurnDeliveryKind::Failed => format!("channel.delivery.failed.{suffix}"),
    }
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn normalize_bounded_required(value: String) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= MAX_CHANNEL_TURN_METADATA_BYTES {
        return trimmed.to_owned();
    }
    // Preserve stable equality for oversized routing identities without
    // copying attacker-controlled metadata into every lifecycle projection.
    format!("oversize-sha256:{}", crate::sha256_hex(trimmed.as_bytes()))
}

fn normalize_bounded_optional(value: Option<String>) -> Option<String> {
    normalize_optional(value).map(normalize_bounded_required)
}

fn normalize_required_identity(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn append_key_field(output: &mut String, value: &str) {
    output.push_str(value.len().to_string().as_str());
    output.push(':');
    output.push_str(value);
    output.push('|');
}

fn append_key_option(output: &mut String, value: Option<&str>) {
    match value {
        Some(value) => append_key_field(output, value),
        None => append_key_field(output, "<none>"),
    }
}

fn elapsed_ms(first_seen_unix_ms: i64, now_unix_ms: i64) -> u64 {
    let elapsed = now_unix_ms.saturating_sub(first_seen_unix_ms);
    u64::try_from(elapsed).unwrap_or(0)
}

fn unix_ms_saturating_add(now_unix_ms: i64, delta_ms: u64) -> i64 {
    now_unix_ms.saturating_add(i64::try_from(delta_ms).unwrap_or(i64::MAX))
}

fn enforce_bot_loop_pair_limit(guard: &mut ChannelBotLoopGuardState, max_tracked_pairs: usize) {
    while guard.buckets.len() > max_tracked_pairs {
        let Some(victim_key) = guard
            .buckets
            .iter()
            .min_by_key(|(_key, bucket)| bucket.last_seen_unix_ms)
            .map(|(key, _bucket)| key.clone())
        else {
            break;
        };
        guard.buckets.remove(&victim_key);
    }
}

fn normalize_reason_fragment(value: &str) -> String {
    let normalized =
        value
            .trim()
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();
    let collapsed =
        normalized.split('_').filter(|part| !part.is_empty()).collect::<Vec<_>>().join("_");
    if collapsed.is_empty() {
        "unspecified".to_owned()
    } else {
        collapsed
    }
}

// Redaction must precede truncation so a shortened preview cannot expose a
// value that the full redactor would have masked.
fn safe_text_preview(value: &str) -> String {
    let redacted = redact_url_segments_in_text(&redact_auth_error(value));
    let mut output = redacted.chars().take(MAX_SAFE_TEXT_PREVIEW_CHARS).collect::<String>();
    if redacted.chars().count() > MAX_SAFE_TEXT_PREVIEW_CHARS {
        output.push_str("...");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> ChannelTurnEnvelope {
        ChannelTurnEnvelope::from_input(ChannelTurnEnvelopeInput {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: "discord:ops".to_owned(),
            conversation_id: Some("C01".to_owned()),
            thread_id: Some("T01".to_owned()),
            sender_handle: Some("U123".to_owned()),
            sender_display: Some("Ops User".to_owned()),
            sender_verified: true,
            gateway_principal: "principal-1".to_owned(),
            gateway_device_id: "device-1".to_owned(),
            text: "please help token=sk-secret-token".to_owned(),
            max_payload_bytes: 4_096,
            is_direct_message: false,
            requested_broadcast: false,
            adapter_message_id: Some("m-1".to_owned()),
            retry_attempt: 0,
            attachment_count: 1,
            json_mode_requested: true,
            route_config_hash: "route-hash".to_owned(),
            received_at_unix_ms: 10,
        })
    }

    fn admission_input() -> ChannelTurnAdmissionInput {
        ChannelTurnAdmissionInput {
            mention: ChannelTurnMentionState::Matched,
            bot: ChannelTurnBotFacts { sender_is_self: false, sender_is_bot: false },
            bot_loop: BotLoopDecision::bypassed("channel.bot_loop.bypassed.test"),
            policy: ChannelTurnPolicyFacts { channel_enabled: true, route_allowed: true },
            binding: ChannelTurnBindingFacts {
                binding_id: Some("binding-1".to_owned()),
                binding_kind: Some("conversation".to_owned()),
                binding_present: true,
            },
            media: ChannelTurnMediaFacts { attachment_count: 1, has_media: true },
            router_outcome: ChannelTurnRouterOutcomeKind::Routed,
            router_reason: None,
            queued_for_retry: false,
            is_channel_command: false,
            urgent_command: false,
            ambient_context_enabled: false,
        }
    }

    #[test]
    fn envelope_redacts_text_preview_before_journal_projection() {
        let envelope = envelope();

        assert_eq!(envelope.schema_version, 1);
        assert!(!envelope.message.text_preview.contains("sk-secret-token"));
        assert_eq!(envelope.message.text_bytes, "please help token=sk-secret-token".len());
        assert!(envelope.message.has_media);

        let payload = channel_turn_received_record(&envelope, Some("session-1"), Some("run-1"))
            .payload_json();
        assert_eq!(payload["event"], CHANNEL_TURN_RECEIVED_EVENT);
        assert_eq!(payload["reason_code"], "channel.turn.received");
        assert_eq!(payload["payload"]["envelope"]["redaction_level"], "redacted_text_preview");
    }

    #[test]
    fn envelope_bounds_connector_metadata_before_journal_projection() {
        let oversized = "x".repeat(64 * 1_024);
        let envelope = ChannelTurnEnvelope::from_input(ChannelTurnEnvelopeInput {
            envelope_id: oversized.clone(),
            channel: oversized.clone(),
            conversation_id: Some(oversized.clone()),
            thread_id: Some(oversized.clone()),
            sender_handle: Some(oversized.clone()),
            sender_display: Some(oversized.clone()),
            sender_verified: true,
            gateway_principal: oversized.clone(),
            gateway_device_id: oversized.clone(),
            text: "bounded body".to_owned(),
            max_payload_bytes: 4_096,
            is_direct_message: false,
            requested_broadcast: false,
            adapter_message_id: Some(oversized.clone()),
            retry_attempt: 0,
            attachment_count: 0,
            json_mode_requested: false,
            route_config_hash: oversized,
            received_at_unix_ms: 10,
        });

        assert!(envelope.envelope_id.starts_with("oversize-sha256:"));
        assert!(envelope.sender.handle.as_deref().is_some_and(|value| {
            value.starts_with("oversize-sha256:") && value.len() <= MAX_CHANNEL_TURN_METADATA_BYTES
        }));
        assert!(envelope.receiver.conversation_id.as_deref().is_some_and(|value| {
            value.starts_with("oversize-sha256:") && value.len() <= MAX_CHANNEL_TURN_METADATA_BYTES
        }));
        assert!(envelope.route_hints.adapter_message_id.as_deref().is_some_and(|value| {
            value.starts_with("oversize-sha256:") && value.len() <= MAX_CHANNEL_TURN_METADATA_BYTES
        }));

        let payload = channel_turn_received_record(&envelope, Some("session-1"), Some("run-1"))
            .payload_json()
            .to_string();
        assert!(payload.len() < 16 * 1_024);
    }

    #[test]
    fn admission_dispatch_for_mention_is_deterministic() {
        let input = admission_input();
        let first = decide_channel_turn_admission(&input);
        let second = decide_channel_turn_admission(&input);

        assert_eq!(first, second);
        assert_eq!(first.kind, ChannelTurnAdmissionKind::Dispatch);
        assert!(first.model_request_permitted);
        assert_eq!(first.reason_code, "channel.admission.dispatch.mention");
    }

    #[test]
    fn observe_only_never_permits_model_request() {
        let mut input = admission_input();
        input.mention = ChannelTurnMentionState::NotMatched;
        input.router_outcome = ChannelTurnRouterOutcomeKind::Rejected;
        input.router_reason = Some("no_matching_mention_or_dm_policy".to_owned());
        input.ambient_context_enabled = true;

        let admission = decide_channel_turn_admission(&input);

        assert_eq!(admission.kind, ChannelTurnAdmissionKind::ObserveOnly);
        assert!(!admission.model_request_permitted);
        assert_eq!(admission.reason_code, "channel.admission.observe.ambient");

        input.router_reason = Some("sender_denied".to_owned());
        let denied = decide_channel_turn_admission(&input);
        assert_eq!(denied.kind, ChannelTurnAdmissionKind::Drop);
        assert_eq!(denied.reason_code, "channel.admission.drop.router_rejected");
    }

    #[test]
    fn bot_loop_guard_serializes_audit_safe_decisions() {
        let key = BotPairKey::new(
            "discord",
            "discord:ops",
            Some("C01".to_owned()),
            Some("T01".to_owned()),
            "bot-a",
            "bot-b",
        )
        .expect("valid pair key");
        let guard = ChannelBotLoopGuard::new(ChannelBotLoopGuardConfig {
            threshold: 1,
            window_ms: 1_000,
            cooldown_ms: 5_000,
            max_tracked_pairs: 4,
        });

        let observed = guard.observe(key.clone(), 10);
        let dropped = guard.observe(key, 20);

        assert_eq!(observed.kind, BotLoopDecisionKind::Observed);
        assert_eq!(observed.reason_code, "channel.bot_loop.observed");
        assert_eq!(dropped.kind, BotLoopDecisionKind::Dropped);
        assert_eq!(dropped.reason_code, "channel.bot_loop.dropped.bot_loop_protection");
        assert_eq!(dropped.cooldown_until_unix_ms, Some(5_020));
        let serialized = serde_json::to_string(&dropped).expect("decision should serialize");
        assert!(serialized.contains("key_hash_sha256"));
        assert!(!serialized.contains("bot-a"));
        let roundtrip: BotLoopDecision =
            serde_json::from_str(serialized.as_str()).expect("decision should deserialize");
        assert_eq!(roundtrip, dropped);

        let payload =
            channel_bot_loop_record(&envelope(), &dropped, Some("session-1"), Some("run-1"))
                .expect("dropped decision should produce a journal record")
                .payload_json();
        assert_eq!(payload["event"], CHANNEL_BOT_LOOP_DROPPED_EVENT);
        assert_eq!(payload["payload"]["bot_loop_kind"], "dropped");
    }

    #[test]
    fn bot_loop_guard_drops_repeated_bot_pair_without_blocking_other_sender() {
        let guard = ChannelBotLoopGuard::new(ChannelBotLoopGuardConfig {
            threshold: 2,
            window_ms: 1_000,
            cooldown_ms: 5_000,
            max_tracked_pairs: 8,
        });
        let bot_a_to_b = BotPairKey::new(
            "discord",
            "discord:ops",
            Some("C01".to_owned()),
            Some("T01".to_owned()),
            "bot-a",
            "bot-b",
        )
        .expect("bot-a key");
        let bot_b_to_a = BotPairKey::new(
            "discord",
            "discord:ops",
            Some("C01".to_owned()),
            Some("T01".to_owned()),
            "bot-b",
            "bot-a",
        )
        .expect("bot-b key");
        let human_to_b = BotPairKey::new(
            "discord",
            "discord:ops",
            Some("C01".to_owned()),
            Some("T01".to_owned()),
            "human-user",
            "bot-b",
        )
        .expect("human key");

        assert_eq!(guard.observe(bot_a_to_b.clone(), 100).kind, BotLoopDecisionKind::Observed);
        assert_eq!(guard.observe(bot_b_to_a.clone(), 150).kind, BotLoopDecisionKind::Observed);
        assert_eq!(guard.observe(bot_a_to_b.clone(), 200).kind, BotLoopDecisionKind::Observed);
        assert_eq!(guard.observe(bot_b_to_a, 250).kind, BotLoopDecisionKind::Observed);
        assert_eq!(guard.observe(bot_a_to_b.clone(), 300).kind, BotLoopDecisionKind::Dropped);
        assert_eq!(guard.observe(bot_a_to_b, 400).kind, BotLoopDecisionKind::Dropped);
        assert_eq!(guard.observe(human_to_b, 450).kind, BotLoopDecisionKind::Observed);
    }

    #[test]
    fn bot_loop_guard_drops_before_dispatch() {
        let mut input = admission_input();
        input.bot.sender_is_self = true;

        let admission = decide_channel_turn_admission(&input);

        assert_eq!(admission.kind, ChannelTurnAdmissionKind::Drop);
        assert_eq!(admission.reason_code, "channel.admission.drop.bot_loop");
        assert!(!admission.durable_history_permitted);
    }

    #[test]
    fn admission_drops_when_bot_loop_guard_enforces_protection() {
        let mut input = admission_input();
        let key = BotPairKey::new("discord", "discord:ops", None, None, "bot-a", "palyra")
            .expect("valid bot loop key");
        let guard = ChannelBotLoopGuard::new(ChannelBotLoopGuardConfig {
            threshold: 1,
            window_ms: 1_000,
            cooldown_ms: 5_000,
            max_tracked_pairs: 4,
        });
        guard.observe(key.clone(), 10);
        input.bot_loop = guard.observe(key, 20);

        let admission = decide_channel_turn_admission(&input);

        assert_eq!(admission.kind, ChannelTurnAdmissionKind::Drop);
        assert_eq!(admission.reason_code, "channel.admission.drop.bot_loop_protection");
        assert!(!admission.model_request_permitted);
        assert!(!admission.durable_history_permitted);
    }

    #[test]
    fn bounded_history_evicts_oldest_record() {
        let envelope = envelope();
        let mut history = ChannelTurnHistory::new(2);
        history.push(channel_turn_received_record(&envelope, None, None));
        history.push(ChannelTurnJournalRecord::new(
            CHANNEL_TURN_ADMITTED_EVENT,
            &envelope,
            None,
            None,
            "second",
            json!({}),
        ));
        history.push(ChannelTurnJournalRecord::new(
            CHANNEL_TURN_DELIVERED_EVENT,
            &envelope,
            None,
            None,
            "third",
            json!({}),
        ));

        let records = history.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].reason_code, "second");
        assert_eq!(records[1].reason_code, "third");
        assert_eq!(history.safe_snapshot_json()["record_count"], 2);
    }

    #[test]
    fn channel_history_store_records_bounded_redacted_turns() {
        let store = ChannelHistoryStore::new(2);
        let admission = decide_channel_turn_admission(&admission_input());
        let first = envelope();
        let mut second = envelope();
        second.envelope_id = "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned();
        second.correlation_id = format!("channel_turn:{}", second.envelope_id);
        let mut third = envelope();
        third.envelope_id = "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned();
        third.correlation_id = format!("channel_turn:{}", third.envelope_id);

        let first_decision = store.record(&first, &admission, 10);
        let second_decision = store.record(&second, &admission, 20);
        let third_decision = store.record(&third, &admission, 30);

        assert_eq!(first_decision.kind, ChannelHistoryDecisionKind::Recorded);
        assert_eq!(second_decision.sequence, Some(1));
        assert_eq!(third_decision.sequence, Some(2));
        assert_eq!(third_decision.evicted_count, 1);
        let records = store.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[1].sequence, 2);
        assert_eq!(records[1].scope.channel, "discord:ops");
        assert_eq!(records[1].admission_reason_code, "channel.admission.dispatch.mention");

        let serialized =
            serde_json::to_string(&records[1]).expect("history entry should serialize");
        assert!(!serialized.contains("sk-secret-token"));
        assert!(serialized.contains("token=<redacted>"));
        let roundtrip: ChannelHistoryEntry =
            serde_json::from_str(serialized.as_str()).expect("history entry should deserialize");
        assert_eq!(roundtrip, records[1]);

        let payload =
            channel_turn_history_record(&third, &third_decision, Some("session-1"), Some("run-1"))
                .payload_json();
        assert_eq!(payload["event"], CHANNEL_HISTORY_RECORDED_EVENT);
        assert_eq!(payload["payload"]["history_kind"], "recorded");
    }

    #[test]
    fn channel_history_store_skips_drop_admissions() {
        let store = ChannelHistoryStore::new(2);
        let mut input = admission_input();
        input.bot.sender_is_self = true;
        let admission = decide_channel_turn_admission(&input);
        let envelope = envelope();

        let decision = store.record(&envelope, &admission, 10);

        assert_eq!(decision.kind, ChannelHistoryDecisionKind::Skipped);
        assert_eq!(decision.reason_code, "channel.history.skipped.durable_history_denied");
        assert_eq!(decision.record_count, 0);
        assert!(store.records().is_empty());
        let payload = channel_turn_history_record(&envelope, &decision, None, None).payload_json();
        assert_eq!(payload["event"], CHANNEL_HISTORY_SKIPPED_EVENT);
        assert_eq!(payload["payload"]["history_kind"], "skipped");
    }

    #[test]
    fn channel_history_selects_recent_observe_only_context_by_scope() {
        let store = ChannelHistoryStore::new(4);
        let mut input = admission_input();
        input.mention = ChannelTurnMentionState::NotMatched;
        input.router_outcome = ChannelTurnRouterOutcomeKind::Rejected;
        input.router_reason = Some("no_matching_mention_or_dm_policy".to_owned());
        input.ambient_context_enabled = true;
        let observe = decide_channel_turn_admission(&input);
        assert_eq!(observe.kind, ChannelTurnAdmissionKind::ObserveOnly);

        let mut first = envelope();
        first.envelope_id = "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned();
        first.message.text_preview = "ambient one".to_owned();
        let mut second = envelope();
        second.envelope_id = "01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned();
        second.message.text_preview = "ambient two".to_owned();
        let mut other_sender = envelope();
        other_sender.envelope_id = "01ARZ3NDEKTSV4RRFFQ69G5FB2".to_owned();
        other_sender.sender.handle = Some("U999".to_owned());
        other_sender.message.text_preview = "different sender".to_owned();
        let current = envelope();

        store.record(&first, &observe, 10);
        store.record(&second, &observe, 20);
        store.record(&other_sender, &observe, 30);
        let context =
            store.ambient_observe_only_context(&current, 1).expect("matching context should exist");

        assert_eq!(context.reason_code, "channel.history.ambient_context.selected");
        assert_eq!(context.entries.len(), 1);
        assert_eq!(context.entries[0].text_preview, "ambient two");
        assert_eq!(context.entries[0].source_refs[0], "channel_history:sequence:1");
        let serialized = serde_json::to_string(&context).expect("ambient context should serialize");
        let roundtrip: ChannelHistoryAmbientContext =
            serde_json::from_str(serialized.as_str()).expect("ambient context should deserialize");
        assert_eq!(roundtrip, context);
    }

    #[test]
    fn channel_history_rejects_ambient_context_without_a_stable_sender_handle() {
        let store = ChannelHistoryStore::new(4);
        let mut input = admission_input();
        input.mention = ChannelTurnMentionState::NotMatched;
        input.router_outcome = ChannelTurnRouterOutcomeKind::Rejected;
        input.router_reason = Some("no_matching_mention_or_dm_policy".to_owned());
        input.ambient_context_enabled = true;
        let observe = decide_channel_turn_admission(&input);
        let mut anonymous = envelope();
        anonymous.sender.handle = None;
        anonymous.sender.display = Some("Anonymous A".to_owned());

        let decision = store.record(&anonymous, &observe, 10);

        assert_eq!(decision.kind, ChannelHistoryDecisionKind::Skipped);
        assert_eq!(decision.reason_code, "channel.history.skipped.missing_stable_sender_handle");
        assert!(store.records().is_empty());
        assert!(store.ambient_observe_only_context(&anonymous, 4).is_none());
    }

    #[test]
    fn route_response_delivery_semantics_are_visible_and_retry_safe() {
        let visible = ChannelTurnDeliveryOutcome::from_route_response(true, false, 1, "routed");
        assert_eq!(visible.kind, ChannelTurnDeliveryKind::VisibleOutput);
        assert!(visible.visible_to_channel);

        let retry =
            ChannelTurnDeliveryOutcome::from_route_response(false, true, 0, "concurrency_limit");
        assert_eq!(retry.kind, ChannelTurnDeliveryKind::DeferredRetry);
        assert!(retry.retry_scheduled);
        assert_eq!(retry.reason_code, "channel.delivery.deferred_retry.concurrency_limit");
    }
}
