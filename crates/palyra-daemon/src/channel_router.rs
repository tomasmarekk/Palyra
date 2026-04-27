use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use ulid::Ulid;

const DEFAULT_RETRY_BACKOFF_MS: u64 = 250;
const MAX_PER_CHANNEL_QUARANTINE_ITEMS: usize = 256;
const FALLBACK_SENDER_COMPONENT: &str = "unknown";
const FALLBACK_CONVERSATION_COMPONENT: &str = "default";
const MIN_DM_PAIRING_CODE_TTL_MS: u64 = 30_000;
const DEFAULT_DM_PAIRING_CODE_TTL_MS: u64 = 10 * 60_000;
const MAX_DM_PAIRING_CODE_TTL_MS: u64 = 24 * 60 * 60_000;
const DM_PAIRING_CODE_LENGTH: usize = 8;
const DEFAULT_DM_PAIRING_PENDING_TTL_MS: u64 = 15 * 60_000;
const DEFAULT_DM_PAIRING_SESSION_TTL_MS: u64 = 8 * 60 * 60_000;
const MAX_DM_PAIRING_SESSION_TTL_MS: u64 = 30 * 24 * 60 * 60_000;
const MASS_MENTION_EVERYONE: &str = "@everyone";
const MASS_MENTION_HERE: &str = "@here";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastStrategy {
    Deny,
    MentionOnly,
    Allow,
}

impl BroadcastStrategy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::MentionOnly => "mention_only",
            Self::Allow => "allow",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "deny" => Some(Self::Deny),
            "mention_only" | "mention-only" => Some(Self::MentionOnly),
            "allow" => Some(Self::Allow),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectMessagePolicy {
    Deny,
    Pairing,
    Allow,
}

impl DirectMessagePolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Pairing => "pairing",
            Self::Allow => "allow",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "deny" => Some(Self::Deny),
            "pairing" | "pair" => Some(Self::Pairing),
            "allow" => Some(Self::Allow),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PairingCodeRecord {
    pub code: String,
    pub channel: String,
    pub issued_by: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PairingPendingRecord {
    pub channel: String,
    pub sender_identity: String,
    pub code: String,
    pub requested_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PairingGrantRecord {
    pub channel: String,
    pub sender_identity: String,
    pub approved_at_unix_ms: i64,
    pub expires_at_unix_ms: Option<i64>,
    pub approval_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelPairingSnapshot {
    pub channel: String,
    pub pending: Vec<PairingPendingRecord>,
    pub paired: Vec<PairingGrantRecord>,
    pub active_codes: Vec<PairingCodeRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairingConsumeReason {
    ChannelMissing,
    SenderMissing,
    InvalidCode,
    CodeExpired,
    PairingDisabled,
    AlreadyPending,
    AlreadyPaired,
}

impl PairingConsumeReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChannelMissing => "channel_missing",
            Self::SenderMissing => "sender_missing_for_dm_pairing",
            Self::InvalidCode => "direct_message_pairing_code_invalid",
            Self::CodeExpired => "direct_message_pairing_code_expired",
            Self::PairingDisabled => "direct_message_pairing_disabled",
            Self::AlreadyPending => "direct_message_pairing_pending_approval",
            Self::AlreadyPaired => "direct_message_pairing_already_active",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingConsumeOutcome {
    Pending(PairingPendingRecord),
    Rejected(PairingConsumeReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingApprovalOutcome {
    Approved(PairingGrantRecord),
    Denied,
    MissingPending,
    PairingDisabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelRoutingRule {
    pub channel: String,
    pub enabled: bool,
    pub mention_patterns: Vec<String>,
    pub allow_from: Vec<String>,
    pub deny_from: Vec<String>,
    pub allow_direct_messages: bool,
    pub direct_message_policy: DirectMessagePolicy,
    pub isolate_session_by_sender: bool,
    pub response_prefix: Option<String>,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    pub broadcast_strategy: BroadcastStrategy,
    pub concurrency_limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelRouterConfig {
    pub enabled: bool,
    pub max_message_bytes: usize,
    pub max_retry_queue_depth_per_channel: usize,
    pub max_retry_attempts: u32,
    pub retry_backoff_ms: u64,
    pub default_response_prefix: Option<String>,
    pub default_channel_enabled: bool,
    pub default_allow_direct_messages: bool,
    pub default_direct_message_policy: DirectMessagePolicy,
    pub default_isolate_session_by_sender: bool,
    pub default_broadcast_strategy: BroadcastStrategy,
    pub default_concurrency_limit: usize,
    pub inbound_coalescing: InboundCoalescingPolicy,
    pub channels: Vec<ChannelRoutingRule>,
}

impl Default for ChannelRouterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_message_bytes: 32 * 1024,
            max_retry_queue_depth_per_channel: 64,
            max_retry_attempts: 3,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS,
            default_response_prefix: None,
            default_channel_enabled: false,
            default_allow_direct_messages: false,
            default_direct_message_policy: DirectMessagePolicy::Deny,
            default_isolate_session_by_sender: false,
            default_broadcast_strategy: BroadcastStrategy::Deny,
            default_concurrency_limit: 2,
            inbound_coalescing: InboundCoalescingPolicy::default(),
            channels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InboundCoalescingPolicy {
    pub enabled: bool,
    pub debounce_ms: u64,
    pub max_tracked_keys: usize,
    pub bypass_commands: bool,
    pub bypass_media: bool,
}

impl Default for InboundCoalescingPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            debounce_ms: 0,
            max_tracked_keys: 1_024,
            bypass_commands: true,
            bypass_media: true,
        }
    }
}

impl InboundCoalescingPolicy {
    #[must_use]
    pub const fn active(&self) -> bool {
        self.enabled && self.debounce_ms > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    pub envelope_id: String,
    pub channel: String,
    pub conversation_id: Option<String>,
    pub sender_handle: Option<String>,
    pub sender_display: Option<String>,
    pub sender_verified: bool,
    pub text: String,
    pub max_payload_bytes: u64,
    pub is_direct_message: bool,
    pub requested_broadcast: bool,
    pub adapter_message_id: Option<String>,
    pub adapter_thread_id: Option<String>,
    pub retry_attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutePlan {
    pub channel: String,
    pub route_key: String,
    pub session_key: String,
    pub session_label: Option<String>,
    pub binding_id: Option<String>,
    pub binding_kind: Option<String>,
    pub binding_expires_at_unix_ms: Option<i64>,
    pub binding_reason: Option<String>,
    pub sender_identity: Option<String>,
    pub is_broadcast: bool,
    pub response_prefix: Option<String>,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    pub in_reply_to_message_id: Option<String>,
    pub reply_thread_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteRejection {
    pub reason: String,
    pub quarantined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteQueued {
    pub reason: String,
    pub retry_after_ms: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutePreview {
    pub accepted: bool,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_identity: Option<String>,
    pub config_hash: String,
}

#[derive(Debug)]
pub struct RoutedMessage {
    pub plan: RoutePlan,
    pub lease: ChannelConcurrencyLease,
}

#[derive(Debug)]
pub enum RouteOutcome {
    Routed(Box<RoutedMessage>),
    Queued(RouteQueued),
    Rejected(RouteRejection),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetryQueueEntry {
    pub envelope_id: String,
    pub channel: String,
    pub retry_attempt: u32,
    pub reason: String,
    pub retry_after_ms: u64,
    pub queued_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QuarantinedMessage {
    pub envelope_id: String,
    pub channel: String,
    pub retry_attempt: u32,
    pub reason: String,
    pub quarantined_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    Queued,
    Quarantined,
    Dropped,
}

#[derive(Debug, Default)]
struct ChannelRuntimeState {
    in_flight: usize,
    retry_queue: VecDeque<RetryQueueEntry>,
    quarantine: VecDeque<QuarantinedMessage>,
    pairing_codes: HashMap<String, PairingCodeRecord>,
    pairing_pending_by_sender: HashMap<String, PairingPendingRecord>,
    pairing_pending_by_approval: HashMap<String, String>,
    pairing_grants: HashMap<String, PairingGrantRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteCandidate {
    channel: String,
    rule: ChannelRoutingRule,
    sender_identity: Option<String>,
    route_key: String,
    session_key: String,
    session_label: Option<String>,
    in_reply_to_message_id: Option<String>,
    reply_thread_id: Option<String>,
}

#[derive(Debug)]
pub struct ChannelRouter {
    config: ChannelRouterConfig,
    state: Arc<Mutex<HashMap<String, ChannelRuntimeState>>>,
}

impl ChannelRouter {
    #[must_use]
    pub fn new(config: ChannelRouterConfig) -> Self {
        Self { config, state: Arc::new(Mutex::new(HashMap::new())) }
    }

    #[must_use]
    pub fn config_hash(&self) -> String {
        let payload = serde_json::to_vec(&self.config).unwrap_or_default();
        sha256_hex(payload.as_slice())
    }

    #[must_use]
    pub fn validation_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if !self.config.enabled {
            warnings.push(
                "channel_router.enabled=false: inbound channel routing is globally disabled"
                    .to_owned(),
            );
        }
        if matches!(self.config.default_broadcast_strategy, BroadcastStrategy::Allow) {
            warnings.push(
                "default_broadcast_strategy=allow enables fan-out by default across unmatched channels"
                    .to_owned(),
            );
        }
        if self.config.default_allow_direct_messages
            && matches!(self.config.default_direct_message_policy, DirectMessagePolicy::Allow)
        {
            warnings.push(
                "default direct_message_policy=allow permits all direct messages for channels without explicit overrides"
                    .to_owned(),
            );
        }
        for rule in &self.config.channels {
            if !rule.enabled {
                continue;
            }
            if rule.mention_patterns.is_empty() && !rule.allow_direct_messages {
                warnings.push(format!(
                    "channel '{}' is enabled but has no mention_patterns and direct messages disabled; no inbound text can match",
                    rule.channel
                ));
            }
            if !rule.allow_direct_messages
                && !matches!(rule.direct_message_policy, DirectMessagePolicy::Deny)
            {
                warnings.push(format!(
                    "channel '{}' sets direct_message_policy='{}' while allow_direct_messages=false; DM policy will not be reachable",
                    rule.channel,
                    rule.direct_message_policy.as_str()
                ));
            }
            if rule.allow_direct_messages
                && matches!(rule.direct_message_policy, DirectMessagePolicy::Allow)
                && rule.allow_from.is_empty()
            {
                warnings.push(format!(
                    "channel '{}' allows all direct messages without allowlist or pairing guardrails",
                    rule.channel
                ));
            }
            if matches!(rule.broadcast_strategy, BroadcastStrategy::Allow) {
                warnings.push(format!(
                    "channel '{}' sets broadcast_strategy=allow; ensure policy gate for message.broadcast is intentionally enabled",
                    rule.channel
                ));
            }
            let deny = rule
                .deny_from
                .iter()
                .map(String::as_str)
                .map(normalize_identifier_match)
                .collect::<HashSet<_>>();
            for allowed in &rule.allow_from {
                let normalized = normalize_identifier_match(allowed.as_str());
                if deny.contains(normalized.as_str()) {
                    warnings.push(format!(
                        "channel '{}' contains sender '{}' in both allow_from and deny_from; deny list wins",
                        rule.channel, normalized
                    ));
                }
            }
        }
        warnings
    }

    #[must_use]
    pub fn preview_route(&self, message: &InboundMessage) -> RoutePreview {
        let config_hash = self.config_hash();
        if !self.config.enabled {
            return RoutePreview {
                accepted: false,
                reason: "channel_router_disabled".to_owned(),
                route_key: None,
                session_key: None,
                sender_identity: sender_identity(message),
                config_hash,
            };
        }
        let Some(channel) = normalize_non_empty(message.channel.as_str()) else {
            return RoutePreview {
                accepted: false,
                reason: "channel_missing".to_owned(),
                route_key: None,
                session_key: None,
                sender_identity: sender_identity(message),
                config_hash,
            };
        };
        if message.text.trim().is_empty() {
            return RoutePreview {
                accepted: false,
                reason: "message_empty".to_owned(),
                route_key: None,
                session_key: None,
                sender_identity: sender_identity(message),
                config_hash,
            };
        }
        if message.text.len() > self.config.max_message_bytes
            || (message.max_payload_bytes as usize) > self.config.max_message_bytes
        {
            return RoutePreview {
                accepted: false,
                reason: "message_oversized".to_owned(),
                route_key: None,
                session_key: None,
                sender_identity: sender_identity(message),
                config_hash,
            };
        }
        match self.evaluate_route_policy(channel.as_str(), message) {
            Ok(candidate) => RoutePreview {
                accepted: true,
                reason: "routed".to_owned(),
                route_key: Some(candidate.route_key),
                session_key: Some(candidate.session_key),
                sender_identity: candidate.sender_identity,
                config_hash,
            },
            Err(rejection) => RoutePreview {
                accepted: false,
                reason: rejection.reason,
                route_key: None,
                session_key: None,
                sender_identity: sender_identity(message),
                config_hash,
            },
        }
    }

    #[must_use]
    pub fn pairing_snapshot(&self, channel: Option<&str>) -> Vec<ChannelPairingSnapshot> {
        let filter = channel.and_then(normalize_non_empty);
        let Ok(mut guard) = self.state.lock() else {
            return Vec::new();
        };
        let now = current_unix_ms();
        let mut snapshots = Vec::new();
        for (state_channel, state) in guard.iter_mut() {
            if filter.as_deref().is_some_and(|value| !state_channel.eq_ignore_ascii_case(value)) {
                continue;
            }
            Self::prune_pairing_state(state, now);
            if state.pairing_codes.is_empty()
                && state.pairing_pending_by_sender.is_empty()
                && state.pairing_grants.is_empty()
            {
                continue;
            }
            let mut active_codes = state.pairing_codes.values().cloned().collect::<Vec<_>>();
            active_codes.sort_by(|left, right| {
                left.expires_at_unix_ms
                    .cmp(&right.expires_at_unix_ms)
                    .then_with(|| left.code.cmp(&right.code))
            });
            let mut pending = state.pairing_pending_by_sender.values().cloned().collect::<Vec<_>>();
            pending.sort_by(|left, right| {
                left.requested_at_unix_ms
                    .cmp(&right.requested_at_unix_ms)
                    .then_with(|| left.sender_identity.cmp(&right.sender_identity))
            });
            let mut paired = state.pairing_grants.values().cloned().collect::<Vec<_>>();
            paired.sort_by(|left, right| {
                left.approved_at_unix_ms
                    .cmp(&right.approved_at_unix_ms)
                    .then_with(|| left.sender_identity.cmp(&right.sender_identity))
            });
            snapshots.push(ChannelPairingSnapshot {
                channel: state_channel.clone(),
                pending,
                paired,
                active_codes,
            });
        }
        snapshots.sort_by(|left, right| left.channel.cmp(&right.channel));
        snapshots
    }

    pub fn mint_pairing_code(
        &self,
        channel: &str,
        issued_by: &str,
        ttl_ms: Option<u64>,
    ) -> Result<PairingCodeRecord, PairingConsumeReason> {
        let Some(channel) = normalize_non_empty(channel) else {
            return Err(PairingConsumeReason::ChannelMissing);
        };
        let rule = self.resolve_rule(channel.as_str());
        if !rule.allow_direct_messages
            || !matches!(rule.direct_message_policy, DirectMessagePolicy::Pairing)
        {
            return Err(PairingConsumeReason::PairingDisabled);
        }
        let issued_by = normalize_non_empty(issued_by).unwrap_or_else(|| "operator".to_owned());
        let ttl_ms =
            Self::normalize_pairing_code_ttl_ms(ttl_ms.unwrap_or(DEFAULT_DM_PAIRING_CODE_TTL_MS));
        let now = current_unix_ms();
        let expires_at_unix_ms = now.saturating_add(ttl_ms as i64);
        let Ok(mut guard) = self.state.lock() else {
            return Err(PairingConsumeReason::PairingDisabled);
        };
        let state = guard.entry(channel.clone()).or_default();
        Self::prune_pairing_state(state, now);
        let mut code = String::new();
        for _ in 0..8 {
            code = Self::generate_pairing_code();
            if !state.pairing_codes.contains_key(code.as_str()) {
                break;
            }
        }
        if code.is_empty() || state.pairing_codes.contains_key(code.as_str()) {
            return Err(PairingConsumeReason::InvalidCode);
        }
        let record = PairingCodeRecord {
            code: code.clone(),
            channel,
            issued_by,
            created_at_unix_ms: now,
            expires_at_unix_ms,
        };
        state.pairing_codes.insert(code, record.clone());
        Ok(record)
    }

    #[must_use]
    pub fn consume_pairing_code(
        &self,
        channel: &str,
        sender_identity: Option<&str>,
        code: &str,
        pending_ttl_ms: Option<u64>,
    ) -> PairingConsumeOutcome {
        let Some(channel) = normalize_non_empty(channel) else {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::ChannelMissing);
        };
        let rule = self.resolve_rule(channel.as_str());
        if !rule.allow_direct_messages
            || !matches!(rule.direct_message_policy, DirectMessagePolicy::Pairing)
        {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::PairingDisabled);
        }
        let Some(sender_identity) = sender_identity
            .and_then(normalize_non_empty)
            .map(|value| normalize_identifier_match(value.as_str()))
        else {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::SenderMissing);
        };
        let Some(code) = Self::sanitize_pairing_code(code) else {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::InvalidCode);
        };
        let now = current_unix_ms();
        let pending_ttl_ms = Self::normalize_pairing_pending_ttl_ms(
            pending_ttl_ms.unwrap_or(DEFAULT_DM_PAIRING_PENDING_TTL_MS),
        );
        let Ok(mut guard) = self.state.lock() else {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::PairingDisabled);
        };
        let state = guard.entry(channel.clone()).or_default();
        Self::prune_pairing_state(state, now);
        if state.pairing_grants.contains_key(sender_identity.as_str()) {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::AlreadyPaired);
        }
        if state.pairing_pending_by_sender.contains_key(sender_identity.as_str()) {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::AlreadyPending);
        }
        let Some(active_code) = state.pairing_codes.get(code.as_str()) else {
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::InvalidCode);
        };
        if active_code.expires_at_unix_ms <= now {
            state.pairing_codes.remove(code.as_str());
            return PairingConsumeOutcome::Rejected(PairingConsumeReason::CodeExpired);
        }
        state.pairing_codes.remove(code.as_str());
        let pending = PairingPendingRecord {
            channel,
            sender_identity: sender_identity.clone(),
            code,
            requested_at_unix_ms: now,
            expires_at_unix_ms: now.saturating_add(pending_ttl_ms as i64),
            approval_id: None,
        };
        state.pairing_pending_by_sender.insert(sender_identity, pending.clone());
        PairingConsumeOutcome::Pending(pending)
    }

    #[must_use]
    pub fn attach_pairing_pending_approval(
        &self,
        channel: &str,
        sender_identity: &str,
        approval_id: &str,
    ) -> Option<PairingPendingRecord> {
        let channel = normalize_non_empty(channel)?;
        let sender_identity = normalize_non_empty(sender_identity)
            .map(|value| normalize_identifier_match(value.as_str()))?;
        let approval_id = normalize_non_empty(approval_id)?;
        let now = current_unix_ms();
        let Ok(mut guard) = self.state.lock() else {
            return None;
        };
        let state = guard.get_mut(channel.as_str())?;
        Self::prune_pairing_state(state, now);
        let pending = state.pairing_pending_by_sender.get_mut(sender_identity.as_str())?;
        if let Some(previous) = pending.approval_id.as_deref() {
            state.pairing_pending_by_approval.remove(previous);
        }
        pending.approval_id = Some(approval_id.clone());
        state.pairing_pending_by_approval.insert(approval_id, sender_identity);
        Some(pending.clone())
    }

    #[must_use]
    pub fn apply_pairing_approval(
        &self,
        approval_id: &str,
        approved: bool,
        decision_scope_ttl_ms: Option<i64>,
    ) -> PairingApprovalOutcome {
        let Some(approval_id) = normalize_non_empty(approval_id) else {
            return PairingApprovalOutcome::MissingPending;
        };
        let now = current_unix_ms();
        let Ok(mut guard) = self.state.lock() else {
            return PairingApprovalOutcome::MissingPending;
        };
        for (channel, state) in guard.iter_mut() {
            Self::prune_pairing_state(state, now);
            let Some(sender_identity) =
                state.pairing_pending_by_approval.remove(approval_id.as_str())
            else {
                continue;
            };
            let Some(pending) = state.pairing_pending_by_sender.remove(sender_identity.as_str())
            else {
                continue;
            };
            let rule = self.resolve_rule(channel.as_str());
            if !rule.allow_direct_messages
                || !matches!(rule.direct_message_policy, DirectMessagePolicy::Pairing)
            {
                return PairingApprovalOutcome::PairingDisabled;
            }
            if !approved {
                return PairingApprovalOutcome::Denied;
            }
            let ttl_ms = Self::normalize_pairing_session_ttl_ms(
                decision_scope_ttl_ms
                    .and_then(|value| if value > 0 { Some(value as u64) } else { None })
                    .unwrap_or(DEFAULT_DM_PAIRING_SESSION_TTL_MS),
            );
            let grant = PairingGrantRecord {
                channel: pending.channel,
                sender_identity: pending.sender_identity.clone(),
                approved_at_unix_ms: now,
                expires_at_unix_ms: Some(now.saturating_add(ttl_ms as i64)),
                approval_id: Some(approval_id.clone()),
            };
            state.pairing_grants.insert(pending.sender_identity, grant.clone());
            return PairingApprovalOutcome::Approved(grant);
        }
        PairingApprovalOutcome::MissingPending
    }

    #[must_use]
    pub fn begin_route(&self, message: &InboundMessage) -> RouteOutcome {
        if !self.config.enabled {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "channel_router_disabled".to_owned(),
                quarantined: false,
            });
        }
        let normalized_channel = normalize_non_empty(message.channel.as_str());
        if normalized_channel.is_none() {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "channel_missing".to_owned(),
                quarantined: self.quarantine_message(
                    message,
                    "channel_missing",
                    message.retry_attempt,
                ),
            });
        }
        let channel = normalized_channel.expect("checked above");
        self.dequeue_retry(channel.as_str(), message.envelope_id.as_str());
        if message.text.trim().is_empty() {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "message_empty".to_owned(),
                quarantined: self.quarantine_message(
                    message,
                    "message_empty",
                    message.retry_attempt,
                ),
            });
        }
        if message.text.len() > self.config.max_message_bytes
            || (message.max_payload_bytes as usize) > self.config.max_message_bytes
        {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "message_oversized".to_owned(),
                quarantined: self.quarantine_message(
                    message,
                    "message_oversized",
                    message.retry_attempt,
                ),
            });
        }
        let candidate = match self.evaluate_route_policy(channel.as_str(), message) {
            Ok(candidate) => candidate,
            Err(rejection) => return RouteOutcome::Rejected(rejection),
        };
        let concurrency_limit = candidate
            .rule
            .concurrency_limit
            .unwrap_or(self.config.default_concurrency_limit)
            .max(1);
        let permit = match self.acquire_channel_slot(channel.as_str(), concurrency_limit) {
            Ok(permit) => permit,
            Err(queue_depth) => {
                if queue_depth <= self.config.max_retry_queue_depth_per_channel {
                    let reason = "backpressure_queue_full".to_owned();
                    if self.enqueue_retry(message, channel.as_str(), reason.clone()) {
                        return RouteOutcome::Queued(RouteQueued {
                            reason,
                            retry_after_ms: self.config.retry_backoff_ms,
                            queue_depth,
                        });
                    }
                    return RouteOutcome::Rejected(RouteRejection {
                        reason: "backpressure_retry_enqueue_failed".to_owned(),
                        quarantined: self.quarantine_message(
                            message,
                            "backpressure_retry_enqueue_failed",
                            message.retry_attempt,
                        ),
                    });
                }
                return RouteOutcome::Rejected(RouteRejection {
                    reason: "backpressure_poison_quarantine".to_owned(),
                    quarantined: self.quarantine_message(
                        message,
                        "backpressure_poison_quarantine",
                        message.retry_attempt,
                    ),
                });
            }
        };

        RouteOutcome::Routed(Box::new(RoutedMessage {
            plan: RoutePlan {
                channel: candidate.channel,
                route_key: candidate.route_key,
                session_key: candidate.session_key,
                session_label: candidate.session_label,
                binding_id: None,
                binding_kind: None,
                binding_expires_at_unix_ms: None,
                binding_reason: None,
                sender_identity: candidate.sender_identity,
                is_broadcast: message.requested_broadcast,
                response_prefix: candidate
                    .rule
                    .response_prefix
                    .clone()
                    .or_else(|| self.config.default_response_prefix.clone()),
                auto_ack_text: candidate.rule.auto_ack_text.clone(),
                auto_reaction: candidate.rule.auto_reaction.clone(),
                in_reply_to_message_id: candidate.in_reply_to_message_id,
                reply_thread_id: candidate.reply_thread_id,
            },
            lease: permit,
        }))
    }

    #[must_use]
    pub fn record_processing_failure(
        &self,
        message: &InboundMessage,
        reason: &str,
    ) -> RetryDisposition {
        if message.retry_attempt.saturating_add(1) > self.config.max_retry_attempts {
            return if self.quarantine_message(
                message,
                format!("retry_exhausted:{reason}").as_str(),
                message.retry_attempt,
            ) {
                RetryDisposition::Quarantined
            } else {
                RetryDisposition::Dropped
            };
        }
        if self.enqueue_retry(message, message.channel.as_str(), reason.to_owned()) {
            RetryDisposition::Queued
        } else if self.quarantine_message(
            message,
            format!("retry_enqueue_failed:{reason}").as_str(),
            message.retry_attempt,
        ) {
            RetryDisposition::Quarantined
        } else {
            RetryDisposition::Dropped
        }
    }

    #[must_use]
    pub fn queue_depth(&self) -> usize {
        match self.state.lock() {
            Ok(guard) => guard.values().map(|state| state.retry_queue.len()).sum(),
            Err(_) => 0,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn channel_queue_depth(&self, channel: &str) -> usize {
        let Some(normalized_channel) = normalize_non_empty(channel) else {
            return 0;
        };
        match self.state.lock() {
            Ok(guard) => guard
                .get(normalized_channel.as_str())
                .map(|state| state.retry_queue.len())
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn quarantine_len(&self, channel: &str) -> usize {
        let Some(normalized_channel) = normalize_non_empty(channel) else {
            return 0;
        };
        match self.state.lock() {
            Ok(guard) => guard
                .get(normalized_channel.as_str())
                .map(|state| state.quarantine.len())
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    fn resolve_rule(&self, channel: &str) -> ChannelRoutingRule {
        self.config
            .channels
            .iter()
            .find(|rule| rule.channel.eq_ignore_ascii_case(channel))
            .cloned()
            .unwrap_or(ChannelRoutingRule {
                channel: channel.to_owned(),
                enabled: self.config.default_channel_enabled,
                mention_patterns: Vec::new(),
                allow_from: Vec::new(),
                deny_from: Vec::new(),
                allow_direct_messages: self.config.default_allow_direct_messages,
                direct_message_policy: self.config.default_direct_message_policy,
                isolate_session_by_sender: self.config.default_isolate_session_by_sender,
                response_prefix: None,
                auto_ack_text: None,
                auto_reaction: None,
                broadcast_strategy: self.config.default_broadcast_strategy,
                concurrency_limit: Some(self.config.default_concurrency_limit),
            })
    }

    fn evaluate_route_policy(
        &self,
        channel: &str,
        message: &InboundMessage,
    ) -> Result<RouteCandidate, RouteRejection> {
        let rule = self.resolve_rule(channel);
        if !rule.enabled {
            return Err(RouteRejection {
                reason: "channel_disabled".to_owned(),
                quarantined: false,
            });
        }
        let sender_identity = sender_identity(message);
        let normalized_sender = sender_identity.as_deref().map(normalize_identifier_match);
        if !rule.deny_from.is_empty()
            && normalized_sender.as_deref().is_some_and(|sender| {
                rule.deny_from.iter().any(|blocked| normalize_identifier_match(blocked) == sender)
            })
        {
            return Err(RouteRejection { reason: "sender_denied".to_owned(), quarantined: false });
        }
        let sender_allowlisted = normalized_sender
            .as_deref()
            .is_some_and(|sender| self.sender_is_allowlisted(&rule, sender));
        let strict_allowlist = !(rule.allow_from.is_empty()
            || message.is_direct_message
                && matches!(rule.direct_message_policy, DirectMessagePolicy::Pairing));
        if strict_allowlist {
            if !message.sender_verified {
                return Err(RouteRejection {
                    reason: "sender_unverified_for_allowlist".to_owned(),
                    quarantined: false,
                });
            }
            let Some(sender) = normalized_sender.as_deref() else {
                return Err(RouteRejection {
                    reason: "sender_missing_for_allowlist".to_owned(),
                    quarantined: false,
                });
            };
            if !self.sender_is_allowlisted(&rule, sender) {
                return Err(RouteRejection {
                    reason: "sender_not_allowlisted".to_owned(),
                    quarantined: false,
                });
            }
        }
        let mention_match =
            has_mention_match(message.text.as_str(), rule.mention_patterns.as_slice());
        if message.is_direct_message {
            if !rule.allow_direct_messages {
                return Err(RouteRejection {
                    reason: "direct_message_disabled".to_owned(),
                    quarantined: false,
                });
            }
            match rule.direct_message_policy {
                DirectMessagePolicy::Deny => {
                    return Err(RouteRejection {
                        reason: "direct_message_denied_by_policy".to_owned(),
                        quarantined: false,
                    });
                }
                DirectMessagePolicy::Allow => {}
                DirectMessagePolicy::Pairing => {
                    let Some(sender) = normalized_sender.as_deref() else {
                        return Err(RouteRejection {
                            reason: PairingConsumeReason::SenderMissing.as_str().to_owned(),
                            quarantined: false,
                        });
                    };
                    if !message.sender_verified {
                        return Err(RouteRejection {
                            reason: "sender_unverified_for_dm_pairing".to_owned(),
                            quarantined: false,
                        });
                    }
                    if !sender_allowlisted && !self.is_sender_paired(channel, sender) {
                        return Err(RouteRejection {
                            reason: "direct_message_pairing_required".to_owned(),
                            quarantined: false,
                        });
                    }
                }
            }
        } else if !mention_match {
            return Err(RouteRejection {
                reason: "no_matching_mention_or_dm_policy".to_owned(),
                quarantined: false,
            });
        }
        if message.requested_broadcast {
            match rule.broadcast_strategy {
                BroadcastStrategy::Deny => {
                    return Err(RouteRejection {
                        reason: "broadcast_denied_by_policy".to_owned(),
                        quarantined: false,
                    });
                }
                BroadcastStrategy::MentionOnly if !mention_match => {
                    return Err(RouteRejection {
                        reason: "broadcast_requires_mention_match".to_owned(),
                        quarantined: false,
                    });
                }
                BroadcastStrategy::MentionOnly | BroadcastStrategy::Allow => {}
            }
        }
        let conversation_component = normalize_session_component(
            message.conversation_id.as_deref().unwrap_or(FALLBACK_CONVERSATION_COMPONENT),
        );
        let sender_component = normalize_session_component(
            sender_identity.as_deref().unwrap_or(FALLBACK_SENDER_COMPONENT),
        );
        let route_key = format!("channel:{channel}:conversation:{conversation_component}");
        let session_key = if rule.isolate_session_by_sender {
            format!("{route_key}:sender:{sender_component}")
        } else {
            route_key.clone()
        };
        Ok(RouteCandidate {
            channel: channel.to_owned(),
            rule,
            sender_identity,
            route_key,
            session_key,
            session_label: normalize_non_empty(
                message.conversation_id.as_deref().unwrap_or_default(),
            ),
            in_reply_to_message_id: normalize_non_empty(
                message.adapter_message_id.as_deref().unwrap_or_default(),
            ),
            reply_thread_id: normalize_non_empty(
                message.adapter_thread_id.as_deref().unwrap_or_default(),
            ),
        })
    }

    fn sender_is_allowlisted(&self, rule: &ChannelRoutingRule, sender: &str) -> bool {
        rule.allow_from.iter().any(|value| normalize_identifier_match(value.as_str()) == sender)
    }

    fn is_sender_paired(&self, channel: &str, sender_identity: &str) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        let Some(state) = guard.get_mut(channel) else {
            return false;
        };
        let now = current_unix_ms();
        Self::prune_pairing_state(state, now);
        state.pairing_grants.contains_key(sender_identity)
    }

    fn generate_pairing_code() -> String {
        let raw = Ulid::new().to_string();
        raw[raw.len().saturating_sub(DM_PAIRING_CODE_LENGTH)..].to_owned()
    }

    fn sanitize_pairing_code(code: &str) -> Option<String> {
        let sanitized = code
            .trim()
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();
        if sanitized.len() != DM_PAIRING_CODE_LENGTH {
            return None;
        }
        Some(sanitized)
    }

    fn prune_pairing_state(state: &mut ChannelRuntimeState, now_unix_ms: i64) {
        state.pairing_codes.retain(|_, record| record.expires_at_unix_ms > now_unix_ms);
        state.pairing_pending_by_sender.retain(|_, record| record.expires_at_unix_ms > now_unix_ms);
        let pending_senders =
            state.pairing_pending_by_sender.keys().cloned().collect::<HashSet<_>>();
        state.pairing_pending_by_approval.retain(|_, sender| pending_senders.contains(sender));
        state.pairing_grants.retain(|_, grant| {
            grant
                .expires_at_unix_ms
                .is_none_or(|expires_at_unix_ms| expires_at_unix_ms > now_unix_ms)
        });
    }

    fn normalize_pairing_code_ttl_ms(value: u64) -> u64 {
        value.clamp(MIN_DM_PAIRING_CODE_TTL_MS, MAX_DM_PAIRING_CODE_TTL_MS)
    }

    fn normalize_pairing_pending_ttl_ms(value: u64) -> u64 {
        value.clamp(MIN_DM_PAIRING_CODE_TTL_MS, MAX_DM_PAIRING_CODE_TTL_MS)
    }

    fn normalize_pairing_session_ttl_ms(value: u64) -> u64 {
        value.clamp(MIN_DM_PAIRING_CODE_TTL_MS, MAX_DM_PAIRING_SESSION_TTL_MS)
    }

    fn acquire_channel_slot(
        &self,
        channel: &str,
        concurrency_limit: usize,
    ) -> Result<ChannelConcurrencyLease, usize> {
        let mut guard = self.state.lock().map_err(|_| 0_usize)?;
        let state = guard.entry(channel.to_owned()).or_default();
        if state.in_flight >= concurrency_limit {
            return Err(state.retry_queue.len().saturating_add(1));
        }
        state.in_flight = state.in_flight.saturating_add(1);
        Ok(ChannelConcurrencyLease {
            state: Arc::clone(&self.state),
            channel: channel.to_owned(),
            released: false,
        })
    }

    fn enqueue_retry(&self, message: &InboundMessage, channel: &str, reason: String) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        let state = guard.entry(channel.to_owned()).or_default();
        if let Some(existing) =
            state.retry_queue.iter_mut().find(|entry| entry.envelope_id == message.envelope_id)
        {
            existing.retry_attempt = message.retry_attempt.saturating_add(1);
            existing.reason = reason;
            existing.retry_after_ms = self.config.retry_backoff_ms;
            existing.queued_at_unix_ms = current_unix_ms();
            return true;
        }
        if state.retry_queue.len() >= self.config.max_retry_queue_depth_per_channel {
            return false;
        }
        state.retry_queue.push_back(RetryQueueEntry {
            envelope_id: message.envelope_id.clone(),
            channel: channel.to_owned(),
            retry_attempt: message.retry_attempt.saturating_add(1),
            reason,
            retry_after_ms: self.config.retry_backoff_ms,
            queued_at_unix_ms: current_unix_ms(),
        });
        true
    }

    fn dequeue_retry(&self, channel: &str, envelope_id: &str) {
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        let Some(state) = guard.get_mut(channel) else {
            return;
        };
        state.retry_queue.retain(|entry| entry.envelope_id != envelope_id);
    }

    fn quarantine_message(
        &self,
        message: &InboundMessage,
        reason: &str,
        retry_attempt: u32,
    ) -> bool {
        let Some(channel) = normalize_non_empty(message.channel.as_str()) else {
            return false;
        };
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        let state = guard.entry(channel.clone()).or_default();
        if state.quarantine.len() >= MAX_PER_CHANNEL_QUARANTINE_ITEMS {
            state.quarantine.pop_front();
        }
        state.quarantine.push_back(QuarantinedMessage {
            envelope_id: message.envelope_id.clone(),
            channel,
            retry_attempt,
            reason: reason.to_owned(),
            quarantined_at_unix_ms: current_unix_ms(),
        });
        true
    }
}

#[derive(Debug)]
pub struct ChannelConcurrencyLease {
    state: Arc<Mutex<HashMap<String, ChannelRuntimeState>>>,
    channel: String,
    released: bool,
}

impl ChannelConcurrencyLease {
    #[cfg(test)]
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        if let Some(state) = guard.get_mut(self.channel.as_str()) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

impl Drop for ChannelConcurrencyLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn has_mention_match(text: &str, mention_patterns: &[String]) -> bool {
    if mention_patterns.is_empty() {
        return false;
    }
    let normalized_text = normalize_text_for_mention_matching(text, mention_patterns);
    mention_patterns.iter().any(|pattern| {
        let normalized_pattern = pattern.trim().to_ascii_lowercase();
        if normalized_pattern.is_empty() {
            return false;
        }
        if normalized_pattern == "*" {
            return !normalized_text.trim().is_empty();
        }
        contains_boundary_delimited_pattern(normalized_text.as_str(), normalized_pattern.as_str())
    })
}

fn normalize_text_for_mention_matching(text: &str, mention_patterns: &[String]) -> String {
    let normalized = text.to_ascii_lowercase();
    if mention_patterns_allow_mass_mentions(mention_patterns) {
        return normalized;
    }
    normalized.replace(MASS_MENTION_EVERYONE, " ").replace(MASS_MENTION_HERE, " ")
}

fn mention_patterns_allow_mass_mentions(mention_patterns: &[String]) -> bool {
    mention_patterns.iter().any(|pattern| {
        matches!(
            pattern.trim().to_ascii_lowercase().as_str(),
            MASS_MENTION_EVERYONE | MASS_MENTION_HERE
        )
    })
}

fn is_identifier_continuation(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
}

fn contains_boundary_delimited_pattern(text: &str, pattern: &str) -> bool {
    text.match_indices(pattern).any(|(start, _)| {
        let left_boundary = start == 0 || !is_identifier_continuation(text.as_bytes()[start - 1]);
        let end = start.saturating_add(pattern.len());
        let right_boundary = end >= text.len() || !is_identifier_continuation(text.as_bytes()[end]);
        left_boundary && right_boundary
    })
}

fn normalize_identifier_match(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn sender_identity(message: &InboundMessage) -> Option<String> {
    normalize_non_empty(message.sender_handle.as_deref().unwrap_or_default())
        .or_else(|| normalize_non_empty(message.sender_display.as_deref().unwrap_or_default()))
}

fn normalize_non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn normalize_session_component(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "default".to_owned();
    }

    let mut normalized = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            normalized.push(ch);
            continue;
        }

        let mut utf8 = [0_u8; 4];
        for byte in ch.encode_utf8(&mut utf8).as_bytes() {
            let _ = write!(&mut normalized, "~{byte:02x}");
        }
    }
    normalized
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_session_component, BroadcastStrategy, ChannelRouter, ChannelRouterConfig,
        ChannelRoutingRule, DirectMessagePolicy, InboundCoalescingPolicy, InboundMessage,
        PairingApprovalOutcome, PairingConsumeOutcome, RetryDisposition, RouteOutcome,
    };

    fn baseline_config() -> ChannelRouterConfig {
        ChannelRouterConfig {
            enabled: true,
            max_message_bytes: 8 * 1024,
            max_retry_queue_depth_per_channel: 2,
            max_retry_attempts: 2,
            retry_backoff_ms: 100,
            default_response_prefix: Some("Palyra: ".to_owned()),
            default_channel_enabled: false,
            default_allow_direct_messages: false,
            default_direct_message_policy: DirectMessagePolicy::Deny,
            default_isolate_session_by_sender: false,
            default_broadcast_strategy: BroadcastStrategy::Deny,
            default_concurrency_limit: 2,
            inbound_coalescing: InboundCoalescingPolicy::default(),
            channels: vec![ChannelRoutingRule {
                channel: "slack".to_owned(),
                enabled: true,
                mention_patterns: vec!["@palyra".to_owned()],
                allow_from: vec![],
                deny_from: vec![],
                allow_direct_messages: false,
                direct_message_policy: DirectMessagePolicy::Deny,
                isolate_session_by_sender: false,
                response_prefix: Some("[bot] ".to_owned()),
                auto_ack_text: Some("processing".to_owned()),
                auto_reaction: Some("eyes".to_owned()),
                broadcast_strategy: BroadcastStrategy::MentionOnly,
                concurrency_limit: Some(1),
            }],
        }
    }

    fn inbound(text: &str) -> InboundMessage {
        InboundMessage {
            envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
            channel: "slack".to_owned(),
            conversation_id: Some("C01TEAM".to_owned()),
            sender_handle: Some("U123".to_owned()),
            sender_display: Some("ops".to_owned()),
            sender_verified: true,
            text: text.to_owned(),
            max_payload_bytes: 4096,
            is_direct_message: false,
            requested_broadcast: false,
            adapter_message_id: Some("m-1".to_owned()),
            adapter_thread_id: Some("thread-1".to_owned()),
            retry_attempt: 0,
        }
    }

    fn inbound_with_id(envelope_id: &str, text: &str) -> InboundMessage {
        let mut message = inbound(text);
        message.envelope_id = envelope_id.to_owned();
        message
    }

    #[test]
    fn mention_match_is_required_when_dm_policy_is_disabled() {
        let router = ChannelRouter::new(baseline_config());
        let without_mention = inbound("hello team");
        let outcome = router.begin_route(&without_mention);
        assert!(matches!(
            outcome,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "no_matching_mention_or_dm_policy"
        ));
    }

    #[test]
    fn mention_match_routes_message_with_session_mapping() {
        let router = ChannelRouter::new(baseline_config());
        let with_mention = inbound("hello @Palyra please summarize");
        let outcome = router.begin_route(&with_mention);
        let RouteOutcome::Routed(routed) = outcome else {
            panic!("mention hit should route");
        };
        assert_eq!(routed.plan.channel, "slack");
        assert_eq!(routed.plan.route_key, "channel:slack:conversation:C01TEAM");
        assert_eq!(routed.plan.session_key, "channel:slack:conversation:C01TEAM");
        assert_eq!(routed.plan.response_prefix.as_deref(), Some("[bot] "));
        routed.lease.release();
    }

    #[test]
    fn session_component_encoding_avoids_normalization_collisions() {
        assert_eq!(normalize_session_component("support:team"), "support~3ateam");
        assert_eq!(normalize_session_component("support_team"), "support_team");
        assert_ne!(
            normalize_session_component("support:team"),
            normalize_session_component("support_team")
        );
        assert_ne!(
            normalize_session_component("CaseSensitive"),
            normalize_session_component("casesensitive")
        );
    }

    #[test]
    fn mention_substring_inside_email_does_not_trigger_route() {
        let router = ChannelRouter::new(baseline_config());
        let outcome = router.begin_route(&inbound("contact alpha@palyra.io for updates"));
        assert!(matches!(
            outcome,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "no_matching_mention_or_dm_policy"
        ));
    }

    #[test]
    fn mention_substring_inside_handle_like_token_does_not_trigger_route() {
        let router = ChannelRouter::new(baseline_config());
        for text in [
            "ping @palyra_admin for approvals",
            "ping @palyra-prod for deploy status",
            "ping @palyra.ops for routing status",
        ] {
            let outcome = router.begin_route(&inbound(text));
            assert!(
                matches!(
                    outcome,
                    RouteOutcome::Rejected(ref rejection)
                        if rejection.reason == "no_matching_mention_or_dm_policy"
                ),
                "handle-like token should not trigger mention pattern: {text}"
            );
        }
    }

    #[test]
    fn mention_pattern_still_matches_with_punctuation_boundaries() {
        let router = ChannelRouter::new(baseline_config());
        for text in ["hello @palyra, summarize status", "hello (@palyra) summarize status"] {
            let outcome = router.begin_route(&inbound(text));
            let RouteOutcome::Routed(routed) = outcome else {
                panic!("punctuation-delimited mention should route: {text}");
            };
            routed.lease.release();
        }
    }

    #[test]
    fn mass_mention_does_not_trigger_wildcard_without_explicit_opt_in() {
        let mut config = baseline_config();
        config.channels[0].mention_patterns = vec!["*".to_owned()];
        let router = ChannelRouter::new(config);
        let outcome = router.begin_route(&inbound("@everyone"));
        assert!(matches!(
            outcome,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "no_matching_mention_or_dm_policy"
        ));
    }

    #[test]
    fn mass_mention_triggers_only_with_explicit_pattern() {
        let mut config = baseline_config();
        config.channels[0].mention_patterns = vec!["@everyone".to_owned()];
        let router = ChannelRouter::new(config);
        let outcome = router.begin_route(&inbound("@everyone please summarize"));
        assert!(
            matches!(outcome, RouteOutcome::Routed(_)),
            "explicit @everyone mention pattern should opt in to mass-mention trigger"
        );
    }

    #[test]
    fn here_mention_requires_explicit_pattern() {
        let mut wildcard_config = baseline_config();
        wildcard_config.channels[0].mention_patterns = vec!["*".to_owned()];
        let wildcard_router = ChannelRouter::new(wildcard_config);
        let rejected = wildcard_router.begin_route(&inbound("@here"));
        assert!(matches!(
            rejected,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "no_matching_mention_or_dm_policy"
        ));

        let mut explicit_config = baseline_config();
        explicit_config.channels[0].mention_patterns = vec!["@here".to_owned()];
        let explicit_router = ChannelRouter::new(explicit_config);
        let accepted = explicit_router.begin_route(&inbound("@here check status"));
        assert!(
            matches!(accepted, RouteOutcome::Routed(_)),
            "explicit @here mention pattern should opt in to mass-mention trigger"
        );
    }

    #[test]
    fn direct_message_policy_allows_messages_without_mentions() {
        let mut config = baseline_config();
        config.channels[0].allow_direct_messages = true;
        config.channels[0].direct_message_policy = DirectMessagePolicy::Allow;
        let router = ChannelRouter::new(config);
        let mut message = inbound("plain dm question");
        message.is_direct_message = true;
        let outcome = router.begin_route(&message);
        assert!(matches!(outcome, RouteOutcome::Routed(_)));
    }

    #[test]
    fn default_broadcast_strategy_denies_broadcast_requests() {
        let mut config = baseline_config();
        config.default_channel_enabled = true;
        config.default_allow_direct_messages = true;
        config.default_direct_message_policy = DirectMessagePolicy::Allow;
        config.channels.clear();
        let router = ChannelRouter::new(config);
        let mut message = inbound("plain dm broadcast request");
        message.channel = "teams".to_owned();
        message.is_direct_message = true;
        message.requested_broadcast = true;
        let outcome = router.begin_route(&message);
        assert!(matches!(
            outcome,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "broadcast_denied_by_policy"
        ));
    }

    #[test]
    fn sender_isolation_changes_session_key() {
        let mut config = baseline_config();
        config.channels[0].isolate_session_by_sender = true;
        let router = ChannelRouter::new(config);
        let message = inbound("@palyra route by sender");
        let outcome = router.begin_route(&message);
        let RouteOutcome::Routed(routed) = outcome else {
            panic!("sender-isolated route should be accepted");
        };
        assert_eq!(routed.plan.session_key, "channel:slack:conversation:C01TEAM:sender:U123");
        routed.lease.release();
    }

    #[test]
    fn backpressure_queues_and_poison_quarantines_when_limit_exceeds_queue_budget() {
        let mut config = baseline_config();
        config.max_retry_queue_depth_per_channel = 1;
        let router = ChannelRouter::new(config);
        let first =
            router.begin_route(&inbound_with_id("01ARZ3NDEKTSV4RRFFQ69G5FAC", "@palyra first"));
        let RouteOutcome::Routed(first_routed) = first else {
            panic!("first route should acquire sole concurrency slot");
        };

        let second =
            router.begin_route(&inbound_with_id("01ARZ3NDEKTSV4RRFFQ69G5FAD", "@palyra second"));
        assert!(matches!(second, RouteOutcome::Queued(_)));
        assert_eq!(router.channel_queue_depth("slack"), 1);

        let third =
            router.begin_route(&inbound_with_id("01ARZ3NDEKTSV4RRFFQ69G5FAE", "@palyra third"));
        assert!(matches!(
            third,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "backpressure_poison_quarantine" && rejection.quarantined
        ));
        assert_eq!(router.quarantine_len("slack"), 1);
        first_routed.lease.release();
    }

    #[test]
    fn repeated_processing_failure_transitions_from_retry_to_quarantine() {
        let router = ChannelRouter::new(baseline_config());
        let message = inbound("@palyra retry me");

        let queued = router.record_processing_failure(&message, "provider_error");
        assert!(matches!(queued, RetryDisposition::Queued));
        assert_eq!(router.channel_queue_depth("slack"), 1);

        let exhausted = InboundMessage { retry_attempt: 2, ..message };
        let disposition = router.record_processing_failure(&exhausted, "provider_error");
        assert!(matches!(disposition, RetryDisposition::Quarantined));
        assert_eq!(router.quarantine_len("slack"), 1);
    }

    #[test]
    fn retry_queue_entry_is_drained_when_same_message_routes_again() {
        let router = ChannelRouter::new(baseline_config());
        let message = inbound("@palyra retry me");
        let queued = router.record_processing_failure(&message, "provider_error");
        assert!(matches!(queued, RetryDisposition::Queued));
        assert_eq!(router.channel_queue_depth("slack"), 1);

        let outcome = router.begin_route(&message);
        let RouteOutcome::Routed(routed) = outcome else {
            panic!("retried message should route");
        };
        assert_eq!(
            router.channel_queue_depth("slack"),
            0,
            "retry queue must be drained when message is retried"
        );
        routed.lease.release();
    }

    #[test]
    fn processing_failure_quarantines_when_retry_queue_is_full() {
        let mut config = baseline_config();
        config.max_retry_queue_depth_per_channel = 1;
        let router = ChannelRouter::new(config);
        let first = inbound_with_id("01ARZ3NDEKTSV4RRFFQ69G5FAA", "@palyra retry one");
        let second = inbound_with_id("01ARZ3NDEKTSV4RRFFQ69G5FAB", "@palyra retry two");

        let first_result = router.record_processing_failure(&first, "provider_error");
        assert!(matches!(first_result, RetryDisposition::Queued));
        assert_eq!(router.channel_queue_depth("slack"), 1);

        let second_result = router.record_processing_failure(&second, "provider_error");
        assert!(matches!(second_result, RetryDisposition::Quarantined));
        assert_eq!(
            router.channel_queue_depth("slack"),
            1,
            "queue depth should stay capped when additional retry cannot be enqueued"
        );
        assert_eq!(router.quarantine_len("slack"), 1);
    }

    #[test]
    fn processing_failure_is_dropped_when_exhausted_retry_cannot_be_quarantined() {
        let router = ChannelRouter::new(baseline_config());
        let mut message = inbound("@palyra retry dropped");
        message.channel = "   ".to_owned();
        message.retry_attempt = 2;

        let disposition = router.record_processing_failure(&message, "provider_error");
        assert!(matches!(disposition, RetryDisposition::Dropped));
        assert_eq!(router.queue_depth(), 0);
    }

    #[test]
    fn processing_failure_is_dropped_when_retry_enqueue_and_quarantine_both_fail() {
        let mut config = baseline_config();
        config.max_retry_queue_depth_per_channel = 0;
        let router = ChannelRouter::new(config);
        let mut message = inbound("@palyra retry dropped");
        message.channel = "   ".to_owned();

        let disposition = router.record_processing_failure(&message, "provider_error");
        assert!(matches!(disposition, RetryDisposition::Dropped));
        assert_eq!(router.queue_depth(), 0);
    }

    #[test]
    fn dm_pairing_policy_rejects_unpaired_sender() {
        let mut config = baseline_config();
        config.channels[0].allow_direct_messages = true;
        config.channels[0].direct_message_policy = DirectMessagePolicy::Pairing;
        let router = ChannelRouter::new(config);

        let mut message = inbound("dm hello");
        message.is_direct_message = true;
        let outcome = router.begin_route(&message);
        assert!(matches!(
            outcome,
            RouteOutcome::Rejected(ref rejection)
                if rejection.reason == "direct_message_pairing_required"
        ));
    }

    #[test]
    fn dm_pairing_code_flow_grants_access_after_approval() {
        let mut config = baseline_config();
        config.channels[0].allow_direct_messages = true;
        config.channels[0].direct_message_policy = DirectMessagePolicy::Pairing;
        let router = ChannelRouter::new(config);

        let code = router
            .mint_pairing_code("slack", "admin:ops", Some(60_000))
            .expect("pairing code mint should succeed");
        let consume = router.consume_pairing_code("slack", Some("U123"), code.code.as_str(), None);
        let PairingConsumeOutcome::Pending(pending) = consume else {
            panic!("valid pairing code should create pending approval request");
        };
        assert_eq!(pending.sender_identity, "u123");

        let approval_id = "01ARZ3NDEKTSV4RRFFQ69G5FB0";
        let pending = router
            .attach_pairing_pending_approval("slack", "u123", approval_id)
            .expect("pending pairing should attach approval id");
        assert_eq!(pending.approval_id.as_deref(), Some(approval_id));

        let approval = router.apply_pairing_approval(approval_id, true, Some(120_000));
        let PairingApprovalOutcome::Approved(grant) = approval else {
            panic!("approval allow should create active pairing grant");
        };
        assert_eq!(grant.sender_identity, "u123");

        let mut message = inbound("dm hello after pairing");
        message.is_direct_message = true;
        let outcome = router.begin_route(&message);
        assert!(
            matches!(outcome, RouteOutcome::Routed(_)),
            "paired sender should pass DM policy checks"
        );
    }

    #[test]
    fn preview_route_reports_rejection_reason_and_hash() {
        let router = ChannelRouter::new(baseline_config());
        let preview = router.preview_route(&inbound("hello team"));
        assert!(!preview.accepted);
        assert_eq!(preview.reason, "no_matching_mention_or_dm_policy");
        assert_eq!(preview.config_hash.len(), 64);
    }
}
