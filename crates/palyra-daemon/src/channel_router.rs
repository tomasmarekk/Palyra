use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

const DEFAULT_RETRY_BACKOFF_MS: u64 = 250;
const MAX_PER_CHANNEL_QUARANTINE_ITEMS: usize = 256;
const FALLBACK_SENDER_COMPONENT: &str = "unknown";
const FALLBACK_CONVERSATION_COMPONENT: &str = "default";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRoutingRule {
    pub channel: String,
    pub enabled: bool,
    pub mention_patterns: Vec<String>,
    pub allow_from: Vec<String>,
    pub deny_from: Vec<String>,
    pub allow_direct_messages: bool,
    pub isolate_session_by_sender: bool,
    pub response_prefix: Option<String>,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    pub broadcast_strategy: BroadcastStrategy,
    pub concurrency_limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRouterConfig {
    pub enabled: bool,
    pub max_message_bytes: usize,
    pub max_retry_queue_depth_per_channel: usize,
    pub max_retry_attempts: u32,
    pub retry_backoff_ms: u64,
    pub default_response_prefix: Option<String>,
    pub default_channel_enabled: bool,
    pub default_allow_direct_messages: bool,
    pub default_isolate_session_by_sender: bool,
    pub default_broadcast_strategy: BroadcastStrategy,
    pub default_concurrency_limit: usize,
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
            default_isolate_session_by_sender: false,
            default_broadcast_strategy: BroadcastStrategy::Deny,
            default_concurrency_limit: 2,
            channels: Vec::new(),
        }
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
}

#[derive(Debug, Default)]
struct ChannelRuntimeState {
    in_flight: usize,
    retry_queue: VecDeque<RetryQueueEntry>,
    quarantine: VecDeque<QuarantinedMessage>,
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
        let rule = self.resolve_rule(channel.as_str());
        if !rule.enabled {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "channel_disabled".to_owned(),
                quarantined: false,
            });
        }
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

        let sender_identity = sender_identity(message);
        let normalized_sender = sender_identity.as_deref().map(normalize_identifier_match);
        if !rule.deny_from.is_empty()
            && normalized_sender.as_deref().is_some_and(|sender| {
                rule.deny_from.iter().any(|blocked| normalize_identifier_match(blocked) == sender)
            })
        {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "sender_denied".to_owned(),
                quarantined: false,
            });
        }
        if !rule.allow_from.is_empty() {
            if !message.sender_verified {
                return RouteOutcome::Rejected(RouteRejection {
                    reason: "sender_unverified_for_allowlist".to_owned(),
                    quarantined: false,
                });
            }
            let Some(sender) = normalized_sender.as_deref() else {
                return RouteOutcome::Rejected(RouteRejection {
                    reason: "sender_missing_for_allowlist".to_owned(),
                    quarantined: false,
                });
            };
            if !rule.allow_from.iter().any(|value| normalize_identifier_match(value) == sender) {
                return RouteOutcome::Rejected(RouteRejection {
                    reason: "sender_not_allowlisted".to_owned(),
                    quarantined: false,
                });
            }
        }

        let mention_match =
            has_mention_match(message.text.as_str(), rule.mention_patterns.as_slice());
        let direct_message_allowed = message.is_direct_message && rule.allow_direct_messages;
        if !mention_match && !direct_message_allowed {
            return RouteOutcome::Rejected(RouteRejection {
                reason: "no_matching_mention_or_dm_policy".to_owned(),
                quarantined: false,
            });
        }

        if message.requested_broadcast {
            match rule.broadcast_strategy {
                BroadcastStrategy::Deny => {
                    return RouteOutcome::Rejected(RouteRejection {
                        reason: "broadcast_denied_by_policy".to_owned(),
                        quarantined: false,
                    });
                }
                BroadcastStrategy::MentionOnly if !mention_match => {
                    return RouteOutcome::Rejected(RouteRejection {
                        reason: "broadcast_requires_mention_match".to_owned(),
                        quarantined: false,
                    });
                }
                BroadcastStrategy::MentionOnly | BroadcastStrategy::Allow => {}
            }
        }

        let concurrency_limit =
            rule.concurrency_limit.unwrap_or(self.config.default_concurrency_limit).max(1);
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
        let session_label =
            normalize_non_empty(message.conversation_id.as_deref().unwrap_or_default());

        RouteOutcome::Routed(Box::new(RoutedMessage {
            plan: RoutePlan {
                channel,
                route_key,
                session_key,
                session_label,
                sender_identity,
                is_broadcast: message.requested_broadcast,
                response_prefix: rule
                    .response_prefix
                    .clone()
                    .or_else(|| self.config.default_response_prefix.clone()),
                auto_ack_text: rule.auto_ack_text.clone(),
                auto_reaction: rule.auto_reaction.clone(),
                in_reply_to_message_id: normalize_non_empty(
                    message.adapter_message_id.as_deref().unwrap_or_default(),
                ),
                reply_thread_id: normalize_non_empty(
                    message.adapter_thread_id.as_deref().unwrap_or_default(),
                ),
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
            let _ = self.quarantine_message(
                message,
                format!("retry_exhausted:{reason}").as_str(),
                message.retry_attempt,
            );
            return RetryDisposition::Quarantined;
        }
        if self.enqueue_retry(message, message.channel.as_str(), reason.to_owned()) {
            RetryDisposition::Queued
        } else {
            let _ = self.quarantine_message(
                message,
                format!("retry_enqueue_failed:{reason}").as_str(),
                message.retry_attempt,
            );
            RetryDisposition::Quarantined
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
                isolate_session_by_sender: self.config.default_isolate_session_by_sender,
                response_prefix: None,
                auto_ack_text: None,
                auto_reaction: None,
                broadcast_strategy: self.config.default_broadcast_strategy,
                concurrency_limit: Some(self.config.default_concurrency_limit),
            })
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
    let normalized_text = text.to_ascii_lowercase();
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

fn contains_boundary_delimited_pattern(text: &str, pattern: &str) -> bool {
    text.match_indices(pattern).any(|(start, _)| {
        let left_boundary =
            start == 0 || !text.as_bytes()[start.saturating_sub(1)].is_ascii_alphanumeric();
        let end = start.saturating_add(pattern.len());
        let right_boundary = end >= text.len() || !text.as_bytes()[end].is_ascii_alphanumeric();
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
    let mut normalized = String::with_capacity(value.len());
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    let collapsed = normalized.trim_matches('_');
    if collapsed.is_empty() {
        "default".to_owned()
    } else {
        collapsed.to_owned()
    }
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        BroadcastStrategy, ChannelRouter, ChannelRouterConfig, ChannelRoutingRule, InboundMessage,
        RetryDisposition, RouteOutcome,
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
            default_isolate_session_by_sender: false,
            default_broadcast_strategy: BroadcastStrategy::Deny,
            default_concurrency_limit: 2,
            channels: vec![ChannelRoutingRule {
                channel: "slack".to_owned(),
                enabled: true,
                mention_patterns: vec!["@palyra".to_owned()],
                allow_from: vec![],
                deny_from: vec![],
                allow_direct_messages: false,
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
        assert_eq!(routed.plan.route_key, "channel:slack:conversation:c01team");
        assert_eq!(routed.plan.session_key, "channel:slack:conversation:c01team");
        assert_eq!(routed.plan.response_prefix.as_deref(), Some("[bot] "));
        routed.lease.release();
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
    fn direct_message_policy_allows_messages_without_mentions() {
        let mut config = baseline_config();
        config.channels[0].allow_direct_messages = true;
        let router = ChannelRouter::new(config);
        let mut message = inbound("plain dm question");
        message.is_direct_message = true;
        let outcome = router.begin_route(&message);
        assert!(matches!(outcome, RouteOutcome::Routed(_)));
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
        assert_eq!(routed.plan.session_key, "channel:slack:conversation:c01team:sender:u123");
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
}
