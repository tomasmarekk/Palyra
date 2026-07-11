//! Inbound/outbound message envelopes, routing results, and delivery receipts.
//!
//! `envelope_id` is the unit of idempotency end to end: inbound dedupe, outbox
//! uniqueness, and delivery receipts all key on `connector_id` + `envelope_id`,
//! so identifiers here must stay stable across retries.

use serde::{Deserialize, Serialize};

use super::{
    attachments::{validate_attachments, OutboundA2uiUpdate, OutboundAttachment},
    capabilities::ConnectorCapabilitySet,
    kinds::{ConnectorAvailability, ConnectorKind, ConnectorLiveness, ConnectorReadiness},
    validation::{
        validate_host_pattern, validate_json_bytes, validate_message_body,
        validate_non_empty_identifier, ProtocolError, MAX_CONNECTOR_ID_BYTES,
        MAX_CONNECTOR_PRINCIPAL_BYTES, MAX_CONVERSATION_ID_BYTES, MAX_ENVELOPE_ID_BYTES,
        MAX_IDENTITY_BYTES, MAX_STRUCTURED_OUTPUT_BYTES,
    },
};

/// Desired configuration of one connector instance as registered with the
/// supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorInstanceSpec {
    pub connector_id: String,
    pub kind: ConnectorKind,
    /// Channel principal the instance acts as (access-control subject).
    pub principal: String,
    pub auth_profile_ref: Option<String>,
    pub token_vault_ref: Option<String>,
    /// Hosts the instance may dial; enforced by `core::net::ConnectorNetGuard`.
    pub egress_allowlist: Vec<String>,
    pub enabled: bool,
}

impl ConnectorInstanceSpec {
    /// Validates identifiers and every egress allowlist host pattern.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.principal.as_str(),
            "principal",
            MAX_CONNECTOR_PRINCIPAL_BYTES,
        )?;
        for host in &self.egress_allowlist {
            validate_host_pattern(host)?;
        }
        Ok(())
    }
}

/// Normalized inbound message as produced by a provider adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundMessageEvent {
    /// Stable per-message identifier; reused on provider redelivery so the
    /// supervisor's dedupe window can drop duplicates.
    pub envelope_id: String,
    pub connector_id: String,
    pub conversation_id: String,
    pub thread_id: Option<String>,
    pub sender_id: String,
    pub sender_display: Option<String>,
    pub body: String,
    pub adapter_message_id: Option<String>,
    pub adapter_thread_id: Option<String>,
    pub received_at_unix_ms: i64,
    pub is_direct_message: bool,
    pub requested_broadcast: bool,
    #[serde(default)]
    pub attachments: Vec<super::AttachmentRef>,
}

impl InboundMessageEvent {
    /// Validates identifiers, the body against `max_body_bytes`, and attachments.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self, max_body_bytes: usize) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.envelope_id.as_str(),
            "envelope_id",
            MAX_ENVELOPE_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.conversation_id.as_str(),
            "conversation_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        validate_non_empty_identifier(self.sender_id.as_str(), "sender_id", MAX_IDENTITY_BYTES)?;
        validate_message_body(self.body.as_str(), max_body_bytes, "body")?;
        validate_attachments(self.attachments.as_slice())?;
        Ok(())
    }
}

/// One outbound message produced by the router for an inbound event.
///
/// The supervisor turns each output into an [`OutboundMessageRequest`] with a
/// derived envelope id before enqueueing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedOutboundMessage {
    pub text: String,
    pub thread_id: Option<String>,
    pub in_reply_to_message_id: Option<String>,
    pub broadcast: bool,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    #[serde(default)]
    pub attachments: Vec<OutboundAttachment>,
    #[serde(default)]
    pub structured_json: Option<Vec<u8>>,
    #[serde(default)]
    pub a2ui_update: Option<OutboundA2uiUpdate>,
}

/// Router verdict for one inbound event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInboundResult {
    pub accepted: bool,
    /// True when the router queued the event for its own retry; the connector
    /// does not re-ingest it (the dedupe window would drop it anyway).
    pub queued_for_retry: bool,
    pub decision_reason: String,
    pub outputs: Vec<RoutedOutboundMessage>,
    pub route_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub retry_attempt: u32,
    #[serde(default)]
    pub route_message_latency_ms: Option<u64>,
}

/// Outbound message persisted in the outbox and handed to a provider adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundMessageRequest {
    /// Idempotency identifier; the outbox enforces uniqueness per
    /// `(connector_id, envelope_id)`, so retries must reuse the same value.
    pub envelope_id: String,
    pub connector_id: String,
    pub conversation_id: String,
    pub reply_thread_id: Option<String>,
    pub in_reply_to_message_id: Option<String>,
    pub text: String,
    pub broadcast: bool,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    #[serde(default)]
    pub attachments: Vec<OutboundAttachment>,
    #[serde(default)]
    pub structured_json: Option<Vec<u8>>,
    #[serde(default)]
    pub a2ui_update: Option<OutboundA2uiUpdate>,
    pub timeout_ms: u64,
    /// Upper bound for structured/A2UI payload bytes; further clamped by the
    /// supervisor's configured outbound limit during validation.
    pub max_payload_bytes: usize,
}

impl OutboundMessageRequest {
    /// Returns the receipt idempotency key (`connector_id:envelope_id`).
    #[must_use]
    pub fn delivery_idempotency_key(&self) -> String {
        format!("{}:{}", self.connector_id, self.envelope_id)
    }

    /// Validates identifiers, text size, attachments, timeout, and payload limits.
    ///
    /// # Errors
    /// Returns a protocol error naming the first invalid field.
    pub fn validate(&self, max_text_bytes: usize) -> Result<(), ProtocolError> {
        validate_non_empty_identifier(
            self.envelope_id.as_str(),
            "envelope_id",
            MAX_ENVELOPE_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.connector_id.as_str(),
            "connector_id",
            MAX_CONNECTOR_ID_BYTES,
        )?;
        validate_non_empty_identifier(
            self.conversation_id.as_str(),
            "conversation_id",
            MAX_CONVERSATION_ID_BYTES,
        )?;
        validate_message_body(self.text.as_str(), max_text_bytes, "text")?;
        validate_attachments(self.attachments.as_slice())?;
        if self.timeout_ms == 0 {
            return Err(ProtocolError::InvalidField {
                field: "timeout_ms",
                reason: "must be greater than zero",
            });
        }
        if self.max_payload_bytes == 0 {
            return Err(ProtocolError::InvalidField {
                field: "max_payload_bytes",
                reason: "must be greater than zero",
            });
        }
        let max_payload_bytes = self.max_payload_bytes.min(max_text_bytes);
        if let Some(structured_json) = self.structured_json.as_deref() {
            validate_json_bytes(
                structured_json,
                "structured_json",
                max_payload_bytes.min(MAX_STRUCTURED_OUTPUT_BYTES),
            )?;
        }
        if let Some(update) = self.a2ui_update.as_ref() {
            update.validate(max_payload_bytes)?;
        }
        Ok(())
    }
}

/// Why an adapter asked for a delivery retry; drives backoff and runtime-state
/// bookkeeping (`ConnectorRestarting` increments the restart counter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    RateLimit,
    TransientNetwork,
    ConnectorRestarting,
}

impl RetryClass {
    /// Returns the stable snake_case label matching the serde encoding.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate_limit",
            Self::TransientNetwork => "transient_network",
            Self::ConnectorRestarting => "connector_restarting",
        }
    }
}

/// Adapter verdict for one delivery attempt.
///
/// `Retry` is reserved for attempts where the adapter can prove that the
/// platform performed no externally visible effect. Once a request may have
/// reached the platform, an unconfirmed result must be `OutcomeUnknown` so the
/// durable outbox requires explicit reconciliation instead of sending again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryOutcome {
    /// The platform acknowledged delivery with its stable message identifier.
    Delivered { native_message_id: String },
    /// No platform effect occurred, so repeating the request is safe.
    Retry { class: RetryClass, reason: String, retry_after_ms: Option<u64> },
    /// The request may have taken effect and must not be repeated blindly.
    OutcomeUnknown { reason: String },
    /// The platform definitively rejected the request without delivering it.
    PermanentFailure { reason: String },
}

impl DeliveryOutcome {
    /// Converts this outcome into the receipt for `request`.
    ///
    /// See [`DeliveryReceipt::from_outcome`] for the state mapping.
    #[must_use]
    pub fn to_receipt(&self, request: &OutboundMessageRequest) -> DeliveryReceipt {
        DeliveryReceipt::from_outcome(request, self)
    }
}

/// Final disposition of a delivery attempt as reported upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryReceiptState {
    Ack,
    Nack,
    Unknown,
}

/// Receipt emitted for a delivery attempt, keyed for idempotent consumption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub state: DeliveryReceiptState,
    pub idempotency_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl DeliveryReceipt {
    /// Builds the receipt for an outcome: `Delivered` maps to `Ack`,
    /// `PermanentFailure` to `Nack`, and both non-terminal outcomes to
    /// `Unknown`. `Retry` is safe to repeat later; `OutcomeUnknown` requires
    /// reconciliation before any repeat is allowed.
    #[must_use]
    pub fn from_outcome(request: &OutboundMessageRequest, outcome: &DeliveryOutcome) -> Self {
        match outcome {
            DeliveryOutcome::Delivered { native_message_id } => Self {
                state: DeliveryReceiptState::Ack,
                idempotency_key: request.delivery_idempotency_key(),
                external_message_id: Some(native_message_id.clone()),
                retry_after_ms: None,
                reason: None,
            },
            DeliveryOutcome::Retry { class, reason, retry_after_ms } => Self {
                state: DeliveryReceiptState::Unknown,
                idempotency_key: request.delivery_idempotency_key(),
                external_message_id: None,
                retry_after_ms: *retry_after_ms,
                reason: Some(format!("{}: {reason}", class.as_str())),
            },
            DeliveryOutcome::OutcomeUnknown { reason } => Self {
                state: DeliveryReceiptState::Unknown,
                idempotency_key: request.delivery_idempotency_key(),
                external_message_id: None,
                retry_after_ms: None,
                reason: Some(reason.clone()),
            },
            DeliveryOutcome::PermanentFailure { reason } => Self {
                state: DeliveryReceiptState::Nack,
                idempotency_key: request.delivery_idempotency_key(),
                external_message_id: None,
                retry_after_ms: None,
                reason: Some(reason.clone()),
            },
        }
    }
}

/// Compact queue totals surfaced in status snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorQueueDepth {
    pub pending_outbox: u64,
    pub dead_letters: u64,
}

/// Point-in-time operator view of one connector instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorStatusSnapshot {
    pub connector_id: String,
    pub kind: ConnectorKind,
    pub availability: ConnectorAvailability,
    pub capabilities: ConnectorCapabilitySet,
    pub principal: String,
    pub enabled: bool,
    pub readiness: ConnectorReadiness,
    pub liveness: ConnectorLiveness,
    pub restart_count: u32,
    pub queue_depth: ConnectorQueueDepth,
    pub last_error: Option<String>,
    pub last_inbound_unix_ms: Option<i64>,
    pub last_outbound_unix_ms: Option<i64>,
    pub updated_at_unix_ms: i64,
}
