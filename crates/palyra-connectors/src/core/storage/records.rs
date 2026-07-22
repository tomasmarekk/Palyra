//! Typed row representations of connector storage tables and their parsers.
//!
//! The `parse_*_row` helpers expect column order to match the SELECT lists in
//! the sibling query modules; keep both sides in sync when columns change.

use palyra_common::runtime_contracts::{
    RuntimeErrorClass, RuntimeErrorEnvelopeV1, RuntimeErrorEnvelopeV1Input, RuntimeErrorPhase,
    RuntimeErrorSecurityClass, RuntimeErrorUserVisibility, RuntimeErrorValidationError,
    RuntimeRetryability, RuntimeSubsystem,
};
use rusqlite::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::protocol::{
    ConnectorKind, ConnectorLiveness, ConnectorQueueDepth, ConnectorReadiness, InboundMessageEvent,
    OutboundMessageRequest,
};
use super::ConnectorStoreError;

/// Persisted state of one connector instance (spec plus runtime bookkeeping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInstanceRecord {
    pub connector_id: String,
    pub kind: ConnectorKind,
    pub principal: String,
    pub auth_profile_ref: Option<String>,
    pub token_vault_ref: Option<String>,
    pub egress_allowlist: Vec<String>,
    pub enabled: bool,
    pub readiness: ConnectorReadiness,
    pub liveness: ConnectorLiveness,
    pub restart_count: u32,
    pub last_error: Option<String>,
    pub last_inbound_unix_ms: Option<i64>,
    pub last_outbound_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Durable side-effect fence for one outbox delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxEffectState {
    /// No external effect has started; an expired claim is safe to reclaim.
    Ready,
    /// The adapter call may be in flight; claim expiry must park the row unknown.
    EffectStarted,
    /// The platform outcome is uncertain and requires explicit reconciliation.
    OutcomeUnknown,
}

impl OutboxEffectState {
    /// Returns the stable storage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::EffectStarted => "effect_started",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, ConnectorStoreError> {
        match value {
            "ready" => Ok(Self::Ready),
            "effect_started" => Ok(Self::EffectStarted),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            other => Err(ConnectorStoreError::UnknownOutboxEffectState(other.to_owned())),
        }
    }
}

/// One claimed outbox entry handed to the supervisor for delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntryRecord {
    pub outbox_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    /// Lease token proving claim ownership; every status mutation must present
    /// it so an expired-and-reclaimed entry cannot be completed twice.
    pub claim_token: String,
    pub payload: OutboundMessageRequest,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_attempt_unix_ms: i64,
    pub effect_state: OutboxEffectState,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Whether an enqueue inserted a new entry (`false` means the envelope was
/// already present and the call was an idempotent no-op).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEnqueueOutcome {
    pub created: bool,
}

/// Payload-free current state of one deterministic outbox envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxDeliverySnapshot {
    pub connector_id: String,
    pub envelope_id: String,
    pub status: String,
    pub effect_state: OutboxEffectState,
    pub native_message_id: Option<String>,
}

/// Operator-safe view of an outbox row whose platform outcome is uncertain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxUnknownRecord {
    pub outbox_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    pub attempts: u32,
    pub last_reason_code: Option<String>,
    pub updated_at_unix_ms: i64,
}

/// Evidence supplied by a reconciler for an outcome-unknown outbox row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum OutboxReconciliationEvidence {
    /// The platform proves delivery and supplies its stable message identifier.
    Delivered { native_message_id: String },
    /// The platform proves no effect occurred, so a later claim may send safely.
    ConfirmedAbsent,
}

/// Durable result of applying explicit outbox reconciliation evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxReconciliationOutcome {
    pub outbox_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    pub effect_state: OutboxEffectState,
    pub delivered: bool,
    pub requeued: bool,
}

/// Durable processing state for one inbound channel event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelIngressStatus {
    /// Persisted and ready to be claimed by a worker.
    Pending,
    /// Claimed under a lease by a worker.
    Claimed,
    /// Waiting for a retry deadline after a transient route failure.
    Retrying,
    /// Routed and fully materialized into delivery intents/outbox rows.
    Completed,
    /// Retry budget exhausted without a successful route.
    Failed,
    /// Terminal validation or poison-message state that requires operator review.
    Quarantined,
}

impl ChannelIngressStatus {
    /// Returns the stable storage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Retrying => "retrying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Quarantined => "quarantined",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ConnectorStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "retrying" => Ok(Self::Retrying),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(ConnectorStoreError::UnknownIngressStatus(other.to_owned())),
        }
    }
}

/// Stored ingress row with the original validated event payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelIngressRecord {
    pub ingress_event_id: i64,
    pub connector_id: String,
    pub principal: String,
    pub conversation_id: String,
    pub envelope_id: String,
    pub payload_hash: String,
    pub payload: InboundMessageEvent,
    pub status: ChannelIngressStatus,
    pub lane_key: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_attempt_unix_ms: i64,
    pub claim_token: Option<String>,
    pub claim_expires_unix_ms: i64,
    pub last_error_reason_code: Option<String>,
    pub last_error_message: Option<String>,
    pub route_key: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub completed_at_unix_ms: Option<i64>,
    pub tombstone_expires_at_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Result of idempotently persisting an inbound event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelIngressEnqueueOutcome {
    pub created: bool,
    pub record: ChannelIngressRecord,
}

/// Head-of-line lane blocked by pending, claimed, or retrying ingress work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressBlockedLaneSnapshot {
    pub lane_key: String,
    pub head_ingress_event_id: i64,
    pub status: ChannelIngressStatus,
    pub attempts: u32,
    pub next_attempt_unix_ms: i64,
    pub claim_expires_unix_ms: i64,
}

/// Durable delivery-intent state layered over outbox entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryIntentStatus {
    /// Intent record was created before planning completed.
    Created,
    /// Intent was planned but not yet queued to an adapter outbox.
    Planned,
    /// Intent has a queued outbox row and is safe to dispatch.
    Queued,
    /// Adapter send has started and the platform outcome is not final yet.
    AdapterSendStarted,
    /// Adapter outcome is unknown; reconciliation or a proven idempotency guard is required.
    PlatformOutcomeUnknown,
    /// Platform acknowledged delivery.
    Delivered,
    /// Router output was intentionally suppressed and no send is pending.
    Suppressed,
    /// Intent failed before it could reach a platform adapter.
    Failed,
    /// Intent is parked in the operator dead-letter queue.
    DeadLettered,
}

impl DeliveryIntentStatus {
    /// Returns the stable storage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Planned => "planned",
            Self::Queued => "queued",
            Self::AdapterSendStarted => "adapter_send_started",
            Self::PlatformOutcomeUnknown => "platform_outcome_unknown",
            Self::Delivered => "delivered",
            Self::Suppressed => "suppressed",
            Self::Failed => "failed",
            Self::DeadLettered => "dead_lettered",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ConnectorStoreError> {
        match value {
            "created" => Ok(Self::Created),
            "planned" => Ok(Self::Planned),
            "queued" => Ok(Self::Queued),
            "adapter_send_started" => Ok(Self::AdapterSendStarted),
            "platform_outcome_unknown" => Ok(Self::PlatformOutcomeUnknown),
            "delivered" => Ok(Self::Delivered),
            "suppressed" => Ok(Self::Suppressed),
            "failed" => Ok(Self::Failed),
            "dead_lettered" => Ok(Self::DeadLettered),
            other => Err(ConnectorStoreError::UnknownDeliveryIntentStatus(other.to_owned())),
        }
    }
}

/// Request for creating or re-reading a delivery intent.
#[derive(Debug, Clone)]
pub struct DeliveryIntentDraft {
    pub intent_id: String,
    pub connector_id: String,
    pub ingress_event_id: i64,
    pub ingress_envelope_id: String,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub principal: String,
    pub conversation_id: String,
    pub outbox_envelope_id: String,
    pub output_index: u32,
    pub payload_hash: String,
    pub visible_text_preview: String,
    pub status: DeliveryIntentStatus,
    pub redaction_summary_json: Option<String>,
}

/// Operator-safe delivery-intent report; raw outbound payloads are never included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryIntentRecord {
    pub intent_id: String,
    pub connector_id: String,
    pub ingress_event_id: i64,
    pub ingress_envelope_id: String,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub principal: String,
    pub conversation_id: String,
    pub outbox_envelope_id: String,
    pub output_index: u32,
    pub payload_hash: String,
    pub visible_text_preview: String,
    pub status: DeliveryIntentStatus,
    pub send_attempts: u32,
    pub native_message_id: Option<String>,
    pub last_reason_code: Option<String>,
    pub redaction_summary_json: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl DeliveryIntentRecord {
    /// Projects a failed or outcome-unknown durable delivery intent into runtime error metadata.
    ///
    /// Active, delivered, and intentionally suppressed intents return `Ok(None)`. The projection
    /// never copies outbound text, connector responses, or arbitrary failure detail. A send attempt
    /// without a final acknowledgement is treated as side-effect uncertain and therefore requires
    /// the durable idempotency guard before retry.
    ///
    /// # Errors
    /// Returns [`RuntimeErrorValidationError`] if a repository-owned projection constant violates
    /// the shared contract. Persisted adapter failure text is never reused as a reason code.
    pub fn runtime_error_envelope(
        &self,
        output_emitted: bool,
    ) -> Result<Option<RuntimeErrorEnvelopeV1>, RuntimeErrorValidationError> {
        let projection = match self.status {
            DeliveryIntentStatus::PlatformOutcomeUnknown => Some((
                RuntimeErrorClass::DeliveryUnknown,
                RuntimeErrorPhase::DeliveryAcknowledgement,
                RuntimeRetryability::RequiresIdempotencyGuard,
                true,
                RuntimeErrorUserVisibility::ActionRequired,
                "delivery platform outcome is unknown",
                "reconcile the durable delivery intent before an idempotency-guarded retry",
                "delivery.intent.platform_outcome_unknown",
            )),
            DeliveryIntentStatus::Failed | DeliveryIntentStatus::DeadLettered => {
                let side_effect_may_have_occurred = self.send_attempts > 0;
                Some((
                    RuntimeErrorClass::RecoveryBlocked,
                    RuntimeErrorPhase::DeliveryAcknowledgement,
                    if side_effect_may_have_occurred {
                        RuntimeRetryability::RequiresIdempotencyGuard
                    } else {
                        RuntimeRetryability::RequiresOperatorReview
                    },
                    side_effect_may_have_occurred,
                    RuntimeErrorUserVisibility::ActionRequired,
                    "delivery intent reached a terminal failure state",
                    "inspect the durable intent and retry only through the operator delivery workflow",
                    if self.status == DeliveryIntentStatus::DeadLettered {
                        "delivery.intent.dead_lettered"
                    } else {
                        "delivery.intent.failed"
                    },
                ))
            }
            DeliveryIntentStatus::Created
            | DeliveryIntentStatus::Planned
            | DeliveryIntentStatus::Queued
            | DeliveryIntentStatus::AdapterSendStarted
            | DeliveryIntentStatus::Delivered
            | DeliveryIntentStatus::Suppressed => None,
        };
        let Some((
            class,
            phase,
            retryability,
            side_effect_may_have_occurred,
            user_visibility,
            safe_message,
            recovery_hint,
            fallback_reason_code,
        )) = projection
        else {
            return Ok(None);
        };

        RuntimeErrorEnvelopeV1::try_new(RuntimeErrorEnvelopeV1Input {
            class,
            reason_code: fallback_reason_code.to_owned(),
            subsystem: RuntimeSubsystem::Delivery,
            phase,
            retryability,
            security_class: RuntimeErrorSecurityClass::Internal,
            user_visibility,
            output_emitted,
            side_effect_may_have_occurred,
            safe_message: safe_message.to_owned(),
            recovery_hint: recovery_hint.to_owned(),
        })
        .map(Some)
    }
}

/// Result of operator retrying a delivery intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryIntentRetryOutcome {
    pub intent: DeliveryIntentRecord,
    pub requeued: bool,
}

/// Outbound message parked after permanent failure or retry exhaustion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub dead_letter_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    pub reason: String,
    /// Original outbox payload preserved verbatim for replay.
    pub payload: Value,
    pub created_at_unix_ms: i64,
}

/// Aggregated queue counters and pause state for one connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorQueueSnapshot {
    pub pending_ingress: u64,
    pub due_ingress: u64,
    pub claimed_ingress: u64,
    pub retrying_ingress: u64,
    pub failed_ingress: u64,
    pub quarantined_ingress: u64,
    pub blocked_ingress_lanes: Vec<IngressBlockedLaneSnapshot>,
    pub pending_outbox: u64,
    pub due_outbox: u64,
    pub claimed_outbox: u64,
    pub dead_letters: u64,
    pub next_attempt_unix_ms: Option<i64>,
    pub oldest_pending_created_at_unix_ms: Option<i64>,
    pub latest_dead_letter_unix_ms: Option<i64>,
    pub paused: bool,
    pub pause_reason: Option<String>,
    pub pause_updated_at_unix_ms: Option<i64>,
}

/// Operational event row used for connector logs and derived metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEventRecord {
    pub event_id: i64,
    pub connector_id: String,
    pub event_type: String,
    pub level: String,
    pub message: String,
    pub details: Option<Value>,
    pub created_at_unix_ms: i64,
}

pub(super) fn parse_instance_row(
    row: &Row<'_>,
) -> Result<ConnectorInstanceRecord, ConnectorStoreError> {
    let kind_value: String = row.get(1)?;
    let readiness_value: String = row.get(7)?;
    let liveness_value: String = row.get(8)?;
    let kind = ConnectorKind::parse(kind_value.as_str())
        .ok_or_else(|| ConnectorStoreError::UnknownConnectorKind(kind_value.clone()))?;
    let readiness = ConnectorReadiness::parse(readiness_value.as_str())
        .ok_or_else(|| ConnectorStoreError::UnknownReadiness(readiness_value.clone()))?;
    let liveness = ConnectorLiveness::parse(liveness_value.as_str())
        .ok_or_else(|| ConnectorStoreError::UnknownLiveness(liveness_value.clone()))?;
    let restart_count_i64: i64 = row.get(9)?;
    let restart_count = u32::try_from(restart_count_i64)
        .map_err(|_| ConnectorStoreError::ValueOverflow { field: "restart_count" })?;
    let allowlist_json: String = row.get(5)?;
    let egress_allowlist = serde_json::from_str::<Vec<String>>(allowlist_json.as_str())?;
    Ok(ConnectorInstanceRecord {
        connector_id: row.get(0)?,
        kind,
        principal: row.get(2)?,
        auth_profile_ref: row.get(3)?,
        token_vault_ref: row.get(4)?,
        egress_allowlist,
        enabled: row.get::<_, i64>(6)? != 0,
        readiness,
        liveness,
        restart_count,
        last_error: row.get(10)?,
        last_inbound_unix_ms: row.get(11)?,
        last_outbound_unix_ms: row.get(12)?,
        created_at_unix_ms: row.get(13)?,
        updated_at_unix_ms: row.get(14)?,
    })
}

pub(super) fn parse_outbox_row(row: &Row<'_>) -> Result<OutboxEntryRecord, ConnectorStoreError> {
    let payload_json: String = row.get(3)?;
    let payload = serde_json::from_str::<OutboundMessageRequest>(payload_json.as_str())?;
    let attempts_i64: i64 = row.get(4)?;
    let max_attempts_i64: i64 = row.get(5)?;
    let claim_token: String = row.get(7)?;
    Ok(OutboxEntryRecord {
        outbox_id: row.get(0)?,
        connector_id: row.get(1)?,
        envelope_id: row.get(2)?,
        claim_token,
        payload,
        attempts: u32::try_from(attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "attempts" })?,
        max_attempts: u32::try_from(max_attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "max_attempts" })?,
        next_attempt_unix_ms: row.get(6)?,
        effect_state: OutboxEffectState::parse(row.get::<_, String>(10)?.as_str())?,
        created_at_unix_ms: row.get(8)?,
        updated_at_unix_ms: row.get(9)?,
    })
}

pub(super) fn parse_channel_ingress_row(
    row: &Row<'_>,
) -> Result<ChannelIngressRecord, ConnectorStoreError> {
    let payload_json: String = row.get(6)?;
    let status_value: String = row.get(7)?;
    let attempts_i64: i64 = row.get(9)?;
    let max_attempts_i64: i64 = row.get(10)?;
    Ok(ChannelIngressRecord {
        ingress_event_id: row.get(0)?,
        connector_id: row.get(1)?,
        principal: row.get(2)?,
        conversation_id: row.get(3)?,
        envelope_id: row.get(4)?,
        payload_hash: row.get(5)?,
        payload: serde_json::from_str(payload_json.as_str())?,
        status: ChannelIngressStatus::parse(status_value.as_str())?,
        lane_key: row.get(8)?,
        attempts: u32::try_from(attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "ingress_attempts" })?,
        max_attempts: u32::try_from(max_attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "ingress_max_attempts" })?,
        next_attempt_unix_ms: row.get(11)?,
        claim_token: row.get(12)?,
        claim_expires_unix_ms: row.get(13)?,
        last_error_reason_code: row.get(14)?,
        last_error_message: row.get(15)?,
        route_key: row.get(16)?,
        session_id: row.get(17)?,
        run_id: row.get(18)?,
        completed_at_unix_ms: row.get(19)?,
        tombstone_expires_at_unix_ms: row.get(20)?,
        created_at_unix_ms: row.get(21)?,
        updated_at_unix_ms: row.get(22)?,
    })
}

pub(super) fn parse_blocked_lane_row(
    row: &Row<'_>,
) -> Result<IngressBlockedLaneSnapshot, ConnectorStoreError> {
    let status_value: String = row.get(2)?;
    let attempts_i64: i64 = row.get(3)?;
    Ok(IngressBlockedLaneSnapshot {
        lane_key: row.get(0)?,
        head_ingress_event_id: row.get(1)?,
        status: ChannelIngressStatus::parse(status_value.as_str())?,
        attempts: u32::try_from(attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "blocked_lane_attempts" })?,
        next_attempt_unix_ms: row.get(4)?,
        claim_expires_unix_ms: row.get(5)?,
    })
}

pub(super) fn parse_delivery_intent_row(
    row: &Row<'_>,
) -> Result<DeliveryIntentRecord, ConnectorStoreError> {
    let status_value: String = row.get(12)?;
    let send_attempts_i64: i64 = row.get(13)?;
    let output_index_i64: i64 = row.get(8)?;
    Ok(DeliveryIntentRecord {
        intent_id: row.get(0)?,
        connector_id: row.get(1)?,
        ingress_event_id: row.get(2)?,
        ingress_envelope_id: row.get(3)?,
        session_id: row.get(4)?,
        run_id: row.get(5)?,
        principal: row.get(6)?,
        conversation_id: row.get(7)?,
        output_index: u32::try_from(output_index_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "output_index" })?,
        outbox_envelope_id: row.get(9)?,
        payload_hash: row.get(10)?,
        visible_text_preview: row.get(11)?,
        status: DeliveryIntentStatus::parse(status_value.as_str())?,
        send_attempts: u32::try_from(send_attempts_i64)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "send_attempts" })?,
        native_message_id: row.get(14)?,
        last_reason_code: row.get(15)?,
        redaction_summary_json: row.get(16)?,
        created_at_unix_ms: row.get(17)?,
        updated_at_unix_ms: row.get(18)?,
    })
}

pub(super) fn parse_dead_letter_row(
    row: &Row<'_>,
) -> Result<DeadLetterRecord, ConnectorStoreError> {
    let payload_json: String = row.get(4)?;
    Ok(DeadLetterRecord {
        dead_letter_id: row.get(0)?,
        connector_id: row.get(1)?,
        envelope_id: row.get(2)?,
        reason: row.get(3)?,
        payload: serde_json::from_str(payload_json.as_str())?,
        created_at_unix_ms: row.get(5)?,
    })
}

pub(super) fn parse_event_row(row: &Row<'_>) -> Result<ConnectorEventRecord, ConnectorStoreError> {
    let details_json: Option<String> = row.get(5)?;
    Ok(ConnectorEventRecord {
        event_id: row.get(0)?,
        connector_id: row.get(1)?,
        event_type: row.get(2)?,
        level: row.get(3)?,
        message: row.get(4)?,
        details: details_json.map(|value| serde_json::from_str(value.as_str())).transpose()?,
        created_at_unix_ms: row.get(6)?,
    })
}

pub(super) fn to_queue_depth(snapshot: &ConnectorQueueSnapshot) -> ConnectorQueueDepth {
    ConnectorQueueDepth {
        pending_outbox: snapshot.pending_outbox,
        dead_letters: snapshot.dead_letters,
    }
}

#[cfg(test)]
mod runtime_error_tests {
    use super::*;

    fn intent(status: DeliveryIntentStatus) -> DeliveryIntentRecord {
        DeliveryIntentRecord {
            intent_id: "intent-1".to_owned(),
            connector_id: "connector-1".to_owned(),
            ingress_event_id: 1,
            ingress_envelope_id: "ingress-1".to_owned(),
            session_id: Some("session-1".to_owned()),
            run_id: Some("run-1".to_owned()),
            principal: "principal-1".to_owned(),
            conversation_id: "conversation-1".to_owned(),
            outbox_envelope_id: "outbox-1".to_owned(),
            output_index: 0,
            payload_hash: "sha256:payload".to_owned(),
            visible_text_preview: "must not enter runtime error metadata".to_owned(),
            status,
            send_attempts: 0,
            native_message_id: None,
            last_reason_code: None,
            redaction_summary_json: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        }
    }

    #[test]
    fn platform_outcome_unknown_requires_idempotency_guard() {
        let mut intent = intent(DeliveryIntentStatus::PlatformOutcomeUnknown);
        intent.send_attempts = 1;
        intent.last_reason_code = Some("connector timed out with bearer live-token".to_owned());
        let error = intent
            .runtime_error_envelope(false)
            .expect("delivery intent should project")
            .expect("unknown outcome should produce runtime error metadata");

        assert_eq!(error.class(), RuntimeErrorClass::DeliveryUnknown);
        assert_eq!(error.phase(), RuntimeErrorPhase::DeliveryAcknowledgement);
        assert_eq!(error.retryability(), RuntimeRetryability::RequiresIdempotencyGuard);
        assert!(error.side_effect_may_have_occurred());
        assert_eq!(error.reason_code(), "delivery.intent.platform_outcome_unknown");
        assert!(!serde_json::to_string(&error)
            .expect("runtime error should serialize")
            .contains("must not enter runtime error metadata"));
    }

    #[test]
    fn known_delivery_failure_before_send_requires_operator_review() {
        let error = intent(DeliveryIntentStatus::Failed)
            .runtime_error_envelope(false)
            .expect("delivery intent should project")
            .expect("failed intent should produce runtime error metadata");

        assert_eq!(error.class(), RuntimeErrorClass::RecoveryBlocked);
        assert_eq!(error.retryability(), RuntimeRetryability::RequiresOperatorReview);
        assert!(!error.side_effect_may_have_occurred());
        assert_eq!(error.reason_code(), "delivery.intent.failed");
    }

    #[test]
    fn delivered_intent_has_no_runtime_error_projection() {
        assert!(intent(DeliveryIntentStatus::Delivered)
            .runtime_error_envelope(true)
            .expect("delivered intent should be valid")
            .is_none());
    }

    #[test]
    fn persisted_delivery_failure_text_is_never_reused_as_reason_code() {
        let mut intent = intent(DeliveryIntentStatus::PlatformOutcomeUnknown);
        intent.last_reason_code = Some("api_key=sk-secret raw connector error".to_owned());

        let error = intent
            .runtime_error_envelope(false)
            .expect("status-owned reason code should remain valid")
            .expect("unknown outcome should produce runtime error metadata");
        let encoded = serde_json::to_string(&error).expect("runtime error should serialize");

        assert_eq!(error.reason_code(), "delivery.intent.platform_outcome_unknown");
        assert!(!encoded.contains("sk-secret"));
    }
}
