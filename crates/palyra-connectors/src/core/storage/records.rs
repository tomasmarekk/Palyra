//! Typed row representations of connector storage tables and their parsers.
//!
//! The `parse_*_row` helpers expect column order to match the SELECT lists in
//! the sibling query modules; keep both sides in sync when columns change.

use rusqlite::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::protocol::{
    ConnectorKind, ConnectorLiveness, ConnectorQueueDepth, ConnectorReadiness,
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
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Whether an enqueue inserted a new entry (`false` means the envelope was
/// already present and the call was an idempotent no-op).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEnqueueOutcome {
    pub created: bool,
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
        created_at_unix_ms: row.get(8)?,
        updated_at_unix_ms: row.get(9)?,
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
