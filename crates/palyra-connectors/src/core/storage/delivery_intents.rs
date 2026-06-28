//! Durable delivery intents layered over connector outbox rows.
//!
//! Delivery intents are the operator-facing delivery lifecycle. They reference
//! outbox envelope ids but expose only hashes, previews, status, attempts, and
//! receipt identifiers; raw outbound payloads stay in the outbox.

use rusqlite::params;

use super::records::parse_delivery_intent_row;
use super::{
    ConnectorStore, ConnectorStoreError, DeliveryIntentDraft, DeliveryIntentRecord,
    DeliveryIntentRetryOutcome, DeliveryIntentStatus,
};

const DELIVERY_INTENT_SELECT: &str = r#"
    SELECT intent_id, connector_id, ingress_event_id, ingress_envelope_id,
           session_id, run_id, principal, conversation_id, output_index,
           outbox_envelope_id, payload_hash, visible_text_preview, status,
           send_attempts, native_message_id, last_reason_code,
           redaction_summary_json, created_at_unix_ms, updated_at_unix_ms
    FROM delivery_intents
"#;

impl ConnectorStore {
    /// Inserts a delivery intent or returns the existing matching row.
    ///
    /// # Errors
    /// Returns a storage error when the insert or follow-up read fails.
    pub fn upsert_delivery_intent(
        &self,
        draft: &DeliveryIntentDraft,
        now_unix_ms: i64,
    ) -> Result<DeliveryIntentRecord, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                INSERT OR IGNORE INTO delivery_intents (
                    intent_id, connector_id, ingress_event_id, ingress_envelope_id,
                    session_id, run_id, principal, conversation_id, output_index,
                    outbox_envelope_id, payload_hash, visible_text_preview, status,
                    send_attempts, native_message_id, last_reason_code,
                    redaction_summary_json, created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, 0, NULL, NULL, ?14, ?15, ?15
                )
                "#,
                params![
                    draft.intent_id,
                    draft.connector_id,
                    draft.ingress_event_id,
                    draft.ingress_envelope_id,
                    draft.session_id,
                    draft.run_id,
                    draft.principal,
                    draft.conversation_id,
                    i64::from(draft.output_index),
                    draft.outbox_envelope_id,
                    draft.payload_hash,
                    draft.visible_text_preview,
                    draft.status.as_str(),
                    draft.redaction_summary_json,
                    now_unix_ms,
                ],
            )?;
            query_delivery_intent_by_id(transaction, draft.intent_id.as_str())?
                .ok_or_else(|| ConnectorStoreError::DeliveryIntentNotFound(draft.intent_id.clone()))
        })
    }

    /// Marks all intents for an outbox envelope as adapter-send started.
    ///
    /// Direct outbox entries do not have delivery intents, so a zero-row
    /// update is allowed and reported as `Ok(0)`.
    ///
    /// # Errors
    /// Returns a storage error when the update fails.
    pub fn mark_delivery_intent_send_started_for_outbox(
        &self,
        connector_id: &str,
        outbox_envelope_id: &str,
        now_unix_ms: i64,
    ) -> Result<usize, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'adapter_send_started',
                    send_attempts = send_attempts + 1,
                    last_reason_code = NULL,
                    updated_at_unix_ms = ?3
                WHERE connector_id = ?1
                  AND outbox_envelope_id = ?2
                  AND status IN ('queued', 'platform_outcome_unknown')
                "#,
                params![connector_id, outbox_envelope_id, now_unix_ms],
            )?;
            Ok(changed)
        })
    }

    /// Marks all intents for an outbox envelope delivered and mirrors a
    /// transcript-safe row.
    ///
    /// # Errors
    /// Returns a storage error when the transition or mirror upsert fails.
    pub fn mark_delivery_intent_delivered_for_outbox(
        &self,
        connector_id: &str,
        outbox_envelope_id: &str,
        native_message_id: &str,
        now_unix_ms: i64,
    ) -> Result<usize, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let intents =
                query_delivery_intents_by_outbox(transaction, connector_id, outbox_envelope_id)?;
            let changed = transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'delivered',
                    native_message_id = ?3,
                    last_reason_code = NULL,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                  AND outbox_envelope_id = ?2
                  AND status <> 'suppressed'
                "#,
                params![connector_id, outbox_envelope_id, native_message_id, now_unix_ms],
            )?;
            for intent in intents {
                transaction.execute(
                    r#"
                    INSERT INTO delivery_transcript_mirror (
                        intent_id, connector_id, conversation_id, native_message_id,
                        visible_text_hash, visible_text_preview, delivered_at_unix_ms
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                    ON CONFLICT(intent_id) DO UPDATE SET
                        native_message_id = excluded.native_message_id,
                        visible_text_hash = excluded.visible_text_hash,
                        visible_text_preview = excluded.visible_text_preview,
                        delivered_at_unix_ms = excluded.delivered_at_unix_ms
                    "#,
                    params![
                        intent.intent_id,
                        intent.connector_id,
                        intent.conversation_id,
                        native_message_id,
                        intent.payload_hash,
                        intent.visible_text_preview,
                        now_unix_ms,
                    ],
                )?;
            }
            Ok(changed)
        })
    }

    /// Marks intents for an outbox envelope as platform-outcome unknown.
    ///
    /// # Errors
    /// Returns a storage error when the update fails.
    pub fn mark_delivery_intent_unknown_for_outbox(
        &self,
        connector_id: &str,
        outbox_envelope_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<usize, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'platform_outcome_unknown',
                    last_reason_code = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                  AND outbox_envelope_id = ?2
                  AND status <> 'delivered'
                  AND status <> 'suppressed'
                "#,
                params![connector_id, outbox_envelope_id, reason_code, now_unix_ms],
            )?;
            Ok(changed)
        })
    }

    /// Marks intents for an outbox envelope dead-lettered.
    ///
    /// # Errors
    /// Returns a storage error when the update fails.
    pub fn mark_delivery_intent_dead_lettered_for_outbox(
        &self,
        connector_id: &str,
        outbox_envelope_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<usize, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'dead_lettered',
                    last_reason_code = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                  AND outbox_envelope_id = ?2
                  AND status <> 'delivered'
                  AND status <> 'suppressed'
                "#,
                params![connector_id, outbox_envelope_id, reason_code, now_unix_ms],
            )?;
            Ok(changed)
        })
    }

    /// Lists delivery intents newest first.
    ///
    /// # Errors
    /// Returns a storage error when the query or row decoding fails.
    pub fn list_delivery_intents(
        &self,
        connector_id: &str,
        status: Option<DeliveryIntentStatus>,
        limit: usize,
    ) -> Result<Vec<DeliveryIntentRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let mut records = Vec::new();
        if let Some(status) = status {
            let mut statement = connection.prepare(&format!(
                "{DELIVERY_INTENT_SELECT} WHERE connector_id = ?1 AND status = ?2 ORDER BY updated_at_unix_ms DESC, intent_id ASC LIMIT ?3"
            ))?;
            let mut rows = statement.query(params![connector_id, status.as_str(), limit_i64])?;
            while let Some(row) = rows.next()? {
                records.push(parse_delivery_intent_row(row)?);
            }
        } else {
            let mut statement = connection.prepare(&format!(
                "{DELIVERY_INTENT_SELECT} WHERE connector_id = ?1 ORDER BY updated_at_unix_ms DESC, intent_id ASC LIMIT ?2"
            ))?;
            let mut rows = statement.query(params![connector_id, limit_i64])?;
            while let Some(row) = rows.next()? {
                records.push(parse_delivery_intent_row(row)?);
            }
        }
        Ok(records)
    }

    /// Returns one delivery intent.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::DeliveryIntentNotFound`] when the id is
    /// unknown.
    pub fn get_delivery_intent(
        &self,
        intent_id: &str,
    ) -> Result<DeliveryIntentRecord, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement =
            connection.prepare(&format!("{DELIVERY_INTENT_SELECT} WHERE intent_id = ?1"))?;
        let record = query_delivery_intent_with_statement(&mut statement, intent_id)?
            .ok_or_else(|| ConnectorStoreError::DeliveryIntentNotFound(intent_id.to_owned()))?;
        Ok(record)
    }

    /// Requeues a delivery intent whose status is safe for operator retry.
    ///
    /// # Errors
    /// Returns a typed error when the intent is missing or currently in an
    /// unsafe state such as `delivered`, `queued`, or `adapter_send_started`.
    pub fn retry_delivery_intent(
        &self,
        intent_id: &str,
        max_attempts: u32,
        now_unix_ms: i64,
    ) -> Result<DeliveryIntentRetryOutcome, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let intent = query_delivery_intent_by_id(transaction, intent_id)?
                .ok_or_else(|| ConnectorStoreError::DeliveryIntentNotFound(intent_id.to_owned()))?;
            if !matches!(
                intent.status,
                DeliveryIntentStatus::Failed
                    | DeliveryIntentStatus::DeadLettered
                    | DeliveryIntentStatus::PlatformOutcomeUnknown
            ) {
                return Err(ConnectorStoreError::InvalidDeliveryIntentRetry {
                    intent_id: intent.intent_id,
                    status: intent.status.as_str().to_owned(),
                });
            }
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET status = 'pending',
                    attempts = 0,
                    max_attempts = ?3,
                    next_attempt_unix_ms = ?4,
                    native_message_id = NULL,
                    last_error = NULL,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                  AND envelope_id = ?2
                  AND status IN ('dead', 'pending')
                  AND claim_expires_unix_ms <= ?4
                "#,
                params![
                    intent.connector_id,
                    intent.outbox_envelope_id,
                    i64::from(max_attempts.max(1)),
                    now_unix_ms,
                ],
            )?;
            transaction.execute(
                r#"
                UPDATE delivery_intents
                SET status = 'queued',
                    native_message_id = NULL,
                    last_reason_code = NULL,
                    updated_at_unix_ms = ?2
                WHERE intent_id = ?1
                "#,
                params![intent_id, now_unix_ms],
            )?;
            let updated = query_delivery_intent_by_id(transaction, intent_id)?
                .ok_or_else(|| ConnectorStoreError::DeliveryIntentNotFound(intent_id.to_owned()))?;
            Ok(DeliveryIntentRetryOutcome { intent: updated, requeued: changed > 0 })
        })
    }
}

fn query_delivery_intent_by_id(
    transaction: &rusqlite::Transaction<'_>,
    intent_id: &str,
) -> Result<Option<DeliveryIntentRecord>, ConnectorStoreError> {
    let mut statement =
        transaction.prepare(&format!("{DELIVERY_INTENT_SELECT} WHERE intent_id = ?1"))?;
    query_delivery_intent_with_statement(&mut statement, intent_id)
}

fn query_delivery_intent_with_statement(
    statement: &mut rusqlite::Statement<'_>,
    intent_id: &str,
) -> Result<Option<DeliveryIntentRecord>, ConnectorStoreError> {
    let mut rows = statement.query(params![intent_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(parse_delivery_intent_row(row)?))
    } else {
        Ok(None)
    }
}

fn query_delivery_intents_by_outbox(
    transaction: &rusqlite::Transaction<'_>,
    connector_id: &str,
    outbox_envelope_id: &str,
) -> Result<Vec<DeliveryIntentRecord>, ConnectorStoreError> {
    let mut statement = transaction.prepare(&format!(
        "{DELIVERY_INTENT_SELECT} WHERE connector_id = ?1 AND outbox_envelope_id = ?2"
    ))?;
    let mut rows = statement.query(params![connector_id, outbox_envelope_id])?;
    let mut intents = Vec::new();
    while let Some(row) = rows.next()? {
        intents.push(parse_delivery_intent_row(row)?);
    }
    Ok(intents)
}
