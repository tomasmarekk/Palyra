//! Explicit reconciliation for connector effects whose platform outcome is unknown.

use rusqlite::{params, OptionalExtension};

use super::super::protocol::{ConnectorLiveness, ConnectorReadiness};
use super::delivery_intents::mark_delivery_intents_delivered_in_transaction;
use super::{
    ConnectorStore, ConnectorStoreError, OutboxEffectState, OutboxReconciliationEvidence,
    OutboxReconciliationOutcome, OutboxUnknownRecord,
};

impl ConnectorStore {
    /// Lists outcome-unknown outbox rows without exposing outbound payloads.
    ///
    /// # Errors
    /// Returns a storage error when the query or integer conversion fails.
    pub fn list_outbox_unknown(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxUnknownRecord>, ConnectorStoreError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement = connection.prepare(
            r#"
            SELECT outbox_id, connector_id, envelope_id, attempts, last_error, updated_at_unix_ms
            FROM outbox
            WHERE connector_id = ?1
              AND status = 'pending'
              AND effect_state = 'outcome_unknown'
            ORDER BY updated_at_unix_ms ASC, outbox_id ASC
            LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![connector_id, limit_i64])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let attempts = u32::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| ConnectorStoreError::ValueOverflow { field: "attempts" })?;
            records.push(OutboxUnknownRecord {
                outbox_id: row.get(0)?,
                connector_id: row.get(1)?,
                envelope_id: row.get(2)?,
                attempts,
                last_reason_code: row.get(4)?,
                updated_at_unix_ms: row.get(5)?,
            });
        }
        Ok(records)
    }

    /// Applies platform evidence to one outcome-unknown outbox row.
    ///
    /// `Delivered` atomically completes the outbox and delivery intents.
    /// `ConfirmedAbsent` only removes the side-effect fence and requeues the
    /// existing envelope; it never performs the external effect itself.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotOutcomeUnknown`] when the row is
    /// not parked, or [`ConnectorStoreError::MissingReconciledNativeMessageId`]
    /// when delivered evidence omits a platform identifier.
    pub fn reconcile_outbox_unknown(
        &self,
        outbox_id: i64,
        evidence: &OutboxReconciliationEvidence,
        now_unix_ms: i64,
    ) -> Result<OutboxReconciliationOutcome, ConnectorStoreError> {
        if matches!(
            evidence,
            OutboxReconciliationEvidence::Delivered { native_message_id }
                if native_message_id.trim().is_empty()
        ) {
            return Err(ConnectorStoreError::MissingReconciledNativeMessageId);
        }
        self.with_transaction(|transaction| {
            let identity = transaction
                .query_row(
                    r#"
                    SELECT connector_id, envelope_id
                    FROM outbox
                    WHERE outbox_id = ?1
                      AND status = 'pending'
                      AND effect_state = 'outcome_unknown'
                    "#,
                    params![outbox_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((connector_id, envelope_id)) = identity else {
                return Err(ConnectorStoreError::OutboxNotOutcomeUnknown(outbox_id));
            };

            let (delivered, requeued, event_type, event_message) = match evidence {
                OutboxReconciliationEvidence::Delivered { native_message_id } => {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET status = 'delivered',
                            effect_state = 'ready',
                            native_message_id = ?2,
                            last_error = NULL,
                            claim_token = NULL,
                            claim_expires_unix_ms = 0,
                            updated_at_unix_ms = ?3
                        WHERE outbox_id = ?1
                          AND effect_state = 'outcome_unknown'
                        "#,
                        params![outbox_id, native_message_id, now_unix_ms],
                    )?;
                    mark_delivery_intents_delivered_in_transaction(
                        transaction,
                        connector_id.as_str(),
                        envelope_id.as_str(),
                        native_message_id.as_str(),
                        now_unix_ms,
                    )?;
                    transaction.execute(
                        r#"
                        UPDATE connector_instances
                        SET last_outbound_unix_ms = ?2,
                            last_error = NULL,
                            readiness = ?3,
                            liveness = ?4,
                            updated_at_unix_ms = ?2
                        WHERE connector_id = ?1
                        "#,
                        params![
                            connector_id,
                            now_unix_ms,
                            ConnectorReadiness::Ready.as_str(),
                            ConnectorLiveness::Running.as_str(),
                        ],
                    )?;
                    (
                        true,
                        false,
                        "outbox.reconciled_delivered",
                        "unknown delivery reconciled as delivered",
                    )
                }
                OutboxReconciliationEvidence::ConfirmedAbsent => {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET effect_state = 'ready',
                            next_attempt_unix_ms = ?2,
                            last_error = NULL,
                            claim_token = NULL,
                            claim_expires_unix_ms = 0,
                            updated_at_unix_ms = ?2
                        WHERE outbox_id = ?1
                          AND status = 'pending'
                          AND effect_state = 'outcome_unknown'
                        "#,
                        params![outbox_id, now_unix_ms],
                    )?;
                    transaction.execute(
                        r#"
                        UPDATE delivery_intents
                        SET status = 'queued',
                            last_reason_code = NULL,
                            updated_at_unix_ms = ?3
                        WHERE connector_id = ?1
                          AND outbox_envelope_id = ?2
                          AND status = 'platform_outcome_unknown'
                        "#,
                        params![connector_id, envelope_id, now_unix_ms],
                    )?;
                    (
                        false,
                        true,
                        "outbox.reconciled_absent",
                        "unknown delivery reconciled as absent",
                    )
                }
            };
            transaction.execute(
                r#"
                INSERT INTO connector_events (
                    connector_id, event_type, level, message, details_json, created_at_unix_ms
                )
                VALUES (?1, ?2, 'info', ?3, ?4, ?5)
                "#,
                params![
                    connector_id,
                    event_type,
                    event_message,
                    serde_json::json!({
                        "outbox_id": outbox_id,
                        "envelope_id": envelope_id,
                    })
                    .to_string(),
                    now_unix_ms,
                ],
            )?;
            Ok(OutboxReconciliationOutcome {
                outbox_id,
                connector_id,
                envelope_id,
                effect_state: OutboxEffectState::Ready,
                delivered,
                requeued,
            })
        })
    }
}
