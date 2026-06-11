//! Dead-letter queue operations: replaying entries into the outbox and
//! discarding them.
//!
//! A dead letter preserves the original outbox payload verbatim; replay
//! re-arms the matching `dead` outbox row (resetting its attempt budget) and
//! removes the dead letter in the same transaction.

use rusqlite::{params, OptionalExtension};

use super::records::parse_dead_letter_row;
use super::{ConnectorStore, ConnectorStoreError, DeadLetterRecord};

impl ConnectorStore {
    /// Replays one dead letter back into the outbox with a fresh attempt
    /// budget and returns the consumed record.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::DeadLetterNotFound`] when the id does
    /// not exist for `connector_id`, and a storage error when the transaction
    /// or payload decoding fails.
    pub fn replay_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
        max_attempts: u32,
        now_unix_ms: i64,
    ) -> Result<DeadLetterRecord, ConnectorStoreError> {
        let (dead_letter_id_value, connector_id_value, envelope_id, reason, payload_json, created_at_unix_ms) =
            self.with_transaction(|transaction| {
                let dead_letter = transaction
                    .query_row(
                        r#"
                        SELECT dead_letter_id, connector_id, envelope_id, reason, payload_json, created_at_unix_ms
                        FROM dead_letters
                        WHERE dead_letter_id = ?1
                          AND connector_id = ?2
                        "#,
                        params![dead_letter_id, connector_id],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, i64>(5)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or(ConnectorStoreError::DeadLetterNotFound(dead_letter_id))?;
                // Re-arm the existing `dead` outbox row first; the envelope id
                // stays reserved there, preserving outbox idempotency.
                let updated = transaction.execute(
                    r#"
                    UPDATE outbox
                    SET payload_json = ?3,
                        attempts = 0,
                        max_attempts = ?4,
                        next_attempt_unix_ms = ?5,
                        status = 'pending',
                        native_message_id = NULL,
                        last_error = NULL,
                        claim_token = NULL,
                        claim_expires_unix_ms = 0,
                        updated_at_unix_ms = ?5
                    WHERE connector_id = ?1
                      AND envelope_id = ?2
                      AND status = 'dead'
                    "#,
                    params![
                        dead_letter.1,
                        dead_letter.2,
                        dead_letter.4,
                        i64::from(max_attempts.max(1)),
                        now_unix_ms,
                    ],
                )?;
                if updated == 0 {
                    // AIDEV-NOTE: if the dead outbox row is gone but a *live*
                    // row for the same (connector_id, envelope_id) exists
                    // (e.g. the envelope was re-enqueued after dead-lettering),
                    // this INSERT hits the UNIQUE constraint and the replay
                    // fails with a raw sqlite error instead of a typed
                    // conflict. Fixing it changes observable behavior; left
                    // as-is deliberately.
                    transaction.execute(
                        r#"
                        INSERT INTO outbox (
                            connector_id, envelope_id, payload_json, attempts, max_attempts,
                            next_attempt_unix_ms, status, native_message_id, last_error,
                            claim_token, claim_expires_unix_ms, created_at_unix_ms, updated_at_unix_ms
                        )
                        VALUES (?1, ?2, ?3, 0, ?4, ?5, 'pending', NULL, NULL, NULL, 0, ?5, ?5)
                        "#,
                        params![
                            dead_letter.1,
                            dead_letter.2,
                            dead_letter.4,
                            i64::from(max_attempts.max(1)),
                            now_unix_ms,
                        ],
                    )?;
                }
                let deleted = transaction.execute(
                    "DELETE FROM dead_letters WHERE dead_letter_id = ?1 AND connector_id = ?2",
                    params![dead_letter_id, connector_id],
                )?;
                if deleted == 0 {
                    return Err(ConnectorStoreError::DeadLetterNotFound(dead_letter_id));
                }
                Ok(dead_letter)
            })?;
        Ok(DeadLetterRecord {
            dead_letter_id: dead_letter_id_value,
            connector_id: connector_id_value,
            envelope_id,
            reason,
            payload: serde_json::from_str(payload_json.as_str())?,
            created_at_unix_ms,
        })
    }

    /// Deletes one dead letter and returns the removed record.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::DeadLetterNotFound`] when the id does
    /// not exist for `connector_id`, and a storage error when the transaction
    /// or payload decoding fails.
    pub fn discard_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ConnectorStoreError> {
        let (dead_letter_id_value, connector_id_value, envelope_id, reason, payload_json, created_at_unix_ms) =
            self.with_transaction(|transaction| {
                let dead_letter = transaction
                    .query_row(
                        r#"
                        SELECT dead_letter_id, connector_id, envelope_id, reason, payload_json, created_at_unix_ms
                        FROM dead_letters
                        WHERE dead_letter_id = ?1
                          AND connector_id = ?2
                        "#,
                        params![dead_letter_id, connector_id],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, i64>(5)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or(ConnectorStoreError::DeadLetterNotFound(dead_letter_id))?;
                let deleted = transaction.execute(
                    "DELETE FROM dead_letters WHERE dead_letter_id = ?1 AND connector_id = ?2",
                    params![dead_letter_id, connector_id],
                )?;
                if deleted == 0 {
                    return Err(ConnectorStoreError::DeadLetterNotFound(dead_letter_id));
                }
                Ok(dead_letter)
            })?;
        Ok(DeadLetterRecord {
            dead_letter_id: dead_letter_id_value,
            connector_id: connector_id_value,
            envelope_id,
            reason,
            payload: serde_json::from_str(payload_json.as_str())?,
            created_at_unix_ms,
        })
    }

    /// Lists up to `limit` dead letters for a connector, newest first.
    ///
    /// # Errors
    /// Returns a storage error when the query fails or a stored payload no
    /// longer decodes as JSON.
    pub fn list_dead_letters(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let mut statement = connection.prepare(
            r#"
            SELECT dead_letter_id, connector_id, envelope_id, reason, payload_json, created_at_unix_ms
            FROM dead_letters
            WHERE connector_id = ?1
            ORDER BY dead_letter_id DESC
            LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![connector_id, limit_i64])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(parse_dead_letter_row(row)?);
        }
        Ok(records)
    }
}
