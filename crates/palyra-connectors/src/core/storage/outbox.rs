//! Durable outbox: idempotent enqueue, claim-based draining, and delivery
//! status transitions.
//!
//! Drains claim due entries under a short lease (see `OUTBOX_CLAIM_LEASE_MS`)
//! and every later transition re-checks the claim token, so a crashed worker's
//! entries become reclaimable after the lease expires without ever being
//! processed concurrently. Entries are always served oldest-deadline-first.

use rusqlite::{params, OptionalExtension};

use super::super::protocol::OutboundMessageRequest;
use super::records::parse_outbox_row;
use super::{
    next_outbox_claim_token, ConnectorStore, ConnectorStoreError, OutboxEnqueueOutcome,
    OutboxEntryRecord, OUTBOX_CLAIM_LEASE_MS,
};

impl ConnectorStore {
    /// Enqueues the payload unless its `(connector_id, envelope_id)` pair is
    /// already present, making enqueue retries idempotent.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Serde`] when the payload cannot be
    /// encoded and [`ConnectorStoreError::Sqlite`] when the insert fails.
    pub fn enqueue_outbox_if_absent(
        &self,
        payload: &OutboundMessageRequest,
        max_attempts: u32,
        now_unix_ms: i64,
    ) -> Result<OutboxEnqueueOutcome, ConnectorStoreError> {
        let payload_json = serde_json::to_string(payload)?;
        let inserted = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                INSERT OR IGNORE INTO outbox (
                    connector_id, envelope_id, payload_json, attempts, max_attempts,
                    next_attempt_unix_ms, status, native_message_id, last_error,
                    created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (?1, ?2, ?3, 0, ?4, ?5, 'pending', NULL, NULL, ?5, ?5)
                "#,
                params![
                    payload.connector_id,
                    payload.envelope_id,
                    payload_json,
                    i64::from(max_attempts.max(1)),
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        Ok(OutboxEnqueueOutcome { created: inserted > 0 })
    }

    /// Claims and returns up to `limit` due pending entries under a fresh
    /// claim token, ordered by `next_attempt_unix_ms` then insertion order.
    ///
    /// Entries whose previous claim lease has expired are reclaimed. Unless
    /// `ignore_queue_pause` is set, paused connectors are skipped.
    ///
    /// # Errors
    /// Returns a storage error when the claim transaction fails or a claimed
    /// payload no longer deserializes.
    pub fn load_due_outbox(
        &self,
        now_unix_ms: i64,
        limit: usize,
        connector_filter: Option<&str>,
        ignore_queue_pause: bool,
    ) -> Result<Vec<OutboxEntryRecord>, ConnectorStoreError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        if limit_i64 <= 0 {
            return Ok(Vec::new());
        }
        let claim_token = next_outbox_claim_token(now_unix_ms);
        let claim_expires_unix_ms = now_unix_ms.saturating_add(OUTBOX_CLAIM_LEASE_MS);

        self.with_transaction(|transaction| {
            // Four static statements instead of dynamically composed SQL: the
            // filter/pause combinations are few and keeping each statement
            // literal keeps them parameterized and individually reviewable.
            if let Some(connector_id) = connector_filter {
                if ignore_queue_pause {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE outbox_id IN (
                            SELECT outbox_id
                            FROM outbox
                            WHERE status = 'pending'
                              AND next_attempt_unix_ms <= ?3
                              AND claim_expires_unix_ms <= ?3
                              AND connector_id = ?4
                            ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                            LIMIT ?5
                        )
                        "#,
                        params![
                            claim_token.as_str(),
                            claim_expires_unix_ms,
                            now_unix_ms,
                            connector_id,
                            limit_i64,
                        ],
                    )?;
                } else {
                    transaction.execute(
                        r#"
                        UPDATE outbox
                        SET claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE outbox_id IN (
                            SELECT outbox_id
                            FROM outbox
                            LEFT JOIN connector_queue_state
                                ON connector_queue_state.connector_id = outbox.connector_id
                            WHERE outbox.status = 'pending'
                              AND COALESCE(connector_queue_state.paused, 0) = 0
                              AND outbox.next_attempt_unix_ms <= ?3
                              AND outbox.claim_expires_unix_ms <= ?3
                              AND outbox.connector_id = ?4
                            ORDER BY outbox.next_attempt_unix_ms ASC, outbox.outbox_id ASC
                            LIMIT ?5
                        )
                        "#,
                        params![
                            claim_token.as_str(),
                            claim_expires_unix_ms,
                            now_unix_ms,
                            connector_id,
                            limit_i64,
                        ],
                    )?;
                }
            } else if ignore_queue_pause {
                transaction.execute(
                    r#"
                    UPDATE outbox
                    SET claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE outbox_id IN (
                        SELECT outbox_id
                        FROM outbox
                        WHERE status = 'pending'
                          AND next_attempt_unix_ms <= ?3
                          AND claim_expires_unix_ms <= ?3
                        ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            } else {
                transaction.execute(
                    r#"
                    UPDATE outbox
                    SET claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE outbox_id IN (
                        SELECT outbox_id
                        FROM outbox
                        LEFT JOIN connector_queue_state
                            ON connector_queue_state.connector_id = outbox.connector_id
                        WHERE outbox.status = 'pending'
                          AND COALESCE(connector_queue_state.paused, 0) = 0
                          AND outbox.next_attempt_unix_ms <= ?3
                          AND outbox.claim_expires_unix_ms <= ?3
                        ORDER BY outbox.next_attempt_unix_ms ASC, outbox.outbox_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            }

            let mut records = Vec::new();
            let mut statement = transaction.prepare(
                r#"
                SELECT outbox_id, connector_id, envelope_id, payload_json, attempts, max_attempts,
                       next_attempt_unix_ms, claim_token, created_at_unix_ms, updated_at_unix_ms
                FROM outbox
                WHERE claim_token = ?1
                ORDER BY next_attempt_unix_ms ASC, outbox_id ASC
                "#,
            )?;
            let mut rows = statement.query(params![claim_token.as_str()])?;
            while let Some(row) = rows.next()? {
                records.push(parse_outbox_row(row)?);
            }
            Ok(records)
        })
    }

    /// Marks a claimed entry delivered and releases its claim.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn mark_outbox_delivered(
        &self,
        outbox_id: i64,
        claim_token: &str,
        native_message_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET status = 'delivered',
                    native_message_id = ?3,
                    last_error = NULL,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?4
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?2
                "#,
                params![outbox_id, claim_token, native_message_id, now_unix_ms],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Releases a claimed entry back to pending with an updated attempt count
    /// and next-attempt deadline.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn schedule_outbox_retry(
        &self,
        outbox_id: i64,
        claim_token: &str,
        attempts: u32,
        reason: &str,
        next_attempt_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET attempts = ?3,
                    next_attempt_unix_ms = ?4,
                    status = 'pending',
                    last_error = ?5,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?4
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?2
                "#,
                params![outbox_id, claim_token, i64::from(attempts), next_attempt_unix_ms, reason],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Copies a claimed entry into `dead_letters` and marks it dead, in one
    /// transaction.
    ///
    /// The outbox row is kept (status `dead`) so the envelope id stays
    /// reserved; replay flips it back to pending instead of inserting anew.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::OutboxNotFound`] when the entry is not
    /// pending under `claim_token` (already resolved or reclaimed elsewhere).
    pub fn move_outbox_to_dead_letter(
        &self,
        outbox_id: i64,
        claim_token: &str,
        reason: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let maybe_payload = transaction
                .query_row(
                    r#"
                    SELECT connector_id, envelope_id, payload_json
                    FROM outbox
                    WHERE outbox_id = ?1
                      AND status = 'pending'
                      AND claim_token = ?2
                    "#,
                    params![outbox_id, claim_token],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            let Some((connector_id, envelope_id, payload_json)) = maybe_payload else {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            };
            transaction.execute(
                r#"
                INSERT INTO dead_letters (
                    connector_id, envelope_id, reason, payload_json, created_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![connector_id, envelope_id, reason, payload_json, now_unix_ms],
            )?;
            let changed = transaction.execute(
                r#"
                UPDATE outbox
                SET status = 'dead',
                    last_error = ?2,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?3
                WHERE outbox_id = ?1
                  AND status = 'pending'
                  AND claim_token = ?4
                "#,
                params![outbox_id, reason, now_unix_ms, claim_token],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::OutboxNotFound(outbox_id));
            }
            Ok(())
        })
    }
}
