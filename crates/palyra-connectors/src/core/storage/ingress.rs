//! Durable inbound queue operations.
//!
//! Ingress events are inserted before routing. Workers claim due rows with a
//! lease and lane-aware ordering so a crash can only delay processing until
//! the lease expires; it cannot lose the inbound event or reorder messages
//! inside one conversation lane.

use rusqlite::params;
use sha2::{Digest, Sha256};

use super::super::protocol::InboundMessageEvent;
use super::records::parse_channel_ingress_row;
use super::{
    next_ingress_claim_token, ChannelIngressEnqueueOutcome, ChannelIngressRecord,
    ChannelIngressStatus, ConnectorStore, ConnectorStoreError,
};

const CHANNEL_INGRESS_SELECT: &str = r#"
    SELECT ingress_event_id, connector_id, principal, conversation_id, envelope_id,
           payload_hash, payload_json, status, lane_key, attempts, max_attempts,
           next_attempt_unix_ms, claim_token, claim_expires_unix_ms,
           last_error_reason_code, last_error_message, route_key, session_id,
           run_id, completed_at_unix_ms, tombstone_expires_at_unix_ms,
           created_at_unix_ms, updated_at_unix_ms
    FROM channel_ingress_events
"#;

impl ConnectorStore {
    /// Persists an inbound event unless its `(connector_id, envelope_id)` row
    /// already exists. Terminal rows whose tombstone expired are purged first.
    ///
    /// # Errors
    /// Returns serialization or storage errors when the event cannot be
    /// encoded, inserted, or re-read.
    pub fn enqueue_channel_ingress_if_absent(
        &self,
        event: &InboundMessageEvent,
        principal: &str,
        now_unix_ms: i64,
        max_attempts: u32,
        tombstone_window_ms: i64,
    ) -> Result<ChannelIngressEnqueueOutcome, ConnectorStoreError> {
        let payload_json = serde_json::to_string(event)?;
        let payload_hash = sha256_hex(payload_json.as_bytes());
        let lane_key = ingress_lane_key(event);
        let tombstone_expires_at_unix_ms = now_unix_ms.saturating_add(tombstone_window_ms.max(1));
        let record = self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                DELETE FROM channel_ingress_events
                WHERE tombstone_expires_at_unix_ms IS NOT NULL
                  AND tombstone_expires_at_unix_ms <= ?1
                  AND status IN ('completed', 'failed', 'quarantined')
                "#,
                params![now_unix_ms],
            )?;
            let inserted = transaction.execute(
                r#"
                INSERT OR IGNORE INTO channel_ingress_events (
                    connector_id, principal, conversation_id, envelope_id,
                    payload_hash, payload_json, status, lane_key, attempts,
                    max_attempts, next_attempt_unix_ms, claim_token,
                    claim_expires_unix_ms, last_error_reason_code, last_error_message,
                    route_key, session_id, run_id, completed_at_unix_ms,
                    tombstone_expires_at_unix_ms, created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, 0, ?8, ?9,
                    NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, ?10, ?9, ?9
                )
                "#,
                params![
                    event.connector_id,
                    principal,
                    event.conversation_id,
                    event.envelope_id,
                    payload_hash,
                    payload_json,
                    lane_key,
                    i64::from(max_attempts.max(1)),
                    now_unix_ms,
                    tombstone_expires_at_unix_ms,
                ],
            )?;
            let record = query_channel_ingress_by_envelope(
                transaction,
                event.connector_id.as_str(),
                event.envelope_id.as_str(),
            )?
            .ok_or_else(|| ConnectorStoreError::NotFound(event.envelope_id.clone()))?;
            Ok((inserted > 0, record))
        })?;
        Ok(ChannelIngressEnqueueOutcome { created: record.0, record: record.1 })
    }

    /// Claims due ingress rows under a fresh lease, preserving lane order.
    ///
    /// # Errors
    /// Returns a storage error when the claim transaction or row decoding
    /// fails.
    pub fn load_due_channel_ingress(
        &self,
        now_unix_ms: i64,
        limit: usize,
        connector_filter: Option<&str>,
        claim_lease_ms: i64,
        ignore_queue_pause: bool,
    ) -> Result<Vec<ChannelIngressRecord>, ConnectorStoreError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        if limit_i64 <= 0 {
            return Ok(Vec::new());
        }
        let claim_token = next_ingress_claim_token(now_unix_ms);
        let claim_expires_unix_ms = now_unix_ms.saturating_add(claim_lease_ms.max(1));

        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                UPDATE channel_ingress_events
                SET status = 'retrying',
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    updated_at_unix_ms = ?1
                WHERE status = 'claimed'
                  AND claim_expires_unix_ms <= ?1
                "#,
                params![now_unix_ms],
            )?;

            if let Some(connector_id) = connector_filter {
                if ignore_queue_pause {
                    transaction.execute(
                        r#"
                        UPDATE channel_ingress_events
                        SET status = 'claimed',
                            attempts = attempts + 1,
                            claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE ingress_event_id IN (
                            SELECT candidate.ingress_event_id
                            FROM channel_ingress_events AS candidate
                            WHERE candidate.status IN ('pending', 'retrying')
                              AND candidate.next_attempt_unix_ms <= ?3
                              AND candidate.connector_id = ?4
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM channel_ingress_events AS older
                                  WHERE older.connector_id = candidate.connector_id
                                    AND older.lane_key = candidate.lane_key
                                    AND older.ingress_event_id < candidate.ingress_event_id
                                    AND older.status IN ('pending', 'claimed', 'retrying')
                              )
                            ORDER BY candidate.next_attempt_unix_ms ASC, candidate.ingress_event_id ASC
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
                        UPDATE channel_ingress_events
                        SET status = 'claimed',
                            attempts = attempts + 1,
                            claim_token = ?1,
                            claim_expires_unix_ms = ?2,
                            updated_at_unix_ms = ?3
                        WHERE ingress_event_id IN (
                            SELECT candidate.ingress_event_id
                            FROM channel_ingress_events AS candidate
                            LEFT JOIN connector_queue_state
                                ON connector_queue_state.connector_id = candidate.connector_id
                            WHERE candidate.status IN ('pending', 'retrying')
                              AND COALESCE(connector_queue_state.paused, 0) = 0
                              AND candidate.next_attempt_unix_ms <= ?3
                              AND candidate.connector_id = ?4
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM channel_ingress_events AS older
                                  WHERE older.connector_id = candidate.connector_id
                                    AND older.lane_key = candidate.lane_key
                                    AND older.ingress_event_id < candidate.ingress_event_id
                                    AND older.status IN ('pending', 'claimed', 'retrying')
                              )
                            ORDER BY candidate.next_attempt_unix_ms ASC, candidate.ingress_event_id ASC
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
                    UPDATE channel_ingress_events
                    SET status = 'claimed',
                        attempts = attempts + 1,
                        claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE ingress_event_id IN (
                        SELECT candidate.ingress_event_id
                        FROM channel_ingress_events AS candidate
                        WHERE candidate.status IN ('pending', 'retrying')
                          AND candidate.next_attempt_unix_ms <= ?3
                          AND NOT EXISTS (
                              SELECT 1
                              FROM channel_ingress_events AS older
                              WHERE older.connector_id = candidate.connector_id
                                AND older.lane_key = candidate.lane_key
                                AND older.ingress_event_id < candidate.ingress_event_id
                                AND older.status IN ('pending', 'claimed', 'retrying')
                          )
                        ORDER BY candidate.next_attempt_unix_ms ASC, candidate.ingress_event_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            } else {
                transaction.execute(
                    r#"
                    UPDATE channel_ingress_events
                    SET status = 'claimed',
                        attempts = attempts + 1,
                        claim_token = ?1,
                        claim_expires_unix_ms = ?2,
                        updated_at_unix_ms = ?3
                    WHERE ingress_event_id IN (
                        SELECT candidate.ingress_event_id
                        FROM channel_ingress_events AS candidate
                        LEFT JOIN connector_queue_state
                            ON connector_queue_state.connector_id = candidate.connector_id
                        WHERE candidate.status IN ('pending', 'retrying')
                          AND COALESCE(connector_queue_state.paused, 0) = 0
                          AND candidate.next_attempt_unix_ms <= ?3
                          AND NOT EXISTS (
                              SELECT 1
                              FROM channel_ingress_events AS older
                              WHERE older.connector_id = candidate.connector_id
                                AND older.lane_key = candidate.lane_key
                                AND older.ingress_event_id < candidate.ingress_event_id
                                AND older.status IN ('pending', 'claimed', 'retrying')
                          )
                        ORDER BY candidate.next_attempt_unix_ms ASC, candidate.ingress_event_id ASC
                        LIMIT ?4
                    )
                    "#,
                    params![claim_token.as_str(), claim_expires_unix_ms, now_unix_ms, limit_i64],
                )?;
            }

            let mut statement = transaction.prepare(&format!(
                "{CHANNEL_INGRESS_SELECT} WHERE claim_token = ?1 ORDER BY next_attempt_unix_ms ASC, ingress_event_id ASC"
            ))?;
            let mut rows = statement.query(params![claim_token.as_str()])?;
            let mut records = Vec::new();
            while let Some(row) = rows.next()? {
                records.push(parse_channel_ingress_row(row)?);
            }
            Ok(records)
        })
    }

    /// Marks a claimed ingress event completed.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::ChannelIngressNotFound`] when the
    /// ingress row is missing, terminal, or no longer claimed by `claim_token`.
    pub fn mark_channel_ingress_completed(
        &self,
        ingress_event_id: i64,
        claim_token: &str,
        route_key: Option<&str>,
        session_id: Option<&str>,
        run_id: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.complete_channel_ingress(
            ingress_event_id,
            claim_token,
            ChannelIngressStatus::Completed,
            None,
            None,
            route_key,
            session_id,
            run_id,
            now_unix_ms,
        )
    }

    /// Schedules a claimed ingress event for retry.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::ChannelIngressNotFound`] when the row
    /// is missing or no longer claimed by `claim_token`.
    pub fn schedule_channel_ingress_retry(
        &self,
        ingress_event_id: i64,
        claim_token: &str,
        reason_code: &str,
        message: &str,
        next_attempt_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE channel_ingress_events
                SET status = 'retrying',
                    next_attempt_unix_ms = ?4,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    last_error_reason_code = ?3,
                    last_error_message = ?5,
                    updated_at_unix_ms = ?4
                WHERE ingress_event_id = ?1
                  AND status = 'claimed'
                  AND claim_token = ?2
                "#,
                params![ingress_event_id, claim_token, reason_code, next_attempt_unix_ms, message],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::ChannelIngressNotFound(ingress_event_id));
            }
            Ok(())
        })
    }

    /// Marks a claimed ingress event failed.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::ChannelIngressNotFound`] when the row
    /// is missing or no longer claimed by `claim_token`.
    pub fn mark_channel_ingress_failed(
        &self,
        ingress_event_id: i64,
        claim_token: &str,
        reason_code: &str,
        message: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.complete_channel_ingress(
            ingress_event_id,
            claim_token,
            ChannelIngressStatus::Failed,
            Some(reason_code),
            Some(message),
            None,
            None,
            None,
            now_unix_ms,
        )
    }

    /// Marks a claimed ingress event quarantined.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::ChannelIngressNotFound`] when the row
    /// is missing or no longer claimed by `claim_token`.
    pub fn mark_channel_ingress_quarantined(
        &self,
        ingress_event_id: i64,
        claim_token: &str,
        reason_code: &str,
        message: &str,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.complete_channel_ingress(
            ingress_event_id,
            claim_token,
            ChannelIngressStatus::Quarantined,
            Some(reason_code),
            Some(message),
            None,
            None,
            None,
            now_unix_ms,
        )
    }

    /// Lists stored ingress events newest first.
    ///
    /// # Errors
    /// Returns a storage error when the query or row decoding fails.
    pub fn list_channel_ingress_events(
        &self,
        connector_id: &str,
        status: Option<ChannelIngressStatus>,
        limit: usize,
    ) -> Result<Vec<ChannelIngressRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let mut records = Vec::new();
        if let Some(status) = status {
            let mut statement = connection.prepare(&format!(
                "{CHANNEL_INGRESS_SELECT} WHERE connector_id = ?1 AND status = ?2 ORDER BY ingress_event_id DESC LIMIT ?3"
            ))?;
            let mut rows = statement.query(params![connector_id, status.as_str(), limit_i64])?;
            while let Some(row) = rows.next()? {
                records.push(parse_channel_ingress_row(row)?);
            }
        } else {
            let mut statement = connection.prepare(&format!(
                "{CHANNEL_INGRESS_SELECT} WHERE connector_id = ?1 ORDER BY ingress_event_id DESC LIMIT ?2"
            ))?;
            let mut rows = statement.query(params![connector_id, limit_i64])?;
            while let Some(row) = rows.next()? {
                records.push(parse_channel_ingress_row(row)?);
            }
        }
        Ok(records)
    }

    /// Returns one stored ingress event.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::ChannelIngressNotFound`] when the id is
    /// not present for `connector_id`.
    pub fn get_channel_ingress_event(
        &self,
        connector_id: &str,
        ingress_event_id: i64,
    ) -> Result<ChannelIngressRecord, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let record = query_channel_ingress_by_id(&connection, connector_id, ingress_event_id)?
            .ok_or(ConnectorStoreError::ChannelIngressNotFound(ingress_event_id))?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_channel_ingress(
        &self,
        ingress_event_id: i64,
        claim_token: &str,
        status: ChannelIngressStatus,
        reason_code: Option<&str>,
        message: Option<&str>,
        route_key: Option<&str>,
        session_id: Option<&str>,
        run_id: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE channel_ingress_events
                SET status = ?3,
                    claim_token = NULL,
                    claim_expires_unix_ms = 0,
                    last_error_reason_code = ?4,
                    last_error_message = ?5,
                    route_key = ?6,
                    session_id = ?7,
                    run_id = ?8,
                    completed_at_unix_ms = ?9,
                    updated_at_unix_ms = ?9
                WHERE ingress_event_id = ?1
                  AND status = 'claimed'
                  AND claim_token = ?2
                "#,
                params![
                    ingress_event_id,
                    claim_token,
                    status.as_str(),
                    reason_code,
                    message,
                    route_key,
                    session_id,
                    run_id,
                    now_unix_ms,
                ],
            )?;
            if changed == 0 {
                return Err(ConnectorStoreError::ChannelIngressNotFound(ingress_event_id));
            }
            Ok(())
        })
    }
}

fn query_channel_ingress_by_envelope(
    connection: &rusqlite::Transaction<'_>,
    connector_id: &str,
    envelope_id: &str,
) -> Result<Option<ChannelIngressRecord>, ConnectorStoreError> {
    let mut statement = connection.prepare(&format!(
        "{CHANNEL_INGRESS_SELECT} WHERE connector_id = ?1 AND envelope_id = ?2"
    ))?;
    let mut rows = statement.query(params![connector_id, envelope_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(parse_channel_ingress_row(row)?))
    } else {
        Ok(None)
    }
}

fn query_channel_ingress_by_id(
    connection: &rusqlite::Connection,
    connector_id: &str,
    ingress_event_id: i64,
) -> Result<Option<ChannelIngressRecord>, ConnectorStoreError> {
    let mut statement = connection.prepare(&format!(
        "{CHANNEL_INGRESS_SELECT} WHERE connector_id = ?1 AND ingress_event_id = ?2"
    ))?;
    let mut rows = statement.query(params![connector_id, ingress_event_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(parse_channel_ingress_row(row)?))
    } else {
        Ok(None)
    }
}

fn ingress_lane_key(event: &InboundMessageEvent) -> String {
    format!(
        "{}:{}:{}",
        event.connector_id,
        event.conversation_id,
        event.thread_id.as_deref().unwrap_or("-")
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
