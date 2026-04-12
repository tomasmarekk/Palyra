use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::protocol::{
    ConnectorInstanceSpec, ConnectorKind, ConnectorLiveness, ConnectorQueueDepth,
    ConnectorReadiness, OutboundMessageRequest,
};

#[derive(Debug)]
pub struct ConnectorStore {
    db_path: PathBuf,
    connection: Mutex<Connection>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntryRecord {
    pub outbox_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    pub claim_token: String,
    pub payload: OutboundMessageRequest,
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_attempt_unix_ms: i64,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEnqueueOutcome {
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub dead_letter_id: i64,
    pub connector_id: String,
    pub envelope_id: String,
    pub reason: String,
    pub payload: Value,
    pub created_at_unix_ms: i64,
}

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

#[derive(Debug, Error)]
pub enum ConnectorStoreError {
    #[error("sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization failed: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("connector storage lock is poisoned")]
    PoisonedLock,
    #[error("connector storage schema contains unknown connector kind '{0}'")]
    UnknownConnectorKind(String),
    #[error("connector storage schema contains unknown readiness '{0}'")]
    UnknownReadiness(String),
    #[error("connector storage schema contains unknown liveness '{0}'")]
    UnknownLiveness(String),
    #[error("connector storage value overflow while converting '{field}'")]
    ValueOverflow { field: &'static str },
    #[error("connector record not found: {0}")]
    NotFound(String),
    #[error("outbox entry not found: {0}")]
    OutboxNotFound(i64),
    #[error("dead-letter entry not found: {0}")]
    DeadLetterNotFound(i64),
}

const OUTBOX_CLAIM_LEASE_MS: i64 = 60_000;
static OUTBOX_CLAIM_SEQUENCE: AtomicU64 = AtomicU64::new(1);

impl ConnectorStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ConnectorStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            }
        }
        let connection = Connection::open(path.as_path())?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let store = Self { db_path: path, connection: Mutex::new(connection) };
        store.initialize_schema()?;
        Ok(store)
    }

    #[must_use]
    pub fn db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    pub fn upsert_instance(
        &self,
        spec: &ConnectorInstanceSpec,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        spec.validate()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let allowlist_json = serde_json::to_string(&spec.egress_allowlist)?;
        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                INSERT INTO connector_instances (
                    connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                    egress_allowlist_json, enabled, readiness, liveness, restart_count,
                    last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                    created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, NULL, NULL, NULL, ?10, ?10)
                ON CONFLICT(connector_id) DO UPDATE SET
                    kind = excluded.kind,
                    principal = excluded.principal,
                    auth_profile_ref = excluded.auth_profile_ref,
                    token_vault_ref = excluded.token_vault_ref,
                    egress_allowlist_json = excluded.egress_allowlist_json,
                    enabled = excluded.enabled,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
                "#,
                params![
                    spec.connector_id,
                    spec.kind.as_str(),
                    spec.principal,
                    spec.auth_profile_ref,
                    spec.token_vault_ref,
                    allowlist_json,
                    if spec.enabled { 1_i64 } else { 0_i64 },
                    ConnectorReadiness::Ready.as_str(),
                    ConnectorLiveness::Stopped.as_str(),
                    now_unix_ms,
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_instances(&self) -> Result<Vec<ConnectorInstanceRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement = connection.prepare(
            r#"
            SELECT connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                   egress_allowlist_json, enabled, readiness, liveness, restart_count,
                   last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                   created_at_unix_ms, updated_at_unix_ms
            FROM connector_instances
            ORDER BY connector_id ASC
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(parse_instance_row(row)?);
        }
        Ok(records)
    }

    pub fn get_instance(
        &self,
        connector_id: &str,
    ) -> Result<Option<ConnectorInstanceRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement = connection.prepare(
            r#"
            SELECT connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                   egress_allowlist_json, enabled, readiness, liveness, restart_count,
                   last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                   created_at_unix_ms, updated_at_unix_ms
            FROM connector_instances
            WHERE connector_id = ?1
            "#,
        )?;
        let mut rows = statement.query(params![connector_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(parse_instance_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn set_instance_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET enabled = ?2,
                    liveness = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    if enabled { 1_i64 } else { 0_i64 },
                    if enabled {
                        ConnectorLiveness::Running.as_str()
                    } else {
                        ConnectorLiveness::Stopped.as_str()
                    },
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn delete_instance(&self, connector_id: &str) -> Result<(), ConnectorStoreError> {
        let deleted = self.with_transaction(|transaction| {
            transaction.execute(
                "DELETE FROM inbound_dedupe WHERE connector_id = ?1",
                params![connector_id],
            )?;
            transaction
                .execute("DELETE FROM outbox WHERE connector_id = ?1", params![connector_id])?;
            let changed = transaction.execute(
                "DELETE FROM connector_instances WHERE connector_id = ?1",
                params![connector_id],
            )?;
            Ok(changed)
        })?;
        if deleted == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn set_instance_runtime_state(
        &self,
        connector_id: &str,
        readiness: ConnectorReadiness,
        liveness: ConnectorLiveness,
        last_error: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET readiness = ?2,
                    liveness = ?3,
                    last_error = ?4,
                    updated_at_unix_ms = ?5
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    readiness.as_str(),
                    liveness.as_str(),
                    last_error,
                    now_unix_ms
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn record_last_inbound(
        &self,
        connector_id: &str,
        at_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET last_inbound_unix_ms = ?2,
                    updated_at_unix_ms = ?2
                WHERE connector_id = ?1
                "#,
                params![connector_id, at_unix_ms],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn record_last_outbound(
        &self,
        connector_id: &str,
        at_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
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
                    at_unix_ms,
                    ConnectorReadiness::Ready.as_str(),
                    ConnectorLiveness::Running.as_str(),
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn increment_restart_count(
        &self,
        connector_id: &str,
        now_unix_ms: i64,
        last_error: &str,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET restart_count = restart_count + 1,
                    liveness = ?2,
                    last_error = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    ConnectorLiveness::Restarting.as_str(),
                    last_error,
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    pub fn record_inbound_dedupe_if_new(
        &self,
        connector_id: &str,
        envelope_id: &str,
        now_unix_ms: i64,
        dedupe_window_ms: i64,
    ) -> Result<bool, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            transaction.execute(
                "DELETE FROM inbound_dedupe WHERE expires_at_unix_ms <= ?1",
                params![now_unix_ms],
            )?;
            let inserted = transaction.execute(
                r#"
                INSERT OR IGNORE INTO inbound_dedupe (
                    connector_id, envelope_id, created_at_unix_ms, expires_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    connector_id,
                    envelope_id,
                    now_unix_ms,
                    now_unix_ms.saturating_add(dedupe_window_ms.max(1)),
                ],
            )?;
            Ok(inserted > 0)
        })
    }

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

    pub fn queue_depth(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorQueueDepth, ConnectorStoreError> {
        let snapshot = self.queue_snapshot(connector_id, 0)?;
        Ok(ConnectorQueueDepth {
            pending_outbox: snapshot.pending_outbox,
            dead_letters: snapshot.dead_letters,
        })
    }

    pub fn queue_snapshot(
        &self,
        connector_id: &str,
        now_unix_ms: i64,
    ) -> Result<ConnectorQueueSnapshot, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let pending_outbox: i64 = connection.query_row(
            "SELECT COUNT(*) FROM outbox WHERE connector_id = ?1 AND status = 'pending'",
            params![connector_id],
            |row| row.get(0),
        )?;
        let due_outbox: i64 = connection.query_row(
            r#"
            SELECT COUNT(*)
            FROM outbox
            WHERE connector_id = ?1
              AND status = 'pending'
              AND next_attempt_unix_ms <= ?2
              AND claim_expires_unix_ms <= ?2
            "#,
            params![connector_id, now_unix_ms],
            |row| row.get(0),
        )?;
        let claimed_outbox: i64 = connection.query_row(
            r#"
            SELECT COUNT(*)
            FROM outbox
            WHERE connector_id = ?1
              AND status = 'pending'
              AND claim_expires_unix_ms > ?2
            "#,
            params![connector_id, now_unix_ms],
            |row| row.get(0),
        )?;
        let dead_letters: i64 = connection.query_row(
            "SELECT COUNT(*) FROM dead_letters WHERE connector_id = ?1",
            params![connector_id],
            |row| row.get(0),
        )?;
        let next_attempt_unix_ms = connection.query_row(
            r#"
                SELECT MIN(next_attempt_unix_ms)
                FROM outbox
                WHERE connector_id = ?1
                  AND status = 'pending'
                "#,
            params![connector_id],
            |row| row.get::<_, Option<i64>>(0),
        )?;
        let oldest_pending_created_at_unix_ms = connection.query_row(
            r#"
                SELECT MIN(created_at_unix_ms)
                FROM outbox
                WHERE connector_id = ?1
                  AND status = 'pending'
                "#,
            params![connector_id],
            |row| row.get::<_, Option<i64>>(0),
        )?;
        let latest_dead_letter_unix_ms = connection.query_row(
            r#"
                SELECT MAX(created_at_unix_ms)
                FROM dead_letters
                WHERE connector_id = ?1
                "#,
            params![connector_id],
            |row| row.get::<_, Option<i64>>(0),
        )?;
        let queue_state = connection
            .query_row(
                r#"
                SELECT paused, pause_reason, updated_at_unix_ms
                FROM connector_queue_state
                WHERE connector_id = ?1
                "#,
                params![connector_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? != 0,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(ConnectorQueueSnapshot {
            pending_outbox: u64::try_from(pending_outbox).unwrap_or(0),
            due_outbox: u64::try_from(due_outbox).unwrap_or(0),
            claimed_outbox: u64::try_from(claimed_outbox).unwrap_or(0),
            dead_letters: u64::try_from(dead_letters).unwrap_or(0),
            next_attempt_unix_ms,
            oldest_pending_created_at_unix_ms,
            latest_dead_letter_unix_ms,
            paused: queue_state.as_ref().is_some_and(|(paused, _, _)| *paused),
            pause_reason: queue_state.as_ref().and_then(|(_, reason, _)| reason.clone()),
            pause_updated_at_unix_ms: queue_state.as_ref().map(|(_, _, updated_at)| *updated_at),
        })
    }

    pub fn set_queue_paused(
        &self,
        connector_id: &str,
        paused: bool,
        reason: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        self.with_transaction(|transaction| {
            let connector_exists: Option<i64> = transaction
                .query_row(
                    "SELECT 1 FROM connector_instances WHERE connector_id = ?1",
                    params![connector_id],
                    |row| row.get(0),
                )
                .optional()?;
            if connector_exists.is_none() {
                return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
            }

            transaction.execute(
                r#"
                INSERT INTO connector_queue_state (
                    connector_id, paused, pause_reason, updated_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(connector_id) DO UPDATE SET
                    paused = excluded.paused,
                    pause_reason = excluded.pause_reason,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
                "#,
                params![connector_id, if paused { 1_i64 } else { 0_i64 }, reason, now_unix_ms,],
            )?;
            Ok(())
        })?;
        Ok(())
    }

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
                let payload_json = dead_letter.4.clone();
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
                    payload_json,
                    i64::from(max_attempts.max(1)),
                    now_unix_ms,
                ],
            )?;
            if updated == 0 {
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
                        payload_json,
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
            let payload: String = row.get(4)?;
            records.push(DeadLetterRecord {
                dead_letter_id: row.get(0)?,
                connector_id: row.get(1)?,
                envelope_id: row.get(2)?,
                reason: row.get(3)?,
                payload: serde_json::from_str(payload.as_str())?,
                created_at_unix_ms: row.get(5)?,
            });
        }
        Ok(records)
    }

    pub fn record_event(
        &self,
        connector_id: &str,
        event_type: &str,
        level: &str,
        message: &str,
        details: Option<&Value>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let encoded_details = details.map(serde_json::to_string).transpose()?;
        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                INSERT INTO connector_events (
                    connector_id, event_type, level, message, details_json, created_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![connector_id, event_type, level, message, encoded_details, now_unix_ms,],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn list_events(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<ConnectorEventRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let mut statement = connection.prepare(
            r#"
            SELECT event_id, connector_id, event_type, level, message, details_json, created_at_unix_ms
            FROM connector_events
            WHERE connector_id = ?1
            ORDER BY event_id DESC
            LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![connector_id, limit_i64])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            let details_json: Option<String> = row.get(5)?;
            events.push(ConnectorEventRecord {
                event_id: row.get(0)?,
                connector_id: row.get(1)?,
                event_type: row.get(2)?,
                level: row.get(3)?,
                message: row.get(4)?,
                details: details_json
                    .map(|value| serde_json::from_str(value.as_str()))
                    .transpose()?,
                created_at_unix_ms: row.get(6)?,
            });
        }
        Ok(events)
    }

    fn initialize_schema(&self) -> Result<(), ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS connector_instances (
                connector_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                principal TEXT NOT NULL,
                auth_profile_ref TEXT,
                token_vault_ref TEXT,
                egress_allowlist_json TEXT NOT NULL,
                enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                readiness TEXT NOT NULL,
                liveness TEXT NOT NULL,
                restart_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_inbound_unix_ms INTEGER,
                last_outbound_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inbound_dedupe (
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(connector_id, envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_inbound_dedupe_expiry
                ON inbound_dedupe(expires_at_unix_ms);

            CREATE TABLE IF NOT EXISTS outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                next_attempt_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'delivered', 'dead')),
                native_message_id TEXT,
                last_error TEXT,
                claim_token TEXT,
                claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_outbox_pending
                ON outbox(status, next_attempt_unix_ms, outbox_id);

            CREATE TABLE IF NOT EXISTS dead_letters (
                dead_letter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_letters_connector
                ON dead_letters(connector_id, dead_letter_id DESC);

            CREATE TABLE IF NOT EXISTS connector_queue_state (
                connector_id TEXT PRIMARY KEY,
                paused INTEGER NOT NULL CHECK(paused IN (0, 1)),
                pause_reason TEXT,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(connector_id) REFERENCES connector_instances(connector_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS connector_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                details_json TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_connector_events_connector
                ON connector_events(connector_id, event_id DESC);
            "#,
        )?;
        ensure_outbox_claim_columns(&connection)?;
        connection.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_outbox_claim_reclaim
                ON outbox(status, claim_expires_unix_ms, next_attempt_unix_ms, outbox_id);
            "#,
        )?;
        Ok(())
    }

    fn with_transaction<T, F>(&self, callback: F) -> Result<T, ConnectorStoreError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, ConnectorStoreError>,
    {
        let mut connection =
            self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let transaction = connection.transaction()?;
        let output = callback(&transaction)?;
        transaction.commit()?;
        Ok(output)
    }
}

fn parse_instance_row(row: &Row<'_>) -> Result<ConnectorInstanceRecord, ConnectorStoreError> {
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

fn parse_outbox_row(row: &Row<'_>) -> Result<OutboxEntryRecord, ConnectorStoreError> {
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

fn next_outbox_claim_token(now_unix_ms: i64) -> String {
    let sequence = OUTBOX_CLAIM_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("claim-{now_unix_ms}-{sequence}")
}

fn ensure_outbox_claim_columns(connection: &Connection) -> Result<(), ConnectorStoreError> {
    if !outbox_column_exists(connection, "claim_token")? {
        connection.execute("ALTER TABLE outbox ADD COLUMN claim_token TEXT", [])?;
    }
    if !outbox_column_exists(connection, "claim_expires_unix_ms")? {
        connection.execute(
            "ALTER TABLE outbox ADD COLUMN claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

fn outbox_column_exists(
    connection: &Connection,
    column_name: &str,
) -> Result<bool, ConnectorStoreError> {
    let mut statement = connection.prepare("PRAGMA table_info(outbox)")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name.eq_ignore_ascii_case(column_name) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::protocol::{ConnectorInstanceSpec, ConnectorKind, OutboundMessageRequest};

    use super::{ConnectorStore, ConnectorStoreError};

    fn open_store() -> (TempDir, ConnectorStore) {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let db_path = tempdir.path().join("connectors.sqlite3");
        let store = ConnectorStore::open(db_path).expect("connector store should initialize");
        (tempdir, store)
    }

    fn sample_spec() -> ConnectorInstanceSpec {
        sample_spec_with_connector("echo:default")
    }

    fn sample_spec_with_connector(connector_id: &str) -> ConnectorInstanceSpec {
        ConnectorInstanceSpec {
            connector_id: connector_id.to_owned(),
            kind: ConnectorKind::Echo,
            principal: format!("channel:{connector_id}"),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
        }
    }

    fn sample_outbound(envelope_id: &str) -> OutboundMessageRequest {
        sample_outbound_for_connector("echo:default", envelope_id)
    }

    fn sample_outbound_for_connector(
        connector_id: &str,
        envelope_id: &str,
    ) -> OutboundMessageRequest {
        OutboundMessageRequest {
            envelope_id: envelope_id.to_owned(),
            connector_id: connector_id.to_owned(),
            conversation_id: "conv-1".to_owned(),
            reply_thread_id: None,
            in_reply_to_message_id: None,
            text: "hello".to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction: None,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: 16_384,
        }
    }

    #[test]
    fn dedupe_accepts_first_event_and_rejects_duplicate_until_expiry() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");

        let first = store
            .record_inbound_dedupe_if_new("echo:default", "env-1", 1_000, 10_000)
            .expect("first dedupe write should succeed");
        let second = store
            .record_inbound_dedupe_if_new("echo:default", "env-1", 1_500, 10_000)
            .expect("duplicate dedupe write should succeed");
        let after_expiry = store
            .record_inbound_dedupe_if_new("echo:default", "env-1", 12_000, 10_000)
            .expect("expired dedupe key should be re-insertable");

        assert!(first, "first inbound should be accepted");
        assert!(!second, "duplicate inbound should be rejected within dedupe window");
        assert!(after_expiry, "dedupe key should expire after configured window");
    }

    #[test]
    fn dedupe_is_scoped_per_connector_instance() {
        let (_tempdir, store) = open_store();
        store
            .upsert_instance(&sample_spec_with_connector("echo:default"), 1_000)
            .expect("default instance should be created");
        store
            .upsert_instance(&sample_spec_with_connector("echo:ops"), 1_000)
            .expect("ops instance should be created");

        let default_first = store
            .record_inbound_dedupe_if_new("echo:default", "env-1", 1_000, 10_000)
            .expect("default first dedupe write should succeed");
        let ops_first = store
            .record_inbound_dedupe_if_new("echo:ops", "env-1", 1_000, 10_000)
            .expect("ops first dedupe write should succeed");
        let default_duplicate = store
            .record_inbound_dedupe_if_new("echo:default", "env-1", 1_100, 10_000)
            .expect("default duplicate dedupe write should succeed");

        assert!(default_first, "default connector should accept first envelope");
        assert!(ops_first, "same envelope id should be accepted for a different connector");
        assert!(
            !default_duplicate,
            "duplicate envelope should still be rejected within dedupe window for the same connector"
        );
    }

    #[test]
    fn outbox_enforces_idempotent_unique_envelope_per_connector() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        let request = sample_outbound("env-1:0");

        let created = store
            .enqueue_outbox_if_absent(&request, 5, 1_000)
            .expect("first outbox enqueue should succeed");
        let duplicate = store
            .enqueue_outbox_if_absent(&request, 5, 1_000)
            .expect("duplicate outbox enqueue should succeed");

        assert!(created.created, "first enqueue must create a record");
        assert!(!duplicate.created, "duplicate envelope must be ignored");
    }

    #[test]
    fn outbox_allows_same_envelope_for_different_connectors() {
        let (_tempdir, store) = open_store();
        store
            .upsert_instance(&sample_spec_with_connector("echo:default"), 1_000)
            .expect("default instance should be created");
        store
            .upsert_instance(&sample_spec_with_connector("echo:ops"), 1_000)
            .expect("ops instance should be created");

        let default_request = sample_outbound_for_connector("echo:default", "env-1:0");
        let ops_request = sample_outbound_for_connector("echo:ops", "env-1:0");

        let default_outcome = store
            .enqueue_outbox_if_absent(&default_request, 5, 1_000)
            .expect("default outbox enqueue should succeed");
        let ops_outcome = store
            .enqueue_outbox_if_absent(&ops_request, 5, 1_000)
            .expect("ops outbox enqueue should succeed");

        assert!(default_outcome.created, "default connector should enqueue envelope");
        assert!(
            ops_outcome.created,
            "same envelope id should still enqueue for a different connector"
        );
    }

    #[test]
    fn delete_instance_removes_runtime_state_but_keeps_audit_records() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        store
            .record_inbound_dedupe_if_new("echo:default", "env-delete", 1_000, 10_000)
            .expect("dedupe write should succeed");
        store
            .record_event("echo:default", "connector.test", "info", "test event", None, 1_000)
            .expect("event should be recorded");
        let request = sample_outbound("env-delete");
        store.enqueue_outbox_if_absent(&request, 5, 1_000).expect("outbox enqueue should succeed");

        store.delete_instance("echo:default").expect("delete should succeed");

        assert!(
            store.get_instance("echo:default").expect("instance lookup should succeed").is_none(),
            "connector instance should be removed"
        );
        assert!(
            store
                .load_due_outbox(1_000, 10, Some("echo:default"), false)
                .expect("outbox lookup should succeed")
                .is_empty(),
            "outbox records should be removed with the connector"
        );
        assert_eq!(
            store.list_events("echo:default", 10).expect("event lookup should succeed").len(),
            1,
            "audit events should remain after connector removal"
        );
    }

    #[test]
    fn outbox_retry_and_dead_letter_flow_persists_state() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        let request = sample_outbound("env-2:0");
        store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

        let due = store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("due outbox query");
        assert_eq!(due.len(), 1);
        let outbox_id = due[0].outbox_id;
        let claim_token = due[0].claim_token.clone();
        store
            .schedule_outbox_retry(outbox_id, claim_token.as_str(), 1, "transient", 2_000)
            .expect("retry should be scheduled");
        let due_after_backoff = store
            .load_due_outbox(1_500, 10, Some("echo:default"), false)
            .expect("outbox due query should succeed");
        assert!(due_after_backoff.is_empty(), "entry should not be due before retry timestamp");
        let due_for_dead_letter = store
            .load_due_outbox(2_100, 10, Some("echo:default"), false)
            .expect("due outbox query should succeed");
        assert_eq!(due_for_dead_letter.len(), 1);
        let dead_letter_claim = due_for_dead_letter[0].claim_token.clone();
        store
            .move_outbox_to_dead_letter(outbox_id, dead_letter_claim.as_str(), "permanent", 2_100)
            .expect("dead letter move should succeed");
        let dead_letters =
            store.list_dead_letters("echo:default", 10).expect("dead letters should be queryable");
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].reason, "permanent");
    }

    #[test]
    fn dead_letter_can_be_replayed_back_into_pending_outbox() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        let request = sample_outbound("env-replay:0");
        store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

        let due = store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("due outbox query should succeed");
        let claimed = due.first().expect("entry should be claimed");
        store
            .move_outbox_to_dead_letter(
                claimed.outbox_id,
                claimed.claim_token.as_str(),
                "permanent",
                1_100,
            )
            .expect("dead letter move should succeed");
        let dead_letter = store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters should be queryable")
            .into_iter()
            .next()
            .expect("dead letter should exist");

        let replayed = store
            .replay_dead_letter("echo:default", dead_letter.dead_letter_id, 5, 2_000)
            .expect("dead letter replay should succeed");
        assert_eq!(replayed.envelope_id, "env-replay:0");

        let dead_letters_after = store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters after replay should be queryable");
        assert!(dead_letters_after.is_empty(), "replayed dead letter should be removed");

        let replay_due = store
            .load_due_outbox(2_000, 10, Some("echo:default"), false)
            .expect("replayed outbox entry should be pending");
        assert_eq!(replay_due.len(), 1, "replayed entry should be ready for immediate retry");
        assert_eq!(replay_due[0].attempts, 0, "replayed outbox should reset attempts");
        assert_eq!(replay_due[0].envelope_id, "env-replay:0");
    }

    #[test]
    fn dead_letter_can_be_discarded_without_requeueing() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        let request = sample_outbound("env-discard:0");
        store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

        let due = store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("due outbox query should succeed");
        let claimed = due.first().expect("entry should be claimed");
        store
            .move_outbox_to_dead_letter(
                claimed.outbox_id,
                claimed.claim_token.as_str(),
                "permanent",
                1_100,
            )
            .expect("dead letter move should succeed");
        let dead_letter = store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters should be queryable")
            .into_iter()
            .next()
            .expect("dead letter should exist");

        let discarded = store
            .discard_dead_letter("echo:default", dead_letter.dead_letter_id)
            .expect("dead letter discard should succeed");
        assert_eq!(discarded.envelope_id, "env-discard:0");
        assert!(
            store
                .list_dead_letters("echo:default", 10)
                .expect("dead letters should remain queryable")
                .is_empty(),
            "discarded dead letter should be removed from listing"
        );
        assert!(
            store
                .load_due_outbox(2_000, 10, Some("echo:default"), false)
                .expect("outbox query should remain valid")
                .is_empty(),
            "discard should not recreate a pending outbox entry"
        );
    }

    #[test]
    fn queue_snapshot_reports_pending_due_claimed_and_dead_letter_counts() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        store
            .enqueue_outbox_if_absent(&sample_outbound("env-a:0"), 2, 1_000)
            .expect("first outbox enqueue should succeed");
        store
            .enqueue_outbox_if_absent(&sample_outbound("env-b:0"), 2, 2_000)
            .expect("second outbox enqueue should succeed");

        let claimed = store
            .load_due_outbox(2_000, 1, Some("echo:default"), false)
            .expect("due outbox query should succeed");
        assert_eq!(claimed.len(), 1, "one entry should be claimed for in-flight delivery");
        let snapshot =
            store.queue_snapshot("echo:default", 2_000).expect("queue snapshot should succeed");
        assert_eq!(snapshot.pending_outbox, 2);
        assert_eq!(snapshot.claimed_outbox, 1);
        assert_eq!(snapshot.due_outbox, 1);
        assert_eq!(snapshot.dead_letters, 0);
        assert_eq!(snapshot.next_attempt_unix_ms, Some(1_000));
        assert_eq!(snapshot.oldest_pending_created_at_unix_ms, Some(1_000));

        store
            .move_outbox_to_dead_letter(
                claimed[0].outbox_id,
                claimed[0].claim_token.as_str(),
                "permanent",
                2_100,
            )
            .expect("dead letter move should succeed");
        let after_dead_letter = store
            .queue_snapshot("echo:default", 2_100)
            .expect("queue snapshot after dead letter should succeed");
        assert_eq!(after_dead_letter.pending_outbox, 1);
        assert_eq!(after_dead_letter.dead_letters, 1);
        assert_eq!(after_dead_letter.latest_dead_letter_unix_ms, Some(2_100));
    }

    #[test]
    fn outbox_due_claims_are_exclusive_between_loads() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        let request = sample_outbound("env-claim-exclusive");
        store.enqueue_outbox_if_absent(&request, 2, 1_000).expect("outbox enqueue should succeed");

        let first = store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("first load should claim");
        assert_eq!(first.len(), 1, "first due load should claim the entry");
        let second = store
            .load_due_outbox(1_000, 10, Some("echo:default"), false)
            .expect("second load should succeed");
        assert!(
            second.is_empty(),
            "second due load should not re-claim entry while lease is active"
        );
    }

    #[test]
    fn mark_outbox_delivered_reports_missing_outbox() {
        let (_tempdir, store) = open_store();
        let error = store
            .mark_outbox_delivered(9_999, "claim-missing", "native-1", 1_000)
            .expect_err("unknown outbox id should be reported");
        assert!(
            matches!(error, ConnectorStoreError::OutboxNotFound(9_999)),
            "expected OutboxNotFound for missing outbox id"
        );
    }

    #[test]
    fn schedule_outbox_retry_reports_missing_outbox() {
        let (_tempdir, store) = open_store();
        let error = store
            .schedule_outbox_retry(9_998, "claim-missing", 1, "retry", 2_000)
            .expect_err("unknown outbox id should be reported");
        assert!(
            matches!(error, ConnectorStoreError::OutboxNotFound(9_998)),
            "expected OutboxNotFound for missing outbox id"
        );
    }

    #[test]
    fn move_outbox_to_dead_letter_reports_missing_outbox() {
        let (_tempdir, store) = open_store();
        let error = store
            .move_outbox_to_dead_letter(9_997, "claim-missing", "dead", 1_000)
            .expect_err("unknown outbox id should be reported");
        assert!(
            matches!(error, ConnectorStoreError::OutboxNotFound(9_997)),
            "expected OutboxNotFound for missing outbox id"
        );
    }

    #[test]
    fn queue_snapshot_reports_pause_state_and_due_counts() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        store
            .enqueue_outbox_if_absent(&sample_outbound("env-1"), 3, 1_000)
            .expect("outbox insert should succeed");
        let claimed = store
            .load_due_outbox(1_000, 1, Some("echo:default"), false)
            .expect("claim should succeed");
        let claim_token = claimed[0].claim_token.clone();
        store
            .schedule_outbox_retry(claimed[0].outbox_id, claim_token.as_str(), 1, "retry", 5_000)
            .expect("retry scheduling should succeed");
        store
            .set_queue_paused("echo:default", true, Some("operator_pause"), 2_000)
            .expect("queue pause should persist");

        let snapshot =
            store.queue_snapshot("echo:default", 2_000).expect("queue snapshot should resolve");

        assert_eq!(snapshot.pending_outbox, 1);
        assert_eq!(snapshot.due_outbox, 0);
        assert_eq!(snapshot.claimed_outbox, 0);
        assert!(snapshot.paused, "queue snapshot should reflect paused state");
        assert_eq!(snapshot.pause_reason.as_deref(), Some("operator_pause"));
        assert_eq!(snapshot.pause_updated_at_unix_ms, Some(2_000));
        assert_eq!(snapshot.next_attempt_unix_ms, Some(5_000));
    }

    #[test]
    fn replay_and_discard_dead_letter_update_queue_state() {
        let (_tempdir, store) = open_store();
        store.upsert_instance(&sample_spec(), 1_000).expect("instance should be created");
        store
            .enqueue_outbox_if_absent(&sample_outbound("env-1"), 3, 1_000)
            .expect("outbox insert should succeed");
        let due = store
            .load_due_outbox(1_000, 1, Some("echo:default"), false)
            .expect("outbox should be claimable");
        let claim_token = due[0].claim_token.clone();
        store
            .move_outbox_to_dead_letter(due[0].outbox_id, claim_token.as_str(), "permanent", 1_500)
            .expect("dead-letter move should succeed");

        let dead_letters =
            store.list_dead_letters("echo:default", 10).expect("dead letters should be listed");
        assert_eq!(dead_letters.len(), 1);

        let replayed = store
            .replay_dead_letter("echo:default", dead_letters[0].dead_letter_id, 5, 2_000)
            .expect("dead letter should replay");
        assert_eq!(replayed.envelope_id, "env-1");
        let snapshot = store
            .queue_snapshot("echo:default", 2_000)
            .expect("queue snapshot should resolve after replay");
        assert_eq!(snapshot.pending_outbox, 1);
        assert_eq!(snapshot.dead_letters, 0);
        assert_eq!(snapshot.due_outbox, 1);

        let due_again = store
            .load_due_outbox(2_000, 1, Some("echo:default"), false)
            .expect("replayed outbox should be claimable");
        let claim_token = due_again[0].claim_token.clone();
        store
            .move_outbox_to_dead_letter(
                due_again[0].outbox_id,
                claim_token.as_str(),
                "retry_exhausted",
                2_100,
            )
            .expect("dead-letter move should succeed");
        let dead_letter_id = store
            .list_dead_letters("echo:default", 10)
            .expect("dead letters should be listed after second failure")[0]
            .dead_letter_id;
        let discarded = store
            .discard_dead_letter("echo:default", dead_letter_id)
            .expect("dead letter should discard");
        assert_eq!(discarded.reason, "retry_exhausted");
        assert!(
            store
                .list_dead_letters("echo:default", 10)
                .expect("dead letters should reload")
                .is_empty(),
            "discard should remove dead letter from operator queue"
        );
    }
}
