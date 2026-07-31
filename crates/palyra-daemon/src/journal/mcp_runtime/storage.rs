//! SQLite adapter for durable MCP runtime records.

use async_trait::async_trait;
use rusqlite::{params, OptionalExtension, Row, TransactionBehavior};

use crate::application::mcp_runtime::{
    McpRuntimeEventV2, McpRuntimeLifecycleState, McpRuntimeRecordStore, McpRuntimeStoreError,
    McpServerRecordV2, McpSessionTransportKind, MCP_SERVER_RECORD_SCHEMA_VERSION,
};

use super::super::JournalStore;

const RECORD_COLUMNS: &str = "
    schema_version, server_id, transport, lifecycle, runtime_generation,
    catalog_epoch, catalog_digest, credential_scope_id, trust_profile_id,
    consecutive_failures, next_retry_at_unix_ms, quarantine_reason_code,
    revision, created_at_unix_ms, updated_at_unix_ms
";

#[async_trait]
impl McpRuntimeRecordStore for JournalStore {
    async fn load_all(&self) -> Result<Vec<McpServerRecordV2>, McpRuntimeStoreError> {
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let query =
            format!("SELECT {RECORD_COLUMNS} FROM mcp_server_records_v2 ORDER BY server_id");
        let mut statement =
            guard.prepare(query.as_str()).map_err(|_| unavailable("query_prepare_failed"))?;
        let rows = statement
            .query_map([], RawMcpServerRecord::from_row)
            .map_err(|_| unavailable("query_failed"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| corrupt("record_decode_failed"))?;

        rows.into_iter().map(RawMcpServerRecord::into_record).collect()
    }

    async fn insert_configured(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<(), McpRuntimeStoreError> {
        validate_initial_record(record)?;
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let result = guard.execute(
            r#"
                INSERT INTO mcp_server_records_v2 (
                    schema_version, server_id, transport, lifecycle,
                    runtime_generation, catalog_epoch, catalog_digest,
                    credential_scope_id, trust_profile_id, consecutive_failures,
                    next_retry_at_unix_ms, quarantine_reason_code, revision,
                    created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15
                )
            "#,
            (
                record.schema_version,
                record.server_id.as_str(),
                transport_str(record.transport),
                lifecycle_str(record.lifecycle),
                to_sqlite(record.runtime_generation, "runtime_generation")?,
                to_sqlite(record.catalog_epoch, "catalog_epoch")?,
                record.catalog_digest.as_deref(),
                record.credential_scope_id.as_deref(),
                record.trust_profile_id.as_str(),
                i64::from(record.consecutive_failures),
                record.next_retry_at_unix_ms,
                record.quarantine_reason_code.as_deref(),
                to_sqlite(record.revision, "revision")?,
                record.created_at_unix_ms,
                record.updated_at_unix_ms,
            ),
        );
        match result {
            Ok(1) => Ok(()),
            Ok(_) => Err(unavailable("insert_count_invalid")),
            Err(error)
                if error
                    .sqlite_error()
                    .is_some_and(|code| code.code == rusqlite::ErrorCode::ConstraintViolation) =>
            {
                let actual = load_revision(&guard, &record.server_id)?;
                Err(McpRuntimeStoreError::RevisionConflict { expected: 0, actual })
            }
            Err(_) => Err(unavailable("insert_failed")),
        }
    }

    async fn persist_transition(
        &self,
        expected_revision: u64,
        record: &McpServerRecordV2,
        event: &McpRuntimeEventV2,
    ) -> Result<(), McpRuntimeStoreError> {
        record.validate().map_err(|_| corrupt("record_invalid"))?;
        if expected_revision.checked_add(1) != Some(record.revision) {
            return Err(corrupt("record_revision_not_adjacent"));
        }

        let mut guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| unavailable("transaction_begin_failed"))?;
        let current = load_record(&transaction, &record.server_id)?.ok_or(
            McpRuntimeStoreError::RevisionConflict { expected: expected_revision, actual: None },
        )?;
        if current.revision != expected_revision {
            return Err(McpRuntimeStoreError::RevisionConflict {
                expected: expected_revision,
                actual: Some(current.revision),
            });
        }
        let expected_event =
            McpRuntimeEventV2::from_transition(&current, record, event.reason_code.clone())
                .map_err(|_| corrupt("event_transition_invalid"))?;
        if &expected_event != event {
            return Err(corrupt("event_projection_mismatch"));
        }

        let changed = transaction
            .execute(
                r#"
                    UPDATE mcp_server_records_v2
                    SET schema_version = ?1,
                        transport = ?3,
                        lifecycle = ?4,
                        runtime_generation = ?5,
                        catalog_epoch = ?6,
                        catalog_digest = ?7,
                        credential_scope_id = ?8,
                        trust_profile_id = ?9,
                        consecutive_failures = ?10,
                        next_retry_at_unix_ms = ?11,
                        quarantine_reason_code = ?12,
                        revision = ?13,
                        created_at_unix_ms = ?14,
                        updated_at_unix_ms = ?15
                    WHERE server_id = ?2 AND revision = ?16
                "#,
                (
                    record.schema_version,
                    record.server_id.as_str(),
                    transport_str(record.transport),
                    lifecycle_str(record.lifecycle),
                    to_sqlite(record.runtime_generation, "runtime_generation")?,
                    to_sqlite(record.catalog_epoch, "catalog_epoch")?,
                    record.catalog_digest.as_deref(),
                    record.credential_scope_id.as_deref(),
                    record.trust_profile_id.as_str(),
                    i64::from(record.consecutive_failures),
                    record.next_retry_at_unix_ms,
                    record.quarantine_reason_code.as_deref(),
                    to_sqlite(record.revision, "revision")?,
                    record.created_at_unix_ms,
                    record.updated_at_unix_ms,
                    to_sqlite(expected_revision, "expected_revision")?,
                ),
            )
            .map_err(|_| unavailable("record_update_failed"))?;
        if changed != 1 {
            let actual = load_revision(&transaction, &record.server_id)?;
            return Err(McpRuntimeStoreError::RevisionConflict {
                expected: expected_revision,
                actual,
            });
        }
        if record.catalog_epoch > current.catalog_epoch {
            let catalog_digest = record
                .catalog_digest
                .as_deref()
                .ok_or_else(|| corrupt("catalog_epoch_digest_missing"))?;
            transaction
                .execute(
                    r#"
                        INSERT INTO mcp_catalog_epoch_evidence_v1 (
                            server_id, runtime_generation, catalog_epoch,
                            catalog_digest, record_revision, reason_code,
                            created_at_unix_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                    "#,
                    params![
                        record.server_id,
                        to_sqlite(record.runtime_generation, "catalog.runtime_generation")?,
                        to_sqlite(record.catalog_epoch, "catalog.catalog_epoch")?,
                        catalog_digest,
                        to_sqlite(record.revision, "catalog.record_revision")?,
                        event.reason_code,
                        event.occurred_at_unix_ms,
                    ],
                )
                .map_err(|_| unavailable("catalog_evidence_insert_failed"))?;
        }
        transaction
            .execute(
                r#"
                    INSERT INTO mcp_connection_lifecycle_events_v2 (
                        server_id, previous_revision, revision,
                        previous_lifecycle, lifecycle, runtime_generation,
                        catalog_epoch, reason_code, occurred_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                params![
                    event.server_id,
                    to_sqlite(event.previous_revision, "event.previous_revision")?,
                    to_sqlite(event.revision, "event.revision")?,
                    lifecycle_str(event.previous_lifecycle),
                    lifecycle_str(event.lifecycle),
                    to_sqlite(event.runtime_generation, "event.runtime_generation")?,
                    to_sqlite(event.catalog_epoch, "event.catalog_epoch")?,
                    event.reason_code,
                    event.occurred_at_unix_ms,
                ],
            )
            .map_err(|_| unavailable("event_insert_failed"))?;
        transaction.commit().map_err(|_| unavailable("transaction_commit_failed"))
    }
}

fn validate_initial_record(record: &McpServerRecordV2) -> Result<(), McpRuntimeStoreError> {
    record.validate().map_err(|_| corrupt("record_invalid"))?;
    if record.lifecycle != McpRuntimeLifecycleState::Configured
        || record.runtime_generation != 0
        || record.catalog_epoch != 0
        || record.catalog_digest.is_some()
        || record.consecutive_failures != 0
        || record.next_retry_at_unix_ms.is_some()
        || record.quarantine_reason_code.is_some()
        || record.revision != 0
    {
        return Err(corrupt("initial_record_invalid"));
    }
    Ok(())
}

fn load_record(
    connection: &rusqlite::Connection,
    server_id: &str,
) -> Result<Option<McpServerRecordV2>, McpRuntimeStoreError> {
    let query = format!("SELECT {RECORD_COLUMNS} FROM mcp_server_records_v2 WHERE server_id = ?1");
    let raw = connection
        .query_row(query.as_str(), params![server_id], RawMcpServerRecord::from_row)
        .optional()
        .map_err(|_| unavailable("record_load_failed"))?;
    raw.map(RawMcpServerRecord::into_record).transpose()
}

fn load_revision(
    connection: &rusqlite::Connection,
    server_id: &str,
) -> Result<Option<u64>, McpRuntimeStoreError> {
    let raw = connection
        .query_row(
            "SELECT revision FROM mcp_server_records_v2 WHERE server_id = ?1",
            params![server_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|_| unavailable("revision_load_failed"))?;
    raw.map(|value| from_sqlite(value, "revision")).transpose()
}

struct RawMcpServerRecord {
    schema_version: i64,
    server_id: String,
    transport: String,
    lifecycle: String,
    runtime_generation: i64,
    catalog_epoch: i64,
    catalog_digest: Option<String>,
    credential_scope_id: Option<String>,
    trust_profile_id: String,
    consecutive_failures: i64,
    next_retry_at_unix_ms: Option<i64>,
    quarantine_reason_code: Option<String>,
    revision: i64,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
}

impl RawMcpServerRecord {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            schema_version: row.get(0)?,
            server_id: row.get(1)?,
            transport: row.get(2)?,
            lifecycle: row.get(3)?,
            runtime_generation: row.get(4)?,
            catalog_epoch: row.get(5)?,
            catalog_digest: row.get(6)?,
            credential_scope_id: row.get(7)?,
            trust_profile_id: row.get(8)?,
            consecutive_failures: row.get(9)?,
            next_retry_at_unix_ms: row.get(10)?,
            quarantine_reason_code: row.get(11)?,
            revision: row.get(12)?,
            created_at_unix_ms: row.get(13)?,
            updated_at_unix_ms: row.get(14)?,
        })
    }

    fn into_record(self) -> Result<McpServerRecordV2, McpRuntimeStoreError> {
        let schema_version =
            u32::try_from(self.schema_version).map_err(|_| corrupt("schema_version_invalid"))?;
        if schema_version != MCP_SERVER_RECORD_SCHEMA_VERSION {
            return Err(corrupt("schema_version_unsupported"));
        }
        let consecutive_failures = u32::try_from(self.consecutive_failures)
            .map_err(|_| corrupt("consecutive_failures_invalid"))?;
        let record = McpServerRecordV2 {
            schema_version,
            server_id: self.server_id,
            transport: parse_transport(&self.transport)?,
            lifecycle: parse_lifecycle(&self.lifecycle)?,
            runtime_generation: from_sqlite(self.runtime_generation, "runtime_generation")?,
            catalog_epoch: from_sqlite(self.catalog_epoch, "catalog_epoch")?,
            catalog_digest: self.catalog_digest,
            credential_scope_id: self.credential_scope_id,
            trust_profile_id: self.trust_profile_id,
            consecutive_failures,
            next_retry_at_unix_ms: self.next_retry_at_unix_ms,
            quarantine_reason_code: self.quarantine_reason_code,
            revision: from_sqlite(self.revision, "revision")?,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
        };
        record.validate().map_err(|_| corrupt("record_invariant_invalid"))?;
        Ok(record)
    }
}

fn transport_str(transport: McpSessionTransportKind) -> &'static str {
    match transport {
        McpSessionTransportKind::Stdio => "stdio",
        McpSessionTransportKind::StreamableHttp => "streamable_http",
        McpSessionTransportKind::ServerSentEvents => "server_sent_events",
    }
}

fn parse_transport(value: &str) -> Result<McpSessionTransportKind, McpRuntimeStoreError> {
    match value {
        "stdio" => Ok(McpSessionTransportKind::Stdio),
        "streamable_http" => Ok(McpSessionTransportKind::StreamableHttp),
        "server_sent_events" => Ok(McpSessionTransportKind::ServerSentEvents),
        _ => Err(corrupt("transport_unknown")),
    }
}

fn lifecycle_str(lifecycle: McpRuntimeLifecycleState) -> &'static str {
    match lifecycle {
        McpRuntimeLifecycleState::Configured => "configured",
        McpRuntimeLifecycleState::Handshaking => "handshaking",
        McpRuntimeLifecycleState::Ready => "ready",
        McpRuntimeLifecycleState::Reconnecting => "reconnecting",
        McpRuntimeLifecycleState::Stopping => "stopping",
        McpRuntimeLifecycleState::Stopped => "stopped",
        McpRuntimeLifecycleState::Quarantined => "quarantined",
        McpRuntimeLifecycleState::Disabled => "disabled",
    }
}

fn parse_lifecycle(value: &str) -> Result<McpRuntimeLifecycleState, McpRuntimeStoreError> {
    match value {
        "configured" => Ok(McpRuntimeLifecycleState::Configured),
        "handshaking" => Ok(McpRuntimeLifecycleState::Handshaking),
        "ready" => Ok(McpRuntimeLifecycleState::Ready),
        "reconnecting" => Ok(McpRuntimeLifecycleState::Reconnecting),
        "stopping" => Ok(McpRuntimeLifecycleState::Stopping),
        "stopped" => Ok(McpRuntimeLifecycleState::Stopped),
        "quarantined" => Ok(McpRuntimeLifecycleState::Quarantined),
        "disabled" => Ok(McpRuntimeLifecycleState::Disabled),
        _ => Err(corrupt("lifecycle_unknown")),
    }
}

fn to_sqlite(value: u64, field: &'static str) -> Result<i64, McpRuntimeStoreError> {
    i64::try_from(value).map_err(|_| corrupt(&format!("{field}_out_of_range")))
}

fn from_sqlite(value: i64, field: &'static str) -> Result<u64, McpRuntimeStoreError> {
    u64::try_from(value).map_err(|_| corrupt(&format!("{field}_negative")))
}

fn corrupt(reason: &str) -> McpRuntimeStoreError {
    McpRuntimeStoreError::Corrupt { reason_code: format!("mcp.runtime.store.{reason}") }
}

fn unavailable(reason: &str) -> McpRuntimeStoreError {
    McpRuntimeStoreError::Unavailable { reason_code: format!("mcp.runtime.store.{reason}") }
}
