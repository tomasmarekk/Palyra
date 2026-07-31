//! SQLite adapter for trusted MCP descriptors and conformance reports.

use async_trait::async_trait;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::application::mcp_runtime::{
    McpConformanceReportV1, McpSecurityEvidenceStore, McpSecurityEvidenceStoreError,
    McpSessionTransportKind, McpTrustedToolActivationState, McpTrustedToolRecordV1,
};

use super::super::JournalStore;

#[async_trait]
impl McpSecurityEvidenceStore for JournalStore {
    async fn load_trusted_tool(
        &self,
        server_id: &str,
        tool_name: &str,
    ) -> Result<Option<McpTrustedToolRecordV1>, McpSecurityEvidenceStoreError> {
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        load_trusted_tool(&guard, server_id, tool_name)
    }

    async fn persist_trusted_tool(
        &self,
        expected_revision: Option<u64>,
        record: &McpTrustedToolRecordV1,
    ) -> Result<(), McpSecurityEvidenceStoreError> {
        record.validate().map_err(|_| corrupt("trusted_tool_invalid"))?;
        let expected_next = expected_revision.map_or(Some(0), |revision| revision.checked_add(1));
        if expected_next != Some(record.revision) {
            return Err(corrupt("trusted_tool_revision_not_adjacent"));
        }
        let descriptor_json = serde_json::to_string(&record.descriptor)
            .map_err(|_| corrupt("trusted_tool_descriptor_encode_failed"))?;
        let mut guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let transaction = guard
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| unavailable("transaction_begin_failed"))?;
        let current = load_trusted_tool(&transaction, &record.server_id, &record.tool_name)?;
        let actual_revision = current.as_ref().map(|current| current.revision);
        if actual_revision != expected_revision {
            return Err(McpSecurityEvidenceStoreError::RevisionConflict {
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        let changed = if let Some(expected_revision) = expected_revision {
            transaction
                .execute(
                    r#"
                        UPDATE mcp_trusted_tool_heads_v1
                        SET schema_version = ?3,
                            runtime_generation = ?4,
                            catalog_epoch = ?5,
                            descriptor_json = ?6,
                            descriptor_sha256 = ?7,
                            verified_issuer_id = ?8,
                            activation = ?9,
                            approved_descriptor_sha256 = ?10,
                            revision = ?11,
                            reason_code = ?12,
                            created_at_unix_ms = ?13,
                            updated_at_unix_ms = ?14
                        WHERE server_id = ?1 AND tool_name = ?2 AND revision = ?15
                    "#,
                    params![
                        record.server_id,
                        record.tool_name,
                        record.schema_version,
                        to_sqlite(record.runtime_generation, "runtime_generation")?,
                        to_sqlite(record.catalog_epoch, "catalog_epoch")?,
                        descriptor_json,
                        record.descriptor_sha256,
                        record.verified_issuer_id,
                        record.activation.as_str(),
                        record.approved_descriptor_sha256,
                        to_sqlite(record.revision, "revision")?,
                        record.reason_code,
                        record.created_at_unix_ms,
                        record.updated_at_unix_ms,
                        to_sqlite(expected_revision, "expected_revision")?,
                    ],
                )
                .map_err(|_| unavailable("trusted_tool_update_failed"))?
        } else {
            transaction
                .execute(
                    r#"
                        INSERT INTO mcp_trusted_tool_heads_v1 (
                            server_id, tool_name, schema_version,
                            runtime_generation, catalog_epoch, descriptor_json,
                            descriptor_sha256, verified_issuer_id, activation,
                            approved_descriptor_sha256, revision, reason_code,
                            created_at_unix_ms, updated_at_unix_ms
                        ) VALUES (
                            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                            ?12, ?13, ?14
                        )
                    "#,
                    params![
                        record.server_id,
                        record.tool_name,
                        record.schema_version,
                        to_sqlite(record.runtime_generation, "runtime_generation")?,
                        to_sqlite(record.catalog_epoch, "catalog_epoch")?,
                        descriptor_json,
                        record.descriptor_sha256,
                        record.verified_issuer_id,
                        record.activation.as_str(),
                        record.approved_descriptor_sha256,
                        to_sqlite(record.revision, "revision")?,
                        record.reason_code,
                        record.created_at_unix_ms,
                        record.updated_at_unix_ms,
                    ],
                )
                .map_err(|_| unavailable("trusted_tool_insert_failed"))?
        };
        if changed != 1 {
            let actual = load_trusted_tool(&transaction, &record.server_id, &record.tool_name)?
                .map(|current| current.revision);
            return Err(McpSecurityEvidenceStoreError::RevisionConflict {
                expected: expected_revision,
                actual,
            });
        }
        transaction
            .execute(
                r#"
                    INSERT INTO mcp_trusted_tool_events_v1 (
                        server_id, tool_name, previous_revision, revision,
                        runtime_generation, catalog_epoch, descriptor_sha256,
                        activation, reason_code, occurred_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                params![
                    record.server_id,
                    record.tool_name,
                    expected_revision
                        .map(|revision| to_sqlite(revision, "previous_revision"))
                        .transpose()?,
                    to_sqlite(record.revision, "revision")?,
                    to_sqlite(record.runtime_generation, "runtime_generation")?,
                    to_sqlite(record.catalog_epoch, "catalog_epoch")?,
                    record.descriptor_sha256,
                    record.activation.as_str(),
                    record.reason_code,
                    record.updated_at_unix_ms,
                ],
            )
            .map_err(|_| unavailable("trusted_tool_event_insert_failed"))?;
        transaction.commit().map_err(|_| unavailable("transaction_commit_failed"))
    }

    async fn persist_conformance_report(
        &self,
        report: &McpConformanceReportV1,
    ) -> Result<(), McpSecurityEvidenceStoreError> {
        report.validate().map_err(|_| corrupt("conformance_report_invalid"))?;
        let report_json = serde_json::to_string(report)
            .map_err(|_| corrupt("conformance_report_encode_failed"))?;
        let report_sha256 = hex::encode(Sha256::digest(report_json.as_bytes()));
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let inserted = guard
            .execute(
                r#"
                    INSERT INTO mcp_conformance_reports_v1 (
                        report_sha256, server_id, transport, runtime_generation,
                        catalog_epoch, qualifies_for_production, report_json,
                        started_at_unix_ms, completed_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    ON CONFLICT(report_sha256) DO NOTHING
                "#,
                params![
                    report_sha256,
                    report.server_id,
                    transport_str(report.transport),
                    to_sqlite(report.runtime_generation, "runtime_generation")?,
                    to_sqlite(report.catalog_epoch, "catalog_epoch")?,
                    i64::from(report.qualifies_for_production()),
                    report_json,
                    report.started_at_unix_ms,
                    report.completed_at_unix_ms,
                ],
            )
            .map_err(|_| unavailable("conformance_report_insert_failed"))?;
        if inserted > 1 {
            return Err(unavailable("conformance_report_insert_count_invalid"));
        }
        Ok(())
    }

    async fn latest_conformance_report(
        &self,
        server_id: &str,
    ) -> Result<Option<McpConformanceReportV1>, McpSecurityEvidenceStoreError> {
        let guard = self.connection.lock().map_err(|_| unavailable("lock_poisoned"))?;
        let report_json = guard
            .query_row(
                r#"
                    SELECT report_json
                    FROM mcp_conformance_reports_v1
                    WHERE server_id = ?1
                    ORDER BY completed_at_unix_ms DESC, report_sha256 DESC
                    LIMIT 1
                "#,
                params![server_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| unavailable("conformance_report_load_failed"))?;
        report_json
            .map(|json| {
                let report = serde_json::from_str::<McpConformanceReportV1>(&json)
                    .map_err(|_| corrupt("conformance_report_decode_failed"))?;
                report.validate().map_err(|_| corrupt("conformance_report_invalid"))?;
                Ok(report)
            })
            .transpose()
    }
}

fn load_trusted_tool(
    connection: &rusqlite::Connection,
    server_id: &str,
    tool_name: &str,
) -> Result<Option<McpTrustedToolRecordV1>, McpSecurityEvidenceStoreError> {
    connection
        .query_row(
            r#"
                SELECT schema_version, server_id, tool_name, runtime_generation,
                       catalog_epoch, descriptor_json, descriptor_sha256,
                       verified_issuer_id, activation,
                       approved_descriptor_sha256, revision, reason_code,
                       created_at_unix_ms, updated_at_unix_ms
                FROM mcp_trusted_tool_heads_v1
                WHERE server_id = ?1 AND tool_name = ?2
            "#,
            params![server_id, tool_name],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, i64>(13)?,
                ))
            },
        )
        .optional()
        .map_err(|_| unavailable("trusted_tool_load_failed"))?
        .map(
            |(
                schema_version,
                server_id,
                tool_name,
                runtime_generation,
                catalog_epoch,
                descriptor_json,
                descriptor_sha256,
                verified_issuer_id,
                activation,
                approved_descriptor_sha256,
                revision,
                reason_code,
                created_at_unix_ms,
                updated_at_unix_ms,
            )| {
                let descriptor = serde_json::from_str(&descriptor_json)
                    .map_err(|_| corrupt("trusted_tool_descriptor_decode_failed"))?;
                let record = McpTrustedToolRecordV1 {
                    schema_version: u32::try_from(schema_version)
                        .map_err(|_| corrupt("trusted_tool_schema_version_invalid"))?,
                    server_id,
                    tool_name,
                    runtime_generation: from_sqlite(runtime_generation, "runtime_generation")?,
                    catalog_epoch: from_sqlite(catalog_epoch, "catalog_epoch")?,
                    descriptor,
                    descriptor_sha256,
                    verified_issuer_id,
                    activation: parse_activation(&activation)?,
                    approved_descriptor_sha256,
                    revision: from_sqlite(revision, "revision")?,
                    reason_code,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                };
                record.validate().map_err(|_| corrupt("trusted_tool_invalid"))?;
                Ok(record)
            },
        )
        .transpose()
}

fn parse_activation(
    value: &str,
) -> Result<McpTrustedToolActivationState, McpSecurityEvidenceStoreError> {
    match value {
        "pending_approval" => Ok(McpTrustedToolActivationState::PendingApproval),
        "active" => Ok(McpTrustedToolActivationState::Active),
        "disabled" => Ok(McpTrustedToolActivationState::Disabled),
        _ => Err(corrupt("trusted_tool_activation_unknown")),
    }
}

fn transport_str(transport: McpSessionTransportKind) -> &'static str {
    match transport {
        McpSessionTransportKind::Stdio => "stdio",
        McpSessionTransportKind::StreamableHttp => "streamable_http",
        McpSessionTransportKind::ServerSentEvents => "server_sent_events",
    }
}

fn to_sqlite(value: u64, field: &'static str) -> Result<i64, McpSecurityEvidenceStoreError> {
    i64::try_from(value).map_err(|_| corrupt(&format!("{field}_out_of_range")))
}

fn from_sqlite(value: i64, field: &'static str) -> Result<u64, McpSecurityEvidenceStoreError> {
    u64::try_from(value).map_err(|_| corrupt(&format!("{field}_negative")))
}

fn corrupt(reason: &str) -> McpSecurityEvidenceStoreError {
    McpSecurityEvidenceStoreError::Corrupt {
        reason_code: format!("mcp.runtime.security.store.{reason}"),
    }
}

fn unavailable(reason: &str) -> McpSecurityEvidenceStoreError {
    McpSecurityEvidenceStoreError::Unavailable {
        reason_code: format!("mcp.runtime.security.store.{reason}"),
    }
}
