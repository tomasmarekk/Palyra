//! Bounded, metadata-only trace summaries for offline support bundles.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use palyra_common::metadata_trace::{metadata_trace_id_sha256, MetadataTraceIdDomainV1};
use rusqlite::{params, Connection, OptionalExtension};

use super::{
    sanitize_diagnostic_error, SupportBundleMetadataTraceRunSnapshot,
    SupportBundleMetadataTraceSnapshot,
};

const METADATA_TRACE_SCHEMA_VERSION: u32 = 1;
const MAX_SUPPORT_BUNDLE_TRACES: usize = 8;
const MAX_SUPPORT_BUNDLE_EVENT_KINDS: usize = 64;
const ALLOWED_EVENT_KINDS: &[&str] = &[
    "run_started",
    "runtime_selected",
    "context_assembled",
    "provider_attempt",
    "tool_gate",
    "approval",
    "tool_outcome",
    "recovery",
    "delivery_intent",
    "terminalization",
    "recovery_continuation",
    "capacity_reached",
];
const ALLOWED_SEGMENT_STATUSES: &[&str] = &["complete", "interrupted", "corrupt_suffix_isolated"];

pub(super) fn unavailable_support_bundle_metadata_trace(
    error: impl Into<String>,
) -> SupportBundleMetadataTraceSnapshot {
    SupportBundleMetadataTraceSnapshot {
        available: false,
        schema_version: METADATA_TRACE_SCHEMA_VERSION,
        reason_code: "metadata_trace.support_bundle.unavailable".to_owned(),
        trace_count: 0,
        recent_runs: Vec::new(),
        error: Some(sanitize_diagnostic_error(error.into().as_str())),
    }
}

pub(super) fn read_support_bundle_metadata_trace(
    connection: &Connection,
) -> SupportBundleMetadataTraceSnapshot {
    match read_support_bundle_metadata_trace_inner(connection) {
        Ok(snapshot) => snapshot,
        Err(error) => unavailable_support_bundle_metadata_trace(error.to_string()),
    }
}

fn read_support_bundle_metadata_trace_inner(
    connection: &Connection,
) -> Result<SupportBundleMetadataTraceSnapshot> {
    if !table_exists(connection, "metadata_trace_segments")?
        || !table_exists(connection, "metadata_trace_events")?
        || !table_exists(connection, "metadata_trace_segment_status_events")?
    {
        return Ok(SupportBundleMetadataTraceSnapshot {
            available: false,
            schema_version: METADATA_TRACE_SCHEMA_VERSION,
            reason_code: "metadata_trace.support_bundle.legacy_schema_missing".to_owned(),
            trace_count: 0,
            recent_runs: Vec::new(),
            error: Some("metadata trace schema is unavailable in this journal".to_owned()),
        });
    }

    let trace_count = connection
        .query_row("SELECT COUNT(DISTINCT run_ulid) FROM metadata_trace_segments", [], |row| {
            row.get::<_, i64>(0)
        })
        .context("failed to count metadata traces")?;
    let mut statement = connection.prepare(
        r#"
            SELECT run_ulid, session_ulid, MAX(opened_at_unix_ms) AS latest_opened_at
            FROM metadata_trace_segments
            GROUP BY run_ulid, session_ulid
            ORDER BY latest_opened_at DESC, run_ulid ASC
            LIMIT ?1
        "#,
    )?;
    let rows = statement.query_map(
        params![i64::try_from(MAX_SUPPORT_BUNDLE_TRACES).unwrap_or(i64::MAX)],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let mut recent_runs = Vec::new();
    for row in rows {
        let (run_id, session_id) = row?;
        recent_runs.push(read_run_summary(connection, run_id.as_str(), session_id.as_str())?);
    }

    Ok(SupportBundleMetadataTraceSnapshot {
        available: true,
        schema_version: METADATA_TRACE_SCHEMA_VERSION,
        reason_code: "metadata_trace.support_bundle.ready".to_owned(),
        trace_count: u64::try_from(trace_count).unwrap_or_default(),
        recent_runs,
        error: None,
    })
}

fn read_run_summary(
    connection: &Connection,
    run_id: &str,
    session_id: &str,
) -> Result<SupportBundleMetadataTraceRunSnapshot> {
    let segment_count = connection.query_row(
        "SELECT COUNT(*) FROM metadata_trace_segments WHERE run_ulid = ?1",
        params![run_id],
        |row| row.get::<_, i64>(0),
    )?;
    let event_count = connection.query_row(
        "SELECT COUNT(*) FROM metadata_trace_events WHERE run_ulid = ?1",
        params![run_id],
        |row| row.get::<_, i64>(0),
    )?;
    let terminal_event_present = connection.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM metadata_trace_events
                WHERE run_ulid = ?1
                  AND event_kind = 'terminalization'
            )
        "#,
        params![run_id],
        |row| row.get::<_, i64>(0),
    )? != 0;

    let mut segment_statuses = BTreeMap::new();
    let mut status_statement = connection.prepare(
        r#"
            SELECT status, COUNT(*)
            FROM metadata_trace_segment_status_events
            WHERE run_ulid = ?1
            GROUP BY status
            ORDER BY status ASC
        "#,
    )?;
    let status_rows = status_statement
        .query_map(params![run_id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;
    for row in status_rows {
        let (status, count) = row?;
        if ALLOWED_SEGMENT_STATUSES.contains(&status.as_str()) {
            segment_statuses.insert(status, u16::try_from(count).unwrap_or(u16::MAX));
        }
    }

    let mut kind_statement = connection.prepare(
        r#"
            SELECT event_kind
            FROM metadata_trace_events
            WHERE run_ulid = ?1
            ORDER BY sequence ASC
            LIMIT ?2
        "#,
    )?;
    let kind_rows = kind_statement.query_map(
        params![
            run_id,
            i64::try_from(MAX_SUPPORT_BUNDLE_EVENT_KINDS.saturating_add(1)).unwrap_or(i64::MAX)
        ],
        |row| row.get::<_, String>(0),
    )?;
    let mut event_kinds = Vec::new();
    let mut kind_rows_seen = 0_usize;
    for row in kind_rows {
        let kind = row?;
        kind_rows_seen = kind_rows_seen.saturating_add(1);
        if event_kinds.len() < MAX_SUPPORT_BUNDLE_EVENT_KINDS
            && ALLOWED_EVENT_KINDS.contains(&kind.as_str())
        {
            event_kinds.push(kind);
        }
    }
    let truncated =
        kind_rows_seen > MAX_SUPPORT_BUNDLE_EVENT_KINDS || event_kinds.len() != kind_rows_seen;
    Ok(SupportBundleMetadataTraceRunSnapshot {
        run_id_sha256: metadata_trace_id_sha256(MetadataTraceIdDomainV1::Run, run_id)
            .context("failed to hash support-bundle trace run identity")?,
        session_id_sha256: metadata_trace_id_sha256(MetadataTraceIdDomainV1::Session, session_id)
            .context("failed to hash support-bundle trace session identity")?,
        segment_count: u16::try_from(segment_count).unwrap_or(u16::MAX),
        event_count: u32::try_from(event_count).unwrap_or(u32::MAX),
        segment_statuses,
        event_kinds,
        terminal_event_present,
        truncated,
    })
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

#[cfg(test)]
mod tests {
    use palyra_common::metadata_trace::{metadata_trace_id_sha256, MetadataTraceIdDomainV1};
    use rusqlite::{params, Connection};

    use super::read_support_bundle_metadata_trace;

    #[test]
    fn legacy_journal_is_reported_without_failing_the_bundle() {
        let connection = Connection::open_in_memory().expect("in-memory journal should open");
        let snapshot = read_support_bundle_metadata_trace(&connection);

        assert!(!snapshot.available);
        assert_eq!(snapshot.reason_code, "metadata_trace.support_bundle.legacy_schema_missing");
        assert!(snapshot.recent_runs.is_empty());
    }

    #[test]
    fn identifier_hashes_are_domain_separated() {
        assert_ne!(
            metadata_trace_id_sha256(MetadataTraceIdDomainV1::Run, "same-id")
                .expect("run id should hash"),
            metadata_trace_id_sha256(MetadataTraceIdDomainV1::Session, "same-id")
                .expect("session id should hash")
        );
    }

    #[test]
    fn support_projection_never_reads_event_payloads_or_raw_identifiers() {
        let connection = Connection::open_in_memory().expect("in-memory journal should open");
        connection
            .execute_batch(
                r#"
                    CREATE TABLE metadata_trace_segments (
                        segment_ulid TEXT PRIMARY KEY,
                        run_ulid TEXT NOT NULL,
                        session_ulid TEXT NOT NULL,
                        opened_at_unix_ms INTEGER NOT NULL
                    );
                    CREATE TABLE metadata_trace_events (
                        run_ulid TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        event_kind TEXT NOT NULL,
                        event_json TEXT NOT NULL
                    );
                    CREATE TABLE metadata_trace_segment_status_events (
                        run_ulid TEXT NOT NULL,
                        status TEXT NOT NULL
                    );
                "#,
            )
            .expect("metadata trace fixture schema should apply");
        connection
            .execute(
                "INSERT INTO metadata_trace_segments VALUES (?1, ?2, ?3, 10)",
                params!["segment-secret", "run-secret", "session-secret"],
            )
            .expect("segment should insert");
        connection
            .execute(
                "INSERT INTO metadata_trace_events VALUES (?1, 0, 'run_started', ?2)",
                params![
                    "run-secret",
                    r#"{"prompt":"Bearer raw-token","stderr":"C:\\private\\secret.txt"}"#
                ],
            )
            .expect("event should insert");
        connection
            .execute(
                "INSERT INTO metadata_trace_segment_status_events VALUES (?1, 'complete')",
                params!["run-secret"],
            )
            .expect("status should insert");

        let snapshot = read_support_bundle_metadata_trace(&connection);
        let encoded = serde_json::to_string(&snapshot).expect("snapshot should serialize");

        assert!(snapshot.available);
        assert_eq!(snapshot.recent_runs[0].event_kinds, ["run_started"]);
        for forbidden in
            ["run-secret", "session-secret", "segment-secret", "raw-token", "private", "secret.txt"]
        {
            assert!(!encoded.contains(forbidden), "support trace leaked `{forbidden}`");
        }
    }
}
