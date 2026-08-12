//! Offline durable-state maintenance commands for `palyra state`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use palyra_common::{
    highest_state_health_severity, StateHealthEvidenceRef, StateHealthFinding, StateHealthSeverity,
};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use ulid::Ulid;

use crate::*;

const STATE_REPORT_SCHEMA_VERSION: u32 = 1;
const STATE_REPAIR_SCHEMA_VERSION: u32 = 1;
const DEFAULT_FAST_HASH_WINDOW: usize = 256;

#[derive(Debug, Clone, Copy)]
enum HashVerificationScope {
    FastWindow { limit: usize },
    Full,
}

impl HashVerificationScope {
    const fn label(self) -> &'static str {
        match self {
            Self::FastWindow { .. } => "fast_window",
            Self::Full => "full",
        }
    }

    const fn limit(self) -> Option<usize> {
        match self {
            Self::FastWindow { limit } => Some(limit),
            Self::Full => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateDoctorReport {
    schema_version: u32,
    generated_at_unix_ms: i64,
    subsystem: String,
    overall_severity: StateHealthSeverity,
    db: StateDbHealth,
    wal: StateWalHealth,
    schema: StateSchemaHealth,
    quick_check: StateQuickCheckHealth,
    hash_chain: HashChainVerificationReport,
    fts: Vec<FtsHealthReport>,
    sidecars: Vec<SidecarIndexDescriptor>,
    findings: Vec<StateHealthFinding>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateDbHealth {
    path_ref: String,
    file_exists: bool,
    file_bytes: Option<u64>,
    parent_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateWalHealth {
    journal_mode: String,
    synchronous: String,
    busy_timeout_ms: i64,
    wal_file_exists: bool,
    wal_file_bytes: Option<u64>,
    shm_file_exists: bool,
    shm_file_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateSchemaHealth {
    migrations_table_exists: bool,
    current_version: Option<i64>,
    applied_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateQuickCheckHealth {
    status: String,
    messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct HashChainVerificationReport {
    scope: String,
    checked_events: usize,
    total_events: usize,
    status: String,
    mismatch: Option<HashChainMismatch>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct HashChainMismatch {
    event_id: String,
    seq: i64,
    code: String,
    expected_hash: Option<String>,
    found_hash: Option<String>,
    expected_prev_hash: Option<String>,
    found_prev_hash: Option<String>,
    safe_summary: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FtsHealthReport {
    subsystem: String,
    fts_table: String,
    authoritative_table: String,
    missing_table: bool,
    authoritative_rows: i64,
    fts_rows: Option<i64>,
    orphan_rows: Option<i64>,
    lagging_rows: Option<i64>,
    repair_plan: Option<FtsRepairPlan>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FtsRepairPlan {
    strategy: String,
    requires_backup: bool,
    target_tables: Vec<String>,
    targeted_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SidecarIndexDescriptor {
    index_id: String,
    subsystem: String,
    schema_version: u32,
    authoritative_source: String,
    authoritative_digest: String,
    rebuild_status: String,
    directory_ref: String,
    allowed_to_be_sidecar: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateRepairReport {
    schema_version: u32,
    generated_at_unix_ms: i64,
    dry_run: bool,
    actor_principal: String,
    backup: Option<StateBackupReport>,
    planned_steps: Vec<String>,
    applied_steps: Vec<String>,
    skipped_steps: Vec<String>,
    remaining_findings: Vec<StateHealthFinding>,
    restore_instructions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateBackupReport {
    backup_ref: String,
    created_at_unix_ms: i64,
    size_bytes: u64,
    owner_only_permissions: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StateCheckpointReport {
    db_path_ref: String,
    mode: String,
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
    duration_ms: u64,
}

#[derive(Debug, Clone)]
struct FtsIndexSpec {
    subsystem: &'static str,
    fts_table: &'static str,
    authoritative_table: &'static str,
    key_column: &'static str,
    create_sql: &'static str,
    rebuild_sql: &'static str,
}

const FTS_INDEXES: &[FtsIndexSpec] = &[
    FtsIndexSpec {
        subsystem: "memory",
        fts_table: "memory_items_fts",
        authoritative_table: "memory_items",
        key_column: "memory_ulid",
        create_sql: r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_items_fts
                USING fts5(memory_ulid UNINDEXED, content_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_fts_ai
            AFTER INSERT ON memory_items
            BEGIN
                INSERT INTO memory_items_fts(memory_ulid, content_text)
                VALUES (new.memory_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_fts_ad
            AFTER DELETE ON memory_items
            BEGIN
                DELETE FROM memory_items_fts WHERE memory_ulid = old.memory_ulid;
            END;
        "#,
        rebuild_sql: r#"
            DELETE FROM memory_items_fts;
            INSERT INTO memory_items_fts(memory_ulid, content_text)
            SELECT memory_ulid, content_text FROM memory_items;
        "#,
    },
    FtsIndexSpec {
        subsystem: "workspace_documents",
        fts_table: "workspace_document_chunks_fts",
        authoritative_table: "workspace_document_chunks",
        key_column: "chunk_ulid",
        create_sql: r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS workspace_document_chunks_fts
                USING fts5(chunk_ulid UNINDEXED, content_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_chunks_ai
            AFTER INSERT ON workspace_document_chunks
            BEGIN
                INSERT INTO workspace_document_chunks_fts(chunk_ulid, content_text)
                VALUES (new.chunk_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_chunks_ad
            AFTER DELETE ON workspace_document_chunks
            BEGIN
                DELETE FROM workspace_document_chunks_fts WHERE chunk_ulid = old.chunk_ulid;
            END;
        "#,
        rebuild_sql: r#"
            DELETE FROM workspace_document_chunks_fts;
            INSERT INTO workspace_document_chunks_fts(chunk_ulid, content_text)
            SELECT chunk_ulid, content_text FROM workspace_document_chunks;
        "#,
    },
    FtsIndexSpec {
        subsystem: "workspace_checkpoints",
        fts_table: "workspace_checkpoint_files_fts",
        authoritative_table: "workspace_checkpoint_files",
        key_column: "artifact_ulid",
        create_sql: r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS workspace_checkpoint_files_fts
                USING fts5(artifact_ulid UNINDEXED, path, search_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_workspace_checkpoint_files_ai
            AFTER INSERT ON workspace_checkpoint_files
            BEGIN
                INSERT INTO workspace_checkpoint_files_fts(artifact_ulid, path, search_text)
                VALUES (new.artifact_ulid, new.path, COALESCE(new.search_text, ''));
            END;
            CREATE TRIGGER IF NOT EXISTS trg_workspace_checkpoint_files_ad
            AFTER DELETE ON workspace_checkpoint_files
            BEGIN
                DELETE FROM workspace_checkpoint_files_fts WHERE artifact_ulid = old.artifact_ulid;
            END;
        "#,
        rebuild_sql: r#"
            DELETE FROM workspace_checkpoint_files_fts;
            INSERT INTO workspace_checkpoint_files_fts(artifact_ulid, path, search_text)
            SELECT artifact_ulid, path, COALESCE(search_text, '') FROM workspace_checkpoint_files;
        "#,
    },
];

pub(crate) fn run_state(command: StateCommand) -> Result<()> {
    match command {
        StateCommand::Doctor { db_path, fast_window, full, json } => {
            let db_path = resolve_state_db_path(db_path)?;
            let scope = hash_scope(full, fast_window);
            let report = build_state_doctor_report(db_path.as_path(), scope)?;
            if json {
                output::print_json_pretty(&report, "failed to encode state doctor report as JSON")
            } else {
                print_state_doctor_report(&report)
            }
        }
        StateCommand::VerifyHashChain { db_path, full, limit, json } => {
            let db_path = resolve_state_db_path(db_path)?;
            let connection = open_state_connection(db_path.as_path())?;
            let report = verify_hash_chain(&connection, hash_scope(full, limit))?;
            if json {
                output::print_json_pretty(
                    &report,
                    "failed to encode state hash-chain report as JSON",
                )
            } else {
                print_hash_chain_report(&report)
            }
        }
        StateCommand::Repair { db_path, dry_run, fts_only, actor_principal, json } => {
            if !fts_only {
                anyhow::bail!(
                    "state repair currently supports only targeted FTS repair; pass --fts-only"
                );
            }
            let db_path = resolve_state_db_path(db_path)?;
            let report = repair_state(db_path.as_path(), dry_run, actor_principal)?;
            if json {
                output::print_json_pretty(&report, "failed to encode state repair report as JSON")
            } else {
                print_state_repair_report(&report)
            }
        }
        StateCommand::Checkpoint { db_path, mode, json } => {
            let db_path = resolve_state_db_path(db_path)?;
            let connection = open_state_connection(db_path.as_path())?;
            let report = checkpoint_wal(&connection, db_path.as_path(), mode)?;
            if json {
                output::print_json_pretty(
                    &report,
                    "failed to encode state checkpoint report as JSON",
                )
            } else {
                println!(
                    "state.checkpoint db_path_ref={} mode={} busy={} log_frames={} checkpointed_frames={}",
                    report.db_path_ref,
                    report.mode,
                    report.busy,
                    report.log_frames,
                    report.checkpointed_frames
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        StateCommand::SidecarsPrepare { db_path, json } => {
            let db_path = resolve_state_db_path(db_path)?;
            let descriptors = prepare_sidecar_storage(db_path.as_path())?;
            if json {
                output::print_json_pretty(
                    &descriptors,
                    "failed to encode state sidecar descriptors as JSON",
                )
            } else {
                for descriptor in descriptors {
                    println!(
                        "state.sidecar index_id={} subsystem={} directory_ref={} rebuild_status={}",
                        descriptor.index_id,
                        descriptor.subsystem,
                        descriptor.directory_ref,
                        descriptor.rebuild_status
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
    }
}

fn resolve_state_db_path(db_path: Option<String>) -> Result<PathBuf> {
    let db_path = resolve_daemon_journal_db_path(db_path)?;
    ensure_journal_db_exists(db_path.as_path())?;
    Ok(db_path)
}

fn open_state_connection(db_path: &Path) -> Result<Connection> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open journal database {}", db_path.display()))?;
    connection
        .execute_batch("PRAGMA busy_timeout = 5000;")
        .with_context(|| format!("failed to configure busy_timeout for {}", db_path.display()))?;
    Ok(connection)
}

fn hash_scope(full: bool, limit: Option<usize>) -> HashVerificationScope {
    if full {
        HashVerificationScope::Full
    } else {
        HashVerificationScope::FastWindow {
            limit: limit.unwrap_or(DEFAULT_FAST_HASH_WINDOW).max(1),
        }
    }
}

fn build_state_doctor_report(
    db_path: &Path,
    hash_scope: HashVerificationScope,
) -> Result<StateDoctorReport> {
    let connection = open_state_connection(db_path)?;
    let generated_at_unix_ms = unix_now_ms();
    let db = collect_db_health(db_path);
    let wal = collect_wal_health(&connection, db_path)?;
    let schema = collect_schema_health(&connection)?;
    let quick_check = collect_quick_check(&connection)?;
    let hash_chain = verify_hash_chain(&connection, hash_scope)?;
    let fts = collect_fts_health(&connection)?;
    let sidecars = sidecar_index_descriptors(db_path);
    let findings = state_findings(&wal, &schema, &quick_check, &hash_chain, fts.as_slice());
    let overall_severity = highest_state_health_severity(findings.as_slice());
    Ok(StateDoctorReport {
        schema_version: STATE_REPORT_SCHEMA_VERSION,
        generated_at_unix_ms,
        subsystem: "journal".to_owned(),
        overall_severity,
        db,
        wal,
        schema,
        quick_check,
        hash_chain,
        fts,
        sidecars,
        findings,
    })
}

fn collect_db_health(db_path: &Path) -> StateDbHealth {
    let metadata = fs::metadata(db_path).ok();
    StateDbHealth {
        path_ref: path_evidence_ref(db_path),
        file_exists: metadata.as_ref().is_some_and(|value| value.is_file()),
        file_bytes: metadata.as_ref().map(fs::Metadata::len),
        parent_ref: db_path.parent().map(path_evidence_ref),
    }
}

fn collect_wal_health(connection: &Connection, db_path: &Path) -> Result<StateWalHealth> {
    let journal_mode = pragma_string(connection, "PRAGMA journal_mode;")?.to_ascii_lowercase();
    let synchronous = pragma_i64(connection, "PRAGMA synchronous;")?.to_string();
    let busy_timeout_ms = pragma_i64(connection, "PRAGMA busy_timeout;")?;
    let wal_path = wal_side_file(db_path, "wal");
    let shm_path = wal_side_file(db_path, "shm");
    let wal_metadata = fs::metadata(wal_path.as_path()).ok();
    let shm_metadata = fs::metadata(shm_path.as_path()).ok();
    Ok(StateWalHealth {
        journal_mode,
        synchronous,
        busy_timeout_ms,
        wal_file_exists: wal_metadata.as_ref().is_some_and(|value| value.is_file()),
        wal_file_bytes: wal_metadata.as_ref().map(fs::Metadata::len),
        shm_file_exists: shm_metadata.as_ref().is_some_and(|value| value.is_file()),
        shm_file_bytes: shm_metadata.as_ref().map(fs::Metadata::len),
    })
}

fn collect_schema_health(connection: &Connection) -> Result<StateSchemaHealth> {
    let migrations_table_exists = table_exists(connection, "schema_migrations")?;
    if !migrations_table_exists {
        return Ok(StateSchemaHealth {
            migrations_table_exists,
            current_version: None,
            applied_count: 0,
        });
    }
    let (current_version, applied_count) = connection
        .query_row("SELECT MAX(version), COUNT(*) FROM schema_migrations", [], |row| {
            Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, i64>(1)?))
        })
        .context("failed to read schema migration status")?;
    Ok(StateSchemaHealth { migrations_table_exists, current_version, applied_count })
}

fn collect_quick_check(connection: &Connection) -> Result<StateQuickCheckHealth> {
    let mut statement = connection.prepare("PRAGMA quick_check;")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let messages = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    let ok = messages.len() == 1 && messages.first().is_some_and(|message| message == "ok");
    Ok(StateQuickCheckHealth { status: if ok { "ok" } else { "failed" }.to_owned(), messages })
}

fn state_findings(
    wal: &StateWalHealth,
    schema: &StateSchemaHealth,
    quick_check: &StateQuickCheckHealth,
    hash_chain: &HashChainVerificationReport,
    fts: &[FtsHealthReport],
) -> Vec<StateHealthFinding> {
    let mut findings = Vec::new();
    if wal.journal_mode != "wal" {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Degraded,
            "journal",
            "journal.wal.not_wal",
            format!("journal mode is {}", wal.journal_mode),
            "restart palyrad with the journal on a local filesystem that supports WAL mode",
            vec![StateHealthEvidenceRef::new(
                "sqlite_pragma",
                "journal_mode",
                wal.journal_mode.clone(),
            )],
        ));
    }
    if !schema.migrations_table_exists {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Degraded,
            "journal",
            "journal.schema.migrations_missing",
            "schema_migrations table is missing",
            "restart palyrad to apply idempotent migrations before repairing state",
            vec![StateHealthEvidenceRef::new("sqlite_table", "schema_migrations", "missing")],
        ));
    }
    if quick_check.status != "ok" {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Critical,
            "journal",
            "journal.sqlite.quick_check_failed",
            "SQLite quick_check reported integrity errors",
            "stop writes, create an external backup, and inspect the database offline",
            quick_check
                .messages
                .iter()
                .map(|message| {
                    StateHealthEvidenceRef::new(
                        "sqlite_pragma",
                        "quick_check",
                        safe_sqlite_error_summary(message),
                    )
                })
                .collect(),
        ));
    }
    if let Some(mismatch) = hash_chain.mismatch.as_ref() {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Critical,
            "journal",
            "journal.hash_chain.mismatch",
            mismatch.safe_summary.clone(),
            "stop writes and run full offline verification before restoring or overriding",
            vec![StateHealthEvidenceRef::new(
                "journal_event",
                mismatch.code.clone(),
                format!("event_id={} seq={}", mismatch.event_id, mismatch.seq),
            )],
        ));
    }
    for report in fts {
        if report.missing_table {
            findings.push(StateHealthFinding::new(
                StateHealthSeverity::Degraded,
                format!("fts.{}", report.subsystem),
                format!("fts.{}.missing_table", report.subsystem),
                format!("FTS table {} is missing", report.fts_table),
                "run `palyra state repair --fts-only --dry-run` and then apply the FTS repair",
                vec![StateHealthEvidenceRef::new(
                    "sqlite_table",
                    report.fts_table.clone(),
                    "missing virtual table",
                )],
            ));
        }
        if report.orphan_rows.unwrap_or_default() > 0 || report.lagging_rows.unwrap_or_default() > 0
        {
            findings.push(StateHealthFinding::new(
                StateHealthSeverity::Warning,
                format!("fts.{}", report.subsystem),
                format!("fts.{}.drift", report.subsystem),
                format!("FTS table {} has orphan or lagging rows", report.fts_table),
                "run `palyra state repair --fts-only --dry-run` to inspect a targeted rebuild",
                vec![StateHealthEvidenceRef::new(
                    "sqlite_table",
                    report.fts_table.clone(),
                    format!(
                        "orphan_rows={} lagging_rows={}",
                        report.orphan_rows.unwrap_or_default(),
                        report.lagging_rows.unwrap_or_default()
                    ),
                )],
            ));
        }
    }
    findings
}

fn verify_hash_chain(
    connection: &Connection,
    scope: HashVerificationScope,
) -> Result<HashChainVerificationReport> {
    if !table_exists(connection, "journal_events")? {
        return Ok(HashChainVerificationReport {
            scope: scope.label().to_owned(),
            checked_events: 0,
            total_events: 0,
            status: "missing_journal_events_table".to_owned(),
            mismatch: Some(HashChainMismatch {
                event_id: "unknown".to_owned(),
                seq: 0,
                code: "journal.hash_chain.missing_events_table".to_owned(),
                expected_hash: None,
                found_hash: None,
                expected_prev_hash: None,
                found_prev_hash: None,
                safe_summary: "journal_events table is missing".to_owned(),
            }),
        });
    }

    let total_events = count_table_rows(connection, "journal_events")?.max(0) as usize;
    let rows = hash_chain_rows(connection, scope)?;
    if rows.is_empty() {
        return Ok(HashChainVerificationReport {
            scope: scope.label().to_owned(),
            checked_events: 0,
            total_events,
            status: "ok".to_owned(),
            mismatch: None,
        });
    }

    if matches!(scope, HashVerificationScope::Full) {
        let first_row = &rows[0];
        if first_row.seq != 1 {
            return Ok(hash_mismatch_report(HashMismatchReportInput {
                scope,
                checked_events: rows.len(),
                total_events,
                row: first_row,
                code: "journal.hash_chain.missing_genesis",
                expected_hash: None,
                found_hash: first_row.hash.clone(),
                expected_prev_hash: None,
                found_prev_hash: first_row.prev_hash.clone(),
                safe_summary: "full journal hash chain does not start at sequence 1",
            }));
        }
        if first_row.prev_hash.is_some() {
            return Ok(hash_mismatch_report(HashMismatchReportInput {
                scope,
                checked_events: rows.len(),
                total_events,
                row: first_row,
                code: "journal.hash_chain.genesis_prev_hash_mismatch",
                expected_hash: None,
                found_hash: first_row.hash.clone(),
                expected_prev_hash: None,
                found_prev_hash: first_row.prev_hash.clone(),
                safe_summary: "full journal hash chain genesis row has a previous hash",
            }));
        }
    }

    let mut previous_seq = None;
    let mut previous_hash = rows.first().and_then(|row| row.prev_hash.clone());
    for row in &rows {
        if let Some(previous_seq) = previous_seq {
            if row.seq != previous_seq + 1 {
                return Ok(hash_mismatch_report(HashMismatchReportInput {
                    scope,
                    checked_events: rows.len(),
                    total_events,
                    row,
                    code: "journal.hash_chain.missing_event",
                    expected_hash: previous_hash.clone(),
                    found_hash: row.prev_hash.clone(),
                    expected_prev_hash: previous_hash.clone(),
                    found_prev_hash: row.hash.clone(),
                    safe_summary: "journal hash chain has a sequence gap",
                }));
            }
            if row.prev_hash != previous_hash {
                return Ok(hash_mismatch_report(HashMismatchReportInput {
                    scope,
                    checked_events: rows.len(),
                    total_events,
                    row,
                    code: "journal.hash_chain.prev_hash_mismatch",
                    expected_hash: previous_hash.clone(),
                    found_hash: row.prev_hash.clone(),
                    expected_prev_hash: previous_hash.clone(),
                    found_prev_hash: row.hash.clone(),
                    safe_summary:
                        "journal hash chain previous hash pointer does not match the prior event",
                }));
            }
        }
        let expected_hash = compute_journal_hash(
            row.prev_hash.as_deref(),
            JournalHashPreimage {
                event_id: row.event_id.as_str(),
                session_id: row.session_id.as_str(),
                run_id: row.run_id.as_str(),
                kind: row.kind,
                actor: row.actor,
                timestamp_unix_ms: row.timestamp_unix_ms,
                principal: row.principal.as_str(),
                device_id: row.device_id.as_str(),
                channel: row.channel.as_deref(),
                payload_json: row.payload_json.as_str(),
            },
        );
        if row.hash.as_deref() != Some(expected_hash.as_str()) {
            return Ok(hash_mismatch_report(HashMismatchReportInput {
                scope,
                checked_events: rows.len(),
                total_events,
                row,
                code: "journal.hash_chain.hash_mismatch",
                expected_hash: Some(expected_hash),
                found_hash: row.hash.clone(),
                expected_prev_hash: row.prev_hash.clone(),
                found_prev_hash: row.hash.clone(),
                safe_summary: "journal hash chain digest does not match the stored event identity",
            }));
        }
        previous_seq = Some(row.seq);
        previous_hash = row.hash.clone();
    }

    Ok(HashChainVerificationReport {
        scope: scope.label().to_owned(),
        checked_events: rows.len(),
        total_events,
        status: "ok".to_owned(),
        mismatch: None,
    })
}

struct HashMismatchReportInput<'a> {
    scope: HashVerificationScope,
    checked_events: usize,
    total_events: usize,
    row: &'a HashChainRow,
    code: &'a str,
    expected_hash: Option<String>,
    found_hash: Option<String>,
    expected_prev_hash: Option<String>,
    found_prev_hash: Option<String>,
    safe_summary: &'a str,
}

fn hash_mismatch_report(input: HashMismatchReportInput<'_>) -> HashChainVerificationReport {
    HashChainVerificationReport {
        scope: input.scope.label().to_owned(),
        checked_events: input.checked_events,
        total_events: input.total_events,
        status: "mismatch".to_owned(),
        mismatch: Some(HashChainMismatch {
            event_id: input.row.event_id.clone(),
            seq: input.row.seq,
            code: input.code.to_owned(),
            expected_hash: input.expected_hash,
            found_hash: input.found_hash,
            expected_prev_hash: input.expected_prev_hash,
            found_prev_hash: input.found_prev_hash,
            safe_summary: input.safe_summary.to_owned(),
        }),
    }
}

#[derive(Debug, Clone)]
struct HashChainRow {
    seq: i64,
    event_id: String,
    session_id: String,
    run_id: String,
    kind: i32,
    actor: i32,
    timestamp_unix_ms: i64,
    payload_json: String,
    hash: Option<String>,
    prev_hash: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
}

fn hash_chain_rows(
    connection: &Connection,
    scope: HashVerificationScope,
) -> Result<Vec<HashChainRow>> {
    let sql = if scope.limit().is_some() {
        r#"
            SELECT * FROM (
                SELECT
                    seq,
                    event_ulid,
                    session_ulid,
                    run_ulid,
                    kind,
                    actor,
                    timestamp_unix_ms,
                    payload_json,
                    hash,
                    prev_hash,
                    principal,
                    device_id,
                    channel
                FROM journal_events
                ORDER BY seq DESC
                LIMIT ?1
            )
            ORDER BY seq ASC
        "#
    } else {
        r#"
            SELECT
                seq,
                event_ulid,
                session_ulid,
                run_ulid,
                kind,
                actor,
                timestamp_unix_ms,
                payload_json,
                hash,
                prev_hash,
                principal,
                device_id,
                channel
            FROM journal_events
            ORDER BY seq ASC
        "#
    };
    let mut statement = connection.prepare(sql)?;
    let map_row = |row: &rusqlite::Row<'_>| {
        Ok(HashChainRow {
            seq: row.get(0)?,
            event_id: row.get(1)?,
            session_id: row.get(2)?,
            run_id: row.get(3)?,
            kind: row.get(4)?,
            actor: row.get(5)?,
            timestamp_unix_ms: row.get(6)?,
            payload_json: row.get(7)?,
            hash: row.get(8)?,
            prev_hash: row.get(9)?,
            principal: row.get(10)?,
            device_id: row.get(11)?,
            channel: row.get(12)?,
        })
    };
    let rows = if let Some(limit) = scope.limit() {
        statement.query_map([limit as i64], map_row)?.collect::<std::result::Result<Vec<_>, _>>()?
    } else {
        statement.query_map([], map_row)?.collect::<std::result::Result<Vec<_>, _>>()?
    };
    Ok(rows)
}

#[derive(Debug, Clone, Copy)]
struct JournalHashPreimage<'a> {
    event_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    kind: i32,
    actor: i32,
    timestamp_unix_ms: i64,
    principal: &'a str,
    device_id: &'a str,
    channel: Option<&'a str>,
    payload_json: &'a str,
}

fn compute_journal_hash(prev_hash: Option<&str>, preimage: JournalHashPreimage<'_>) -> String {
    let mut hasher = Sha256::new();
    // INTENTIONAL: this mirrors the persisted daemon hash-chain preimage order.
    if let Some(prev_hash) = prev_hash {
        hasher.update(prev_hash.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(preimage.event_id.as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.session_id.as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.kind.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.actor.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.timestamp_unix_ms.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.principal.as_bytes());
    hasher.update(b"|");
    hasher.update(preimage.device_id.as_bytes());
    hasher.update(b"|");
    if let Some(channel) = preimage.channel {
        hasher.update(channel.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(preimage.payload_json.as_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push_str(format!("{byte:02x}").as_str());
    }
    encoded
}

fn collect_fts_health(connection: &Connection) -> Result<Vec<FtsHealthReport>> {
    FTS_INDEXES.iter().map(|spec| collect_single_fts_health(connection, spec)).collect()
}

fn collect_single_fts_health(
    connection: &Connection,
    spec: &FtsIndexSpec,
) -> Result<FtsHealthReport> {
    let authoritative_rows = if table_exists(connection, spec.authoritative_table)? {
        count_table_rows(connection, spec.authoritative_table)?
    } else {
        0
    };
    let missing_table = !table_exists(connection, spec.fts_table)?;
    let (fts_rows, orphan_rows, lagging_rows) = if missing_table {
        (None, None, None)
    } else {
        (
            Some(count_table_rows(connection, spec.fts_table)?),
            Some(fts_orphan_rows(connection, spec)?),
            Some(fts_lagging_rows(connection, spec)?),
        )
    };
    let repair_plan = if missing_table
        || orphan_rows.unwrap_or_default() > 0
        || lagging_rows.unwrap_or_default() > 0
    {
        Some(fts_repair_plan(spec, missing_table))
    } else {
        None
    };
    Ok(FtsHealthReport {
        subsystem: spec.subsystem.to_owned(),
        fts_table: spec.fts_table.to_owned(),
        authoritative_table: spec.authoritative_table.to_owned(),
        missing_table,
        authoritative_rows,
        fts_rows,
        orphan_rows,
        lagging_rows,
        repair_plan,
    })
}

fn fts_repair_plan(spec: &FtsIndexSpec, missing_table: bool) -> FtsRepairPlan {
    let mut steps = Vec::new();
    if missing_table {
        steps.push(format!("fts.create.{}", spec.fts_table));
    }
    steps.push(format!("fts.rebuild.{}", spec.fts_table));
    FtsRepairPlan {
        strategy: "drop_rebuild_from_authoritative_table".to_owned(),
        requires_backup: true,
        target_tables: vec![spec.fts_table.to_owned()],
        targeted_steps: steps,
    }
}

fn fts_orphan_rows(connection: &Connection, spec: &FtsIndexSpec) -> Result<i64> {
    let sql = format!(
        "SELECT COUNT(*) FROM {fts} f LEFT JOIN {auth} a ON f.{key}=a.{key} WHERE a.{key} IS NULL",
        fts = spec.fts_table,
        auth = spec.authoritative_table,
        key = spec.key_column
    );
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn fts_lagging_rows(connection: &Connection, spec: &FtsIndexSpec) -> Result<i64> {
    let sql = format!(
        "SELECT COUNT(*) FROM {auth} a LEFT JOIN {fts} f ON f.{key}=a.{key} WHERE f.{key} IS NULL",
        auth = spec.authoritative_table,
        fts = spec.fts_table,
        key = spec.key_column
    );
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn repair_state(
    db_path: &Path,
    dry_run: bool,
    actor_principal: String,
) -> Result<StateRepairReport> {
    let connection = open_state_connection(db_path)?;
    let fts_reports = collect_fts_health(&connection)?;
    let mut planned_steps = Vec::new();
    for report in &fts_reports {
        if let Some(plan) = report.repair_plan.as_ref() {
            planned_steps.extend(plan.targeted_steps.iter().cloned());
        }
    }
    let mut applied_steps = Vec::new();
    let mut skipped_steps = Vec::new();
    let mut backup = None;
    if planned_steps.is_empty() {
        skipped_steps.push("fts.repair.noop".to_owned());
    } else if dry_run {
        skipped_steps.push("fts.repair.dry_run".to_owned());
    } else {
        backup = Some(create_state_repair_backup(&connection, db_path)?);
        for spec in FTS_INDEXES {
            let report = collect_single_fts_health(&connection, spec)?;
            if report.repair_plan.is_none() {
                continue;
            }
            connection
                .execute_batch(spec.create_sql)
                .with_context(|| format!("failed to create FTS objects for {}", spec.fts_table))?;
            connection
                .execute_batch(spec.rebuild_sql)
                .with_context(|| format!("failed to rebuild FTS table {}", spec.fts_table))?;
            applied_steps.push(format!("fts.rebuild.{}", spec.fts_table));
        }
    }

    let remaining_report =
        build_state_doctor_report(db_path, HashVerificationScope::FastWindow { limit: 64 })?;
    let mut remaining_findings = remaining_report.findings;
    remaining_findings.retain(|finding| finding.subsystem.starts_with("fts"));
    let restore_instructions = backup
        .as_ref()
        .map(|backup| {
            vec![
                format!("stop palyrad before restoring backup {}", backup.backup_ref),
                "replace the journal database with the backup copy using an operator shell"
                    .to_owned(),
                "start palyrad and run `palyra state doctor --json` before resuming writes"
                    .to_owned(),
            ]
        })
        .unwrap_or_default();

    Ok(StateRepairReport {
        schema_version: STATE_REPAIR_SCHEMA_VERSION,
        generated_at_unix_ms: unix_now_ms(),
        dry_run,
        actor_principal,
        backup,
        planned_steps,
        applied_steps,
        skipped_steps,
        remaining_findings,
        restore_instructions,
    })
}

fn create_state_repair_backup(
    connection: &Connection,
    db_path: &Path,
) -> Result<StateBackupReport> {
    let created_at_unix_ms = unix_now_ms();
    let backup_dir = db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("backups")
        .join("state-repair");
    fs::create_dir_all(backup_dir.as_path())
        .with_context(|| format!("failed to create backup directory {}", backup_dir.display()))?;
    palyra_vault::ensure_owner_only_dir(backup_dir.as_path()).with_context(|| {
        format!("failed to harden backup directory permissions at {}", backup_dir.display())
    })?;
    let backup_path =
        backup_dir.join(format!("journal-{}-{}.sqlite3", created_at_unix_ms, Ulid::generate()));
    connection
        .execute("VACUUM INTO ?1", rusqlite::params![backup_path.to_string_lossy().as_ref()])
        .with_context(|| {
            format!("failed to create state repair backup {}", backup_path.display())
        })?;
    palyra_vault::ensure_owner_only_file(backup_path.as_path()).with_context(|| {
        format!("failed to harden backup file permissions at {}", backup_path.display())
    })?;
    let size_bytes =
        fs::metadata(backup_path.as_path()).map(|metadata| metadata.len()).unwrap_or(0);
    Ok(StateBackupReport {
        backup_ref: path_evidence_ref(backup_path.as_path()),
        created_at_unix_ms,
        size_bytes,
        owner_only_permissions: true,
    })
}

fn checkpoint_wal(
    connection: &Connection,
    db_path: &Path,
    mode: JournalCheckpointModeArg,
) -> Result<StateCheckpointReport> {
    let started = Instant::now();
    let pragma_sql = format!("PRAGMA wal_checkpoint({});", checkpoint_mode_sql(mode));
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = connection
        .query_row(pragma_sql.as_str(), [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .with_context(|| {
            format!(
                "failed to run wal_checkpoint({}) on journal database {}",
                checkpoint_mode_sql(mode),
                db_path.display()
            )
        })?;
    Ok(StateCheckpointReport {
        db_path_ref: path_evidence_ref(db_path),
        mode: checkpoint_mode_label(mode).to_owned(),
        busy,
        log_frames,
        checkpointed_frames,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

fn prepare_sidecar_storage(db_path: &Path) -> Result<Vec<SidecarIndexDescriptor>> {
    let root = sidecar_root_for(db_path);
    fs::create_dir_all(root.as_path())
        .with_context(|| format!("failed to create sidecar root {}", root.display()))?;
    palyra_vault::ensure_owner_only_dir(root.as_path())
        .with_context(|| format!("failed to harden sidecar root {}", root.display()))?;
    for descriptor in sidecar_index_descriptors(db_path) {
        let dir = root.join(descriptor.index_id.as_str());
        fs::create_dir_all(dir.as_path())
            .with_context(|| format!("failed to create sidecar directory {}", dir.display()))?;
        palyra_vault::ensure_owner_only_dir(dir.as_path())
            .with_context(|| format!("failed to harden sidecar directory {}", dir.display()))?;
    }
    Ok(sidecar_index_descriptors(db_path))
}

fn sidecar_index_descriptors(db_path: &Path) -> Vec<SidecarIndexDescriptor> {
    let root = sidecar_root_for(db_path);
    [
        ("retrieval", "workspace_retrieval", "journal_store.workspace_documents"),
        ("memory_vector", "memory_vector_cache", "journal_store.memory_items"),
        ("workspace_fts", "workspace_fts_cache", "journal_store.workspace_document_chunks"),
        ("artifact_projection", "artifact_projection_cache", "journal_store.tool_result_artifacts"),
    ]
    .into_iter()
    .map(|(index_id, subsystem, source)| SidecarIndexDescriptor {
        index_id: index_id.to_owned(),
        subsystem: subsystem.to_owned(),
        schema_version: 1,
        authoritative_source: source.to_owned(),
        authoritative_digest: sha256_hex(source.as_bytes()),
        rebuild_status: "not_created".to_owned(),
        directory_ref: path_evidence_ref(root.join(index_id).as_path()),
        allowed_to_be_sidecar: true,
    })
    .collect()
}

fn sidecar_root_for(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("sidecars")
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view') LIMIT 1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(Into::into)
}

fn count_table_rows(connection: &Connection, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn pragma_string(connection: &Connection, sql: &str) -> Result<String> {
    connection.query_row(sql, [], |row| row.get::<_, String>(0)).map_err(Into::into)
}

fn pragma_i64(connection: &Connection, sql: &str) -> Result<i64> {
    connection.query_row(sql, [], |row| row.get::<_, i64>(0)).map_err(Into::into)
}

fn wal_side_file(db_path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}-{suffix}", db_path.display()))
}

fn path_evidence_ref(path: &Path) -> String {
    let display_name = path.file_name().and_then(|value| value.to_str()).unwrap_or("path");
    format!("{}:sha256:{}", display_name, &sha256_hex(path.to_string_lossy().as_bytes())[..12])
}

fn safe_sqlite_error_summary(message: &str) -> String {
    message.chars().take(160).collect()
}

fn print_state_doctor_report(report: &StateDoctorReport) -> Result<()> {
    println!(
        "state.doctor severity={} subsystem={} generated_at_unix_ms={} db_path_ref={}",
        report.overall_severity.as_str(),
        report.subsystem,
        report.generated_at_unix_ms,
        report.db.path_ref
    );
    println!(
        "state.wal journal_mode={} synchronous={} busy_timeout_ms={} wal_file_exists={} wal_file_bytes={}",
        report.wal.journal_mode,
        report.wal.synchronous,
        report.wal.busy_timeout_ms,
        report.wal.wal_file_exists,
        report.wal.wal_file_bytes.unwrap_or(0)
    );
    println!(
        "state.hash_chain status={} scope={} checked_events={} total_events={}",
        report.hash_chain.status,
        report.hash_chain.scope,
        report.hash_chain.checked_events,
        report.hash_chain.total_events
    );
    for finding in &report.findings {
        println!(
            "state.finding severity={} subsystem={} code={} summary={}",
            finding.severity.as_str(),
            finding.subsystem,
            finding.code,
            finding.summary
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn print_hash_chain_report(report: &HashChainVerificationReport) -> Result<()> {
    println!(
        "state.hash_chain status={} scope={} checked_events={} total_events={}",
        report.status, report.scope, report.checked_events, report.total_events
    );
    if let Some(mismatch) = report.mismatch.as_ref() {
        println!(
            "state.hash_chain.mismatch event_id={} seq={} code={} summary={}",
            mismatch.event_id, mismatch.seq, mismatch.code, mismatch.safe_summary
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn print_state_repair_report(report: &StateRepairReport) -> Result<()> {
    println!(
        "state.repair dry_run={} planned_steps={} applied_steps={} skipped_steps={} actor_principal={}",
        report.dry_run,
        report.planned_steps.len(),
        report.applied_steps.len(),
        report.skipped_steps.len(),
        report.actor_principal
    );
    for step in &report.planned_steps {
        println!("state.repair.plan step={step}");
    }
    for step in &report.applied_steps {
        println!("state.repair.applied step={step}");
    }
    if let Some(backup) = report.backup.as_ref() {
        println!(
            "state.repair.backup backup_ref={} size_bytes={} owner_only_permissions={}",
            backup.backup_ref, backup.size_bytes, backup.owner_only_permissions
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}
