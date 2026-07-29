//! State doctor, repair, and replay-fixture contracts for the journal store.
//!
//! Keeps state-foundation logic outside the large `journal.rs` facade while preserving
//! `JournalStore` as the stable API used by daemon runtime and transport handlers.

use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use palyra_common::{
    highest_state_health_severity, StateHealthEvidenceRef, StateHealthFinding, StateHealthSeverity,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{
    compute_hash, current_unix_ms, enforce_owner_only_permissions, JournalAppendRequest,
    JournalError, JournalStore, MIGRATIONS,
};

const STATE_HEALTH_SCHEMA_VERSION: u32 = 1;
const STATE_REPAIR_REPORT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_FAST_HASH_WINDOW: usize = 256;
const SQLITE_BUSY_RETRY_ATTEMPTS: usize = 4;
const SQLITE_BUSY_RETRY_BASE_MS: u64 = 15;
const HASH_CHAIN_WRITE_OVERRIDE_ENV: &str = "PALYRA_JOURNAL_HASH_CHAIN_WRITE_OVERRIDE";
const HASH_CHAIN_WRITE_OVERRIDE_VALUE: &str = "allow";

/// Scope for journal hash-chain verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalHashVerificationScope {
    /// Verify only the most recent `limit` events.
    FastWindow { limit: usize },
    /// Verify every event in the journal.
    Full,
}

impl JournalHashVerificationScope {
    fn label(self) -> &'static str {
        match self {
            Self::FastWindow { .. } => "fast_window",
            Self::Full => "full",
        }
    }

    fn limit(self) -> Option<usize> {
        match self {
            Self::FastWindow { limit } => Some(limit.max(1)),
            Self::Full => None,
        }
    }
}

/// WAL checkpoint mode accepted by state maintenance.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JournalWalCheckpointMode {
    /// Run a passive checkpoint without blocking writers.
    Passive,
    /// Block until all frames are checkpointed.
    Full,
    /// Checkpoint and restart the WAL when possible.
    Restart,
    /// Checkpoint and truncate the WAL file when possible.
    Truncate,
}

impl JournalWalCheckpointMode {
    fn sql_label(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }

    fn wire_label(self) -> &'static str {
        match self {
            Self::Passive => "passive",
            Self::Full => "full",
            Self::Restart => "restart",
            Self::Truncate => "truncate",
        }
    }
}

/// Read-only state doctor report for the SQLite journal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalHealthReport {
    pub schema_version: u32,
    pub generated_at_unix_ms: i64,
    pub subsystem: String,
    pub overall_severity: StateHealthSeverity,
    pub db: JournalDbHealth,
    pub wal: JournalWalHealth,
    pub schema: JournalSchemaHealth,
    pub quick_check: JournalQuickCheckHealth,
    pub write_probe: JournalWriteProbeHealth,
    pub hash_chain: JournalHashChainVerificationReport,
    pub fts: Vec<FtsHealthReport>,
    pub sidecars: Vec<SidecarIndexDescriptor>,
    pub findings: Vec<StateHealthFinding>,
}

/// Database identity and file evidence with path-safe references.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalDbHealth {
    pub path_ref: String,
    pub file_exists: bool,
    pub file_bytes: Option<u64>,
    pub parent_ref: Option<String>,
    pub owner_only_permissions_expected: bool,
}

/// WAL and SQLite runtime posture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalWalHealth {
    pub journal_mode: String,
    pub synchronous: String,
    pub busy_timeout_ms: i64,
    pub wal_file_exists: bool,
    pub wal_file_bytes: Option<u64>,
    pub shm_file_exists: bool,
    pub shm_file_bytes: Option<u64>,
    pub last_checkpoint: Option<JournalWalCheckpointReport>,
}

/// Applied schema migration status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalSchemaHealth {
    pub migrations_table_exists: bool,
    pub current_version: Option<i64>,
    pub expected_version: i64,
    pub applied_count: i64,
}

/// Result of `PRAGMA quick_check`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalQuickCheckHealth {
    pub status: String,
    pub checked: bool,
    pub messages: Vec<String>,
}

/// Rolled-back write probe status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalWriteProbeHealth {
    pub attempted: bool,
    pub outcome: String,
    pub rolled_back: bool,
    pub duration_ms: u64,
    pub error_class: Option<String>,
    pub fix_hint: Option<String>,
}

/// Hash-chain verification report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalHashChainVerificationReport {
    pub scope: String,
    pub checked_events: usize,
    pub total_events: usize,
    pub status: String,
    pub mismatch: Option<JournalHashChainMismatch>,
}

/// First detected hash-chain mismatch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalHashChainMismatch {
    pub event_id: String,
    pub seq: i64,
    pub code: String,
    pub expected_hash: Option<String>,
    pub found_hash: Option<String>,
    pub expected_prev_hash: Option<String>,
    pub found_prev_hash: Option<String>,
    pub safe_summary: String,
}

/// FTS index health for one rebuildable subsystem index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FtsHealthReport {
    pub subsystem: String,
    pub fts_table: String,
    pub authoritative_table: String,
    pub authoritative_rows: i64,
    pub indexed_rows: Option<i64>,
    pub missing_table: bool,
    pub orphan_rows: Option<i64>,
    pub lagging_rows: Option<i64>,
    pub status: String,
    pub repair_plan: Option<FtsRepairPlan>,
}

/// Targeted repair plan for a single FTS index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FtsRepairPlan {
    pub plan_id: String,
    pub subsystem: String,
    pub safety_level: String,
    pub dry_run_supported: bool,
    pub targeted_steps: Vec<String>,
}

/// Descriptor for a rebuildable sidecar index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SidecarIndexDescriptor {
    pub index_id: String,
    pub subsystem: String,
    pub schema_version: u32,
    pub authoritative_source: String,
    pub authoritative_digest: String,
    pub rebuild_status: String,
    pub directory_ref: String,
    pub allowed_to_be_sidecar: bool,
}

/// Request for journal state repair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalStateRepairRequest {
    pub dry_run: bool,
    pub fts_only: bool,
    pub actor_principal: String,
}

/// Result of a state repair operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalStateRepairReport {
    pub schema_version: u32,
    pub generated_at_unix_ms: i64,
    pub dry_run: bool,
    pub actor_principal: String,
    pub backup: Option<JournalStateBackupReport>,
    pub planned_steps: Vec<String>,
    pub applied_steps: Vec<String>,
    pub skipped_steps: Vec<String>,
    pub warnings: Vec<String>,
    pub remaining_findings: Vec<StateHealthFinding>,
    pub restore_instructions: Vec<String>,
    pub audit_event_id: Option<String>,
}

/// Backup created before applying a mutating state repair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalStateBackupReport {
    pub backup_ref: String,
    pub created_at_unix_ms: i64,
    pub size_bytes: u64,
    pub owner_only_permissions: bool,
}

/// WAL checkpoint result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalWalCheckpointReport {
    pub mode: String,
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
    pub duration_ms: u64,
}

/// Test-only fixture metadata for crash replay coverage.
#[cfg(test)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StateCrashReplayFixture {
    pub fixture_id: &'static str,
    pub initial_state: &'static str,
    pub crash_point: &'static str,
    pub expected_recovery: &'static str,
    pub expected_health_report: &'static str,
}

#[derive(Debug, Clone, Default)]
pub(super) struct JournalWriteGuardState {
    blocked: Option<JournalHashChainMismatch>,
}

#[derive(Debug, Clone, Copy)]
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
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ai
            AFTER INSERT ON memory_items
            BEGIN
                INSERT INTO memory_items_fts(memory_ulid, content_text)
                VALUES (new.memory_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ad
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
        subsystem: "workspace",
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

impl JournalStore {
    /// Builds a read-only state doctor report for the live journal store.
    ///
    /// # Errors
    /// Returns [`JournalError`] if SQLite health probes cannot run.
    pub fn state_health_report(
        &self,
        fast_window: Option<usize>,
    ) -> Result<JournalHealthReport, JournalError> {
        let limit = fast_window.unwrap_or(DEFAULT_FAST_HASH_WINDOW).max(1);
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let report = journal_health_report_for_connection(
            &guard,
            self.config.db_path.as_path(),
            self.config.hash_chain_enabled,
            JournalHashVerificationScope::FastWindow { limit },
        )?;
        if report.overall_severity == StateHealthSeverity::Critical {
            if let Some(mismatch) = report.hash_chain.mismatch.clone() {
                self.block_hash_chain_writes(mismatch)?;
            }
        }
        Ok(report)
    }

    /// Verifies the journal hash chain over a fast window or the full event history.
    ///
    /// # Errors
    /// Returns [`JournalError`] if the verification query fails.
    pub fn verify_hash_chain(
        &self,
        scope: JournalHashVerificationScope,
    ) -> Result<JournalHashChainVerificationReport, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let report = verify_hash_chain_for_connection(&guard, scope)?;
        if let Some(mismatch) = report.mismatch.clone() {
            self.block_hash_chain_writes(mismatch)?;
        }
        Ok(report)
    }

    /// Applies or previews targeted state repair steps.
    ///
    /// # Errors
    /// Returns [`JournalError`] if backup creation, repair execution, audit,
    /// or post-repair health collection fails.
    pub fn repair_state(
        &self,
        request: &JournalStateRepairRequest,
    ) -> Result<JournalStateRepairReport, JournalError> {
        if !request.fts_only {
            return Err(JournalError::InvalidArgument(
                "state repair currently supports only targeted FTS repair; pass --fts-only"
                    .to_owned(),
            ));
        }

        let generated_at_unix_ms = current_unix_ms()?;
        let mut planned_steps = Vec::new();
        let mut applied_steps = Vec::new();
        let mut skipped_steps = Vec::new();
        let mut warnings = Vec::new();
        let mut backup = None;

        {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            let fts_reports = collect_fts_health(&guard)?;
            for report in &fts_reports {
                if let Some(plan) = report.repair_plan.as_ref() {
                    planned_steps.extend(plan.targeted_steps.iter().cloned());
                }
            }
            if planned_steps.is_empty() {
                skipped_steps.push("fts.repair.noop".to_owned());
            } else if request.dry_run {
                skipped_steps.push("fts.repair.dry_run".to_owned());
            }
        }

        if !request.dry_run && !planned_steps.is_empty() {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            backup = Some(create_state_repair_backup(&guard, self.config.db_path.as_path())?);
            for spec in FTS_INDEXES {
                let report = collect_single_fts_health(&guard, *spec)?;
                if report.repair_plan.is_none() {
                    continue;
                }
                guard.execute_batch(spec.create_sql)?;
                guard.execute_batch(spec.rebuild_sql)?;
                applied_steps.push(format!("fts.rebuild.{}", spec.fts_table));
            }
        }

        let remaining_report = self.state_health_report(Some(DEFAULT_FAST_HASH_WINDOW))?;
        let mut remaining_findings = remaining_report.findings;
        remaining_findings.retain(|finding| finding.subsystem.starts_with("fts"));
        if !request.dry_run && applied_steps.is_empty() && !planned_steps.is_empty() {
            warnings.push("no FTS repair steps were applied despite a non-empty plan".to_owned());
        }

        let audit_event_id = if !request.dry_run && !applied_steps.is_empty() {
            Some(self.append_state_repair_audit_event(
                request,
                backup.as_ref(),
                applied_steps.as_slice(),
                remaining_findings.as_slice(),
            )?)
        } else {
            None
        };

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

        Ok(JournalStateRepairReport {
            schema_version: STATE_REPAIR_REPORT_SCHEMA_VERSION,
            generated_at_unix_ms,
            dry_run: request.dry_run,
            actor_principal: request.actor_principal.clone(),
            backup,
            planned_steps,
            applied_steps,
            skipped_steps,
            warnings,
            remaining_findings,
            restore_instructions,
            audit_event_id,
        })
    }

    /// Runs a WAL checkpoint against the journal connection.
    ///
    /// # Errors
    /// Returns [`JournalError`] if SQLite rejects the checkpoint.
    pub fn checkpoint_wal(
        &self,
        mode: JournalWalCheckpointMode,
    ) -> Result<JournalWalCheckpointReport, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        retry_sqlite_busy(|| checkpoint_wal_for_connection(&guard, mode))
    }

    /// Prepares the rebuildable sidecar directory structure with owner-only permissions.
    ///
    /// # Errors
    /// Returns [`JournalError`] if a directory cannot be created or secured.
    pub fn prepare_sidecar_storage(&self) -> Result<Vec<SidecarIndexDescriptor>, JournalError> {
        let root = sidecar_root_for(self.config.db_path.as_path());
        fs::create_dir_all(root.as_path())
            .map_err(|source| JournalError::CreateDirectory { path: root.clone(), source })?;
        enforce_owner_only_permissions(root.as_path(), 0o700)?;
        for descriptor in sidecar_index_descriptors(self.config.db_path.as_path()) {
            let dir = root.join(descriptor.index_id.as_str());
            fs::create_dir_all(dir.as_path())
                .map_err(|source| JournalError::CreateDirectory { path: dir.clone(), source })?;
            enforce_owner_only_permissions(dir.as_path(), 0o700)?;
        }
        Ok(sidecar_index_descriptors(self.config.db_path.as_path()))
    }

    pub(super) fn ensure_hash_chain_writes_allowed(&self) -> Result<(), JournalError> {
        let guard = self.write_guard.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(mismatch) = guard.blocked.as_ref() else {
            return Ok(());
        };
        if std::env::var(HASH_CHAIN_WRITE_OVERRIDE_ENV)
            .ok()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(HASH_CHAIN_WRITE_OVERRIDE_VALUE))
        {
            return Ok(());
        }
        Err(JournalError::WriteBlockedByHashChainMismatch {
            event_id: mismatch.event_id.clone(),
            reason_code: mismatch.code.clone(),
            fix_hint:
                "run full offline state verification and restore or explicitly override writes"
                    .to_owned(),
        })
    }

    fn block_hash_chain_writes(
        &self,
        mismatch: JournalHashChainMismatch,
    ) -> Result<(), JournalError> {
        let mut guard = self.write_guard.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.blocked = Some(mismatch);
        Ok(())
    }

    fn append_state_repair_audit_event(
        &self,
        request: &JournalStateRepairRequest,
        backup: Option<&JournalStateBackupReport>,
        applied_steps: &[String],
        remaining_findings: &[StateHealthFinding],
    ) -> Result<String, JournalError> {
        let event_id = Ulid::new().to_string();
        let now = current_unix_ms()?;
        let payload = json!({
            "event_name": "state.repair.applied",
            "actor_principal": request.actor_principal,
            "backup_ref": backup.map(|backup| backup.backup_ref.as_str()),
            "applied_steps": applied_steps,
            "remaining_finding_codes": remaining_findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
        });
        self.append(&JournalAppendRequest {
            event_id: event_id.clone(),
            session_id: event_id.clone(),
            run_id: event_id.clone(),
            kind: 0,
            actor: 0,
            timestamp_unix_ms: now,
            payload_json: serde_json::to_vec(&payload)?,
            principal: request.actor_principal.clone(),
            device_id: "state-doctor".to_owned(),
            channel: Some("maintenance".to_owned()),
        })?;
        Ok(event_id)
    }
}

pub(super) fn retry_sqlite_busy<T>(
    mut operation: impl FnMut() -> Result<T, JournalError>,
) -> Result<T, JournalError> {
    let mut last_error = None;
    for attempt in 0..SQLITE_BUSY_RETRY_ATTEMPTS {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if journal_error_is_busy_or_locked(&error) => {
                last_error = Some(error);
                let delay_ms = SQLITE_BUSY_RETRY_BASE_MS.saturating_mul((attempt as u64) + 1);
                thread::sleep(Duration::from_millis(delay_ms));
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        JournalError::InvalidArgument(
            "sqlite busy retry exhausted without a captured error".to_owned(),
        )
    }))
}

fn journal_health_report_for_connection(
    connection: &Connection,
    db_path: &Path,
    hash_chain_enabled: bool,
    hash_scope: JournalHashVerificationScope,
) -> Result<JournalHealthReport, JournalError> {
    let generated_at_unix_ms = current_unix_ms()?;
    let db = collect_db_health(db_path);
    let wal = collect_wal_health(connection, db_path)?;
    let schema = collect_schema_health(connection)?;
    let quick_check = collect_quick_check(connection)?;
    let write_probe = collect_write_probe(connection);
    let hash_chain = verify_hash_chain_for_connection(connection, hash_scope)?;
    let fts = collect_fts_health(connection)?;
    let sidecars = sidecar_index_descriptors(db_path);

    let mut findings = Vec::new();
    if !hash_chain_enabled {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Warning,
            "journal",
            "journal.hash_chain.disabled",
            "journal hash chaining is disabled",
            "re-enable journal hash chaining unless this is an explicit compatibility environment",
            vec![StateHealthEvidenceRef::new(
                "config",
                "journal.hash_chain_enabled",
                "hash_chain_enabled=false",
            )],
        ));
    }
    if wal.journal_mode != "wal" {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Degraded,
            "journal",
            "journal.wal.not_wal",
            format!("journal mode is {}", wal.journal_mode),
            "move the state path to a local disk and restart so SQLite can use WAL mode",
            vec![StateHealthEvidenceRef::new(
                "sqlite_pragma",
                "journal_mode",
                wal.journal_mode.clone(),
            )],
        ));
    }
    if !schema.migrations_table_exists || schema.current_version != Some(schema.expected_version) {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Degraded,
            "journal",
            "journal.schema.version_mismatch",
            "journal schema migrations are missing or behind the current contract",
            "restart palyrad to rerun idempotent migrations before applying repairs",
            vec![StateHealthEvidenceRef::new(
                "sqlite_table",
                "schema_migrations",
                format!(
                    "current={:?} expected={}",
                    schema.current_version, schema.expected_version
                ),
            )],
        ));
    }
    if quick_check.status != "ok" {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Critical,
            "journal",
            "journal.sqlite.quick_check_failed",
            "SQLite quick_check reported integrity errors",
            "stop writes, create an external backup, and inspect with sqlite tooling",
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
    if write_probe.outcome != "ok" {
        findings.push(StateHealthFinding::new(
            StateHealthSeverity::Degraded,
            "journal",
            "journal.write_probe.failed",
            "rolled-back journal write probe failed",
            write_probe.fix_hint.clone().unwrap_or_else(|| {
                "check state path permissions and move the journal to a local writable disk"
                    .to_owned()
            }),
            vec![StateHealthEvidenceRef::new(
                "sqlite_write_probe",
                write_probe.error_class.clone().unwrap_or_else(|| "unknown".to_owned()),
                write_probe.outcome.clone(),
            )],
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
    for report in &fts {
        if report.missing_table {
            findings.push(StateHealthFinding::new(
                StateHealthSeverity::Degraded,
                format!("fts.{}", report.subsystem),
                format!("fts.{}.missing_table", report.subsystem),
                format!("FTS table {} is missing", report.fts_table),
                "run `palyra state repair --fts-only` after reviewing the dry run plan",
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

    let overall_severity = highest_state_health_severity(findings.as_slice());
    Ok(JournalHealthReport {
        schema_version: STATE_HEALTH_SCHEMA_VERSION,
        generated_at_unix_ms,
        subsystem: "journal".to_owned(),
        overall_severity,
        db,
        wal,
        schema,
        quick_check,
        write_probe,
        hash_chain,
        fts,
        sidecars,
        findings,
    })
}

fn collect_db_health(db_path: &Path) -> JournalDbHealth {
    let metadata = fs::metadata(db_path).ok();
    JournalDbHealth {
        path_ref: path_evidence_ref(db_path),
        file_exists: metadata.as_ref().is_some_and(fs::Metadata::is_file),
        file_bytes: metadata.as_ref().map(fs::Metadata::len),
        parent_ref: db_path.parent().map(path_evidence_ref),
        owner_only_permissions_expected: true,
    }
}

fn collect_wal_health(
    connection: &Connection,
    db_path: &Path,
) -> Result<JournalWalHealth, JournalError> {
    let journal_mode = pragma_string(connection, "PRAGMA journal_mode;")?.to_ascii_lowercase();
    let synchronous = pragma_i64(connection, "PRAGMA synchronous;")?.to_string();
    let busy_timeout_ms = pragma_i64(connection, "PRAGMA busy_timeout;")?;
    let wal_path = wal_side_file(db_path, "wal");
    let shm_path = wal_side_file(db_path, "shm");
    let wal_metadata = fs::metadata(wal_path.as_path()).ok();
    let shm_metadata = fs::metadata(shm_path.as_path()).ok();
    Ok(JournalWalHealth {
        journal_mode,
        synchronous,
        busy_timeout_ms,
        wal_file_exists: wal_metadata.as_ref().is_some_and(fs::Metadata::is_file),
        wal_file_bytes: wal_metadata.as_ref().map(fs::Metadata::len),
        shm_file_exists: shm_metadata.as_ref().is_some_and(fs::Metadata::is_file),
        shm_file_bytes: shm_metadata.as_ref().map(fs::Metadata::len),
        last_checkpoint: None,
    })
}

fn collect_schema_health(connection: &Connection) -> Result<JournalSchemaHealth, JournalError> {
    let migrations_table_exists = table_exists(connection, "schema_migrations")?;
    let expected_version = MIGRATIONS.last().map(|migration| migration.version).unwrap_or(0);
    if !migrations_table_exists {
        return Ok(JournalSchemaHealth {
            migrations_table_exists,
            current_version: None,
            expected_version,
            applied_count: 0,
        });
    }
    let (current_version, applied_count) = connection.query_row(
        "SELECT MAX(version), COUNT(*) FROM schema_migrations",
        [],
        |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, i64>(1)?)),
    )?;
    Ok(JournalSchemaHealth {
        migrations_table_exists,
        current_version,
        expected_version,
        applied_count,
    })
}

fn collect_quick_check(connection: &Connection) -> Result<JournalQuickCheckHealth, JournalError> {
    let mut statement = connection.prepare("PRAGMA quick_check;")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let messages = rows.collect::<Result<Vec<_>, _>>()?;
    let ok = messages.len() == 1 && messages.first().is_some_and(|message| message == "ok");
    Ok(JournalQuickCheckHealth {
        status: if ok { "ok" } else { "failed" }.to_owned(),
        checked: true,
        messages,
    })
}

fn collect_write_probe(connection: &Connection) -> JournalWriteProbeHealth {
    let started = Instant::now();
    let result = connection.execute_batch(
        r#"
            SAVEPOINT palyra_state_doctor_write_probe;
            CREATE TEMP TABLE IF NOT EXISTS palyra_state_doctor_write_probe(
                probe_id TEXT PRIMARY KEY,
                created_at_unix_ms INTEGER NOT NULL
            );
            INSERT OR REPLACE INTO palyra_state_doctor_write_probe(probe_id, created_at_unix_ms)
            VALUES ('probe', 0);
            ROLLBACK TO palyra_state_doctor_write_probe;
            RELEASE palyra_state_doctor_write_probe;
        "#,
    );
    match result {
        Ok(()) => JournalWriteProbeHealth {
            attempted: true,
            outcome: "ok".to_owned(),
            rolled_back: true,
            duration_ms: started.elapsed().as_millis() as u64,
            error_class: None,
            fix_hint: None,
        },
        Err(error) => JournalWriteProbeHealth {
            attempted: true,
            outcome: "failed".to_owned(),
            rolled_back: true,
            duration_ms: started.elapsed().as_millis() as u64,
            error_class: Some(sqlite_error_class(&error)),
            fix_hint: Some(write_probe_fix_hint(&error)),
        },
    }
}

fn verify_hash_chain_for_connection(
    connection: &Connection,
    scope: JournalHashVerificationScope,
) -> Result<JournalHashChainVerificationReport, JournalError> {
    let total_events = count_table_rows(connection, "journal_events").unwrap_or(0).max(0) as usize;
    if !table_exists(connection, "journal_events")? {
        return Ok(JournalHashChainVerificationReport {
            scope: scope.label().to_owned(),
            checked_events: 0,
            total_events: 0,
            status: "missing_journal_events_table".to_owned(),
            mismatch: Some(JournalHashChainMismatch {
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

    let rows = hash_chain_rows(connection, scope)?;
    if rows.is_empty() {
        return Ok(JournalHashChainVerificationReport {
            scope: scope.label().to_owned(),
            checked_events: 0,
            total_events,
            status: "ok".to_owned(),
            mismatch: None,
        });
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
        let request = JournalAppendRequest {
            event_id: row.event_id.clone(),
            session_id: row.session_id.clone(),
            run_id: row.run_id.clone(),
            kind: row.kind,
            actor: row.actor,
            timestamp_unix_ms: row.timestamp_unix_ms,
            payload_json: row.payload_json.as_bytes().to_vec(),
            principal: row.principal.clone(),
            device_id: row.device_id.clone(),
            channel: row.channel.clone(),
        };
        let expected_hash =
            compute_hash(row.prev_hash.as_deref(), &request, row.payload_json.as_str());
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

    Ok(JournalHashChainVerificationReport {
        scope: scope.label().to_owned(),
        checked_events: rows.len(),
        total_events,
        status: "ok".to_owned(),
        mismatch: None,
    })
}

struct HashMismatchReportInput<'a> {
    scope: JournalHashVerificationScope,
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

fn hash_mismatch_report(input: HashMismatchReportInput<'_>) -> JournalHashChainVerificationReport {
    JournalHashChainVerificationReport {
        scope: input.scope.label().to_owned(),
        checked_events: input.checked_events,
        total_events: input.total_events,
        status: "mismatch".to_owned(),
        mismatch: Some(JournalHashChainMismatch {
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
    scope: JournalHashVerificationScope,
) -> Result<Vec<HashChainRow>, JournalError> {
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
    let mut rows = if let Some(limit) = scope.limit() {
        statement.query(params![limit as i64])?
    } else {
        statement.query([])?
    };
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        records.push(HashChainRow {
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
        });
    }
    Ok(records)
}

fn collect_fts_health(connection: &Connection) -> Result<Vec<FtsHealthReport>, JournalError> {
    FTS_INDEXES.iter().copied().map(|spec| collect_single_fts_health(connection, spec)).collect()
}

fn collect_single_fts_health(
    connection: &Connection,
    spec: FtsIndexSpec,
) -> Result<FtsHealthReport, JournalError> {
    let authoritative_exists = table_exists(connection, spec.authoritative_table)?;
    let fts_exists = table_exists(connection, spec.fts_table)?;
    let authoritative_rows = if authoritative_exists {
        count_table_rows(connection, spec.authoritative_table)?
    } else {
        0
    };
    if !fts_exists {
        return Ok(FtsHealthReport {
            subsystem: spec.subsystem.to_owned(),
            fts_table: spec.fts_table.to_owned(),
            authoritative_table: spec.authoritative_table.to_owned(),
            authoritative_rows,
            indexed_rows: None,
            missing_table: true,
            orphan_rows: None,
            lagging_rows: None,
            status: "missing_table".to_owned(),
            repair_plan: Some(fts_repair_plan(spec, true)),
        });
    }
    let indexed_rows = count_table_rows(connection, spec.fts_table)?;
    let orphan_rows = fts_orphan_rows(connection, spec)?;
    let lagging_rows = fts_lagging_rows(connection, spec)?;
    let drifted = orphan_rows > 0 || lagging_rows > 0;
    Ok(FtsHealthReport {
        subsystem: spec.subsystem.to_owned(),
        fts_table: spec.fts_table.to_owned(),
        authoritative_table: spec.authoritative_table.to_owned(),
        authoritative_rows,
        indexed_rows: Some(indexed_rows),
        missing_table: false,
        orphan_rows: Some(orphan_rows),
        lagging_rows: Some(lagging_rows),
        status: if drifted { "drifted" } else { "ok" }.to_owned(),
        repair_plan: drifted.then(|| fts_repair_plan(spec, false)),
    })
}

fn fts_repair_plan(spec: FtsIndexSpec, missing_table: bool) -> FtsRepairPlan {
    let mut targeted_steps = Vec::new();
    if missing_table {
        targeted_steps.push(format!("fts.create.{}", spec.fts_table));
    }
    targeted_steps.push(format!("fts.rebuild.{}", spec.fts_table));
    FtsRepairPlan {
        plan_id: format!("fts-rebuild-{}", spec.fts_table),
        subsystem: spec.subsystem.to_owned(),
        safety_level: "rebuildable_index_only".to_owned(),
        dry_run_supported: true,
        targeted_steps,
    }
}

fn fts_orphan_rows(connection: &Connection, spec: FtsIndexSpec) -> Result<i64, JournalError> {
    let sql = format!(
        "SELECT COUNT(*) FROM {fts} f LEFT JOIN {auth} a ON f.{key} = a.{key} WHERE a.{key} IS NULL",
        fts = spec.fts_table,
        auth = spec.authoritative_table,
        key = spec.key_column
    );
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn fts_lagging_rows(connection: &Connection, spec: FtsIndexSpec) -> Result<i64, JournalError> {
    let sql = format!(
        "SELECT COUNT(*) FROM {auth} a LEFT JOIN {fts} f ON f.{key} = a.{key} WHERE f.{key} IS NULL",
        fts = spec.fts_table,
        auth = spec.authoritative_table,
        key = spec.key_column
    );
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn checkpoint_wal_for_connection(
    connection: &Connection,
    mode: JournalWalCheckpointMode,
) -> Result<JournalWalCheckpointReport, JournalError> {
    let started = Instant::now();
    let sql = format!("PRAGMA wal_checkpoint({});", mode.sql_label());
    let (busy, log_frames, checkpointed_frames) =
        connection.query_row(sql.as_str(), [], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
        })?;
    Ok(JournalWalCheckpointReport {
        mode: mode.wire_label().to_owned(),
        busy,
        log_frames,
        checkpointed_frames,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

fn create_state_repair_backup(
    connection: &Connection,
    db_path: &Path,
) -> Result<JournalStateBackupReport, JournalError> {
    let created_at_unix_ms = current_unix_ms()?;
    let backup_dir = db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("backups")
        .join("state-repair");
    fs::create_dir_all(backup_dir.as_path())
        .map_err(|source| JournalError::CreateDirectory { path: backup_dir.clone(), source })?;
    enforce_owner_only_permissions(backup_dir.as_path(), 0o700)?;
    let backup_path =
        backup_dir.join(format!("journal-{}-{}.sqlite3", created_at_unix_ms, Ulid::new()));
    connection.execute("VACUUM INTO ?1", params![backup_path.to_string_lossy().as_ref()])?;
    enforce_owner_only_permissions(backup_path.as_path(), 0o600)?;
    let size_bytes =
        fs::metadata(backup_path.as_path()).map(|metadata| metadata.len()).unwrap_or(0);
    Ok(JournalStateBackupReport {
        backup_ref: path_evidence_ref(backup_path.as_path()),
        created_at_unix_ms,
        size_bytes,
        owner_only_permissions: true,
    })
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

fn table_exists(connection: &Connection, table: &str) -> Result<bool, JournalError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view') LIMIT 1",
            params![table],
            |_row| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(Into::into)
}

fn count_table_rows(connection: &Connection, table: &str) -> Result<i64, JournalError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection.query_row(sql.as_str(), [], |row| row.get(0)).map_err(Into::into)
}

fn pragma_string(connection: &Connection, sql: &str) -> Result<String, JournalError> {
    connection.query_row(sql, [], |row| row.get::<_, String>(0)).map_err(Into::into)
}

fn pragma_i64(connection: &Connection, sql: &str) -> Result<i64, JournalError> {
    connection.query_row(sql, [], |row| row.get::<_, i64>(0)).map_err(Into::into)
}

fn wal_side_file(db_path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}-{suffix}", db_path.display()))
}

fn path_evidence_ref(path: &Path) -> String {
    let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or("path");
    let digest = sha256_hex(path.to_string_lossy().as_bytes());
    format!("{file_name}:sha256:{}", &digest[..12])
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sqlite_error_class(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(inner, _) => {
            format!("{:?}", inner.code).to_ascii_lowercase()
        }
        rusqlite::Error::ExecuteReturnedResults => "execute_returned_results".to_owned(),
        rusqlite::Error::QueryReturnedNoRows => "query_returned_no_rows".to_owned(),
        _ => "sqlite_error".to_owned(),
    }
}

fn write_probe_fix_hint(error: &rusqlite::Error) -> String {
    if sqlite_error_is_busy_or_locked(error) {
        "retry after active writers finish; if this recurs, move the state path to a local disk"
            .to_owned()
    } else if sqlite_error_class(error).contains("readonly") {
        "fix journal file permissions or run with a writable state root".to_owned()
    } else {
        "inspect journal permissions and SQLite health before applying repair".to_owned()
    }
}

fn safe_sqlite_error_summary(message: &str) -> String {
    message.chars().take(160).collect()
}

fn journal_error_is_busy_or_locked(error: &JournalError) -> bool {
    match error {
        JournalError::Sqlite(error) => sqlite_error_is_busy_or_locked(error),
        _ => false,
    }
}

fn sqlite_error_is_busy_or_locked(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if matches!(
                inner.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(test)]
pub(crate) fn state_crash_replay_fixtures() -> Vec<StateCrashReplayFixture> {
    vec![
        StateCrashReplayFixture {
            fixture_id: "state.repair.after_backup_before_rebuild",
            initial_state: "healthy_journal_with_drifted_fts",
            crash_point: "after_backup_before_repair_step",
            expected_recovery: "backup_remains_and_doctor_reports_original_fts_drift",
            expected_health_report: "journal_health_report.v1",
        },
        StateCrashReplayFixture {
            fixture_id: "state.checkpoint.after_request",
            initial_state: "wal_journal_with_frames",
            crash_point: "after_checkpoint_request_before_operator_ack",
            expected_recovery: "doctor_reports_wal_state_without_replaying_mutation",
            expected_health_report: "journal_health_report.v1",
        },
        StateCrashReplayFixture {
            fixture_id: "state.hash_verify.after_mismatch",
            initial_state: "journal_with_corrupt_payload_digest",
            crash_point: "after_hash_verify_before_next_append",
            expected_recovery: "next_append_is_blocked_until_operator_override",
            expected_health_report: "journal_hash_chain_verification.v1",
        },
    ]
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;

    use rusqlite::{params, Connection};
    use tempfile::tempdir;

    use super::*;
    use crate::journal::{JournalConfig, JournalStore};

    fn test_config(path: PathBuf, hash_chain_enabled: bool) -> JournalConfig {
        JournalConfig {
            db_path: path,
            hash_chain_enabled,
            max_payload_bytes: 64 * 1024,
            max_events: 10_000,
        }
    }

    fn append_event(store: &JournalStore, event_id: &str, payload: &[u8]) {
        store
            .append(&JournalAppendRequest {
                event_id: event_id.to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAR".to_owned(),
                kind: 1,
                actor: 1,
                timestamp_unix_ms: 1,
                payload_json: payload.to_vec(),
                principal: "user:test".to_owned(),
                device_id: "device:test".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("event append should succeed");
    }

    #[test]
    fn state_health_report_redacts_db_paths_and_reports_ok() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");
        append_event(&store, "01ARZ3NDEKTSV4RRFFQ69G5FA1", br#"{"message":"ok"}"#);

        let report = store.state_health_report(Some(8)).expect("report should build");
        let encoded = serde_json::to_string(&report).expect("report should serialize");

        assert_eq!(report.overall_severity, StateHealthSeverity::Ok);
        assert!(!encoded.contains(temp.path().to_string_lossy().as_ref()));
        assert!(report.db.path_ref.contains("journal.sqlite3:sha256:"));
    }

    #[test]
    fn hash_chain_verifier_detects_corrupt_payload_without_raw_payload() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");
        append_event(&store, "01ARZ3NDEKTSV4RRFFQ69G5FB1", br#"{"message":"one"}"#);
        append_event(&store, "01ARZ3NDEKTSV4RRFFQ69G5FB2", br#"{"message":"two"}"#);
        drop(store);

        let connection = Connection::open(db_path.as_path()).expect("db should open");
        connection
            .execute_batch("DROP TRIGGER trg_journal_events_prevent_update;")
            .expect("test should drop append-only trigger");
        connection
            .execute(
                "UPDATE journal_events SET payload_json = ?1 WHERE event_ulid = ?2",
                params![r#"{"message":"corrupt-secret-token"}"#, "01ARZ3NDEKTSV4RRFFQ69G5FB2"],
            )
            .expect("test corruption should be written");
        drop(connection);

        let store = JournalStore::open(test_config(db_path, true)).expect("journal should reopen");
        let report = store
            .verify_hash_chain(JournalHashVerificationScope::Full)
            .expect("verification should run");
        let mismatch = report.mismatch.expect("corruption should be detected");

        assert_eq!(mismatch.code, "journal.hash_chain.hash_mismatch");
        assert!(!mismatch.safe_summary.contains("corrupt-secret-token"));
        let append_error = store
            .append(&JournalAppendRequest {
                event_id: "01ARZ3NDEKTSV4RRFFQ69G5FB3".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAR".to_owned(),
                kind: 1,
                actor: 1,
                timestamp_unix_ms: 2,
                payload_json: br#"{"message":"blocked"}"#.to_vec(),
                principal: "user:test".to_owned(),
                device_id: "device:test".to_owned(),
                channel: None,
            })
            .expect_err("hash mismatch should block subsequent appends");
        assert!(matches!(append_error, JournalError::WriteBlockedByHashChainMismatch { .. }));
    }

    #[test]
    fn fts_repair_dry_run_does_not_recreate_missing_table() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");
        {
            let connection = Connection::open(db_path.as_path()).expect("db should open");
            connection
                .execute_batch("DROP TABLE memory_items_fts;")
                .expect("test should remove FTS table");
        }

        let report = store
            .repair_state(&JournalStateRepairRequest {
                dry_run: true,
                fts_only: true,
                actor_principal: "admin:test".to_owned(),
            })
            .expect("dry run should succeed");
        let connection = Connection::open(db_path.as_path()).expect("db should open");

        assert!(report.planned_steps.iter().any(|step| step == "fts.create.memory_items_fts"));
        assert!(report.applied_steps.is_empty());
        assert!(!table_exists(&connection, "memory_items_fts").expect("table probe should run"));
    }

    #[test]
    fn fts_repair_creates_backup_and_rebuilds_missing_table() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");
        {
            let connection = Connection::open(db_path.as_path()).expect("db should open");
            connection
                .execute_batch("DROP TABLE memory_items_fts;")
                .expect("test should remove FTS table");
        }

        let report = store
            .repair_state(&JournalStateRepairRequest {
                dry_run: false,
                fts_only: true,
                actor_principal: "admin:test".to_owned(),
            })
            .expect("repair should succeed");
        let connection = Connection::open(db_path.as_path()).expect("db should open");

        assert!(report.backup.is_some(), "mutating repair must create backup");
        assert!(report.applied_steps.iter().any(|step| step == "fts.rebuild.memory_items_fts"));
        assert!(table_exists(&connection, "memory_items_fts").expect("table probe should run"));
    }

    #[test]
    fn sidecar_storage_prepares_owner_only_directories() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");

        let descriptors = store.prepare_sidecar_storage().expect("sidecars should be prepared");

        assert!(!descriptors.is_empty());
        for descriptor in descriptors {
            let dir = sidecar_root_for(db_path.as_path()).join(descriptor.index_id);
            assert!(dir.is_dir(), "sidecar directory should exist: {}", dir.display());
        }
    }

    #[test]
    fn crash_replay_fixtures_are_test_only_and_cover_phase_one_cases() {
        let fixtures = state_crash_replay_fixtures();

        assert!(fixtures
            .iter()
            .any(|fixture| fixture.crash_point == "after_backup_before_repair_step"));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.crash_point == "after_checkpoint_request_before_operator_ack"));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.crash_point == "after_hash_verify_before_next_append"));
    }

    #[test]
    fn wal_checkpoint_reports_sqlite_frame_counts() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store = JournalStore::open(test_config(db_path, true)).expect("journal should open");
        append_event(&store, "01ARZ3NDEKTSV4RRFFQ69G5FC1", br#"{"message":"checkpoint"}"#);

        let report =
            store.checkpoint_wal(JournalWalCheckpointMode::Passive).expect("checkpoint should run");

        assert_eq!(report.mode, "passive");
        assert!(report.log_frames >= 0);
        assert!(report.checkpointed_frames >= 0);
    }

    #[test]
    fn retry_sqlite_busy_retries_transient_busy_errors() {
        let mut attempts = 0;

        let value = retry_sqlite_busy(|| {
            attempts += 1;
            if attempts == 1 {
                return Err(JournalError::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error {
                        code: rusqlite::ErrorCode::DatabaseBusy,
                        extended_code: rusqlite::ffi::SQLITE_BUSY,
                    },
                    Some("busy".to_owned()),
                )));
            }
            Ok("ok")
        })
        .expect("retry should recover");

        assert_eq!(value, "ok");
        assert_eq!(attempts, 2);
    }

    #[cfg(unix)]
    #[test]
    fn mutating_repair_backup_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("journal.sqlite3");
        let store =
            JournalStore::open(test_config(db_path.clone(), true)).expect("journal should open");
        {
            let connection = Connection::open(db_path.as_path()).expect("db should open");
            connection
                .execute_batch("DROP TABLE memory_items_fts;")
                .expect("test should remove FTS table");
        }

        let report = store
            .repair_state(&JournalStateRepairRequest {
                dry_run: false,
                fts_only: true,
                actor_principal: "admin:test".to_owned(),
            })
            .expect("repair should succeed");
        let backup_dir = db_path.parent().unwrap().join("backups").join("state-repair");
        let mode =
            fs::metadata(backup_dir).expect("backup dir metadata").permissions().mode() & 0o777;

        assert_eq!(mode, 0o700);
        assert!(report.backup.expect("backup report").owner_only_permissions);
    }
}
