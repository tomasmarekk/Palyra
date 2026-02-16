use std::{
    fmt, fs,
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::orchestrator::RunLifecycleState;

const REDACTED_MARKER: &str = "<redacted>";
const MAX_RECENT_EVENTS_LIMIT: usize = 500;
const SENSITIVE_KEY_FRAGMENTS: &[&str] = &[
    "secret",
    "token",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "private_key",
    "proof",
    "pin",
    "signature",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalConfig {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalAppendRequest {
    pub event_id: String,
    pub session_id: String,
    pub run_id: String,
    pub kind: i32,
    pub actor: i32,
    pub timestamp_unix_ms: i64,
    pub payload_json: Vec<u8>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalAppendOutcome {
    pub redacted: bool,
    pub hash: Option<String>,
    pub prev_hash: Option<String>,
    pub write_duration: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JournalEventRecord {
    pub seq: i64,
    pub event_id: String,
    pub session_id: String,
    pub run_id: String,
    pub kind: i32,
    pub actor: i32,
    pub timestamp_unix_ms: i64,
    pub payload_json: String,
    pub redacted: bool,
    pub hash: Option<String>,
    pub prev_hash: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorSessionUpsertRequest {
    pub session_id: String,
    pub principal: String,
    pub device_id: String,
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorRunStartRequest {
    pub run_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorUsageDelta {
    pub run_id: String,
    pub prompt_tokens_delta: u64,
    pub completion_tokens_delta: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorTapeAppendRequest {
    pub run_id: String,
    pub seq: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorCancelRequest {
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorTapeRecord {
    pub seq: i64,
    pub event_type: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrchestratorRunSnapshot {
    pub run_id: String,
    pub session_id: String,
    pub state: String,
    pub cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub created_at_unix_ms: i64,
    pub started_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub tape: Vec<OrchestratorTapeRecord>,
}

#[derive(thiserror::Error, Debug)]
pub enum JournalError {
    #[error("journal db path cannot be empty")]
    EmptyPath,
    #[error("journal db path cannot contain parent traversal ('..'): {path}")]
    ParentTraversalPath { path: String },
    #[error("failed to create journal directory at {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open journal sqlite database at {path}: {source}")]
    OpenConnection {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("journal lock poisoned")]
    LockPoisoned,
    #[error("journal event already exists: {event_id}")]
    DuplicateEventId { event_id: String },
    #[error("orchestrator run already exists: {run_id}")]
    DuplicateRunId { run_id: String },
    #[error("orchestrator tape sequence already exists for run {run_id} at seq {seq}")]
    DuplicateTapeSequence { run_id: String, seq: i64 },
    #[error("orchestrator run not found: {run_id}")]
    RunNotFound { run_id: String },
    #[error("journal sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("failed to serialize journal payload: {0}")]
    SerializePayload(#[from] serde_json::Error),
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_event_journal",
        sql: r#"
            CREATE TABLE IF NOT EXISTS journal_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_ulid TEXT NOT NULL UNIQUE,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                kind INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                timestamp_unix_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                redacted INTEGER NOT NULL,
                hash TEXT,
                prev_hash TEXT,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_journal_events_run_ts
                ON journal_events(run_ulid, timestamp_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_journal_events_created_at
                ON journal_events(created_at_unix_ms);
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_update
            BEFORE UPDATE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_delete
            BEFORE DELETE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
        "#,
    },
    Migration {
        version: 2,
        name: "create_orchestrator_tables",
        sql: r#"
            CREATE TABLE IF NOT EXISTS orchestrator_sessions (
                session_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orchestrator_runs (
                run_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                state TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                cancel_reason TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                updated_at_unix_ms INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_session
                ON orchestrator_runs(session_ulid);

            CREATE TABLE IF NOT EXISTS orchestrator_tape (
                run_ulid TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(run_ulid, seq),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_tape_run_seq
                ON orchestrator_tape(run_ulid, seq);
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_update
            BEFORE UPDATE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_delete
            BEFORE DELETE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
        "#,
    },
];

pub struct JournalStore {
    config: JournalConfig,
    connection: Mutex<Connection>,
}

impl fmt::Debug for JournalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalStore")
            .field("db_path", &self.config.db_path)
            .field("hash_chain_enabled", &self.config.hash_chain_enabled)
            .finish()
    }
}

impl JournalStore {
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        validate_db_path(&config.db_path)?;
        if let Some(parent) = config.db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|source| JournalError::CreateDirectory {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
        }

        let mut connection = Connection::open(&config.db_path).map_err(|source| {
            JournalError::OpenConnection { path: config.db_path.clone(), source }
        })?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )?;

        apply_migrations(&mut connection)?;
        Ok(Self { config, connection: Mutex::new(connection) })
    }

    pub fn append(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<JournalAppendOutcome, JournalError> {
        let started_at = Instant::now();
        let (payload_json, redacted) = sanitize_payload(&request.payload_json)?;
        let created_at_unix_ms = current_unix_ms()?;

        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;

        let prev_hash = if self.config.hash_chain_enabled {
            transaction
                .query_row("SELECT hash FROM journal_events ORDER BY seq DESC LIMIT 1", [], |row| {
                    row.get::<_, Option<String>>(0)
                })
                .optional()?
                .flatten()
        } else {
            None
        };

        let hash = if self.config.hash_chain_enabled {
            Some(compute_hash(prev_hash.as_deref(), request, &payload_json))
        } else {
            None
        };

        match transaction.execute(
            r#"
                INSERT INTO journal_events (
                    event_ulid,
                    session_ulid,
                    run_ulid,
                    kind,
                    actor,
                    timestamp_unix_ms,
                    payload_json,
                    redacted,
                    hash,
                    prev_hash,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                request.event_id,
                request.session_id,
                request.run_id,
                request.kind,
                request.actor,
                request.timestamp_unix_ms,
                payload_json,
                redacted as i64,
                hash,
                prev_hash,
                request.principal,
                request.device_id,
                request.channel,
                created_at_unix_ms,
            ],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("journal_events.event_ulid"))
                            .unwrap_or(false)) =>
            {
                return Err(JournalError::DuplicateEventId { event_id: request.event_id.clone() });
            }
            Err(error) => return Err(error.into()),
        }
        transaction.commit()?;

        Ok(JournalAppendOutcome { redacted, hash, prev_hash, write_duration: started_at.elapsed() })
    }

    pub fn total_events(&self) -> Result<usize, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let total_events: i64 =
            guard.query_row("SELECT COUNT(*) FROM journal_events", [], |row| row.get(0))?;
        Ok(total_events as usize)
    }

    pub fn latest_hash(&self) -> Result<Option<String>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard
            .query_row("SELECT hash FROM journal_events ORDER BY seq DESC LIMIT 1", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .map(|row| row.flatten())
            .map_err(JournalError::from)
    }

    pub fn recent(&self, requested_limit: usize) -> Result<Vec<JournalEventRecord>, JournalError> {
        let limit = requested_limit.clamp(1, MAX_RECENT_EVENTS_LIMIT) as i64;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
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
                    redacted,
                    hash,
                    prev_hash,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms
                FROM journal_events
                ORDER BY seq DESC
                LIMIT ?1
            "#,
        )?;
        let mut rows = statement.query(params![limit])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(JournalEventRecord {
                seq: row.get(0)?,
                event_id: row.get(1)?,
                session_id: row.get(2)?,
                run_id: row.get(3)?,
                kind: row.get(4)?,
                actor: row.get(5)?,
                timestamp_unix_ms: row.get(6)?,
                payload_json: row.get(7)?,
                redacted: row.get::<_, i64>(8)? == 1,
                hash: row.get(9)?,
                prev_hash: row.get(10)?,
                principal: row.get(11)?,
                device_id: row.get(12)?,
                channel: row.get(13)?,
                created_at_unix_ms: row.get(14)?,
            });
        }
        Ok(events)
    }

    pub fn upsert_orchestrator_session(
        &self,
        request: &OrchestratorSessionUpsertRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.execute(
            r#"
                INSERT INTO orchestrator_sessions (
                    session_ulid,
                    principal,
                    device_id,
                    channel,
                    created_at_unix_ms,
                    updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                ON CONFLICT(session_ulid) DO UPDATE SET
                    principal = excluded.principal,
                    device_id = excluded.device_id,
                    channel = excluded.channel,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                request.session_id,
                request.principal,
                request.device_id,
                request.channel,
                now,
            ],
        )?;
        Ok(())
    }

    pub fn start_orchestrator_run(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO orchestrator_runs (
                    run_ulid,
                    session_ulid,
                    state,
                    cancel_requested,
                    cancel_reason,
                    created_at_unix_ms,
                    started_at_unix_ms,
                    completed_at_unix_ms,
                    updated_at_unix_ms,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    last_error
                ) VALUES (?1, ?2, ?3, 0, NULL, ?4, ?4, NULL, ?4, 0, 0, 0, NULL)
            "#,
            params![request.run_id, request.session_id, RunLifecycleState::Accepted.as_str(), now,],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("orchestrator_runs.run_ulid"))
                            .unwrap_or(false)) =>
            {
                Err(JournalError::DuplicateRunId { run_id: request.run_id.clone() })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn update_orchestrator_run_state(
        &self,
        run_id: &str,
        state: RunLifecycleState,
        error_message: Option<&str>,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let completed_at = if state.is_terminal() { Some(now) } else { None };
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    state = ?2,
                    completed_at_unix_ms = COALESCE(?3, completed_at_unix_ms),
                    updated_at_unix_ms = ?4,
                    last_error = COALESCE(?5, last_error)
                WHERE run_ulid = ?1
            "#,
            params![run_id, state.as_str(), completed_at, now, error_message],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: run_id.to_owned() });
        }
        Ok(())
    }

    pub fn add_orchestrator_usage(
        &self,
        delta: &OrchestratorUsageDelta,
    ) -> Result<(), JournalError> {
        if delta.prompt_tokens_delta == 0 && delta.completion_tokens_delta == 0 {
            return Ok(());
        }
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    prompt_tokens = prompt_tokens + ?2,
                    completion_tokens = completion_tokens + ?3,
                    total_tokens = total_tokens + (?2 + ?3),
                    updated_at_unix_ms = ?4
                WHERE run_ulid = ?1
            "#,
            params![
                delta.run_id,
                delta.prompt_tokens_delta as i64,
                delta.completion_tokens_delta as i64,
                now,
            ],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: delta.run_id.clone() });
        }
        Ok(())
    }

    pub fn append_orchestrator_tape_event(
        &self,
        request: &OrchestratorTapeAppendRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        match guard.execute(
            r#"
                INSERT INTO orchestrator_tape (
                    run_ulid,
                    seq,
                    event_type,
                    payload_json,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![request.run_id, request.seq, request.event_type, request.payload_json, now,],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, message))
                if error.code == ErrorCode::ConstraintViolation
                    && (error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
                        || error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                        || message
                            .as_deref()
                            .map(|value| value.contains("orchestrator_tape"))
                            .unwrap_or(false)) =>
            {
                Err(JournalError::DuplicateTapeSequence {
                    run_id: request.run_id.clone(),
                    seq: request.seq,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn request_orchestrator_cancel(
        &self,
        request: &OrchestratorCancelRequest,
    ) -> Result<(), JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let updated = guard.execute(
            r#"
                UPDATE orchestrator_runs
                SET
                    cancel_requested = 1,
                    cancel_reason = ?2,
                    updated_at_unix_ms = ?3
                WHERE run_ulid = ?1
            "#,
            params![request.run_id, request.reason, now],
        )?;
        if updated == 0 {
            return Err(JournalError::RunNotFound { run_id: request.run_id.clone() });
        }
        Ok(())
    }

    pub fn is_orchestrator_cancel_requested(&self, run_id: &str) -> Result<bool, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let value = guard
            .query_row(
                "SELECT cancel_requested FROM orchestrator_runs WHERE run_ulid = ?1",
                params![run_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(value) = value else {
            return Err(JournalError::RunNotFound { run_id: run_id.to_owned() });
        };
        Ok(value == 1)
    }

    pub fn orchestrator_tape(
        &self,
        run_id: &str,
    ) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_orchestrator_tape(&guard, run_id)
    }

    pub fn orchestrator_run_snapshot(
        &self,
        run_id: &str,
    ) -> Result<Option<OrchestratorRunSnapshot>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    runs.run_ulid,
                    runs.session_ulid,
                    runs.state,
                    runs.cancel_requested,
                    runs.cancel_reason,
                    sessions.principal,
                    sessions.device_id,
                    sessions.channel,
                    runs.prompt_tokens,
                    runs.completion_tokens,
                    runs.total_tokens,
                    runs.created_at_unix_ms,
                    runs.started_at_unix_ms,
                    runs.completed_at_unix_ms,
                    runs.updated_at_unix_ms,
                    runs.last_error
                FROM orchestrator_runs AS runs
                INNER JOIN orchestrator_sessions AS sessions
                    ON sessions.session_ulid = runs.session_ulid
                WHERE runs.run_ulid = ?1
            "#,
        )?;
        let row = statement
            .query_row(params![run_id], |row| {
                let raw_state: String = row.get(2)?;
                let normalized_state = RunLifecycleState::from_str(raw_state.as_str())
                    .map(|state| state.as_str().to_owned())
                    .unwrap_or(raw_state);
                Ok(OrchestratorRunSnapshot {
                    run_id: row.get(0)?,
                    session_id: row.get(1)?,
                    state: normalized_state,
                    cancel_requested: row.get::<_, i64>(3)? == 1,
                    cancel_reason: row.get(4)?,
                    principal: row.get(5)?,
                    device_id: row.get(6)?,
                    channel: row.get(7)?,
                    prompt_tokens: row.get::<_, i64>(8)? as u64,
                    completion_tokens: row.get::<_, i64>(9)? as u64,
                    total_tokens: row.get::<_, i64>(10)? as u64,
                    created_at_unix_ms: row.get(11)?,
                    started_at_unix_ms: row.get(12)?,
                    completed_at_unix_ms: row.get(13)?,
                    updated_at_unix_ms: row.get(14)?,
                    last_error: row.get(15)?,
                    tape: Vec::new(),
                })
            })
            .optional()?;
        let Some(mut snapshot) = row else {
            return Ok(None);
        };
        snapshot.tape = load_orchestrator_tape(&guard, run_id)?;
        Ok(Some(snapshot))
    }
}

fn load_orchestrator_tape(
    connection: &Connection,
    run_id: &str,
) -> Result<Vec<OrchestratorTapeRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT seq, event_type, payload_json
            FROM orchestrator_tape
            WHERE run_ulid = ?1
            ORDER BY seq ASC
        "#,
    )?;
    let mut rows = statement.query(params![run_id])?;
    let mut tape = Vec::new();
    while let Some(row) = rows.next()? {
        tape.push(OrchestratorTapeRecord {
            seq: row.get(0)?,
            event_type: row.get(1)?,
            payload_json: row.get(2)?,
        });
    }
    Ok(tape)
}

fn apply_migrations(connection: &mut Connection) -> Result<(), JournalError> {
    connection.execute_batch(
        r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at_unix_ms INTEGER NOT NULL
            )
        "#,
    )?;

    for migration in MIGRATIONS {
        let already_applied = connection
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1 LIMIT 1",
                params![migration.version],
                |_row| Ok(()),
            )
            .optional()?
            .is_some();
        if already_applied {
            continue;
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(migration.sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations (version, name, applied_at_unix_ms) VALUES (?1, ?2, ?3)",
            params![migration.version, migration.name, current_unix_ms()?],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn sanitize_payload(raw_payload: &[u8]) -> Result<(String, bool), JournalError> {
    if raw_payload.is_empty() {
        return Ok(("{}".to_owned(), false));
    }

    let raw_text = match std::str::from_utf8(raw_payload) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                json!({
                    "redacted": true,
                    "reason": "binary_or_non_utf8_payload",
                    "bytes": raw_payload.len(),
                })
                .to_string(),
                true,
            ));
        }
    };

    let mut value: Value = match serde_json::from_str(raw_text) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                json!({
                    "redacted": true,
                    "reason": "non_json_payload",
                    "bytes": raw_payload.len(),
                })
                .to_string(),
                true,
            ));
        }
    };

    let redacted = redact_value(&mut value, None);
    Ok((serde_json::to_string(&value)?, redacted))
}

fn redact_value(value: &mut Value, key_context: Option<&str>) -> bool {
    match value {
        Value::Object(object) => {
            let mut redacted = false;
            for (key, child) in object.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String(REDACTED_MARKER.to_owned());
                    redacted = true;
                    continue;
                }
                redacted |= redact_value(child, Some(key.as_str()));
            }
            redacted
        }
        Value::Array(items) => {
            let mut redacted = false;
            for item in items {
                redacted |= redact_value(item, key_context);
            }
            redacted
        }
        Value::String(text) => {
            if key_context.map(is_sensitive_key).unwrap_or(false) || looks_like_secret(text) {
                *value = Value::String(REDACTED_MARKER.to_owned());
                true
            } else {
                false
            }
        }
        _ => key_context.map(is_sensitive_key).unwrap_or(false),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    SENSITIVE_KEY_FRAGMENTS.iter().any(|fragment| normalized.contains(fragment))
}

fn looks_like_secret(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("bearer ")
        || normalized.starts_with("sk-")
        || normalized.contains("api_key=")
        || normalized.contains("secret=")
        || normalized.contains("token=")
}

fn compute_hash(
    prev_hash: Option<&str>,
    request: &JournalAppendRequest,
    payload_json: &str,
) -> String {
    let mut hasher = Sha256::new();
    if let Some(prev_hash) = prev_hash {
        hasher.update(prev_hash.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(request.event_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.session_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(request.kind.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.actor.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.timestamp_unix_ms.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(request.principal.as_bytes());
    hasher.update(b"|");
    hasher.update(request.device_id.as_bytes());
    hasher.update(b"|");
    if let Some(channel) = &request.channel {
        hasher.update(channel.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(payload_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn current_unix_ms() -> Result<i64, JournalError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(now.as_millis() as i64)
}

fn validate_db_path(path: &Path) -> Result<(), JournalError> {
    let path_text = path.to_string_lossy();
    if path_text.trim().is_empty() {
        return Err(JournalError::EmptyPath);
    }
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(JournalError::ParentTraversalPath { path: path_text.into_owned() });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use rusqlite::{params, Connection};

    use crate::orchestrator::RunLifecycleState;

    use super::{
        JournalAppendRequest, JournalConfig, JournalError, JournalStore, OrchestratorCancelRequest,
        OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
        OrchestratorTapeAppendRequest, OrchestratorUsageDelta,
    };

    fn build_request(event_id: &str, payload_json: &[u8]) -> JournalAppendRequest {
        JournalAppendRequest {
            event_id: event_id.to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            kind: 1,
            actor: 1,
            timestamp_unix_ms: 1_730_000_000_000,
            payload_json: payload_json.to_vec(),
            principal: "user:ops".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("cli".to_owned()),
        }
    }

    fn upsert_orchestrator_session(store: &JournalStore, session_id: &str) {
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
    }

    fn temp_db_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-journal-test-{nonce}-{}.sqlite3", std::process::id()))
    }

    #[test]
    fn open_applies_initial_migration() {
        let db_path = temp_db_path();
        let _store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: false,
        })
        .expect("journal store should open");

        let connection = Connection::open(db_path).expect("journal db should open");
        let migration_v1: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![1],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        let migration_v2: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = ?1",
                params![2],
                |row| row.get(0),
            )
            .expect("schema migrations should be queryable");
        assert_eq!(migration_v1, 1, "migration v1 should be recorded exactly once");
        assert_eq!(migration_v2, 1, "migration v2 should be recorded exactly once");
    }

    #[test]
    fn append_redacts_sensitive_payload_fields() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        store
            .append(&build_request(
                "01ARZ3NDEKTSV4RRFFQ69G5FB0",
                br#"{"token":"SECRET_TOKEN_VALUE","nested":{"password":"123456"},"safe":"ok"}"#,
            ))
            .expect("append should succeed");
        let records = store.recent(1).expect("recent journal query should succeed");
        assert_eq!(records.len(), 1);
        assert!(
            !records[0].payload_json.contains("SECRET_TOKEN_VALUE"),
            "raw secret token must not leak into journal payload"
        );
        assert!(!records[0].payload_json.contains("123456"), "password must be redacted");
        assert!(records[0].payload_json.contains("<redacted>"), "payload should contain marker");
        assert!(records[0].redacted, "record should flag that redaction occurred");
    }

    #[test]
    fn append_non_json_payload_is_stored_as_redacted_metadata() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB1", b"api_token=SECRET"))
            .expect("append should succeed");

        let records = store.recent(1).expect("recent journal query should succeed");
        assert!(records[0].redacted, "non-JSON payloads must be marked as redacted");
        assert!(
            records[0].payload_json.contains("non_json_payload"),
            "redacted metadata should explain why payload was transformed"
        );
    }

    #[test]
    fn append_duplicate_event_id_returns_deterministic_duplicate_error() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB6", br#"{"kind":"first"}"#))
            .expect("first append should succeed");
        let error = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB6", br#"{"kind":"duplicate"}"#))
            .expect_err("duplicate event ids must be rejected deterministically");
        assert!(matches!(
            error,
            JournalError::DuplicateEventId { ref event_id }
                if event_id == "01ARZ3NDEKTSV4RRFFQ69G5FB6"
        ));
    }

    #[test]
    fn hash_chain_links_events_when_enabled() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: true,
        })
        .expect("journal store should open");

        let first = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB2", br#"{"kind":"first"}"#))
            .expect("first append should succeed");
        let second = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB3", br#"{"kind":"second"}"#))
            .expect("second append should succeed");

        assert!(
            first.hash.is_some() && second.hash.is_some(),
            "hash chain mode should generate hashes"
        );
        assert_eq!(
            second.prev_hash, first.hash,
            "second event must link to first event hash in hash-chain mode"
        );
    }

    #[test]
    fn append_only_triggers_reject_update_and_delete() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled: false,
        })
        .expect("journal store should open");

        store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB4", br#"{"kind":"immutable"}"#))
            .expect("append should succeed");
        let connection = Connection::open(db_path).expect("journal db should open");

        let update_error = connection
            .execute(
                "UPDATE journal_events SET actor = 99 WHERE event_ulid = ?1",
                params!["01ARZ3NDEKTSV4RRFFQ69G5FB4"],
            )
            .expect_err("updates must be rejected");
        assert!(
            update_error.to_string().contains("append-only"),
            "update errors should mention append-only invariant"
        );

        let delete_error = connection
            .execute(
                "DELETE FROM journal_events WHERE event_ulid = ?1",
                params!["01ARZ3NDEKTSV4RRFFQ69G5FB4"],
            )
            .expect_err("deletes must be rejected");
        assert!(
            delete_error.to_string().contains("append-only"),
            "delete errors should mention append-only invariant"
        );
    }

    #[test]
    fn recent_query_limit_is_clamped_to_prevent_unbounded_reads() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        for index in 0..3 {
            let event_id = format!("01ARZ3NDEKTSV4RRFFQ69G5FC{index}");
            let payload = format!(r#"{{"index":{index}}}"#);
            store
                .append(&build_request(event_id.as_str(), payload.as_bytes()))
                .expect("append should succeed");
        }

        let records = store.recent(0).expect("recent query should clamp low limit");
        assert_eq!(records.len(), 1, "limit=0 should be clamped to a single record");
    }

    #[test]
    fn orchestrator_run_snapshot_persists_usage_and_tape() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");
        store
            .update_orchestrator_run_state(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                RunLifecycleState::InProgress,
                None,
            )
            .expect("run should transition to in_progress");
        store
            .add_orchestrator_usage(&OrchestratorUsageDelta {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                prompt_tokens_delta: 3,
                completion_tokens_delta: 2,
            })
            .expect("usage counters should persist");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 0,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape event should persist");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 1,
                event_type: "model_token".to_owned(),
                payload_json: r#"{"token":"alpha","is_final":false}"#.to_owned(),
            })
            .expect("second tape event should persist");
        store
            .update_orchestrator_run_state(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                RunLifecycleState::Done,
                None,
            )
            .expect("run should transition to done");

        let snapshot = store
            .orchestrator_run_snapshot("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("run snapshot query should succeed")
            .expect("snapshot should exist");
        assert_eq!(snapshot.state, "done");
        assert_eq!(snapshot.prompt_tokens, 3);
        assert_eq!(snapshot.completion_tokens, 2);
        assert_eq!(snapshot.total_tokens, 5);
        assert_eq!(snapshot.tape.len(), 2);
        assert_eq!(snapshot.tape[0].seq, 0);
        assert_eq!(snapshot.tape[1].event_type, "model_token");
    }

    #[test]
    fn orchestrator_rejects_duplicate_run_id_and_tape_sequence() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("first run start should succeed");
        let duplicate_run = store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect_err("duplicate run IDs must be rejected");
        assert!(matches!(
            duplicate_run,
            JournalError::DuplicateRunId { ref run_id }
                if run_id == "01ARZ3NDEKTSV4RRFFQ69G5FAX"
        ));

        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 7,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape sequence should succeed");
        let duplicate_tape = store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 7,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect_err("duplicate tape sequence should be rejected");
        assert!(matches!(
            duplicate_tape,
            JournalError::DuplicateTapeSequence { ref run_id, seq }
                if run_id == "01ARZ3NDEKTSV4RRFFQ69G5FAX" && seq == 7
        ));
    }

    #[test]
    fn orchestrator_cancel_flag_is_persisted() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        upsert_orchestrator_session(&store, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("run start should succeed");
        assert!(
            !store
                .is_orchestrator_cancel_requested("01ARZ3NDEKTSV4RRFFQ69G5FAX")
                .expect("cancel status should query"),
            "cancel request should be false before cancellation"
        );
        store
            .request_orchestrator_cancel(&OrchestratorCancelRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                reason: "operator_requested".to_owned(),
            })
            .expect("cancel request should persist");
        assert!(
            store
                .is_orchestrator_cancel_requested("01ARZ3NDEKTSV4RRFFQ69G5FAX")
                .expect("cancel status should query"),
            "cancel request should persist"
        );
    }

    #[test]
    fn write_duration_is_reported_for_observability() {
        let db_path = temp_db_path();
        let store = JournalStore::open(JournalConfig { db_path, hash_chain_enabled: false })
            .expect("journal store should open");

        let outcome = store
            .append(&build_request("01ARZ3NDEKTSV4RRFFQ69G5FB5", br#"{"status":"ok"}"#))
            .expect("append should succeed");
        assert!(
            outcome.write_duration < Duration::from_secs(1),
            "local sqlite append should complete in bounded time"
        );
    }
}
