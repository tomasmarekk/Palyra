//! Pins `palyra daemon journal` vacuum/checkpoint and `gateway journal recent` maintenance
//! commands against a seeded WAL-mode SQLite journal database.

use std::{
    io::{Read, Write},
    net::TcpListener,
    path::Path,
    process::Command,
    thread,
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[test]
fn palyra_daemon_journal_vacuum_succeeds_for_existing_db() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_wal_journal_db(db_path.as_path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["daemon", "journal-vacuum", "--db-path", &db_path.to_string_lossy()])
        .output()
        .context("failed to execute palyra daemon journal-vacuum")?;

    assert!(
        output.status.success(),
        "journal-vacuum should succeed for existing DB path: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("journal.vacuum") && stdout.contains("status=ok"),
        "expected journal vacuum success output, got: {stdout}"
    );
    Ok(())
}

#[test]
fn palyra_daemon_journal_checkpoint_reports_checkpoint_stats() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_wal_journal_db(db_path.as_path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "daemon",
            "journal-checkpoint",
            "--db-path",
            &db_path.to_string_lossy(),
            "--mode",
            "truncate",
        ])
        .output()
        .context("failed to execute palyra daemon journal-checkpoint")?;

    assert!(
        output.status.success(),
        "journal-checkpoint should succeed for existing DB path: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("journal.checkpoint")
            && stdout.contains("mode=truncate")
            && stdout.contains("busy=")
            && stdout.contains("log_frames=")
            && stdout.contains("checkpointed_frames="),
        "expected checkpoint stats output, got: {stdout}"
    );
    Ok(())
}

#[test]
fn palyra_daemon_journal_vacuum_rejects_missing_db_path() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let missing = tempdir.path().join("missing.sqlite3");

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["daemon", "journal-vacuum", "--db-path", &missing.to_string_lossy()])
        .output()
        .context("failed to execute palyra daemon journal-vacuum")?;

    assert!(!output.status.success(), "journal-vacuum should fail for missing DB path");
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("journal database path does not exist"),
        "expected missing DB path validation error, got: {stderr}"
    );
    Ok(())
}

#[test]
fn palyra_gateway_journal_recent_supports_json_output() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let server = MockJournalRecentServer::spawn()?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(tempdir.path())
        .env("XDG_CONFIG_HOME", tempdir.path().join("xdg-config"))
        .env("HOME", tempdir.path().join("home"))
        .env("LOCALAPPDATA", tempdir.path().join("localappdata"))
        .env("APPDATA", tempdir.path().join("appdata"))
        .env("PROGRAMDATA", tempdir.path().join("programdata"))
        .args([
            "gateway",
            "journal-recent",
            "--url",
            server.base_url.as_str(),
            "--limit",
            "2",
            "--json",
        ])
        .output()
        .context("failed to execute palyra gateway journal-recent")?;

    assert!(
        output.status.success(),
        "gateway journal-recent --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(payload.get("total_events").and_then(Value::as_u64), Some(1));
    assert_eq!(
        payload
            .get("events")
            .and_then(Value::as_array)
            .and_then(|events| events.first())
            .and_then(|event| event.get("event_id"))
            .and_then(Value::as_str),
        Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    );
    server.finish()?;
    Ok(())
}

#[test]
fn palyra_state_verify_hash_chain_supports_json_output() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_hash_chained_journal_db(db_path.as_path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "state",
            "verify-hash-chain",
            "--db-path",
            &db_path.to_string_lossy(),
            "--full",
            "--json",
        ])
        .output()
        .context("failed to execute palyra state verify-hash-chain")?;

    assert!(
        output.status.success(),
        "state verify-hash-chain should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("ok"));
    assert_eq!(payload.get("checked_events").and_then(Value::as_u64), Some(2));
    Ok(())
}

#[test]
fn palyra_state_full_hash_verification_rejects_deleted_prefix() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_hash_chained_journal_db(db_path.as_path())?;
    Connection::open(db_path.as_path())
        .context("failed to reopen seeded journal database")?
        .execute("DELETE FROM journal_events WHERE seq = 1", [])
        .context("failed to delete journal hash-chain prefix")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "state",
            "verify-hash-chain",
            "--db-path",
            &db_path.to_string_lossy(),
            "--full",
            "--json",
        ])
        .output()
        .context("failed to execute palyra state verify-hash-chain")?;

    assert!(
        output.status.success(),
        "state verify-hash-chain should report the mismatch: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("scope").and_then(Value::as_str), Some("full"));
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("mismatch"));
    assert_eq!(
        payload.get("mismatch").and_then(|mismatch| mismatch.get("code")).and_then(Value::as_str),
        Some("journal.hash_chain.missing_genesis")
    );
    assert_eq!(
        payload.get("mismatch").and_then(|mismatch| mismatch.get("seq")).and_then(Value::as_i64),
        Some(2)
    );
    Ok(())
}

#[test]
fn palyra_state_repair_dry_run_reports_fts_plan() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_hash_chained_journal_db(db_path.as_path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "state",
            "repair",
            "--db-path",
            &db_path.to_string_lossy(),
            "--fts-only",
            "--dry-run",
            "--json",
        ])
        .output()
        .context("failed to execute palyra state repair")?;

    assert!(
        output.status.success(),
        "state repair dry-run should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("dry_run").and_then(Value::as_bool), Some(true));
    assert!(
        payload
            .get("planned_steps")
            .and_then(Value::as_array)
            .is_some_and(|steps| !steps.is_empty()),
        "state repair dry-run should report planned FTS steps: {payload}"
    );
    Ok(())
}

#[test]
fn palyra_state_checkpoint_reports_checkpoint_stats() -> Result<()> {
    let tempdir = tempfile::tempdir().context("failed to create tempdir")?;
    let db_path = tempdir.path().join("journal.sqlite3");
    seed_hash_chained_journal_db(db_path.as_path())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "state",
            "checkpoint",
            "--db-path",
            &db_path.to_string_lossy(),
            "--mode",
            "truncate",
            "--json",
        ])
        .output()
        .context("failed to execute palyra state checkpoint")?;

    assert!(
        output.status.success(),
        "state checkpoint should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("mode").and_then(Value::as_str), Some("truncate"));
    assert!(payload.get("busy").and_then(Value::as_i64).is_some());
    assert!(payload.get("checkpointed_frames").and_then(Value::as_i64).is_some());
    Ok(())
}

fn seed_wal_journal_db(db_path: &Path) -> Result<()> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open sqlite database {}", db_path.display()))?;
    connection
        .execute_batch(
            r#"
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL
                );
            "#,
        )
        .with_context(|| format!("failed to initialize sqlite schema {}", db_path.display()))?;
    for index in 0..64 {
        connection
            .execute("INSERT INTO events(payload) VALUES (?1)", params![format!("event-{index}")])
            .with_context(|| {
                format!("failed to seed sqlite row {index} in {}", db_path.display())
            })?;
    }
    Ok(())
}

fn seed_hash_chained_journal_db(db_path: &Path) -> Result<()> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open sqlite database {}", db_path.display()))?;
    connection
        .execute_batch(
            r#"
                PRAGMA journal_mode = WAL;
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at_unix_ms INTEGER NOT NULL
                );
                INSERT INTO schema_migrations(version, name, applied_at_unix_ms)
                VALUES (1, 'create_event_journal', 1700000000000);
                CREATE TABLE journal_events (
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
            "#,
        )
        .with_context(|| format!("failed to initialize journal schema {}", db_path.display()))?;
    let first = TestJournalEvent {
        event_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1",
        session_id: "01ARZ3NDEKTSV4RRFFQ69G5FS1",
        run_id: "01ARZ3NDEKTSV4RRFFQ69G5FR1",
        kind: 1,
        actor: 2,
        timestamp_unix_ms: 1700000000000,
        payload_json: r#"{"event":"first"}"#,
        principal: "operator",
        device_id: "device",
        channel: Some("cli"),
    };
    let first_hash = insert_test_journal_event(&connection, None, &first)?;
    let second = TestJournalEvent {
        event_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2",
        session_id: first.session_id,
        run_id: first.run_id,
        kind: 1,
        actor: 2,
        timestamp_unix_ms: 1700000000001,
        payload_json: r#"{"event":"second"}"#,
        principal: first.principal,
        device_id: first.device_id,
        channel: first.channel,
    };
    insert_test_journal_event(&connection, Some(first_hash.as_str()), &second)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct TestJournalEvent<'a> {
    event_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    kind: i32,
    actor: i32,
    timestamp_unix_ms: i64,
    payload_json: &'a str,
    principal: &'a str,
    device_id: &'a str,
    channel: Option<&'a str>,
}

fn insert_test_journal_event(
    connection: &Connection,
    prev_hash: Option<&str>,
    event: &TestJournalEvent<'_>,
) -> Result<String> {
    let hash = compute_test_journal_hash(prev_hash, event);
    connection
        .execute(
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
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                event.event_id,
                event.session_id,
                event.run_id,
                event.kind,
                event.actor,
                event.timestamp_unix_ms,
                event.payload_json,
                hash,
                prev_hash,
                event.principal,
                event.device_id,
                event.channel,
                event.timestamp_unix_ms,
            ],
        )
        .context("failed to insert test journal event")?;
    Ok(hash)
}

fn compute_test_journal_hash(prev_hash: Option<&str>, event: &TestJournalEvent<'_>) -> String {
    let mut hasher = Sha256::new();
    if let Some(prev_hash) = prev_hash {
        hasher.update(prev_hash.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(event.event_id.as_bytes());
    hasher.update(b"|");
    hasher.update(event.session_id.as_bytes());
    hasher.update(b"|");
    hasher.update(event.run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(event.kind.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(event.actor.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(event.timestamp_unix_ms.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(event.principal.as_bytes());
    hasher.update(b"|");
    hasher.update(event.device_id.as_bytes());
    hasher.update(b"|");
    if let Some(channel) = event.channel {
        hasher.update(channel.as_bytes());
    }
    hasher.update(b"|");
    hasher.update(event.payload_json.as_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push_str(format!("{byte:02x}").as_str());
    }
    encoded
}

struct MockJournalRecentServer {
    base_url: String,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl MockJournalRecentServer {
    fn spawn() -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind mock journal server")?;
        let address = listener.local_addr().context("failed to read mock journal server addr")?;
        let handle = thread::spawn(move || -> Result<()> {
            let (mut stream, _) =
                listener.accept().context("failed to accept journal recent request")?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).context("failed to read request")?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request_text = String::from_utf8_lossy(request.as_slice()).to_string();
            assert!(
                request_text.starts_with("GET /admin/v1/journal/recent?limit=2 "),
                "unexpected request line: {request_text}"
            );
            let body = r#"{"total_events":1,"hash_chain_enabled":true,"events":[{"event_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","kind":1,"actor":2,"redacted":false,"timestamp_unix_ms":1700000000000,"hash":"abc123"}]}"#;
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(reply.as_bytes()).context("failed to write response")?;
            stream.flush().context("failed to flush response")?;
            Ok(())
        });
        Ok(Self { base_url: format!("http://{}", address), handle: Some(handle) })
    }

    fn finish(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| anyhow::anyhow!("mock journal server panicked"))??;
        }
        Ok(())
    }
}
