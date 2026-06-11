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
