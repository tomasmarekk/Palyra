use std::{path::Path, process::Command};

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

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
