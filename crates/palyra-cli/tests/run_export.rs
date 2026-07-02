//! Integration tests for `palyra run export`.

use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{json, Value};

const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FB2";
const SESSION_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FB1";
const PROPOSAL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FB3";

#[test]
fn run_export_writes_redacted_trajectory_jsonl_from_journal() -> Result<()> {
    let temp = tempfile::tempdir().context("failed to create temp dir")?;
    let journal_path = temp.path().join("journal.sqlite3");
    seed_journal(journal_path.as_path())?;
    let output_path = temp.path().join("artifacts").join("run.jsonl");

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["run", "export", "--run-id", RUN_ID, "--output"])
        .arg(output_path.as_os_str())
        .args(["--journal-db"])
        .arg(journal_path.as_os_str())
        .args(["--trajectory", "--redacted", "true"])
        .output()
        .context("failed to execute palyra run export")?;

    assert!(
        output.status.success(),
        "run export should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = fs::read_to_string(output_path.as_path())
        .with_context(|| format!("failed to read {}", output_path.display()))?;
    assert!(!text.contains("access_token=raw"));
    assert!(!text.contains("Bearer raw"));

    let rows = text
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).context("trajectory JSONL row should parse")
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = rows.first().context("trajectory should contain manifest row")?;
    assert_eq!(manifest.get("format").and_then(Value::as_str), Some("palyra-run-trajectory-jsonl"));
    assert!(manifest.get("manifest_hash_sha256").and_then(Value::as_str).is_some());
    assert!(rows
        .iter()
        .skip(1)
        .any(|row| { row.get("category").and_then(Value::as_str) == Some("tool_output") }));
    Ok(())
}

fn seed_journal(db_path: &Path) -> Result<()> {
    let connection = Connection::open(db_path)
        .with_context(|| format!("failed to open journal db {}", db_path.display()))?;
    connection
        .execute_batch(
            r#"
            CREATE TABLE orchestrator_sessions (
                session_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT
            );
            CREATE TABLE orchestrator_runs (
                run_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                state TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL,
                completion_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                last_error TEXT,
                origin_kind TEXT NOT NULL,
                origin_run_ulid TEXT,
                parent_run_ulid TEXT,
                parameter_delta_json TEXT
            );
            CREATE TABLE orchestrator_tape (
                run_ulid TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (run_ulid, seq)
            );
            "#,
        )
        .context("failed to initialize journal schema")?;
    connection
        .execute(
            "INSERT INTO orchestrator_sessions (session_ulid, principal, device_id, channel) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![SESSION_ID, "user:ops", "device:local", "cli"],
        )
        .context("failed to insert session")?;
    connection
        .execute(
            "INSERT INTO orchestrator_runs (
                run_ulid, session_ulid, state, prompt_tokens, completion_tokens, total_tokens,
                last_error, origin_kind, origin_run_ulid, parent_run_ulid, parameter_delta_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                RUN_ID,
                SESSION_ID,
                "completed",
                12_i64,
                4_i64,
                16_i64,
                Option::<String>::None,
                "run_stream",
                Option::<String>::None,
                Option::<String>::None,
                json!({ "user_input": { "text": "fetch https://example.test?token=raw" } })
                    .to_string(),
            ],
        )
        .context("failed to insert run")?;
    insert_tape_event(
        &connection,
        0,
        "tool_proposal",
        json!({
            "proposal_id": PROPOSAL_ID,
            "tool_name": "palyra.http.fetch",
            "input_json": {
                "url": "https://example.test/callback?access_token=raw&mode=ok",
                "headers": { "authorization": "Bearer raw" }
            }
        }),
    )?;
    insert_tape_event(
        &connection,
        1,
        "tool_result",
        json!({
            "proposal_id": PROPOSAL_ID,
            "success": true,
            "output_json": { "status": 200 },
            "error": "",
        }),
    )?;
    insert_tape_event(
        &connection,
        2,
        "model_token",
        json!({
            "token": "done",
            "is_final": true,
        }),
    )?;
    Ok(())
}

fn insert_tape_event(
    connection: &Connection,
    seq: i64,
    event_type: &str,
    payload: Value,
) -> Result<()> {
    connection
        .execute(
            "INSERT INTO orchestrator_tape (run_ulid, seq, event_type, payload_json) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![RUN_ID, seq, event_type, payload.to_string()],
        )
        .with_context(|| format!("failed to insert tape event {event_type}"))?;
    Ok(())
}
