//! Pins `palyra logs` and `gateway logs` behavior without a journal: a notice instead of an
//! error, in both text and JSON output.

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;

mod support;

use support::cli_harness::{run_cli, temp_workdir};

#[test]
fn logs_commands_report_missing_journal_as_notice() -> Result<()> {
    let workdir = temp_workdir()?;
    for args in [&["logs", "--lines", "50"][..], &["gateway", "logs", "--lines", "50"][..]] {
        let output = run_cli(workdir.path(), args, &[])?;
        assert!(
            output.status.success(),
            "{} should succeed without an existing journal\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
        assert!(
            stdout.contains("logs.notice"),
            "{} should emit a notice when no journal exists: {stdout}",
            args.join(" ")
        );
        assert!(
            stdout.contains("no journal or service logs exist yet"),
            "{} should explain that no logs exist yet: {stdout}",
            args.join(" ")
        );
        assert!(
            stdout.contains("palyra gateway run"),
            "{} should point users to foreground startup logs: {stdout}",
            args.join(" ")
        );
    }
    Ok(())
}

#[test]
fn logs_commands_accept_local_json_flag() -> Result<()> {
    let workdir = temp_workdir()?;
    for args in [
        &["logs", "--lines", "50", "--json"][..],
        &["gateway", "logs", "--lines", "50", "--json"][..],
    ] {
        let output = run_cli(workdir.path(), args, &[])?;
        assert!(
            output.status.success(),
            "{} should succeed without an existing journal\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let payload: Value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("{} stdout should be valid JSON", args.join(" ")))?;
        let records = payload.as_array().context("logs JSON output should be an array")?;
        assert_eq!(records.len(), 1, "{} should emit one diagnostic record", args.join(" "));
        assert_eq!(records[0].get("source").and_then(Value::as_str), Some("diagnostic"));
        assert!(
            records[0]
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains("no journal or service logs exist yet")),
            "{} should explain that no logs exist yet: {payload}",
            args.join(" ")
        );
    }
    Ok(())
}

#[test]
fn logs_commands_report_missing_journal_events_table_as_notice() -> Result<()> {
    let workdir = temp_workdir()?;
    let journal_path = workdir.path().join("journal.sqlite3");
    Connection::open(journal_path.as_path())
        .with_context(|| format!("failed to create {}", journal_path.display()))?;

    let output = run_cli(workdir.path(), &["logs", "--lines", "50"], &[])?;
    assert!(
        output.status.success(),
        "logs should succeed without journal_events table\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("logs.notice"), "missing table should emit a notice: {stdout}");
    assert!(
        stdout.contains("no journal or service logs exist yet"),
        "missing table should use the same no-logs diagnostic: {stdout}"
    );
    Ok(())
}
