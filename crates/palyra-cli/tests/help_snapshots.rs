use std::process::Command;

use anyhow::{Context, Result};

fn normalize_help_text(text: &str) -> String {
    text.replace("palyra.exe", "palyra")
}

fn run_help(args: &[&str]) -> Result<String> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(args)
        .output()
        .with_context(|| format!("failed to execute palyra {}", args.join(" ")))?;
    assert!(
        output.status.success(),
        "palyra {} should succeed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout)
        .context("help output must be valid UTF-8")?
        .replace("\r\n", "\n");
    let normalized = text.lines().map(str::trim_end).collect::<Vec<_>>();
    let mut collapsed = Vec::with_capacity(normalized.len());
    let mut previous_blank = false;
    for line in normalized {
        let is_blank = line.is_empty();
        if is_blank && previous_blank {
            continue;
        }
        previous_blank = is_blank;
        collapsed.push(line);
    }
    Ok(normalize_help_text(collapsed.join("\n").trim_end()))
}

fn assert_help_snapshot(args: &[&str], snapshot: &str) -> Result<()> {
    assert_eq!(run_help(args)?, normalize_help_text(snapshot.trim_end()));
    Ok(())
}

#[cfg(windows)]
const ROOT_HELP_SNAPSHOT: &str = include_str!("help_snapshots/root-help-windows.txt");

#[cfg(not(windows))]
const ROOT_HELP_SNAPSHOT: &str = include_str!("help_snapshots/root-help-unix.txt");

#[test]
fn root_help_snapshot_matches() -> Result<()> {
    assert_help_snapshot(&["--help"], ROOT_HELP_SNAPSHOT)
}

#[test]
fn setup_help_snapshot_matches() -> Result<()> {
    assert_help_snapshot(&["setup", "--help"], include_str!("help_snapshots/setup-help.txt"))
}

#[test]
fn gateway_help_snapshot_matches() -> Result<()> {
    assert_help_snapshot(&["gateway", "--help"], include_str!("help_snapshots/gateway-help.txt"))
}

#[test]
fn dashboard_help_snapshot_matches() -> Result<()> {
    assert_help_snapshot(
        &["dashboard", "--help"],
        include_str!("help_snapshots/dashboard-help.txt"),
    )
}

#[test]
fn completion_help_snapshot_matches() -> Result<()> {
    assert_help_snapshot(
        &["completion", "--help"],
        include_str!("help_snapshots/completion-help.txt"),
    )
}
