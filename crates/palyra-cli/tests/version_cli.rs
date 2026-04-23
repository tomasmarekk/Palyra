use std::process::{Command, Output};

use anyhow::{Context, Result};
use tempfile::TempDir;

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(args)
        .output()
        .with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

#[test]
fn root_version_flag_prints_cli_version() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["--version"])?;

    assert!(
        output.status.success(),
        "palyra --version should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.starts_with("palyra "), "unexpected version output: {stdout}");
    Ok(())
}

#[test]
fn version_json_reports_checkout_git_hash() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["--output-format", "json", "version"])?;

    assert!(
        output.status.success(),
        "palyra version --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(output.stdout.as_slice()).context("stdout was not JSON")?;
    let git_hash = payload
        .get("git_hash")
        .and_then(serde_json::Value::as_str)
        .context("version JSON should include git_hash")?;

    assert!(!git_hash.trim().is_empty(), "git_hash must not be empty");
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .context("crate manifest should live under crates/palyra-cli")?;
    if repo_root.join(".git").exists() {
        assert_ne!(git_hash, "unknown", "git checkout builds should report a concrete git hash");
    }
    Ok(())
}
