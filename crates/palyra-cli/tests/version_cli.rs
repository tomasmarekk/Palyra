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
