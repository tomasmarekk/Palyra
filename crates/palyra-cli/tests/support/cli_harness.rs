#![allow(dead_code)]

// Shared integration-test helpers are compiled into each test binary, so some helpers are
// intentionally unused in narrower binaries such as `logs_cli`.

use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

use super::bin_under_test::palyra_bin;

pub fn temp_workdir() -> Result<TempDir> {
    TempDir::new().context("failed to create temporary workdir")
}

pub fn configure_cli_env(command: &mut Command, workdir: &Path) {
    command
        .env("PALYRA_STATE_ROOT", workdir.join("state-root"))
        .env("PALYRA_VAULT_DIR", workdir.join("vault"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("PALYRA_JOURNAL_DB_PATH", workdir.join("journal.sqlite3"))
        .env("XDG_STATE_HOME", workdir.join("xdg-state"))
        .env("XDG_CONFIG_HOME", workdir.join("xdg-config"))
        .env("XDG_DATA_HOME", workdir.join("xdg-data"))
        .env("XDG_CACHE_HOME", workdir.join("xdg-cache"))
        .env("HOME", workdir.join("home"))
        .env("LOCALAPPDATA", workdir.join("localappdata"))
        .env("APPDATA", workdir.join("appdata"));
}

pub fn command(workdir: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(palyra_bin());
    command.current_dir(workdir).args(args);
    configure_cli_env(&mut command, workdir);
    command
}

pub fn run_cli(workdir: &Path, args: &[&str], envs: &[(&str, &str)]) -> Result<Output> {
    let mut command = command(workdir, args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

pub fn run_cli_with_stdin(
    workdir: &Path,
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_payload: &[u8],
) -> Result<Output> {
    let mut command = command(workdir, args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child =
        command.spawn().with_context(|| format!("failed to spawn palyra {}", args.join(" ")))?;
    let stdin = child.stdin.as_mut().context("palyra command stdin was not available")?;
    stdin.write_all(stdin_payload).context("failed to write stdin payload to palyra command")?;
    child
        .wait_with_output()
        .with_context(|| format!("failed to wait for palyra {}", args.join(" ")))
}

pub fn required_env_var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("expected environment variable {name}"))
}

pub fn required_env_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_env_var(name)?))
}

pub fn assert_success(output: &Output, command_name: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    anyhow::bail!(
        "{command_name} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

pub fn assert_json_success(output: Output, command_name: &str) -> Result<Value> {
    assert_success(&output, command_name)?;
    serde_json::from_slice::<Value>(&output.stdout)
        .with_context(|| format!("{command_name} stdout should be valid JSON"))
}
