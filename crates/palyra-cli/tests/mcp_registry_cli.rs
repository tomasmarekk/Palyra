//! Exercises `palyra mcp` registry mutations as an installed CLI binary would.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(args)
        .output()
        .with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn backup_path(path: &Path, index: usize) -> PathBuf {
    let mut raw: OsString = path.as_os_str().to_os_string();
    raw.push(format!(".bak.{index}"));
    PathBuf::from(raw)
}

#[test]
fn mcp_registry_add_list_show_uses_canonical_vault_refs() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let add = run_cli(
        &workdir,
        &[
            "mcp",
            "add",
            "docs",
            "--path",
            &config_path_string,
            "--transport",
            "stdio",
            "--command",
            "mcp-docs",
            "--arg=--root",
            "--arg",
            "docs",
            "--namespace",
            "docs",
            "--env-vault-ref",
            "DOCS_TOKEN=global/docs-token",
            "--tool-allow",
            "search",
            "--backups",
            "2",
            "--json",
        ],
    )?;
    assert!(
        add.status.success(),
        "mcp add should succeed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    let add_stdout = String::from_utf8(add.stdout).context("stdout was not UTF-8")?;
    let add_payload: Value = serde_json::from_str(add_stdout.as_str())
        .with_context(|| format!("stdout was not valid JSON: {add_stdout}"))?;
    assert_eq!(add_payload["id"], "docs");
    assert_eq!(add_payload["enabled"], false);
    assert!(backup_path(&config_path, 1).exists(), "mcp add should rotate a backup");

    let config = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(config.contains("[mcp]"), "config should use canonical [mcp]: {config}");
    assert!(
        config.contains("[[mcp.servers]]"),
        "config should use canonical [[mcp.servers]]: {config}"
    );
    assert!(
        !config.contains("mcp_servers"),
        "config should not write legacy mcp_servers: {config}"
    );
    assert!(config.contains("vault_ref = \"global/docs-token\""));
    assert!(!config.contains("plain-secret"));

    let list = run_cli(&workdir, &["mcp", "list", "--path", &config_path_string, "--json"])?;
    assert!(
        list.status.success(),
        "mcp list should succeed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let list_stdout = String::from_utf8(list.stdout).context("stdout was not UTF-8")?;
    let list_payload: Value = serde_json::from_str(list_stdout.as_str())
        .with_context(|| format!("stdout was not valid JSON: {list_stdout}"))?;
    assert_eq!(list_payload["servers"][0]["id"], "docs");
    assert_eq!(list_payload["servers"][0]["namespace"], "docs");

    let show =
        run_cli(&workdir, &["mcp", "show", "docs", "--path", &config_path_string, "--json"])?;
    assert!(
        show.status.success(),
        "mcp show should succeed: {}",
        String::from_utf8_lossy(&show.stderr)
    );
    let show_stdout = String::from_utf8(show.stdout).context("stdout was not UTF-8")?;
    let show_payload: Value = serde_json::from_str(show_stdout.as_str())
        .with_context(|| format!("stdout was not valid JSON: {show_stdout}"))?;
    assert_eq!(show_payload["server"]["id"], "docs");
    assert_eq!(show_payload["server"]["env_vault_refs"][0]["name"], "DOCS_TOKEN");
    Ok(())
}

#[test]
fn mcp_registry_add_rejects_inline_env_like_secret() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    let original = "version = 1\n";
    fs::write(&config_path, original)
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "mcp",
            "add",
            "docs",
            "--path",
            &config_path_string,
            "--transport",
            "stdio",
            "--command",
            "mcp-docs",
            "--env-vault-ref",
            "DOCS_TOKEN=plain-secret",
        ],
    )?;
    assert!(!output.status.success(), "mcp add should reject plain env-like values");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("env vault ref"), "unexpected stderr: {stderr}");
    let config = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(config, original, "failed mutation must not rewrite config");
    Ok(())
}
