use std::fs;
use std::process::Command;

use anyhow::{Context, Result};
use tempfile::TempDir;

#[test]
fn config_validate_without_path_uses_defaults_when_file_is_missing() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate"])
        .output()
        .context("failed to execute palyra config validate")?;

    assert!(
        output.status.success(),
        "config validate should succeed without explicit path: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("config=valid source=defaults"));
    Ok(())
}

#[test]
fn config_validate_with_explicit_missing_path_fails() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate", "--path", "missing.toml"])
        .output()
        .context("failed to execute palyra config validate with explicit path")?;

    assert!(!output.status.success(), "explicit missing config path must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("config file does not exist: missing.toml"));
    Ok(())
}

#[test]
fn config_validate_without_path_discovers_palyra_capitalized_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("Palyra.toml");
    fs::write(&config_path, "[daemon]\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate"])
        .output()
        .context("failed to execute palyra config validate")?;

    assert!(
        output.status.success(),
        "config validate should succeed with Palyra.toml: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    #[cfg(windows)]
    assert!(stdout.contains("config=valid source=palyra.toml"));
    #[cfg(not(windows))]
    assert!(stdout.contains("config=valid source=Palyra.toml"));
    Ok(())
}

#[test]
fn config_validate_without_path_discovers_config_directory_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_dir = workdir.path().join("config");
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("failed to create {}", config_dir.display()))?;
    let config_path = config_dir.join("palyra.toml");
    fs::write(&config_path, "[daemon]\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate"])
        .output()
        .context("failed to execute palyra config validate")?;

    assert!(
        output.status.success(),
        "config validate should succeed with config/palyra.toml: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("config=valid source=config/palyra.toml"));
    Ok(())
}
