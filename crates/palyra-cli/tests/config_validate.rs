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
    assert!(
        stdout.contains("config=valid source=Palyra.toml")
            || stdout.contains("config=valid source=palyra.toml"),
        "unexpected config source output: {stdout}"
    );
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

#[test]
fn config_validate_with_explicit_path_rejects_non_numeric_daemon_port() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-port.toml");
    fs::write(&config_path, "[daemon]\nport='not-a-number'\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate", "--path", "invalid-port.toml"])
        .output()
        .context("failed to execute palyra config validate with invalid daemon port")?;

    assert!(!output.status.success(), "config with string daemon port must fail validation");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_non_boolean_identity_flag() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-identity.toml");
    fs::write(&config_path, "[identity]\nallow_insecure_node_rpc_without_mtls='definitely'\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate", "--path", "invalid-identity.toml"])
        .output()
        .context("failed to execute palyra config validate with invalid identity flag")?;

    assert!(!output.status.success(), "config with non-boolean identity flag must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_unknown_identity_key() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("unknown-identity-key.toml");
    fs::write(
        &config_path,
        "[identity]\nallow_insecure_node_rpc_without_mtls=true\nallow_insecure_node_rpc_without_mtls_typo=true\n",
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(["config", "validate", "--path", "unknown-identity-key.toml"])
        .output()
        .context("failed to execute palyra config validate with unknown identity key")?;

    assert!(!output.status.success(), "config with unknown identity key must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}
