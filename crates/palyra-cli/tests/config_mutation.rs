use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result};
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
fn config_set_get_unset_roundtrip_and_rotates_backups() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[daemon]\nport = 7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let set_output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "daemon.port",
            "--value",
            "7443",
            "--backups",
            "2",
        ],
    )?;
    assert!(
        set_output.status.success(),
        "config set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );
    assert!(
        backup_path(&config_path, 1).exists(),
        "set should create backup .bak.1 for existing config"
    );

    let get_output = run_cli(
        &workdir,
        &["config", "get", "--path", &config_path_string, "--key", "daemon.port"],
    )?;
    assert!(
        get_output.status.success(),
        "config get should succeed: {}",
        String::from_utf8_lossy(&get_output.stderr)
    );
    let get_stdout = String::from_utf8(get_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        get_stdout.contains("config.get key=daemon.port value=7443"),
        "unexpected config get output: {get_stdout}"
    );

    let unset_output = run_cli(
        &workdir,
        &[
            "config",
            "unset",
            "--path",
            &config_path_string,
            "--key",
            "daemon.port",
            "--backups",
            "2",
        ],
    )?;
    assert!(
        unset_output.status.success(),
        "config unset should succeed: {}",
        String::from_utf8_lossy(&unset_output.stderr)
    );

    let missing_get = run_cli(
        &workdir,
        &["config", "get", "--path", &config_path_string, "--key", "daemon.port"],
    )?;
    assert!(!missing_get.status.success(), "config get should fail for removed key");
    let missing_stderr = String::from_utf8(missing_get.stderr).context("stderr was not UTF-8")?;
    assert!(
        missing_stderr.contains("config key not found: daemon.port"),
        "unexpected stderr output: {missing_stderr}"
    );
    Ok(())
}

#[test]
fn config_set_rejects_prototype_pollution_key_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "tool_call.__proto__.enabled",
            "--value",
            "true",
        ],
    )?;
    assert!(!output.status.success(), "unsafe key path should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid config key path"), "unexpected stderr output: {stderr}");
    Ok(())
}

#[test]
fn config_migrate_adds_version_and_recover_restores_latest_backup() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("legacy.toml");
    fs::write(&config_path, "[daemon]\nport = 7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let migrate_output =
        run_cli(&workdir, &["config", "migrate", "--path", &config_path_string, "--backups", "3"])?;
    assert!(
        migrate_output.status.success(),
        "config migrate should succeed: {}",
        String::from_utf8_lossy(&migrate_output.stderr)
    );
    let migrated_content = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(migrated_content.contains("version = 1"), "migrate should write config version marker");

    let set_output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "daemon.port",
            "--value",
            "7444",
            "--backups",
            "3",
        ],
    )?;
    assert!(
        set_output.status.success(),
        "config set should succeed after migration: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    let recover_output = run_cli(
        &workdir,
        &["config", "recover", "--path", &config_path_string, "--backup", "1", "--backups", "3"],
    )?;
    assert!(
        recover_output.status.success(),
        "config recover should succeed: {}",
        String::from_utf8_lossy(&recover_output.stderr)
    );

    let recovered = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        recovered.contains("port = 7142"),
        "recover should restore the immediate previous version"
    );
    Ok(())
}

#[test]
fn config_set_requires_valid_toml_value_literal() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "daemon.bind_addr",
            "--value",
            "not_a_toml_literal",
        ],
    )?;
    assert!(!output.status.success(), "invalid literal should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("config set value must be a valid TOML literal"),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}
