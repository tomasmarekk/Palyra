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
fn config_get_redacts_secret_values_by_default() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[admin]\nauth_token = \"super-secret-token\"\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let redacted_output = run_cli(
        &workdir,
        &["config", "get", "--path", &config_path_string, "--key", "admin.auth_token"],
    )?;
    assert!(redacted_output.status.success(), "config get should succeed");
    let redacted_stdout =
        String::from_utf8(redacted_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        redacted_stdout.contains("value=\"<redacted>\""),
        "secret config value must be redacted by default: {redacted_stdout}"
    );
    assert!(
        !redacted_stdout.contains("super-secret-token"),
        "raw secret value must not be printed by default: {redacted_stdout}"
    );

    let unredacted_output = run_cli(
        &workdir,
        &[
            "config",
            "get",
            "--path",
            &config_path_string,
            "--key",
            "admin.auth_token",
            "--show-secrets",
        ],
    )?;
    assert!(unredacted_output.status.success(), "config get --show-secrets should succeed");
    let unredacted_stdout =
        String::from_utf8(unredacted_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        unredacted_stdout.contains("value=\"super-secret-token\""),
        "show-secrets should print the actual value: {unredacted_stdout}"
    );
    Ok(())
}

#[test]
fn config_list_redacts_secret_paths_unless_show_secrets_is_used() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"version = 1
[admin]
auth_token = "super-secret-token"
[model_provider]
kind = "openai"
openai_api_key = "sk-test-secret"
openai_model = "gpt-4o-mini"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let redacted_output = run_cli(&workdir, &["config", "list", "--path", &config_path_string])?;
    assert!(redacted_output.status.success(), "config list should succeed");
    let redacted_stdout =
        String::from_utf8(redacted_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        redacted_stdout.contains("auth_token = \"<redacted>\""),
        "auth token should be redacted in list output: {redacted_stdout}"
    );
    assert!(
        redacted_stdout.contains("openai_api_key = \"<redacted>\""),
        "openai api key should be redacted in list output: {redacted_stdout}"
    );
    assert!(
        !redacted_stdout.contains("super-secret-token")
            && !redacted_stdout.contains("sk-test-secret"),
        "raw secrets must not appear in redacted list output: {redacted_stdout}"
    );

    let unredacted_output =
        run_cli(&workdir, &["config", "list", "--path", &config_path_string, "--show-secrets"])?;
    assert!(unredacted_output.status.success(), "config list --show-secrets should succeed");
    let unredacted_stdout =
        String::from_utf8(unredacted_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        unredacted_stdout.contains("auth_token = \"super-secret-token\"")
            && unredacted_stdout.contains("openai_api_key = \"sk-test-secret\""),
        "show-secrets should preserve secret values: {unredacted_stdout}"
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

#[test]
fn config_set_rejects_schema_invalid_typed_value_and_preserves_file() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[daemon]\nport = 7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let before = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "daemon.port",
            "--value",
            "\"oops\"",
            "--backups",
            "2",
        ],
    )?;
    assert!(!output.status.success(), "schema-invalid set should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("does not match daemon schema"), "unexpected stderr output: {stderr}");
    let after = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(before, after, "failed set must not mutate active config");
    assert!(!backup_path(&config_path, 1).exists(), "failed set must not rotate backups");
    Ok(())
}

#[test]
fn config_set_rejects_unknown_top_level_key_and_preserves_file() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let before = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "config",
            "set",
            "--path",
            &config_path_string,
            "--key",
            "unknown.section",
            "--value",
            "1",
            "--backups",
            "2",
        ],
    )?;
    assert!(!output.status.success(), "unknown top-level section should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("does not match daemon schema"), "unexpected stderr output: {stderr}");
    let after = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(before, after, "failed set must not mutate active config");
    assert!(!backup_path(&config_path, 1).exists(), "failed set must not rotate backups");
    Ok(())
}

#[test]
fn config_migrate_rejects_schema_invalid_document() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[daemon]\nport = \"bad\"\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let before = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output =
        run_cli(&workdir, &["config", "migrate", "--path", &config_path_string, "--backups", "2"])?;
    assert!(!output.status.success(), "schema-invalid migrate should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("does not match daemon schema"), "unexpected stderr output: {stderr}");
    let after = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(before, after, "failed migrate must not mutate active config");
    assert!(!backup_path(&config_path, 1).exists(), "failed migrate must not rotate backups");
    Ok(())
}

#[test]
fn config_recover_rejects_invalid_backup_without_mutating_active_config() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[daemon]\nport = 7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    fs::write(backup_path(&config_path, 1), "version = 1\n[daemon\nport = 7000\n")
        .with_context(|| format!("failed to write backup for {}", config_path.display()))?;
    let before = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &["config", "recover", "--path", &config_path_string, "--backup", "1", "--backups", "3"],
    )?;
    assert!(!output.status.success(), "recover with invalid backup TOML should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("failed to parse backup config"), "unexpected stderr output: {stderr}");
    let after = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(before, after, "failed recover must not mutate active config");
    Ok(())
}

#[test]
fn config_recover_rejects_schema_invalid_backup_without_mutating_active_config() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n[daemon]\nport = 7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    fs::write(backup_path(&config_path, 1), "version = 1\n[daemon]\nport = \"oops\"\n")
        .with_context(|| format!("failed to write backup for {}", config_path.display()))?;
    let before = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &["config", "recover", "--path", &config_path_string, "--backup", "1", "--backups", "3"],
    )?;
    assert!(!output.status.success(), "recover with schema-invalid backup should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("does not match daemon schema"), "unexpected stderr output: {stderr}");
    let after = fs::read(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert_eq!(before, after, "failed recover must not mutate active config");
    Ok(())
}
