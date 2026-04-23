use std::fs;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_STATE_ROOT", workdir.path().join("state-root"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env_remove("PALYRA_ADMIN_TOKEN")
        .env_remove("PALYRA_DAEMON_URL")
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"))
        .env("PROGRAMDATA", workdir.path().join("programdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

#[test]
fn doctor_json_uses_global_config_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("custom-palyra.toml");
    fs::write(
        config_path.as_path(),
        r#"
version = 1

[admin]
require_auth = false
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(&workdir, &["--config", &config_path_string, "doctor", "--json"])?;

    assert!(
        output.status.success(),
        "doctor --json should succeed without --strict: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/config/path").and_then(Value::as_str),
        Some(config_path_string.as_str())
    );
    assert_eq!(payload.pointer("/diagnostics/config/exists").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.pointer("/diagnostics/config/parsed").and_then(Value::as_bool), Some(true));
    Ok(())
}

#[test]
fn doctor_flags_missing_anthropic_vault_refs_without_daemon_diagnostics() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("minimax-palyra.toml");
    fs::write(
        config_path.as_path(),
        r#"
version = 1

[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(&workdir, &["--config", &config_path_string, "doctor", "--json"])?;

    assert!(
        output.status.success(),
        "doctor --json should succeed for missing-vault-ref diagnostics: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/config_ref_health/severity").and_then(Value::as_str),
        Some("blocking")
    );
    assert_eq!(
        payload
            .pointer("/diagnostics/config_ref_health/summary/blocking_refs")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        payload
            .pointer("/diagnostics/checks")
            .and_then(Value::as_array)
            .is_some_and(|checks| {
                checks.iter().any(|check| {
                    check.get("key").and_then(Value::as_str) == Some("config_secret_refs_ok")
                        && check.get("ok").and_then(Value::as_bool) == Some(false)
                        && check.get("severity").and_then(Value::as_str) == Some("blocking")
                })
            }),
        "doctor checks should include a blocking config_secret_refs_ok failure: {payload}"
    );
    Ok(())
}
