use std::fs;
use std::io::Write;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result};
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_VAULT_DIR", workdir.path().join("vault"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn run_cli_with_stdin(workdir: &TempDir, args: &[&str], stdin_payload: &[u8]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command
        .current_dir(workdir.path())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_cli_env(&mut command, workdir);
    let mut child =
        command.spawn().with_context(|| format!("failed to spawn palyra {}", args.join(" ")))?;
    let stdin = child.stdin.as_mut().context("palyra command stdin was not available")?;
    stdin.write_all(stdin_payload).context("failed to write stdin payload to palyra command")?;
    child
        .wait_with_output()
        .with_context(|| format!("failed to wait for palyra {}", args.join(" ")))
}

fn bootstrap_local_config(workdir: &TempDir) -> Result<String> {
    let config_path = workdir.path().join("palyra.toml");
    let config_arg = config_path.to_string_lossy().into_owned();
    let output =
        run_cli(workdir, &["setup", "--mode", "local", "--path", config_arg.as_str(), "--force"])?;
    assert!(
        output.status.success(),
        "setup should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(config_arg)
}

#[test]
fn secrets_set_then_get_reveal_returns_exact_bytes() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"sk-test-secret-line-1\nline-2\n";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["secrets", "set", "global", "openai_api_key", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );
    let set_stdout = String::from_utf8(set_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        set_stdout.contains("secrets.set scope=global key=openai_api_key"),
        "unexpected secrets set output: {set_stdout}"
    );

    let get_output =
        run_cli(&workdir, &["secrets", "get", "global", "openai_api_key", "--reveal"])?;
    assert!(
        get_output.status.success(),
        "secrets get --reveal should succeed: {}",
        String::from_utf8_lossy(&get_output.stderr)
    );
    assert_eq!(
        get_output.stdout, secret_value,
        "secrets get --reveal must return exact stored bytes"
    );
    let get_stderr = String::from_utf8(get_output.stderr).context("stderr was not UTF-8")?;
    assert!(
        get_stderr.contains("warning: printing secret bytes to stdout"),
        "expected warning when revealing secret output: {get_stderr}"
    );
    Ok(())
}

#[test]
fn secrets_get_without_reveal_redacts_output() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"super-secret-token";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["secrets", "set", "global", "slack_bot_token", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    let get_output = run_cli(&workdir, &["secrets", "get", "global", "slack_bot_token"])?;
    assert!(
        get_output.status.success(),
        "secrets get without reveal should succeed: {}",
        String::from_utf8_lossy(&get_output.stderr)
    );
    let get_stdout = String::from_utf8(get_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        get_stdout.contains("value=<redacted>"),
        "secret output should be redacted by default: {get_stdout}"
    );
    assert!(
        !get_stdout.contains("super-secret-token"),
        "raw secret bytes must not appear without --reveal: {get_stdout}"
    );
    Ok(())
}

#[test]
fn secrets_get_missing_key_fails_with_not_found_error() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["secrets", "get", "global", "missing_key"])?;
    assert!(!output.status.success(), "reading missing secret key must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("secret not found"),
        "missing secret errors should include not found context: {stderr}"
    );
    Ok(())
}

#[test]
fn secrets_configure_openai_api_key_updates_config_and_audit() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = bootstrap_local_config(&workdir)?;

    let configure_output = run_cli_with_stdin(
        &workdir,
        &[
            "secrets",
            "configure",
            "openai-api-key",
            "global",
            "openai_api_key",
            "--value-stdin",
            "--path",
            config_path.as_str(),
            "--json",
        ],
        b"sk-test-openai-secret",
    )?;
    assert!(
        configure_output.status.success(),
        "secrets configure openai-api-key should succeed: {}",
        String::from_utf8_lossy(&configure_output.stderr)
    );
    let configure_stdout =
        String::from_utf8(configure_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        configure_stdout.contains("\"vault_ref_configured\": true"),
        "configure output should confirm the vault ref was configured without echoing it: {configure_stdout}"
    );

    let config_toml = fs::read_to_string(&config_path).context("failed to read mutated config")?;
    assert!(
        config_toml.contains("openai_api_key_vault_ref = \"global/openai_api_key\""),
        "config should reference the configured vault secret: {config_toml}"
    );
    assert!(
        !config_toml.contains("sk-test-openai-secret"),
        "raw secret must never be written into config: {config_toml}"
    );

    let audit_output = run_cli(
        &workdir,
        &["secrets", "audit", "--path", config_path.as_str(), "--offline", "--json"],
    )?;
    assert!(
        audit_output.status.success(),
        "secrets audit should succeed: {}",
        String::from_utf8_lossy(&audit_output.stderr)
    );
    let audit_stdout = String::from_utf8(audit_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        audit_stdout.contains("\"blocking_findings\": 0"),
        "audit should report zero blocking findings for configured secret refs: {audit_stdout}"
    );
    assert!(
        audit_stdout.contains("\"total_references\": 1"),
        "audit should summarize the configured secret references: {audit_stdout}"
    );
    assert!(
        audit_stdout.contains("\"resolved_references\": 1"),
        "audit should summarize resolved secret references without echoing raw refs: {audit_stdout}"
    );

    let apply_output = run_cli(
        &workdir,
        &["secrets", "apply", "--path", config_path.as_str(), "--offline", "--json"],
    )?;
    assert!(
        apply_output.status.success(),
        "secrets apply should succeed: {}",
        String::from_utf8_lossy(&apply_output.stderr)
    );
    let apply_stdout = String::from_utf8(apply_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        apply_stdout.contains("\"apply_mode\": \"daemon_restart_required\""),
        "apply should surface the daemon restart requirement for model provider secrets: {apply_stdout}"
    );
    Ok(())
}

#[test]
fn secrets_configure_browser_state_key_updates_config() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = bootstrap_local_config(&workdir)?;

    let output = run_cli_with_stdin(
        &workdir,
        &[
            "secrets",
            "configure",
            "browser-state-key",
            "global",
            "browser_state_key",
            "--value-stdin",
            "--path",
            config_path.as_str(),
            "--json",
        ],
        b"0123456789abcdef0123456789abcdef",
    )?;
    assert!(
        output.status.success(),
        "secrets configure browser-state-key should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_toml = fs::read_to_string(&config_path).context("failed to read mutated config")?;
    assert!(
        config_toml.contains("state_key_vault_ref = \"global/browser_state_key\""),
        "config should reference the configured browser state key: {config_toml}"
    );
    Ok(())
}
