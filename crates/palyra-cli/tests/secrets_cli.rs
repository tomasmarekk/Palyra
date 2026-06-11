//! Pins `palyra secrets` set/get/explain/configure/audit/apply contracts: byte-exact
//! roundtrips, redaction by default, and JSON output. Uses a temp encrypted-file vault
//! backend instead of the OS keychain.

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
fn secrets_set_accepts_env_style_uppercase_keys() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"sk-e2e-uppercase-key";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["secrets", "set", "global", "PALYRA_E2E_API_KEY", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should accept env-style uppercase keys: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );
    let set_stdout = String::from_utf8(set_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        set_stdout.contains("secrets.set scope=global key=PALYRA_E2E_API_KEY"),
        "unexpected secrets set output: {set_stdout}"
    );

    let get_output =
        run_cli(&workdir, &["secrets", "get", "global", "PALYRA_E2E_API_KEY", "--reveal"])?;
    assert!(
        get_output.status.success(),
        "secrets get --reveal should read env-style uppercase keys: {}",
        String::from_utf8_lossy(&get_output.stderr)
    );
    assert_eq!(get_output.stdout, secret_value);
    Ok(())
}

#[test]
fn secrets_set_help_documents_scope_syntax() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["secrets", "set", "--help"])?;
    assert!(output.status.success(), "secrets set --help should succeed");
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(
        stdout.contains("Secret scope: global | principal:<id> | channel:<name>:<account_id> | skill:<skill_id>"),
        "help should document the supported scope grammar: {stdout}"
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
        !get_stdout.contains("value_bytes"),
        "redacted secret output must not disclose secret length: {get_stdout}"
    );
    assert!(
        !get_stdout.contains("super-secret-token"),
        "raw secret bytes must not appear without --reveal: {get_stdout}"
    );
    Ok(())
}

#[test]
fn secrets_get_honors_global_json_output_format() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"json-secret-token";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["secrets", "set", "global", "discord_token", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    let get_output = run_cli(
        &workdir,
        &["--output-format", "json", "secrets", "get", "global", "discord_token"],
    )?;
    assert!(
        get_output.status.success(),
        "secrets get should succeed: {}",
        String::from_utf8_lossy(&get_output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&get_output.stdout).context("secrets get stdout was not JSON")?;
    assert_eq!(value.get("scope").and_then(serde_json::Value::as_str), Some("global"));
    assert_eq!(value.get("key").and_then(serde_json::Value::as_str), Some("discord_token"));
    assert_eq!(value.get("value").and_then(serde_json::Value::as_str), Some("<redacted>"));
    assert_eq!(value.get("redacted").and_then(serde_json::Value::as_bool), Some(true));
    assert!(
        !value.to_string().contains("json-secret-token"),
        "JSON get output must not reveal secrets without --reveal: {value}"
    );
    Ok(())
}

#[test]
fn secrets_mutations_honor_global_json_output_format() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"json-mutation-secret-token";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["--output-format", "json", "secrets", "set", "global", "e2e_dummy", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );
    let set_value: serde_json::Value =
        serde_json::from_slice(&set_output.stdout).context("secrets set stdout was not JSON")?;
    assert_eq!(set_value.get("scope").and_then(serde_json::Value::as_str), Some("global"));
    assert_eq!(set_value.get("key").and_then(serde_json::Value::as_str), Some("e2e_dummy"));
    assert_eq!(set_value.get("operation").and_then(serde_json::Value::as_str), Some("set"));
    assert!(set_value.get("backend").and_then(serde_json::Value::as_str).is_some());
    assert!(
        !set_value.to_string().contains("json-mutation-secret-token"),
        "JSON set output must not reveal secret bytes: {set_value}"
    );

    let delete_output = run_cli(
        &workdir,
        &["--output-format", "json", "secrets", "delete", "global", "e2e_dummy"],
    )?;
    assert!(
        delete_output.status.success(),
        "secrets delete should succeed: {}",
        String::from_utf8_lossy(&delete_output.stderr)
    );
    let delete_value: serde_json::Value = serde_json::from_slice(&delete_output.stdout)
        .context("secrets delete stdout was not JSON")?;
    assert_eq!(delete_value.get("scope").and_then(serde_json::Value::as_str), Some("global"));
    assert_eq!(delete_value.get("key").and_then(serde_json::Value::as_str), Some("e2e_dummy"));
    assert_eq!(delete_value.get("operation").and_then(serde_json::Value::as_str), Some("delete"));
    assert_eq!(delete_value.get("deleted").and_then(serde_json::Value::as_bool), Some(true));
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
fn secrets_explain_accepts_vault_reference_from_list() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let secret_value = b"minimax-secret-value";

    let set_output = run_cli_with_stdin(
        &workdir,
        &["secrets", "set", "global", "minimax_api_key", "--value-stdin"],
        secret_value,
    )?;
    assert!(
        set_output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    let explain_output =
        run_cli(&workdir, &["secrets", "explain", "global/minimax_api_key", "--json"])?;
    assert!(
        explain_output.status.success(),
        "secrets explain should accept vault refs from secrets list: {}",
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout =
        String::from_utf8(explain_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        explain_stdout.contains("\"kind\": \"vault_secret\""),
        "vault ref explain output should identify local vault secrets: {explain_stdout}"
    );
    assert!(
        explain_stdout.contains("\"reference\": \"global/minimax_api_key\""),
        "vault ref explain output should include the requested reference: {explain_stdout}"
    );
    assert!(
        explain_stdout.contains("\"status\": \"stored\""),
        "vault ref explain output should report readable stored secrets: {explain_stdout}"
    );
    assert!(
        explain_stdout.contains("\"configured\": false"),
        "unreferenced vault secrets should be explained instead of rejected: {explain_stdout}"
    );
    assert!(
        !explain_stdout.contains("value_bytes"),
        "secrets explain must not disclose raw secret lengths"
    );
    assert!(
        !explain_stdout.contains("minimax-secret-value"),
        "secrets explain must not reveal raw secret values"
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
    let audit_payload: serde_json::Value =
        serde_json::from_str(&audit_stdout).context("secrets audit stdout was not JSON")?;
    assert_eq!(
        audit_payload.pointer("/summary/blocking_findings").and_then(serde_json::Value::as_u64),
        Some(0),
        "audit should report zero blocking findings for configured secret refs: {audit_payload}"
    );
    assert_eq!(
        audit_payload.pointer("/summary/total_references").and_then(serde_json::Value::as_u64),
        Some(2),
        "audit should summarize the configured model and browser secret references: {audit_payload}"
    );
    assert_eq!(
        audit_payload.pointer("/summary/resolved_references").and_then(serde_json::Value::as_u64),
        Some(2),
        "audit should summarize resolved secret references without echoing raw refs: {audit_payload}"
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
fn secrets_audit_flags_anthropic_inline_model_key() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1

[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key = "sk-test-anthropic-inline"
"#,
    )
    .context("failed to write test config")?;
    let config_arg = config_path.to_string_lossy().into_owned();

    let audit_output = run_cli(
        &workdir,
        &["secrets", "audit", "--path", config_arg.as_str(), "--offline", "--json"],
    )?;
    assert!(
        audit_output.status.success(),
        "secrets audit should succeed: {}",
        String::from_utf8_lossy(&audit_output.stderr)
    );
    let audit_stdout = String::from_utf8(audit_output.stdout).context("stdout was not UTF-8")?;
    let payload: serde_json::Value =
        serde_json::from_str(&audit_stdout).context("secrets audit stdout was not JSON")?;
    assert_eq!(
        payload.pointer("/summary/warning_findings").and_then(serde_json::Value::as_u64),
        Some(1),
        "audit should summarize the inline Anthropic-compatible key warning: {payload}"
    );
    assert_eq!(
        payload.pointer("/findings/0/code").and_then(serde_json::Value::as_str),
        Some("inline_secret_configured"),
        "audit JSON should expose the warning finding code: {payload}"
    );
    assert!(
        payload
            .pointer("/findings/0/message")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| message.contains("model_provider.anthropic_api_key")),
        "audit JSON should expose actionable finding details: {payload}"
    );
    assert!(
        !audit_stdout.contains("sk-test-anthropic-inline"),
        "audit output must not echo inline secret material: {audit_stdout}"
    );
    Ok(())
}

#[test]
fn secrets_audit_non_json_output_surfaces_redacted_findings() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1

[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key = "sk-test-anthropic-inline"
"#,
    )
    .context("failed to write test config")?;
    let config_arg = config_path.to_string_lossy().into_owned();

    let audit_output =
        run_cli(&workdir, &["secrets", "audit", "--path", config_arg.as_str(), "--offline"])?;
    assert!(
        audit_output.status.success(),
        "secrets audit should succeed: {}",
        String::from_utf8_lossy(&audit_output.stderr)
    );
    let audit_stdout = String::from_utf8(audit_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        audit_stdout.contains(
            "secrets.finding severity=warning code=inline_secret_configured component=model_provider"
        ),
        "audit text output should expose finding details: {audit_stdout}"
    );
    assert!(
        audit_stdout.contains("message=\"model_provider.anthropic_api_key"),
        "audit text output should include a quoted redacted message: {audit_stdout}"
    );
    assert!(
        !audit_stdout.contains("sk-test-anthropic-inline"),
        "audit text output must not echo inline secret material: {audit_stdout}"
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
        b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
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

#[test]
fn secrets_configure_browser_state_key_rejects_empty_value() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = bootstrap_local_config(&workdir)?;
    let original_config_toml =
        fs::read_to_string(&config_path).context("failed to read original config")?;

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
        b" \r\n\t",
    )?;
    assert!(!output.status.success(), "empty browser state key should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("browser state key secret must not be empty"),
        "unexpected stderr output: {stderr}"
    );

    let config_toml = fs::read_to_string(&config_path).context("failed to read config")?;
    assert_eq!(
        config_toml, original_config_toml,
        "failed configure should leave the existing browser state key config unchanged"
    );
    Ok(())
}

#[test]
fn secrets_audit_non_json_output_is_redacted() -> Result<()> {
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
        ],
        b"sk-test-openai-secret",
    )?;
    assert!(
        configure_output.status.success(),
        "secrets configure openai-api-key should succeed: {}",
        String::from_utf8_lossy(&configure_output.stderr)
    );

    let audit_output =
        run_cli(&workdir, &["secrets", "audit", "--path", config_path.as_str(), "--offline"])?;
    assert!(
        audit_output.status.success(),
        "secrets audit should succeed: {}",
        String::from_utf8_lossy(&audit_output.stderr)
    );
    let audit_stdout = String::from_utf8(audit_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        audit_stdout.contains("secrets.audit summary=<redacted>"),
        "audit stdout should be redacted in non-json mode: {audit_stdout}"
    );
    assert!(
        audit_stdout.contains("use --json for structured output"),
        "audit stdout should point callers to --json for details: {audit_stdout}"
    );
    assert!(
        !audit_stdout.contains("global/openai_api_key"),
        "audit stdout must not echo vault refs in non-json mode: {audit_stdout}"
    );
    Ok(())
}

#[test]
fn secrets_apply_non_json_output_is_redacted() -> Result<()> {
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
        ],
        b"sk-test-openai-secret",
    )?;
    assert!(
        configure_output.status.success(),
        "secrets configure openai-api-key should succeed: {}",
        String::from_utf8_lossy(&configure_output.stderr)
    );

    let apply_output =
        run_cli(&workdir, &["secrets", "apply", "--path", config_path.as_str(), "--offline"])?;
    assert!(
        apply_output.status.success(),
        "secrets apply should succeed: {}",
        String::from_utf8_lossy(&apply_output.stderr)
    );
    let apply_stdout = String::from_utf8(apply_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        apply_stdout.contains("secrets.apply summary=<redacted>"),
        "apply stdout should be redacted in non-json mode: {apply_stdout}"
    );
    assert!(
        apply_stdout.contains("use --json for structured output"),
        "apply stdout should point callers to --json for details: {apply_stdout}"
    );
    assert!(
        !apply_stdout.contains("daemon_restart_required"),
        "apply stdout must not expose plan details in non-json mode: {apply_stdout}"
    );
    Ok(())
}
