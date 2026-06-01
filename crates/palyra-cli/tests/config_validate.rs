use std::fs;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
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

fn run_cli_with_env(workdir: &TempDir, args: &[&str], envs: &[(&str, &str)]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

#[test]
fn config_validate_without_path_uses_defaults_when_file_is_missing() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["config", "validate"])?;

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
fn config_path_with_explicit_state_root_uses_state_root_config_slot() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("alternate-state");
    let state_root_arg = state_root.to_string_lossy().into_owned();
    let managed_config = state_root.join("config").join("palyra.toml");
    let env_config = workdir.path().join("installed").join("palyra.toml");
    fs::create_dir_all(env_config.parent().expect("env config parent"))?;
    fs::write(env_config.as_path(), "version = 1\n")?;
    let env_config_arg = env_config.to_string_lossy().into_owned();

    let output = run_cli_with_env(
        &workdir,
        &["--state-root", state_root_arg.as_str(), "config", "path"],
        &[("PALYRA_CONFIG", env_config_arg.as_str())],
    )?;

    assert!(
        output.status.success(),
        "config path should report the explicit state-root slot: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let managed_config_text = managed_config.to_string_lossy();
    assert!(stdout.contains(managed_config_text.as_ref()), "unexpected stdout: {stdout}");
    assert!(!stdout.contains(env_config_arg.as_str()), "unexpected stdout: {stdout}");
    Ok(())
}

#[test]
fn config_validate_with_explicit_missing_path_fails() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(&workdir, &["config", "validate", "--path", "missing.toml"])?;

    assert!(!output.status.success(), "explicit missing config path must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("config file does not exist: missing.toml"));
    Ok(())
}

#[test]
fn config_validate_without_path_ignores_palyra_capitalized_path_in_cwd() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("Palyra.toml");
    fs::write(&config_path, "[daemon]\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate"])?;

    assert!(
        output.status.success(),
        "config validate should succeed with defaults even when Palyra.toml exists in CWD: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(
        stdout.contains("config=valid source=defaults"),
        "unexpected config source output: {stdout}"
    );
    Ok(())
}

#[test]
fn config_validate_without_path_ignores_config_directory_path_in_cwd() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_dir = workdir.path().join("config");
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("failed to create {}", config_dir.display()))?;
    let config_path = config_dir.join("palyra.toml");
    fs::write(&config_path, "[daemon]\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate"])?;

    assert!(
        output.status.success(),
        "config validate should succeed with defaults even when config/palyra.toml exists in CWD: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(
        stdout.contains("config=valid source=defaults"),
        "unexpected config source output: {stdout}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_non_numeric_daemon_port() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-port.toml");
    fs::write(&config_path, "[daemon]\nport='not-a-number'\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "invalid-port.toml"])?;

    assert!(!output.status.success(), "config with string daemon port must fail validation");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_invalid_bind_address() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-bind.toml");
    fs::write(&config_path, "[daemon]\nbind_addr='bad host value'\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "invalid-bind.toml"])?;

    assert!(!output.status.success(), "config with invalid bind address must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("invalid daemon bind address or port"),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_model_provider_secret_conflict() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("conflicting-model-secret.toml");
    fs::write(
        &config_path,
        r#"
[model_provider]
kind = "openai_compatible"
openai_base_url = "https://api.openai.com/v1"
openai_model = "gpt-4o-mini"
openai_api_key_vault_ref = "global/openai_api_key"

[model_provider.openai_api_key_secret_ref]
kind = "env"
variable = "PALYRA_OPENAI_API_KEY"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output =
        run_cli(&workdir, &["config", "validate", "--path", "conflicting-model-secret.toml"])?;

    assert!(!output.status.success(), "config with two secret sources must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains(
            "model_provider.openai_api_key cannot set both *_secret_ref and legacy vault_ref"
        ),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_json_warns_when_runtime_model_auth_is_missing() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("missing-model-auth.toml");
    fs::write(
        &config_path,
        r#"
version = 1

[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_model = "MiniMax-M2.7"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output =
        run_cli(&workdir, &["config", "validate", "--path", "missing-model-auth.toml", "--json"])?;

    assert!(
        output.status.success(),
        "schema-valid config should still validate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: serde_json::Value =
        serde_json::from_str(&stdout).context("config validate stdout was not JSON")?;
    assert_eq!(payload.get("status").and_then(serde_json::Value::as_str), Some("valid"));
    assert_eq!(
        payload.pointer("/warnings/0/code").and_then(serde_json::Value::as_str),
        Some("model_provider_missing_auth"),
        "validation output should warn that runtime model auth is not ready: {payload}"
    );
    assert!(
        payload
            .pointer("/warnings/0/message")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| message.contains("missing_auth")),
        "warning should point at the runtime failure mode: {payload}"
    );
    Ok(())
}

#[test]
fn config_validate_rejects_provider_registry_metadata_source_before_startup() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-registry-metadata-source.toml");
    fs::write(
        &config_path,
        r#"
version = 1

[model_provider]
kind = "anthropic"
anthropic_model = "MiniMax-M3"

[[model_provider.providers]]
provider_id = "minimax-primary"
kind = "anthropic"
enabled = true
auth_provider_kind = "minimax"
api_key_vault_ref = "global/minimax_api_key"

[[model_provider.models]]
model_id = "MiniMax-M3"
provider_id = "minimax-primary"
role = "chat"
metadata_source = "live_discovery"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(
        &workdir,
        &["config", "validate", "--path", "invalid-registry-metadata-source.toml"],
    )?;

    assert!(
        !output.status.success(),
        "config validate must reject daemon-incompatible registry metadata_source"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("model_provider.models[0].metadata_source must be one of"),
        "validation should point at the bad registry metadata_source before daemon startup: {stderr}"
    );
    assert!(
        stderr.contains("legacy_migration, static, discovery, operator_override"),
        "validation should list the daemon-compatible metadata_source values: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_browser_state_key_secret_conflict() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("conflicting-browser-state-key.toml");
    fs::write(
        &config_path,
        r#"
[tool_call.browser_service]
enabled = true
state_key_vault_ref = "global/browser_state_key"

[tool_call.browser_service.state_key_secret_ref]
kind = "env"
variable = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output =
        run_cli(&workdir, &["config", "validate", "--path", "conflicting-browser-state-key.toml"])?;

    assert!(!output.status.success(), "config with two browser state key sources must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains(
            "tool_call.browser_service.state_key cannot set both *_secret_ref and legacy vault_ref"
        ),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_invalid_gateway_grpc_bind_address() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-gateway-grpc-bind.toml");
    fs::write(&config_path, "[gateway]\ngrpc_bind_addr='bad host value'\ngrpc_port=7443\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output =
        run_cli(&workdir, &["config", "validate", "--path", "invalid-gateway-grpc-bind.toml"])?;

    assert!(!output.status.success(), "config with invalid gateway gRPC bind must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("invalid gateway gRPC bind address or port"),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_invalid_gateway_quic_bind_when_enabled() -> Result<()>
{
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-gateway-quic-bind.toml");
    fs::write(
        &config_path,
        "[gateway]\nquic_enabled=true\nquic_bind_addr='bad host value'\nquic_port=7444\n",
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output =
        run_cli(&workdir, &["config", "validate", "--path", "invalid-gateway-quic-bind.toml"])?;

    assert!(!output.status.success(), "config with invalid gateway QUIC bind must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(
        stderr.contains("invalid gateway QUIC bind address or port"),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_ignores_invalid_gateway_quic_bind_when_disabled() -> Result<()>
{
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("disabled-gateway-quic-invalid-bind.toml");
    fs::write(
        &config_path,
        "[gateway]\nquic_enabled=false\nquic_bind_addr='bad host value'\nquic_port=7444\n",
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(
        &workdir,
        &["config", "validate", "--path", "disabled-gateway-quic-invalid-bind.toml"],
    )?;

    assert!(
        output.status.success(),
        "config validate should ignore invalid QUIC bind when QUIC is disabled: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("config=valid source=disabled-gateway-quic-invalid-bind.toml"));
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_accepts_valid_bind_address_and_port() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("valid-bind.toml");
    fs::write(&config_path, "[daemon]\nbind_addr='127.0.0.1'\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "valid-bind.toml"])?;

    assert!(
        output.status.success(),
        "config with valid bind address should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("config=valid source=valid-bind.toml"));
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_accepts_ipv6_bind_address_without_brackets() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("valid-ipv6-bind.toml");
    fs::write(&config_path, "[daemon]\nbind_addr='::1'\nport=7142\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "valid-ipv6-bind.toml"])?;

    assert!(
        output.status.success(),
        "config with valid ipv6 bind address should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("config=valid source=valid-ipv6-bind.toml"));
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_non_boolean_identity_flag() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-identity.toml");
    fs::write(&config_path, "[identity]\nallow_insecure_node_rpc_without_mtls='definitely'\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "invalid-identity.toml"])?;

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

    let output = run_cli(&workdir, &["config", "validate", "--path", "unknown-identity-key.toml"])?;

    assert!(!output.status.success(), "config with unknown identity key must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}

#[test]
fn config_validate_with_explicit_path_rejects_non_boolean_orchestrator_flag() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("invalid-orchestrator.toml");
    fs::write(&config_path, "[orchestrator]\nrunloop_v1_enabled='sometimes'\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let output = run_cli(&workdir, &["config", "validate", "--path", "invalid-orchestrator.toml"])?;

    assert!(!output.status.success(), "config with non-boolean orchestrator flag must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid daemon config schema"), "unexpected stderr output: {stderr}");
    Ok(())
}
