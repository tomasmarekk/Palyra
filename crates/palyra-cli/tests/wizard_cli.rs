//! Pins the setup/onboarding wizard and `configure` flows: quickstart and non-interactive
//! JSON summaries, stdin secret handling, config backups, and the profile lifecycle. Uses
//! mock loopback provider servers for model discovery.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    ffi::OsString,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_vault::{
    BackendPreference as VaultBackendPreference, Vault, VaultConfig as VaultConfigOptions,
    VaultScope,
};
use serde_json::Value;
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_STATE_ROOT", workdir.path().join("state-root"))
        .env("PALYRA_VAULT_DIR", workdir.path().join("vault"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str], envs: &[(&str, &str)]) -> Result<Output> {
    run_cli_with_stdin(workdir, args, envs, None)
}

fn unused_loopback_ports(count: usize) -> Result<Vec<u16>> {
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        listeners.push(
            TcpListener::bind("127.0.0.1:0").context("failed to reserve loopback test port")?,
        );
    }
    listeners
        .iter()
        .map(|listener| {
            listener
                .local_addr()
                .map(|addr| addr.port())
                .context("failed to read reserved loopback test port")
        })
        .collect()
}

fn assert_no_pending_connection(listener: &TcpListener, context: &str) -> Result<()> {
    listener
        .set_nonblocking(true)
        .with_context(|| format!("failed to configure {context} listener"))?;
    match listener.accept() {
        Ok((_stream, address)) => {
            anyhow::bail!("{context} received unexpected connection from {address}")
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {context} listener")),
    }
}

fn run_cli_without_explicit_vault_dir(
    workdir: &TempDir,
    args: &[&str],
    envs: &[(&str, &str)],
) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    command
        .env("PALYRA_STATE_ROOT", workdir.path().join("state-root"))
        .env_remove("PALYRA_VAULT_DIR")
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn run_cli_with_stdin(
    workdir: &TempDir,
    args: &[&str],
    envs: &[(&str, &str)],
    stdin_bytes: Option<&[u8]>,
) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    for (key, value) in envs {
        command.env(key, value);
    }
    if stdin_bytes.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child =
        command.spawn().with_context(|| format!("failed to execute palyra {}", args.join(" ")))?;
    if let Some(stdin_bytes) = stdin_bytes {
        use std::io::Write;

        let mut stdin = child.stdin.take().context("child stdin should be piped")?;
        stdin
            .write_all(stdin_bytes)
            .with_context(|| format!("failed to write stdin for palyra {}", args.join(" ")))?;
    }
    child
        .wait_with_output()
        .with_context(|| format!("failed to collect output for palyra {}", args.join(" ")))
}

fn backup_path(path: &Path, index: usize) -> PathBuf {
    let mut raw: OsString = path.as_os_str().to_os_string();
    raw.push(format!(".bak.{index}"));
    PathBuf::from(raw)
}

fn profiles_registry_path(workdir: &TempDir) -> PathBuf {
    workdir.path().join("state-root").join("cli").join("profiles.toml")
}

fn seed_quickstart_config(workdir: &TempDir, config_path: &Path) -> Result<()> {
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("OPENAI_API_KEY", "sk-test-setup")],
    )?;
    assert!(
        output.status.success(),
        "quickstart seed should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn setup_wizard_quickstart_emits_json_summary() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let ports = unused_loopback_ports(3)?;
    let daemon_port = ports[0];
    let grpc_port = ports[1];
    let quic_port = ports[2];
    let daemon_port_arg = daemon_port.to_string();
    let grpc_port_arg = grpc_port.to_string();
    let quic_port_arg = quic_port.to_string();
    let dashboard_url = format!("http://127.0.0.1:{daemon_port}/");
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--daemon-port",
            &daemon_port_arg,
            "--grpc-port",
            &grpc_port_arg,
            "--quic-port",
            &quic_port_arg,
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("OPENAI_API_KEY", "sk-test-setup")],
    )?;
    assert!(
        output.status.success(),
        "setup wizard should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("setup wizard stdout should be JSON")?;
    assert_eq!(
        payload.get("status").and_then(Value::as_str),
        Some("configured_runtime_start_required")
    );
    assert_eq!(payload.get("recommended_step_id").and_then(Value::as_str), Some("agent_identity"));
    assert_eq!(payload.get("flow").and_then(Value::as_str), Some("quickstart"));
    assert_eq!(payload.get("dashboard_url").and_then(Value::as_str), Some(dashboard_url.as_str()));
    assert_eq!(
        payload.get("config_path").and_then(Value::as_str),
        Some(config_path_string.as_str())
    );
    assert!(
        payload.get("risk_events").and_then(Value::as_array).is_some_and(|values| values
            .iter()
            .any(|value| value.as_str() == Some("wizard_risk_acknowledged"))),
        "expected risk acknowledgement in JSON summary: {payload}"
    );
    assert!(
        payload
            .get("skipped_sections")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("channels"))
                && values.iter().any(|value| value.as_str() == Some("skills"))),
        "explicit skip flags should remain visible in JSON summary: {payload}"
    );
    assert!(
        payload
            .get("health_checks")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty()),
        "expected structured health checks in JSON summary: {payload}"
    );
    assert!(config_path.exists(), "setup wizard should create config file");
    let config_toml = fs::read_to_string(config_path.as_path())
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    let config_document: toml::Value =
        toml::from_str(config_toml.as_str()).context("setup wizard config should be valid TOML")?;
    assert_eq!(
        config_document
            .get("daemon")
            .and_then(|value| value.get("port"))
            .and_then(toml::Value::as_integer),
        Some(i64::from(daemon_port))
    );
    assert_eq!(
        config_document
            .get("gateway")
            .and_then(|value| value.get("grpc_port"))
            .and_then(toml::Value::as_integer),
        Some(i64::from(grpc_port))
    );
    assert_eq!(
        config_document
            .get("gateway")
            .and_then(|value| value.get("quic_port"))
            .and_then(toml::Value::as_integer),
        Some(i64::from(quic_port))
    );
    Ok(())
}

#[test]
fn setup_wizard_non_interactive_missing_risk_ack_names_accept_risk_flag() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--auth-method",
            "skip",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
        ],
        &[],
    )?;

    assert!(!output.status.success(), "setup wizard must require explicit risk acknowledgement");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("accept_risk_ack"), "stderr should name the missing step: {stderr}");
    assert!(stderr.contains("--accept-risk"), "stderr should name the required flag: {stderr}");
    Ok(())
}

#[test]
fn setup_non_wizard_emits_json_summary() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &["setup", "--mode", "local", "--path", &config_path_string, "--force", "--json"],
        &[],
    )?;
    assert!(
        output.status.success(),
        "setup should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("non-wizard setup stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
    assert_eq!(payload.get("mode").and_then(Value::as_str), Some("local_desktop"));
    assert_eq!(
        payload.get("config_path").and_then(Value::as_str),
        Some(config_path_string.as_str())
    );
    assert_eq!(payload.get("force").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.get("deployment_profile").and_then(Value::as_str), Some("local"));
    assert!(payload.get("state_root").and_then(Value::as_str).is_some());
    assert!(
        payload.get("next").and_then(Value::as_array).is_some_and(|steps| !steps.is_empty()),
        "setup JSON should include next steps: {payload}"
    );
    assert!(config_path.exists(), "setup should create config file");
    let config_toml = fs::read_to_string(&config_path).context("failed to read setup config")?;
    assert!(
        config_toml.contains("[tool_call.browser_service]")
            && config_toml.contains("enabled = true")
            && config_toml.contains("auth_token = \"palyra_browser_")
            && config_toml.contains("state_key_vault_ref = \"global/browser_state_key\""),
        "local setup should configure browser prerequisites by default: {config_toml}"
    );
    Ok(())
}

#[test]
fn onboarding_wizard_stdin_secret_requires_non_interactive_mode() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli_with_stdin(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-stdin",
        ],
        &[],
        Some(b"sk-test-setup\n"),
    )?;

    assert!(!output.status.success(), "wizard must reject stdin secrets without scripted mode");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--api-key-stdin"), "stderr should name the stdin flag: {stderr}");
    assert!(
        stderr.contains("--non-interactive"),
        "stderr should explain the scripted mode requirement: {stderr}"
    );
    assert!(
        !stderr.contains("stdin/stdout/stderr TTY"),
        "specific stdin guidance should be emitted before the generic TTY guard: {stderr}"
    );
    Ok(())
}

#[test]
fn quickstart_defaults_do_not_report_optional_sections_as_explicitly_skipped() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-health",
            "--json",
        ],
        &[("OPENAI_API_KEY", "sk-test-setup")],
    )?;

    assert!(
        output.status.success(),
        "quickstart should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("setup wizard stdout should be JSON")?;
    let skipped = payload
        .get("skipped_sections")
        .and_then(Value::as_array)
        .context("summary should expose skipped_sections")?;
    assert!(
        skipped.iter().all(|value| {
            value.as_str() != Some("channels") && value.as_str() != Some("skills")
        }),
        "quickstart defaults should not be reported as explicit skips: {payload}"
    );
    assert!(
        payload.get("warnings").and_then(Value::as_array).is_some_and(|values| values
            .iter()
            .filter_map(Value::as_str)
            .any(|warning| warning.contains("quickstart default")
                && warning.contains("--skip-channels"))),
        "summary should explain deferred channel setup: {payload}"
    );
    assert!(
        payload.get("warnings").and_then(Value::as_array).is_some_and(|values| values
            .iter()
            .filter_map(Value::as_str)
            .any(|warning| warning.contains("quickstart default")
                && warning.contains("--skip-skills"))),
        "summary should explain deferred skill setup: {payload}"
    );
    Ok(())
}

#[test]
fn setup_wizard_bootstraps_missing_global_config_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("global-config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &[
            "--config",
            &config_path_string,
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("OPENAI_API_KEY", "sk-test-bootstrap-config")],
    )?;
    assert!(
        output.status.success(),
        "setup wizard should accept a missing global --config bootstrap target: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("setup wizard stdout should be JSON")?;
    assert_eq!(
        payload.get("config_path").and_then(Value::as_str),
        Some(config_path_string.as_str())
    );
    assert!(config_path.exists(), "setup wizard should create global config path");
    Ok(())
}

#[test]
fn setup_wizard_quickstart_supports_anthropic_api_key() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let forbidden_listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind forbidden Anthropic endpoint")?;
    let forbidden_base_url =
        format!("http://{}", forbidden_listener.local_addr().context("listener address")?);
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "anthropic-api-key",
            "--api-key-env",
            "ANTHROPIC_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[
            ("ANTHROPIC_API_KEY", "sk-ant-test-setup"),
            ("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL", forbidden_base_url.as_str()),
        ],
    )?;
    assert_no_pending_connection(&forbidden_listener, "Anthropic API-key env base URL override")?;
    assert!(
        output.status.success(),
        "anthropic quickstart should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(written.contains("kind = \"anthropic\""), "expected anthropic provider kind");
    assert!(
        written.contains("anthropic_api_key_vault_ref"),
        "expected vault-backed Anthropic auth in onboarding config"
    );
    assert!(
        written.contains("anthropic_base_url = \"https://api.anthropic.com\""),
        "Anthropic API-key onboarding must use the official Anthropic base URL"
    );
    assert!(
        !written.contains(forbidden_base_url.as_str()),
        "Anthropic API-key onboarding must ignore env-supplied base URLs"
    );
    assert!(
        !written.contains("anthropic_model = "),
        "Anthropic API-key onboarding must not write a model discovered with a freshly supplied key"
    );
    assert!(
        written.contains("provider_id = \"anthropic-primary\""),
        "expected Anthropic provider discovery to remain pending after secure API-key setup"
    );
    Ok(())
}

#[test]
fn setup_wizard_text_summary_surfaces_gateway_start_guidance() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-channels",
            "--skip-skills",
        ],
        &[("OPENAI_API_KEY", "sk-test-runtime-guidance")],
    )?;
    assert!(
        output.status.success(),
        "setup wizard should succeed in text mode: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout should be valid UTF-8")?;
    assert!(
        stdout.contains("onboarding.warning="),
        "text summary should emit concrete warnings instead of only a count: {stdout}"
    );
    assert!(
        stdout.contains("palyra gateway run"),
        "text summary should point to the immediate runtime start command: {stdout}"
    );
    assert!(
        stdout.contains("palyra gateway install --start"),
        "text summary should mention the persistent service install path: {stdout}"
    );
    Ok(())
}

#[test]
fn setup_wizard_rejects_custom_minimax_discovery_base_url() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let forbidden_listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind forbidden MiniMax endpoint")?;
    let minimax_base_url =
        format!("http://{}", forbidden_listener.local_addr().context("listener address")?);
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "minimax-api-key",
            "--api-key-env",
            "MINIMAX_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[
            ("MINIMAX_API_KEY", "sk-minimax-test-setup"),
            ("PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL", minimax_base_url.as_str()),
        ],
    )?;
    assert_no_pending_connection(&forbidden_listener, "MiniMax API-key discovery base URL")?;
    assert!(
        !output.status.success(),
        "MiniMax setup must fail before sending a new key to a custom base URL"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("official MiniMax endpoints")
            && stderr.contains("PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL"),
        "expected trusted-endpoint error for MiniMax custom discovery URL: {stderr}"
    );
    Ok(())
}

#[test]
fn setup_wizard_quickstart_supports_openrouter_api_key() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let model_server = MockProviderServer::spawn(
        r#"{"data":[{"id":"image-only-newer","created":1800000000,"supported_parameters":["temperature"]},{"id":"openrouter-tools-model","created":1700000000,"supported_parameters":["tools","response_format"]}]}"#,
    )?;
    let openrouter_base_url = model_server.base_url.clone();
    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "openrouter-api-key",
            "--api-key-env",
            "OPENROUTER_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[
            ("OPENROUTER_API_KEY", "sk-openrouter-test-setup"),
            ("PALYRA_MODEL_PROVIDER_OPENROUTER_BASE_URL", openrouter_base_url.as_str()),
        ],
    )?;
    let discovery_request = model_server.finish()?;
    assert!(
        discovery_request.starts_with("GET /v1/models "),
        "setup should discover OpenRouter models before writing config: {discovery_request}"
    );
    assert!(
        output.status.success(),
        "OpenRouter quickstart should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        written.contains("auth_provider_kind = \"openrouter\""),
        "expected OpenRouter auth provider kind: {written}"
    );
    assert!(
        written.contains(format!("openai_base_url = \"{openrouter_base_url}\"").as_str()),
        "expected OpenRouter OpenAI-compatible base URL: {written}"
    );
    assert!(
        written.contains("default_chat_model_id = \"openrouter-tools-model\""),
        "expected tool-capable discovered OpenRouter chat model: {written}"
    );
    assert!(
        written.contains("tool_calls = true"),
        "expected discovered OpenRouter tool capability to be persisted: {written}"
    );
    assert!(
        written.contains("allow_private_base_url = true"),
        "expected loopback OpenRouter discovery endpoint to opt into private base URLs"
    );
    assert!(
        written.contains("provider_id = \"openrouter-primary\""),
        "expected OpenRouter provider registry entry: {written}"
    );
    assert!(
        written.contains("api_key_vault_ref = \"global/openrouter_api_key\""),
        "expected vault-backed OpenRouter registry auth: {written}"
    );

    let revealed =
        run_cli(&workdir, &["secrets", "get", "global", "openrouter_api_key", "--reveal"], &[])?;
    assert!(
        revealed.status.success(),
        "secrets get --reveal should succeed after OpenRouter setup: {}",
        String::from_utf8_lossy(&revealed.stderr)
    );
    let revealed_secret =
        String::from_utf8(revealed.stdout).context("revealed secret should be valid UTF-8")?;
    assert_eq!(revealed_secret.trim_end(), "sk-openrouter-test-setup");
    Ok(())
}

#[test]
fn onboarding_wizard_existing_config_preserves_ready_auth_state() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    seed_quickstart_config(&workdir, &config_path)?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--path",
            &config_path_string,
            "--flow",
            "quickstart",
            "--auth-method",
            "existing-config",
            "--non-interactive",
            "--accept-risk",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[],
    )?;
    assert!(
        output.status.success(),
        "existing-config onboarding should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("onboarding stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("existing_config_ready"));
    assert_eq!(payload.get("auth_method").and_then(Value::as_str), Some("existing_config"));
    assert_eq!(
        payload.get("recommended_step_id").and_then(Value::as_str),
        Some("onboarding_status")
    );
    assert!(
        payload
            .get("next_step")
            .and_then(Value::as_str)
            .is_some_and(|value| value.contains("Existing model-provider config was preserved")),
        "existing-config summary should describe config refresh instead of stale startup blockers: {payload}"
    );
    assert!(
        payload.get("risk_events").and_then(Value::as_array).is_some_and(|values| !values
            .iter()
            .any(|value| value.as_str() == Some("model_auth_skipped"))),
        "existing-config should not be reported as skipped auth: {payload}"
    );
    assert!(
        payload.get("warnings").and_then(Value::as_array).is_some_and(|values| !values.iter().any(
            |value| value
                .as_str()
                .is_some_and(|warning| warning.contains("Model-provider auth was skipped")
                    || warning.contains("local runtime startup was deferred"))
        )),
        "existing-config should not emit stale auth/runtime warnings: {payload}"
    );
    Ok(())
}

#[test]
fn onboarding_wizard_without_path_uses_palyra_config_env_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("env-config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "skip",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("PALYRA_CONFIG", &config_path_string)],
    )?;
    assert!(
        output.status.success(),
        "onboarding wizard should honor PALYRA_CONFIG without --path: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: Value =
        serde_json::from_slice(&output.stdout).context("onboarding stdout should be JSON")?;
    assert_eq!(
        payload.get("config_path").and_then(Value::as_str),
        Some(config_path_string.as_str()),
        "onboarding summary should report the PALYRA_CONFIG path: {payload}"
    );
    assert!(config_path.is_file(), "onboarding should write the PALYRA_CONFIG target path");
    assert!(
        !workdir.path().join("palyra.toml").exists(),
        "onboarding should not create an implicit cwd palyra.toml when PALYRA_CONFIG is set"
    );
    Ok(())
}

#[test]
fn setup_wizard_stores_openai_secret_in_state_root_vault_by_default() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli_without_explicit_vault_dir(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("OPENAI_API_KEY", "sk-openai-state-root")],
    )?;
    assert!(
        output.status.success(),
        "OpenAI quickstart should succeed without PALYRA_VAULT_DIR: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let state_root = workdir.path().join("state-root");
    let scope = "global".parse::<VaultScope>().context("failed to parse global vault scope")?;
    let vault = Vault::open_with_config(VaultConfigOptions {
        root: Some(state_root.join("vault")),
        identity_store_root: Some(state_root.join("identity")),
        backend_preference: VaultBackendPreference::EncryptedFile,
        ..VaultConfigOptions::default()
    })
    .context("failed to open state-root vault")?;
    let secret = vault
        .get_secret(&scope, "openai_api_key")
        .context("state-root vault should contain the OpenAI secret")?;
    let secret = String::from_utf8(secret).context("vault secret should be valid UTF-8")?;
    assert_eq!(secret, "sk-openai-state-root");
    Ok(())
}

#[test]
fn setup_wizard_reuse_backfills_admin_defaults() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(config_path.as_path(), "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(config_path.as_path(), fs::Permissions::from_mode(0o644)).with_context(
        || format!("failed to seed broad permissions for {}", config_path.display()),
    )?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "setup",
            "--wizard",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--flow",
            "quickstart",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("OPENAI_API_KEY", "sk-test-setup")],
    )?;

    assert!(
        output.status.success(),
        "setup wizard reuse should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;

    assert!(
        written.contains("require_auth = true"),
        "expected reused config to enable admin auth: {written}"
    );
    assert!(
        written.contains("auth_token = "),
        "expected reused config to contain an admin token: {written}"
    );
    assert!(
        written.contains("bound_principal = \"admin:local\""),
        "expected reused config to contain the local admin principal: {written}"
    );
    Ok(())
}

#[test]
fn onboarding_manual_flow_writes_public_tls_config() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("manual").join("palyra.toml");
    let cert_path = workdir.path().join("tls").join("gateway.crt");
    let key_path = workdir.path().join("tls").join("gateway.key");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let cert_path_string = cert_path.to_string_lossy().into_owned();
    let key_path_string = key_path.to_string_lossy().into_owned();
    let output = run_cli(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--path",
            &config_path_string,
            "--flow",
            "manual",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-env",
            "OPENAI_API_KEY",
            "--bind-profile",
            "public-tls",
            "--daemon-port",
            "7210",
            "--grpc-port",
            "7510",
            "--quic-port",
            "7511",
            "--tls-scaffold",
            "bring-your-own",
            "--tls-cert-path",
            &cert_path_string,
            "--tls-key-path",
            &key_path_string,
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
        ],
        &[("OPENAI_API_KEY", "sk-test-manual")],
    )?;
    assert!(
        output.status.success(),
        "manual onboarding should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(written.contains("bind_profile = \"public_tls\""), "missing public TLS bind profile");
    assert!(written.contains("enabled = true"), "expected TLS enablement in config");
    assert!(
        written.contains(cert_path_string.as_str()) && written.contains(key_path_string.as_str()),
        "expected configured TLS cert/key paths"
    );
    assert!(
        written.contains("dangerous_remote_bind_ack = true"),
        "expected explicit dangerous remote bind acknowledgement"
    );
    Ok(())
}

#[test]
fn onboarding_remote_flow_emits_json_summary_and_persists_pins() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("remote").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let output = run_cli(
        &workdir,
        &[
            "onboarding",
            "wizard",
            "--path",
            &config_path_string,
            "--flow",
            "remote",
            "--non-interactive",
            "--accept-risk",
            "--remote-base-url",
            "https://dashboard.example.com/",
            "--remote-verification",
            "server-cert",
            "--pinned-server-cert-sha256",
            fingerprint,
            "--admin-token-env",
            "PALYRA_REMOTE_ADMIN_TOKEN",
            "--skip-health",
            "--skip-channels",
            "--skip-skills",
            "--json",
        ],
        &[("PALYRA_REMOTE_ADMIN_TOKEN", "test-remote-admin-token")],
    )?;
    assert!(
        output.status.success(),
        "remote onboarding should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(&output.stdout)
        .context("remote onboarding stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("next_step_required"));
    assert_eq!(
        payload.get("recommended_step_id").and_then(Value::as_str),
        Some("onboarding_status")
    );
    assert_eq!(payload.get("flow").and_then(Value::as_str), Some("remote"));
    assert_eq!(payload.get("remote_verification").and_then(Value::as_str), Some("server_cert"));
    assert!(
        payload.get("health_checks").and_then(Value::as_array).is_some_and(|checks| checks
            .iter()
            .any(|check| { check.get("status").and_then(Value::as_str) == Some("skipped") })),
        "expected skipped health check record: {payload}"
    );
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(written.contains("remote_base_url = \"https://dashboard.example.com/\""));
    assert!(
        written.contains("pinned_server_cert_fingerprint_sha256"),
        "expected pinned server certificate fingerprint in config"
    );
    Ok(())
}

#[test]
fn configure_gateway_emits_section_diff_and_rotates_backup() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    seed_quickstart_config(&workdir, &config_path)?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let cert_path = workdir.path().join("tls").join("configured.crt");
    let key_path = workdir.path().join("tls").join("configured.key");
    let cert_path_string = cert_path.to_string_lossy().into_owned();
    let key_path_string = key_path.to_string_lossy().into_owned();
    let fingerprint = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let output = run_cli(
        &workdir,
        &[
            "configure",
            "--path",
            &config_path_string,
            "--section",
            "gateway",
            "--non-interactive",
            "--accept-risk",
            "--bind-profile",
            "public-tls",
            "--daemon-port",
            "7310",
            "--grpc-port",
            "7610",
            "--quic-port",
            "7611",
            "--tls-scaffold",
            "bring-your-own",
            "--tls-cert-path",
            &cert_path_string,
            "--tls-key-path",
            &key_path_string,
            "--remote-base-url",
            "https://dashboard.example.com/",
            "--remote-verification",
            "gateway-ca",
            "--pinned-gateway-ca-sha256",
            fingerprint,
            "--json",
        ],
        &[],
    )?;
    assert!(
        output.status.success(),
        "configure should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("configure stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
    assert!(
        payload
            .get("changed_sections")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("gateway"))),
        "expected gateway in changed sections: {payload}"
    );
    assert!(
        payload.get("section_changes").and_then(Value::as_array).is_some_and(|values| values
            .iter()
            .any(|change| {
                change.get("section").and_then(Value::as_str) == Some("gateway")
                    && change.get("changed").and_then(Value::as_bool) == Some(true)
            })),
        "expected gateway section diff in JSON summary: {payload}"
    );
    assert!(backup_path(&config_path, 1).exists(), "configure should rotate a backup");
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(written.contains("bind_profile = \"public_tls\""));
    assert!(written.contains("remote_base_url = \"https://dashboard.example.com/\""));
    assert!(written.contains("pinned_gateway_ca_fingerprint_sha256"));
    Ok(())
}

#[test]
fn configure_auth_model_accepts_api_key_from_stdin() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    seed_quickstart_config(&workdir, &config_path)?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let secret_bytes = b"sk-configure-stdin-secret\n";
    let forbidden_listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind forbidden OpenAI endpoint")?;
    let forbidden_base_url =
        format!("http://{}", forbidden_listener.local_addr().context("listener address")?);
    let output = run_cli_with_stdin(
        &workdir,
        &[
            "configure",
            "--path",
            &config_path_string,
            "--section",
            "auth-model",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-stdin",
            "--json",
        ],
        &[("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", forbidden_base_url.as_str())],
        Some(secret_bytes),
    )?;
    assert_no_pending_connection(&forbidden_listener, "OpenAI API-key env base URL override")?;
    assert!(
        output.status.success(),
        "configure auth-model should accept stdin secret: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("configure stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
    assert!(
        ["changed_sections", "unchanged_sections"].iter().any(|field| {
            payload.get(*field).and_then(Value::as_array).is_some_and(|values| {
                values.iter().any(|value| value.as_str() == Some("auth-model"))
            })
        }),
        "expected auth-model section to complete after stdin secret input: {payload}"
    );

    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        written.contains("openai_api_key_vault_ref = \"global/openai_api_key\""),
        "expected vault-backed OpenAI auth after configure"
    );
    assert!(
        written.contains("allowed_credential_vault_refs = [\"global/openai_api_key\"]"),
        "configure auth-model should allow the model-provider vault ref for HTTP credential bindings: {written}"
    );
    assert!(
        written.contains("openai_base_url = \"https://api.openai.com/v1\""),
        "OpenAI API-key onboarding must use the official OpenAI base URL"
    );
    assert!(
        !written.contains(forbidden_base_url.as_str()),
        "OpenAI API-key onboarding must ignore env-supplied base URLs"
    );
    assert!(
        !written.contains("openai_model = "),
        "OpenAI API-key onboarding must not write a model discovered with a freshly supplied key"
    );
    assert!(
        written.contains("provider_id = \"openai-primary\""),
        "expected OpenAI provider discovery to remain pending after secure API-key setup"
    );

    let revealed =
        run_cli(&workdir, &["secrets", "get", "global", "openai_api_key", "--reveal"], &[])?;
    assert!(
        revealed.status.success(),
        "secrets get --reveal should succeed after configure auth-model: {}",
        String::from_utf8_lossy(&revealed.stderr)
    );
    let revealed_secret =
        String::from_utf8(revealed.stdout).context("revealed secret should be valid UTF-8")?;
    assert_eq!(revealed_secret.trim_end(), "sk-configure-stdin-secret");
    Ok(())
}

#[test]
fn configure_auth_model_reports_openrouter_registry_vault_ref() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    fs::create_dir_all(config_path.parent().expect("config parent"))
        .context("failed to create config parent")?;
    fs::write(config_path.as_path(), "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli_with_stdin(
        &workdir,
        &[
            "configure",
            "--path",
            &config_path_string,
            "--section",
            "auth-model",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "openrouter-api-key",
            "--api-key-stdin",
            "--skip-health",
            "--json",
        ],
        &[],
        Some(b"sk-openrouter-configure\n"),
    )?;
    assert!(
        output.status.success(),
        "OpenRouter configure should succeed without health probing: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("configure stdout should be JSON")?;
    let auth_model_after = payload
        .get("section_changes")
        .and_then(Value::as_array)
        .and_then(|changes| {
            changes
                .iter()
                .find(|change| change.get("section").and_then(Value::as_str) == Some("auth-model"))
        })
        .and_then(|change| change.get("after"))
        .and_then(Value::as_array)
        .context("OpenRouter configure summary should include auth-model after values")?;
    assert!(
        auth_model_after.iter().any(|value| value.as_str() == Some("auth_source=vault_ref")),
        "OpenRouter configure should report registry vault auth in the section summary: {payload}"
    );

    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        written.contains("api_key_vault_ref = \"global/openrouter_api_key\""),
        "OpenRouter configure should store the key as a registry vault ref: {written}"
    );
    Ok(())
}

#[test]
fn configure_auth_model_rejects_custom_minimax_discovery_base_url() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    fs::create_dir_all(config_path.parent().expect("config parent"))
        .context("failed to create config parent")?;
    fs::write(config_path.as_path(), "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let forbidden_listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind forbidden MiniMax endpoint")?;
    let forbidden_base_url =
        format!("http://{}", forbidden_listener.local_addr().context("listener address")?);
    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli_with_stdin(
        &workdir,
        &[
            "configure",
            "--path",
            &config_path_string,
            "--section",
            "auth-model",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "minimax-api-key",
            "--api-key-stdin",
            "--json",
        ],
        &[("PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL", forbidden_base_url.as_str())],
        Some(b"sk-minimax-rejected\n"),
    )?;
    assert_no_pending_connection(&forbidden_listener, "MiniMax configure discovery base URL")?;
    assert!(
        !output.status.success(),
        "MiniMax configure must fail before sending a new key to a custom base URL"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("official MiniMax endpoints")
            && stderr.contains("PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL"),
        "expected trusted-endpoint error for MiniMax custom discovery URL: {stderr}"
    );
    Ok(())
}

#[test]
fn configure_auth_model_backfills_admin_defaults_for_resume_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    fs::create_dir_all(config_path.parent().expect("config parent"))
        .context("failed to create config parent")?;
    fs::write(config_path.as_path(), "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(config_path.as_path(), fs::Permissions::from_mode(0o644)).with_context(
        || format!("failed to seed broad permissions for {}", config_path.display()),
    )?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let forbidden_listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind forbidden OpenAI endpoint")?;
    let forbidden_base_url =
        format!("http://{}", forbidden_listener.local_addr().context("listener address")?);
    let output = run_cli_with_stdin(
        &workdir,
        &[
            "configure",
            "--path",
            &config_path_string,
            "--section",
            "auth-model",
            "--non-interactive",
            "--accept-risk",
            "--auth-method",
            "api-key",
            "--api-key-stdin",
            "--json",
        ],
        &[("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", forbidden_base_url.as_str())],
        Some(b"sk-openai-resume\n"),
    );
    let output = output?;
    assert_no_pending_connection(&forbidden_listener, "OpenAI API-key env base URL override")?;
    assert!(
        output.status.success(),
        "configure auth-model should complete resume repair: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("configure stdout should be JSON")?;
    assert!(
        payload
            .get("changed_sections")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("auth-model"))),
        "auth-model should be marked changed when admin defaults are backfilled: {payload}"
    );
    let auth_model_change = payload
        .get("section_changes")
        .and_then(Value::as_array)
        .and_then(|changes| {
            changes
                .iter()
                .find(|change| change.get("section").and_then(Value::as_str) == Some("auth-model"))
        })
        .context("configure summary should include auth-model change details")?;
    let after_values = auth_model_change
        .get("after")
        .and_then(Value::as_array)
        .context("auth-model change should include after values")?;
    assert_eq!(
        after_values.first().and_then(Value::as_str),
        Some("provider_display_name=OpenAI-compatible"),
        "OpenAI configure output should lead with the selected provider display name: {payload}"
    );
    assert!(
        after_values
            .iter()
            .any(|value| value.as_str() == Some("protocol_compatibility=openai_compatible")),
        "configure output should expose OpenAI-compatible protocol as a secondary detail: {payload}"
    );
    assert!(
        after_values.iter().any(|value| value.as_str() == Some("provider_kind=openai_compatible")),
        "configure output should preserve the technical compatibility provider kind: {payload}"
    );
    assert!(
        after_values.iter().any(|value| value.as_str() == Some("chat_model=unset")),
        "secure OpenAI API-key setup should leave model discovery pending: {payload}"
    );
    let follow_up_checks = payload
        .get("follow_up_checks")
        .and_then(Value::as_array)
        .context("configure summary should include follow-up checks")?;
    assert!(
        follow_up_checks
            .iter()
            .filter_map(Value::as_str)
            .any(|value| value == "palyra models status"),
        "configure auth-model should keep model diagnostics as the follow-up check: {payload}"
    );
    assert!(
        follow_up_checks.iter().filter_map(Value::as_str).all(|value| {
            !value.contains("model-provider auth changes require runtime reload")
                && !value.contains("restart daemon so model-provider auth changes take effect")
        }),
        "configure auth-model should not emit stale restart guidance: {payload}"
    );
    assert!(
        payload.get("restart_required").and_then(Value::as_array).is_some_and(Vec::is_empty),
        "configure auth-model should not require manual restart: {payload}"
    );
    assert!(
        payload.get("runtime_reload").is_some(),
        "configure auth-model should report best-effort runtime reload state: {payload}"
    );

    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        written.contains("openai_api_key_vault_ref = \"global/openai_api_key\""),
        "expected vault-backed OpenAI auth after configure: {written}"
    );
    assert!(
        written.contains("openai_base_url = \"https://api.openai.com/v1\""),
        "OpenAI API-key onboarding must use the official OpenAI base URL: {written}"
    );
    assert!(
        !written.contains(forbidden_base_url.as_str()),
        "OpenAI API-key onboarding must ignore env-supplied base URLs: {written}"
    );
    assert!(
        !written.contains("openai_model = "),
        "OpenAI API-key onboarding must not write a model discovered with a freshly supplied key: {written}"
    );
    assert!(
        written.contains("identity_store_dir = "),
        "configure auth-model should backfill the gateway identity store path for daemon startup: {written}"
    );
    assert!(
        written.contains("vault_dir = "),
        "configure auth-model should backfill the storage vault path used by CLI-stored secrets: {written}"
    );
    assert!(
        written.contains("runloop_v1_enabled = true"),
        "configure auth-model should enable the local orchestrator run loop for first agent smoke prompts: {written}"
    );
    assert!(
        written.contains("require_auth = true"),
        "configure auth-model should enable admin auth when it repairs a partial install: {written}"
    );
    assert!(
        written.contains("auth_token = "),
        "configure auth-model should write an admin token when missing: {written}"
    );
    assert!(
        written.contains("bound_principal = \"admin:local\""),
        "configure auth-model should write the local admin principal when missing: {written}"
    );
    assert!(
        written.contains("profile = \"local\""),
        "configure auth-model should backfill the local deployment profile for preflight: {written}"
    );
    assert!(
        written.contains("mode = \"local_desktop\""),
        "configure auth-model should backfill local deployment mode for preflight: {written}"
    );
    assert!(
        written.contains("bind_profile = \"loopback_only\""),
        "configure auth-model should backfill loopback bind profile for preflight: {written}"
    );
    #[cfg(unix)]
    {
        let mode = fs::metadata(config_path.as_path())
            .with_context(|| format!("failed to stat {}", config_path.display()))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "configure auth-model must tighten existing config permissions before writing generated admin token"
        );
    }
    Ok(())
}

#[test]
fn profile_lifecycle_create_and_setup_attach_profile_paths() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let create = run_cli(
        &workdir,
        &["profile", "create", "staging", "--mode", "remote", "--set-default", "--json"],
        &[],
    )?;
    assert!(
        create.status.success(),
        "profile create should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    let create_payload: Value =
        serde_json::from_slice(&create.stdout).context("profile create stdout should be JSON")?;
    assert_eq!(create_payload.get("action").and_then(Value::as_str), Some("create"));
    assert_eq!(create_payload.get("default_profile").and_then(Value::as_str), Some("staging"));
    assert_eq!(create_payload.pointer("/profile/name").and_then(Value::as_str), Some("staging"));

    let config_path = workdir.path().join("profiles").join("staging.toml");
    let config_path_string = config_path.to_string_lossy().into_owned();
    let setup = run_cli(
        &workdir,
        &[
            "--profile",
            "staging",
            "setup",
            "--mode",
            "local",
            "--path",
            &config_path_string,
            "--force",
        ],
        &[],
    )?;
    assert!(
        setup.status.success(),
        "profile-scoped setup should succeed: {}",
        String::from_utf8_lossy(&setup.stderr)
    );

    let profiles = fs::read_to_string(profiles_registry_path(&workdir))
        .context("expected CLI profiles registry to exist after profile setup")?;
    assert!(profiles.contains("default_profile = \"staging\""));
    assert!(
        profiles.contains(config_path_string.as_str()),
        "expected setup to persist profile config path"
    );
    assert!(
        profiles.contains("state-root")
            && profiles.contains("profiles")
            && profiles.contains("staging"),
        "expected setup to keep an isolated per-profile state root"
    );
    Ok(())
}

#[test]
fn profile_delete_requires_yes_for_active_profile() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let create = run_cli(
        &workdir,
        &["profile", "create", "prod", "--mode", "remote", "--set-default"],
        &[],
    )?;
    assert!(
        create.status.success(),
        "profile create should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    let delete = run_cli(&workdir, &["--profile", "prod", "profile", "delete", "prod"], &[])?;
    assert!(!delete.status.success(), "active profile delete should require --yes");
    let stderr = String::from_utf8_lossy(&delete.stderr);
    assert!(stderr.contains("without --yes"), "expected explicit safety message, got: {stderr}");
    Ok(())
}

#[test]
fn profile_clone_copies_config_into_isolated_namespace() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let source_config = workdir.path().join("profiles").join("prod.toml");
    seed_quickstart_config(&workdir, &source_config)?;
    let source_config_string = source_config.to_string_lossy().into_owned();
    let create = run_cli(
        &workdir,
        &[
            "profile",
            "create",
            "prod",
            "--mode",
            "remote",
            "--config-path",
            &source_config_string,
            "--set-default",
            "--json",
        ],
        &[],
    )?;
    assert!(
        create.status.success(),
        "profile create should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let cloned = run_cli(
        &workdir,
        &["profile", "clone", "prod", "staging", "--set-default", "--json"],
        &[],
    )?;
    assert!(
        cloned.status.success(),
        "profile clone should succeed: {}",
        String::from_utf8_lossy(&cloned.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&cloned.stdout).context("profile clone stdout should be JSON")?;
    assert_eq!(payload.get("action").and_then(Value::as_str), Some("clone"));
    assert_eq!(payload.get("source_profile").and_then(Value::as_str), Some("prod"));
    assert_eq!(payload.pointer("/profile/name").and_then(Value::as_str), Some("staging"));
    assert_eq!(
        payload.pointer("/validation/config_snapshot_written").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/validation/isolated_state_root").and_then(Value::as_bool),
        Some(true)
    );
    let cloned_config = workdir
        .path()
        .join("state-root")
        .join("profiles")
        .join("staging")
        .join("config")
        .join("palyra.toml");
    assert!(cloned_config.exists(), "expected cloned config snapshot to exist");
    let cloned_config_raw = fs::read_to_string(&cloned_config)
        .with_context(|| format!("failed to read {}", cloned_config.display()))?;
    assert!(
        cloned_config_raw.contains("openai_api_key_vault_ref"),
        "expected cloned config to preserve config snapshot"
    );
    let registry = fs::read_to_string(profiles_registry_path(&workdir))
        .context("expected CLI profiles registry after clone")?;
    assert!(registry.contains("default_profile = \"staging\""));
    assert!(
        registry.contains("state-root")
            && registry.contains("profiles")
            && registry.contains("staging")
            && registry.contains("config")
    );
    Ok(())
}

#[test]
fn profile_export_redacted_hides_inline_secrets() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("profiles").join("redacted.toml");
    fs::create_dir_all(config_path.parent().context("missing config parent")?)?;
    fs::write(
        &config_path,
        r#"
[daemon]
port = 7142

[model_provider]
kind = "openai_compatible"
openai_base_url = "https://api.openai.com/v1"
openai_api_key = "sk-inline-secret"
anthropic_api_key_vault_ref = "global/anthropic_api_key"
"#,
    )?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let create = run_cli(
        &workdir,
        &["profile", "create", "redacted", "--config-path", &config_path_string, "--force"],
        &[],
    )?;
    assert!(
        create.status.success(),
        "profile create should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let export_path = workdir.path().join("exports").join("redacted-profile.json");
    let export_path_string = export_path.to_string_lossy().into_owned();
    let exported = run_cli(
        &workdir,
        &["profile", "export", "redacted", "--output", &export_path_string, "--json"],
        &[],
    )?;
    assert!(
        exported.status.success(),
        "profile export should succeed: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let exported_bundle: Value = serde_json::from_slice(
        &fs::read(&export_path)
            .with_context(|| format!("failed to read {}", export_path.display()))?,
    )
    .context("exported bundle should be JSON")?;
    let config_content = exported_bundle
        .pointer("/config/content")
        .and_then(Value::as_str)
        .context("expected config snapshot in exported bundle")?;
    assert!(
        config_content.contains("<redacted>"),
        "expected redacted bundle to hide secret values"
    );
    assert!(
        !config_content.contains("sk-inline-secret"),
        "inline secret must not survive redacted export"
    );
    assert_eq!(
        exported_bundle.pointer("/secret_references/0/reference").and_then(Value::as_str),
        Some("global/anthropic_api_key")
    );
    Ok(())
}

#[test]
fn profile_export_encrypted_and_import_reports_missing_secret_refs() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("profiles").join("prod.toml");
    fs::create_dir_all(config_path.parent().context("missing config parent")?)?;
    fs::write(
        &config_path,
        r#"
[daemon]
port = 7142

[model_provider]
kind = "openai_compatible"
openai_base_url = "https://api.openai.com/v1"
openai_api_key_vault_ref = "global/missing_openai_key"
"#,
    )?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let create = run_cli(
        &workdir,
        &[
            "profile",
            "create",
            "prod",
            "--mode",
            "remote",
            "--config-path",
            &config_path_string,
            "--admin-token-env",
            "PALYRA_PROD_ADMIN_TOKEN",
            "--force",
        ],
        &[],
    )?;
    assert!(
        create.status.success(),
        "profile create should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let export_path = workdir.path().join("exports").join("prod-profile.enc");
    let export_path_string = export_path.to_string_lossy().into_owned();
    let exported = run_cli_with_stdin(
        &workdir,
        &[
            "profile",
            "export",
            "prod",
            "--output",
            &export_path_string,
            "--mode",
            "encrypted",
            "--password-stdin",
            "--json",
        ],
        &[],
        Some(b"test-password\n"),
    )?;
    assert!(
        exported.status.success(),
        "encrypted profile export should succeed: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let encrypted_raw = fs::read_to_string(&export_path)
        .with_context(|| format!("failed to read {}", export_path.display()))?;
    assert!(encrypted_raw.contains("palyra_cli_profile_bundle_encrypted_v1"));
    assert!(
        !encrypted_raw.contains("missing_openai_key")
            && !encrypted_raw.contains("PALYRA_PROD_ADMIN_TOKEN"),
        "encrypted bundle should not expose exported profile details in plaintext"
    );

    let imported = run_cli_with_stdin(
        &workdir,
        &[
            "profile",
            "import",
            "--input",
            &export_path_string,
            "--name",
            "imported",
            "--password-stdin",
            "--json",
        ],
        &[],
        Some(b"test-password\n"),
    )?;
    assert!(
        imported.status.success(),
        "profile import should succeed: {}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&imported.stdout).context("profile import stdout should be JSON")?;
    assert_eq!(payload.get("action").and_then(Value::as_str), Some("import"));
    assert_eq!(payload.pointer("/profile/name").and_then(Value::as_str), Some("imported"));
    assert_eq!(
        payload.pointer("/validation/summary/blocking_findings").and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        payload.pointer("/validation/findings").and_then(Value::as_array).is_some_and(|findings| {
            findings.iter().any(|finding| {
                finding.get("code").and_then(Value::as_str) == Some("missing_secret_reference")
            })
        }),
        "expected missing secret validation finding: {payload}"
    );
    assert_eq!(
        payload.pointer("/validation/isolated_config_path").and_then(Value::as_bool),
        Some(true)
    );
    let imported_config = workdir
        .path()
        .join("state-root")
        .join("profiles")
        .join("imported")
        .join("config")
        .join("palyra.toml");
    assert!(imported_config.exists(), "expected imported config snapshot");
    Ok(())
}

struct MockProviderServer {
    base_url: String,
    handle: thread::JoinHandle<Result<String>>,
}

impl MockProviderServer {
    fn spawn(response_body: &'static str) -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind mock provider server")?;
        listener.set_nonblocking(true).context("failed to configure mock provider listener")?;
        let base_url = format!("http://{}", listener.local_addr().context("listener address")?);
        let handle = thread::spawn(move || -> Result<String> {
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .context("failed to configure mock provider stream")?;
                        let mut buffer = [0_u8; 4096];
                        let read =
                            stream.read(&mut buffer).context("failed to read provider request")?;
                        let request_text = String::from_utf8_lossy(&buffer[..read]).to_string();
                        if !request_text.starts_with("GET /v1/models ") {
                            anyhow::bail!("unexpected provider request: {request_text}");
                        }
                        let request_lower = request_text.to_ascii_lowercase();
                        if !request_lower.contains("authorization: bearer ") {
                            anyhow::bail!(
                                "model discovery request should use bearer auth: {request_text}"
                            );
                        }
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .context("failed to write provider response")?;
                        return Ok(request_text);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => return Err(error).context("mock provider accept failed"),
                }
            }
            anyhow::bail!("mock provider did not receive a model discovery request")
        });

        Ok(Self { base_url, handle })
    }

    fn finish(self) -> Result<String> {
        self.handle.join().map_err(|_| anyhow::anyhow!("mock provider server panicked"))?
    }
}
