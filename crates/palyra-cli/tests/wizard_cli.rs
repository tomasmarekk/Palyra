use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result};
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
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
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
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
    assert_eq!(payload.get("flow").and_then(Value::as_str), Some("quickstart"));
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
            .get("health_checks")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty()),
        "expected structured health checks in JSON summary: {payload}"
    );
    assert!(config_path.exists(), "setup wizard should create config file");
    Ok(())
}

#[test]
fn setup_wizard_quickstart_supports_anthropic_api_key() -> Result<()> {
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
            "anthropic-api-key",
            "--api-key-env",
            "ANTHROPIC_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("ANTHROPIC_API_KEY", "sk-ant-test-setup")],
    )?;
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
        written.contains("anthropic_model = \"claude-3-5-sonnet-latest\""),
        "expected anthropic model default in config"
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
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
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
