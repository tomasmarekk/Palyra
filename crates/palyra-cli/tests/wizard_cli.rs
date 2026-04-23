use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
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
fn setup_wizard_quickstart_supports_minimax_api_key() -> Result<()> {
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
            "minimax-api-key",
            "--api-key-env",
            "MINIMAX_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("MINIMAX_API_KEY", "sk-minimax-test-setup")],
    )?;
    assert!(
        output.status.success(),
        "MiniMax quickstart should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(written.contains("kind = \"anthropic\""), "expected Anthropic-compatible provider");
    assert!(
        written.contains("auth_provider_kind = \"minimax\""),
        "expected MiniMax auth provider kind"
    );
    assert!(
        written.contains("anthropic_base_url = \"https://api.minimax.io/anthropic\""),
        "expected MiniMax Anthropic-compatible endpoint"
    );
    assert!(
        written.contains("anthropic_model = \"MiniMax-M2.7\""),
        "expected MiniMax default model"
    );
    assert!(
        written.contains("anthropic_api_key_vault_ref = \"global/minimax_api_key\""),
        "expected vault-backed MiniMax auth in onboarding config"
    );
    Ok(())
}

#[test]
fn setup_wizard_stores_minimax_secret_in_state_root_vault_by_default() -> Result<()> {
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
            "minimax-api-key",
            "--api-key-env",
            "MINIMAX_API_KEY",
            "--skip-channels",
            "--skip-skills",
            "--skip-health",
        ],
        &[("MINIMAX_API_KEY", "sk-minimax-state-root")],
    )?;
    assert!(
        output.status.success(),
        "MiniMax quickstart should succeed without PALYRA_VAULT_DIR: {}",
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
        .get_secret(&scope, "minimax_api_key")
        .context("state-root vault should contain the MiniMax secret")?;
    let secret = String::from_utf8(secret).context("vault secret should be valid UTF-8")?;
    assert_eq!(secret, "sk-minimax-state-root");
    Ok(())
}

#[test]
fn setup_wizard_reuse_backfills_admin_defaults() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(config_path.as_path(), "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
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
fn configure_auth_model_accepts_api_key_from_stdin() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("configure").join("palyra.toml");
    seed_quickstart_config(&workdir, &config_path)?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let secret_bytes = b"sk-configure-stdin-secret\n";
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
        &[],
        Some(secret_bytes),
    )?;
    assert!(
        output.status.success(),
        "configure auth-model should accept stdin secret: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("configure stdout should be JSON")?;
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("complete"));
    assert!(
        payload
            .get("unchanged_sections")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value.as_str() == Some("auth-model"))),
        "expected auth-model section to complete even when only the stored secret value changes: {payload}"
    );

    let written = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        written.contains("openai_api_key_vault_ref = \"global/openai_api_key\""),
        "expected vault-backed OpenAI auth after configure"
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
