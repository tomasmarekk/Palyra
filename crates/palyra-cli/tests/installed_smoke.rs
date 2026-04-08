use std::fs;

use anyhow::{Context, Result};
use serde_json::Value;

mod support;

use support::bin_under_test::palyra_bin;
use support::cli_harness::{
    assert_json_success, assert_success, required_env_path, required_env_var, run_cli,
    run_cli_with_stdin, temp_workdir,
};

const SERVER_CERT_PIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const GATEWAY_CA_PIN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct InstallSmokeContext {
    archive_path: std::path::PathBuf,
    install_root: std::path::PathBuf,
    config_path: std::path::PathBuf,
    state_root: std::path::PathBuf,
}

fn install_smoke_context() -> Result<Option<InstallSmokeContext>> {
    if std::env::var_os("PALYRA_BIN_UNDER_TEST").is_none() {
        eprintln!("skipping installed smoke test because PALYRA_BIN_UNDER_TEST is not configured");
        return Ok(None);
    }

    Ok(Some(InstallSmokeContext {
        archive_path: required_env_path("PALYRA_INSTALL_ARCHIVE_PATH")?,
        install_root: required_env_path("PALYRA_INSTALL_ROOT")?,
        config_path: required_env_path("PALYRA_CONFIG_UNDER_TEST")?,
        state_root: required_env_path("PALYRA_STATE_ROOT_UNDER_TEST")?,
    }))
}

#[test]
fn installed_binary_runs_baseline_smoke_commands() -> Result<()> {
    let Some(context) = install_smoke_context()? else {
        return Ok(());
    };
    let binary_path = palyra_bin()
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", palyra_bin().display()))?;
    let install_root_resolved = context
        .install_root
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", context.install_root.display()))?;

    assert!(
        binary_path.starts_with(&install_root_resolved),
        "installed smoke must execute the installed binary, got {} outside {}",
        binary_path.display(),
        install_root_resolved.display()
    );

    let workdir = temp_workdir()?;
    let workdir_path = workdir.path();
    let config_path_string = context.config_path.display().to_string();
    let install_root_string = context.install_root.display().to_string();
    let archive_path_string = context.archive_path.display().to_string();
    let state_root_string = context.state_root.display().to_string();
    let envs = [
        ("PALYRA_CONFIG", config_path_string.as_str()),
        ("PALYRA_STATE_ROOT", state_root_string.as_str()),
    ];

    assert_success(&run_cli(workdir_path, &["version"], &envs)?, "palyra version")?;
    assert_success(&run_cli(workdir_path, &["--help"], &envs)?, "palyra --help")?;

    let doctor_payload = assert_json_success(
        run_cli(workdir_path, &["doctor", "--json"], &envs)?,
        "palyra doctor --json",
    )?;
    assert!(
        doctor_payload.pointer("/diagnostics/checks").and_then(Value::as_array).is_some(),
        "doctor output should include diagnostics.checks array: {doctor_payload}"
    );

    assert_success(
        &run_cli(workdir_path, &["protocol", "version"], &envs)?,
        "palyra protocol version",
    )?;
    assert_success(
        &run_cli(
            workdir_path,
            &["config", "validate", "--path", config_path_string.as_str()],
            &envs,
        )?,
        "palyra config validate",
    )?;
    assert_success(
        &run_cli(workdir_path, &["docs", "search", "migration"], &envs)?,
        "palyra docs search migration",
    )?;
    assert_success(
        &run_cli(workdir_path, &["docs", "search", "acp"], &envs)?,
        "palyra docs search acp",
    )?;

    let update_payload = assert_json_success(
        run_cli(
            workdir_path,
            &[
                "--output-format",
                "json",
                "update",
                "--install-root",
                install_root_string.as_str(),
                "--archive",
                archive_path_string.as_str(),
                "--dry-run",
            ],
            &envs,
        )?,
        "palyra update --dry-run",
    )?;
    assert_eq!(update_payload.get("mode").and_then(Value::as_str), Some("candidate-plan"));
    assert_eq!(update_payload.get("apply_supported").and_then(Value::as_bool), Some(false));
    assert_eq!(
        update_payload.pointer("/candidate/archive_path").and_then(Value::as_str),
        Some(archive_path_string.as_str())
    );

    let uninstall_payload = assert_json_success(
        run_cli(
            workdir_path,
            &[
                "--output-format",
                "json",
                "uninstall",
                "--install-root",
                install_root_string.as_str(),
                "--dry-run",
            ],
            &envs,
        )?,
        "palyra uninstall --dry-run",
    )?;
    assert_eq!(
        uninstall_payload.get("install_root").and_then(Value::as_str),
        Some(install_root_resolved.display().to_string().as_str())
    );
    assert_eq!(uninstall_payload.get("dry_run").and_then(Value::as_bool), Some(true));

    Ok(())
}

#[test]
fn installed_binary_runs_noninteractive_setup_and_configure_flows() -> Result<()> {
    if install_smoke_context()?.is_none() {
        return Ok(());
    }

    let workdir = temp_workdir()?;
    let workdir_path = workdir.path();
    let local_config = workdir_path.join("config").join("palyra.toml");
    let remote_config = workdir_path.join("remote").join("palyra.toml");
    let cert_path = workdir_path.join("tls").join("gateway.crt");
    let key_path = workdir_path.join("tls").join("gateway.key");
    let local_config_string = local_config.display().to_string();
    let remote_config_string = remote_config.display().to_string();
    let cert_path_string = cert_path.display().to_string();
    let key_path_string = key_path.display().to_string();

    let setup_payload = assert_json_success(
        run_cli(
            workdir_path,
            &[
                "setup",
                "--wizard",
                "--mode",
                "local",
                "--path",
                local_config_string.as_str(),
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
            &[("OPENAI_API_KEY", "sk-installed-smoke")],
        )?,
        "palyra setup wizard",
    )?;
    assert_eq!(setup_payload.get("status").and_then(Value::as_str), Some("complete"));
    assert!(local_config.is_file(), "setup should create {}", local_config.display());

    let remote_payload = assert_json_success(
        run_cli(
            workdir_path,
            &[
                "onboarding",
                "wizard",
                "--path",
                remote_config_string.as_str(),
                "--flow",
                "remote",
                "--non-interactive",
                "--accept-risk",
                "--remote-base-url",
                "https://dashboard.example.com/",
                "--remote-verification",
                "server-cert",
                "--pinned-server-cert-sha256",
                SERVER_CERT_PIN,
                "--admin-token-env",
                "PALYRA_REMOTE_ADMIN_TOKEN",
                "--skip-health",
                "--skip-channels",
                "--skip-skills",
                "--json",
            ],
            &[("PALYRA_REMOTE_ADMIN_TOKEN", "remote-admin-token")],
        )?,
        "palyra onboarding wizard remote",
    )?;
    assert_eq!(remote_payload.get("flow").and_then(Value::as_str), Some("remote"));
    assert!(remote_config.is_file(), "remote onboarding should create {}", remote_config.display());

    let configure_payload = assert_json_success(
        run_cli(
            workdir_path,
            &[
                "configure",
                "--path",
                local_config_string.as_str(),
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
                cert_path_string.as_str(),
                "--tls-key-path",
                key_path_string.as_str(),
                "--remote-base-url",
                "https://dashboard.example.com/",
                "--remote-verification",
                "gateway-ca",
                "--pinned-gateway-ca-sha256",
                GATEWAY_CA_PIN,
                "--json",
            ],
            &[],
        )?,
        "palyra configure gateway",
    )?;
    assert!(
        configure_payload
            .get("changed_sections")
            .and_then(Value::as_array)
            .is_some_and(|sections| sections.iter().any(|value| value.as_str() == Some("gateway"))),
        "configure should report the gateway section change: {configure_payload}"
    );

    Ok(())
}

#[test]
fn installed_binary_roundtrips_secret_values() -> Result<()> {
    if install_smoke_context()?.is_none() {
        return Ok(());
    }

    let workdir = temp_workdir()?;
    let workdir_path = workdir.path();
    let secret_bytes = b"sk-installed-secret\nline-2\n";

    assert_success(
        &run_cli_with_stdin(
            workdir_path,
            &["secrets", "set", "global", "openai_api_key", "--value-stdin"],
            &[],
            secret_bytes,
        )?,
        "palyra secrets set",
    )?;

    let reveal_output =
        run_cli(workdir_path, &["secrets", "get", "global", "openai_api_key", "--reveal"], &[])?;
    assert_success(&reveal_output, "palyra secrets get --reveal")?;
    assert_eq!(reveal_output.stdout, secret_bytes);

    let redacted_output =
        run_cli(workdir_path, &["secrets", "get", "global", "openai_api_key"], &[])?;
    assert_success(&redacted_output, "palyra secrets get")?;
    let stdout = String::from_utf8(redacted_output.stdout).context("stdout was not UTF-8")?;
    assert!(
        stdout.contains("value=<redacted>"),
        "secrets get without --reveal should redact output: {stdout}"
    );

    Ok(())
}

#[test]
fn installed_smoke_requires_explicit_binary_override_in_install_context() -> Result<()> {
    let Some(context) = install_smoke_context()? else {
        return Ok(());
    };

    let install_root_resolved = context
        .install_root
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", context.install_root.display()))?;
    let binary_path = palyra_bin();
    let resolved = binary_path
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", binary_path.display()))?;

    assert!(
        resolved.starts_with(&install_root_resolved),
        "install smoke should be pinned to PALYRA_BIN_UNDER_TEST inside {} but resolved {}",
        install_root_resolved.display(),
        resolved.display()
    );

    let metadata_path = context.install_root.join("install-metadata.json");
    let metadata = serde_json::from_slice::<Value>(
        fs::read(&metadata_path)
            .with_context(|| format!("failed to read {}", metadata_path.display()))?
            .as_slice(),
    )
    .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    let configured_binary = required_env_var("PALYRA_BIN_UNDER_TEST")?;
    assert!(
        metadata.pointer("/cli_exposure/target_binary_path").and_then(Value::as_str)
            == Some(configured_binary.as_str()),
        "install metadata should reference the configured installed binary path: {metadata}"
    );

    Ok(())
}
