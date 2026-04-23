use std::fs;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"))
        .env("PROGRAMDATA", workdir.path().join("programdata"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("PALYRA_VAULT_DIR", workdir.path().join("vault"));
}

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn bootstrap_local_config(workdir: &TempDir) -> Result<String> {
    let config_path = workdir.path().join("config").join("palyra.toml");
    fs::create_dir_all(config_path.parent().expect("config parent"))?;
    let config_arg = config_path.display().to_string();
    let output =
        run_cli(workdir, &["setup", "--mode", "local", "--path", config_arg.as_str(), "--force"])?;
    assert!(
        output.status.success(),
        "setup should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(config_arg)
}

fn parse_stdout_json(output: Output, label: &str) -> Result<Value> {
    assert!(
        output.status.success(),
        "{label} should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).with_context(|| {
        format!("{label} should emit valid JSON: {}", String::from_utf8_lossy(&output.stdout))
    })
}

#[test]
fn global_output_format_json_is_honored_for_core_cli_surfaces() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = bootstrap_local_config(&workdir)?;
    let support_bundle_path = workdir.path().join("artifacts").join("support-bundle.json");
    let support_bundle_path_string = support_bundle_path.display().to_string();

    let doctor = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "doctor"])?,
        "doctor --output-format json",
    )?;
    assert!(doctor.get("diagnostics").is_some(), "doctor JSON should include diagnostics");

    let validate = parse_stdout_json(
        run_cli(
            &workdir,
            &["--output-format", "json", "config", "validate", "--path", config_path.as_str()],
        )?,
        "config validate --output-format json",
    )?;
    assert_eq!(validate.get("status").and_then(Value::as_str), Some("valid"));

    let config_list = parse_stdout_json(
        run_cli(
            &workdir,
            &["--output-format", "json", "config", "list", "--path", config_path.as_str()],
        )?,
        "config list --output-format json",
    )?;
    assert!(
        config_list.pointer("/document/daemon").is_some(),
        "config list JSON should include the config document: {config_list}"
    );

    let docs_search = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "docs", "search", "gateway"])?,
        "docs search --output-format json",
    )?;
    assert!(docs_search.is_array(), "docs search should emit a JSON array: {docs_search}");

    let secrets_list = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "secrets", "list", "global"])?,
        "secrets list --output-format json",
    )?;
    assert_eq!(secrets_list.get("scope").and_then(Value::as_str), Some("global"));
    assert!(secrets_list.get("entries").and_then(Value::as_array).is_some());

    let policy_explain = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "policy", "explain"])?,
        "policy explain --output-format json",
    )?;
    assert_eq!(policy_explain.get("decision").and_then(Value::as_str), Some("deny_by_default"));

    let protocol_version = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "protocol", "version"])?,
        "protocol version --output-format json",
    )?;
    assert_eq!(protocol_version.get("protocol_major").and_then(Value::as_u64), Some(1));

    let skills_list = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "skills", "list"])?,
        "skills list --output-format json",
    )?;
    assert_eq!(skills_list.get("count").and_then(Value::as_u64), Some(0));

    let support_bundle = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "--output-format",
                "json",
                "support-bundle",
                "export",
                "--output",
                support_bundle_path_string.as_str(),
            ],
        )?,
        "support-bundle export --output-format json",
    )?;
    assert_eq!(
        support_bundle.get("path").and_then(Value::as_str),
        Some(support_bundle_path_string.as_str())
    );
    assert!(support_bundle_path.is_file(), "support bundle artifact should be written");

    Ok(())
}
