use std::fs;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use rusqlite::Connection;
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

fn parse_stderr_json(output: &Output, label: &str) -> Result<Value> {
    serde_json::from_slice(&output.stderr).with_context(|| {
        format!(
            "{label} should emit valid JSON to stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn global_output_format_json_is_honored_for_early_cli_errors() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    bootstrap_local_config(&workdir)?;

    let missing_config =
        run_cli(&workdir, &["--config", "missing.toml", "status", "--output-format", "json"])?;
    assert!(!missing_config.status.success(), "missing config should fail");
    let missing_config_payload =
        parse_stderr_json(&missing_config, "missing config --output-format json")?;
    assert_eq!(
        missing_config_payload.pointer("/error/kind").and_then(Value::as_str),
        Some("not_found")
    );
    assert!(
        missing_config_payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("config file does not exist")),
        "unexpected missing config envelope: {missing_config_payload}"
    );

    let profile_mismatch =
        run_cli(&workdir, &["--expect-profile", "prod", "status", "--output-format", "json"])?;
    assert!(!profile_mismatch.status.success(), "profile mismatch should fail");
    let profile_mismatch_payload =
        parse_stderr_json(&profile_mismatch, "profile mismatch --output-format json")?;
    assert_eq!(
        profile_mismatch_payload.pointer("/error/kind").and_then(Value::as_str),
        Some("validation_error")
    );
    assert_eq!(
        profile_mismatch_payload.pointer("/error/profile").and_then(Value::as_str),
        Some("local")
    );

    let parser_error = run_cli(
        &workdir,
        &["config", "get", "tool_call.allowed_tools", "--output-format", "json"],
    )?;
    assert!(!parser_error.status.success(), "parser error should fail");
    let parser_error_payload = parse_stderr_json(&parser_error, "parser --output-format json")?;
    assert_eq!(
        parser_error_payload.pointer("/error/kind").and_then(Value::as_str),
        Some("validation_error")
    );
    assert!(
        parser_error_payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("unexpected argument")),
        "unexpected parser error envelope: {parser_error_payload}"
    );

    Ok(())
}

#[test]
fn command_level_health_json_reports_unavailable_runtime_as_json() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli(
        &workdir,
        &[
            "health",
            "--url",
            "http://user:HTTP_PASS_123@127.0.0.1:1?api_key=HTTP_KEY_456",
            "--grpc-url",
            "http://user:GRPC_PASS_ABC@127.0.0.1:1?password=GRPC_SECRET_XYZ",
            "--json",
        ],
    )?;

    assert!(!output.status.success(), "health should fail when runtime is unavailable");
    let payload = parse_stderr_json(&output, "health --json failure")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    for secret in ["HTTP_PASS_123", "HTTP_KEY_456", "GRPC_PASS_ABC", "GRPC_SECRET_XYZ"] {
        assert!(!stderr.contains(secret));
    }
    assert!(stderr.contains("<redacted>"), "health JSON should redact URL credentials: {stderr}");
    assert_eq!(payload.get("status").and_then(Value::as_str), Some("error"));
    assert_eq!(payload.get("overall").and_then(Value::as_str), Some("unavailable"));
    assert_eq!(
        payload.pointer("/error/kind").and_then(Value::as_str),
        Some("connectivity_failure")
    );
    assert_eq!(payload.pointer("/http/status").and_then(Value::as_str), Some("error"));
    assert_eq!(payload.pointer("/grpc/status").and_then(Value::as_str), Some("error"));
    Ok(())
}

#[test]
fn global_output_format_json_is_honored_for_core_cli_surfaces() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = bootstrap_local_config(&workdir)?;
    let support_bundle_path = workdir.path().join("artifacts").join("support-bundle.json");
    let support_bundle_path_string = support_bundle_path.display().to_string();
    let setup_global_path = workdir.path().join("config").join("setup-global.toml");
    let setup_global_path_string = setup_global_path.display().to_string();

    let setup = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "--output-format",
                "json",
                "setup",
                "--mode",
                "local",
                "--path",
                setup_global_path_string.as_str(),
                "--force",
            ],
        )?,
        "setup --output-format json",
    )?;
    assert_eq!(setup.get("status").and_then(Value::as_str), Some("complete"));
    assert_eq!(
        setup.get("config_path").and_then(Value::as_str),
        Some(setup_global_path_string.as_str())
    );

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

    let config_set = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "--output-format",
                "json",
                "config",
                "set",
                "--path",
                config_path.as_str(),
                "--key",
                "daemon.port",
                "--value",
                "7444",
            ],
        )?,
        "config set --output-format json",
    )?;
    assert_eq!(config_set.get("key").and_then(Value::as_str), Some("daemon.port"));
    assert_eq!(config_set.get("source").and_then(Value::as_str), Some(config_path.as_str()));
    assert_eq!(config_set.get("backups").and_then(Value::as_u64), Some(5));
    assert!(config_set.get("migrated").and_then(Value::as_bool).is_some());

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

    let config_get = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "--output-format",
                "json",
                "config",
                "get",
                "--path",
                config_path.as_str(),
                "--key",
                "daemon.port",
            ],
        )?,
        "config get --output-format json",
    )?;
    assert_eq!(config_get.get("key").and_then(Value::as_str), Some("daemon.port"));
    assert_eq!(config_get.get("value").and_then(Value::as_i64), Some(7444));
    assert_eq!(config_get.get("source").and_then(Value::as_str), Some(config_path.as_str()));

    let local_config_set = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "config",
                "set",
                "--path",
                config_path.as_str(),
                "--key",
                "daemon.port",
                "--value",
                "7445",
                "--json",
            ],
        )?,
        "config set --json",
    )?;
    assert_eq!(local_config_set.get("key").and_then(Value::as_str), Some("daemon.port"));
    assert_eq!(local_config_set.get("source").and_then(Value::as_str), Some(config_path.as_str()));
    assert_eq!(local_config_set.get("backups").and_then(Value::as_u64), Some(5));
    assert!(local_config_set.get("migrated").and_then(Value::as_bool).is_some());

    let docs_search = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "docs", "search", "gateway"])?,
        "docs search --output-format json",
    )?;
    assert!(docs_search.is_array(), "docs search should emit a JSON array: {docs_search}");

    let secrets_output = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "secrets", "list", "global"])?,
        "secrets list --output-format json",
    )?;
    assert_eq!(secrets_output.get("scope").and_then(Value::as_str), Some("global"));
    assert!(secrets_output.get("entries").and_then(Value::as_array).is_some());
    assert!(
        !secrets_output.to_string().contains("value_bytes"),
        "secrets list JSON must not disclose secret lengths"
    );

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

    let skills_audit = parse_stdout_json(
        run_cli(&workdir, &["--output-format", "json", "skills", "audit"])?,
        "skills audit --output-format json",
    )?;
    assert_eq!(skills_audit.pointer("/summary/audited").and_then(Value::as_u64), Some(0));

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

    let journal_path = workdir.path().join("usage-cost.sqlite3");
    create_usage_cost_fixture(journal_path.as_path())?;
    let journal_path_string = journal_path.display().to_string();
    let usage_cost = parse_stdout_json(
        run_cli(
            &workdir,
            &[
                "gateway",
                "usage-cost",
                "--db-path",
                journal_path_string.as_str(),
                "--days",
                "7",
                "--json",
            ],
        )?,
        "gateway usage-cost --json",
    )?;
    assert_eq!(usage_cost.get("days").and_then(Value::as_u64), Some(7));
    assert_eq!(
        usage_cost.get("db_path").and_then(Value::as_str),
        Some(journal_path_string.as_str())
    );
    assert_eq!(usage_cost.pointer("/totals/runs").and_then(Value::as_i64), Some(0));

    Ok(())
}

fn create_usage_cost_fixture(path: &std::path::Path) -> Result<()> {
    let connection =
        Connection::open(path).with_context(|| format!("failed to create {}", path.display()))?;
    connection.execute_batch(
        r#"
        CREATE TABLE usage_pricing_catalog (
            model_id TEXT NOT NULL,
            input_cost_per_million_usd REAL,
            output_cost_per_million_usd REAL,
            effective_from_unix_ms INTEGER NOT NULL
        );
        CREATE TABLE usage_routing_decisions (
            run_ulid TEXT NOT NULL,
            mode TEXT NOT NULL,
            default_model_id TEXT NOT NULL,
            actual_model_id TEXT NOT NULL,
            created_at_unix_ms INTEGER NOT NULL
        );
        CREATE TABLE usage_alerts (
            resolved_at_unix_ms INTEGER,
            last_observed_at_unix_ms INTEGER NOT NULL
        );
        CREATE TABLE orchestrator_runs (
            run_ulid TEXT NOT NULL,
            started_at_unix_ms INTEGER NOT NULL,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}
