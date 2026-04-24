use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_VAULT_DIR", workdir.path().join("vault"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env("PALYRA_JOURNAL_DB_PATH", workdir.path().join("journal.sqlite3"))
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
}

fn run_cli(workdir: &TempDir, args: &[String]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn run_cli_with_stdin(workdir: &TempDir, args: &[String], stdin_payload: &[u8]) -> Result<Output> {
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

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir should have parent")
        .parent()
        .expect("workspace dir should have parent")
        .to_path_buf()
}

fn example_skill_paths() -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let base = repo_root().join("crates").join("palyra-skills").join("examples").join("echo-http");
    (
        base.join("skill.toml"),
        base.join("module.wasm"),
        base.join("assets").join("prompt.txt"),
        base.join("sbom.cdx.json"),
        base.join("provenance.json"),
    )
}

fn build_sample_artifact(workdir: &TempDir, artifact_path: &Path) -> Result<()> {
    let (manifest, module, asset, sbom, provenance) = example_skill_paths();
    let args = vec![
        "skills".to_owned(),
        "package".to_owned(),
        "build".to_owned(),
        "--manifest".to_owned(),
        manifest.to_string_lossy().into_owned(),
        "--module".to_owned(),
        module.to_string_lossy().into_owned(),
        "--asset".to_owned(),
        asset.to_string_lossy().into_owned(),
        "--sbom".to_owned(),
        sbom.to_string_lossy().into_owned(),
        "--provenance".to_owned(),
        provenance.to_string_lossy().into_owned(),
        "--output".to_owned(),
        artifact_path.to_string_lossy().into_owned(),
        "--signing-key-stdin".to_owned(),
    ];

    let output = run_cli_with_stdin(
        workdir,
        args.as_slice(),
        b"0101010101010101010101010101010101010101010101010101010101010101\n",
    )?;
    assert!(
        output.status.success(),
        "skills package build should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn seed_skill_secret(workdir: &TempDir, scope: &str, key: &str, value: &str) -> Result<()> {
    let args = vec![
        "secrets".to_owned(),
        "set".to_owned(),
        scope.to_owned(),
        key.to_owned(),
        "--value-stdin".to_owned(),
    ];
    let output = run_cli_with_stdin(workdir, args.as_slice(), format!("{value}\n").as_bytes())?;
    assert!(
        output.status.success(),
        "secrets set should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn seed_skill_status(
    workdir: &TempDir,
    skill_id: &str,
    version: &str,
    status: &str,
    reason: Option<&str>,
) -> Result<()> {
    let journal_path = workdir.path().join("journal.sqlite3");
    let connection = Connection::open(journal_path.as_path())
        .with_context(|| format!("failed to open journal db {}", journal_path.display()))?;
    connection.execute_batch(
        r#"
            CREATE TABLE IF NOT EXISTS skill_status (
                skill_id TEXT NOT NULL,
                version TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                detected_at_ms INTEGER NOT NULL,
                operator_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(skill_id, version)
            );
        "#,
    )?;
    connection.execute(
        r#"
            INSERT INTO skill_status (
                skill_id,
                version,
                status,
                reason,
                detected_at_ms,
                operator_principal,
                created_at_unix_ms,
                updated_at_unix_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(skill_id, version) DO UPDATE SET
                status = excluded.status,
                reason = excluded.reason,
                detected_at_ms = excluded.detected_at_ms,
                operator_principal = excluded.operator_principal,
                updated_at_unix_ms = excluded.updated_at_unix_ms
        "#,
        rusqlite::params![
            skill_id,
            version,
            status,
            reason,
            1_730_000_000_000_i64,
            "user:test",
            1_730_000_000_000_i64,
            1_730_000_000_000_i64
        ],
    )?;
    Ok(())
}

#[test]
fn skills_audit_empty_inventory_succeeds() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let skills_dir = workdir.path().join("skills-managed");
    let trust_store = workdir.path().join("trust-store.json");

    let audit_args = vec![
        "skills".to_owned(),
        "audit".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--trust-store".to_owned(),
        trust_store.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ];
    let audit_output = run_cli(&workdir, audit_args.as_slice())?;
    assert!(
        audit_output.status.success(),
        "empty skills audit should succeed: {}",
        String::from_utf8_lossy(&audit_output.stderr)
    );

    let payload: Value = serde_json::from_slice(audit_output.stdout.as_slice())
        .context("empty audit output should be JSON")?;
    let audits = payload
        .get("audits")
        .and_then(Value::as_array)
        .context("empty audit output must include audits array")?;
    assert!(audits.is_empty(), "empty skills directory should audit zero artifacts");
    assert_eq!(
        payload
            .get("summary")
            .and_then(Value::as_object)
            .and_then(|summary| summary.get("audited"))
            .and_then(Value::as_u64),
        Some(0)
    );
    assert_eq!(
        payload.get("message").and_then(Value::as_str),
        Some("no installed skill artifacts were selected for audit")
    );
    Ok(())
}

#[test]
fn skills_install_verify_remove_lifecycle_roundtrip() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let skills_dir = workdir.path().join("skills-managed");
    let artifact_path = workdir.path().join("dist").join("acme.echo_http.palyra-skill");

    build_sample_artifact(&workdir, artifact_path.as_path())?;
    seed_skill_secret(&workdir, "skill:acme.echo_http", "api_token", "test-token")?;

    let install_args = vec![
        "skills".to_owned(),
        "install".to_owned(),
        "--artifact".to_owned(),
        artifact_path.to_string_lossy().into_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--allow-untrusted".to_owned(),
        "--non-interactive".to_owned(),
        "--json".to_owned(),
    ];
    let install_output = run_cli(&workdir, install_args.as_slice())?;
    assert!(
        install_output.status.success(),
        "skills install should succeed: {}",
        String::from_utf8_lossy(&install_output.stderr)
    );

    let list_args = vec![
        "skills".to_owned(),
        "list".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ];
    let list_output = run_cli(&workdir, list_args.as_slice())?;
    assert!(
        list_output.status.success(),
        "skills list should succeed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let list_payload: Value = serde_json::from_slice(list_output.stdout.as_slice())
        .context("list output should be JSON")?;
    let entries = list_payload
        .get("entries")
        .and_then(Value::as_array)
        .context("list output must include entries array")?;
    assert_eq!(entries.len(), 1, "one skill version should be installed");
    assert_eq!(entries[0].get("skill_id").and_then(Value::as_str), Some("acme.echo_http"));
    assert_eq!(
        entries[0]
            .get("runtime_status")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unknown")
    );

    seed_skill_status(&workdir, "acme.echo_http", "1.0.0", "active", None)?;

    let eligible_list_args = vec![
        "skills".to_owned(),
        "list".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--eligible-only".to_owned(),
        "--json".to_owned(),
    ];
    let eligible_list_output = run_cli(&workdir, eligible_list_args.as_slice())?;
    assert!(
        eligible_list_output.status.success(),
        "eligible-only skills list should succeed: {}",
        String::from_utf8_lossy(&eligible_list_output.stderr)
    );
    let eligible_list_payload: Value =
        serde_json::from_slice(eligible_list_output.stdout.as_slice())
            .context("eligible-only list output should be JSON")?;
    let eligible_entries = eligible_list_payload
        .get("entries")
        .and_then(Value::as_array)
        .context("eligible-only list output must include entries array")?;
    assert_eq!(eligible_entries.len(), 1, "skill should be eligible after secret + active status");

    let info_args = vec![
        "skills".to_owned(),
        "info".to_owned(),
        "acme.echo_http".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ];
    let info_output = run_cli(&workdir, info_args.as_slice())?;
    assert!(
        info_output.status.success(),
        "skills info should succeed: {}",
        String::from_utf8_lossy(&info_output.stderr)
    );
    let info_payload: Value = serde_json::from_slice(info_output.stdout.as_slice())
        .context("info output should be JSON")?;
    assert_eq!(
        info_payload
            .get("inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("skill_id"))
            .and_then(Value::as_str),
        Some("acme.echo_http")
    );

    let check_args = vec![
        "skills".to_owned(),
        "check".to_owned(),
        "acme.echo_http".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--allow-untrusted".to_owned(),
        "--json".to_owned(),
    ];
    let check_output = run_cli(&workdir, check_args.as_slice())?;
    assert!(
        check_output.status.success(),
        "skills check should succeed: {}",
        String::from_utf8_lossy(&check_output.stderr)
    );
    let check_payload: Value = serde_json::from_slice(check_output.stdout.as_slice())
        .context("check output should be JSON")?;
    let check_results = check_payload
        .get("results")
        .and_then(Value::as_array)
        .context("check output must include results array")?;
    assert_eq!(check_results.len(), 1, "one skill should be checked");
    assert_eq!(check_results[0].get("check_status").and_then(Value::as_str), Some("ready"));

    let verify_args = vec![
        "skills".to_owned(),
        "verify".to_owned(),
        "acme.echo_http".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--allow-untrusted".to_owned(),
        "--json".to_owned(),
    ];
    let verify_output = run_cli(&workdir, verify_args.as_slice())?;
    assert!(
        verify_output.status.success(),
        "skills verify should succeed: {}",
        String::from_utf8_lossy(&verify_output.stderr)
    );

    let remove_args = vec![
        "skills".to_owned(),
        "remove".to_owned(),
        "acme.echo_http".to_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ];
    let remove_output = run_cli(&workdir, remove_args.as_slice())?;
    assert!(
        remove_output.status.success(),
        "skills remove should succeed: {}",
        String::from_utf8_lossy(&remove_output.stderr)
    );

    let list_after_output = run_cli(&workdir, list_args.as_slice())?;
    assert!(
        list_after_output.status.success(),
        "skills list after remove should succeed: {}",
        String::from_utf8_lossy(&list_after_output.stderr)
    );
    let list_after_payload: Value = serde_json::from_slice(list_after_output.stdout.as_slice())
        .context("list-after output should be JSON")?;
    let entries_after = list_after_payload
        .get("entries")
        .and_then(Value::as_array)
        .context("list-after output must include entries array")?;
    assert!(entries_after.is_empty(), "installed skill list should be empty after removal");

    Ok(())
}

#[test]
fn skills_install_rejects_tampered_artifact() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let skills_dir = workdir.path().join("skills-managed");
    let artifact_path = workdir.path().join("dist").join("acme.echo_http.palyra-skill");

    build_sample_artifact(&workdir, artifact_path.as_path())?;
    let mut artifact_bytes =
        fs::read(artifact_path.as_path()).context("failed to read built artifact for tamper")?;
    let tamper_offset =
        artifact_bytes.len().checked_div(2).context("artifact should not be empty")?;
    artifact_bytes[tamper_offset] ^= 0xFF;
    fs::write(artifact_path.as_path(), artifact_bytes.as_slice())
        .context("failed to persist tampered artifact")?;

    let install_args = vec![
        "skills".to_owned(),
        "install".to_owned(),
        "--artifact".to_owned(),
        artifact_path.to_string_lossy().into_owned(),
        "--skills-dir".to_owned(),
        skills_dir.to_string_lossy().into_owned(),
        "--allow-untrusted".to_owned(),
        "--non-interactive".to_owned(),
    ];
    let output = run_cli(&workdir, install_args.as_slice())?;
    assert!(!output.status.success(), "tampered artifact install must fail");
    let stderr = String::from_utf8(output.stderr).context("stderr should be UTF-8")?;
    assert!(
        stderr.contains("skill artifact failed structural verification"),
        "tampered artifact error should mention structural verification: {stderr}"
    );
    Ok(())
}
