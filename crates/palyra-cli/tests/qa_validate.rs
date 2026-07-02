//! Integration tests for the `palyra qa validate` command.

use std::{fs, path::PathBuf, process::Command};

use anyhow::{Context, Result};
use serde_json::Value;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory has a workspace parent")
        .parent()
        .expect("workspace crates directory has a repository parent")
        .to_path_buf()
}

#[test]
fn qa_validate_accepts_example_scenario_file() -> Result<()> {
    let scenario_path = repo_root().join("qa/scenarios/text_run_basic.yaml");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["qa", "validate", "--path"])
        .arg(scenario_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa validate")?;

    assert!(
        output.status.success(),
        "qa validate should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa validate JSON output should parse")?;
    assert_eq!(payload.pointer("/valid").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.pointer("/scenario_count").and_then(Value::as_u64), Some(1));
    assert_eq!(payload.pointer("/scenarios/0/id").and_then(Value::as_str), Some("text.run.basic"));
    Ok(())
}

#[test]
fn qa_validate_rejects_missing_id_with_jsonpath_error() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_path = temp_dir.path().join("missing-id.yaml");
    fs::write(
        scenario_path.as_path(),
        r#"
schema_version: 1
area: text
mode:
  provider: mock
requires:
  capabilities: [agent_run]
steps:
  - id: prompt
    action: user_prompt
    prompt: "Say hello."
expect:
  terminal_state: completed
  final_answer:
    contains: ["hello"]
forbidden:
  tool_calls: []
  events: []
  artifacts: []
artifacts: []
maturity:
  labels: [p0]
timeout:
  run_ms: 30000
"#,
    )
    .context("failed to write invalid scenario")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["qa", "validate", "--path"])
        .arg(scenario_path.as_os_str())
        .output()
        .context("failed to execute palyra qa validate")?;

    assert!(!output.status.success(), "invalid scenario should fail validation");
    let stderr = String::from_utf8_lossy(output.stderr.as_slice());
    assert!(
        stderr.contains("missing_scenario_id at $.id"),
        "validation error should include precise path: {stderr}"
    );
    Ok(())
}

#[test]
fn qa_run_pack_accepts_full_p0_pack() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "run-pack", "--path", "qa/scenarios", "--tag", "p0", "--json"])
        .output()
        .context("failed to execute palyra qa run-pack")?;

    assert!(
        output.status.success(),
        "qa run-pack p0 should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa run-pack JSON output should parse")?;
    assert_eq!(payload.pointer("/selected_count").and_then(Value::as_u64), Some(11));
    assert_eq!(payload.pointer("/passed").and_then(Value::as_u64), Some(11));
    assert_eq!(payload.pointer("/failed").and_then(Value::as_u64), Some(0));
    assert_eq!(payload.pointer("/skipped").and_then(Value::as_u64), Some(0));

    let scenarios = payload
        .pointer("/scenarios")
        .and_then(Value::as_array)
        .context("qa run-pack report should include scenarios")?;
    for scenario_id in [
        "compaction.retry_mutating_tool",
        "process.background_verification",
        "plugin_hook.blocks_tool",
    ] {
        let scenario = scenarios
            .iter()
            .find(|scenario| scenario.get("id").and_then(Value::as_str) == Some(scenario_id))
            .with_context(|| format!("missing sandbox scenario {scenario_id}"))?;
        assert_eq!(
            scenario.get("status").and_then(Value::as_str),
            Some("pass"),
            "{scenario_id} should pass in the full P0 pack"
        );
        assert_eq!(
            scenario.get("sandbox_fixture").and_then(Value::as_bool),
            Some(true),
            "{scenario_id} should declare a sandbox workspace fixture"
        );
        assert_eq!(
            scenario.get("sandbox_cleanup_verified").and_then(Value::as_bool),
            Some(true),
            "{scenario_id} should report sandbox cleanup verification"
        );
    }
    Ok(())
}

#[test]
fn qa_run_pack_filters_release_smoke_and_writes_aggregate_report() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let output_path = temp_dir.path().join("reports").join("qa-pack-report.json");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "run-pack", "--path", "qa/scenarios", "--tag", "release_smoke", "--output"])
        .arg(output_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa run-pack")?;

    assert!(
        output.status.success(),
        "qa run-pack release_smoke should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout_payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa run-pack JSON output should parse")?;
    let file_payload: Value = serde_json::from_slice(
        fs::read(output_path.as_path())
            .context("qa run-pack should write an aggregate report")?
            .as_slice(),
    )
    .context("written aggregate report should parse")?;
    let golden_path = repo_root().join("fixtures/golden/qa_p0_release_smoke_pack_report.json");
    let golden_payload: Value = serde_json::from_slice(
        fs::read(golden_path.as_path())
            .with_context(|| format!("failed to read {}", golden_path.display()))?
            .as_slice(),
    )
    .context("golden aggregate report should parse")?;

    assert_eq!(stdout_payload, file_payload, "stdout and report file should match");
    assert_eq!(stdout_payload, golden_payload, "release_smoke aggregate report drifted");
    Ok(())
}

#[test]
fn qa_provider_compat_reports_failure_classes_and_recovery_paths() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let output_path = temp_dir.path().join("provider-compat").join("report.json");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "provider-compat", "--path", "fixtures/provider_compat", "--output"])
        .arg(output_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa provider-compat")?;

    assert!(
        output.status.success(),
        "qa provider-compat should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout_payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa provider-compat JSON output should parse")?;
    let file_payload: Value = serde_json::from_slice(
        fs::read(output_path.as_path())
            .context("qa provider-compat should write an aggregate report")?
            .as_slice(),
    )
    .context("written provider compatibility report should parse")?;
    assert_eq!(stdout_payload, file_payload, "stdout and report file should match");
    assert_eq!(stdout_payload.pointer("/pack_count").and_then(Value::as_u64), Some(1));
    assert_eq!(stdout_payload.pointer("/fixture_count").and_then(Value::as_u64), Some(12));
    assert_eq!(stdout_payload.pointer("/category_count").and_then(Value::as_u64), Some(12));

    let fixtures = stdout_payload
        .pointer("/packs/0/fixtures")
        .and_then(Value::as_array)
        .context("provider compatibility report should include fixtures")?;
    let invalid_json = fixtures
        .iter()
        .find(|fixture| {
            fixture.get("category").and_then(Value::as_str) == Some("invalid_json_arguments")
        })
        .context("invalid JSON arguments fixture should be reported")?;
    assert_eq!(
        invalid_json.get("expected_failure_class").and_then(Value::as_str),
        Some("malformed_response")
    );
    assert_eq!(
        invalid_json.get("expected_recovery_decision").and_then(Value::as_str),
        Some("fail_closed")
    );
    assert!(
        invalid_json
            .get("recovery_path")
            .and_then(Value::as_str)
            .is_some_and(|path| path.contains("Reject invalid tool JSON")),
        "report should expose an actionable recovery path"
    );
    Ok(())
}

#[test]
fn qa_validate_accepts_provider_compatibility_scenario_pack() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "validate", "--path", "qa/scenarios/provider", "--json"])
        .output()
        .context("failed to execute palyra qa validate for provider scenarios")?;

    assert!(
        output.status.success(),
        "provider compatibility scenarios should validate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa validate provider scenario JSON output should parse")?;
    assert_eq!(payload.pointer("/valid").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.pointer("/scenario_count").and_then(Value::as_u64), Some(12));
    let scenarios = payload
        .pointer("/scenarios")
        .and_then(Value::as_array)
        .context("provider scenario report should include scenarios")?;
    assert!(scenarios.iter().all(|scenario| {
        scenario.get("maturity_labels").and_then(Value::as_array).is_some_and(|labels| {
            labels.iter().any(|label| label.as_str() == Some("provider_compat"))
        })
    }));
    Ok(())
}
