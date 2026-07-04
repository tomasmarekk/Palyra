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
        Some("bad_tool_arguments")
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
fn qa_gate_pr_smoke_writes_json_and_markdown_reports() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let json_path = temp_dir.path().join("qa-lab").join("pr-smoke.json");
    let markdown_path = temp_dir.path().join("qa-lab").join("pr-smoke.md");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/pr_smoke.yaml", "--output-json"])
        .arg(json_path.as_os_str())
        .arg("--output-markdown")
        .arg(markdown_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa gate")?;

    assert!(
        output.status.success(),
        "qa gate pr_smoke should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout_payload: Value =
        serde_json::from_slice(output.stdout.as_slice()).context("qa gate JSON should parse")?;
    let file_payload: Value = serde_json::from_slice(
        fs::read(json_path.as_path()).context("qa gate should write JSON report")?.as_slice(),
    )
    .context("written QA gate report should parse")?;
    let markdown = fs::read_to_string(markdown_path.as_path())
        .context("qa gate should write Markdown report")?;

    assert_eq!(stdout_payload, file_payload, "stdout and JSON report should match");
    assert_eq!(stdout_payload.pointer("/decision").and_then(Value::as_str), Some("pass"));
    assert_eq!(stdout_payload.pointer("/summary/failed").and_then(Value::as_u64), Some(0));
    assert!(markdown.contains("# QA Lab Gate: pr_smoke"));
    assert!(markdown.contains("## Maturity Scorecard"));
    Ok(())
}

#[test]
fn qa_gate_release_scorecard_matches_snapshot() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/release.yaml", "--json"])
        .output()
        .context("failed to execute palyra qa gate release")?;

    assert!(
        output.status.success(),
        "qa gate release should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa gate release JSON should parse")?;
    let scorecard = payload
        .get("maturity_scorecard")
        .context("QA gate report should include maturity_scorecard")?;
    let golden_path = repo_root().join("fixtures/golden/qa_release_maturity_scorecard.json");
    let golden: Value = serde_json::from_slice(
        fs::read(golden_path.as_path())
            .with_context(|| format!("failed to read {}", golden_path.display()))?
            .as_slice(),
    )
    .context("golden QA maturity scorecard should parse")?;

    assert_eq!(scorecard, &golden, "release maturity scorecard drifted");
    Ok(())
}

#[test]
fn qa_gate_fails_unavailable_capability_without_skip_reason() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let suite_path = temp_dir.path().join("missing-skip-reason.yaml");
    fs::write(
        suite_path.as_path(),
        format!(
            r#"
schema_version: 1
id: missing_skip_reason
mode: pr
scenario_roots:
  - "{}"
include_tags: [release_smoke]
allow_provider_modes: [mock]
available_capabilities:
  - qa_lab
flaky_policy:
  max_retries: 0
  fail_on_flaky: true
  require_issue: true
scorecard:
  categories:
    - id: security
      label: Security
      labels: [security]
"#,
            repo_root().join("qa/scenarios").display().to_string().replace('\\', "/")
        ),
    )
    .context("failed to write suite fixture")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["qa", "gate", "--suite"])
        .arg(suite_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa gate missing skip reason")?;

    assert!(!output.status.success(), "missing skip reason should fail the gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("failing QA gate JSON should parse")?;
    assert_eq!(payload.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert!(payload.pointer("/policy_violations").and_then(Value::as_array).is_some_and(
        |violations| violations.iter().any(|violation| {
            violation.get("code").and_then(Value::as_str) == Some("missing_skip_reason")
        })
    ));
    Ok(())
}

#[test]
fn qa_gate_release_blocks_failing_p0_scenario() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_dir = temp_dir.path().join("scenarios");
    fs::create_dir_all(scenario_dir.as_path()).context("failed to create scenario dir")?;
    fs::write(
        scenario_dir.join("failing-p0.yaml"),
        r#"
schema_version: 1
id: p0.fixture_missing
area: tools
mode:
  provider: mock
  deterministic: true
requires:
  capabilities: [agent_run, qa_lab]
  tools: []
  fixtures:
    - missing/fixture.yaml
steps:
  - id: prompt
    action: user_prompt
    prompt: "Exercise missing fixture handling."
expect:
  terminal_state: completed
  final_answer:
    contains: ["done"]
  events:
    - event_type: run.completed
      min_count: 1
  tool_calls: []
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [p0]
timeout:
  run_ms: 30000
"#,
    )
    .context("failed to write failing P0 scenario")?;
    let suite_path = temp_dir.path().join("release.yaml");
    fs::write(
        suite_path.as_path(),
        format!(
            r#"
schema_version: 1
id: release_regression
mode: release
scenario_roots:
  - "{}"
include_tags: [p0]
allow_provider_modes: [mock]
require_p0_green: true
available_capabilities:
  - agent_run
  - qa_lab
flaky_policy:
  max_retries: 0
  fail_on_flaky: true
  require_issue: true
scorecard:
  categories:
    - id: execution_backends
      label: Execution backends
      areas: [tools]
"#,
            scenario_dir.display().to_string().replace('\\', "/")
        ),
    )
    .context("failed to write release regression suite")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["qa", "gate", "--suite"])
        .arg(suite_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa gate release regression")?;

    assert!(!output.status.success(), "failing P0 scenario should fail release gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("failing release gate JSON should parse")?;
    assert_eq!(payload.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert_eq!(payload.pointer("/summary/failed").and_then(Value::as_u64), Some(1));
    assert!(payload.pointer("/policy_violations").and_then(Value::as_array).is_some_and(
        |violations| violations.iter().any(|violation| {
            violation.get("code").and_then(Value::as_str) == Some("release_p0_not_green")
        })
    ));
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
