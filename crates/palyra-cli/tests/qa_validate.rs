//! Integration tests for the `palyra qa` command family.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

static REAL_RUNTIME_GATE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn real_runtime_gate_test_guard() -> MutexGuard<'static, ()> {
    // These tests validate daemon behavior, not how many debug daemons a shared CI runner can boot
    // concurrently. Recover poisoning so one assertion still lets the remaining tests diagnose.
    REAL_RUNTIME_GATE_TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory has a workspace parent")
        .parent()
        .expect("workspace crates directory has a repository parent")
        .to_path_buf()
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn run_live_gate(suite: &Path, output_path: &Path, allow_live: bool) -> Result<(Output, Value)> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite"])
        .arg(suite)
        .arg("--output-json")
        .arg(output_path)
        .arg("--json")
        .env_remove("PALYRA_QA_LIVE_AUTH_PROFILE_ID");
    if allow_live {
        command.arg("--allow-live");
    }
    let output = command.output().context("failed to execute live QA gate")?;
    let report = serde_json::from_slice(output.stdout.as_slice())
        .context("live QA gate JSON output should parse")?;
    Ok((output, report))
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
fn qa_run_pack_accepts_provider_p0_schema_preview() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "run-pack", "--path", "qa/scenarios/provider", "--tag", "p0", "--json"])
        .output()
        .context("failed to execute provider P0 schema preview")?;

    assert!(
        output.status.success(),
        "provider P0 schema preview should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("provider P0 schema preview JSON should parse")?;
    assert_eq!(payload.pointer("/selected_count").and_then(Value::as_u64), Some(12));
    assert_eq!(payload.pointer("/passed").and_then(Value::as_u64), Some(12));
    assert_eq!(payload.pointer("/failed").and_then(Value::as_u64), Some(0));
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
    assert_eq!(stdout_payload.pointer("/fixture_count").and_then(Value::as_u64), Some(15));
    assert_eq!(stdout_payload.pointer("/category_count").and_then(Value::as_u64), Some(15));

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
fn qa_gate_pr_smoke_writes_v3_reports_and_resumes_selectively() -> Result<()> {
    let _runtime_guard = real_runtime_gate_test_guard();
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let json_path = temp_dir.path().join("qa-lab").join("pr-smoke.json");
    let markdown_path = temp_dir.path().join("qa-lab").join("pr-smoke.md");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/pr_smoke.yaml", "--output-json"])
        .arg(json_path.as_os_str())
        .arg("--output-markdown")
        .arg(markdown_path.as_os_str())
        .arg("--resume")
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
    assert_eq!(stdout_payload.pointer("/schema_version").and_then(Value::as_u64), Some(3));
    assert_eq!(stdout_payload.pointer("/decision").and_then(Value::as_str), Some("pass"));
    assert_eq!(stdout_payload.pointer("/resume_requested").and_then(Value::as_bool), Some(true));
    assert_eq!(stdout_payload.pointer("/force_rerun").and_then(Value::as_bool), Some(false));
    assert_eq!(
        stdout_payload.pointer("/artifact_reference_base").and_then(Value::as_str),
        Some(".")
    );
    assert_eq!(stdout_payload.pointer("/summary/failed").and_then(Value::as_u64), Some(0));
    let scenarios = stdout_payload
        .get("scenarios")
        .and_then(Value::as_array)
        .context("real QA gate should include scenario executions")?;
    assert_eq!(scenarios.len(), 18);
    let executed_scenarios =
        scenarios.iter().filter(|scenario| scenario.get("execution").is_some()).collect::<Vec<_>>();
    assert_eq!(executed_scenarios.len(), 6);
    let replay_scenario = scenarios
        .iter()
        .find(|scenario| scenario["id"].as_str() == Some("real_runtime.record_replay_text"))
        .context("PR smoke report should include the filtered record-replay scenario")?;
    assert_eq!(replay_scenario["status"].as_str(), Some("skipped"));
    assert!(replay_scenario.get("execution").is_none());
    assert!(stdout_payload
        .pointer("/campaign_checkpoint/campaign_key")
        .and_then(Value::as_str)
        .is_some_and(is_sha256));
    assert!(stdout_payload
        .pointer("/campaign_checkpoint/checkpoint_artifact/sha256")
        .and_then(Value::as_str)
        .is_some_and(is_sha256));
    assert!(executed_scenarios.iter().all(|scenario| {
        let execution = &scenario["execution"];
        execution["run_id"].as_str().is_some_and(|value| value.len() == 26)
            && execution["session_id"].as_str().is_some_and(|value| value.len() == 26)
            && execution["cleanup"]["verified"].as_bool() == Some(true)
            && execution["result_artifact"]["sha256"].as_str().is_some_and(is_sha256)
            && execution["evidence_artifacts"][0]["sha256"].as_str().is_some_and(is_sha256)
    }));
    let serialized = serde_json::to_string(&stdout_payload)
        .context("QA gate report should serialize for bounded-shape assertions")?;
    assert!(!serialized.contains("\"transcript\""));
    assert!(!serialized.contains("\"tape_events\""));
    let normalized_temp_root = temp_dir.path().to_string_lossy().replace('\\', "/");
    assert!(!serialized.replace('\\', "/").contains(normalized_temp_root.as_str()));
    assert!(!markdown.replace('\\', "/").contains(normalized_temp_root.as_str()));
    assert!(markdown.contains("# QA Lab Gate: pr_smoke"));
    assert!(markdown.contains("- Artifact reference base: `.`"));
    assert!(markdown.contains("## Maturity Scorecard"));

    let first_attempts = executed_scenarios
        .iter()
        .map(|scenario| {
            let id = scenario["id"].as_str().context("scenario report should contain an id")?;
            let execution = &scenario["execution"];
            let execution_id = execution["execution_id"]
                .as_str()
                .context("scenario execution should contain an execution id")?;
            let result_path = execution["result_artifact"]["path"]
                .as_str()
                .context("scenario execution should reference its result artifact")?;
            Ok((id.to_owned(), execution_id.to_owned(), result_path.to_owned()))
        })
        .collect::<Result<Vec<_>>>()?;
    let first_references = executed_scenarios
        .iter()
        .flat_map(|scenario| {
            let execution = &scenario["execution"];
            std::iter::once(&execution["result_artifact"])
                .chain(execution["evidence_artifacts"].as_array().into_iter().flatten())
        })
        .map(|reference| {
            Ok((
                reference["path"]
                    .as_str()
                    .context("artifact reference should contain a relative path")?
                    .to_owned(),
                reference["sha256"]
                    .as_str()
                    .context("artifact reference should contain a digest")?
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let second_json_path = temp_dir.path().join("qa-lab").join("pr-smoke-second.json");
    let second_output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/pr_smoke.yaml", "--output-json"])
        .arg(second_json_path.as_os_str())
        .arg("--resume")
        .arg("--json")
        .output()
        .context("failed to execute a second palyra qa gate")?;
    assert!(
        second_output.status.success(),
        "second QA gate should pass: {}",
        String::from_utf8_lossy(&second_output.stderr)
    );
    let second_payload: Value = serde_json::from_slice(second_output.stdout.as_slice())
        .context("second QA gate JSON should parse")?;
    let second_scenarios = second_payload
        .get("scenarios")
        .and_then(Value::as_array)
        .context("second QA gate should include scenario executions")?;
    assert_eq!(second_scenarios.len(), scenarios.len());
    for (scenario_id, execution_id, result_path) in &first_attempts {
        let scenario = second_scenarios
            .iter()
            .find(|scenario| scenario["id"].as_str() == Some(scenario_id.as_str()))
            .with_context(|| format!("second QA gate should include {scenario_id}"))?;
        assert_eq!(scenario["reused"].as_bool(), Some(true));
        assert_eq!(scenario["resume_reason_code"].as_str(), Some("qa.resume.passed_reused"));
        assert_eq!(scenario["execution"]["execution_id"].as_str(), Some(execution_id.as_str()));
        assert_eq!(
            scenario["execution"]["result_artifact"]["path"].as_str(),
            Some(result_path.as_str())
        );
    }

    let artifact_root = json_path.parent().context("QA report should have a parent directory")?;
    for (relative_path, expected_hash) in first_references {
        assert!(!PathBuf::from(relative_path.as_str()).is_absolute());
        let bytes = fs::read(artifact_root.join(relative_path.as_str()))
            .with_context(|| format!("failed to reopen referenced artifact {relative_path}"))?;
        assert_eq!(sha256_hex(bytes.as_slice()), expected_hash);
    }

    let (corrupted_scenario_id, corrupted_execution_id, corrupted_result_path) = first_attempts
        .first()
        .cloned()
        .context("PR smoke gate should execute at least one scenario")?;
    assert!(!PathBuf::from(corrupted_result_path.as_str()).is_absolute());
    fs::write(artifact_root.join(corrupted_result_path.as_str()), b"corrupted QA result")
        .context("failed to corrupt one QA result artifact")?;

    let third_json_path = temp_dir.path().join("qa-lab").join("pr-smoke-third.json");
    let third_output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/pr_smoke.yaml", "--output-json"])
        .arg(third_json_path.as_os_str())
        .arg("--resume")
        .arg("--json")
        .output()
        .context("failed to execute a third palyra qa gate")?;
    assert!(
        third_output.status.success(),
        "third QA gate should pass after rerunning one corrupt result: {}",
        String::from_utf8_lossy(&third_output.stderr)
    );
    let third_payload: Value = serde_json::from_slice(third_output.stdout.as_slice())
        .context("third QA gate JSON should parse")?;
    let third_scenarios = third_payload
        .get("scenarios")
        .and_then(Value::as_array)
        .context("third QA gate should include scenario executions")?;
    let mut rerun_count = 0;
    let mut reused_count = 0;
    for (scenario_id, execution_id, result_path) in &first_attempts {
        let scenario = third_scenarios
            .iter()
            .find(|scenario| scenario["id"].as_str() == Some(scenario_id.as_str()))
            .with_context(|| format!("third QA gate should include {scenario_id}"))?;
        if scenario_id == &corrupted_scenario_id {
            rerun_count += 1;
            assert_eq!(scenario.get("reused").and_then(Value::as_bool), None);
            assert_eq!(scenario["resume_reason_code"].as_str(), Some("qa.resume.result_untrusted"));
            assert_ne!(
                scenario["execution"]["execution_id"].as_str(),
                Some(corrupted_execution_id.as_str())
            );
            assert_eq!(scenario["execution"]["attempt"]["generation"].as_u64(), Some(2));
            assert_eq!(
                scenario["execution"]["attempt"]["previous_result_artifact"]["path"].as_str(),
                Some(corrupted_result_path.as_str())
            );
        } else {
            reused_count += 1;
            assert_eq!(scenario["reused"].as_bool(), Some(true));
            assert_eq!(scenario["resume_reason_code"].as_str(), Some("qa.resume.passed_reused"));
            assert_eq!(scenario["execution"]["execution_id"].as_str(), Some(execution_id.as_str()));
            assert_eq!(
                scenario["execution"]["result_artifact"]["path"].as_str(),
                Some(result_path.as_str())
            );
        }
    }
    assert_eq!(rerun_count, 1);
    assert_eq!(reused_count, first_attempts.len() - 1);

    let checkpoint_path = third_payload
        .pointer("/campaign_checkpoint/checkpoint_artifact/path")
        .and_then(Value::as_str)
        .context("third QA gate should reference its campaign checkpoint")?;
    assert!(!PathBuf::from(checkpoint_path).is_absolute());
    let checkpoint: Value = serde_json::from_slice(
        fs::read(artifact_root.join(checkpoint_path))
            .context("failed to read the latest campaign checkpoint")?
            .as_slice(),
    )
    .context("campaign checkpoint JSON should parse")?;
    assert_eq!(
        checkpoint.pointer("/generation").and_then(Value::as_u64),
        third_payload.pointer("/campaign_checkpoint/checkpoint_generation").and_then(Value::as_u64)
    );
    let entries = checkpoint
        .pointer("/entries")
        .and_then(Value::as_object)
        .context("campaign checkpoint should retain scenario entries")?;
    let corrupted_entry = entries
        .values()
        .find(|entry| entry["scenario_id"].as_str() == Some(corrupted_scenario_id.as_str()))
        .context("campaign checkpoint should retain the rerun scenario")?;
    let corrupted_history = corrupted_entry["attempts"]
        .as_array()
        .context("rerun checkpoint entry should retain attempt history")?;
    assert_eq!(corrupted_history.len(), 2);
    assert_eq!(
        corrupted_history[0]["result_artifact"]["path"].as_str(),
        Some(corrupted_result_path.as_str())
    );
    assert_eq!(
        corrupted_history[1]["previous_result_artifact"]["path"].as_str(),
        Some(corrupted_result_path.as_str())
    );
    assert_eq!(
        entries
            .values()
            .filter(|entry| entry["scenario_id"].as_str() != Some(corrupted_scenario_id.as_str()))
            .filter(|entry| entry["attempts"]
                .as_array()
                .is_some_and(|attempts| attempts.len() == 1))
            .count(),
        first_attempts.len() - 1
    );
    Ok(())
}

#[test]
fn qa_gate_live_lane_requires_suite_cli_and_secret_profile_authorization() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create live gate temp dir")?;
    let live_suite = repo_root().join("qa/suites/live_smoke.yaml");

    let (cli_denied, cli_denied_report) =
        run_live_gate(live_suite.as_path(), &temp_dir.path().join("cli-denied.json"), false)?;
    assert!(!cli_denied.status.success());
    assert_eq!(cli_denied_report.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert_eq!(
        cli_denied_report.pointer("/scenarios/0/status").and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        cli_denied_report.pointer("/scenarios/0/issue_codes/0").and_then(Value::as_str),
        Some("qa.runner.live_not_enabled")
    );

    let suite_denied_path = temp_dir.path().join("suite-denied.yaml");
    let suite_denied = fs::read_to_string(live_suite.as_path())
        .context("failed to read live smoke suite")?
        .replace("id: live_smoke", "id: live_smoke_suite_denied")
        .replace("allow_live_providers: true", "allow_live_providers: false");
    fs::write(suite_denied_path.as_path(), suite_denied)
        .context("failed to write suite-denied live fixture")?;
    let (suite_denied, suite_denied_report) = run_live_gate(
        suite_denied_path.as_path(),
        &temp_dir.path().join("suite-denied.json"),
        true,
    )?;
    assert!(!suite_denied.status.success());
    assert_eq!(
        suite_denied_report.pointer("/scenarios/0/issue_codes/0").and_then(Value::as_str),
        Some("qa.runner.live_not_enabled")
    );

    let (profile_denied, profile_denied_report) =
        run_live_gate(live_suite.as_path(), &temp_dir.path().join("profile-denied.json"), true)?;
    assert!(!profile_denied.status.success());
    assert_eq!(
        profile_denied_report.pointer("/scenarios/0/status").and_then(Value::as_str),
        Some("fail")
    );
    assert_eq!(
        profile_denied_report.pointer("/scenarios/0/issue_codes/0").and_then(Value::as_str),
        Some("qa.runner.live_profile_unavailable")
    );
    assert!(profile_denied_report.pointer("/scenarios/0/execution").is_none());
    Ok(())
}

#[test]
fn qa_gate_rejects_reports_with_different_artifact_reference_directories() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/pr_smoke.yaml", "--output-json"])
        .arg(temp_dir.path().join("json").join("gate.json"))
        .arg("--output-markdown")
        .arg(temp_dir.path().join("markdown").join("gate.md"))
        .arg("--json")
        .output()
        .context("failed to execute gate with split report directories")?;

    assert!(!output.status.success(), "split report directories must fail closed");
    assert!(String::from_utf8_lossy(output.stderr.as_slice())
        .contains("qa.runner.output_directory_mismatch"));
    Ok(())
}

#[test]
fn qa_gate_fails_a_real_runtime_answer_mismatch() -> Result<()> {
    let _runtime_guard = real_runtime_gate_test_guard();
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_dir = temp_dir.path().join("scenarios");
    fs::create_dir_all(scenario_dir.as_path()).context("failed to create scenario dir")?;
    fs::write(
        scenario_dir.join("answer-mismatch.yaml"),
        r#"
schema_version: 2
id: real_runtime.answer_mismatch
area: text
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/real_agent_runner.yaml
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_no_tools
requires:
  model: text
  capabilities: [agent_run, qa_lab]
  tools: []
  fixtures:
    - qa/fixtures/real_agent_runner.yaml
    - qa/fixtures/sandbox_workspaces/repo_basic
steps:
  - id: prompt
    action: user_prompt
    prompt: "Return the exact deterministic QA response."
expect:
  terminal_state: completed
  final_answer:
    equals: "This intentionally differs from the fixture output."
  events:
    - event_type: run.completed
      min_count: 1
  tool_calls: []
forbidden:
  tool_calls: ["*"]
  events: [run.failed]
  artifacts: []
  claims: []
artifacts:
  - path: qa/reports/real_runtime/answer_mismatch.evidence.json
    kind: evidence
    required: true
maturity:
  labels: [p0, negative_real_runtime]
timeout:
  run_ms: 30000
  step_ms: 10000
"#,
    )
    .context("failed to write mismatch scenario")?;
    let suite_path = temp_dir.path().join("mismatch-suite.yaml");
    fs::write(
        suite_path.as_path(),
        format!(
            r#"
schema_version: 1
id: answer_mismatch
mode: pr
scenario_roots:
  - "{}"
include_tags: [negative_real_runtime]
allow_runner_modes: [fixture]
available_capabilities: [agent_run, qa_lab]
flaky_policy:
  max_retries: 0
  fail_on_flaky: true
  require_issue: true
scorecard:
  fail_on_required_blockers: true
  categories:
    - id: text
      label: Text
      areas: [text]
      required: true
"#,
            scenario_dir.display().to_string().replace('\\', "/")
        ),
    )
    .context("failed to write mismatch suite")?;
    let report_path = temp_dir.path().join("reports").join("mismatch.json");

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite"])
        .arg(suite_path.as_os_str())
        .arg("--output-json")
        .arg(report_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute mismatching real QA gate")?;

    assert!(!output.status.success(), "an actual answer mismatch must fail the gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("mismatch gate JSON should parse")?;
    assert_eq!(payload.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert!(payload.pointer("/scenarios/0/issue_codes").and_then(Value::as_array).is_some_and(
        |codes| { codes.iter().any(|code| code.as_str() == Some("final_answer_mismatch")) }
    ));
    assert_eq!(
        payload.pointer("/scenarios/0/execution/cleanup/verified").and_then(Value::as_bool),
        Some(true)
    );
    assert!(payload
        .pointer("/scenarios/0/execution/result_artifact/sha256")
        .and_then(Value::as_str)
        .is_some_and(is_sha256));
    let serialized = serde_json::to_string(&payload)
        .context("mismatch gate report should serialize for bounded-shape assertions")?;
    assert!(!serialized.contains("\"transcript\""));
    assert!(!serialized.contains("\"tape_events\""));
    let normalized_temp_root = temp_dir.path().to_string_lossy().replace('\\', "/");
    assert!(!serialized.replace('\\', "/").contains(normalized_temp_root.as_str()));
    assert_eq!(
        payload.pointer("/suite_path").and_then(Value::as_str),
        Some("<normalized:absolute_path>")
    );
    assert_eq!(
        payload.pointer("/scenario_roots/0").and_then(Value::as_str),
        Some("<normalized:absolute_path>")
    );
    assert_eq!(
        payload.pointer("/scenarios/0/path").and_then(Value::as_str),
        Some("<normalized:absolute_path>")
    );
    Ok(())
}

#[test]
fn qa_gate_rejects_recovery_without_failed_attempt_evidence() -> Result<()> {
    let _runtime_guard = real_runtime_gate_test_guard();
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_dir = temp_dir.path().join("scenarios");
    fs::create_dir_all(scenario_dir.as_path()).context("failed to create scenario dir")?;
    fs::write(
        scenario_dir.join("missing-retry.yaml"),
        r#"
schema_version: 2
id: real_runtime.missing_retry_evidence
area: provider
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_retry_missing_attempt.yaml
  policy_profile: qa_provider_recovery
requires:
  model: text
  capabilities: [agent_run, qa_lab, provider_recovery]
  tools: []
  fixtures: [qa/fixtures/provider_retry_missing_attempt.yaml]
steps:
  - id: prompt
    action: user_prompt
    prompt: "Recover a malformed response without retry evidence."
expect:
  terminal_state: completed
  final_answer:
    equals: "Recovered after a retryable malformed response."
  events:
    - event_type: provider.retry.started
      min_count: 1
    - event_type: run.completed
      min_count: 1
  tool_calls: []
forbidden:
  tool_calls: ["*"]
  events: [run.failed]
  artifacts: []
  claims: []
artifacts:
  - path: qa/reports/real_runtime/missing_retry_evidence.json
    kind: evidence
    required: true
maturity:
  labels: [p0, negative_retry_evidence]
timeout:
  run_ms: 60000
  step_ms: 20000
"#,
    )
    .context("failed to write missing-retry scenario")?;
    let suite_path = temp_dir.path().join("missing-retry-suite.yaml");
    fs::write(
        suite_path.as_path(),
        format!(
            r#"
schema_version: 1
id: missing_retry_evidence
mode: pr
scenario_roots:
  - {}
include_tags: [negative_retry_evidence]
allow_runner_modes: [fixture]
require_p0_green: true
available_capabilities: [agent_run, qa_lab, provider_recovery]
flaky_policy:
  max_retries: 0
  fail_on_flaky: true
  require_issue: true
scorecard:
  fail_on_required_blockers: true
  categories:
    - id: provider
      label: Provider
      areas: [provider]
      required: true
"#,
            scenario_dir.display().to_string().replace('\\', "/")
        ),
    )
    .context("failed to write missing-retry suite")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite"])
        .arg(suite_path.as_os_str())
        .arg("--output-json")
        .arg(temp_dir.path().join("reports/missing-retry.json"))
        .arg("--json")
        .output()
        .context("failed to execute missing-retry gate")?;

    assert!(!output.status.success(), "missing retry evidence must fail the gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("missing-retry gate JSON should parse")?;
    assert!(
        payload
            .pointer("/scenarios/0/issue_codes")
            .and_then(Value::as_array)
            .is_some_and(|codes| codes.iter().any(|code| code.as_str() == Some("missing_event"))),
        "missing-retry gate lost the expected evidence verdict: {payload:#}"
    );
    assert_eq!(
        payload.pointer("/scenarios/0/execution/cleanup/verified").and_then(Value::as_bool),
        Some(true)
    );
    Ok(())
}

#[test]
fn qa_gate_persists_verified_cleanup_after_runtime_timeout() -> Result<()> {
    let _runtime_guard = real_runtime_gate_test_guard();
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_dir = temp_dir.path().join("scenarios");
    fs::create_dir_all(scenario_dir.as_path()).context("failed to create scenario dir")?;
    fs::write(
        scenario_dir.join("runtime-timeout.yaml"),
        r#"
schema_version: 2
id: real_runtime.timeout_cleanup
area: provider
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_slow_timeout.yaml
  policy_profile: qa_provider_recovery
requires:
  model: text
  capabilities: [agent_run, qa_lab]
  tools: []
  fixtures: [qa/fixtures/provider_slow_timeout.yaml]
steps:
  - id: prompt
    action: user_prompt
    prompt: "Exceed the QA runner deadline."
expect:
  terminal_state: completed
  events:
    - event_type: run.completed
  tool_calls: []
forbidden:
  tool_calls: ["*"]
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [p0, negative_error_cleanup]
timeout:
  run_ms: 50
  step_ms: 50
"#,
    )
    .context("failed to write runtime-timeout scenario")?;
    let suite_path = temp_dir.path().join("runtime-timeout-suite.yaml");
    fs::write(
        suite_path.as_path(),
        format!(
            r#"
schema_version: 1
id: runtime_timeout_cleanup
mode: pr
scenario_roots:
  - {}
include_tags: [negative_error_cleanup]
allow_runner_modes: [fixture]
require_p0_green: true
available_capabilities: [agent_run, qa_lab]
flaky_policy:
  max_retries: 0
  fail_on_flaky: true
  require_issue: true
scorecard:
  fail_on_required_blockers: true
  categories:
    - id: provider
      label: Provider
      areas: [provider]
      required: true
"#,
            scenario_dir.display().to_string().replace('\\', "/")
        ),
    )
    .context("failed to write runtime-timeout suite")?;
    let report_path = temp_dir.path().join("reports/runtime-timeout.json");

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite"])
        .arg(suite_path.as_os_str())
        .arg("--output-json")
        .arg(report_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute runtime-timeout gate")?;

    assert!(!output.status.success(), "runtime timeout must fail the gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("runtime-timeout gate JSON should parse")?;
    let execution = payload
        .pointer("/scenarios/0/execution")
        .context("runtime error should retain an execution descriptor")?;
    assert!(
        payload.pointer("/scenarios/0/issue_codes").and_then(Value::as_array).is_some_and(
            |codes| codes.iter().any(|code| code.as_str() == Some("qa.runner.run_timeout"))
        ),
        "runtime timeout report lost the stable timeout reason: {payload:#}"
    );
    assert_eq!(
        payload.pointer("/scenarios/0/reason").and_then(Value::as_str),
        Some("qa.runner.run_timeout")
    );
    assert!(!payload.pointer("/scenarios/0/issue_codes").and_then(Value::as_array).is_some_and(
        |codes| codes.iter().any(|code| code.as_str() == Some("qa.runner.stream_decode_failed"))
    ));
    assert_eq!(execution.pointer("/cleanup/session_cleaned").and_then(Value::as_bool), Some(true));
    assert_eq!(
        execution.pointer("/cleanup/daemon_terminated").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        execution.pointer("/cleanup/workspace_removed").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(execution.pointer("/cleanup/verified").and_then(Value::as_bool), Some(true));
    assert_eq!(
        execution.pointer("/cleanup/run_terminal_observed").and_then(Value::as_bool),
        Some(false)
    );
    let failure_artifacts = execution
        .pointer("/evidence_artifacts")
        .and_then(Value::as_array)
        .context("runtime timeout should retain bounded failure diagnostics")?;
    assert_eq!(failure_artifacts.len(), 1);
    assert_eq!(failure_artifacts[0]["kind"], "failure_diagnostics");
    assert!(failure_artifacts[0]["path"]
        .as_str()
        .is_some_and(|path| !PathBuf::from(path).is_absolute()));
    let result_path = execution
        .pointer("/result_artifact/path")
        .and_then(Value::as_str)
        .context("runtime error should reference its result descriptor")?;
    assert!(!PathBuf::from(result_path).is_absolute());
    assert!(report_path
        .parent()
        .context("runtime-timeout report should have a parent")?
        .join(result_path)
        .is_file());
    Ok(())
}

#[test]
fn qa_gate_release_scorecard_matches_snapshot() -> Result<()> {
    let _runtime_guard = real_runtime_gate_test_guard();
    let temp_dir = tempfile::tempdir().context("failed to create release gate output root")?;
    let report_path = temp_dir.path().join("qa-lab").join("release.json");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/release.yaml", "--output-json"])
        .arg(report_path.as_os_str())
        .arg("--json")
        .output()
        .context("failed to execute palyra qa gate release")?;

    assert!(
        output.status.success(),
        "qa gate release should pass: {}\nreport: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("qa gate release JSON should parse")?;
    let file_payload: Value = serde_json::from_slice(
        fs::read(report_path.as_path())
            .context("qa gate release should write an isolated JSON report")?
            .as_slice(),
    )
    .context("written release gate JSON should parse")?;
    assert_eq!(payload, file_payload, "release stdout and report file should match");
    assert_eq!(payload.pointer("/schema_version").and_then(Value::as_u64), Some(3));
    assert_eq!(payload.pointer("/summary/selected_count").and_then(Value::as_u64), Some(6));
    assert_eq!(payload.pointer("/summary/passed").and_then(Value::as_u64), Some(6));
    let scorecard = payload
        .get("maturity_scorecard")
        .context("QA gate report should include maturity_scorecard")?;
    let golden_path =
        repo_root().join("fixtures/golden/qa_release_runtime_maturity_scorecard_v1.json");
    let golden: Value = serde_json::from_slice(
        fs::read(golden_path.as_path())
            .with_context(|| format!("failed to read {}", golden_path.display()))?
            .as_slice(),
    )
    .context("golden QA maturity scorecard should parse")?;

    assert_eq!(scorecard, &golden, "release maturity scorecard drifted");
    let replay = scorecard["categories"]
        .as_array()
        .and_then(|categories| {
            categories.iter().find(|category| category["id"].as_str() == Some("replay"))
        })
        .context("release scorecard should include the record-replay category")?;
    assert_eq!(replay["total"].as_u64(), Some(1));
    assert_eq!(replay["passed"].as_u64(), Some(1));
    assert_eq!(replay["score_bps"].as_u64(), Some(10_000));
    Ok(())
}

#[test]
fn qa_gate_rejects_legacy_suite_as_schema_preview_instead_of_simulating_runtime() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/nightly_schema_preview.yaml", "--json"])
        .output()
        .context("failed to execute legacy nightly schema-preview gate")?;

    assert!(!output.status.success(), "schema preview must not qualify as a runtime gate");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("legacy gate JSON should parse")?;
    assert_eq!(payload.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert!(payload
        .pointer("/summary/unsupported")
        .and_then(Value::as_u64)
        .is_some_and(|count| count > 0));
    let encoded = serde_json::to_string(&payload).context("legacy gate report should serialize")?;
    assert!(encoded.contains("qa.runner.schema_preview_only"));
    assert!(!encoded.contains("qa.runner.missing_config"));
    Ok(())
}

#[test]
fn qa_gate_fails_when_tag_filters_select_no_scenarios() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(repo_root())
        .args(["qa", "gate", "--suite", "qa/suites/backend_harness_conformance.yaml", "--json"])
        .output()
        .context("failed to execute empty-selection QA gate")?;

    assert!(!output.status.success(), "an empty runtime gate must fail closed");
    let payload: Value = serde_json::from_slice(output.stdout.as_slice())
        .context("empty-selection gate JSON should parse")?;
    assert_eq!(payload.pointer("/decision").and_then(Value::as_str), Some("fail"));
    assert_eq!(payload.pointer("/summary/selected_count").and_then(Value::as_u64), Some(0));
    assert!(payload.pointer("/policy_violations").and_then(Value::as_array).is_some_and(
        |violations| violations.iter().any(|violation| {
            violation.get("code").and_then(Value::as_str) == Some("qa.runner.no_scenarios_selected")
        })
    ));
    Ok(())
}

#[test]
fn qa_gate_fails_unavailable_capability_without_skip_reason() -> Result<()> {
    let temp_dir = tempfile::tempdir().context("failed to create temp dir")?;
    let scenario_dir = temp_dir.path().join("scenarios");
    fs::create_dir_all(scenario_dir.as_path()).context("failed to create scenario dir")?;
    fs::write(
        scenario_dir.join("capability-unavailable.yaml"),
        r#"
schema_version: 2
id: runtime.capability_unavailable
area: text
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/real_agent_runner.yaml
  policy_profile: qa_no_tools
requires:
  model: text
  capabilities: [agent_run, qa_lab]
  tools: []
  fixtures: [qa/fixtures/real_agent_runner.yaml]
steps:
  - id: prompt
    action: user_prompt
    prompt: "Return the exact deterministic QA response."
expect:
  terminal_state: completed
  events:
    - event_type: run.completed
  tool_calls: []
forbidden:
  tool_calls: ["*"]
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [release_smoke]
timeout:
  run_ms: 30000
"#,
    )
    .context("failed to write unavailable-capability scenario")?;
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
allow_runner_modes: [fixture]
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
            scenario_dir.display().to_string().replace('\\', "/")
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
schema_version: 2
id: p0.fixture_missing
area: tools
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: missing/fixture.yaml
  policy_profile: qa_no_tools
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
allow_runner_modes: [fixture]
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
