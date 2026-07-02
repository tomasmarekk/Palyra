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
