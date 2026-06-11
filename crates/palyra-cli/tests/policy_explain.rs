//! Pins `palyra policy explain`: deny-by-default when no config exists and honoring a
//! configured tool allowlist.

use std::process::Command;

use anyhow::{Context, Result};
use serde_json::Value;

#[test]
fn policy_explain_reports_deny_by_default() -> Result<()> {
    let temp = tempfile::tempdir().context("failed to create tempdir")?;
    let missing_config_path = temp.path().join("missing-palyra.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .env("PALYRA_CONFIG", missing_config_path.as_path())
        .env_remove("PALYRA_TOOL_CALL_ALLOWED_TOOLS")
        .args([
            "policy",
            "explain",
            "--principal",
            "user:test",
            "--action",
            "tool.execute.shell",
            "--resource",
            "tool:shell",
        ])
        .output()
        .context("failed to execute palyra policy explain")?;

    assert!(
        output.status.success(),
        "policy explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("decision=deny_by_default"));
    assert!(stdout.contains("approval_required=true"));
    Ok(())
}

#[test]
fn policy_explain_uses_configured_tool_allowlist() -> Result<()> {
    let temp = tempfile::tempdir().context("failed to create tempdir")?;
    let config_path = temp.path().join("palyra.toml");
    std::fs::write(
        config_path.as_path(),
        r#"
[tool_call]
allowed_tools = ["palyra.fs.apply_patch"]
"#,
    )
    .context("failed to write policy config")?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .env("PALYRA_CONFIG", config_path.as_path())
        .env_remove("PALYRA_TOOL_CALL_ALLOWED_TOOLS")
        .args([
            "policy",
            "explain",
            "--principal",
            "admin:local",
            "--action",
            "tool.execute",
            "--resource",
            "tool:palyra.fs.apply_patch",
            "--json",
        ])
        .output()
        .context("failed to execute palyra policy explain")?;

    assert!(
        output.status.success(),
        "policy explain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("decision").and_then(Value::as_str), Some("deny_by_default"));
    assert_eq!(payload.get("approval_required").and_then(Value::as_bool), Some(true));
    assert_eq!(
        payload.pointer("/runtime_approval_overlay/approval_required").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/explanation/is_allowlisted_tool").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/explanation/is_sensitive_action").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/explanation/requested_tool").and_then(Value::as_str),
        Some("palyra.fs.apply_patch")
    );
    assert!(
        payload
            .pointer("/policy_config/source")
            .and_then(Value::as_str)
            .is_some_and(|source| source.starts_with("config:")),
        "policy explain should report that it loaded the local config: {payload}"
    );
    Ok(())
}
