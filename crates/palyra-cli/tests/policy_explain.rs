use std::process::Command;

use anyhow::{Context, Result};

#[test]
fn policy_explain_reports_deny_by_default() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
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
