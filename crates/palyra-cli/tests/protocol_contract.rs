use std::process::Command;

use anyhow::{Context, Result};

#[test]
fn protocol_version_reports_major_versions() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["protocol", "version"])
        .output()
        .context("failed to execute palyra protocol version")?;

    assert!(
        output.status.success(),
        "protocol version failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("protocol.major=1"));
    assert!(stdout.contains("json.envelope.v=1"));
    Ok(())
}

#[test]
fn protocol_validate_id_accepts_canonical_ulid() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["protocol", "validate-id", "--id", "01ARZ3NDEKTSV4RRFFQ69G5FAV"])
        .output()
        .context("failed to execute palyra protocol validate-id")?;

    assert!(
        output.status.success(),
        "protocol validate-id failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("canonical_id.valid=true"));
    Ok(())
}

#[test]
fn protocol_validate_id_rejects_invalid_ulid() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["protocol", "validate-id", "--id", "01ARZ3NDEKTSV4RRFFQ69G5FAI"])
        .output()
        .context("failed to execute palyra protocol validate-id")?;

    assert!(!output.status.success(), "expected invalid canonical ID to fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("invalid canonical ID"));
    Ok(())
}
