use std::process::Command;

use anyhow::{Context, Result};
#[cfg(not(windows))]
use tempfile::TempDir;

const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

#[test]
fn pairing_requires_explicit_approval() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["pairing", "pair", "--device-id", DEVICE_ID, "--proof", "123456"])
        .output()
        .context("failed to execute palyra pairing pair")?;

    assert!(!output.status.success(), "pairing should fail without --approve");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("decision=deny_by_default"));
    assert!(stderr.contains("approval_required=true"));
    Ok(())
}

#[cfg(not(windows))]
#[test]
fn pairing_pair_outputs_verifiable_identity_and_rotation() -> Result<()> {
    let identity_dir = TempDir::new().context("failed to create temporary identity directory")?;
    let store_dir = identity_dir.path().to_string_lossy().into_owned();
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "pairing",
            "pair",
            "--device-id",
            DEVICE_ID,
            "--client-kind",
            "desktop",
            "--method",
            "qr",
            "--proof",
            "0123456789ABCDEF0123456789ABCDEF",
            "--store-dir",
            &store_dir,
            "--approve",
            "--simulate-rotation",
        ])
        .output()
        .context("failed to execute palyra pairing pair")?;

    assert!(
        output.status.success(),
        "pairing command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("pairing.status=paired"));
    assert!(stdout.contains("device_id=01ARZ3NDEKTSV4RRFFQ69G5FAV"));
    assert!(stdout.contains("client_kind=desktop"));
    assert!(stdout.contains("identity_fingerprint="));
    assert!(stdout.contains("transcript_hash="));
    assert!(stdout.contains("pairing.rotation=simulated"));
    Ok(())
}

#[cfg(windows)]
#[test]
fn pairing_pair_refuses_windows_volatile_storage() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["pairing", "pair", "--device-id", DEVICE_ID, "--proof", "123456", "--approve"])
        .output()
        .context("failed to execute palyra pairing pair on windows")?;

    assert!(!output.status.success(), "pairing should fail on windows without durable store");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("persistent identity storage is not available on Windows yet"));
    Ok(())
}
