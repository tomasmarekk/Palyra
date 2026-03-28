#[cfg(not(windows))]
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[cfg(not(windows))]
use anyhow::{Context, Result};
#[cfg(not(windows))]
use tempfile::TempDir;

#[cfg(not(windows))]
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

#[test]
#[cfg(not(windows))]
fn pairing_requires_explicit_approval() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["pairing", "pair", "--device-id", DEVICE_ID, "--proof-stdin"])
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
            "--allow-insecure-proof-arg",
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
    assert!(!stdout.contains("identity_fingerprint="));
    assert!(!stdout.contains("signing_public_key_hex="));
    assert!(!stdout.contains("transcript_hash="));
    assert!(stdout.contains("pairing.rotation=simulated"));
    Ok(())
}

#[cfg(not(windows))]
#[test]
fn pairing_pair_accepts_proof_via_stdin() -> Result<()> {
    let identity_dir = TempDir::new().context("failed to create temporary identity directory")?;
    let store_dir = identity_dir.path().to_string_lossy().into_owned();
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "pairing",
            "pair",
            "--device-id",
            DEVICE_ID,
            "--proof-stdin",
            "--store-dir",
            &store_dir,
            "--approve",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to execute palyra pairing pair with --proof-stdin")?;

    {
        let stdin = child.stdin.as_mut().context("child stdin should be available")?;
        stdin.write_all(b"123456\n").context("failed to write pairing proof into stdin")?;
    }

    let output = child.wait_with_output().context("failed to wait for pairing process")?;
    assert!(
        output.status.success(),
        "pairing command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    assert!(stdout.contains("pairing.status=paired"));
    Ok(())
}

#[cfg(not(windows))]
#[test]
fn failed_pairing_does_not_persist_device_identity_secret() -> Result<()> {
    let identity_dir = TempDir::new().context("failed to create temporary identity directory")?;
    let store_dir = identity_dir.path().to_string_lossy().into_owned();
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "pairing",
            "pair",
            "--device-id",
            DEVICE_ID,
            "--proof",
            "00000",
            "--allow-insecure-proof-arg",
            "--store-dir",
            &store_dir,
            "--approve",
        ])
        .output()
        .context("failed to execute palyra pairing pair with invalid proof")?;

    assert!(!output.status.success(), "pairing with invalid proof must fail");
    let device_identity_file = identity_dir
        .path()
        .join(hex_encode(format!("device/{DEVICE_ID}/identity.json").as_bytes()));
    assert!(
        !device_identity_file.exists(),
        "device identity secret should not be persisted on failed pairing"
    );
    Ok(())
}

#[cfg(not(windows))]
#[test]
fn pairing_pair_rejects_proof_arg_without_insecure_ack_flag() -> Result<()> {
    let identity_dir = TempDir::new().context("failed to create temporary identity directory")?;
    let store_dir = identity_dir.path().to_string_lossy().into_owned();
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "pairing",
            "pair",
            "--device-id",
            DEVICE_ID,
            "--proof",
            "123456",
            "--store-dir",
            &store_dir,
            "--approve",
        ])
        .output()
        .context("failed to execute palyra pairing pair without insecure ack flag")?;

    assert!(!output.status.success(), "pairing should reject --proof without insecure ack flag");
    let stderr = String::from_utf8(output.stderr).context("stderr was not UTF-8")?;
    assert!(stderr.contains("--allow-insecure-proof-arg"));
    Ok(())
}

#[cfg(not(windows))]
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing into String should not fail");
    }
    output
}
