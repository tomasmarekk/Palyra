use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;

const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

#[test]
fn palyra_daemon_status_reads_health_endpoint() -> Result<()> {
    let (child, port) = spawn_palyrad_with_dynamic_port()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(port, daemon.child_mut())?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    let output = command
        .args(["daemon", "status", "--url", &format!("http://127.0.0.1:{port}")])
        .output()
        .context("failed to execute palyra daemon status")?;

    assert!(
        output.status.success(),
        "palyra daemon status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(stdout.contains("status=ok"), "expected status=ok in output, got: {stdout}");
    Ok(())
}

#[test]
fn palyra_daemon_admin_status_without_token_succeeds_when_auth_is_disabled() -> Result<()> {
    let (child, port) =
        spawn_palyrad_with_dynamic_port_and_env(&[("PALYRA_ADMIN_REQUIRE_AUTH", "false")])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(port, daemon.child_mut())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "daemon",
            "admin-status",
            "--url",
            &format!("http://127.0.0.1:{port}"),
            "--principal",
            "user:ops",
            "--device-id",
            DEVICE_ID,
        ])
        .output()
        .context("failed to execute palyra daemon admin-status without token")?;

    assert!(
        output.status.success(),
        "admin-status should work without token when auth is disabled: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("admin.status=ok"),
        "expected admin.status=ok in output, got: {stdout}"
    );
    Ok(())
}

#[test]
fn palyra_daemon_admin_status_without_token_fails_when_auth_is_required() -> Result<()> {
    let (child, port) = spawn_palyrad_with_dynamic_port()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(port, daemon.child_mut())?;

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "daemon",
            "admin-status",
            "--url",
            &format!("http://127.0.0.1:{port}"),
            "--principal",
            "user:ops",
            "--device-id",
            DEVICE_ID,
        ])
        .output()
        .context("failed to execute palyra daemon admin-status without token")?;

    assert!(
        !output.status.success(),
        "admin-status should fail without token when auth is required"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("daemon admin status endpoint returned non-success status"),
        "expected CLI to fail on endpoint status, got: {stderr}"
    );
    Ok(())
}

#[test]
fn palyra_daemon_run_status_rejects_non_canonical_run_id() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["daemon", "run-status", "--run-id", "invalid-ulid"])
        .output()
        .context("failed to execute palyra daemon run-status")?;
    assert!(!output.status.success(), "run-status should reject non-canonical run IDs");
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("run_id must be a canonical ULID"),
        "expected canonical ULID validation failure, got: {stderr}"
    );
    Ok(())
}

#[test]
fn palyra_daemon_run_cancel_rejects_non_canonical_run_id() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["daemon", "run-cancel", "--run-id", "invalid-ulid"])
        .output()
        .context("failed to execute palyra daemon run-cancel")?;
    assert!(!output.status.success(), "run-cancel should reject non-canonical run IDs");
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("run_id must be a canonical ULID"),
        "expected canonical ULID validation failure, got: {stderr}"
    );
    Ok(())
}

fn spawn_palyrad_with_dynamic_port() -> Result<(Child, u16)> {
    spawn_palyrad_with_dynamic_port_and_env(&[])
}

fn spawn_palyrad_with_dynamic_port_and_env(extra_env: &[(&str, &str)]) -> Result<(Child, u16)> {
    let port = reserve_loopback_port()?;
    let mut command = Command::new(resolve_palyrad_binary_path()?);
    command
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env("RUST_LOG", "info");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let child = command.spawn().context("failed to spawn palyrad process")?;
    Ok((child, port))
}

fn resolve_palyrad_binary_path() -> Result<PathBuf> {
    let mut path = std::env::current_exe().context("failed to resolve test binary path")?;
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    let executable = if cfg!(windows) { "palyrad.exe" } else { "palyrad" };
    path.push(executable);
    if path.exists() {
        Ok(path)
    } else {
        anyhow::bail!("failed to resolve palyrad binary path: {}", path.display());
    }
}

fn reserve_loopback_port() -> Result<u16> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to reserve loopback port for palyrad")?;
    let port = listener
        .local_addr()
        .context("failed to inspect reserved loopback listener address")?
        .port();
    drop(listener);
    Ok(port)
}

fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + Duration::from_secs(90);
    let url = format!("http://127.0.0.1:{port}/healthz");
    let client = Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build HTTP client")?;

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for daemon health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check daemon status")? {
            anyhow::bail!("palyrad exited before becoming healthy with status: {status}");
        }
        if client.get(&url).send().and_then(|response| response.error_for_status()).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
