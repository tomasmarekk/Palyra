use std::{
    io::Read,
    net::TcpListener,
    path::PathBuf,
    process::{Child, ChildStderr, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;

const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const STARTUP_RETRY_ATTEMPTS: usize = 8;
const STARTUP_HEALTH_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn palyra_daemon_status_reads_health_endpoint() -> Result<()> {
    let (child, port) = spawn_palyrad_with_dynamic_port()?;
    let _daemon = ChildGuard::new(child);

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
    let _daemon = ChildGuard::new(child);

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
    let _daemon = ChildGuard::new(child);

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
    let mut last_error = None;

    for attempt in 1..=STARTUP_RETRY_ATTEMPTS {
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
            .stderr(Stdio::piped())
            .env("RUST_LOG", "info");
        for (key, value) in extra_env {
            command.env(key, value);
        }

        let mut child = command.spawn().context("failed to spawn palyrad process")?;
        match wait_for_health(port, &mut child, STARTUP_HEALTH_TIMEOUT) {
            Ok(()) => return Ok((child, port)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                last_error = Some(error.context(format!(
                    "palyrad startup attempt {attempt}/{STARTUP_RETRY_ATTEMPTS} failed on port {port}"
                )));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("failed to start palyrad for tests")))
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

fn wait_for_health(port: u16, daemon: &mut Child, timeout: Duration) -> Result<()> {
    let timeout_at = Instant::now() + timeout;
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
            let stderr = read_child_stderr(daemon.stderr.take());
            if stderr.is_empty() {
                anyhow::bail!("palyrad exited before becoming healthy with status: {status}");
            }
            anyhow::bail!(
                "palyrad exited before becoming healthy with status: {status}; stderr: {stderr}"
            );
        }
        if client.get(&url).send().and_then(|response| response.error_for_status()).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn read_child_stderr(stderr: Option<ChildStderr>) -> String {
    let Some(mut stderr) = stderr else {
        return String::new();
    };
    let mut output = String::new();
    if stderr.read_to_string(&mut output).is_err() {
        return String::new();
    }
    output.trim().to_owned()
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
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
