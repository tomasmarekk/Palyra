use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    process::{Child, ChildStdout, Command, Stdio},
    sync::mpsc,
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
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let mut command = Command::new(cargo);
    command
        .args([
            "run",
            "--quiet",
            "-p",
            "palyra-daemon",
            "--bin",
            "palyrad",
            "--",
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env("RUST_LOG", "info");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child = command.spawn().context("failed to spawn palyrad process")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let port = wait_for_listen_port(stdout, &mut child)?;
    Ok((child, port))
}

fn wait_for_listen_port(stdout: ChildStdout, daemon: &mut Child) -> Result<u16> {
    let (sender, receiver) = mpsc::channel::<Result<u16, String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read palyrad stdout line".to_owned()));
                }
                return;
            };

            if let Some(port) = parse_listen_port(&line) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok(port));
                }
            }
        }

        if let Some(sender) = sender.take() {
            let _ = sender
                .send(Err("palyrad stdout closed before listen address was published".to_owned()));
        }
    });

    let timeout_at = Instant::now() + Duration::from_secs(90);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(port)) => return Ok(port),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing a port");
            }
        }

        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for daemon listen address log");
        }
        if let Some(status) = daemon.try_wait().context("failed to check daemon status")? {
            anyhow::bail!("palyrad exited before publishing listen address with status: {status}");
        }
    }
}

fn parse_listen_port(line: &str) -> Option<u16> {
    const LISTEN_ADDR_PREFIX: &str = "\"listen_addr\":\"";
    let start = line.find(LISTEN_ADDR_PREFIX)? + LISTEN_ADDR_PREFIX.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
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
