use std::{
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::{Child, ChildStderr, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use tempfile::TempDir;

const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const STARTUP_RETRY_ATTEMPTS: usize = 8;
const STARTUP_HEALTH_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn palyra_daemon_status_reads_health_endpoint() -> Result<()> {
    let (child, port, state_root) = spawn_palyrad_with_dynamic_port()?;
    let _daemon = ChildGuard::new(child, state_root);

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
    let (child, port, state_root) =
        spawn_palyrad_with_dynamic_port_and_env(&[("PALYRA_ADMIN_REQUIRE_AUTH", "false")])?;
    let _daemon = ChildGuard::new(child, state_root);

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
    let (child, port, state_root) = spawn_palyrad_with_dynamic_port()?;
    let _daemon = ChildGuard::new(child, state_root);

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
fn palyra_daemon_run_inspection_supports_json_output() -> Result<()> {
    let run_id = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
    let status_fixture = spawn_json_fixture(
        format!("/admin/v1/runs/{run_id}"),
        format!(
            r#"{{"run_id":"{run_id}","state":"completed","cancel_requested":false,"prompt_tokens":11,"completion_tokens":7,"total_tokens":18,"tape_events":2}}"#
        ),
    )?;

    let status_output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "--output-format",
            "json",
            "daemon",
            "run-status",
            "--url",
            status_fixture.base_url.as_str(),
            "--run-id",
            run_id,
        ])
        .output()
        .context("failed to execute palyra daemon run-status with global JSON output")?;
    status_fixture.finish()?;
    assert!(
        status_output.status.success(),
        "run-status should support global JSON output: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_payload: serde_json::Value = serde_json::from_slice(status_output.stdout.as_slice())
        .context("run-status stdout should be JSON")?;
    assert_eq!(status_payload.get("run_id").and_then(serde_json::Value::as_str), Some(run_id));
    assert_eq!(status_payload.get("total_tokens").and_then(serde_json::Value::as_u64), Some(18));

    let tape_fixture = spawn_json_fixture(
        format!("/admin/v1/runs/{run_id}/tape"),
        format!(
            r#"{{"run_id":"{run_id}","returned_bytes":45,"next_after_seq":7,"events":[{{"seq":7,"event_type":"model_token","payload_json":"{{\"text\":\"ok\"}}"}}]}}"#
        ),
    )?;
    let tape_output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "daemon",
            "run-tape",
            "--url",
            tape_fixture.base_url.as_str(),
            "--run-id",
            run_id,
            "--after-seq",
            "6",
            "--limit",
            "1",
            "--json",
        ])
        .output()
        .context("failed to execute palyra daemon run-tape with local JSON flag")?;
    tape_fixture.finish()?;
    assert!(
        tape_output.status.success(),
        "run-tape should accept --json: {}",
        String::from_utf8_lossy(&tape_output.stderr)
    );
    let tape_payload: serde_json::Value = serde_json::from_slice(tape_output.stdout.as_slice())
        .context("run-tape stdout should be JSON")?;
    assert_eq!(tape_payload.get("run_id").and_then(serde_json::Value::as_str), Some(run_id));
    assert_eq!(
        tape_payload.pointer("/events/0/event_type").and_then(serde_json::Value::as_str),
        Some("model_token")
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

fn spawn_palyrad_with_dynamic_port() -> Result<(Child, u16, TempDir)> {
    spawn_palyrad_with_dynamic_port_and_env(&[])
}

fn spawn_palyrad_with_dynamic_port_and_env(
    extra_env: &[(&str, &str)],
) -> Result<(Child, u16, TempDir)> {
    let mut last_error = None;

    for attempt in 1..=STARTUP_RETRY_ATTEMPTS {
        let port = reserve_loopback_port()?;
        let state_root =
            tempfile::tempdir().context("failed to create isolated palyrad state root")?;
        let identity_store_dir = state_root.path().join("identity");
        std::fs::create_dir_all(&identity_store_dir).with_context(|| {
            format!("failed to create isolated test identity dir {}", identity_store_dir.display())
        })?;
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
            .env("RUST_LOG", "info")
            .env("PALYRA_ADMIN_TOKEN", "test-admin-token")
            .env("PALYRA_STATE_ROOT", state_root.path().to_string_lossy().to_string())
            .env(
                "PALYRA_GATEWAY_IDENTITY_STORE_DIR",
                identity_store_dir.to_string_lossy().to_string(),
            )
            .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
            .env("PALYRA_GATEWAY_QUIC_PORT", "0");
        for (key, value) in extra_env {
            command.env(key, value);
        }

        let mut child = command.spawn().context("failed to spawn palyrad process")?;
        match wait_for_health(port, &mut child, STARTUP_HEALTH_TIMEOUT) {
            Ok(()) => return Ok((child, port, state_root)),
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

struct JsonFixture {
    base_url: String,
    handle: thread::JoinHandle<Result<()>>,
}

impl JsonFixture {
    fn finish(self) -> Result<()> {
        self.handle.join().map_err(|_| anyhow!("JSON fixture thread panicked"))?
    }
}

fn spawn_json_fixture(expected_path_prefix: String, body: String) -> Result<JsonFixture> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind JSON fixture listener")?;
    listener.set_nonblocking(true).context("failed to configure JSON fixture listener")?;
    let port =
        listener.local_addr().context("failed to inspect JSON fixture listener address")?.port();
    let handle = thread::spawn(move || -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        anyhow::bail!("timed out waiting for JSON fixture request");
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error).context("failed to accept JSON fixture request"),
            }
        };
        stream
            .set_nonblocking(false)
            .context("failed to configure JSON fixture request as blocking")?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .context("failed to configure JSON fixture request timeout")?;
        let mut buffer = [0_u8; 4096];
        let bytes = stream.read(&mut buffer).context("failed to read JSON fixture request")?;
        let request = String::from_utf8_lossy(&buffer[..bytes]);
        let first_line = request.lines().next().unwrap_or_default();
        let expected = format!("GET {expected_path_prefix}");
        if !first_line.starts_with(expected.as_str()) {
            anyhow::bail!(
                "expected request path prefix '{}' but received '{}'",
                expected_path_prefix,
                first_line
            );
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).context("failed to write JSON fixture response")?;
        Ok(())
    });
    Ok(JsonFixture { base_url: format!("http://127.0.0.1:{port}"), handle })
}

struct ChildGuard {
    child: Child,
    _state_root: TempDir,
}

impl ChildGuard {
    fn new(child: Child, state_root: TempDir) -> Self {
        Self { child, _state_root: state_root }
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
