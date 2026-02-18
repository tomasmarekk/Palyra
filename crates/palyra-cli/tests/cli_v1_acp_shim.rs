use std::{
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    path::PathBuf,
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;
use tempfile::TempDir;
use ulid::Ulid;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

#[test]
fn status_reports_http_grpc_and_admin_health() -> Result<()> {
    let (child, admin_port, grpc_port) = spawn_palyrad_with_dynamic_ports()?;
    let _daemon = ChildGuard::new(child);

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "status",
            "--url",
            &format!("http://127.0.0.1:{admin_port}"),
            "--grpc-url",
            &format!("http://127.0.0.1:{grpc_port}"),
            "--admin",
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "user:ops",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
        ])
        .output()
        .context("failed to execute palyra status")?;

    assert!(
        output.status.success(),
        "status command should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(stdout.contains("status.http=ok"), "missing HTTP health output: {stdout}");
    assert!(stdout.contains("status.grpc=ok"), "missing gRPC health output: {stdout}");
    assert!(stdout.contains("status.admin=ok"), "missing admin health output: {stdout}");
    Ok(())
}

#[test]
fn agent_run_streams_status_events() -> Result<()> {
    let (child, _admin_port, grpc_port) = spawn_palyrad_with_dynamic_ports()?;
    let _daemon = ChildGuard::new(child);
    let session_id = generate_canonical_ulid();
    let run_id = generate_canonical_ulid();

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "agent",
            "run",
            "--grpc-url",
            &format!("http://127.0.0.1:{grpc_port}"),
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "user:ops",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
            "--session-id",
            session_id.as_str(),
            "--run-id",
            run_id.as_str(),
            "--prompt",
            "hello from cli",
        ])
        .output()
        .context("failed to execute palyra agent run")?;

    assert!(
        output.status.success(),
        "agent run should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("agent.status") && stdout.contains("kind=accepted"),
        "agent run should include accepted status event: {stdout}"
    );
    assert!(
        stdout.contains("agent.status") && stdout.contains("kind=done"),
        "agent run should include done status event: {stdout}"
    );
    Ok(())
}

#[test]
fn agent_acp_shim_emits_ndjson_events() -> Result<()> {
    let (child, _admin_port, grpc_port) = spawn_palyrad_with_dynamic_ports()?;
    let _daemon = ChildGuard::new(child);
    let session_id = generate_canonical_ulid();
    let run_id = generate_canonical_ulid();

    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args([
            "agent",
            "acp-shim",
            "--grpc-url",
            &format!("http://127.0.0.1:{grpc_port}"),
            "--token",
            ADMIN_TOKEN,
            "--principal",
            "user:ops",
            "--device-id",
            DEVICE_ID,
            "--channel",
            "cli",
            "--session-id",
            session_id.as_str(),
            "--run-id",
            run_id.as_str(),
            "--prompt",
            "hello from ndjson",
        ])
        .output()
        .context("failed to execute palyra agent acp-shim")?;

    assert!(
        output.status.success(),
        "agent acp-shim should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let mut saw_done = false;
    let mut saw_token = false;
    for line in stdout.lines() {
        let parsed: Value = serde_json::from_str(line).context("acp-shim output must be NDJSON")?;
        if parsed.get("type").and_then(Value::as_str) == Some("run.status")
            && parsed.get("kind").and_then(Value::as_str) == Some("done")
        {
            saw_done = true;
        }
        if parsed.get("type").and_then(Value::as_str) == Some("model.token") {
            saw_token = true;
        }
    }
    assert!(saw_done, "acp-shim stream should include done status line: {stdout}");
    assert!(saw_token, "acp-shim stream should include at least one token line: {stdout}");
    Ok(())
}

#[test]
fn agent_acp_shim_rejects_invalid_ndjson_input() -> Result<()> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["agent", "acp-shim", "--grpc-url", "http://127.0.0.1:7443", "--ndjson-stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn palyra agent acp-shim")?;
    let mut stdin = child.stdin.take().context("failed to access child stdin")?;
    stdin.write_all(b"{invalid-json}\n").context("failed to write stdin payload")?;
    drop(stdin);

    let output = child.wait_with_output().context("failed waiting for command output")?;
    assert!(!output.status.success(), "invalid NDJSON input should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("failed to parse NDJSON ACP input line"),
        "unexpected stderr output: {stderr}"
    );
    Ok(())
}

#[test]
fn agent_interactive_ndjson_keeps_stdout_machine_readable() -> Result<()> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["agent", "interactive", "--ndjson"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn palyra agent interactive")?;
    let mut stdin = child.stdin.take().context("failed to access child stdin")?;
    stdin.write_all(b"/exit\n").context("failed to write stdin payload")?;
    drop(stdin);

    let output = child.wait_with_output().context("failed waiting for command output")?;
    assert!(
        output.status.success(),
        "interactive mode should exit cleanly: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.trim().is_empty(),
        "interactive --ndjson must not write non-NDJSON banners to stdout: {stdout}"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("agent.interactive=session_started"),
        "interactive start hint should move to stderr in --ndjson mode: {stderr}"
    );
    Ok(())
}

#[test]
fn agent_acp_shim_rejects_whitespace_prompt_ndjson_input() -> Result<()> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["agent", "acp-shim", "--grpc-url", "http://127.0.0.1:7443", "--ndjson-stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn palyra agent acp-shim")?;
    let mut stdin = child.stdin.take().context("failed to access child stdin")?;
    stdin.write_all(b"{\"prompt\":\"   \"}").context("failed to write stdin payload")?;
    stdin.write_all(b"\n").context("failed to write stdin newline")?;
    drop(stdin);

    let output = child.wait_with_output().context("failed waiting for command output")?;
    assert!(!output.status.success(), "whitespace prompt NDJSON input should fail");
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("non-empty text"),
        "expected prompt validation error for whitespace-only NDJSON input: {stderr}"
    );
    assert!(
        !stderr.contains("failed to connect gateway gRPC endpoint"),
        "prompt validation should fail before gRPC connection attempts: {stderr}"
    );
    Ok(())
}

#[test]
fn completion_generates_bash_script() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["completion", "--shell", "bash"])
        .output()
        .context("failed to execute palyra completion")?;
    assert!(
        output.status.success(),
        "completion command should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(stdout.contains("_palyra"), "expected bash completion function in output");
    Ok(())
}

#[test]
fn onboarding_wizard_writes_config_file() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("config").join("palyra.toml");
    let config_path_string = config_path.to_string_lossy().to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_palyra"))
        .args(["onboarding", "wizard", "--path", config_path_string.as_str()])
        .output()
        .context("failed to execute palyra onboarding wizard")?;
    assert!(
        output.status.success(),
        "onboarding wizard should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(stdout.contains("onboarding.status=complete"), "unexpected output: {stdout}");
    let written = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read written config {}", config_path.display()))?;
    assert!(written.contains("version = 1"), "expected config version marker");
    assert!(written.contains("[orchestrator]"), "expected orchestrator section");
    Ok(())
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16, u16)> {
    let journal_db_path = unique_temp_journal_db_path();
    let mut command = Command::new(resolve_palyrad_binary_path()?);
    command
        .args([
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
        .stderr(Stdio::piped())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("RUST_LOG", "info");

    let mut child = command.spawn().context("failed to spawn palyrad process")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child, STARTUP_TIMEOUT)?;
    wait_for_health(admin_port, &mut child, STARTUP_TIMEOUT)?;
    Ok((child, admin_port, grpc_port))
}

fn unique_temp_journal_db_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "palyra-cli-v1-journal-{}-{}.sqlite3",
        std::process::id(),
        generate_canonical_ulid()
    ))
}

fn generate_canonical_ulid() -> String {
    Ulid::new().to_string()
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

fn wait_for_listen_ports(
    stdout: ChildStdout,
    daemon: &mut Child,
    timeout: Duration,
) -> Result<(u16, u16)> {
    let (sender, receiver) = mpsc::channel::<Result<(u16, u16), String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut admin_port = None::<u16>;
        let mut grpc_port = None::<u16>;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read palyrad stdout line".to_owned()));
                }
                return;
            };

            if admin_port.is_none() {
                admin_port = parse_port_from_log(&line, "\"listen_addr\":\"");
            }
            if grpc_port.is_none() {
                grpc_port = parse_port_from_log(&line, "\"grpc_listen_addr\":\"");
            }

            if let (Some(admin_port), Some(grpc_port)) = (admin_port, grpc_port) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok((admin_port, grpc_port)));
                }
                return;
            }
        }

        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(
                "palyrad stdout closed before admin and gRPC listen addresses were published"
                    .to_owned(),
            ));
        }
    });

    let timeout_at = Instant::now() + timeout;
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(ports)) => return Ok(ports),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing ports");
            }
        }
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad listen address logs");
        }
        if let Some(status) = daemon.try_wait().context("failed to check daemon status")? {
            anyhow::bail!(
                "palyrad exited before publishing listen addresses with status: {status}"
            );
        }
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|socket_address| socket_address.port())
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
