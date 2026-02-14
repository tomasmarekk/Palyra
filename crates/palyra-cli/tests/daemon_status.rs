use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;

#[test]
fn palyra_daemon_status_reads_health_endpoint() -> Result<()> {
    let port = random_port()?;
    let child = spawn_palyrad(port)?;
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

fn spawn_palyrad(port: u16) -> Result<Child> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let child = Command::new(cargo)
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
            &port.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn palyrad process")?;
    Ok(child)
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
        if client.get(&url).send().is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn random_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("failed to reserve random port")?;
    let port = listener.local_addr().context("failed to resolve listener local addr")?.port();
    drop(listener);
    Ok(port)
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
