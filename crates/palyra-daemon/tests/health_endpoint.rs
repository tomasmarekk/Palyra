use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;

#[test]
fn palyrad_health_endpoint_returns_ok() -> Result<()> {
    let port = random_port()?;
    let child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
        .args(["--bind", "127.0.0.1", "--port", &port.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad")?;
    let mut daemon = ChildGuard::new(child);

    wait_for_health(port, daemon.child_mut())?;

    let response_body = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .context("failed to call health endpoint")?
        .error_for_status()
        .context("daemon returned non-success status")?
        .text()
        .context("failed to read health response body")?;

    assert!(response_body.contains("\"status\":\"ok\""));
    Ok(())
}

fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + Duration::from_secs(10);
    let url = format!("http://127.0.0.1:{port}/healthz");
    let client = Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build HTTP client")?;

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
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
    let port = listener.local_addr().context("failed to read local addr")?.port();
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
