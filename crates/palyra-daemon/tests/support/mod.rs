use std::{
    fs,
    io::Read,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, ChildStderr, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use reqwest::{blocking::Client, Method, StatusCode};
use serde_json::Value;
use tempfile::TempDir;

pub const ADMIN_TOKEN: &str = "test-admin-token";
pub const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
pub const CONSOLE_ADMIN_PRINCIPAL: &str = "admin:web-console";

const PALYRAD_STARTUP_ATTEMPTS: usize = 3;
const PALYRAD_STARTUP_RETRY_DELAY: Duration = Duration::from_millis(150);
const PALYRAD_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct ConsoleSession {
    pub cookie: String,
    pub _csrf_token: String,
}

pub struct DaemonHarness {
    _tempdir: TempDir,
    pub admin_port: u16,
    pub client: Client,
    child: Child,
}

impl DaemonHarness {
    pub fn spawn(extra_env: &[(&str, &str)]) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .context("failed to build daemon harness HTTP client")?;
        let mut last_error: Option<anyhow::Error> = None;
        for attempt in 1..=PALYRAD_STARTUP_ATTEMPTS {
            match spawn_palyrad_once(extra_env) {
                Ok((tempdir, mut child, admin_port)) => {
                    match wait_for_health(admin_port, &mut child) {
                        Ok(()) => {
                            return Ok(Self { _tempdir: tempdir, admin_port, client, child });
                        }
                        Err(error) => {
                            let _ = child.kill();
                            let _ = child.wait();
                            last_error = Some(error);
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
            if attempt < PALYRAD_STARTUP_ATTEMPTS {
                thread::sleep(PALYRAD_STARTUP_RETRY_DELAY);
            }
        }
        let Some(last_error) = last_error else {
            bail!("failed to spawn palyrad for integration test harness");
        };
        Err(last_error).context(format!(
            "failed to spawn palyrad after {PALYRAD_STARTUP_ATTEMPTS} startup attempts"
        ))
    }

    pub fn login_as_admin(&self) -> Result<ConsoleSession> {
        let response = self
            .client
            .post(format!("http://127.0.0.1:{}/console/v1/auth/login", self.admin_port))
            .json(&serde_json::json!({
                "admin_token": ADMIN_TOKEN,
                "principal": CONSOLE_ADMIN_PRINCIPAL,
                "device_id": DEVICE_ID,
                "channel": "web",
            }))
            .send()
            .context("failed to call console login")?;
        let response = response;
        if let Err(error) = response.error_for_status_ref() {
            let status = response.status();
            let body = response.text().unwrap_or_else(|_| "<unavailable>".to_owned());
            return Err(anyhow::anyhow!(
                "console login returned non-success status {}: {}; body={}",
                status.as_u16(),
                error,
                body
            ));
        }
        let cookie = response
            .headers()
            .get("set-cookie")
            .context("console login did not return session cookie")?
            .to_str()
            .context("console login session cookie must be valid utf-8")?
            .to_owned();
        let body =
            response.json::<Value>().context("failed to parse console login response json")?;
        let csrf_token = body
            .get("csrf_token")
            .and_then(Value::as_str)
            .context("console login did not return csrf_token")?
            .to_owned();
        Ok(ConsoleSession { cookie, _csrf_token: csrf_token })
    }

    pub fn console_json(&self, path: &str, session: &ConsoleSession) -> Result<Value> {
        self.client
            .get(format!("http://127.0.0.1:{}{path}", self.admin_port))
            .header("Cookie", session.cookie.as_str())
            .send()
            .with_context(|| format!("failed to call console endpoint {path}"))?
            .error_for_status()
            .with_context(|| format!("console endpoint {path} returned non-success status"))?
            .json::<Value>()
            .with_context(|| format!("failed to parse console endpoint {path} response json"))
    }

    pub fn route_registered(&self, method: Method, path: &str) -> Result<bool> {
        let url = format!("http://127.0.0.1:{}{path}", self.admin_port);
        let request = match method {
            Method::GET => self.client.get(url),
            Method::POST => self.client.post(url).json(&serde_json::json!({})),
            other => bail!("unsupported method for test harness route probe: {other}"),
        };
        let status =
            request.send().with_context(|| format!("failed to probe route {path}"))?.status();
        Ok(status != StatusCode::NOT_FOUND)
    }
}

impl Drop for DaemonHarness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn assert_json_golden(name: &str, actual: &Value) -> Result<()> {
    let path = golden_path(name);
    let serialized =
        serde_json::to_string_pretty(actual).context("failed to serialize actual golden json")?;
    if should_update_goldens() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create golden fixture directory {}", parent.display())
            })?;
        }
        fs::write(&path, format!("{serialized}\n"))
            .with_context(|| format!("failed to update golden fixture {}", path.display()))?;
        return Ok(());
    }
    let expected_raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read golden fixture {}", path.display()))?;
    let expected = serde_json::from_str::<Value>(expected_raw.as_str())
        .with_context(|| format!("failed to parse golden fixture {}", path.display()))?;
    if &expected != actual {
        bail!(
            "golden fixture {} did not match actual output; rerun with PALYRA_UPDATE_GOLDENS=1 to update it",
            path.display()
        );
    }
    Ok(())
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("golden").join(name)
}

fn should_update_goldens() -> bool {
    std::env::var("PALYRA_UPDATE_GOLDENS")
        .ok()
        .map(|value| {
            matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

fn spawn_palyrad_once(extra_env: &[(&str, &str)]) -> Result<(TempDir, Child, u16)> {
    let tempdir = tempfile::tempdir().context("failed to create temporary test directory")?;
    let state_root_dir = tempdir.path().join("state-root");
    let journal_db_path = state_root_dir.join("journal.sqlite3");
    let identity_store_dir = state_root_dir.join("identity");
    let vault_dir = state_root_dir.join("vault");
    let skills_trust_store_path = state_root_dir.join("skills").join("trust-store.json");
    let config_path = tempdir.path().join("palyra.toml");
    let admin_port = reserve_loopback_port()?;
    fs::create_dir_all(&identity_store_dir).with_context(|| {
        format!("failed to create test identity dir {}", identity_store_dir.display())
    })?;
    if let Some(parent) = skills_trust_store_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create test skills dir {}", parent.display()))?;
    }
    prepare_test_vault_dir(&vault_dir)?;
    write_test_config(&config_path, "version = 1\n")?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_palyrad"));
    command
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            admin_port.to_string().as_str(),
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .env("PALYRA_CONFIG", config_path.to_string_lossy().to_string())
        .env("PALYRA_STATE_ROOT", state_root_dir.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
        .env("PALYRA_GATEWAY_QUIC_PORT", "0")
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_REQUIRE_AUTH", "false")
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("PALYRA_SKILLS_TRUST_STORE", skills_trust_store_path.to_string_lossy().to_string())
        .env_remove("PALYRA_SKILL_REAUDIT_INTERVAL_MS")
        .env_remove("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY")
        .env_remove("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF")
        .env_remove("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL")
        .env_remove("HTTP_PROXY")
        .env_remove("HTTPS_PROXY")
        .env_remove("ALL_PROXY")
        .env_remove("NO_PROXY")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let child = command.spawn().context("failed to start palyrad")?;
    Ok((tempdir, child, admin_port))
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

fn prepare_test_vault_dir(vault_dir: &Path) -> Result<()> {
    fs::create_dir_all(vault_dir)
        .with_context(|| format!("failed to create test vault dir {}", vault_dir.display()))?;
    let backend_marker = vault_dir.join("backend.kind");
    fs::write(&backend_marker, b"encrypted_file").with_context(|| {
        format!("failed to write vault backend marker {}", backend_marker.display())
    })?;
    Ok(())
}

fn write_test_config(config_path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create test config dir {}", parent.display()))?;
    }
    fs::write(config_path, contents)
        .with_context(|| format!("failed to write test config {}", config_path.display()))?;
    Ok(())
}

fn read_child_stderr(stderr: Option<ChildStderr>) -> String {
    let Some(mut stderr) = stderr else {
        return String::new();
    };
    let mut buffer = String::new();
    let _ = stderr.read_to_string(&mut buffer);
    buffer.trim().to_owned()
}

fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + PALYRAD_STARTUP_TIMEOUT;
    let url = format!("http://127.0.0.1:{port}/healthz");
    let client = Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build health probe client")?;

    loop {
        if Instant::now() > timeout_at {
            bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            let stderr = read_child_stderr(daemon.stderr.take());
            if stderr.is_empty() {
                bail!("palyrad exited before becoming healthy with status: {status}");
            }
            bail!("palyrad exited before becoming healthy with status: {status}; stderr: {stderr}");
        }
        if client.get(&url).send().and_then(|response| response.error_for_status()).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
}
