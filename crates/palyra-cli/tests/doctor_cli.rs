//! Pins `palyra doctor` diagnostics: global config resolution, vault-ref and gateway
//! connectivity findings, and admin-token probing against a stub loopback HTTP server
//! standing in for the daemon.

use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn configure_cli_env(command: &mut Command, workdir: &TempDir) {
    command
        .env("PALYRA_STATE_ROOT", workdir.path().join("state-root"))
        .env("PALYRA_VAULT_BACKEND", "encrypted_file")
        .env_remove("PALYRA_ADMIN_TOKEN")
        .env_remove("PALYRA_DAEMON_URL")
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"))
        .env("PROGRAMDATA", workdir.path().join("programdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn unused_loopback_port_pair() -> Result<(u16, u16)> {
    let first = TcpListener::bind("127.0.0.1:0").context("failed to bind first test port")?;
    let second = TcpListener::bind("127.0.0.1:0").context("failed to bind second test port")?;
    let first_port = first.local_addr().context("failed to read first test port")?.port();
    let second_port = second.local_addr().context("failed to read second test port")?.port();
    Ok((first_port, second_port))
}

fn spawn_doctor_http_server(admin_token: &str) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    spawn_doctor_http_server_with_admin_body(
        admin_token,
        r#"{"status":"ok","counters":{"journal_events":1,"denied_requests":0},"auth":{"summary":{"total_profiles":0,"ok":0,"missing":0,"expired":0,"expiring":0}}}"#
            .to_owned(),
    )
}

fn spawn_doctor_http_server_with_admin_body(
    admin_token: &str,
    admin_body: String,
) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind doctor test server")?;
    let address = listener.local_addr().context("failed to read doctor test server address")?;
    listener.set_nonblocking(true).context("failed to mark doctor test server as non-blocking")?;
    let expected_auth = format!("authorization: bearer {}", admin_token.to_ascii_lowercase());
    let handle = thread::spawn(move || -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                    continue;
                }
                Err(error) => return Err(error).context("failed to accept doctor test request"),
            };
            stream
                .set_nonblocking(false)
                .context("failed to restore blocking mode for doctor test request")?;
            let mut buffer = [0_u8; 4096];
            let bytes_read =
                stream.read(&mut buffer).context("failed to read doctor test request")?;
            let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            let request_lower = request.to_ascii_lowercase();
            let is_admin_probe = request.starts_with("GET /admin/v1/status ");
            let (status_line, body) = if request.starts_with("GET /healthz ") {
                (
                    "200 OK",
                    r#"{"service":"palyrad","status":"ok","version":"0.1.0","git_hash":"deadbeef","build_profile":"test","uptime_seconds":1}"#,
                )
            } else if is_admin_probe {
                anyhow::ensure!(
                    request_lower.contains(expected_auth.as_str()),
                    "doctor admin probe should send configured admin bearer token: {request}"
                );
                ("200 OK", admin_body.as_str())
            } else {
                ("404 Not Found", r#"{"error":"not found"}"#)
            };
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .context("failed to write doctor test response")?;
            if is_admin_probe {
                return Ok(());
            }
        }
        anyhow::bail!(
            "doctor should request /admin/v1/status when config resolves admin auth within 60s"
        )
    });
    Ok((format!("http://{address}"), handle))
}

#[test]
fn doctor_json_uses_global_config_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("custom-palyra.toml");
    fs::write(
        config_path.as_path(),
        r#"
version = 1

[admin]
require_auth = false
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(&workdir, &["--config", &config_path_string, "doctor", "--json"])?;

    assert!(
        output.status.success(),
        "doctor --json should succeed without --strict: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/config/path").and_then(Value::as_str),
        Some(config_path_string.as_str())
    );
    assert_eq!(payload.pointer("/diagnostics/config/exists").and_then(Value::as_bool), Some(true));
    assert_eq!(payload.pointer("/diagnostics/config/parsed").and_then(Value::as_bool), Some(true));
    Ok(())
}

#[test]
fn doctor_flags_missing_anthropic_vault_refs_without_daemon_diagnostics() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("minimax-palyra.toml");
    fs::write(
        config_path.as_path(),
        r#"
version = 1

[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(&workdir, &["--config", &config_path_string, "doctor", "--json"])?;

    assert!(
        output.status.success(),
        "doctor --json should succeed for missing-vault-ref diagnostics: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/config_ref_health/severity").and_then(Value::as_str),
        Some("blocking")
    );
    assert_eq!(
        payload
            .pointer("/diagnostics/config_ref_health/summary/blocking_refs")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        payload.pointer("/diagnostics/checks").and_then(Value::as_array).is_some_and(|checks| {
            checks.iter().any(|check| {
                check.get("key").and_then(Value::as_str) == Some("config_secret_refs_ok")
                    && check.get("ok").and_then(Value::as_bool) == Some(false)
                    && check.get("severity").and_then(Value::as_str) == Some("blocking")
            })
        }),
        "doctor checks should include a blocking config_secret_refs_ok failure: {payload}"
    );
    Ok(())
}

#[test]
fn doctor_text_surfaces_gateway_runtime_connectivity_as_blocking() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("local-palyra.toml");
    let (daemon_port, grpc_port) = unused_loopback_port_pair()?;
    fs::write(
        config_path.as_path(),
        format!(
            r#"
version = 1

[daemon]
bind_addr = "127.0.0.1"
port = {daemon_port}

[gateway]
grpc_bind_addr = "127.0.0.1"
grpc_port = {grpc_port}

[admin]
require_auth = false
"#
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let output = run_cli(&workdir, &["--config", &config_path_string, "doctor"])?;

    assert!(
        output.status.success(),
        "doctor should succeed without --strict even when runtime is down: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;

    assert!(
        stdout.contains(
            "doctor.check key=gateway_runtime_reachable ok=false required=true severity=blocking"
        ),
        "doctor text output should surface gateway connectivity as a blocking check: {stdout}"
    );
    assert!(
        stdout.contains("doctor.connectivity http_ok=false grpc_ok=false"),
        "doctor text output should emit connectivity probe state: {stdout}"
    );
    assert!(
        stdout.contains("doctor.next_step=palyra gateway run"),
        "doctor next steps should point to the immediate runtime start path: {stdout}"
    );
    Ok(())
}

#[test]
fn doctor_uses_configured_admin_token_for_admin_probe() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let admin_token = "config-admin-token";
    let (daemon_url, server_handle) = spawn_doctor_http_server(admin_token)?;
    let daemon_port =
        daemon_url.rsplit(':').next().context("doctor test server URL should include a port")?;
    let config_path = workdir.path().join("admin-config-palyra.toml");
    fs::write(
        config_path.as_path(),
        format!(
            r#"
version = 1

[daemon]
bind_addr = "127.0.0.1"
port = {daemon_port}

[admin]
auth_token = "{admin_token}"
"#
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    configure_cli_env(&mut command, &workdir);
    command
        .current_dir(workdir.path())
        .args(["--config", config_path_string.as_str(), "doctor", "--json"])
        .env("PALYRA_DAEMON_URL", daemon_url.as_str());
    let output = command.output().with_context(|| {
        format!("failed to execute palyra --config {} doctor --json", config_path.display())
    })?;
    let server_result = server_handle.join();
    if let Err(panic) = server_result {
        anyhow::bail!(
            "doctor test server thread panicked: {:?}\nstdout={}\nstderr={}",
            panic,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if let Err(error) = server_result.expect("server join result should exist") {
        anyhow::bail!(
            "doctor test server failed: {error:#}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert!(
        output.status.success(),
        "doctor --json should succeed with config-backed admin auth: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/connectivity/http/ok").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/diagnostics/connectivity/admin/ok").and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        payload.pointer("/diagnostics/connectivity/admin/message").is_none(),
        "doctor should not skip admin diagnostics when config resolves admin auth: {payload}"
    );
    Ok(())
}

#[test]
fn doctor_json_surfaces_feature_rollout_maturity_from_admin_payload() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let admin_token = "config-admin-token";
    let admin_body = r#"{
        "status":"ok",
        "counters":{"journal_events":1,"denied_requests":0},
        "auth":{"summary":{"total_profiles":0,"ok":0,"missing":0,"expired":0,"expiring":0}},
        "feature_rollouts":{
            "tool_repair":{
                "enabled":true,
                "source":"env",
                "maturity":"gated_production",
                "owner_component":"run stream/tool repair",
                "public_api_exposure":"operator diagnostics",
                "activation_blockers":["tool repair fixes must remain replay-safe"],
                "required_tests":["cargo test -p palyra-daemon --test current_state_inventory --locked"]
            },
            "networked_workers":{
                "enabled":false,
                "source":"default",
                "maturity":"blocked",
                "owner_component":"workerd/execution backends",
                "public_api_exposure":"runtime preview controls",
                "activation_blockers":["Enable feature_rollouts.execution_backend_networked_worker first"],
                "required_tests":["bash scripts/test/run-critical-attack-scenarios.sh"]
            }
        },
        "feature_rollout_maturity":{
            "schema_version":1,
            "migration_note":"flag renames must add deprecated_aliases before aliases are removed"
        }
    }"#;
    let (daemon_url, server_handle) =
        spawn_doctor_http_server_with_admin_body(admin_token, admin_body.to_owned())?;
    let daemon_port =
        daemon_url.rsplit(':').next().context("doctor test server URL should include a port")?;
    let config_path = workdir.path().join("rollout-maturity-palyra.toml");
    fs::write(
        config_path.as_path(),
        format!(
            r#"
version = 1

[daemon]
bind_addr = "127.0.0.1"
port = {daemon_port}

[admin]
auth_token = "{admin_token}"
"#
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    configure_cli_env(&mut command, &workdir);
    command
        .current_dir(workdir.path())
        .args(["--config", config_path_string.as_str(), "doctor", "--json"])
        .env("PALYRA_DAEMON_URL", daemon_url.as_str());
    let output = command.output().with_context(|| {
        format!("failed to execute palyra --config {} doctor --json", config_path.display())
    })?;
    let server_result = server_handle.join();
    if let Err(panic) = server_result {
        anyhow::bail!(
            "doctor test server thread panicked: {:?}\nstdout={}\nstderr={}",
            panic,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if let Err(error) = server_result.expect("server join result should exist") {
        anyhow::bail!(
            "doctor test server failed: {error:#}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert!(
        output.status.success(),
        "doctor --json should succeed with rollout maturity payload: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/diagnostics/feature_rollouts/fetched").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/diagnostics/feature_rollouts/flag_count").and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(
        payload
            .pointer("/diagnostics/feature_rollouts/maturity_counts/blocked")
            .and_then(Value::as_u64),
        Some(1)
    );
    let inactive = payload
        .pointer("/diagnostics/feature_rollouts/inactive")
        .and_then(Value::as_array)
        .context("doctor feature rollout snapshot should include inactive entries")?;
    assert!(
        inactive.iter().any(|entry| {
            entry.get("flag").and_then(Value::as_str) == Some("networked_workers")
                && entry.get("activation_blockers").and_then(Value::as_array).is_some_and(
                    |blockers| {
                        blockers.iter().any(|blocker| {
                            blocker.as_str().is_some_and(|text| {
                                text.contains("execution_backend_networked_worker")
                            })
                        })
                    },
                )
        }),
        "doctor should explain why networked workers are inactive: {payload}"
    );
    assert_eq!(
        payload.pointer("/diagnostics/feature_rollouts/migration_note").and_then(Value::as_str),
        Some("flag renames must add deprecated_aliases before aliases are removed")
    );
    Ok(())
}

#[test]
fn doctor_browser_error_distinguishes_missing_gateway_diagnostics() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let admin_token = "config-admin-token";
    let (daemon_url, server_handle) = spawn_doctor_http_server(admin_token)?;
    let daemon_port =
        daemon_url.rsplit(':').next().context("doctor test server URL should include a port")?;
    let config_path = workdir.path().join("browser-config-palyra.toml");
    fs::write(
        config_path.as_path(),
        format!(
            r#"
version = 1

[daemon]
bind_addr = "127.0.0.1"
port = {daemon_port}

[admin]
auth_token = "{admin_token}"

[tool_call.browser_service]
enabled = true
endpoint = "http://127.0.0.1:7543"
"#
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;

    let config_path_string = config_path.to_string_lossy().into_owned();
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    configure_cli_env(&mut command, &workdir);
    command
        .current_dir(workdir.path())
        .args(["--config", config_path_string.as_str(), "doctor", "--json"])
        .env("PALYRA_DAEMON_URL", daemon_url.as_str());
    let output = command.output().with_context(|| {
        format!("failed to execute palyra --config {} doctor --json", config_path.display())
    })?;
    let server_result = server_handle.join();
    if let Err(panic) = server_result {
        anyhow::bail!(
            "doctor test server thread panicked: {:?}\nstdout={}\nstderr={}",
            panic,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if let Err(error) = server_result.expect("server join result should exist") {
        anyhow::bail!(
            "doctor test server failed: {error:#}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert!(
        output.status.success(),
        "doctor --json should succeed while reporting browser diagnostics gap: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    let error =
        payload.pointer("/diagnostics/browser/error").and_then(Value::as_str).unwrap_or_default();

    assert_eq!(
        payload.pointer("/diagnostics/browser/configured_enabled").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload.pointer("/diagnostics/browser/diagnostics_fetched").and_then(Value::as_bool),
        Some(false)
    );
    assert!(
        error.contains("admin status payload did not include browserd diagnostics")
            && error.contains("palyra browser status --json"),
        "doctor should explain gateway-vs-direct browser status gap: {payload}"
    );
    Ok(())
}
