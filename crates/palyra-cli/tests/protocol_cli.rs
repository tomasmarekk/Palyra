//! Pins `palyra protocol` runtime introspection against a stub admin endpoint.
//!
//! The method registry command is intentionally tested at the binary boundary
//! because it depends on root-context connection resolution and admin headers.

use std::{
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
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"))
        .env("PROGRAMDATA", workdir.path().join("programdata"));
}

fn run_cli(workdir: &TempDir, args: &[&str], env: &[(&str, &str)]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command.current_dir(workdir.path()).args(args);
    configure_cli_env(&mut command, workdir);
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

fn spawn_protocol_methods_server(
    admin_token: &str,
) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind protocol test server")?;
    let address = listener.local_addr().context("failed to read protocol test server address")?;
    listener.set_nonblocking(true).context("failed to mark protocol server non-blocking")?;
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
                Err(error) => return Err(error).context("failed to accept protocol test request"),
            };
            stream
                .set_nonblocking(false)
                .context("failed to restore blocking mode for protocol request")?;
            let mut buffer = [0_u8; 4096];
            let bytes_read =
                stream.read(&mut buffer).context("failed to read protocol test request")?;
            let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            let request_lower = request.to_ascii_lowercase();
            anyhow::ensure!(
                request.starts_with("GET /admin/v1/methods "),
                "protocol methods should request /admin/v1/methods: {request}"
            );
            anyhow::ensure!(
                request_lower.contains(expected_auth.as_str()),
                "protocol methods should send configured admin bearer token: {request}"
            );
            let body = r#"{"schema_version":1,"registry_version":"method-registry.v1","methods":[{"surface":"admin","route":"/admin/v1/status","http_method":"GET","method_name":"admin.get.admin.v1.status","stability":"stable","required_scope":"admin.read","request_schema_id":"admin.get.admin.v1.status.request.method-registry.v1","request_schema_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","response_schema_id":"admin.get.admin.v1.status.response.method-registry.v1","response_schema_hash":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","streaming_supported":false,"idempotency_supported":true}],"scopes":[{"scope":"admin.read","category":"admin","description":"Authenticated read-only daemon administration.","grants":[]}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).context("failed to write protocol response")?;
            return Ok(());
        }
        anyhow::bail!("protocol methods did not request /admin/v1/methods within 60s")
    });
    Ok((format!("http://{address}"), handle))
}

#[test]
fn protocol_methods_json_fetches_runtime_registry() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let admin_token = "test-admin-token";
    let (base_url, server) = spawn_protocol_methods_server(admin_token)?;

    let output = run_cli(
        &workdir,
        &["protocol", "methods", "--json"],
        &[("PALYRA_DAEMON_URL", base_url.as_str()), ("PALYRA_ADMIN_TOKEN", admin_token)],
    )?;

    server.join().expect("protocol server thread should not panic")?;
    assert!(
        output.status.success(),
        "protocol methods --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("registry_version").and_then(Value::as_str), Some("method-registry.v1"));
    assert_eq!(
        payload.pointer("/methods/0/route").and_then(Value::as_str),
        Some("/admin/v1/status")
    );
    assert_eq!(payload.pointer("/scopes/0/scope").and_then(Value::as_str), Some("admin.read"));
    Ok(())
}
