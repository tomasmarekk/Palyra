//! Black-box ACP live-runtime conformance through the real console HTTP route.
//!
//! The deterministic managed child proves production config and stdio wiring.

#[allow(dead_code)]
mod support;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use support::{ConsoleSession, DaemonHarness, CONSOLE_ADMIN_PRINCIPAL, DEVICE_ID};

#[test]
fn selected_acp_backend_executes_through_managed_child() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_palyra-managed-runtime-fixture");
    let cwd = std::path::Path::new(executable)
        .parent()
        .context("managed runtime fixture must have a parent directory")?
        .to_string_lossy()
        .into_owned();
    let digest = "a".repeat(64);
    let harness = DaemonHarness::spawn(&[
        ("PALYRA_EXPERIMENTAL_ACP_RUNTIME", "true"),
        ("PALYRA_ACP_RUNTIME_BACKEND_ID", "fixture"),
        ("PALYRA_ACP_RUNTIME_EXECUTABLE", executable),
        ("PALYRA_ACP_RUNTIME_CWD", cwd.as_str()),
        ("PALYRA_ACP_RUNTIME_PROTOCOL_VERSION", "acp.fixture.v1"),
        ("PALYRA_ACP_RUNTIME_CAPABILITY_SHA256", digest.as_str()),
    ])?;
    let session = harness.login_as_admin()?;
    let client = json!({
        "protocol_version": 1,
        "client_id": "acp-live-test",
        "transport": "http",
        "owner_principal": CONSOLE_ADMIN_PRINCIPAL,
        "device_id": DEVICE_ID,
        "channel": "web",
        "scopes": ["sessions:read", "sessions:write", "runs:read", "runs:write"],
        "capabilities": ["session_new", "run_control", "runtime_status"],
    });

    let created = post_acp(
        &harness,
        &session,
        &client,
        "request-session",
        "session.new",
        json!({
            "acp_session_id": "live-session",
            "config": { "runtime_backend": "fixture" },
        }),
    )?;
    assert!(created["ok"].as_bool().unwrap_or(false), "session.new failed: {created}");
    let binding_id = created
        .pointer("/result/binding/binding_id")
        .and_then(Value::as_str)
        .context("session.new must return the live binding id")?;

    let mut foreign_client = client.clone();
    foreign_client["client_id"] = Value::String("acp-live-foreign".to_owned());
    let denied = post_acp(
        &harness,
        &session,
        &foreign_client,
        "request-foreign-run",
        "run.create",
        json!({
            "binding_id": binding_id,
            "run_id": "foreign-live-run",
            "prompt": "must not reach the managed child",
        }),
    )?;
    assert_eq!(denied["ok"], Value::Bool(false));
    assert_eq!(
        denied.pointer("/error/code").and_then(Value::as_str),
        Some("acp/permission_denied")
    );

    let executed = post_acp(
        &harness,
        &session,
        &client,
        "request-run",
        "run.create",
        json!({
            "acp_session_id": "live-session",
            "run_id": "live-run",
            "prompt": "deterministic conformance prompt",
        }),
    )?;
    assert!(executed["ok"].as_bool().unwrap_or(false), "run.create failed: {executed}");
    assert_eq!(
        executed.pointer("/result/runtime/terminal/final_message").and_then(Value::as_str),
        Some("fixture complete")
    );
    assert_eq!(
        executed.pointer("/result/runtime/backend_id").and_then(Value::as_str),
        Some("fixture")
    );

    let status = harness.console_json("/console/v1/acp/status", &session)?;
    assert_eq!(
        status.pointer("/live_runtime/handles/0/state").and_then(Value::as_str),
        Some("ready")
    );
    assert!(status
        .pointer("/live_runtime/handles/0/process_lease_sha256")
        .and_then(Value::as_str)
        .is_some_and(|value| value.len() == 64));
    Ok(())
}

fn post_acp(
    harness: &DaemonHarness,
    session: &ConsoleSession,
    client: &Value,
    request_id: &str,
    command: &str,
    params: Value,
) -> Result<Value> {
    harness
        .client
        .post(format!("http://127.0.0.1:{}/console/v1/acp/command", harness.admin_port))
        .header("Cookie", session.cookie.as_str())
        .header("x-palyra-csrf-token", session._csrf_token.as_str())
        .json(&json!({
            "client": client,
            "command": {
                "request_id": request_id,
                "command": command,
                "params": params,
            },
        }))
        .send()
        .context("failed to call ACP command route")?
        .error_for_status()
        .context("ACP command route returned non-success status")?
        .json()
        .context("failed to parse ACP command response")
}
