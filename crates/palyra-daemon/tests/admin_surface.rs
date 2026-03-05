use std::{
    fs,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const CONSOLE_ADMIN_PRINCIPAL: &str = "admin:web-console";
const CONSOLE_AUDITOR_PRINCIPAL: &str = "admin:web-auditor";
const PALYRAD_STARTUP_ATTEMPTS: usize = 3;
const PALYRAD_STARTUP_RETRY_DELAY: Duration = Duration::from_millis(150);
static TEMP_IDENTITY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn admin_status_requires_token_and_context() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("http://127.0.0.1:{admin_port}/admin/v1/status");

    let missing_auth = client.get(&url).send().context("failed to call admin status")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "missing auth must be rejected");
    assert_admin_console_security_headers(missing_auth.headers())?;

    let invalid_context = client
        .get(&url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", "invalid")
        .send()
        .context("failed to call admin status with invalid context")?;
    assert_eq!(invalid_context.status().as_u16(), 400, "invalid context must be rejected");

    let success_response = client
        .get(&url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin status with valid context")?
        .error_for_status()
        .context("admin status with valid context returned non-success status")?;
    assert_admin_console_security_headers(success_response.headers())?;
    let success = success_response.text().context("failed to read admin status response body")?;

    assert!(success.contains("\"status\":\"ok\""), "expected admin status to be healthy");
    assert!(success.contains("\"admin_auth_required\":true"));
    assert!(success.contains("\"grpc_port\""));
    Ok(())
}

#[test]
fn admin_journal_recent_requires_token_and_returns_snapshot() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("http://127.0.0.1:{admin_port}/admin/v1/journal/recent?limit=5");

    let missing_auth = client.get(&url).send().context("failed to call admin journal recent")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "missing auth must be rejected");

    let response_body = client
        .get(&url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin journal recent with valid context")?
        .error_for_status()
        .context("admin journal recent returned non-success status")?
        .text()
        .context("failed to read admin journal recent response body")?;

    assert!(
        response_body.contains("\"events\":") && response_body.contains("\"total_events\":"),
        "journal snapshot response should include events and total count"
    );
    Ok(())
}

#[test]
fn admin_policy_explain_requires_token_and_returns_decision_payload() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let url = format!(
        "http://127.0.0.1:{admin_port}/admin/v1/policy/explain?principal=user%3Aops&action=tool.execute.shell&resource=tool%3Ashell"
    );

    let missing_auth = client.get(&url).send().context("failed to call admin policy explain")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "missing auth must be rejected");

    let body = client
        .get(&url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin policy explain with valid context")?
        .error_for_status()
        .context("admin policy explain returned non-success status")?
        .text()
        .context("failed to read admin policy explain response body")?;

    assert!(body.contains("\"decision\":"), "policy explain must include decision: {body}");
    assert!(
        body.contains("\"approval_required\":"),
        "policy explain must include approval_required flag: {body}"
    );
    assert!(
        body.contains("\"matched_policies\":"),
        "policy explain must include matched policies: {body}"
    );
    Ok(())
}

#[test]
fn admin_status_bruteforce_attempts_are_rate_limited() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let url = format!("http://127.0.0.1:{admin_port}/admin/v1/status");

    let mut rate_limited_response = None;
    for attempt in 0..200 {
        let response = client
            .get(&url)
            .header("Authorization", "Bearer invalid-admin-token")
            .header("x-palyra-principal", "user:attacker")
            .header("x-palyra-device-id", DEVICE_ID)
            .header("x-palyra-channel", "cli")
            .send()
            .with_context(|| format!("failed to call admin status attempt {attempt}"))?;
        if response.status().as_u16() == 429 {
            rate_limited_response = Some(response);
            break;
        }
        assert_eq!(
            response.status().as_u16(),
            401,
            "invalid token should return unauthorized until rate-limit threshold is reached"
        );
    }

    let rate_limited_response = rate_limited_response.ok_or_else(|| {
        anyhow::anyhow!(
            "expected repeated invalid-token attempts to trigger HTTP 429 rate limiting"
        )
    })?;
    assert_admin_console_security_headers(rate_limited_response.headers())?;
    Ok(())
}

#[test]
fn admin_run_endpoints_require_token_and_report_not_found_for_unknown_run() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let status_url = format!("http://127.0.0.1:{admin_port}/admin/v1/runs/{RUN_ID}");
    let cancel_url = format!("http://127.0.0.1:{admin_port}/admin/v1/runs/{RUN_ID}/cancel");

    let missing_auth =
        client.get(&status_url).send().context("failed to call admin run status without auth")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "missing auth must be rejected");
    assert_admin_console_security_headers(missing_auth.headers())?;

    let unknown_run = client
        .get(&status_url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .send()
        .context("failed to call admin run status with auth")?;
    assert_eq!(unknown_run.status().as_u16(), 404, "unknown run should return not found");

    let unknown_cancel = client
        .post(&cancel_url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .send()
        .context("failed to call admin run cancel with auth")?;
    assert_eq!(unknown_cancel.status().as_u16(), 404, "unknown run cancel should return not found");
    Ok(())
}

#[test]
fn admin_skill_quarantine_and_enable_require_override_acknowledgement() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let quarantine_url =
        format!("http://127.0.0.1:{admin_port}/admin/v1/skills/Acme.Echo_Http/quarantine");
    let enable_url = format!("http://127.0.0.1:{admin_port}/admin/v1/skills/Acme.Echo_Http/enable");

    let missing_auth = client
        .post(&quarantine_url)
        .json(&serde_json::json!({
            "version": "1.2.3",
            "reason": "security hold",
        }))
        .send()
        .context("failed to call quarantine endpoint without auth")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "missing auth must be rejected");

    let quarantine_response = client
        .post(&quarantine_url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:security")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({
            "version": "1.2.3",
            "reason": "security hold",
        }))
        .send()
        .context("failed to call quarantine endpoint with auth")?
        .error_for_status()
        .context("quarantine endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse quarantine JSON response")?;
    assert_eq!(
        quarantine_response.get("status").and_then(Value::as_str),
        Some("quarantined"),
        "quarantine endpoint should set quarantined status"
    );
    assert_eq!(
        quarantine_response.get("version").and_then(Value::as_str),
        Some("1.2.3"),
        "quarantine endpoint should preserve requested version"
    );
    assert_eq!(
        quarantine_response.get("skill_id").and_then(Value::as_str),
        Some("acme.echo_http"),
        "quarantine endpoint should canonicalize skill_id to lowercase"
    );

    let missing_override = client
        .post(&enable_url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:security")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({
            "version": "1.2.3",
            "reason": "operator review complete",
        }))
        .send()
        .context("failed to call enable endpoint without override")?;
    assert_eq!(
        missing_override.status().as_u16(),
        400,
        "enable endpoint must reject missing override acknowledgment"
    );

    let enable_response = client
        .post(&enable_url)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:security")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({
            "version": "1.2.3",
            "reason": "operator review complete",
            "override": true,
        }))
        .send()
        .context("failed to call enable endpoint with override")?
        .error_for_status()
        .context("enable endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse enable JSON response")?;
    assert_eq!(
        enable_response.get("status").and_then(Value::as_str),
        Some("active"),
        "enable endpoint should restore active status"
    );
    assert_eq!(
        enable_response.get("skill_id").and_then(Value::as_str),
        Some("acme.echo_http"),
        "enable endpoint should canonicalize skill_id to lowercase"
    );

    Ok(())
}

#[test]
fn console_session_and_csrf_guards_are_enforced() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let rejected_login = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/login"))
        .json(&serde_json::json!({
            "admin_token": ADMIN_TOKEN,
            "principal": "user:ops",
            "device_id": DEVICE_ID,
            "channel": "web",
        }))
        .send()
        .context("failed to call console login with non-admin principal")?;
    assert_eq!(
        rejected_login.status().as_u16(),
        403,
        "console login should reject non-admin principal"
    );

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let session_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/auth/session"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call console session endpoint")?
        .error_for_status()
        .context("console session endpoint returned non-success status")?;
    assert_admin_console_security_headers(session_response.headers())?;
    let refreshed_cookie = header_value(session_response.headers(), "set-cookie")?;
    assert!(
        refreshed_cookie.starts_with(cookie.as_str()) && refreshed_cookie.contains("Max-Age=1800"),
        "session endpoint should refresh the session cookie Max-Age"
    );
    let session_response =
        session_response.text().context("failed to read console session response body")?;
    assert!(
        session_response.contains(CONSOLE_ADMIN_PRINCIPAL),
        "session payload should include authenticated principal"
    );

    let logout_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/logout"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call console logout without csrf")?;
    assert_eq!(
        logout_without_csrf.status().as_u16(),
        403,
        "console logout should reject missing csrf token"
    );
    assert_admin_console_security_headers(logout_without_csrf.headers())?;

    let logout_with_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/logout"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .send()
        .context("failed to call console logout with csrf")?;
    assert_eq!(
        logout_with_csrf.status().as_u16(),
        200,
        "console logout should succeed with valid csrf token"
    );
    Ok(())
}

#[test]
fn console_login_requires_bound_principal_when_auth_is_enabled() -> Result<()> {
    let (child, admin_port) =
        spawn_palyrad_with_dynamic_ports_with_env(&[("PALYRA_ADMIN_BOUND_PRINCIPAL", "")])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/login"))
        .json(&serde_json::json!({
            "admin_token": ADMIN_TOKEN,
            "principal": CONSOLE_ADMIN_PRINCIPAL,
            "device_id": DEVICE_ID,
            "channel": "web",
        }))
        .send()
        .context("failed to call console login without bound principal")?;
    assert_eq!(
        response.status().as_u16(),
        412,
        "console login should fail closed when auth is enabled without bound principal"
    );
    let body = response.text().context("failed to read console login error response body")?;
    assert!(
        body.contains("admin.bound_principal"),
        "console login error should explain missing bound principal requirement"
    );

    Ok(())
}

#[test]
fn console_login_uses_configured_bound_principal() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal("admin:bound-console")?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/login"))
        .json(&serde_json::json!({
            "admin_token": ADMIN_TOKEN,
            "principal": CONSOLE_AUDITOR_PRINCIPAL,
            "device_id": DEVICE_ID,
            "channel": "web",
        }))
        .send()
        .context("failed to call console login with bound principal")?
        .error_for_status()
        .context("console login with bound principal returned non-success status")?
        .json::<Value>()
        .context("failed to parse console login response json")?;
    assert_eq!(
        response.get("principal").and_then(Value::as_str),
        Some("admin:bound-console"),
        "console login should ignore caller-selected principal when bound principal is configured"
    );

    Ok(())
}

#[test]
fn console_approvals_flow_requires_session_and_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let no_session = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/approvals"))
        .send()
        .context("failed to call console approvals without session")?;
    assert_eq!(no_session.status().as_u16(), 403, "approvals endpoint must require session");

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let approvals_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/approvals"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call console approvals endpoint")?
        .error_for_status()
        .context("console approvals endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse approvals response json")?;
    assert!(
        approvals_response.get("approvals").and_then(Value::as_array).is_some(),
        "approvals list response should include approvals array"
    );

    let unknown_approval_id = "01ARZ3NDEKTSV4RRFFQ69G5FBA";
    let missing_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/approvals/{unknown_approval_id}/decision"
        ))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({
            "approved": true,
            "decision_scope": "once",
            "reason": "operator approve",
        }))
        .send()
        .context("failed to call approval decision without csrf token")?;
    assert_eq!(missing_csrf.status().as_u16(), 403, "decision endpoint must enforce csrf token");

    let decision_response = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/approvals/{unknown_approval_id}/decision"
        ))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "approved": true,
            "decision_scope": "once",
            "reason": "operator approve",
        }))
        .send()
        .context("failed to call approval decision endpoint")?;
    assert_eq!(
        decision_response.status().as_u16(),
        404,
        "decision endpoint should report not-found for unknown approval id"
    );
    Ok(())
}

#[test]
fn console_chat_endpoints_require_session_and_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let no_session = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/chat/sessions"))
        .send()
        .context("failed to call chat sessions endpoint without session")?;
    assert_eq!(no_session.status().as_u16(), 403, "chat sessions endpoint must require session");

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let sessions_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/chat/sessions"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list chat sessions with session cookie")?
        .error_for_status()
        .context("chat sessions endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse chat sessions response json")?;
    assert!(
        sessions_response.get("sessions").and_then(Value::as_array).is_some(),
        "chat sessions response should include sessions array"
    );

    let create_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/chat/sessions"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({}))
        .send()
        .context("failed to create chat session without csrf token")?;
    assert_eq!(
        create_without_csrf.status().as_u16(),
        403,
        "chat session create endpoint must enforce csrf token"
    );

    client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/chat/sessions"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({}))
        .send()
        .context("failed to create chat session with csrf token")?
        .error_for_status()
        .context("chat session create endpoint returned non-success status")?;

    let session_route_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    let rename_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/chat/sessions/{session_route_id}/rename"
        ))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({
            "session_label": "renamed",
        }))
        .send()
        .context("failed to rename chat session without csrf token")?;
    assert_eq!(
        rename_without_csrf.status().as_u16(),
        403,
        "chat session rename endpoint must enforce csrf token"
    );

    let reset_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/chat/sessions/{session_route_id}/reset"
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to reset chat session without csrf token")?;
    assert_eq!(
        reset_without_csrf.status().as_u16(),
        403,
        "chat session reset endpoint must enforce csrf token"
    );

    let stream_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/chat/sessions/{session_route_id}/messages/stream"
        ))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({
            "text": "hello",
        }))
        .send()
        .context("failed to call chat stream endpoint without csrf token")?;
    assert_eq!(
        stream_without_csrf.status().as_u16(),
        403,
        "chat stream endpoint must enforce csrf token"
    );

    let unknown_run_id = "01ARZ3NDEKTSV4RRFFQ69G5FB9";
    let run_status = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/chat/runs/{unknown_run_id}/status"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch unknown chat run status")?;
    assert_eq!(
        run_status.status().as_u16(),
        404,
        "unknown chat run status should return not-found"
    );

    let run_events = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/chat/runs/{unknown_run_id}/events"))
        .header("Cookie", cookie)
        .send()
        .context("failed to fetch unknown chat run events")?;
    assert_eq!(
        run_events.status().as_u16(),
        404,
        "unknown chat run events should return not-found"
    );

    Ok(())
}

#[test]
fn console_cron_workflow_create_disable_and_list_runs() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let created_job = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "name": "web-console-job",
            "prompt": "echo from web console",
            "schedule_type": "every",
            "every_interval_ms": 60000,
            "enabled": true,
            "channel": "web",
        }))
        .send()
        .context("failed to create cron job from console endpoint")?
        .error_for_status()
        .context("console cron create returned non-success status")?
        .json::<Value>()
        .context("failed to parse console cron create response json")?;
    let job_id = created_job
        .get("job")
        .and_then(|job| job.get("job_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("console cron create response did not include job.job_id"))?
        .to_owned();

    let jobs_list = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list cron jobs from console endpoint")?
        .error_for_status()
        .context("console cron list returned non-success status")?
        .json::<Value>()
        .context("failed to parse console cron list response json")?;
    let jobs = jobs_list
        .get("jobs")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("console cron list response missing jobs array"))?;
    assert!(
        jobs.iter().any(|job| job.get("job_id").and_then(Value::as_str) == Some(job_id.as_str())),
        "created cron job should appear in list response"
    );

    let disable_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/enabled"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .context("failed to disable cron job from console endpoint")?
        .error_for_status()
        .context("console cron disable returned non-success status")?
        .json::<Value>()
        .context("failed to parse console cron disable response json")?;
    assert_eq!(
        disable_response.get("job").and_then(|job| job.get("enabled")).and_then(Value::as_bool),
        Some(false),
        "console cron disable response should set enabled=false"
    );

    let runs_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/runs"))
        .header("Cookie", cookie)
        .send()
        .context("failed to fetch cron runs from console endpoint")?
        .error_for_status()
        .context("console cron runs endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse console cron runs response json")?;
    assert!(
        runs_response.get("runs").and_then(Value::as_array).is_some(),
        "console cron runs response should include runs array"
    );
    Ok(())
}

#[test]
fn console_cron_endpoints_enforce_owner_principal_boundaries() -> Result<()> {
    let (child, admin_port) =
        spawn_palyrad_with_dynamic_ports_with_env(&[("PALYRA_ADMIN_REQUIRE_AUTH", "false")])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let (owner_cookie, owner_csrf_token) =
        login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let (auditor_cookie, auditor_csrf_token) =
        login_console_session(&client, admin_port, CONSOLE_AUDITOR_PRINCIPAL)?;

    let created_job = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs"))
        .header("Cookie", owner_cookie.clone())
        .header("x-palyra-csrf-token", owner_csrf_token.clone())
        .json(&serde_json::json!({
            "name": "owner-boundary-job",
            "prompt": "owner boundary validation",
            "schedule_type": "every",
            "every_interval_ms": 60000,
            "enabled": true,
            "channel": "web",
        }))
        .send()
        .context("failed to create owner-boundary cron job from console endpoint")?
        .error_for_status()
        .context("owner-boundary console cron create returned non-success status")?
        .json::<Value>()
        .context("failed to parse owner-boundary console cron create response json")?;
    let job_id = created_job
        .get("job")
        .and_then(|job| job.get("job_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("owner-boundary create response missing job.job_id"))?
        .to_owned();

    let forbidden_disable = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/enabled"))
        .header("Cookie", auditor_cookie.clone())
        .header("x-palyra-csrf-token", auditor_csrf_token)
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .context("failed to disable owner-boundary cron job as non-owner principal")?;
    assert_eq!(
        forbidden_disable.status().as_u16(),
        403,
        "console cron enabled endpoint must reject non-owner principal"
    );

    let forbidden_runs = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/runs"))
        .header("Cookie", auditor_cookie)
        .send()
        .context("failed to list owner-boundary cron runs as non-owner principal")?;
    assert_eq!(
        forbidden_runs.status().as_u16(),
        403,
        "console cron runs endpoint must reject non-owner principal"
    );

    let owner_disable = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/enabled"))
        .header("Cookie", owner_cookie.clone())
        .header("x-palyra-csrf-token", owner_csrf_token)
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .context("failed to disable owner-boundary cron job as owner principal")?
        .error_for_status()
        .context("owner principal cron disable returned non-success status")?
        .json::<Value>()
        .context("failed to parse owner-boundary cron disable response json")?;
    assert_eq!(
        owner_disable.get("job").and_then(|job| job.get("enabled")).and_then(Value::as_bool),
        Some(false),
        "owner principal should be able to disable owned cron job"
    );

    let owner_runs = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/cron/jobs/{job_id}/runs"))
        .header("Cookie", owner_cookie)
        .send()
        .context("failed to list owner-boundary cron runs as owner principal")?
        .error_for_status()
        .context("owner principal cron runs returned non-success status")?
        .json::<Value>()
        .context("failed to parse owner-boundary cron runs response json")?;
    assert!(
        owner_runs.get("runs").and_then(Value::as_array).is_some(),
        "owner principal should retain access to cron runs endpoint"
    );

    Ok(())
}

#[test]
fn console_browser_relay_action_rejects_body_token_without_authorization_header() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/browser/relay/actions"))
        .json(&serde_json::json!({
            "relay_token": "relay-token-from-body",
            "session_id": DEVICE_ID,
            "extension_id": "com.palyra.extension",
            "action": "capture_selection",
            "capture_selection": {
                "selector": "body",
                "max_selection_bytes": 128
            }
        }))
        .send()
        .context("failed to call relay action endpoint without authorization header")?;
    assert_eq!(
        response.status().as_u16(),
        403,
        "relay action endpoint must require authorization header token"
    );
    let body = response.text().context("failed to read relay action error response body")?;
    assert!(
        !body.contains("relay-token-from-body"),
        "relay action denial must not echo relay token from request body"
    );

    Ok(())
}

#[test]
fn console_channels_endpoints_require_session_and_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let connector_id = "echo:default";

    let no_session = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/channels"))
        .send()
        .context("failed to call channels list endpoint without session")?;
    assert_eq!(no_session.status().as_u16(), 403, "channels list must require session");

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let list_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/channels"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call channels list endpoint")?
        .error_for_status()
        .context("channels list endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse channels list response json")?;
    let connectors = list_response
        .get("connectors")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("channels list response missing connectors array"))?;
    assert!(
        connectors.iter().any(|entry| {
            entry.get("connector_id").and_then(Value::as_str) == Some(connector_id)
        }),
        "channels list response should include {connector_id}"
    );

    let missing_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/enabled"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({ "enabled": true }))
        .send()
        .context("failed to call channels enable endpoint without csrf token")?;
    assert_eq!(
        missing_csrf.status().as_u16(),
        403,
        "channels enable endpoint must enforce csrf token"
    );

    let enabled_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/enabled"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({ "enabled": true }))
        .send()
        .context("failed to call channels enable endpoint with csrf token")?
        .error_for_status()
        .context("channels enable endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse channels enable response json")?;
    assert_eq!(
        enabled_response
            .get("connector")
            .and_then(|connector| connector.get("connector_id"))
            .and_then(Value::as_str),
        Some(connector_id),
        "channels enable response should include connector payload"
    );

    let logs_response = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/logs?limit=5"
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call channels logs endpoint")?
        .error_for_status()
        .context("channels logs endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse channels logs response json")?;
    assert!(
        logs_response.get("events").and_then(Value::as_array).is_some(),
        "channels logs response should include events array"
    );
    assert!(
        logs_response.get("dead_letters").and_then(Value::as_array).is_some(),
        "channels logs response should include dead_letters array"
    );

    let test_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/test"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({ "text": "hello from console test" }))
        .send()
        .context("failed to call channels test endpoint without csrf token")?;
    assert_eq!(
        test_without_csrf.status().as_u16(),
        403,
        "channels test endpoint must enforce csrf token"
    );

    let test_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/test"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "text": "hello from console test",
            "conversation_id": "test:conversation",
            "sender_id": "test-user",
            "is_direct_message": true,
            "requested_broadcast": false,
        }))
        .send()
        .context("failed to call channels test endpoint with csrf token")?;
    assert_eq!(
        test_response.status().as_u16(),
        412,
        "channels test endpoint should fail with failed-precondition when connector token is missing"
    );
    let test_response = test_response
        .json::<Value>()
        .context("failed to parse channels test error response json")?;
    assert!(
        test_response
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|value| value.contains("connector_token is required")),
        "channels test error response should explain missing connector token requirement"
    );

    let discord_connector_id = "discord:default";
    let discord_test_send_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/{discord_connector_id}/test-send"
        ))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({
            "target": "channel:1234567890",
            "confirm": true,
        }))
        .send()
        .context("failed to call channels discord test-send endpoint without csrf token")?;
    assert_eq!(
        discord_test_send_without_csrf.status().as_u16(),
        403,
        "channels discord test-send endpoint must enforce csrf token"
    );

    let discord_test_send_response = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/{discord_connector_id}/test-send"
        ))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "target": "channel:1234567890",
            "text": "hello discord",
            "confirm": true,
        }))
        .send()
        .context("failed to call channels discord test-send endpoint with csrf token")?
        .error_for_status()
        .context("channels discord test-send endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse channels discord test-send response json")?;
    assert!(
        discord_test_send_response.get("dispatch").is_some(),
        "channels discord test-send response should include dispatch payload"
    );
    assert!(
        discord_test_send_response.get("status").is_some(),
        "channels discord test-send response should include status payload"
    );

    Ok(())
}

#[test]
fn console_memory_purge_requires_session_and_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let no_session = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/memory/purge"))
        .json(&serde_json::json!({ "purge_all_principal": true }))
        .send()
        .context("failed to call memory purge endpoint without session")?;
    assert_eq!(no_session.status().as_u16(), 403, "memory purge endpoint must require session");

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let missing_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/memory/purge"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({ "purge_all_principal": true }))
        .send()
        .context("failed to call memory purge endpoint without csrf token")?;
    assert_eq!(
        missing_csrf.status().as_u16(),
        403,
        "memory purge endpoint must enforce csrf token"
    );

    let purge_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/memory/purge"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({ "purge_all_principal": true }))
        .send()
        .context("failed to call memory purge endpoint with csrf token")?
        .error_for_status()
        .context("memory purge endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse memory purge response json")?;
    assert!(
        purge_response.get("deleted_count").and_then(Value::as_u64).is_some(),
        "memory purge response should include deleted_count"
    );

    Ok(())
}

#[test]
fn console_login_rejects_oversized_request_body() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let oversized_principal = format!("admin:{}", "a".repeat(80 * 1024));

    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/login"))
        .json(&serde_json::json!({
            "admin_token": ADMIN_TOKEN,
            "principal": oversized_principal,
            "device_id": DEVICE_ID,
            "channel": "web",
        }))
        .send()
        .context("failed to call console login with oversized request body")?;
    assert_eq!(
        response.status().as_u16(),
        413,
        "console login should reject oversized request bodies with payload-too-large status"
    );

    Ok(())
}

#[test]
fn admin_run_cancel_rejects_oversized_request_body() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let oversized_reason = "a".repeat(80 * 1024);

    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/admin/v1/runs/{RUN_ID}/cancel"))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "admin:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({ "reason": oversized_reason }))
        .send()
        .context("failed to call admin run cancel with oversized request body")?;
    assert_eq!(
        response.status().as_u16(),
        413,
        "admin run cancel should reject oversized request bodies with payload-too-large status"
    );

    Ok(())
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16)> {
    spawn_palyrad_with_dynamic_ports_with_env(&[])
}

fn spawn_palyrad_with_bound_console_principal(principal: &str) -> Result<(Child, u16)> {
    spawn_palyrad_with_dynamic_ports_with_env(&[("PALYRA_ADMIN_BOUND_PRINCIPAL", principal)])
}

fn spawn_palyrad_with_dynamic_ports_with_env(extra_env: &[(&str, &str)]) -> Result<(Child, u16)> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=PALYRAD_STARTUP_ATTEMPTS {
        match spawn_palyrad_with_dynamic_ports_once(extra_env) {
            Ok(started) => return Ok(started),
            Err(error) => {
                last_error = Some(error);
                if attempt < PALYRAD_STARTUP_ATTEMPTS {
                    thread::sleep(PALYRAD_STARTUP_RETRY_DELAY);
                }
            }
        }
    }
    let Some(last_error) = last_error else {
        anyhow::bail!("failed to spawn palyrad for admin surface tests");
    };
    Err(last_error).context(format!(
        "failed to spawn palyrad after {PALYRAD_STARTUP_ATTEMPTS} startup attempts"
    ))
}

fn login_console_session(
    client: &Client,
    admin_port: u16,
    principal: &str,
) -> Result<(String, String)> {
    let response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/login"))
        .json(&serde_json::json!({
            "admin_token": ADMIN_TOKEN,
            "principal": principal,
            "device_id": DEVICE_ID,
            "channel": "web",
        }))
        .send()
        .context("failed to call console login")?
        .error_for_status()
        .context("console login returned non-success status")?;
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("console login response missing set-cookie header"))?
        .to_owned();
    let cookie = set_cookie
        .split(';')
        .next()
        .ok_or_else(|| anyhow::anyhow!("console set-cookie header missing cookie pair"))?
        .to_owned();
    let body = response.json::<Value>().context("failed to parse console login response json")?;
    let csrf_token = body
        .get("csrf_token")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("console login response missing csrf_token"))?
        .to_owned();
    Ok((cookie, csrf_token))
}

fn assert_admin_console_security_headers(headers: &reqwest::header::HeaderMap) -> Result<()> {
    assert_eq!(
        header_value(headers, "cache-control")?,
        "no-store",
        "admin/console responses must disable cache persistence"
    );
    assert_eq!(
        header_value(headers, "x-content-type-options")?,
        "nosniff",
        "admin/console responses must set X-Content-Type-Options=nosniff"
    );
    assert_eq!(
        header_value(headers, "x-frame-options")?,
        "DENY",
        "admin/console responses must deny framing"
    );
    assert_eq!(
        header_value(headers, "referrer-policy")?,
        "no-referrer",
        "admin/console responses must not leak referrer values"
    );
    Ok(())
}

fn header_value(headers: &reqwest::header::HeaderMap, name: &str) -> Result<String> {
    headers
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing expected response header {name}"))?
        .to_str()
        .with_context(|| format!("header {name} contains invalid UTF-8")) // security headers must be simple ASCII directives
        .map(ToOwned::to_owned)
}

fn spawn_palyrad_with_dynamic_ports_once(extra_env: &[(&str, &str)]) -> Result<(Child, u16)> {
    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = unique_temp_identity_store_dir();
    let vault_dir = unique_temp_vault_dir();
    prepare_test_vault_dir(&vault_dir)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyrad"));
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
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
        .env("PALYRA_GATEWAY_QUIC_PORT", "0")
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let mut child = command.spawn().context("failed to start palyrad")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let admin_port = match wait_for_admin_port(stdout, &mut child) {
        Ok(port) => port,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error).context("failed to capture palyrad admin listen port");
        }
    };
    Ok((child, admin_port))
}

fn unique_temp_journal_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-surface-{nonce}-{}-{counter}.sqlite3", std::process::id()))
}

fn unique_temp_identity_store_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-identity-{nonce}-{}-{counter}", std::process::id()))
}

fn unique_temp_vault_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-vault-{nonce}-{}-{counter}", std::process::id()))
}

fn prepare_test_vault_dir(vault_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(vault_dir)
        .with_context(|| format!("failed to create test vault dir {}", vault_dir.display()))?;
    let backend_marker = vault_dir.join("backend.kind");
    fs::write(&backend_marker, b"encrypted_file").with_context(|| {
        format!("failed to write vault backend marker {}", backend_marker.display())
    })?;
    Ok(())
}

fn wait_for_admin_port(stdout: ChildStdout, daemon: &mut Child) -> Result<u16> {
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
            if let Some(port) = parse_port_from_log(&line, "\"listen_addr\":\"") {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok(port));
                }
                return;
            }
        }
        if let Some(sender) = sender.take() {
            let _ =
                sender
                    .send(Err("palyrad stdout closed before admin listen address was published"
                        .to_owned()));
        }
    });

    let timeout_at = Instant::now() + Duration::from_secs(10);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(port)) => return Ok(port),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("admin listen-address reader disconnected before publishing a port");
            }
        }

        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad admin listen address log");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!(
                "palyrad exited before publishing admin listen address with status: {status}"
            );
        }
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
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
