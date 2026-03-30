use std::{
    fs,
    io::Read,
    net::TcpListener,
    path::PathBuf,
    process::{Child, ChildStderr, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use palyra_skills::{build_signed_skill_artifact, ArtifactFile, SkillArtifactBuildRequest};
use reqwest::blocking::Client;
use reqwest::Url;
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const CONSOLE_ADMIN_PRINCIPAL: &str = "admin:web-console";
const CONSOLE_AUDITOR_PRINCIPAL: &str = "admin:web-auditor";
const PALYRAD_STARTUP_ATTEMPTS: usize = 3;
const PALYRAD_STARTUP_RETRY_DELAY: Duration = Duration::from_millis(150);
const PALYRAD_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
static TEMP_IDENTITY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn admin_status_requires_token_and_context() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
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
        refreshed_cookie.starts_with(cookie.as_str())
            && refreshed_cookie.contains("Max-Age=1800")
            && refreshed_cookie.contains("SameSite=Lax"),
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
fn console_browser_handoff_bootstraps_a_browser_session() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let handoff_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/browser-handoff"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "redirect_path": "/#/control/overview"
        }))
        .send()
        .context("failed to create console browser handoff")?
        .error_for_status()
        .context("console browser handoff endpoint returned non-success status")?;
    assert_admin_console_security_headers(handoff_response.headers())?;
    let handoff = handoff_response
        .json::<Value>()
        .context("failed to parse console browser handoff response json")?;
    let handoff_url = handoff
        .get("handoff_url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("browser handoff response missing handoff_url"))?;
    let consume_client = Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build browser handoff consume client")?;
    let bootstrap_response = consume_client
        .get(handoff_url)
        .send()
        .context("failed to consume console browser handoff")?;
    assert_eq!(
        bootstrap_response.status().as_u16(),
        303,
        "console browser handoff should redirect into the dashboard"
    );
    assert_admin_console_security_headers(bootstrap_response.headers())?;
    let handoff_token = Url::parse(handoff_url)
        .context("failed to parse handoff_url")?
        .query_pairs()
        .find_map(|(key, value)| (key == "token").then(|| value.into_owned()))
        .ok_or_else(|| anyhow::anyhow!("browser handoff_url missing token query parameter"))?;
    assert_eq!(
        header_value(bootstrap_response.headers(), "location")?,
        format!("/?desktop_handoff_token={handoff_token}#/control/overview"),
        "browser handoff should redirect into the web bootstrap route"
    );
    assert!(
        bootstrap_response.headers().get("set-cookie").is_none(),
        "browser handoff redirect should not try to set the session cookie directly"
    );

    let session_bootstrap_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/browser-handoff/session"))
        .json(&serde_json::json!({ "token": handoff_token }))
        .send()
        .context("failed to consume console browser handoff through session endpoint")?
        .error_for_status()
        .context("console browser handoff session endpoint returned non-success status")?;
    assert_admin_console_security_headers(session_bootstrap_response.headers())?;
    let handoff_cookie = header_value(session_bootstrap_response.headers(), "set-cookie")?
        .split(';')
        .next()
        .ok_or_else(|| anyhow::anyhow!("browser handoff set-cookie header missing cookie pair"))?
        .to_owned();
    let handoff_set_cookie = header_value(session_bootstrap_response.headers(), "set-cookie")?;
    assert!(
        handoff_set_cookie.contains("SameSite=Lax"),
        "browser handoff session bootstrap should issue a top-level navigation compatible session cookie"
    );

    let session_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/auth/session"))
        .header("Cookie", handoff_cookie)
        .send()
        .context("failed to verify browser handoff session")?
        .error_for_status()
        .context("browser handoff session did not authorize console auth/session")?;
    assert_admin_console_security_headers(session_response.headers())?;

    Ok(())
}

#[test]
fn console_browser_handoff_session_endpoint_bootstraps_a_browser_session() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let handoff_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/browser-handoff"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "redirect_path": "/#/control/overview"
        }))
        .send()
        .context("failed to create console browser handoff")?
        .error_for_status()
        .context("console browser handoff endpoint returned non-success status")?;
    let handoff = handoff_response
        .json::<Value>()
        .context("failed to parse console browser handoff response json")?;
    let handoff_url = handoff
        .get("handoff_url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("browser handoff response missing handoff_url"))?;
    let handoff_token = Url::parse(handoff_url)
        .context("failed to parse handoff_url")?
        .query_pairs()
        .find_map(|(key, value)| (key == "token").then(|| value.into_owned()))
        .ok_or_else(|| anyhow::anyhow!("browser handoff_url missing token query parameter"))?;

    let bootstrap_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/auth/browser-handoff/session"))
        .json(&serde_json::json!({ "token": handoff_token }))
        .send()
        .context("failed to consume console browser handoff through session endpoint")?
        .error_for_status()
        .context("console browser handoff session endpoint returned non-success status")?;
    assert_admin_console_security_headers(bootstrap_response.headers())?;
    let handoff_cookie = header_value(bootstrap_response.headers(), "set-cookie")?
        .split(';')
        .next()
        .ok_or_else(|| anyhow::anyhow!("browser handoff set-cookie header missing cookie pair"))?
        .to_owned();
    let handoff_set_cookie = header_value(bootstrap_response.headers(), "set-cookie")?;
    assert!(
        handoff_set_cookie.contains("SameSite=Lax"),
        "browser handoff session bootstrap should issue a top-level navigation compatible session cookie"
    );
    let session = bootstrap_response
        .json::<Value>()
        .context("failed to parse handoff session response json")?;
    assert_eq!(
        session.get("principal").and_then(Value::as_str),
        Some(CONSOLE_ADMIN_PRINCIPAL),
        "browser handoff session bootstrap should inherit the desktop admin principal"
    );

    let session_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/auth/session"))
        .header("Cookie", handoff_cookie)
        .send()
        .context("failed to verify browser handoff session")?
        .error_for_status()
        .context("browser handoff session did not authorize console auth/session")?;
    assert_admin_console_security_headers(session_response.headers())?;

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
fn console_session_catalog_endpoints_require_session_and_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let no_session = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/sessions"))
        .send()
        .context("failed to call session catalog endpoint without session")?;
    assert_eq!(
        no_session.status().as_u16(),
        403,
        "session catalog endpoint must require session"
    );

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let created_session = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/chat/sessions"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "session_label": "ops-catalog-session",
        }))
        .send()
        .context("failed to create chat session for catalog test")?
        .error_for_status()
        .context("chat session create for catalog test returned non-success status")?
        .json::<Value>()
        .context("failed to parse created chat session response json")?;
    let session_id = created_session
        .get("session")
        .and_then(|value| value.get("session_id"))
        .and_then(Value::as_str)
        .context("created chat session response should include session id")?;

    let catalog_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/sessions"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list session catalog with session cookie")?
        .error_for_status()
        .context("session catalog endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse session catalog response json")?;
    assert!(
        catalog_response.get("sessions").and_then(Value::as_array).is_some(),
        "session catalog response should include sessions array"
    );
    assert!(
        catalog_response.get("summary").is_some(),
        "session catalog response should include summary payload"
    );

    let detail_response = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/console/v1/sessions/{session_id}"
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch session catalog detail")?
        .error_for_status()
        .context("session catalog detail returned non-success status")?
        .json::<Value>()
        .context("failed to parse session catalog detail response json")?;
    assert_eq!(
        detail_response
            .get("session")
            .and_then(|value| value.get("session_id"))
            .and_then(Value::as_str),
        Some(session_id),
        "session catalog detail should return the requested session"
    );

    let archive_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/sessions/{session_id}/archive"
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to archive session without csrf token")?;
    assert_eq!(
        archive_without_csrf.status().as_u16(),
        403,
        "session archive endpoint must enforce csrf token"
    );

    let archive_response = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/sessions/{session_id}/archive"
        ))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to archive session with csrf token")?
        .error_for_status()
        .context("session archive endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse session archive response json")?;
    assert_eq!(
        archive_response.get("action").and_then(Value::as_str),
        Some("archived"),
        "session archive endpoint should report archived action"
    );
    assert_eq!(
        archive_response
            .get("session")
            .and_then(|value| value.get("archived"))
            .and_then(Value::as_bool),
        Some(true),
        "session archive endpoint should mark the session archived"
    );

    let unknown_run_id = "01ARZ3NDEKTSV4RRFFQ69G5FB8";
    let abort_without_csrf = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/sessions/runs/{unknown_run_id}/abort"
        ))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({}))
        .send()
        .context("failed to cancel session run without csrf token")?;
    assert_eq!(
        abort_without_csrf.status().as_u16(),
        403,
        "session run abort endpoint must enforce csrf token"
    );

    let unknown_abort = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/sessions/runs/{unknown_run_id}/abort"
        ))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({}))
        .send()
        .context("failed to cancel unknown session run")?;
    assert_eq!(
        unknown_abort.status().as_u16(),
        404,
        "unknown session run abort should return not-found"
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
fn console_agent_endpoints_require_session_and_csrf_and_bridge_runtime() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;

    let no_session = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .send()
        .context("failed to call agents list endpoint without session")?;
    assert_eq!(no_session.status().as_u16(), 403, "agents list endpoint must require session");

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let empty_list = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call agents list endpoint with session")?
        .error_for_status()
        .context("agents list endpoint returned non-success status")?
        .json::<palyra_control_plane::AgentListEnvelope>()
        .context("failed to parse agents list response json")?;
    assert!(empty_list.agents.is_empty(), "fresh daemon should start with an empty agent registry");
    assert_eq!(
        empty_list.default_agent_id, None,
        "fresh daemon should not publish a default agent id"
    );

    let oversized_after_agent_id_response = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/console/v1/agents?after_agent_id={}",
            "a".repeat(512)
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call agents list endpoint with oversized after_agent_id")?;
    assert_eq!(
        oversized_after_agent_id_response.status().as_u16(),
        400,
        "agents list endpoint should reject oversized after_agent_id query values"
    );
    assert_admin_console_security_headers(oversized_after_agent_id_response.headers())?;

    let oversized_agent_path_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents/{}", "a".repeat(512)))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call agent get endpoint with oversized agent_id path")?;
    assert_eq!(
        oversized_agent_path_response.status().as_u16(),
        400,
        "agent get endpoint should reject oversized agent_id path values"
    );
    assert_admin_console_security_headers(oversized_agent_path_response.headers())?;

    let create_main_payload = serde_json::json!({
        "agent_id": "main",
        "display_name": "Main",
        "workspace_roots": ["workspace"],
        "default_tool_allowlist": ["palyra.echo"],
        "default_skill_allowlist": ["acme.echo"],
        "set_default": true,
        "allow_absolute_paths": false
    });

    let create_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie.clone())
        .json(&create_main_payload)
        .send()
        .context("failed to create agent without csrf token")?;
    assert_eq!(
        create_without_csrf.status().as_u16(),
        403,
        "agent create endpoint must enforce csrf token"
    );

    let created_main = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&create_main_payload)
        .send()
        .context("failed to create main agent with csrf token")?
        .error_for_status()
        .context("main agent create returned non-success status")?
        .json::<palyra_control_plane::AgentCreateEnvelope>()
        .context("failed to parse main agent create response json")?;
    assert_eq!(created_main.agent.agent_id, "main");
    assert!(created_main.default_changed, "first created agent should become default");
    assert_eq!(created_main.default_agent_id.as_deref(), Some("main"));
    assert!(
        !created_main.agent.agent_dir.trim().is_empty(),
        "created agent should include its canonical agent_dir"
    );

    let created_review = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "agent_id": "review",
            "display_name": "Review",
            "workspace_roots": ["workspace-review"],
            "default_tool_allowlist": ["palyra.echo"],
            "default_skill_allowlist": ["acme.review"],
            "set_default": false,
            "allow_absolute_paths": false
        }))
        .send()
        .context("failed to create review agent with csrf token")?
        .error_for_status()
        .context("review agent create returned non-success status")?
        .json::<palyra_control_plane::AgentCreateEnvelope>()
        .context("failed to parse review agent create response json")?;
    assert_eq!(created_review.agent.agent_id, "review");
    assert!(
        !created_review.default_changed,
        "non-default create should not switch the default agent"
    );
    assert_eq!(created_review.default_agent_id.as_deref(), Some("main"));

    let listed = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list agents after creation")?
        .error_for_status()
        .context("agents list after creation returned non-success status")?
        .json::<palyra_control_plane::AgentListEnvelope>()
        .context("failed to parse populated agents list response json")?;
    assert_eq!(listed.agents.len(), 2, "agents list should include both created agents");
    assert_eq!(listed.default_agent_id.as_deref(), Some("main"));

    let fetched_main = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents/main"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch main agent")?
        .error_for_status()
        .context("main agent fetch returned non-success status")?
        .json::<palyra_control_plane::AgentEnvelope>()
        .context("failed to parse main agent response json")?;
    assert_eq!(fetched_main.agent.agent_id, "main");
    assert!(fetched_main.is_default, "main agent should be default before set-default");

    let set_default_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/agents/review/set-default"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call set-default without csrf token")?;
    assert_eq!(
        set_default_without_csrf.status().as_u16(),
        403,
        "set-default endpoint must enforce csrf token"
    );

    let set_default = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/agents/review/set-default"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to call set-default with csrf token")?
        .error_for_status()
        .context("set-default returned non-success status")?
        .json::<palyra_control_plane::AgentSetDefaultEnvelope>()
        .context("failed to parse set-default response json")?;
    assert_eq!(set_default.previous_default_agent_id.as_deref(), Some("main"));
    assert_eq!(set_default.default_agent_id, "review");

    let fetched_review = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents/review"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch review agent")?
        .error_for_status()
        .context("review agent fetch returned non-success status")?
        .json::<palyra_control_plane::AgentEnvelope>()
        .context("failed to parse review agent response json")?;
    assert_eq!(fetched_review.agent.agent_id, "review");
    assert!(fetched_review.is_default, "review agent should be default after set-default");

    let final_list = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/agents"))
        .header("Cookie", cookie)
        .send()
        .context("failed to list agents after set-default")?
        .error_for_status()
        .context("agents list after set-default returned non-success status")?
        .json::<palyra_control_plane::AgentListEnvelope>()
        .context("failed to parse final agents list response json")?;
    assert_eq!(final_list.default_agent_id.as_deref(), Some("review"));

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
    assert!(
        connectors
            .iter()
            .all(|entry| { entry.get("availability").and_then(Value::as_str) != Some("deferred") }),
        "channels list response should not surface deferred connectors"
    );
    assert!(
        connectors.iter().all(|entry| {
            entry.get("connector_id").and_then(Value::as_str) != Some("slack:default")
                && entry.get("connector_id").and_then(Value::as_str) != Some("telegram:default")
        }),
        "channels list response should hide deferred connector ids from the default operator view"
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
    assert_eq!(
        enabled_response
            .get("connector")
            .and_then(|connector| connector.get("availability"))
            .and_then(Value::as_str),
        Some("internal_test_only"),
        "echo connector should be labeled as internal_test_only in console responses"
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
fn console_memory_status_and_index_surface_return_operator_payloads() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let status_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/memory/status"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call memory status endpoint")?
        .error_for_status()
        .context("memory status endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse memory status response json")?;
    assert!(
        status_response.get("usage").is_some(),
        "memory status response should include usage payload"
    );
    assert!(
        status_response.get("embeddings").is_some(),
        "memory status response should include embeddings payload"
    );

    let missing_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/memory/index"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({ "batch_size": 8, "until_complete": true }))
        .send()
        .context("failed to call memory index endpoint without csrf token")?;
    assert_eq!(
        missing_csrf.status().as_u16(),
        403,
        "memory index endpoint must enforce csrf token"
    );

    let index_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/memory/index"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "batch_size": 8,
            "until_complete": true,
            "run_maintenance": true,
        }))
        .send()
        .context("failed to call memory index endpoint with csrf token")?
        .error_for_status()
        .context("memory index endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse memory index response json")?;
    assert!(
        index_response.get("index").is_some(),
        "memory index response should include index payload"
    );
    assert!(
        index_response.get("embeddings").is_some(),
        "memory index response should include embeddings payload"
    );

    Ok(())
}

#[test]
fn console_system_surface_returns_presence_and_enforces_emit_csrf() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let heartbeat_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/system/heartbeat"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call system heartbeat endpoint")?
        .error_for_status()
        .context("system heartbeat endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse system heartbeat response json")?;
    assert_eq!(
        heartbeat_response.get("status").and_then(Value::as_str),
        Some("ok"),
        "system heartbeat should surface the daemon status"
    );
    assert!(
        heartbeat_response.get("transport").is_some(),
        "system heartbeat response should include transport payload"
    );

    let presence_response = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/system/presence"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call system presence endpoint")?
        .error_for_status()
        .context("system presence endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse system presence response json")?;
    assert!(
        presence_response.pointer("/subsystems/gateway").is_some(),
        "system presence should include gateway subsystem"
    );
    assert!(
        presence_response.pointer("/subsystems/model_provider").is_some(),
        "system presence should include model provider subsystem"
    );

    let initial_events = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/system/events?limit=5"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call system events endpoint")?
        .error_for_status()
        .context("system events endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse system events response json")?;
    assert!(
        initial_events.get("events").is_some(),
        "system events response should include events payload"
    );

    let missing_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/system/events/emit"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({
            "name": "maintenance.pulse",
            "summary": "operator confirmed runtime posture",
            "details": { "source": "admin_surface" },
        }))
        .send()
        .context("failed to call system event emit endpoint without csrf token")?;
    assert_eq!(
        missing_csrf.status().as_u16(),
        403,
        "system event emit endpoint must enforce csrf token"
    );

    let emitted = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/system/events/emit"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "name": "maintenance.pulse",
            "summary": "operator confirmed runtime posture",
            "details": { "source": "admin_surface" },
        }))
        .send()
        .context("failed to call system event emit endpoint with csrf token")?
        .error_for_status()
        .context("system event emit endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse system event emit response json")?;
    assert_eq!(
        emitted.get("event").and_then(Value::as_str),
        Some("system.operator.maintenance.pulse"),
        "system event emit response should namespace operator events"
    );

    let filtered_events = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/console/v1/system/events?event=system.operator.maintenance.pulse&limit=10"
        ))
        .header("Cookie", cookie)
        .send()
        .context("failed to query filtered system events")?
        .error_for_status()
        .context("filtered system events endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse filtered system events response json")?;
    assert!(
        filtered_events.get("returned_events").and_then(Value::as_u64).unwrap_or(0) >= 1,
        "filtered system events should include the emitted operator event"
    );

    Ok(())
}

#[test]
fn admin_channel_queue_pause_resume_preserves_enabled_connector_state() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let connector_id = "echo:default";

    let paused = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{connector_id}/operations/queue/pause"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin queue pause endpoint")?
        .error_for_status()
        .context("admin queue pause endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin queue pause response json")?;
    assert_eq!(
        paused.get("action").and_then(|value| value.get("type")).and_then(Value::as_str),
        Some("queue_pause"),
        "queue pause action should be labeled"
    );
    assert_eq!(
        paused.get("connector").and_then(|value| value.get("enabled")).and_then(Value::as_bool),
        Some(true),
        "queue pause must not disable the connector"
    );
    assert_eq!(
        paused
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("paused"))
            .and_then(Value::as_bool),
        Some(true),
        "queue pause should expose paused=true in operations snapshot"
    );
    assert_eq!(
        paused
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("pause_reason"))
            .and_then(Value::as_str),
        Some("operator requested queue pause via admin API"),
        "queue pause should expose the operator reason"
    );

    let resumed = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{connector_id}/operations/queue/resume"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin queue resume endpoint")?
        .error_for_status()
        .context("admin queue resume endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin queue resume response json")?;
    assert_eq!(
        resumed.get("action").and_then(|value| value.get("type")).and_then(Value::as_str),
        Some("queue_resume"),
        "queue resume action should be labeled"
    );
    assert_eq!(
        resumed.get("connector").and_then(|value| value.get("enabled")).and_then(Value::as_bool),
        Some(true),
        "queue resume must leave the connector enabled"
    );
    assert_eq!(
        resumed
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("paused"))
            .and_then(Value::as_bool),
        Some(false),
        "queue resume should expose paused=false in operations snapshot"
    );

    Ok(())
}

#[test]
fn admin_channel_health_refresh_and_dead_letter_recovery_publish_operator_state() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let discord_connector_id = "discord:default";

    let enabled = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/enabled"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({ "enabled": true }))
        .send()
        .context("failed to enable discord connector for recovery test")?
        .error_for_status()
        .context("enabling discord connector for recovery test returned non-success status")?
        .json::<Value>()
        .context("failed to parse discord connector enable response json")?;
    assert_eq!(
        enabled.get("connector").and_then(|value| value.get("enabled")).and_then(Value::as_bool),
        Some(true),
        "discord connector should be enabled before running recovery actions"
    );

    let health_refresh = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/operations/health-refresh"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({}))
        .send()
        .context("failed to call admin channel health-refresh endpoint")?
        .error_for_status()
        .context("admin channel health-refresh endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin channel health-refresh response json")?;
    assert_eq!(
        health_refresh
            .get("health_refresh")
            .and_then(|value| value.get("supported"))
            .and_then(Value::as_bool),
        Some(true),
        "discord health refresh should be supported"
    );
    assert_eq!(
        health_refresh
            .get("health_refresh")
            .and_then(|value| value.get("refreshed"))
            .and_then(Value::as_bool),
        Some(false),
        "health refresh should report refreshed=false when the bot token is unavailable"
    );
    assert!(
        health_refresh
            .get("health_refresh")
            .and_then(|value| value.get("required_permissions"))
            .and_then(Value::as_array)
            .is_some(),
        "health refresh should still publish required Discord permissions"
    );

    let test_send = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/test-send"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&serde_json::json!({
            "target": "channel:1234567890",
            "text": "trigger dead letter",
            "confirm": true,
        }))
        .send()
        .context("failed to call admin discord test-send endpoint")?
        .error_for_status()
        .context("admin discord test-send endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin discord test-send response json")?;
    assert!(
        test_send.get("dispatch").is_some() && test_send.get("status").is_some(),
        "discord test-send should still return dispatch and status payloads"
    );

    let logs_after_send = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/logs?limit=5"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin channel logs endpoint after dead-lettering")?
        .error_for_status()
        .context("admin channel logs endpoint returned non-success status after dead-lettering")?
        .json::<Value>()
        .context("failed to parse admin channel logs response json after dead-lettering")?;
    let dead_letter_id = logs_after_send
        .get("dead_letters")
        .and_then(Value::as_array)
        .and_then(|entries| entries.first())
        .and_then(|entry| entry.get("dead_letter_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("expected admin channel logs to expose a dead-letter id"))?;

    let logs_after_send_query = client
        .post(format!("http://127.0.0.1:{admin_port}/admin/v1/channels/logs/query"))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&json!({
            "connector_id": discord_connector_id,
            "limit": 5,
        }))
        .send()
        .context("failed to call admin channel logs query endpoint after dead-lettering")?
        .error_for_status()
        .context(
            "admin channel logs query endpoint returned non-success status after dead-lettering",
        )?
        .json::<Value>()
        .context("failed to parse admin channel logs query response json after dead-lettering")?;
    assert_eq!(
        logs_after_send_query
            .get("dead_letters")
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .and_then(|entry| entry.get("dead_letter_id"))
            .and_then(Value::as_i64),
        Some(dead_letter_id),
        "body-based logs query should resolve the same dead-letter record as the legacy path route"
    );

    let replayed = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/operations/dead-letters/{dead_letter_id}/replay"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin dead-letter replay endpoint")?
        .error_for_status()
        .context("admin dead-letter replay endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin dead-letter replay response json")?;
    assert_eq!(
        replayed.get("action").and_then(|value| value.get("type")).and_then(Value::as_str),
        Some("dead_letter_replay"),
        "dead-letter replay action should be labeled"
    );
    assert_eq!(
        replayed
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("dead_letters"))
            .and_then(Value::as_u64),
        Some(0),
        "replay should remove the item from dead letters"
    );

    let drained = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/operations/queue/drain"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin queue drain endpoint")?
        .error_for_status()
        .context("admin queue drain endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin queue drain response json")?;
    assert_eq!(
        drained.get("action").and_then(|value| value.get("type")).and_then(Value::as_str),
        Some("queue_drain"),
        "queue drain action should be labeled"
    );
    assert_eq!(
        drained
            .get("action")
            .and_then(|value| value.get("drain"))
            .and_then(|value| value.get("dead_lettered"))
            .and_then(Value::as_u64),
        Some(1),
        "forced drain should process the replayed item back into dead letters when the token is still missing"
    );

    let logs_after_drain = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/logs?limit=5"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin channel logs endpoint after forced drain")?
        .error_for_status()
        .context("admin channel logs endpoint returned non-success status after forced drain")?
        .json::<Value>()
        .context("failed to parse admin channel logs response json after forced drain")?;
    let dead_letter_id = logs_after_drain
        .get("dead_letters")
        .and_then(Value::as_array)
        .and_then(|entries| entries.first())
        .and_then(|entry| entry.get("dead_letter_id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("expected forced drain to recreate a dead-letter entry"))?;

    let discarded = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/admin/v1/channels/{discord_connector_id}/operations/dead-letters/{dead_letter_id}/discard"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call admin dead-letter discard endpoint")?
        .error_for_status()
        .context("admin dead-letter discard endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse admin dead-letter discard response json")?;
    assert_eq!(
        discarded.get("action").and_then(|value| value.get("type")).and_then(Value::as_str),
        Some("dead_letter_discard"),
        "dead-letter discard action should be labeled"
    );
    assert_eq!(
        discarded
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("dead_letters"))
            .and_then(Value::as_u64),
        Some(0),
        "discard should clear the recreated dead-letter entry"
    );

    Ok(())
}

#[test]
fn console_channel_operations_return_recovery_payloads() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let connector_id = "echo:default";

    let queue_pause = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/operations/queue/pause"
        ))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to call console queue pause endpoint")?
        .error_for_status()
        .context("console queue pause endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse console queue pause response json")?;
    assert_eq!(
        queue_pause
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("paused"))
            .and_then(Value::as_bool),
        Some(true),
        "console queue pause should expose paused queue state"
    );
    assert_eq!(
        queue_pause
            .get("connector")
            .and_then(|value| value.get("enabled"))
            .and_then(Value::as_bool),
        Some(true),
        "console queue pause must not disable the connector"
    );

    let queue_resume = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/{connector_id}/operations/queue/resume"
        ))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to call console queue resume endpoint")?
        .error_for_status()
        .context("console queue resume endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse console queue resume response json")?;
    assert_eq!(
        queue_resume
            .get("operations")
            .and_then(|value| value.get("queue"))
            .and_then(|value| value.get("paused"))
            .and_then(Value::as_bool),
        Some(false),
        "console queue resume should expose resumed queue state"
    );

    let health_refresh = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/channels/discord:default/operations/health-refresh"
        ))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({}))
        .send()
        .context("failed to call console discord health-refresh endpoint")?
        .error_for_status()
        .context("console discord health-refresh endpoint returned non-success status")?
        .json::<Value>()
        .context("failed to parse console discord health-refresh response json")?;
    assert_eq!(
        health_refresh
            .get("health_refresh")
            .and_then(|value| value.get("supported"))
            .and_then(Value::as_bool),
        Some(true),
        "console health refresh should surface Discord support"
    );
    assert!(
        health_refresh
            .get("health_refresh")
            .and_then(|value| value.get("required_permissions"))
            .and_then(Value::as_array)
            .is_some(),
        "console health refresh should expose Discord permission guidance"
    );

    Ok(())
}

#[test]
fn console_m52_control_plane_domains_publish_contract_metadata() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let capability_catalog = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/control-plane/capabilities"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch capability catalog")?
        .error_for_status()
        .context("capability catalog returned non-success status")?
        .json::<Value>()
        .context("failed to parse capability catalog response json")?;
    assert_eq!(
        capability_catalog
            .get("contract")
            .and_then(|value| value.get("contract_version"))
            .and_then(Value::as_str),
        Some("control-plane.v1"),
        "capability catalog should expose control-plane contract version"
    );
    assert!(
        capability_catalog.get("capabilities").and_then(Value::as_array).is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.get("id").and_then(Value::as_str) == Some("auth.profiles"))
        }),
        "capability catalog should enumerate auth.profiles capability"
    );
    let capability_entries = capability_catalog
        .get("capabilities")
        .and_then(Value::as_array)
        .context("capability catalog should include capability entries array")?;
    let gateway_verify = capability_entries
        .iter()
        .find(|entry| {
            entry.get("id").and_then(Value::as_str) == Some("gateway.access.verify_remote")
        })
        .context("capability catalog should include gateway.access.verify_remote handoff entry")?;
    assert_eq!(
        gateway_verify.get("dashboard_section").and_then(Value::as_str),
        Some("access"),
        "gateway verify handoff should map to the access dashboard section"
    );
    assert_eq!(
        gateway_verify.get("dashboard_exposure").and_then(Value::as_str),
        Some("cli_handoff"),
        "gateway verify handoff should be explicitly marked as a CLI handoff"
    );
    assert_eq!(
        gateway_verify.get("execution_mode").and_then(Value::as_str),
        Some("generated_cli"),
        "gateway verify handoff should keep execution mode focused on CLI mechanics"
    );
    assert!(
        gateway_verify.get("cli_handoff_commands").and_then(Value::as_array).is_some_and(
            |commands| commands.iter().any(|command| {
                command.as_str().is_some_and(|value| value.contains("dashboard --verify-remote"))
            })
        ),
        "gateway verify handoff should publish the generated CLI command"
    );
    let policy_explain = capability_entries
        .iter()
        .find(|entry| entry.get("id").and_then(Value::as_str) == Some("policy.explain"))
        .context("capability catalog should include policy.explain internal-only entry")?;
    assert_eq!(
        policy_explain.get("dashboard_exposure").and_then(Value::as_str),
        Some("internal_only"),
        "policy explain should remain explicitly internal-only"
    );
    assert_eq!(
        policy_explain.get("execution_mode").and_then(Value::as_str),
        Some("internal"),
        "policy explain should keep execution mode focused on internal mechanics"
    );
    assert!(
        policy_explain.get("notes").and_then(Value::as_str).is_some(),
        "internal-only catalog entries should publish a justification note"
    );

    let deployment = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/deployment/posture"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch deployment posture")?
        .error_for_status()
        .context("deployment posture returned non-success status")?
        .json::<Value>()
        .context("failed to parse deployment posture response json")?;
    assert_eq!(
        deployment
            .get("contract")
            .and_then(|value| value.get("contract_version"))
            .and_then(Value::as_str),
        Some("control-plane.v1")
    );
    assert!(
        deployment.get("bind_addresses").is_some(),
        "deployment posture should expose bind addresses"
    );

    let auth_profiles = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/auth/profiles"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch auth profiles")?
        .error_for_status()
        .context("auth profiles returned non-success status")?
        .json::<Value>()
        .context("failed to parse auth profiles response json")?;
    assert_eq!(
        auth_profiles
            .get("contract")
            .and_then(|value| value.get("contract_version"))
            .and_then(Value::as_str),
        Some("control-plane.v1")
    );
    assert!(
        auth_profiles
            .get("page")
            .and_then(|value| value.get("has_more"))
            .and_then(Value::as_bool)
            .is_some(),
        "auth profile list should publish normalized page metadata"
    );

    let auth_health = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/auth/health?include_profiles=true"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch auth health")?
        .error_for_status()
        .context("auth health returned non-success status")?
        .json::<Value>()
        .context("failed to parse auth health response json")?;
    assert!(
        auth_health.get("summary").is_some() && auth_health.get("refresh_metrics").is_some(),
        "auth health should expose summary and refresh metrics"
    );

    let secrets = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/secrets?scope=global"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch secrets metadata")?
        .error_for_status()
        .context("secrets metadata returned non-success status")?
        .json::<Value>()
        .context("failed to parse secrets metadata response json")?;
    assert_eq!(
        secrets
            .get("contract")
            .and_then(|value| value.get("contract_version"))
            .and_then(Value::as_str),
        Some("control-plane.v1")
    );
    assert!(secrets.get("page").is_some(), "secret metadata list should publish page metadata");

    let inspect_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/inspect"))
        .header("Cookie", cookie.clone())
        .json(&serde_json::json!({}))
        .send()
        .context("failed to call config inspect without csrf")?;
    assert_eq!(
        inspect_without_csrf.status().as_u16(),
        403,
        "config inspect should enforce csrf even though it is read-oriented POST"
    );

    let config_inspect = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/inspect"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({}))
        .send()
        .context("failed to call config inspect with csrf")?
        .error_for_status()
        .context("config inspect returned non-success status")?
        .json::<Value>()
        .context("failed to parse config inspect response json")?;
    assert!(
        config_inspect.get("document_toml").is_some(),
        "config inspect should expose serialized TOML snapshot"
    );

    let support_jobs = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/support-bundle/jobs"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch support-bundle jobs")?
        .error_for_status()
        .context("support-bundle jobs returned non-success status")?
        .json::<Value>()
        .context("failed to parse support-bundle jobs response json")?;
    assert_eq!(
        support_jobs
            .get("contract")
            .and_then(|value| value.get("contract_version"))
            .and_then(Value::as_str),
        Some("control-plane.v1")
    );
    assert!(
        support_jobs.get("jobs").and_then(Value::as_array).is_some(),
        "support-bundle job list should expose jobs array"
    );

    Ok(())
}

#[test]
fn console_m52_error_envelope_exposes_validation_metadata() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let reveal_error = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/secrets/reveal"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "scope": "global",
            "key": "missing",
            "reveal": false
        }))
        .send()
        .context("failed to call secret reveal with invalid body")?;
    assert_eq!(reveal_error.status().as_u16(), 400, "invalid reveal request should be rejected");
    let body = reveal_error.json::<Value>().context("failed to parse reveal error json")?;
    assert_eq!(body.get("code").and_then(Value::as_str), Some("validation_error"));
    assert_eq!(body.get("category").and_then(Value::as_str), Some("validation"));
    assert!(
        body.get("validation_errors").and_then(Value::as_array).is_some_and(|issues| issues
            .iter()
            .any(|issue| { issue.get("field").and_then(Value::as_str) == Some("reveal") })),
        "validation error should name the offending field"
    );
    assert!(
        body.get("error").and_then(Value::as_str).is_some(),
        "error envelope must preserve top-level error message for backward compatibility"
    );

    Ok(())
}

#[test]
fn console_secret_reveal_allows_sensitive_ref_via_server_side_console_flow() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let secret_value = b"sk-sensitive-vault-value";
    let secret_value_base64 = BASE64_STANDARD.encode(secret_value);

    let stored = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/secrets"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "scope": "global",
            "key": "openai_api_key",
            "value_base64": secret_value_base64.clone()
        }))
        .send()
        .context("failed to store sensitive secret through console")?
        .error_for_status()
        .context("console secret set returned non-success status")?
        .json::<Value>()
        .context("failed to parse console secret set response json")?;
    assert_eq!(
        stored.pointer("/secret/key").and_then(Value::as_str),
        Some("openai_api_key"),
        "console secret set should persist the selected sensitive ref"
    );

    let revealed = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/secrets/reveal"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({
            "scope": "global",
            "key": "openai_api_key",
            "reveal": true
        }))
        .send()
        .context("failed to reveal sensitive secret through console")?
        .error_for_status()
        .context("console secret reveal returned non-success status")?
        .json::<Value>()
        .context("failed to parse console secret reveal response json")?;
    assert_eq!(
        revealed.get("value_base64").and_then(Value::as_str),
        Some(secret_value_base64.as_str()),
        "console reveal should return the stored sensitive bytes through the server-side path"
    );
    assert_eq!(
        revealed.get("value_utf8").and_then(Value::as_str),
        Some("sk-sensitive-vault-value"),
        "console reveal should preserve UTF-8 content for operator display"
    );

    Ok(())
}

#[test]
fn console_webhooks_support_secret_aware_lifecycle_and_diagnostics() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build HTTP client")?;
    let webhook_payload = serde_json::json!({
        "integration_id": "github_repo_a",
        "provider": "github",
        "display_name": "GitHub Repo A",
        "secret_vault_ref": "global/github_repo_a",
        "allowed_events": ["push"],
        "allowed_sources": ["github.repo_a"],
        "enabled": true,
        "signature_required": true,
        "max_payload_bytes": 8192
    });

    let unauthenticated = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks"))
        .json(&webhook_payload)
        .send()
        .context("failed to call webhook create endpoint without session")?;
    assert_eq!(
        unauthenticated.status().as_u16(),
        403,
        "webhook create should reject unauthenticated console requests"
    );
    assert_admin_console_security_headers(unauthenticated.headers())?;

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let stored_secret = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/secrets"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "scope": "global",
            "key": "github_repo_a",
            "value_base64": BASE64_STANDARD.encode("test-signing-secret"),
        }))
        .send()
        .context("failed to store webhook signing secret through console")?
        .error_for_status()
        .context("console secret set for webhook returned non-success status")?
        .json::<Value>()
        .context("failed to parse webhook secret set response json")?;
    assert_eq!(
        stored_secret.pointer("/secret/key").and_then(Value::as_str),
        Some("github_repo_a"),
        "webhook signing secret should be stored in the vault before integration create"
    );

    let created = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&webhook_payload)
        .send()
        .context("failed to create webhook integration")?
        .error_for_status()
        .context("webhook create returned non-success status")?;
    assert_admin_console_security_headers(created.headers())?;
    let created =
        created.json::<Value>().context("failed to parse webhook create response json")?;
    assert_eq!(
        created.pointer("/integration/integration_id").and_then(Value::as_str),
        Some("github_repo_a")
    );
    assert_eq!(
        created.pointer("/integration/status").and_then(Value::as_str),
        Some("ready"),
        "webhook integration should become ready once the referenced secret exists"
    );
    assert_eq!(
        created.pointer("/integration/signature_required").and_then(Value::as_bool),
        Some(true)
    );

    let listed = client
        .get(format!(
            "http://127.0.0.1:{admin_port}/console/v1/webhooks?provider=github&enabled=true"
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list webhook integrations")?
        .error_for_status()
        .context("webhook list returned non-success status")?;
    assert_admin_console_security_headers(listed.headers())?;
    let listed = listed.json::<Value>().context("failed to parse webhook list response json")?;
    let integrations = listed
        .get("integrations")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("webhook list response missing integrations array"))?;
    assert_eq!(integrations.len(), 1, "webhook list should return the created integration");

    let fetched = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks/github_repo_a"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to fetch webhook integration")?
        .error_for_status()
        .context("webhook get returned non-success status")?
        .json::<Value>()
        .context("failed to parse webhook get response json")?;
    assert_eq!(
        fetched.pointer("/integration/allowed_sources/0").and_then(Value::as_str),
        Some("github.repo_a")
    );

    let tested = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks/github_repo_a/test"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "payload_base64": BASE64_STANDARD.encode(build_test_webhook_payload(
                "github.repo_a",
                "push",
                Some("sig:test"),
            )),
        }))
        .send()
        .context("failed to test webhook integration")?
        .error_for_status()
        .context("webhook test returned non-success status")?
        .json::<Value>()
        .context("failed to parse webhook test response json")?;
    assert_eq!(
        tested.pointer("/result/valid").and_then(Value::as_bool),
        Some(true),
        "webhook test should accept a structurally valid payload that matches the allowlists"
    );
    assert_eq!(tested.pointer("/result/outcome").and_then(Value::as_str), Some("accepted"));
    assert_eq!(
        tested.pointer("/integration/integration_id").and_then(Value::as_str),
        tested.pointer("/result/integration_id").and_then(Value::as_str),
        "webhook test response should keep integration identity consistent across envelope fields"
    );
    assert_eq!(tested.pointer("/result/signature_present").and_then(Value::as_bool), Some(true));
    assert_eq!(
        tested.pointer("/integration/last_test_status").and_then(Value::as_str),
        Some("passed"),
        "webhook test should update last_test metadata on the integration view"
    );

    let diagnostics = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/diagnostics"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to load console diagnostics after webhook create")?
        .error_for_status()
        .context("console diagnostics after webhook create returned non-success status")?
        .json::<Value>()
        .context("failed to parse console diagnostics response json")?;
    assert_eq!(
        diagnostics.pointer("/webhooks/total").and_then(Value::as_u64),
        Some(1),
        "diagnostics should include the registered webhook integration count"
    );
    assert_eq!(
        diagnostics.pointer("/webhooks/ready").and_then(Value::as_u64),
        Some(1),
        "diagnostics should report the webhook integration as ready"
    );

    let deleted = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks/github_repo_a/delete"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to delete webhook integration")?
        .error_for_status()
        .context("webhook delete returned non-success status")?
        .json::<Value>()
        .context("failed to parse webhook delete response json")?;
    assert_eq!(deleted.get("deleted").and_then(Value::as_bool), Some(true));

    let post_delete_list = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/webhooks"))
        .header("Cookie", cookie)
        .send()
        .context("failed to list webhook integrations after delete")?
        .error_for_status()
        .context("webhook list after delete returned non-success status")?
        .json::<Value>()
        .context("failed to parse webhook list after delete response json")?;
    assert_eq!(
        post_delete_list.get("integrations").and_then(Value::as_array).map(std::vec::Vec::len),
        Some(0),
        "webhook integration delete should remove the registry entry"
    );

    Ok(())
}

#[test]
fn console_plugins_and_hooks_bind_installed_skills_and_dispatch_skill_events() -> Result<()> {
    let config = r#"
version = 1

[tool_call]
allowed_tools = ["palyra.plugin.run"]
execution_timeout_ms = 4000

[tool_call.wasm_runtime]
enabled = true
allow_inline_modules = false
max_module_size_bytes = 131072
fuel_budget = 500000
max_memory_bytes = 1048576
max_table_elements = 128
max_instances = 1
allowed_http_hosts = []
allowed_secrets = []
allowed_storage_prefixes = []
allowed_channels = []
"#;
    let (child, admin_port) = spawn_palyrad_with_config_and_env(
        config,
        &[("PALYRA_ADMIN_BOUND_PRINCIPAL", CONSOLE_ADMIN_PRINCIPAL)],
    )?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let artifact_path = unique_temp_skill_artifact_path();
    fs::write(
        &artifact_path,
        build_test_skill_artifact("acme.hook_skill", "1.0.0")
            .context("failed to build signed test skill artifact")?,
    )
    .with_context(|| format!("failed to write test skill artifact {}", artifact_path.display()))?;

    let bound_plugin_response = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/plugins/install-or-bind"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&json!({
            "plugin_id": "acme-hook-plugin",
            "artifact_path": artifact_path,
            "module_path": "modules/module.wasm",
            "entrypoint": "run",
            "enabled": true
        }))
        .send()
        .context("failed to bind plugin from signed skill artifact")?;
    let bound_plugin_status = bound_plugin_response.status();
    let bound_plugin_body =
        bound_plugin_response.text().context("failed to read plugin bind error body")?;
    anyhow::ensure!(
        bound_plugin_status.is_success(),
        "plugin install-or-bind returned status {} body {}",
        bound_plugin_status,
        bound_plugin_body
    );
    let bound_plugin = serde_json::from_str::<Value>(bound_plugin_body.as_str())
        .context("failed to parse plugin bind response json")?;
    assert_eq!(
        bound_plugin.pointer("/binding/plugin_id").and_then(Value::as_str),
        Some("acme-hook-plugin")
    );
    assert_eq!(
        bound_plugin.pointer("/check/ready").and_then(Value::as_bool),
        Some(true),
        "plugin check should resolve the installed signed skill artifact"
    );

    let listed_plugins = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/plugins"))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to list plugins")?
        .error_for_status()
        .context("plugin list returned non-success status")?
        .json::<Value>()
        .context("failed to parse plugin list response json")?;
    assert_eq!(
        listed_plugins.get("count").and_then(Value::as_u64),
        Some(1),
        "plugin list should include the newly bound plugin"
    );

    let bound_hook = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/hooks/bind"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&json!({
            "hook_id": "acme-skill-enabled-hook",
            "event": "skill:enabled",
            "plugin_id": "acme-hook-plugin",
            "enabled": true
        }))
        .send()
        .context("failed to bind hook to plugin")?
        .error_for_status()
        .context("hook bind returned non-success status")?
        .json::<Value>()
        .context("failed to parse hook bind response json")?;
    assert_eq!(
        bound_hook.pointer("/binding/hook_id").and_then(Value::as_str),
        Some("acme-skill-enabled-hook")
    );
    assert_eq!(
        bound_hook.pointer("/check/ready").and_then(Value::as_bool),
        Some(true),
        "hook check should confirm the referenced plugin exists and is enabled"
    );

    let plugin_delete_while_referenced = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/plugins/acme-hook-plugin/delete"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to call plugin delete while hook still references it")?;
    assert_eq!(
        plugin_delete_while_referenced.status().as_u16(),
        412,
        "plugin delete should fail closed while hooks still reference the binding"
    );

    let enabled_skill = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/skills/acme.hook_skill/enable"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&json!({
            "version": "1.0.0",
            "override": true,
            "reason": "test-trigger"
        }))
        .send()
        .context("failed to emit skill enabled event through console skill surface")?
        .error_for_status()
        .context("console skill enable returned non-success status")?
        .json::<Value>()
        .context("failed to parse console skill enable response json")?;
    assert_eq!(enabled_skill.get("status").and_then(Value::as_str), Some("active"));

    let dispatched = wait_for_admin_journal_event(&client, admin_port, "hook.dispatched")
        .context("hook runtime did not dispatch the plugin after skill:enabled event")?;
    assert_eq!(
        dispatched.pointer("/details/hook_id").and_then(Value::as_str),
        Some("acme-skill-enabled-hook")
    );
    assert_eq!(
        dispatched.pointer("/details/plugin_id").and_then(Value::as_str),
        Some("acme-hook-plugin")
    );
    assert_eq!(
        dispatched.pointer("/details/details/output/exit_code").and_then(Value::as_i64),
        Some(7),
        "hook dispatch journal event should include the wasm plugin exit code"
    );

    let deleted_hook = client
        .post(format!(
            "http://127.0.0.1:{admin_port}/console/v1/hooks/acme-skill-enabled-hook/delete"
        ))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to delete hook binding")?
        .error_for_status()
        .context("hook delete returned non-success status")?
        .json::<Value>()
        .context("failed to parse hook delete response json")?;
    assert_eq!(deleted_hook.get("deleted").and_then(Value::as_bool), Some(true));

    let deleted_plugin = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/plugins/acme-hook-plugin/delete"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .send()
        .context("failed to delete plugin binding after hook removal")?
        .error_for_status()
        .context("plugin delete returned non-success status")?
        .json::<Value>()
        .context("failed to parse plugin delete response json")?;
    assert_eq!(deleted_plugin.get("deleted").and_then(Value::as_bool), Some(true));

    Ok(())
}

#[test]
fn console_config_migrate_and_recover_require_session_csrf_and_keep_secrets_redacted() -> Result<()>
{
    let config_path = unique_temp_config_path();
    write_test_config(
        &config_path,
        r#"
version = 1
[admin]
auth_token = "config-admin-secret"

[model_provider]
openai_api_key = "sk-config-secret"
"#,
    )?;
    let config_path_string = config_path.to_string_lossy().to_string();

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports_with_env(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL", CONSOLE_ADMIN_PRINCIPAL),
        ("PALYRA_CONFIG", config_path_string.as_str()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("failed to build HTTP client")?;
    let migrate_payload = serde_json::json!({
        "path": config_path_string,
        "backups": 2
    });

    let migrate_without_session = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/migrate"))
        .json(&migrate_payload)
        .send()
        .context("failed to call config migrate without session")?;
    assert_eq!(
        migrate_without_session.status().as_u16(),
        403,
        "config migrate must reject unauthenticated requests before any config mutation occurs"
    );

    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let migrate_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/migrate"))
        .header("Cookie", cookie.clone())
        .json(&migrate_payload)
        .send()
        .context("failed to call config migrate without csrf")?;
    assert_eq!(
        migrate_without_csrf.status().as_u16(),
        403,
        "config migrate must enforce csrf on an authenticated console session"
    );

    let migrated = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/migrate"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&migrate_payload)
        .send()
        .context("failed to call config migrate with csrf")?
        .error_for_status()
        .context("config migrate returned non-success status")?
        .json::<Value>()
        .context("failed to parse config migrate response json")?;
    assert_eq!(migrated.get("operation").and_then(Value::as_str), Some("migrate"));
    assert_eq!(
        migrated.get("source_path").and_then(Value::as_str),
        Some(config_path.to_string_lossy().as_ref()),
        "config migrate should report the migrated config path"
    );

    let mutated = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/mutate"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "path": config_path.to_string_lossy(),
            "key": "model_provider.auth_profile_id",
            "value": "\"openai-default\"",
            "backups": 2
        }))
        .send()
        .context("failed to mutate config with csrf")?
        .error_for_status()
        .context("config mutate returned non-success status")?
        .json::<Value>()
        .context("failed to parse config mutate response json")?;
    assert_eq!(mutated.get("operation").and_then(Value::as_str), Some("set"));
    assert_eq!(
        mutated.get("changed_key").and_then(Value::as_str),
        Some("model_provider.auth_profile_id")
    );

    let inspect = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/inspect"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&serde_json::json!({
            "path": config_path.to_string_lossy(),
            "backups": 2
        }))
        .send()
        .context("failed to inspect mutated config")?
        .error_for_status()
        .context("config inspect for mutated config returned non-success status")?
        .json::<Value>()
        .context("failed to parse mutated config inspect response json")?;
    let document_toml = inspect
        .get("document_toml")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("config inspect did not include document_toml"))?;
    assert!(
        document_toml.contains("<redacted>"),
        "config inspect should keep secret values redacted after migration"
    );
    assert!(
        !document_toml.contains("config-admin-secret")
            && !document_toml.contains("sk-config-secret"),
        "config inspect must not leak raw config secrets after migration"
    );

    let recover_payload = serde_json::json!({
        "path": config_path.to_string_lossy(),
        "backup": 1,
        "backups": 2
    });
    let recover_without_csrf = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/recover"))
        .header("Cookie", cookie.clone())
        .json(&recover_payload)
        .send()
        .context("failed to call config recover without csrf")?;
    assert_eq!(
        recover_without_csrf.status().as_u16(),
        403,
        "config recover must enforce csrf on an authenticated console session"
    );

    let recovered = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/config/recover"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(&recover_payload)
        .send()
        .context("failed to call config recover with csrf")?
        .error_for_status()
        .context("config recover returned non-success status")?
        .json::<Value>()
        .context("failed to parse config recover response json")?;
    assert_eq!(recovered.get("operation").and_then(Value::as_str), Some("recover"));
    assert_eq!(
        recovered.get("source_path").and_then(Value::as_str),
        Some(config_path.to_string_lossy().as_ref()),
        "config recover should report the recovered config path"
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

#[test]
fn console_support_bundle_job_lifecycle_publishes_deterministic_completion_state() -> Result<()> {
    let (child, admin_port) = spawn_palyrad_with_bound_console_principal(CONSOLE_ADMIN_PRINCIPAL)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("failed to build HTTP client")?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let created = client
        .post(format!("http://127.0.0.1:{admin_port}/console/v1/support-bundle/jobs"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token)
        .json(&serde_json::json!({ "retain_jobs": 4 }))
        .send()
        .context("failed to create support bundle job")?
        .error_for_status()
        .context("support bundle job create returned non-success status")?
        .json::<Value>()
        .context("failed to parse support bundle create response json")?;
    let created_job = created
        .get("job")
        .ok_or_else(|| anyhow::anyhow!("support bundle create response missing job"))?;
    let job_id = created_job
        .get("job_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("support bundle create response missing job_id"))?
        .to_owned();
    assert_eq!(
        created_job.get("state").and_then(Value::as_str),
        Some("queued"),
        "support bundle job creation should start in queued state"
    );

    let timeout_at = Instant::now() + Duration::from_secs(90);
    let mut seen_states = vec!["queued".to_owned()];
    let completed_job = loop {
        let current = client
            .get(format!("http://127.0.0.1:{admin_port}/console/v1/support-bundle/jobs/{job_id}"))
            .header("Cookie", cookie.clone())
            .send()
            .with_context(|| format!("failed to load support bundle job {job_id}"))?
            .error_for_status()
            .with_context(|| format!("support bundle job {job_id} returned non-success status"))?
            .json::<Value>()
            .with_context(|| {
                format!("failed to parse support bundle job {job_id} response json")
            })?;
        let job = current
            .get("job")
            .ok_or_else(|| anyhow::anyhow!("support bundle job envelope missing job payload"))?;
        let state = job
            .get("state")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("support bundle job payload missing state"))?
            .to_owned();
        if seen_states.last() != Some(&state) {
            seen_states.push(state.clone());
        }
        match state.as_str() {
            "queued" | "running" => {
                if Instant::now() > timeout_at {
                    anyhow::bail!(
                        "timed out waiting for support bundle job {job_id} to complete; seen states: {:?}",
                        seen_states
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
            "succeeded" => break job.clone(),
            "failed" => {
                anyhow::bail!("support bundle job {job_id} failed unexpectedly: {job}");
            }
            other => anyhow::bail!("support bundle job {job_id} returned unexpected state {other}"),
        }
    };

    let output_path = completed_job
        .get("output_path")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("completed support bundle job missing output_path"))?;
    assert!(
        completed_job.get("started_at_unix_ms").and_then(Value::as_i64).is_some(),
        "completed support bundle job should record started_at_unix_ms"
    );
    assert!(
        completed_job.get("completed_at_unix_ms").and_then(Value::as_i64).is_some(),
        "completed support bundle job should record completed_at_unix_ms"
    );
    assert!(
        PathBuf::from(output_path).is_file(),
        "support bundle output path should exist on disk: {output_path}"
    );
    let bundle_contents = fs::read_to_string(output_path)
        .with_context(|| format!("failed to read support bundle output {output_path}"))?;
    assert!(
        bundle_contents.contains("\"generated_at_unix_ms\""),
        "support bundle export should persist a structured json report"
    );
    assert!(
        !bundle_contents.contains(ADMIN_TOKEN),
        "support bundle contents must not leak the admin token"
    );
    assert!(
        completed_job
            .get("command_output")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.contains(ADMIN_TOKEN)),
        "support bundle command output must remain redacted"
    );
    assert!(
        seen_states.iter().any(|state| state == "succeeded"),
        "support bundle lifecycle should converge to succeeded: {:?}",
        seen_states
    );

    let listed = client
        .get(format!("http://127.0.0.1:{admin_port}/console/v1/support-bundle/jobs"))
        .header("Cookie", cookie)
        .send()
        .context("failed to list support bundle jobs")?
        .error_for_status()
        .context("support bundle jobs list returned non-success status")?
        .json::<Value>()
        .context("failed to parse support bundle jobs list response json")?;
    let listed_job = listed
        .get("jobs")
        .and_then(Value::as_array)
        .and_then(|jobs| {
            jobs.iter()
                .find(|job| job.get("job_id").and_then(Value::as_str) == Some(job_id.as_str()))
        })
        .ok_or_else(|| {
            anyhow::anyhow!("support bundle jobs list did not include created job {job_id}")
        })?;
    assert_eq!(
        listed_job.get("state").and_then(Value::as_str),
        Some("succeeded"),
        "support bundle jobs list should publish the terminal job state"
    );

    Ok(())
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16)> {
    spawn_palyrad_with_dynamic_ports_with_env(&[])
}

fn spawn_palyrad_with_bound_console_principal(principal: &str) -> Result<(Child, u16)> {
    spawn_palyrad_with_dynamic_ports_with_env(&[("PALYRA_ADMIN_BOUND_PRINCIPAL", principal)])
}

fn spawn_palyrad_with_config_and_env(
    config_toml: &str,
    extra_env: &[(&str, &str)],
) -> Result<(Child, u16)> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=PALYRAD_STARTUP_ATTEMPTS {
        match spawn_palyrad_with_config_and_env_once(config_toml, extra_env) {
            Ok((mut child, admin_port)) => match wait_for_health(admin_port, &mut child) {
                Ok(()) => return Ok((child, admin_port)),
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    last_error = Some(error);
                }
            },
            Err(error) => {
                last_error = Some(error);
            }
        }
        if attempt < PALYRAD_STARTUP_ATTEMPTS {
            thread::sleep(PALYRAD_STARTUP_RETRY_DELAY);
        }
    }
    let Some(last_error) = last_error else {
        anyhow::bail!("failed to spawn palyrad with custom config for admin surface tests");
    };
    Err(last_error).context(format!(
        "failed to spawn palyrad with custom config after {PALYRAD_STARTUP_ATTEMPTS} startup attempts"
    ))
}

fn spawn_palyrad_with_dynamic_ports_with_env(extra_env: &[(&str, &str)]) -> Result<(Child, u16)> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=PALYRAD_STARTUP_ATTEMPTS {
        match spawn_palyrad_with_dynamic_ports_once(extra_env) {
            Ok((mut child, admin_port)) => match wait_for_health(admin_port, &mut child) {
                Ok(()) => return Ok((child, admin_port)),
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    last_error = Some(error);
                }
            },
            Err(error) => {
                last_error = Some(error);
            }
        }
        if attempt < PALYRAD_STARTUP_ATTEMPTS {
            thread::sleep(PALYRAD_STARTUP_RETRY_DELAY);
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
    spawn_palyrad_with_config_and_env_once("version = 1\n", extra_env)
}

fn spawn_palyrad_with_config_and_env_once(
    config_toml: &str,
    extra_env: &[(&str, &str)],
) -> Result<(Child, u16)> {
    let state_root_dir = unique_temp_state_root_dir();
    let journal_db_path = unique_temp_journal_db_path();
    let identity_store_dir = state_root_dir.join("identity");
    let vault_dir = state_root_dir.join("vault");
    let default_config_path = unique_temp_config_path();
    let admin_port = reserve_loopback_port()?;
    fs::create_dir_all(&identity_store_dir).with_context(|| {
        format!("failed to create test identity dir {}", identity_store_dir.display())
    })?;
    prepare_test_vault_dir(&vault_dir)?;
    write_test_config(&default_config_path, config_toml)?;
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
        .env("PALYRA_CONFIG", default_config_path.to_string_lossy().to_string())
        .env("PALYRA_STATE_ROOT", state_root_dir.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
        .env("PALYRA_GATEWAY_QUIC_PORT", "0")
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let child = command.spawn().context("failed to start palyrad")?;
    Ok((child, admin_port))
}

fn wait_for_admin_journal_event(
    client: &Client,
    admin_port: u16,
    event_name: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(12);
    while Instant::now() < deadline {
        let response = client
            .get(format!("http://127.0.0.1:{admin_port}/admin/v1/journal/recent?limit=64"))
            .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
            .header("x-palyra-principal", CONSOLE_ADMIN_PRINCIPAL)
            .header("x-palyra-device-id", DEVICE_ID)
            .header("x-palyra-channel", "cli")
            .send()
            .context("failed to poll admin journal recent for expected hook event")?
            .error_for_status()
            .context("admin journal recent poll returned non-success status")?
            .json::<Value>()
            .context("failed to parse admin journal recent response json")?;
        if let Some(events) = response.get("events").and_then(Value::as_array) {
            for event in events {
                let Some(payload_json) = event.get("payload_json").and_then(Value::as_str) else {
                    continue;
                };
                let Ok(payload) = serde_json::from_str::<Value>(payload_json) else {
                    continue;
                };
                if payload.get("event").and_then(Value::as_str) == Some(event_name) {
                    return Ok(payload);
                }
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
    anyhow::bail!("timed out waiting for journal event {event_name}");
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

fn unique_temp_state_root_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-state-root-{nonce}-{}-{counter}", std::process::id()))
}

fn unique_temp_config_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-config-{nonce}-{}-{counter}.toml", std::process::id()))
}

fn unique_temp_skill_artifact_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-admin-skill-{nonce}-{}-{counter}.palyra-skill", std::process::id()))
}

fn build_test_skill_artifact(skill_id: &str, version: &str) -> Result<Vec<u8>> {
    let manifest_toml = format!(
        r#"
manifest_version = 1
skill_id = "{skill_id}"
name = "Hook Skill"
version = "{version}"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.hook_run"
name = "hook_run"
description = "Run the hook entrypoint"
input_schema = {{ type = "object" }}
output_schema = {{ type = "object" }}
risk = {{ default_sensitive = false, requires_approval = false }}

[capabilities.filesystem]
read_roots = []
write_roots = []

[capabilities]
http_egress_allowlist = []
device_capabilities = []
node_capabilities = []

[capabilities.quotas]
wall_clock_timeout_ms = 2000
fuel_budget = 500000
max_memory_bytes = 1048576

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"
"#
    );
    let output =
        build_signed_skill_artifact(SkillArtifactBuildRequest {
            manifest_toml,
            modules: vec![ArtifactFile {
                path: "module.wasm".to_owned(),
                bytes: br#"(module (func (export "run") (result i32) i32.const 7))"#.to_vec(),
            }],
            assets: Vec::new(),
            sbom_cyclonedx_json:
                br#"{"bomFormat":"CycloneDX","specVersion":"1.5","version":1,"components":[]}"#
                    .to_vec(),
            provenance_json:
                br#"{"builder":{"id":"palyra-test"},"subject":[{"name":"modules/module.wasm"}]}"#
                    .to_vec(),
            signing_key: [7_u8; 32],
        })
        .context("failed to build signed test skill artifact")?;
    Ok(output.artifact_bytes)
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

fn prepare_test_vault_dir(vault_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(vault_dir)
        .with_context(|| format!("failed to create test vault dir {}", vault_dir.display()))?;
    let backend_marker = vault_dir.join("backend.kind");
    fs::write(&backend_marker, b"encrypted_file").with_context(|| {
        format!("failed to write vault backend marker {}", backend_marker.display())
    })?;
    Ok(())
}

fn write_test_config(config_path: &PathBuf, contents: &str) -> Result<()> {
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create test config dir {}", parent.display()))?;
    }
    fs::write(config_path, contents)
        .with_context(|| format!("failed to write test config {}", config_path.display()))?;
    Ok(())
}

fn build_test_webhook_payload(source: &str, event: &str, signature: Option<&str>) -> Vec<u8> {
    let timestamp_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis() as u64;
    let nonce = format!(
        "{timestamp_unix_ms:016x}{:016x}",
        TEMP_IDENTITY_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let signature_field =
        signature.map(|value| format!(r#","signature":"{value}""#)).unwrap_or_default();
    format!(
        r#"{{
            "v": 1,
            "id": "{DEVICE_ID}",
            "event": "{event}",
            "source": "{source}",
            "payload": {{"channel": "C123", "text": "hello"}},
            "replay_protection": {{
                "nonce": "{nonce}",
                "timestamp_unix_ms": {timestamp_unix_ms}
                {signature_field}
            }}
        }}"#
    )
    .into_bytes()
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
        .context("failed to build HTTP client")?;

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
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
