use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const CONSOLE_ADMIN_PRINCIPAL: &str = "admin:web-console";
const PALYRAD_STARTUP_ATTEMPTS: usize = 3;
const PALYRAD_STARTUP_RETRY_DELAY: Duration = Duration::from_millis(150);
static TEMP_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn console_openai_api_key_flow_persists_vault_refs_and_default_selection() -> Result<()> {
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-live-openai");

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "OpenAI Production",
            "scope": { "kind": "global" },
            "api_key": "sk-live-openai",
            "set_default": true
        }),
    )?;
    let profile_id = connected
        .get("profile_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("api-key connect response missing profile_id"))?
        .to_owned();
    assert_eq!(
        connected.get("action").and_then(Value::as_str),
        Some("api_key"),
        "api-key connect should identify the action"
    );
    assert_eq!(
        connected.get("state").and_then(Value::as_str),
        Some("selected"),
        "api-key connect with set_default=true should select the profile"
    );

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/openai", &cookie)?;
    assert_eq!(
        provider_state.get("default_profile_id").and_then(Value::as_str),
        Some(profile_id.as_str()),
        "provider state should publish the selected default profile"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    let profile = find_profile(&profiles, profile_id.as_str())?;
    assert_eq!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("type"))
            .and_then(Value::as_str),
        Some("api_key"),
        "stored profile should preserve api_key credential type"
    );
    let vault_ref = profile
        .get("credential")
        .and_then(|credential| credential.get("api_key_vault_ref"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("api-key credential is missing api_key_vault_ref"))?;
    assert!(
        vault_ref.contains("openai"),
        "api-key credential should be stored through an OpenAI-scoped vault ref: {vault_ref}"
    );
    assert!(
        !profile.to_string().contains("sk-live-openai"),
        "auth profile payload must not leak the raw API key"
    );

    let config = post_console_json(
        &client,
        admin_port,
        "/console/v1/config/inspect",
        &cookie,
        &csrf_token,
        &json!({}),
    )?;
    let document_toml = config
        .get("document_toml")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("config inspect response missing document_toml"))?;
    assert!(
        document_toml.contains("auth_profile_id"),
        "config inspect should persist model_provider.auth_profile_id after default selection"
    );
    assert!(
        document_toml.contains(profile_id.as_str()),
        "config inspect should point model_provider.auth_profile_id at the selected auth profile"
    );
    assert!(
        !document_toml.contains("sk-live-openai"),
        "config inspect must not leak the raw OpenAI API key"
    );

    let audit =
        get_console_json(&client, admin_port, "/console/v1/audit/events?limit=50", &cookie)?;
    assert!(
        audit.to_string().contains("auth.profile.default_selected"),
        "audit stream should record default profile selection after api-key connect"
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot.model_request_paths.iter().any(|path| path == "/v1/models"),
        "OpenAI credential validation must target /v1/models, not a root /models endpoint: {:?}",
        mock_snapshot.model_request_paths
    );

    Ok(())
}

#[test]
fn console_openai_api_key_flow_surfaces_invalid_credentials() -> Result<()> {
    let mock = OpenAiMockServer::new(None, None)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let response = client
        .post(console_url(admin_port, "/console/v1/auth/providers/openai/api-key"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&json!({
            "profile_name": "OpenAI Invalid",
            "scope": { "kind": "global" },
            "api_key": "sk-invalid",
            "set_default": false
        }))
        .send()
        .context("failed to submit invalid OpenAI API key")?;
    assert_eq!(
        response.status().as_u16(),
        400,
        "invalid OpenAI API key should fail closed with HTTP 400"
    );
    let error =
        response.json::<Value>().context("failed to parse invalid api-key error response json")?;
    assert_eq!(
        error.get("code").and_then(Value::as_str),
        Some("validation_error"),
        "invalid API key should surface the normalized validation error envelope"
    );
    assert!(
        error
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|message| message.to_ascii_lowercase().contains("invalid")),
        "invalid API key should explain the provider credential failure: {error}"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    assert!(
        profiles
            .get("profiles")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.is_empty()),
        "failed API key validation must not persist a partial auth profile"
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot.model_request_paths.iter().any(|path| path == "/v1/models"),
        "invalid API key validation must still target /v1/models: {:?}",
        mock_snapshot.model_request_paths
    );

    Ok(())
}

#[test]
fn console_openai_oauth_flow_supports_happy_path_refresh_reconnect_and_revoke() -> Result<()> {
    let mock = OpenAiMockServer::new(
        Some(TokenReply {
            access_token: "oauth-access-1".to_owned(),
            refresh_token: "oauth-refresh-1".to_owned(),
            expires_in_seconds: Some(0),
        }),
        Some(TokenReply {
            access_token: "oauth-access-2".to_owned(),
            refresh_token: "oauth-refresh-2".to_owned(),
            expires_in_seconds: Some(3600),
        }),
    )?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_OPENAI_OAUTH_AUTHORIZATION_ENDPOINT".to_owned(), mock.authorization_endpoint()),
        ("PALYRA_OPENAI_OAUTH_TOKEN_ENDPOINT".to_owned(), mock.token_endpoint()),
        ("PALYRA_OPENAI_OAUTH_REVOCATION_ENDPOINT".to_owned(), mock.revocation_endpoint()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let bootstrap = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/bootstrap",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "OpenAI OAuth",
            "scope": { "kind": "global" },
            "client_id": "client-live-123",
            "client_secret": "client-secret-live",
            "scopes": ["openid", "offline_access"],
            "set_default": true
        }),
    )?;
    let attempt_id = bootstrap
        .get("attempt_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing attempt_id"))?
        .to_owned();
    let profile_id = bootstrap
        .get("profile_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing profile_id"))?
        .to_owned();
    let authorization_url = bootstrap
        .get("authorization_url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing authorization_url"))?;
    assert!(
        authorization_url.contains("client_id=client-live-123")
            && authorization_url.contains(&format!("state={attempt_id}")),
        "oauth bootstrap should issue a usable authorization URL: {authorization_url}"
    );

    let pending = get_console_json(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
    )?;
    assert_eq!(
        pending.get("state").and_then(Value::as_str),
        Some("pending"),
        "callback state should report pending before the OAuth callback arrives"
    );

    let callback_html = client
        .get(console_url(
            admin_port,
            format!(
                "/console/v1/auth/providers/openai/callback?state={attempt_id}&code=oauth-code-1"
            )
            .as_str(),
        ))
        .send()
        .context("failed to submit OpenAI OAuth callback")?
        .error_for_status()
        .context("OpenAI OAuth callback returned non-success status")?
        .text()
        .context("failed to read OpenAI OAuth callback HTML body")?;
    assert!(
        callback_html.contains("OpenAI Connected"),
        "oauth callback should render a success page after a valid callback"
    );

    let callback_state = get_console_json(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
    )?;
    assert_eq!(
        callback_state.get("state").and_then(Value::as_str),
        Some("succeeded"),
        "callback state should converge to succeeded after the OAuth callback completes"
    );
    assert_eq!(
        callback_state.get("profile_id").and_then(Value::as_str),
        Some(profile_id.as_str()),
        "callback state should retain the profile_id associated with the OAuth attempt"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    let profile = find_profile(&profiles, profile_id.as_str())?;
    assert_eq!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("type"))
            .and_then(Value::as_str),
        Some("oauth"),
        "successful OAuth callback should persist an oauth credential profile"
    );
    assert_eq!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("client_id"))
            .and_then(Value::as_str),
        Some("client-live-123"),
        "OAuth credential should preserve the operator-supplied client_id"
    );
    assert!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("access_token_vault_ref"))
            .and_then(Value::as_str)
            .is_some(),
        "OAuth access tokens must be stored through a vault ref"
    );
    assert!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("refresh_token_vault_ref"))
            .and_then(Value::as_str)
            .is_some(),
        "OAuth refresh tokens must be stored through a vault ref"
    );
    assert!(
        profile
            .get("credential")
            .and_then(|credential| credential.get("client_secret_vault_ref"))
            .and_then(Value::as_str)
            .is_some(),
        "OAuth client secrets must be stored through a vault ref when provided"
    );
    assert!(
        !profile.to_string().contains("client-secret-live")
            && !profile.to_string().contains("oauth-access-1")
            && !profile.to_string().contains("oauth-refresh-1"),
        "OAuth profile JSON must not leak raw provider secrets"
    );

    let refresh = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/refresh",
        &cookie,
        &csrf_token,
        &json!({ "profile_id": profile_id }),
    )?;
    assert_eq!(
        refresh.get("state").and_then(Value::as_str),
        Some("refreshed"),
        "expired OAuth credentials should refresh immediately through the M54 refresh action"
    );

    let reconnect = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/reconnect",
        &cookie,
        &csrf_token,
        &json!({ "profile_id": profile_id }),
    )?;
    assert_eq!(
        reconnect.get("profile_id").and_then(Value::as_str),
        Some(profile_id.as_str()),
        "reconnect should target the stored OpenAI OAuth profile"
    );
    assert!(
        reconnect
            .get("authorization_url")
            .and_then(Value::as_str)
            .is_some_and(|url| url.contains("client_id=client-live-123")),
        "reconnect should bootstrap OAuth using the stored client_id"
    );

    let revoked = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/revoke",
        &cookie,
        &csrf_token,
        &json!({ "profile_id": profile_id }),
    )?;
    assert_eq!(
        revoked.get("state").and_then(Value::as_str),
        Some("revoked"),
        "oauth revoke should delete the profile after remote revocation succeeds"
    );

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/openai", &cookie)?;
    assert!(
        provider_state.get("default_profile_id").is_none(),
        "revoking the selected profile should clear model_provider.auth_profile_id"
    );
    assert!(
        provider_state.get("available_profile_ids").and_then(Value::as_array).is_some_and(
            |entries| { entries.iter().all(|entry| entry.as_str() != Some(profile_id.as_str())) }
        ),
        "revoked profile should disappear from the provider state profile list"
    );

    let audit =
        get_console_json(&client, admin_port, "/console/v1/audit/events?limit=100", &cookie)?;
    let audit_blob = audit.to_string();
    assert!(
        audit_blob.contains("auth.profile.revoked") && audit_blob.contains("auth.token.refreshed"),
        "audit stream should capture OAuth refresh and revoke lifecycle events"
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot
            .token_request_bodies
            .iter()
            .any(|body| body.contains("grant_type=authorization_code")),
        "oauth mock should observe an authorization_code exchange: {:?}",
        mock_snapshot.token_request_bodies
    );
    assert!(
        mock_snapshot
            .token_request_bodies
            .iter()
            .any(|body| body.contains("grant_type=refresh_token")),
        "oauth mock should observe a refresh_token exchange after refresh action: {:?}",
        mock_snapshot.token_request_bodies
    );
    assert!(
        mock_snapshot
            .revoke_request_bodies
            .iter()
            .any(|body| {
                body.contains("token=oauth-refresh-2") || body.contains("token=oauth-refresh-1")
            }),
        "oauth revoke should call the remote revocation endpoint with the stored refresh token: {:?}",
        mock_snapshot.revoke_request_bodies
    );
    assert!(
        mock_snapshot.model_request_paths.iter().all(|path| path == "/v1/models"),
        "OAuth credential validation should consistently target /v1/models: {:?}",
        mock_snapshot.model_request_paths
    );

    Ok(())
}

#[test]
fn console_openai_oauth_callback_denial_persists_failed_attempt_state() -> Result<()> {
    let mock = OpenAiMockServer::new(None, None)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_OPENAI_OAUTH_AUTHORIZATION_ENDPOINT".to_owned(), mock.authorization_endpoint()),
        ("PALYRA_OPENAI_OAUTH_TOKEN_ENDPOINT".to_owned(), mock.token_endpoint()),
        ("PALYRA_OPENAI_OAUTH_REVOCATION_ENDPOINT".to_owned(), mock.revocation_endpoint()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let bootstrap = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/bootstrap",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "OpenAI Denied",
            "scope": { "kind": "global" },
            "client_id": "client-denied",
            "client_secret": "client-secret-denied",
            "scopes": ["openid", "offline_access"],
            "set_default": false
        }),
    )?;
    let attempt_id = bootstrap
        .get("attempt_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing attempt_id"))?
        .to_owned();

    let denied_html = client
        .get(console_url(
            admin_port,
            format!(
                "/console/v1/auth/providers/openai/callback?state={attempt_id}&error=access_denied&error_description=User%20denied"
            )
            .as_str(),
        ))
        .send()
        .context("failed to submit denied OpenAI OAuth callback")?
        .error_for_status()
        .context("denied OpenAI OAuth callback returned non-success status")?
        .text()
        .context("failed to read denied OpenAI OAuth callback HTML body")?;
    assert!(
        denied_html.contains("OpenAI Connection Failed"),
        "denied callback should render a failure page for the operator"
    );

    let callback_state = get_console_json(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
    )?;
    assert_eq!(
        callback_state.get("state").and_then(Value::as_str),
        Some("failed"),
        "callback-state endpoint should surface the denied OAuth attempt as failed"
    );
    assert!(
        callback_state
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("access_denied")),
        "denied callback state should preserve the provider denial reason: {callback_state}"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    assert!(
        profiles
            .get("profiles")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.is_empty()),
        "denied OAuth callback must not persist an auth profile"
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot.token_request_bodies.is_empty(),
        "denied OAuth callback should not attempt a token exchange: {:?}",
        mock_snapshot.token_request_bodies
    );

    Ok(())
}

#[derive(Debug, Clone)]
struct TokenReply {
    access_token: String,
    refresh_token: String,
    expires_in_seconds: Option<u64>,
}

#[derive(Debug, Default, Clone)]
struct OpenAiMockSnapshot {
    model_request_paths: Vec<String>,
    token_request_bodies: Vec<String>,
    revoke_request_bodies: Vec<String>,
}

#[derive(Debug, Default)]
struct OpenAiMockState {
    valid_tokens: HashSet<String>,
    model_request_paths: Vec<String>,
    token_request_bodies: Vec<String>,
    revoke_request_bodies: Vec<String>,
    authorization_code_reply: Option<TokenReply>,
    refresh_reply: Option<TokenReply>,
}

struct OpenAiMockServer {
    base_url: String,
    authorization_endpoint: String,
    token_endpoint: String,
    revocation_endpoint: String,
    state: Arc<Mutex<OpenAiMockState>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl OpenAiMockServer {
    fn new(
        authorization_code_reply: Option<TokenReply>,
        refresh_reply: Option<TokenReply>,
    ) -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind OpenAI mock listener")?;
        listener
            .set_nonblocking(true)
            .context("failed to set OpenAI mock listener non-blocking")?;
        let address =
            listener.local_addr().context("failed to resolve OpenAI mock listener address")?;
        let base_url = format!("http://{}:{}", address.ip(), address.port());
        let state = Arc::new(Mutex::new(OpenAiMockState {
            authorization_code_reply,
            refresh_reply,
            ..OpenAiMockState::default()
        }));
        let state_for_worker = Arc::clone(&state);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_worker = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !stop_for_worker.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = handle_openai_mock_request(&mut stream, &state_for_worker);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            authorization_endpoint: format!("{base_url}/authorize"),
            token_endpoint: format!("{base_url}/oauth/token"),
            revocation_endpoint: format!("{base_url}/oauth/revoke"),
            base_url,
            state,
            stop,
            worker: Some(worker),
        })
    }

    fn allow_token(&self, token: &str) {
        let mut state = self.state.lock().expect("OpenAI mock state lock should be available");
        state.valid_tokens.insert(token.to_owned());
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    fn authorization_endpoint(&self) -> String {
        self.authorization_endpoint.clone()
    }

    fn token_endpoint(&self) -> String {
        self.token_endpoint.clone()
    }

    fn revocation_endpoint(&self) -> String {
        self.revocation_endpoint.clone()
    }

    fn snapshot(&self) -> OpenAiMockSnapshot {
        let state = self.state.lock().expect("OpenAI mock state lock should be available");
        OpenAiMockSnapshot {
            model_request_paths: state.model_request_paths.clone(),
            token_request_bodies: state.token_request_bodies.clone(),
            revoke_request_bodies: state.revoke_request_bodies.clone(),
        }
    }
}

impl Drop for OpenAiMockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug)]
struct HttpRequest {
    request_line: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

fn handle_openai_mock_request(
    stream: &mut TcpStream,
    state: &Arc<Mutex<OpenAiMockState>>,
) -> Result<()> {
    let request = read_http_request(stream)?;
    if request.request_line.starts_with("GET /v1/models ") {
        let authorization =
            request.headers.get("authorization").map(String::as_str).unwrap_or_default();
        let bearer_token =
            authorization.strip_prefix("Bearer ").map(str::trim).unwrap_or_default().to_owned();
        let authorized = {
            let mut guard = state.lock().expect("OpenAI mock state lock should be available");
            guard.model_request_paths.push(request.path);
            guard.valid_tokens.contains(&bearer_token)
        };
        if authorized {
            write_json_response(stream, "200 OK", r#"{"data":[]}"#)?;
        } else {
            write_json_response(stream, "401 Unauthorized", r#"{"error":"invalid_api_key"}"#)?;
        }
        return Ok(());
    }

    if request.request_line.starts_with("POST /oauth/token ") {
        let mut guard = state.lock().expect("OpenAI mock state lock should be available");
        guard.token_request_bodies.push(request.body.clone());
        let reply = if request.body.contains("grant_type=authorization_code") {
            guard.authorization_code_reply.clone()
        } else if request.body.contains("grant_type=refresh_token") {
            guard.refresh_reply.clone()
        } else {
            None
        };
        let Some(reply) = reply else {
            write_json_response(
                stream,
                "400 Bad Request",
                r#"{"error":"unsupported_grant_type"}"#,
            )?;
            return Ok(());
        };
        guard.valid_tokens.insert(reply.access_token.clone());
        let payload = json!({
            "access_token": reply.access_token,
            "refresh_token": reply.refresh_token,
            "expires_in": reply.expires_in_seconds
        })
        .to_string();
        write_json_response(stream, "200 OK", payload.as_str())?;
        return Ok(());
    }

    if request.request_line.starts_with("POST /oauth/revoke ") {
        let mut guard = state.lock().expect("OpenAI mock state lock should be available");
        guard.revoke_request_bodies.push(request.body);
        write_json_response(stream, "200 OK", r#"{}"#)?;
        return Ok(());
    }

    write_json_response(stream, "404 Not Found", r#"{"error":"not_found"}"#)
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .context("failed to set OpenAI mock stream read timeout")?;
    let mut buffer = Vec::new();
    let mut header_end = None;
    while header_end.is_none() {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).context("failed to read OpenAI mock request bytes")?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    }
    let Some(header_end) = header_end else {
        anyhow::bail!("OpenAI mock request did not include HTTP headers");
    };
    let header_bytes = &buffer[..header_end];
    let mut body_bytes = buffer[(header_end + 4)..].to_vec();
    let header_text = String::from_utf8(header_bytes.to_vec())
        .context("OpenAI mock request headers were not valid UTF-8")?;
    let mut lines = header_text.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("OpenAI mock request is missing request-line"))?
        .to_owned();
    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("OpenAI mock request-line is missing path"))?
        .to_owned();

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let normalized_name = name.trim().to_ascii_lowercase();
        let normalized_value = value.trim().to_owned();
        if normalized_name == "content-length" {
            content_length = normalized_value.parse::<usize>().unwrap_or_default();
        }
        headers.insert(normalized_name, normalized_value);
    }

    while body_bytes.len() < content_length {
        let mut chunk = vec![0_u8; content_length.saturating_sub(body_bytes.len())];
        let read = stream
            .read(chunk.as_mut_slice())
            .context("failed to read OpenAI mock request body bytes")?;
        if read == 0 {
            break;
        }
        body_bytes.extend_from_slice(&chunk[..read]);
    }
    body_bytes.truncate(content_length);
    let body =
        String::from_utf8(body_bytes).context("OpenAI mock request body was not valid UTF-8")?;

    Ok(HttpRequest { request_line, path, headers, body })
}

fn write_json_response(stream: &mut TcpStream, status_line: &str, body: &str) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).context("failed to write OpenAI mock response")?;
    stream.flush().context("failed to flush OpenAI mock response")
}

fn http_client() -> Result<Client> {
    Client::builder().timeout(Duration::from_secs(4)).build().context("failed to build HTTP client")
}

fn get_console_json(client: &Client, admin_port: u16, path: &str, cookie: &str) -> Result<Value> {
    client
        .get(console_url(admin_port, path))
        .header("Cookie", cookie)
        .send()
        .with_context(|| format!("failed to GET console path {path}"))?
        .error_for_status()
        .with_context(|| format!("console GET {path} returned non-success status"))?
        .json::<Value>()
        .with_context(|| format!("failed to parse console GET {path} response json"))
}

fn post_console_json(
    client: &Client,
    admin_port: u16,
    path: &str,
    cookie: &str,
    csrf_token: &str,
    payload: &Value,
) -> Result<Value> {
    client
        .post(console_url(admin_port, path))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .json(payload)
        .send()
        .with_context(|| format!("failed to POST console path {path}"))?
        .error_for_status()
        .with_context(|| format!("console POST {path} returned non-success status"))?
        .json::<Value>()
        .with_context(|| format!("failed to parse console POST {path} response json"))
}

fn find_profile<'a>(profiles: &'a Value, profile_id: &str) -> Result<&'a Value> {
    profiles
        .get("profiles")
        .and_then(Value::as_array)
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.get("profile_id").and_then(Value::as_str) == Some(profile_id))
        })
        .ok_or_else(|| anyhow::anyhow!("auth profile {profile_id} was not found"))
}

fn console_url(admin_port: u16, path: &str) -> String {
    format!("http://127.0.0.1:{admin_port}{path}")
}

fn spawn_palyrad_with_dynamic_ports(extra_env: &[(String, String)]) -> Result<(Child, u16)> {
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
        anyhow::bail!("failed to spawn palyrad for OpenAI auth surface tests");
    };
    Err(last_error).context(format!(
        "failed to spawn palyrad after {PALYRAD_STARTUP_ATTEMPTS} startup attempts"
    ))
}

fn spawn_palyrad_with_dynamic_ports_once(extra_env: &[(String, String)]) -> Result<(Child, u16)> {
    let journal_db_path = unique_temp_path("palyra-openai-auth-journal", "sqlite3");
    let identity_store_dir = unique_temp_dir("palyra-openai-auth-identity");
    let vault_dir = unique_temp_dir("palyra-openai-auth-vault");
    let config_path = unique_temp_path("palyra-openai-auth-config", "toml");
    prepare_test_vault_dir(&vault_dir)?;
    prepare_test_config(&config_path)?;

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
        .env("PALYRA_CONFIG", config_path.to_string_lossy().to_string())
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

fn login_console_session(
    client: &Client,
    admin_port: u16,
    principal: &str,
) -> Result<(String, String)> {
    let response = client
        .post(console_url(admin_port, "/console/v1/auth/login"))
        .json(&json!({
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

fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("{prefix}-{nonce}-{}-{counter}.{extension}", std::process::id()))
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{nonce}-{}-{counter}", std::process::id()))
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

fn prepare_test_config(config_path: &PathBuf) -> Result<()> {
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create test config dir {}", parent.display()))?;
    }
    fs::write(config_path, b"version = 1\n")
        .with_context(|| format!("failed to write test config file {}", config_path.display()))?;
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
