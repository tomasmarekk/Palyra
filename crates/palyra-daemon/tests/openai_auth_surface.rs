use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex, MutexGuard, OnceLock,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use reqwest::Url;
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const CONSOLE_ADMIN_PRINCIPAL: &str = "admin:web-console";
const PALYRAD_STARTUP_ATTEMPTS: usize = 3;
const PALYRAD_STARTUP_RETRY_DELAY: Duration = Duration::from_millis(150);
static TEMP_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn console_openai_api_key_flow_persists_vault_refs_and_default_selection() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-live-openai");
    wait_for_openai_mock_ready(&mock)?;

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
fn console_anthropic_api_key_flow_persists_vault_refs_and_default_selection() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-live-anthropic");
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL".to_owned(), mock.base_url()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/anthropic/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "Anthropic Production",
            "scope": { "kind": "global" },
            "api_key": "sk-live-anthropic",
            "set_default": true
        }),
    )?;
    let profile_id = connected
        .get("profile_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("api-key connect response missing profile_id"))?
        .to_owned();
    assert_eq!(connected.get("provider").and_then(Value::as_str), Some("anthropic"));
    assert_eq!(connected.get("state").and_then(Value::as_str), Some("selected"));

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/anthropic", &cookie)?;
    assert_eq!(
        provider_state.get("default_profile_id").and_then(Value::as_str),
        Some(profile_id.as_str()),
        "Anthropic provider state should publish the selected default profile"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    let profile = find_profile(&profiles, profile_id.as_str())?;
    assert_eq!(
        profile.get("provider").and_then(|provider| provider.get("kind")).and_then(Value::as_str),
        Some("anthropic"),
        "stored profile should preserve anthropic provider kind"
    );
    let vault_ref = profile
        .get("credential")
        .and_then(|credential| credential.get("api_key_vault_ref"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("anthropic credential is missing api_key_vault_ref"))?;
    assert!(
        vault_ref.contains("anthropic"),
        "Anthropic API key should be stored through an anthropic-scoped vault ref: {vault_ref}"
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
        document_toml.contains("kind = \"anthropic\""),
        "default selection should switch the model provider kind to anthropic: {document_toml}"
    );
    assert!(
        document_toml.contains("auth_provider_kind = \"anthropic\""),
        "default selection should persist the anthropic auth provider kind: {document_toml}"
    );
    assert!(
        !document_toml.contains("sk-live-anthropic"),
        "config inspect must not leak the raw Anthropic API key"
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot.model_request_paths.iter().any(|path| path == "/v1/models"),
        "Anthropic credential validation must target /v1/models: {:?}",
        mock_snapshot.model_request_paths
    );

    Ok(())
}

#[test]
fn console_models_probe_and_discover_publish_live_openai_results() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-probe-openai");
    mock.set_models_response_body(
        r#"{"data":[{"id":"gpt-4.1-mini"},{"id":"text-embedding-3-large"}]}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "OpenAI Probe",
            "scope": { "kind": "global" },
            "api_key": "sk-probe-openai",
            "set_default": true
        }),
    )?;

    let probe = post_console_json(
        &client,
        admin_port,
        "/console/v1/models/test-connection",
        &cookie,
        &csrf_token,
        &json!({
            "provider_id": "openai-primary",
            "timeout_ms": 5000
        }),
    )?;
    assert_eq!(probe.get("mode").and_then(Value::as_str), Some("test_connection"));
    assert_eq!(probe.get("provider_count").and_then(Value::as_u64), Some(1));
    let probe_provider = probe
        .get("providers")
        .and_then(Value::as_array)
        .and_then(|providers| providers.first())
        .ok_or_else(|| anyhow::anyhow!("probe response missing provider payload"))?;
    assert_eq!(probe_provider.get("state").and_then(Value::as_str), Some("ok"));
    assert_eq!(
        probe_provider.get("message").and_then(Value::as_str),
        Some("provider connection succeeded")
    );
    assert_eq!(
        probe_provider.get("credential_source").and_then(Value::as_str),
        Some("auth_profile")
    );

    let discovery = post_console_json(
        &client,
        admin_port,
        "/console/v1/models/discover",
        &cookie,
        &csrf_token,
        &json!({
            "provider_id": "openai-primary",
            "timeout_ms": 5000
        }),
    )?;
    assert_eq!(discovery.get("mode").and_then(Value::as_str), Some("discover"));
    let discovered_provider = discovery
        .get("providers")
        .and_then(Value::as_array)
        .and_then(|providers| providers.first())
        .ok_or_else(|| anyhow::anyhow!("discover response missing provider payload"))?;
    assert_eq!(discovered_provider.get("state").and_then(Value::as_str), Some("ok"));
    assert_eq!(discovered_provider.get("discovery_source").and_then(Value::as_str), Some("live"));
    assert_eq!(
        discovered_provider
            .get("discovered_model_ids")
            .and_then(Value::as_array)
            .map(|entries| entries.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec!["gpt-4.1-mini", "text-embedding-3-large"]),
    );

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot
            .model_request_paths
            .iter()
            .filter(|path| path.as_str() == "/v1/models")
            .count()
            >= 3,
        "provider validation + probe + discovery should all hit /v1/models: {:?}",
        mock_snapshot.model_request_paths
    );

    Ok(())
}

#[test]
fn console_models_probe_redacts_provider_auth_failures() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-ant-invalid-secret");
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL".to_owned(), mock.base_url()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/anthropic/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "Anthropic Probe",
            "scope": { "kind": "global" },
            "api_key": "sk-ant-invalid-secret",
            "set_default": true
        }),
    )?;
    mock.remove_token("sk-ant-invalid-secret");

    let probe = post_console_json(
        &client,
        admin_port,
        "/console/v1/models/test-connection",
        &cookie,
        &csrf_token,
        &json!({
            "provider_id": "anthropic-primary",
            "timeout_ms": 5000
        }),
    )?;
    let provider = probe
        .get("providers")
        .and_then(Value::as_array)
        .and_then(|providers| providers.first())
        .ok_or_else(|| anyhow::anyhow!("probe response missing anthropic provider payload"))?;
    assert_eq!(provider.get("state").and_then(Value::as_str), Some("auth_failed"));
    let message = provider
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("probe response missing error message"))?;
    assert!(
        message.contains("HTTP 401"),
        "probe should preserve failure class without exposing the raw secret: {message}"
    );
    assert!(
        !message.contains("sk-ant-invalid-secret"),
        "probe payload must redact the provider credential: {message}"
    );

    Ok(())
}

#[test]
fn console_openai_default_selection_and_revoke_use_palyra_config_override() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-config-openai");
    wait_for_openai_mock_ready(&mock)?;

    let config_path = unique_temp_path("palyra-openai-config-override", "toml");
    prepare_test_config(&config_path)?;
    let mut extra_env = vec![
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_CONFIG".to_owned(), config_path.to_string_lossy().to_string()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ];
    extra_env.extend(isolated_default_config_env());

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports_once(&extra_env)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let profile_id = "openai-config-override";

    let connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_id": profile_id,
            "profile_name": "OpenAI Config Override",
            "scope": { "kind": "global" },
            "api_key": "sk-config-openai",
            "set_default": true
        }),
    )?;
    assert_eq!(
        connected.get("state").and_then(Value::as_str),
        Some("selected"),
        "api-key connect should report the profile as selected when set_default=true"
    );
    assert_eq!(
        read_config_profile_id(config_path.as_path())?,
        Some(profile_id.to_owned()),
        "default profile selection must be written into the PALYRA_CONFIG override file"
    );

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/openai", &cookie)?;
    assert_eq!(
        provider_state.get("default_profile_id").and_then(Value::as_str),
        Some(profile_id),
        "provider state must read default_profile_id from the PALYRA_CONFIG override file"
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
        "revoking the selected API-key profile should succeed"
    );
    assert_eq!(
        read_config_profile_id(config_path.as_path())?,
        None,
        "revoking the selected profile must clear model_provider.auth_profile_id in PALYRA_CONFIG"
    );

    Ok(())
}

#[test]
fn console_anthropic_api_key_flow_surfaces_invalid_credentials() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_ANTHROPIC_BASE_URL".to_owned(), mock.base_url()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let response = client
        .post(console_url(admin_port, "/console/v1/auth/providers/anthropic/api-key"))
        .header("Cookie", cookie.clone())
        .header("x-palyra-csrf-token", csrf_token.clone())
        .json(&json!({
            "profile_name": "Anthropic Invalid",
            "scope": { "kind": "global" },
            "api_key": "sk-invalid",
            "set_default": false
        }))
        .send()
        .context("failed to submit invalid Anthropic API key")?;
    let status = response.status();
    let error_body =
        response.text().context("failed to read invalid anthropic api-key error response body")?;
    assert_eq!(status.as_u16(), 400, "invalid Anthropic API key should fail closed: {error_body}");

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    assert!(
        profiles
            .get("profiles")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.is_empty()),
        "failed Anthropic API key validation must not persist a partial auth profile"
    );
    Ok(())
}

#[test]
fn console_openai_api_key_flow_surfaces_invalid_credentials() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    wait_for_openai_mock_ready(&mock)?;

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
    let status = response.status();
    let error_body =
        response.text().context("failed to read invalid api-key error response body")?;
    assert_eq!(
        status.as_u16(),
        400,
        "invalid OpenAI API key should fail closed with HTTP 400: {error_body}"
    );
    let error = serde_json::from_str::<Value>(&error_body)
        .context("failed to parse invalid api-key error response json")?;
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
fn console_openai_provider_mutations_require_console_session_and_csrf() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    wait_for_openai_mock_ready(&mock)?;

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
    let unauthorized_provider_state = client
        .get(console_url(admin_port, "/console/v1/auth/providers/openai"))
        .header("Authorization", "Bearer sk-live-openai")
        .send()
        .context("failed to call provider state without console session")?;
    assert_eq!(
        unauthorized_provider_state.status().as_u16(),
        403,
        "provider state must reject provider bearer tokens and still enforce the console session boundary"
    );

    let callback_state_without_session = client
        .get(console_url(
            admin_port,
            "/console/v1/auth/providers/openai/callback-state?attempt_id=missing",
        ))
        .header("Authorization", "Bearer oauth-provider-token")
        .send()
        .context("failed to call callback-state without console session")?;
    assert_eq!(
        callback_state_without_session.status().as_u16(),
        403,
        "callback-state endpoint must reject provider tokens and keep the console session boundary intact"
    );

    let (cookie, _csrf_token) =
        login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    let bootstrap_without_csrf = client
        .post(console_url(admin_port, "/console/v1/auth/providers/openai/bootstrap"))
        .header("Cookie", cookie.clone())
        .json(&json!({
            "profile_name": "OpenAI OAuth",
            "scope": { "kind": "global" },
            "client_id": "client-live-123",
            "client_secret": "client-secret-live",
            "scopes": ["openid", "offline_access"],
            "set_default": false
        }))
        .send()
        .context("failed to submit OpenAI OAuth bootstrap without CSRF")?;
    assert_eq!(
        bootstrap_without_csrf.status().as_u16(),
        403,
        "oauth bootstrap must enforce CSRF on an authenticated console session"
    );

    let api_key_without_csrf = client
        .post(console_url(admin_port, "/console/v1/auth/providers/openai/api-key"))
        .header("Cookie", cookie)
        .json(&json!({
            "profile_name": "OpenAI Production",
            "scope": { "kind": "global" },
            "api_key": "sk-live-openai",
            "set_default": false
        }))
        .send()
        .context("failed to submit OpenAI API key without CSRF")?;
    assert_eq!(
        api_key_without_csrf.status().as_u16(),
        403,
        "api-key connect must enforce CSRF on an authenticated console session"
    );

    Ok(())
}

#[test]
fn console_openai_oauth_flow_supports_happy_path_refresh_reconnect_and_revoke() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
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
    wait_for_openai_mock_ready(&mock)?;

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
    let callback_mock_snapshot = mock.snapshot();
    assert!(
        callback_html.contains("OpenAI Connected"),
        "oauth callback should render a success page after a valid callback: {callback_html}; mock={callback_mock_snapshot:?}"
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
        "expired OAuth credentials should refresh immediately through the M54 refresh action: {refresh}"
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
        mock_snapshot.request_errors.is_empty(),
        "oauth mock should not report request parsing/transport errors: {:?}",
        mock_snapshot.request_errors
    );
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
fn console_openai_oauth_bootstrap_uses_configured_remote_base_url_for_redirect_uri() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(
        Some(TokenReply {
            access_token: "oauth-access-1".to_owned(),
            refresh_token: "oauth-refresh-1".to_owned(),
            expires_in_seconds: Some(3600),
        }),
        None,
    )?;
    wait_for_openai_mock_ready(&mock)?;

    let config_path = unique_temp_path("palyra-openai-oauth-remote-base", "toml");
    prepare_test_config(&config_path)?;
    fs::write(
        &config_path,
        b"version = 1\n[gateway_access]\nremote_base_url = \"https://console.example.test/palyra\"\n",
    )
    .with_context(|| format!("failed to write test config file {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().to_string();
    let validation_base_url = format!("{}/v1", mock.base_url());
    let authorization_endpoint = mock.authorization_endpoint();
    let token_endpoint = mock.token_endpoint();
    let revocation_endpoint = mock.revocation_endpoint();

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports_once(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_CONFIG".to_owned(), config_path_string),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), validation_base_url),
        ("PALYRA_OPENAI_OAUTH_AUTHORIZATION_ENDPOINT".to_owned(), authorization_endpoint),
        ("PALYRA_OPENAI_OAUTH_TOKEN_ENDPOINT".to_owned(), token_endpoint),
        ("PALYRA_OPENAI_OAUTH_REVOCATION_ENDPOINT".to_owned(), revocation_endpoint),
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
            "set_default": false
        }),
    )?;
    let authorization_url = bootstrap
        .get("authorization_url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing authorization_url"))?;
    let redirect_uri = Url::parse(authorization_url)
        .context("authorization_url should parse")?
        .query_pairs()
        .find_map(|(key, value)| (key == "redirect_uri").then(|| value.into_owned()))
        .ok_or_else(|| anyhow::anyhow!("authorization_url missing redirect_uri query parameter"))?;
    assert_eq!(
        redirect_uri,
        "https://console.example.test/palyra/console/v1/auth/providers/openai/callback",
        "oauth bootstrap must derive redirect_uri from configured gateway_access.remote_base_url"
    );

    Ok(())
}

#[test]
fn console_openai_oauth_bootstrap_rejects_forwarded_host_without_trusted_remote_base_url(
) -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    wait_for_openai_mock_ready(&mock)?;

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
    let response = client
        .post(console_url(admin_port, "/console/v1/auth/providers/openai/bootstrap"))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
        .header("x-forwarded-host", "evil.example")
        .header("x-forwarded-proto", "https")
        .json(&json!({
            "profile_name": "OpenAI OAuth",
            "scope": { "kind": "global" },
            "client_id": "client-live-123",
            "client_secret": "client-secret-live",
            "scopes": ["openid", "offline_access"],
            "set_default": false
        }))
        .send()
        .context("failed to submit OpenAI OAuth bootstrap with spoofed forwarded host")?;
    assert_eq!(
        response.status().as_u16(),
        412,
        "spoofed forwarded host should be rejected unless gateway_access.remote_base_url is configured"
    );

    Ok(())
}

#[test]
fn console_openai_oauth_callback_rejects_malformed_token_response_without_persisting_profile(
) -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.set_authorization_code_raw_response(
        "200 OK",
        r#"{"access_token":"oauth-secret","refresh_token":"   ","expires_in":"oops"}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

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
            "profile_name": "OpenAI OAuth Invalid",
            "scope": { "kind": "global" },
            "client_id": "client-live-123",
            "client_secret": "client-secret-live",
            "scopes": ["openid", "offline_access"],
            "set_default": false
        }),
    )?;
    let attempt_id = bootstrap
        .get("attempt_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth bootstrap response missing attempt_id"))?
        .to_owned();

    let callback_html = client
        .get(console_url(
            admin_port,
            format!(
                "/console/v1/auth/providers/openai/callback?state={attempt_id}&code=oauth-code-invalid"
            )
            .as_str(),
        ))
        .send()
        .context("failed to submit OpenAI OAuth callback with malformed token reply")?
        .error_for_status()
        .context("malformed OpenAI OAuth callback returned non-success status")?
        .text()
        .context("failed to read malformed OAuth callback HTML body")?;
    assert!(
        callback_html.contains("OpenAI Connection Failed"),
        "malformed token response should render a failure page"
    );
    assert!(
        !callback_html.contains("oauth-secret"),
        "failure page must not leak the raw token response: {callback_html}"
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
        "callback state should converge to failed after malformed token parsing"
    );
    assert!(
        callback_state
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| {
                (message.contains("OpenAI OAuth token response")
                    || message.contains("OpenAI OAuth token exchange request failed"))
                    && !message.contains("oauth-secret")
            }),
        "callback-state failure message should describe the malformed token exchange without leaking secrets: {callback_state}"
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    assert!(
        profiles
            .get("profiles")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.is_empty()),
        "malformed OAuth token replies must not persist a partial auth profile"
    );

    Ok(())
}

#[test]
fn console_openai_oauth_callback_denial_persists_failed_attempt_state() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    wait_for_openai_mock_ready(&mock)?;

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
                "/console/v1/auth/providers/openai/callback?state={attempt_id}&error=access_denied&error_description=bad%20%3C%2Fscript%3E%3Cscript%3Ealert(1)%3C%2Fscript%3E"
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
    assert!(
        !denied_html.contains("</script><script>alert(1)</script>"),
        "denied callback page must not contain raw script breakout content: {denied_html}"
    );
    assert!(
        denied_html.contains("\\u003c/script\\u003e\\u003cscript\\u003ealert(1)\\u003c/script\\u003e"),
        "denied callback page should escape the callback payload before embedding it in a script tag: {denied_html}"
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

#[derive(Debug, Clone)]
struct MockHttpResponse {
    status: String,
    body: String,
}

#[derive(Debug, Default, Clone)]
struct OpenAiMockSnapshot {
    model_request_paths: Vec<String>,
    token_request_bodies: Vec<String>,
    revoke_request_bodies: Vec<String>,
    request_errors: Vec<String>,
}

#[derive(Debug, Default)]
struct OpenAiMockState {
    valid_tokens: HashSet<String>,
    model_request_paths: Vec<String>,
    models_response_body: Option<String>,
    token_request_bodies: Vec<String>,
    revoke_request_bodies: Vec<String>,
    request_errors: Vec<String>,
    authorization_code_reply: Option<TokenReply>,
    refresh_reply: Option<TokenReply>,
    authorization_code_raw_response: Option<MockHttpResponse>,
    refresh_raw_response: Option<MockHttpResponse>,
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
                        let state_for_connection = Arc::clone(&state_for_worker);
                        thread::spawn(move || {
                            if let Err(error) =
                                handle_openai_mock_request(&mut stream, &state_for_connection)
                            {
                                let mut guard = state_for_connection
                                    .lock()
                                    .expect("OpenAI mock state lock should be available");
                                guard.request_errors.push(error.to_string());
                            }
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => {
                        let mut guard = state_for_worker
                            .lock()
                            .expect("OpenAI mock state lock should be available");
                        guard
                            .request_errors
                            .push(format!("listener accept error ({}): {error}", error.kind()));
                        drop(guard);
                        thread::sleep(Duration::from_millis(25));
                    }
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

    fn remove_token(&self, token: &str) {
        let mut state = self.state.lock().expect("OpenAI mock state lock should be available");
        state.valid_tokens.remove(token);
    }

    fn set_authorization_code_raw_response(&self, status: &str, body: &str) {
        let mut state = self.state.lock().expect("OpenAI mock state lock should be available");
        state.authorization_code_raw_response =
            Some(MockHttpResponse { status: status.to_owned(), body: body.to_owned() });
    }

    fn set_models_response_body(&self, body: &str) {
        let mut state = self.state.lock().expect("OpenAI mock state lock should be available");
        state.models_response_body = Some(body.to_owned());
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
            request_errors: state.request_errors.clone(),
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
    let request = match read_http_request(stream)? {
        Some(request) => request,
        None => return Ok(()),
    };
    if request.request_line.starts_with("GET /v1/models ") {
        let authorization =
            request.headers.get("authorization").map(String::as_str).unwrap_or_default();
        let bearer_token = authorization.strip_prefix("Bearer ").map(str::trim);
        let api_key_token = request.headers.get("x-api-key").map(String::as_str).map(str::trim);
        let presented_token =
            api_key_token.filter(|value| !value.is_empty()).or(bearer_token).unwrap_or_default();
        let authorized = {
            let mut guard = state.lock().expect("OpenAI mock state lock should be available");
            guard.model_request_paths.push(request.path);
            guard.valid_tokens.contains(presented_token)
        };
        if authorized {
            let body = {
                let guard = state.lock().expect("OpenAI mock state lock should be available");
                guard.models_response_body.clone().unwrap_or_else(|| r#"{"data":[]}"#.to_owned())
            };
            write_json_response(stream, "200 OK", body.as_str())?;
        } else {
            write_json_response(stream, "401 Unauthorized", r#"{"error":"invalid_api_key"}"#)?;
        }
        return Ok(());
    }

    if request.request_line.starts_with("POST /oauth/token ") {
        let mut guard = state.lock().expect("OpenAI mock state lock should be available");
        guard.token_request_bodies.push(request.body.clone());
        let raw_response = if request.body.contains("grant_type=authorization_code") {
            guard.authorization_code_raw_response.clone()
        } else if request.body.contains("grant_type=refresh_token") {
            guard.refresh_raw_response.clone()
        } else {
            None
        };
        if let Some(response) = raw_response {
            write_json_response(stream, response.status.as_str(), response.body.as_str())?;
            return Ok(());
        }

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

fn read_http_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .context("failed to set OpenAI mock stream read timeout")?;
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    let request_line_bytes =
        reader.read_line(&mut request_line).context("failed to read OpenAI mock request line")?;
    if request_line_bytes == 0 {
        return Ok(None);
    }
    let request_line = request_line.trim_end_matches(&['\r', '\n'][..]).to_owned();
    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("OpenAI mock request-line is missing path"))?
        .to_owned();

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .context("failed to read OpenAI mock request header line")?;
        if bytes == 0 {
            anyhow::bail!("OpenAI mock request ended before the header block completed");
        }
        let line = line.trim_end_matches(&['\r', '\n'][..]);
        if line.is_empty() {
            break;
        }
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

    let mut body_bytes = vec![0_u8; content_length];
    reader
        .read_exact(body_bytes.as_mut_slice())
        .context("failed to read OpenAI mock request body bytes")?;
    let body =
        String::from_utf8(body_bytes).context("OpenAI mock request body was not valid UTF-8")?;

    Ok(Some(HttpRequest { request_line, path, headers, body }))
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

fn read_config_profile_id(path: &std::path::Path) -> Result<Option<String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let document: toml::Value = toml::from_str(content.as_str())
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    Ok(document
        .get("model_provider")
        .and_then(|value| value.get("auth_profile_id"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned))
}

fn isolated_default_config_env() -> Vec<(String, String)> {
    #[cfg(windows)]
    {
        vec![
            (
                "APPDATA".to_owned(),
                unique_temp_dir("palyra-openai-auth-appdata").to_string_lossy().to_string(),
            ),
            (
                "PROGRAMDATA".to_owned(),
                unique_temp_dir("palyra-openai-auth-programdata").to_string_lossy().to_string(),
            ),
        ]
    }
    #[cfg(not(windows))]
    {
        vec![
            (
                "XDG_CONFIG_HOME".to_owned(),
                unique_temp_dir("palyra-openai-auth-xdg-config").to_string_lossy().to_string(),
            ),
            (
                "HOME".to_owned(),
                unique_temp_dir("palyra-openai-auth-home").to_string_lossy().to_string(),
            ),
        ]
    }
}

fn wait_for_openai_mock_ready(mock: &OpenAiMockServer) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build OpenAI mock readiness client")?;
    let url = format!("{}/v1/models", mock.base_url());
    let timeout_at = Instant::now() + Duration::from_secs(3);

    loop {
        let response = client.get(&url).bearer_auth("readiness-probe").send();
        if response.as_ref().ok().is_some_and(|value| value.status().as_u16() == 401) {
            return Ok(());
        }
        if Instant::now() > timeout_at {
            if let Ok(response) = response {
                anyhow::bail!(
                    "timed out waiting for OpenAI mock readiness; last status was {}",
                    response.status()
                );
            }
            let error = response.err().map(|value| value.to_string()).unwrap_or_default();
            anyhow::bail!("timed out waiting for OpenAI mock readiness: {error}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn lock_openai_auth_surface_test() -> MutexGuard<'static, ()> {
    static OPENAI_AUTH_SURFACE_TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    OPENAI_AUTH_SURFACE_TEST_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    let state_root_dir = unique_temp_dir("palyra-openai-auth-state-root");
    let journal_db_path = unique_temp_path("palyra-openai-auth-journal", "sqlite3");
    let identity_store_dir = state_root_dir.join("identity");
    let vault_dir = state_root_dir.join("vault");
    let auth_profiles_path = state_root_dir.join("auth_profiles.toml");
    let agents_registry_path = state_root_dir.join("agents.toml");
    let config_path = unique_temp_path("palyra-openai-auth-config", "toml");
    fs::create_dir_all(&identity_store_dir).with_context(|| {
        format!("failed to create test identity dir {}", identity_store_dir.display())
    })?;
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
        .env("PALYRA_STATE_ROOT", state_root_dir.to_string_lossy().to_string())
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_AUTH_PROFILES_PATH", auth_profiles_path.to_string_lossy().to_string())
        .env("PALYRA_AGENTS_REGISTRY_PATH", agents_registry_path.to_string_lossy().to_string())
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
