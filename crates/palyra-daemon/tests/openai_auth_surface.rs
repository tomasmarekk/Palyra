//! Console and compat surface tests for model-provider auth: OpenAI and
//! Anthropic API-key flows persisting vault refs, model probe/discover
//! results, and registry-backed model/embeddings/tools compat payloads.

#[allow(dead_code)]
mod support;

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
use support::assert_json_golden;

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
fn console_anthropic_oauth_token_flow_persists_vault_refs_and_default_selection() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[(
        "PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(),
        CONSOLE_ADMIN_PRINCIPAL.to_owned(),
    )])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/anthropic/oauth-token",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "Claude Subscription",
            "scope": { "kind": "global" },
            "access_token": "sk-ant-oat-test-access",
            "refresh_token": "sk-ant-ort-test-refresh",
            "token_endpoint": "https://console.anthropic.com/v1/oauth/token",
            "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
            "expires_at_unix_ms": 1_900_000_000_000_i64,
            "set_default": true
        }),
    )?;
    let profile_id = connected
        .get("profile_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("oauth-token connect response missing profile_id"))?
        .to_owned();
    assert_eq!(connected.get("provider").and_then(Value::as_str), Some("anthropic"));
    assert_eq!(connected.get("action").and_then(Value::as_str), Some("oauth"));
    assert_eq!(connected.get("state").and_then(Value::as_str), Some("selected"));

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/anthropic", &cookie)?;
    assert_eq!(
        provider_state.get("default_profile_id").and_then(Value::as_str),
        Some(profile_id.as_str())
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    let profile = find_profile(&profiles, profile_id.as_str())?;
    assert_eq!(
        profile.get("provider").and_then(|provider| provider.get("kind")).and_then(Value::as_str),
        Some("anthropic")
    );
    let credential = profile
        .get("credential")
        .ok_or_else(|| anyhow::anyhow!("anthropic OAuth profile missing credential"))?;
    assert_eq!(credential.get("type").and_then(Value::as_str), Some("oauth"));
    let access_ref = credential
        .get("access_token_vault_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("anthropic OAuth credential missing access ref"))?;
    let refresh_ref = credential
        .get("refresh_token_vault_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("anthropic OAuth credential missing refresh ref"))?;
    assert!(access_ref.contains("anthropic"), "access token ref should be provider scoped");
    assert!(refresh_ref.contains("anthropic"), "refresh token ref should be provider scoped");
    let profile_json = profile.to_string();
    assert!(
        !profile_json.contains("sk-ant-oat-test-access")
            && !profile_json.contains("sk-ant-ort-test-refresh"),
        "auth profile JSON must not expose raw Anthropic OAuth tokens"
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
    assert!(document_toml.contains("auth_provider_kind = \"anthropic\""));
    assert!(document_toml.contains("kind = \"anthropic\""));
    assert!(
        !document_toml.contains("sk-ant-oat-test-access")
            && !document_toml.contains("sk-ant-ort-test-refresh"),
        "config inspect must not leak raw Anthropic OAuth tokens"
    );

    Ok(())
}

#[test]
fn console_xai_oauth_token_flow_persists_vault_refs_and_default_selection() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("xai-oauth-test-access");
    mock.set_models_response_body(
        r#"{"data":[{"id":"provider-older","created":1700000000},{"id":"provider-newer","created":1800000000}]}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_XAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/xai/oauth-token",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "xAI OAuth",
            "scope": { "kind": "global" },
            "access_token": "xai-oauth-test-access",
            "refresh_token": "xai-oauth-test-refresh",
            "token_endpoint": "https://auth.x.ai/oauth/token",
            "client_id": "b1a00492-073a-47ea-816f-4c329264a828",
            "scopes": ["openid", "offline_access", "api:access"],
            "expires_at_unix_ms": 1_900_000_000_000_i64,
            "set_default": true
        }),
    )?;
    let profile_id = connected
        .get("profile_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("xAI oauth-token connect response missing profile_id"))?
        .to_owned();
    assert_eq!(connected.get("provider").and_then(Value::as_str), Some("xai"));
    assert_eq!(connected.get("action").and_then(Value::as_str), Some("oauth"));
    assert_eq!(connected.get("state").and_then(Value::as_str), Some("selected"));

    let provider_state =
        get_console_json(&client, admin_port, "/console/v1/auth/providers/xai", &cookie)?;
    assert_eq!(
        provider_state.get("default_profile_id").and_then(Value::as_str),
        Some(profile_id.as_str())
    );

    let profiles = get_console_json(&client, admin_port, "/console/v1/auth/profiles", &cookie)?;
    let profile = find_profile(&profiles, profile_id.as_str())?;
    assert_eq!(
        profile.get("provider").and_then(|provider| provider.get("kind")).and_then(Value::as_str),
        Some("custom")
    );
    assert_eq!(
        profile
            .get("provider")
            .and_then(|provider| provider.get("custom_name"))
            .and_then(Value::as_str),
        Some("xai")
    );
    let credential = profile
        .get("credential")
        .ok_or_else(|| anyhow::anyhow!("xAI OAuth profile missing credential"))?;
    assert_eq!(credential.get("type").and_then(Value::as_str), Some("oauth"));
    assert_eq!(
        credential.get("client_id").and_then(Value::as_str),
        Some("b1a00492-073a-47ea-816f-4c329264a828")
    );
    assert_eq!(
        credential.get("token_endpoint").and_then(Value::as_str),
        Some("https://auth.x.ai/oauth/token")
    );
    let scopes = credential
        .get("scopes")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("xAI OAuth credential missing scopes"))?;
    assert!(
        scopes.iter().any(|scope| scope.as_str() == Some("offline_access")),
        "xAI OAuth scopes must preserve offline_access for refresh: {scopes:?}"
    );
    let access_ref = credential
        .get("access_token_vault_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("xAI OAuth credential missing access ref"))?;
    let refresh_ref = credential
        .get("refresh_token_vault_ref")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("xAI OAuth credential missing refresh ref"))?;
    assert!(access_ref.contains("xai"), "access token ref should be provider scoped");
    assert!(refresh_ref.contains("xai"), "refresh token ref should be provider scoped");
    let profile_json = profile.to_string();
    assert!(
        !profile_json.contains("xai-oauth-test-access")
            && !profile_json.contains("xai-oauth-test-refresh"),
        "auth profile JSON must not expose raw xAI OAuth tokens"
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
    assert!(document_toml.contains("auth_provider_kind = \"xai\""));
    assert!(document_toml.contains("kind = \"openai_compatible\""));
    assert!(document_toml.contains("openai_base_url = \"https://api.x.ai/v1\""));
    assert!(
        document_toml.contains("openai_model = \"provider-newer\""),
        "xAI default selection should store the model returned by live discovery"
    );
    assert!(
        !document_toml.contains("xai-oauth-test-access")
            && !document_toml.contains("xai-oauth-test-refresh"),
        "config inspect must not leak raw xAI OAuth tokens"
    );

    Ok(())
}

#[test]
fn console_openai_default_selection_after_xai_resets_shared_endpoint() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-return-openai");
    mock.allow_token("xai-access");
    mock.set_models_response_body(
        r#"{"data":[{"id":"provider-older","created":1700000000},{"id":"xai-provider-newer","created":1800000000}]}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_XAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;

    let xai_connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/xai/oauth-token",
        &cookie,
        &csrf_token,
        &json!({
            "profile_name": "xAI OAuth",
            "scope": { "kind": "global" },
            "access_token": "xai-access",
            "refresh_token": "xai-refresh",
            "token_endpoint": "https://auth.x.ai/oauth/token",
            "client_id": "b1a00492-073a-47ea-816f-4c329264a828",
            "expires_at_unix_ms": 1_900_000_000_000_i64,
            "set_default": true
        }),
    )?;
    assert_eq!(xai_connected.get("state").and_then(Value::as_str), Some("selected"));

    let xai_config = post_console_json(
        &client,
        admin_port,
        "/console/v1/config/inspect",
        &cookie,
        &csrf_token,
        &json!({}),
    )?;
    let xai_document_toml = xai_config
        .get("document_toml")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("xAI config inspect response missing document_toml"))?;
    assert!(xai_document_toml.contains("auth_provider_kind = \"xai\""));
    assert!(xai_document_toml.contains("openai_base_url = \"https://api.x.ai/v1\""));
    assert!(
        xai_document_toml.contains("openai_model = \"xai-provider-newer\""),
        "xAI default selection should store the model returned by live discovery"
    );

    mock.set_models_response_body(
        r#"{"data":[{"id":"openai-provider-older","created":1700000000},{"id":"openai-provider-newer","created":1800000000,"supported_parameters":["tools"]}]}"#,
    );
    let openai_connected = post_console_json(
        &client,
        admin_port,
        "/console/v1/auth/providers/openai/api-key",
        &cookie,
        &csrf_token,
        &json!({
            "profile_id": "openai-after-xai",
            "profile_name": "OpenAI After xAI",
            "scope": { "kind": "global" },
            "api_key": "sk-return-openai",
            "set_default": true
        }),
    )?;
    assert_eq!(openai_connected.get("state").and_then(Value::as_str), Some("selected"));
    assert_eq!(
        openai_connected.get("profile_id").and_then(Value::as_str),
        Some("openai-after-xai")
    );

    let openai_config = post_console_json(
        &client,
        admin_port,
        "/console/v1/config/inspect",
        &cookie,
        &csrf_token,
        &json!({}),
    )?;
    let openai_document_toml = openai_config
        .get("document_toml")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("OpenAI config inspect response missing document_toml"))?;
    assert!(openai_document_toml.contains("auth_profile_id = \"openai-after-xai\""));
    assert!(openai_document_toml.contains("auth_provider_kind = \"openai\""));
    assert!(openai_document_toml.contains("kind = \"openai_compatible\""));
    assert!(openai_document_toml.contains(&format!("openai_base_url = \"{}/v1\"", mock.base_url())));
    assert!(openai_document_toml.contains("openai_model = \"openai-provider-newer\""));
    assert!(
        !openai_document_toml.contains("https://api.x.ai/v1")
            && !openai_document_toml.contains("xai-provider-newer"),
        "OpenAI reselection must not leave stale xAI endpoint/model values"
    );

    Ok(())
}

#[test]
fn console_models_probe_and_discover_publish_live_openai_results() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-probe-openai");
    mock.set_models_response_body(
        r#"{"data":[{"id":"gpt-4.1-mini","supported_parameters":["tools"]},{"id":"text-embedding-3-large"}]}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
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
    assert_eq!(
        probe_provider.get("state").and_then(Value::as_str),
        Some("ok"),
        "probe provider payload should succeed: {probe_provider:#?}"
    );
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
fn compat_model_detail_and_embeddings_surface_publish_registry_backed_payloads() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-compat-openai");
    mock.set_embeddings_response_body(
        r#"{"data":[{"index":0,"embedding":[0.1,0.2,0.3]},{"index":1,"embedding":[0.3,0.2,0.1]}],"model":"text-embedding-3-small"}"#,
    );
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_KIND".to_owned(), "openai_compatible".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_MODEL".to_owned(), "gpt-4.1-mini".to_owned()),
        (
            "PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL".to_owned(),
            "text-embedding-3-small".to_owned(),
        ),
        ("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS".to_owned(), "3".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY".to_owned(), "sk-compat-openai".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_embeddings_api")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat surface token",
        &["compat.models.read", "compat.embeddings.create"],
    )?;

    let (model_status, model_detail) =
        compat_get_json(&client, admin_port, "/v1/models/gpt-4.1-mini", token.as_str())?;
    assert_eq!(model_status, 200);
    assert_eq!(model_detail.get("id").and_then(Value::as_str), Some("gpt-4.1-mini"));
    assert_eq!(model_detail.pointer("/metadata/role").and_then(Value::as_str), Some("chat"),);
    assert!(
        model_detail.pointer("/metadata/provider_id").is_none(),
        "compat model detail must not expose internal provider identifiers"
    );
    assert!(
        model_detail.pointer("/metadata/credential_id").is_none(),
        "compat model detail must not expose internal credential identifiers"
    );
    assert_json_golden("compat_model_detail.json", &model_detail)?;
    let (embedding_model_status, embedding_model_detail) =
        compat_get_json(&client, admin_port, "/v1/models/text-embedding-3-small", token.as_str())?;
    assert_eq!(embedding_model_status, 200);
    assert_eq!(
        embedding_model_detail
            .pointer("/metadata/capabilities/tool_calls")
            .and_then(Value::as_bool),
        Some(false),
        "embeddings compat model must not advertise tool calls"
    );
    assert_eq!(
        embedding_model_detail
            .pointer("/metadata/capabilities/structured_outputs/supported")
            .and_then(Value::as_bool),
        Some(false),
        "embeddings compat model must not advertise structured outputs"
    );

    let (capabilities_status, capabilities) =
        compat_get_json(&client, admin_port, "/v1/capabilities", token.as_str())?;
    assert_eq!(capabilities_status, 200, "capabilities response should succeed: {capabilities}");
    assert_eq!(capabilities.get("object").and_then(Value::as_str), Some("capabilities"));
    let runtime_capability_ids = capabilities
        .pointer("/runtime/capabilities")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("capabilities response missing runtime capabilities"))?
        .iter()
        .filter_map(|entry| entry.get("id").and_then(Value::as_str))
        .collect::<HashSet<_>>();
    for expected in ["streaming_tokens", "tool_calls", "approvals", "sessions", "runs", "responses"]
    {
        assert!(
            runtime_capability_ids.contains(expected),
            "capabilities response missing runtime capability {expected}: {capabilities}"
        );
    }
    assert_eq!(
        capabilities
            .pointer("/method_registry/methods")
            .and_then(Value::as_array)
            .map(std::vec::Vec::len),
        Some(16),
        "capabilities response should expose current compat method registry"
    );
    let encoded_capabilities = capabilities.to_string();
    assert!(
        !encoded_capabilities.contains("sk-compat-openai") && !encoded_capabilities.contains(mock.base_url().as_str()),
        "capabilities response must not leak provider secrets or private base URLs: {encoded_capabilities}"
    );
    let mut normalized_capabilities = capabilities.clone();
    normalized_capabilities["generated_at"] = json!(0);
    normalized_capabilities["generated_at_unix_ms"] = json!(0);
    assert_json_golden("compat_capabilities.json", &normalized_capabilities)?;

    let (embeddings_status, embeddings_response) = compat_post_json(
        &client,
        admin_port,
        "/v1/embeddings",
        token.as_str(),
        &json!({
            "model": "text-embedding-3-small",
            "input": ["alpha rollout", "beta recall"]
        }),
    )?;
    assert_eq!(embeddings_status, 200);
    assert_eq!(
        embeddings_response.get("model").and_then(Value::as_str),
        Some("text-embedding-3-small"),
    );
    assert_eq!(
        embeddings_response.get("data").and_then(Value::as_array).map(std::vec::Vec::len),
        Some(2),
    );
    assert_json_golden("compat_embeddings_response.json", &embeddings_response)?;

    let mock_snapshot = mock.snapshot();
    assert!(
        mock_snapshot.embedding_request_paths.iter().any(|path| path == "/v1/embeddings"),
        "compat embeddings should hit the upstream /v1/embeddings endpoint: {:?}",
        mock_snapshot.embedding_request_paths
    );
    assert!(
        mock_snapshot
            .embedding_request_bodies
            .iter()
            .any(|body| body.contains("\"text-embedding-3-small\"")),
        "compat embeddings should forward the configured embeddings model: {:?}",
        mock_snapshot.embedding_request_bodies
    );

    Ok(())
}

#[test]
fn compat_model_detail_returns_not_found_for_unknown_model() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-compat-openai");
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_KIND".to_owned(), "openai_compatible".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_MODEL".to_owned(), "gpt-4.1-mini".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY".to_owned(), "sk-compat-openai".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat model detail token",
        &["compat.models.read"],
    )?;

    let (status, payload) =
        compat_get_json(&client, admin_port, "/v1/models/missing-model", token.as_str())?;
    assert_eq!(status, 404);
    assert_json_golden("compat_model_detail_not_found.json", &payload)?;

    Ok(())
}

#[test]
fn compat_embeddings_surface_reports_feature_disabled_and_degraded_posture() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let mock = OpenAiMockServer::new(None, None)?;
    mock.allow_token("sk-compat-openai");
    wait_for_openai_mock_ready(&mock)?;

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_KIND".to_owned(), "openai_compatible".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
        (
            "PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL".to_owned(),
            "operator-embedding-v1".to_owned(),
        ),
        ("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY".to_owned(), "sk-compat-openai".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat embeddings token",
        &["compat.embeddings.create"],
    )?;

    let (feature_disabled_status, feature_disabled_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/embeddings",
        token.as_str(),
        &json!({ "input": "alpha" }),
    )?;
    assert_eq!(feature_disabled_status, 403);
    assert_json_golden("compat_embeddings_feature_disabled.json", &feature_disabled_payload)?;

    drop(daemon);

    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_MODEL_PROVIDER_KIND".to_owned(), "openai_compatible".to_owned()),
        ("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL".to_owned(), format!("{}/v1", mock.base_url())),
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
        (
            "PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL".to_owned(),
            "operator-embedding-v1".to_owned(),
        ),
        ("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY".to_owned(), "sk-compat-openai".to_owned()),
        ("PALYRA_OFFLINE".to_owned(), "1".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_embeddings_api")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat degraded embeddings token",
        &["compat.embeddings.create"],
    )?;

    let (degraded_status, degraded_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/embeddings",
        token.as_str(),
        &json!({ "input": "alpha" }),
    )?;
    assert_eq!(degraded_status, 503);
    assert_json_golden("compat_embeddings_degraded.json", &degraded_payload)?;

    Ok(())
}

#[test]
fn compat_tools_invoke_is_disabled_by_default_and_refuses_when_enabled() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[(
        "PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(),
        CONSOLE_ADMIN_PRINCIPAL.to_owned(),
    )])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat tools token",
        &["compat.tools.invoke"],
    )?;

    let (disabled_status, disabled_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/tools/invoke",
        token.as_str(),
        &json!({ "tool": "palyra.echo", "input": { "text": "hello" } }),
    )?;
    assert_eq!(disabled_status, 403);
    assert_json_golden("compat_tools_invoke_feature_disabled.json", &disabled_payload)?;

    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_tools_invoke")?;
    let (enabled_status, enabled_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/tools/invoke",
        token.as_str(),
        &json!({ "tool": "palyra.echo", "input": { "text": "hello" } }),
    )?;
    assert_eq!(enabled_status, 501);
    assert_json_golden("compat_tools_invoke_refusal.json", &enabled_payload)?;

    Ok(())
}

#[test]
fn compat_chat_streams_text_with_keepalive_and_structured_finish() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat chat stream token",
        &["compat.chat.create"],
    )?;

    let (status, content_type, body) = compat_post_sse(
        &client,
        admin_port,
        "/v1/chat/completions",
        token.as_str(),
        &json!({
            "messages": [{ "role": "user", "content": "streamed chat text" }],
            "stream": true
        }),
    )?;
    assert_eq!(status, 200, "chat stream should open successfully: {body}");
    assert!(
        content_type.starts_with("text/event-stream"),
        "chat stream should use SSE content type: {content_type}"
    );
    assert!(body.contains(": keepalive"), "chat stream should emit SSE keepalive comments: {body}");

    let messages = parse_sse_messages(body.as_str())?;
    assert_json_golden(
        "compat_chat_stream_text_events.json",
        &normalize_chat_stream_grammar(messages.as_slice())?,
    )?;

    let json_events = parse_sse_json_events(messages.as_slice())?;
    assert!(
        json_events.len() >= 3,
        "chat stream should emit role, text, and terminal chunks: {json_events:?}"
    );
    assert_eq!(
        json_events.last().and_then(|(_, event)| event
            .pointer("/choices/0/finish_reason")
            .and_then(Value::as_str)),
        Some("stop")
    );
    assert!(
        messages.last().is_some_and(|(_, data)| data == "[DONE]"),
        "chat stream should terminate with [DONE]"
    );

    Ok(())
}

#[test]
fn compat_chat_stream_maps_stream_failures_to_failed_event() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[(
        "PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(),
        CONSOLE_ADMIN_PRINCIPAL.to_owned(),
    )])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat chat failure token",
        &["compat.chat.create"],
    )?;

    let (status, content_type, body) = compat_post_sse(
        &client,
        admin_port,
        "/v1/chat/completions",
        token.as_str(),
        &json!({
            "messages": [{ "role": "user", "content": "stream should fail cleanly" }],
            "stream": true
        }),
    )?;
    assert_eq!(status, 200, "chat stream failures should be reported as SSE events: {body}");
    assert!(
        content_type.starts_with("text/event-stream"),
        "failed chat stream should still use SSE content type: {content_type}"
    );

    let messages = parse_sse_messages(body.as_str())?;
    let json_events = parse_sse_json_events(messages.as_slice())?;
    assert_eq!(
        json_events.iter().filter_map(|event| event.0.as_deref()).collect::<Vec<_>>(),
        vec!["chat.failed"],
        "failed chat stream should emit one typed failure event after opening chunk"
    );
    let failed = json_events
        .iter()
        .find_map(|(event, value)| (event.as_deref() == Some("chat.failed")).then_some(value))
        .ok_or_else(|| anyhow::anyhow!("failed chat stream missing chat.failed event"))?;
    assert_eq!(failed.get("object").and_then(Value::as_str), Some("chat.completion.chunk"));
    assert_eq!(failed.pointer("/choices/0/finish_reason").and_then(Value::as_str), Some("error"));
    assert_eq!(
        failed.pointer("/error/code").and_then(Value::as_str),
        Some("gateway_stream_failed")
    );
    assert!(
        messages.last().is_some_and(|(_, data)| data == "[DONE]"),
        "failed chat stream should still terminate with [DONE]"
    );

    Ok(())
}

#[test]
fn compat_api_security_negative_matrix_covers_public_surface() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let matrix_token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat API matrix token",
        &["compat.models.read", "compat.chat.create", "compat.responses.create"],
    )?;
    let models_only_token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat API models-only token",
        &["compat.models.read"],
    )?;

    let models_response = client
        .get(format!("http://127.0.0.1:{admin_port}/v1/models"))
        .header("Authorization", format!("Bearer {matrix_token}"))
        .header("Origin", "https://evil.example")
        .send()
        .context("failed to call compat models matrix probe")?;
    assert_eq!(models_response.status().as_u16(), 200);
    assert_compat_security_headers(models_response.headers())?;
    assert!(
        models_response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| value != "*"),
        "compat API must not emit wildcard CORS for arbitrary origins"
    );

    let capabilities_response = client
        .get(format!("http://127.0.0.1:{admin_port}/v1/capabilities"))
        .header("Authorization", format!("Bearer {matrix_token}"))
        .send()
        .context("failed to call compat capabilities matrix probe")?;
    assert_eq!(capabilities_response.status().as_u16(), 200);

    let missing_auth = client
        .get(format!("http://127.0.0.1:{admin_port}/v1/models"))
        .send()
        .context("failed to call compat models without auth")?;
    assert_eq!(missing_auth.status().as_u16(), 401, "compat API must require bearer auth");

    let wrong_scope = client
        .post(format!("http://127.0.0.1:{admin_port}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {models_only_token}"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "scope check" }]
        }))
        .send()
        .context("failed to call compat chat with wrong scope")?;
    assert_eq!(wrong_scope.status().as_u16(), 403, "compat API must enforce per-route scopes");

    let invalid_payload = client
        .post(format!("http://127.0.0.1:{admin_port}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {matrix_token}"))
        .json(&json!({ "stream": false }))
        .send()
        .context("failed to call compat chat with invalid payload")?;
    assert!(
        matches!(invalid_payload.status().as_u16(), 400 | 422),
        "invalid compat chat payload should fail before run execution: {}",
        invalid_payload.status()
    );

    let unsupported_route = client
        .post(format!("http://127.0.0.1:{admin_port}/v1/unsupported"))
        .header("Authorization", format!("Bearer {matrix_token}"))
        .json(&json!({ "input": "unsupported" }))
        .send()
        .context("failed to call unsupported compat route")?;
    assert!(
        matches!(unsupported_route.status().as_u16(), 404 | 405),
        "unsupported compat route should not fall through to a successful response: {}",
        unsupported_route.status()
    );

    let oversized_body = json!({
        "messages": [{ "role": "user", "content": "a".repeat(80 * 1024) }]
    })
    .to_string();
    let oversized = client
        .post(format!("http://127.0.0.1:{admin_port}/v1/chat/completions"))
        .header("Authorization", format!("Bearer {matrix_token}"))
        .header("content-type", "application/json")
        .body(oversized_body)
        .send()
        .context("failed to call compat chat with oversized body")?;
    assert_eq!(
        oversized.status().as_u16(),
        413,
        "compat API should reject oversized request bodies with payload-too-large status"
    );

    let (run_status, run_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/runs",
        matrix_token.as_str(),
        &json!({ "input": "matrix run" }),
    )?;
    assert_eq!(run_status, 200);
    let run_id = run_payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("matrix run response missing id: {run_payload}"))?;
    let terminal =
        wait_for_compat_run_terminal(&client, admin_port, matrix_token.as_str(), run_id)?;
    assert_eq!(terminal.get("status").and_then(Value::as_str), Some("completed"));
    let (events_status, events_content_type, events_body) = compat_get_sse(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/events").as_str(),
        matrix_token.as_str(),
    )?;
    assert_eq!(events_status, 200, "run events matrix stream should open: {events_body}");
    assert!(
        events_content_type.starts_with("text/event-stream"),
        "run events matrix stream should use SSE content type: {events_content_type}"
    );

    Ok(())
}

#[test]
fn compat_responses_streams_text_events_and_preserves_non_stream_shape() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat responses token",
        &["compat.responses.create"],
    )?;

    let (stream_status, stream_content_type, stream_body) = compat_post_sse(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &json!({
            "input": "streamed responses text",
            "stream": true
        }),
    )?;
    assert_eq!(stream_status, 200, "responses stream should open successfully: {stream_body}");
    assert!(
        stream_content_type.starts_with("text/event-stream"),
        "responses stream should use SSE content type: {stream_content_type}"
    );
    assert!(
        stream_body.contains(": keepalive"),
        "responses stream should emit SSE keepalive comments: {stream_body}"
    );

    let messages = parse_sse_messages(stream_body.as_str())?;
    assert_json_golden(
        "compat_responses_stream_text_events.json",
        &normalize_responses_stream_grammar(messages.as_slice())?,
    )?;

    let json_events = parse_sse_json_events(messages.as_slice())?;
    assert_eq!(
        json_events.iter().filter_map(|event| event.0.as_deref()).collect::<Vec<_>>(),
        vec!["response.created", "response.output_text.delta", "response.completed"],
        "responses stream should preserve the public event order"
    );

    let created = &json_events[0].1;
    let response_id = created
        .pointer("/response/id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("response.created missing response id"))?;
    assert!(response_id.starts_with("resp_"));
    assert_eq!(created.pointer("/response/status").and_then(Value::as_str), Some("in_progress"));

    let delta_text = json_events[1]
        .1
        .get("delta")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("response.output_text.delta missing text"))?;
    assert_eq!(delta_text, "streamed responses text");

    let completed = &json_events[2].1;
    assert_eq!(completed.pointer("/response/id").and_then(Value::as_str), Some(response_id));
    assert_eq!(completed.pointer("/response/status").and_then(Value::as_str), Some("completed"));
    assert_eq!(
        completed.pointer("/response/output/0/content/0/text").and_then(Value::as_str),
        Some(delta_text),
        "final response text should match streamed deltas"
    );
    assert!(
        completed
            .pointer("/response/usage/total_tokens")
            .and_then(Value::as_u64)
            .is_some_and(|tokens| tokens > 0),
        "response.completed should include final usage"
    );
    assert!(
        messages.last().is_some_and(|(_, data)| data == "[DONE]"),
        "responses stream should terminate with [DONE]"
    );

    let (non_stream_status, non_stream_payload) = compat_post_json(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &json!({ "input": "non stream responses text" }),
    )?;
    assert_eq!(non_stream_status, 200, "non-stream responses should still succeed");
    assert_eq!(non_stream_payload.get("object").and_then(Value::as_str), Some("response"));
    assert_eq!(non_stream_payload.get("status").and_then(Value::as_str), Some("completed"));
    assert!(
        non_stream_payload
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.starts_with("resp_")),
        "non-stream response id shape should stay compatible"
    );
    assert_eq!(
        non_stream_payload.pointer("/output/0/content/0/text").and_then(Value::as_str),
        Some("non stream responses text")
    );

    Ok(())
}

#[test]
fn compat_responses_store_get_delete_and_idempotency() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat responses store token",
        &["compat.responses.create"],
    )?;

    let create_payload = json!({ "input": "stored response body" });
    let (first_status, first_payload) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &create_payload,
        Some("responses-store-key-1"),
    )?;
    assert_eq!(first_status, 200, "initial response create should succeed");
    let response_id = first_payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("created response missing id"))?;
    assert!(response_id.starts_with("resp_"));

    let (get_status, get_payload) = compat_get_json(
        &client,
        admin_port,
        format!("/v1/responses/{response_id}").as_str(),
        token.as_str(),
    )?;
    assert_eq!(get_status, 200, "stored response should be readable");
    assert_eq!(get_payload.get("id").and_then(Value::as_str), Some(response_id));
    assert_eq!(
        get_payload.pointer("/output/0/content/0/text").and_then(Value::as_str),
        Some("stored response body")
    );

    let (replay_status, replay_payload) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &create_payload,
        Some("responses-store-key-1"),
    )?;
    assert_eq!(replay_status, 200, "same idempotency key should replay");
    assert_eq!(
        replay_payload.get("id").and_then(Value::as_str),
        Some(response_id),
        "idempotency replay must not create a second response"
    );

    let (conflict_status, conflict_payload) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &json!({ "input": "changed response body" }),
        Some("responses-store-key-1"),
    )?;
    assert_eq!(conflict_status, 409, "changed payload should conflict");
    assert_eq!(
        conflict_payload.pointer("/error/code").and_then(Value::as_str),
        Some("idempotency_conflict")
    );

    let (delete_status, delete_payload) = compat_delete_json(
        &client,
        admin_port,
        format!("/v1/responses/{response_id}").as_str(),
        token.as_str(),
    )?;
    assert_eq!(delete_status, 200, "delete should return a tombstone payload");
    assert_eq!(delete_payload.get("deleted").and_then(Value::as_bool), Some(true));
    assert_eq!(delete_payload.get("id").and_then(Value::as_str), Some(response_id));

    let (missing_status, missing_payload) = compat_get_json(
        &client,
        admin_port,
        format!("/v1/responses/{response_id}").as_str(),
        token.as_str(),
    )?;
    assert_eq!(missing_status, 404, "deleted public response view should not be readable");
    assert_eq!(
        missing_payload.pointer("/error/code").and_then(Value::as_str),
        Some("response_not_found")
    );

    Ok(())
}

#[test]
fn compat_runs_create_status_events_idempotency_and_owner_scope() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat runs token",
        &["compat.responses.create"],
    )?;
    let unauthorized_token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat runs unauthorized token",
        &["compat.models.read"],
    )?;

    let create_payload = json!({
        "instructions": "Answer directly.",
        "messages": [{
            "role": "user",
            "content": "runs API text"
        }],
        "session": {
            "label": "Runs API integration"
        },
        "tool_exposure_policy": "configured"
    });
    let (create_status, create_response) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/runs",
        token.as_str(),
        &create_payload,
        Some("runs-create-key-1"),
    )?;
    assert_eq!(create_status, 200, "run create should be accepted: {create_response}");
    assert_eq!(create_response.get("object").and_then(Value::as_str), Some("run"));
    assert_eq!(create_response.get("status").and_then(Value::as_str), Some("queued"));
    assert_eq!(create_response.get("queue_state").and_then(Value::as_str), Some("accepted"));
    assert!(create_response.get("accepted_at").and_then(Value::as_i64).is_some());
    let run_id = create_response
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("run create response missing id"))?
        .to_owned();
    assert_eq!(create_response.get("run_id").and_then(Value::as_str), Some(run_id.as_str()));
    let status_url = format!("/v1/runs/{run_id}");
    let events_url = format!("/v1/runs/{run_id}/events");
    assert_eq!(
        create_response.get("status_url").and_then(Value::as_str),
        Some(status_url.as_str())
    );
    assert_eq!(
        create_response.get("events_url").and_then(Value::as_str),
        Some(events_url.as_str())
    );

    let wait_observation = create_compat_run_wait_observation(
        &client,
        admin_port,
        token.as_str(),
        &json!({ "input": "runs API wait timeout text" }),
    )?;
    assert_compat_run_wait_timeout_or_fast_completion(&wait_observation);

    let terminal =
        wait_for_compat_run_terminal(&client, admin_port, token.as_str(), run_id.as_str())?;
    assert_eq!(terminal.get("id").and_then(Value::as_str), Some(run_id.as_str()));
    assert_eq!(terminal.get("status").and_then(Value::as_str), Some("completed"));
    assert_eq!(terminal.get("active_phase").and_then(Value::as_str), Some("completed"));
    assert!(
        terminal
            .pointer("/usage/total_tokens")
            .and_then(Value::as_u64)
            .is_some_and(|tokens| tokens > 0),
        "run status should include usage: {terminal}"
    );
    assert_eq!(terminal.get("pending_approval"), Some(&Value::Null));
    assert_eq!(
        terminal.pointer("/verification_summary/state").and_then(Value::as_str),
        Some("disabled")
    );
    assert_eq!(
        terminal.pointer("/verification_summary/rollout_enabled").and_then(Value::as_bool),
        Some(false)
    );

    let (wait_completed_status, wait_completed_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/wait").as_str(),
        token.as_str(),
        &json!({ "timeout_ms": 5000 }),
    )?;
    assert_eq!(
        wait_completed_status, 200,
        "terminal wait should succeed: {wait_completed_response}"
    );
    assert_eq!(wait_completed_response.get("object").and_then(Value::as_str), Some("run.wait"));
    assert_eq!(wait_completed_response.get("status").and_then(Value::as_str), Some("completed"));
    assert_eq!(wait_completed_response.get("timed_out").and_then(Value::as_bool), Some(false));
    assert_eq!(
        wait_completed_response.pointer("/run/status").and_then(Value::as_str),
        Some("completed")
    );

    let (replay_status, replay_response) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/runs",
        token.as_str(),
        &create_payload,
        Some("runs-create-key-1"),
    )?;
    assert_eq!(replay_status, 200, "same run idempotency key should replay status");
    assert_eq!(
        replay_response.get("id").and_then(Value::as_str),
        Some(run_id.as_str()),
        "idempotency replay must not create a second run"
    );

    let (conflict_status, conflict_response) = compat_post_json_with_idempotency_key(
        &client,
        admin_port,
        "/v1/runs",
        token.as_str(),
        &json!({ "input": "changed runs API text" }),
        Some("runs-create-key-1"),
    )?;
    assert_eq!(conflict_status, 409, "changed run payload should conflict");
    assert_eq!(
        conflict_response.pointer("/error/code").and_then(Value::as_str),
        Some("idempotency_conflict")
    );

    let (unauthorized_status, unauthorized_response) = compat_get_json(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}").as_str(),
        unauthorized_token.as_str(),
    )?;
    assert_eq!(unauthorized_status, 403, "token without runs scope must be rejected");
    assert_eq!(
        unauthorized_response.pointer("/error/code").and_then(Value::as_str),
        Some("missing_scope")
    );

    let (events_status, events_content_type, events_body) = compat_get_sse(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/events").as_str(),
        token.as_str(),
    )?;
    assert_eq!(events_status, 200, "run events stream should open: {events_body}");
    assert!(
        events_content_type.starts_with("text/event-stream"),
        "run events should use SSE content type: {events_content_type}"
    );
    let messages = parse_sse_messages(events_body.as_str())?;
    assert!(messages.last().is_some_and(|(_, data)| data == "[DONE]"));
    let json_events = parse_sse_json_events(messages.as_slice())?;
    let public_event_names = json_events
        .iter()
        .filter_map(|(_, event)| event.get("event").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        public_event_names.contains(&"run.queued")
            && public_event_names.contains(&"run.started")
            && public_event_names.contains(&"model.delta")
            && public_event_names.contains(&"run.completed"),
        "run events should replay public runtime taxonomy events: {public_event_names:?}"
    );
    assert!(
        json_events.iter().all(|(_, event)| {
            event.pointer("/correlation/run_id").and_then(Value::as_str) == Some(run_id.as_str())
        }),
        "all public run events should carry the requested run correlation"
    );

    let (detach_status, detach_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/detach").as_str(),
        token.as_str(),
        &json!({ "reason": "test client closed stream" }),
    )?;
    assert_eq!(detach_status, 200, "run detach should be accepted: {detach_response}");
    assert_eq!(detach_response.get("object").and_then(Value::as_str), Some("run.detach"));
    assert_eq!(detach_response.get("detached").and_then(Value::as_bool), Some(true));
    assert_eq!(
        detach_response.get("cancel_on_disconnect").and_then(Value::as_bool),
        Some(false),
        "detach must not cancel the underlying run"
    );

    let (approval_missing_status, approval_missing_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/approval").as_str(),
        token.as_str(),
        &json!({ "action": "approve", "decision_scope": "once" }),
    )?;
    assert_eq!(
        approval_missing_status, 404,
        "approval decision without a pending approval should be explicit: {approval_missing_response}"
    );
    assert_eq!(
        approval_missing_response.pointer("/error/code").and_then(Value::as_str),
        Some("approval_not_found")
    );

    let (modify_status, modify_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{run_id}/approval").as_str(),
        token.as_str(),
        &json!({ "action": "modify" }),
    )?;
    assert_eq!(modify_status, 422, "modify must fail closed: {modify_response}");
    assert_eq!(
        modify_response.pointer("/error/code").and_then(Value::as_str),
        Some("approval_modify_unsupported")
    );

    let stop_create_payload = json!({
        "input": "run stop API text",
        "session": {
            "label": "Runs stop integration"
        }
    });
    let (stop_create_status, stop_create_response) =
        compat_post_json(&client, admin_port, "/v1/runs", token.as_str(), &stop_create_payload)?;
    assert_eq!(
        stop_create_status, 200,
        "run create before stop should be accepted: {stop_create_response}"
    );
    let stop_run_id = stop_create_response
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("stop run create response missing id"))?
        .to_owned();
    let _visible_stop_run =
        wait_for_compat_run_visible(&client, admin_port, token.as_str(), stop_run_id.as_str())?;
    let (stop_status, stop_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{stop_run_id}/stop").as_str(),
        token.as_str(),
        &json!({
            "reason": "integration requested stop",
            "mode": "graceful",
            "cleanup_policy": "none"
        }),
    )?;
    assert_eq!(stop_status, 200, "run stop should be accepted: {stop_response}");
    assert_eq!(stop_response.get("object").and_then(Value::as_str), Some("run.stop"));
    assert_eq!(stop_response.get("mode").and_then(Value::as_str), Some("cancel"));
    assert_eq!(stop_response.get("cleanup_policy").and_then(Value::as_str), Some("none"));
    let stop_requested =
        stop_response.pointer("/_palyra/effect/cancel_requested").and_then(Value::as_bool)
            == Some(true);
    let stop_settled =
        stop_response.pointer("/run/status").and_then(Value::as_str) == Some("cancelled");
    assert_eq!(
        stop_response.pointer("/run/_palyra/cancel_requested").and_then(Value::as_bool),
        Some(stop_requested),
        "stop response snapshot must observe the durable cancellation intent"
    );
    assert_eq!(
        stop_response.get("stopped").and_then(Value::as_bool),
        Some(stop_requested && stop_settled),
        "stop acknowledgement must require accepted intent and cancelled settlement"
    );
    let stopped_terminal =
        wait_for_compat_run_terminal(&client, admin_port, token.as_str(), stop_run_id.as_str())?;
    assert!(
        stopped_terminal.get("status").and_then(Value::as_str).is_some_and(|status| {
            status == "cancelled" || (!stop_requested && status == "completed")
        }),
        "stopped run should be terminal without failing: {stopped_terminal}"
    );
    let (repeat_stop_status, repeat_stop_response) = compat_post_json(
        &client,
        admin_port,
        format!("/v1/runs/{stop_run_id}/stop").as_str(),
        token.as_str(),
        &json!({ "reason": "integration repeated stop" }),
    )?;
    assert_eq!(
        repeat_stop_status, 200,
        "repeated terminal stop should remain idempotent: {repeat_stop_response}"
    );
    assert_eq!(
        repeat_stop_response.get("stopped").and_then(Value::as_bool),
        Some(false),
        "repeated terminal stop must report that this request changed no state"
    );
    assert_eq!(
        repeat_stop_response.pointer("/_palyra/effect/cancel_requested").and_then(Value::as_bool),
        Some(false),
        "repeated terminal stop must remain an explicit no-op"
    );
    assert_eq!(
        repeat_stop_response.pointer("/run/status").and_then(Value::as_str),
        stopped_terminal.get("status").and_then(Value::as_str),
        "repeated terminal stop must preserve the settled status"
    );

    Ok(())
}

#[test]
fn compat_responses_stream_maps_stream_failures_to_failed_event() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[(
        "PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(),
        CONSOLE_ADMIN_PRINCIPAL.to_owned(),
    )])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat responses failure token",
        &["compat.responses.create"],
    )?;

    let (status, content_type, body) = compat_post_sse(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &json!({
            "input": "stream should fail cleanly",
            "stream": true
        }),
    )?;
    assert_eq!(status, 200, "stream failures should be reported as SSE events: {body}");
    assert!(
        content_type.starts_with("text/event-stream"),
        "failed responses stream should still use SSE content type: {content_type}"
    );

    let messages = parse_sse_messages(body.as_str())?;
    let json_events = parse_sse_json_events(messages.as_slice())?;
    assert_eq!(
        json_events.iter().filter_map(|event| event.0.as_deref()).collect::<Vec<_>>(),
        vec!["response.created", "response.failed"],
        "failed stream should emit created then failed"
    );
    let failed = &json_events[1].1;
    assert_eq!(failed.get("type").and_then(Value::as_str), Some("response.failed"));
    assert_eq!(failed.pointer("/response/status").and_then(Value::as_str), Some("failed"));
    assert_eq!(
        failed.pointer("/error/code").and_then(Value::as_str),
        Some("gateway_stream_failed")
    );
    assert!(
        messages.last().is_some_and(|(_, data)| data == "[DONE]"),
        "failed responses stream should still terminate with [DONE]"
    );

    Ok(())
}

#[test]
fn compat_responses_stream_maps_tool_call_and_approval_events() -> Result<()> {
    let _test_guard = lock_openai_auth_surface_test();
    let (child, admin_port) = spawn_palyrad_with_dynamic_ports(&[
        ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_ADMIN_PRINCIPAL.to_owned()),
        ("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED".to_owned(), "true".to_owned()),
        (
            "PALYRA_TOOL_CALL_ALLOWED_TOOLS".to_owned(),
            "palyra.fs.apply_patch,palyra.fs.read_file".to_owned(),
        ),
    ])?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let client = http_client()?;
    let (cookie, csrf_token) = login_console_session(&client, admin_port, CONSOLE_ADMIN_PRINCIPAL)?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "compat_api")?;
    enable_access_feature_flag(&client, admin_port, &cookie, &csrf_token, "api_tokens")?;
    let token = create_personal_api_token(
        &client,
        admin_port,
        &cookie,
        &csrf_token,
        "Compat responses tools token",
        &["compat.responses.create"],
    )?;

    let (status, content_type, body) = compat_post_sse(
        &client,
        admin_port,
        "/v1/responses",
        token.as_str(),
        &json!({
            "input": "Create reports/deterministic-provider.md and verify the fixture.",
            "stream": true
        }),
    )?;
    assert_eq!(status, 200, "responses tool stream should open successfully: {body}");
    assert!(
        content_type.starts_with("text/event-stream"),
        "responses tool stream should use SSE content type: {content_type}"
    );

    let messages = parse_sse_messages(body.as_str())?;
    assert_json_golden(
        "compat_responses_stream_tool_approval_events.json",
        &normalize_responses_stream_grammar(messages.as_slice())?,
    )?;

    let json_events = parse_sse_json_events(messages.as_slice())?;
    let event_names = json_events.iter().filter_map(|event| event.0.as_deref()).collect::<Vec<_>>();
    let tool_added_index = event_names
        .iter()
        .position(|event| *event == "response.output_item.added")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing tool item added event"))?;
    let args_delta_index = event_names
        .iter()
        .position(|event| *event == "response.function_call_arguments.delta")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing tool arguments delta"))?;
    let args_done_index = event_names
        .iter()
        .position(|event| *event == "response.function_call_arguments.done")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing tool arguments done"))?;
    let approval_required_index = event_names
        .iter()
        .position(|event| *event == "approval.required")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing approval.required"))?;
    let approval_resolved_index = event_names
        .iter()
        .position(|event| *event == "approval.resolved")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing approval.resolved"))?;
    let tool_result_index = event_names
        .iter()
        .position(|event| *event == "response.output_item.done")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing tool result completion"))?;
    let terminal_index = event_names
        .iter()
        .position(|event| *event == "response.failed" || *event == "response.completed")
        .ok_or_else(|| anyhow::anyhow!("responses stream missing terminal response event"))?;
    assert!(
        tool_added_index < args_delta_index
            && args_delta_index < args_done_index
            && args_done_index < approval_required_index
            && approval_required_index < approval_resolved_index
            && approval_resolved_index < tool_result_index
            && tool_result_index < terminal_index,
        "responses tool stream events should preserve proposal/approval/result/final order: {event_names:?}"
    );

    let args_delta = json_events[args_delta_index]
        .1
        .get("delta")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tool arguments delta missing"))?;
    let args_done = json_events[args_done_index]
        .1
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("tool arguments done missing"))?;
    assert_eq!(args_delta, args_done, "tool call arguments should be reconstructable");
    assert!(
        args_done.contains("reports/deterministic-provider.md"),
        "tool call arguments should include deterministic fixture path: {args_done}"
    );

    let approval_required = &json_events[approval_required_index].1;
    assert_eq!(approval_required.get("type").and_then(Value::as_str), Some("approval.required"));
    assert_eq!(
        approval_required.get("tool_name").and_then(Value::as_str),
        Some("palyra.fs.apply_patch")
    );
    assert!(
        approval_required.get("input_json").is_none(),
        "approval.required should not expose raw tool input: {approval_required}"
    );

    let approval_resolved = &json_events[approval_resolved_index].1;
    assert_eq!(approval_resolved.get("approved").and_then(Value::as_bool), Some(false));
    assert_eq!(
        approval_resolved.get("reason").and_then(Value::as_str),
        Some("interactive_tool_approval_not_supported_for_compat_api")
    );
    let tool_result = &json_events[tool_result_index].1;
    assert_eq!(tool_result.pointer("/item/status").and_then(Value::as_str), Some("failed"));
    assert_eq!(tool_result.pointer("/tool_result/success").and_then(Value::as_bool), Some(false));
    assert!(
        tool_result.get("output_json").is_none(),
        "Responses SSE tool result must not expose raw output: {tool_result}"
    );
    assert!(
        !tool_result.pointer("/tool_result/output_ref").is_some_and(Value::is_null),
        "Responses SSE tool result should reference the journal artifact when output exists"
    );
    assert!(
        messages.last().is_some_and(|(_, data)| data == "[DONE]"),
        "responses tool stream should terminate with [DONE]"
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
        ("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL".to_owned(), "true".to_owned()),
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
    assert_eq!(
        provider.get("state").and_then(Value::as_str),
        Some("auth_failed"),
        "probe provider payload should preserve auth failure: {provider:#?}"
    );
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
            .get("validation_errors")
            .and_then(Value::as_array)
            .is_some_and(|entries| entries.iter().any(|entry| {
                entry.get("field").and_then(Value::as_str) == Some("api_key")
                    && entry.get("code").and_then(Value::as_str) == Some("invalid_credential")
                    && entry
                        .get("message")
                        .and_then(Value::as_str)
                        .is_some_and(|message| message.to_ascii_lowercase().contains("invalid"))
            })),
        "invalid API key should explain the provider credential failure in validation errors: {error}"
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
    let callback_state_without_csrf = client
        .get(console_url(
            admin_port,
            "/console/v1/auth/providers/openai/callback-state?attempt_id=missing",
        ))
        .header("Cookie", cookie.clone())
        .send()
        .context("failed to call OpenAI callback-state without CSRF")?;
    assert_eq!(
        callback_state_without_csrf.status().as_u16(),
        403,
        "OpenAI callback-state can poll device OAuth and must enforce CSRF on authenticated sessions"
    );

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

    let pending = get_console_json_with_csrf(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
        &csrf_token,
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

    let callback_state = get_console_json_with_csrf(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
        &csrf_token,
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
        "expired OAuth credentials should refresh immediately through the refresh action: {refresh}"
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

    let callback_state = get_console_json_with_csrf(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
        &csrf_token,
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

    let callback_state = get_console_json_with_csrf(
        &client,
        admin_port,
        format!("/console/v1/auth/providers/openai/callback-state?attempt_id={attempt_id}")
            .as_str(),
        &cookie,
        &csrf_token,
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
    embedding_request_paths: Vec<String>,
    embedding_request_bodies: Vec<String>,
    token_request_bodies: Vec<String>,
    revoke_request_bodies: Vec<String>,
    request_errors: Vec<String>,
}

#[derive(Debug, Default)]
struct OpenAiMockState {
    valid_tokens: HashSet<String>,
    model_request_paths: Vec<String>,
    models_response_body: Option<String>,
    embedding_request_paths: Vec<String>,
    embedding_request_bodies: Vec<String>,
    embeddings_response_body: Option<String>,
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

    fn set_embeddings_response_body(&self, body: &str) {
        let mut state = self.state.lock().expect("OpenAI mock state lock should be available");
        state.embeddings_response_body = Some(body.to_owned());
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
            embedding_request_paths: state.embedding_request_paths.clone(),
            embedding_request_bodies: state.embedding_request_bodies.clone(),
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
    stream.set_nonblocking(false).context("failed to switch OpenAI mock connection to blocking")?;
    let Some(request) = read_http_request(stream)? else {
        return Ok(());
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

    if request.request_line.starts_with("POST /v1/embeddings ") {
        let authorization =
            request.headers.get("authorization").map(String::as_str).unwrap_or_default();
        let bearer_token = authorization.strip_prefix("Bearer ").map(str::trim);
        let presented_token = bearer_token.unwrap_or_default();
        let (authorized, body) = {
            let mut guard = state.lock().expect("OpenAI mock state lock should be available");
            guard.embedding_request_paths.push(request.path.clone());
            guard.embedding_request_bodies.push(request.body.clone());
            let body = guard.embeddings_response_body.clone().unwrap_or_else(|| {
                r#"{"data":[{"index":0,"embedding":[0.0,0.0,0.0]}],"model":"text-embedding-3-small"}"#
                    .to_owned()
            });
            (guard.valid_tokens.contains(presented_token), body)
        };
        if authorized {
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

fn get_console_json_with_csrf(
    client: &Client,
    admin_port: u16,
    path: &str,
    cookie: &str,
    csrf_token: &str,
) -> Result<Value> {
    client
        .get(console_url(admin_port, path))
        .header("Cookie", cookie)
        .header("x-palyra-csrf-token", csrf_token)
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

fn enable_access_feature_flag(
    client: &Client,
    admin_port: u16,
    cookie: &str,
    csrf_token: &str,
    feature_key: &str,
) -> Result<Value> {
    post_console_json(
        client,
        admin_port,
        format!("/console/v1/access/features/{feature_key}").as_str(),
        cookie,
        csrf_token,
        &json!({
            "enabled": true,
            "stage": "test"
        }),
    )
}

fn create_personal_api_token(
    client: &Client,
    admin_port: u16,
    cookie: &str,
    csrf_token: &str,
    label: &str,
    scopes: &[&str],
) -> Result<String> {
    create_api_token_for_principal(
        client,
        admin_port,
        cookie,
        csrf_token,
        label,
        CONSOLE_ADMIN_PRINCIPAL,
        scopes,
    )
}

fn create_api_token_for_principal(
    client: &Client,
    admin_port: u16,
    cookie: &str,
    csrf_token: &str,
    label: &str,
    principal: &str,
    scopes: &[&str],
) -> Result<String> {
    let created = post_console_json(
        client,
        admin_port,
        "/console/v1/access/api-tokens",
        cookie,
        csrf_token,
        &json!({
            "label": label,
            "scopes": scopes,
            "principal": principal,
            "role": "owner"
        }),
    )?;
    created
        .get("created")
        .and_then(|value| value.get("token"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("API token create response missing token secret"))
}

fn compat_get_json(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
) -> Result<(u16, Value)> {
    let response = client
        .get(console_url(admin_port, path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .with_context(|| format!("failed to GET compat path {path}"))?;
    let status = response.status().as_u16();
    let payload = response
        .json::<Value>()
        .with_context(|| format!("failed to parse compat GET {path} response json"))?;
    Ok((status, payload))
}

fn compat_post_json(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
    payload: &Value,
) -> Result<(u16, Value)> {
    compat_post_json_with_idempotency_key(client, admin_port, path, token, payload, None)
}

fn compat_post_json_with_idempotency_key(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Result<(u16, Value)> {
    let mut request = client
        .post(console_url(admin_port, path))
        .header("Authorization", format!("Bearer {token}"))
        .json(payload);
    if let Some(idempotency_key) = idempotency_key {
        request = request.header("Idempotency-Key", idempotency_key);
    }
    let response = request.send().with_context(|| format!("failed to POST compat path {path}"))?;
    let status = response.status().as_u16();
    let body = response
        .json::<Value>()
        .with_context(|| format!("failed to parse compat POST {path} response json"))?;
    Ok((status, body))
}

fn compat_delete_json(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
) -> Result<(u16, Value)> {
    let response = client
        .delete(console_url(admin_port, path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .with_context(|| format!("failed to DELETE compat path {path}"))?;
    let status = response.status().as_u16();
    let body = response
        .json::<Value>()
        .with_context(|| format!("failed to parse compat DELETE {path} response json"))?;
    Ok((status, body))
}

fn compat_post_sse(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
    payload: &Value,
) -> Result<(u16, String, String)> {
    let response = client
        .post(console_url(admin_port, path))
        .header("Authorization", format!("Bearer {token}"))
        .json(payload)
        .send()
        .with_context(|| format!("failed to POST compat SSE path {path}"))?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body =
        response.text().with_context(|| format!("failed to read compat SSE body for {path}"))?;
    Ok((status, content_type, body))
}

fn compat_get_sse(
    client: &Client,
    admin_port: u16,
    path: &str,
    token: &str,
) -> Result<(u16, String, String)> {
    let response = client
        .get(console_url(admin_port, path))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .with_context(|| format!("failed to GET compat SSE path {path}"))?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body =
        response.text().with_context(|| format!("failed to read compat SSE body for {path}"))?;
    Ok((status, content_type, body))
}

fn wait_for_compat_run_terminal(
    client: &Client,
    admin_port: u16,
    token: &str,
    run_id: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, payload) =
            compat_get_json(client, admin_port, format!("/v1/runs/{run_id}").as_str(), token)?;
        if status == 200
            && payload
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| matches!(status, "completed" | "failed" | "cancelled"))
        {
            return Ok(payload);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("compat run {run_id} did not reach a terminal state: {payload}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_compat_run_visible(
    client: &Client,
    admin_port: u16,
    token: &str,
    run_id: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, payload) =
            compat_get_json(client, admin_port, format!("/v1/runs/{run_id}").as_str(), token)?;
        if status == 200 {
            return Ok(payload);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("compat run {run_id} did not become visible: {payload}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn create_compat_run_wait_observation(
    client: &Client,
    admin_port: u16,
    token: &str,
    payload: &Value,
) -> Result<Value> {
    let mut last_payload = Value::Null;
    for attempt in 0..8 {
        let (create_status, create_response) =
            compat_post_json(client, admin_port, "/v1/runs?mode=accepted", token, payload)?;
        assert_eq!(
            create_status, 200,
            "run create for wait timeout attempt {attempt} should be accepted: {create_response}"
        );
        let run_id = create_response
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("wait timeout create response missing id"))?;
        let visible = wait_for_compat_run_visible(client, admin_port, token, run_id)?;
        if visible
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "completed" | "failed" | "cancelled"))
        {
            last_payload = visible;
            continue;
        }
        let (wait_status, wait_payload) = compat_post_json(
            client,
            admin_port,
            format!("/v1/runs/{run_id}/wait").as_str(),
            token,
            &json!({ "timeout_ms": 1 }),
        )?;
        assert_eq!(wait_status, 200, "run wait should return a JSON payload: {wait_payload}");
        if wait_payload.get("timed_out").and_then(Value::as_bool) == Some(true)
            || wait_payload
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status == "completed")
        {
            return Ok(wait_payload);
        }
        last_payload = wait_payload;
    }
    if last_payload.get("status").and_then(Value::as_str) == Some("completed") {
        return Ok(last_payload);
    }
    anyhow::bail!(
        "compat run wait did not observe a timeout or fast completion; last payload: {last_payload}"
    )
}

fn assert_compat_run_wait_timeout_or_fast_completion(payload: &Value) {
    match (
        payload.get("object").and_then(Value::as_str),
        payload.get("timed_out").and_then(Value::as_bool),
        payload.get("status").and_then(Value::as_str),
    ) {
        (Some("run.wait"), Some(true), Some("timeout")) => {
            assert_eq!(payload.get("timeout_ms").and_then(Value::as_u64), Some(1));
            assert!(
                payload.pointer("/run/status").and_then(Value::as_str).is_some_and(
                    |status| matches!(
                        status,
                        "queued" | "running" | "completed" | "failed" | "cancelled"
                    )
                ),
                "timeout payload should include current run status: {payload}"
            );
        }
        (Some("run.wait"), Some(false), Some("completed")) => {
            assert_eq!(
                payload.pointer("/run/status").and_then(Value::as_str),
                Some("completed"),
                "completed wait payload should include the terminal run: {payload}"
            );
        }
        (Some("run"), _, Some("completed")) => {
            assert_eq!(
                payload.get("active_phase").and_then(Value::as_str),
                Some("completed"),
                "fast completion payload should include the terminal phase: {payload}"
            );
        }
        _ => panic!("unexpected run wait observation payload: {payload}"),
    }
}

fn parse_sse_messages(body: &str) -> Result<Vec<(Option<String>, String)>> {
    let mut messages = Vec::new();
    for raw_block in body.replace("\r\n", "\n").split("\n\n") {
        let mut event = None;
        let mut data_lines = Vec::new();
        for line in raw_block.lines() {
            if line.starts_with(':') || line.is_empty() {
                continue;
            }
            if let Some(value) = line.strip_prefix("event:") {
                event = Some(value.trim_start().to_owned());
                continue;
            }
            if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim_start().to_owned());
            }
        }
        if event.is_some() || !data_lines.is_empty() {
            messages.push((event, data_lines.join("\n")));
        }
    }
    if messages.is_empty() {
        anyhow::bail!("SSE body did not contain any messages: {body}");
    }
    Ok(messages)
}

fn parse_sse_json_events(
    messages: &[(Option<String>, String)],
) -> Result<Vec<(Option<String>, Value)>> {
    messages
        .iter()
        .filter(|(_, data)| data != "[DONE]")
        .map(|(event, data)| {
            serde_json::from_str::<Value>(data)
                .map(|value| (event.clone(), value))
                .with_context(|| format!("failed to parse SSE data as JSON: {data}"))
        })
        .collect()
}

fn normalize_chat_stream_grammar(messages: &[(Option<String>, String)]) -> Result<Value> {
    let mut normalized = Vec::new();
    for (event, data) in messages {
        if data == "[DONE]" {
            normalized.push(json!({
                "event": Value::Null,
                "type": "[DONE]"
            }));
            continue;
        }
        let value = serde_json::from_str::<Value>(data)
            .with_context(|| format!("failed to parse SSE data as JSON: {data}"))?;
        normalized.push(json!({
            "event": event,
            "object": value.get("object").and_then(Value::as_str),
            "has_id": value.get("id").and_then(Value::as_str).is_some(),
            "has_model": value.get("model").and_then(Value::as_str).is_some(),
            "delta_role": value.pointer("/choices/0/delta/role").and_then(Value::as_str),
            "delta_content": value.pointer("/choices/0/delta/content").and_then(Value::as_str).map(|_| "<text>"),
            "has_tool_calls": value.pointer("/choices/0/delta/tool_calls").is_some(),
            "finish_reason": value.pointer("/choices/0/finish_reason").and_then(Value::as_str),
            "error_code": value.pointer("/error/code").and_then(Value::as_str),
            "palyra_public_event_type": value.pointer("/_palyra/public_event_type").and_then(Value::as_str),
        }));
    }
    Ok(Value::Array(normalized))
}

fn normalize_responses_stream_grammar(messages: &[(Option<String>, String)]) -> Result<Value> {
    let mut normalized = Vec::new();
    for (event, data) in messages {
        if data == "[DONE]" {
            normalized.push(json!({
                "event": Value::Null,
                "type": "[DONE]"
            }));
            continue;
        }
        let value = serde_json::from_str::<Value>(data)
            .with_context(|| format!("failed to parse SSE data as JSON: {data}"))?;
        let event_type = value.get("type").and_then(Value::as_str).unwrap_or_default();
        match event_type {
            "response.created" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "response_object": value.pointer("/response/object").and_then(Value::as_str),
                "response_status": value.pointer("/response/status").and_then(Value::as_str),
                "has_response_id": value.pointer("/response/id").and_then(Value::as_str).is_some(),
                "has_usage": !value.pointer("/response/usage").is_some_and(Value::is_null),
            })),
            "response.output_text.delta" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "output_index": value.get("output_index").and_then(Value::as_u64),
                "content_index": value.get("content_index").and_then(Value::as_u64),
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_item_id": value.get("item_id").and_then(Value::as_str).is_some(),
                "delta": "<text>",
            })),
            "response.output_item.added" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "output_index": value.get("output_index").and_then(Value::as_u64),
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_item_id": value.pointer("/item/id").and_then(Value::as_str).is_some(),
                "item_type": value.pointer("/item/type").and_then(Value::as_str),
                "item_status": value.pointer("/item/status").and_then(Value::as_str),
                "tool_name": value.pointer("/item/name").and_then(Value::as_str),
                "arguments": value.pointer("/item/arguments").and_then(Value::as_str),
            })),
            "response.function_call_arguments.delta" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "output_index": value.get("output_index").and_then(Value::as_u64),
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_item_id": value.get("item_id").and_then(Value::as_str).is_some(),
                "delta": "<json>",
            })),
            "response.function_call_arguments.done" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "output_index": value.get("output_index").and_then(Value::as_u64),
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_item_id": value.get("item_id").and_then(Value::as_str).is_some(),
                "has_arguments": value.get("arguments").and_then(Value::as_str).is_some(),
            })),
            "response.output_item.done" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "output_index": value.get("output_index").and_then(Value::as_u64),
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_item_id": value.pointer("/item/id").and_then(Value::as_str).is_some(),
                "item_type": value.pointer("/item/type").and_then(Value::as_str),
                "item_status": value.pointer("/item/status").and_then(Value::as_str),
                "tool_name": value.pointer("/item/name").and_then(Value::as_str),
                "tool_success": value.pointer("/tool_result/success").and_then(Value::as_bool),
                "output_visibility": value.pointer("/tool_result/output_visibility").and_then(Value::as_str),
                "has_output_ref": !value.pointer("/tool_result/output_ref").is_some_and(Value::is_null),
            })),
            "approval.required" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_approval_id": value.get("approval_id").and_then(Value::as_str).is_some(),
                "has_tool_call_id": value.get("tool_call_id").and_then(Value::as_str).is_some(),
                "tool_name": value.get("tool_name").and_then(Value::as_str),
                "approval_required": value.get("approval_required").and_then(Value::as_bool),
                "risk_level": value.get("risk_level").and_then(Value::as_str),
                "has_raw_input_json": value.get("input_json").is_some(),
            })),
            "approval.resolved" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "has_response_id": value.get("response_id").and_then(Value::as_str).is_some(),
                "has_approval_id": value.get("approval_id").and_then(Value::as_str).is_some(),
                "has_tool_call_id": value.get("tool_call_id").and_then(Value::as_str).is_some(),
                "approved": value.get("approved").and_then(Value::as_bool),
                "has_reason": value.get("reason").and_then(Value::as_str).is_some(),
                "decision_scope": value.get("decision_scope").and_then(Value::as_str),
            })),
            "response.completed" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "response_object": value.pointer("/response/object").and_then(Value::as_str),
                "response_status": value.pointer("/response/status").and_then(Value::as_str),
                "has_response_id": value.pointer("/response/id").and_then(Value::as_str).is_some(),
                "has_usage": value.pointer("/response/usage/total_tokens").and_then(Value::as_u64).is_some(),
            })),
            "response.failed" => normalized.push(json!({
                "event": event,
                "type": event_type,
                "response_object": value.pointer("/response/object").and_then(Value::as_str),
                "response_status": value.pointer("/response/status").and_then(Value::as_str),
                "has_response_id": value.pointer("/response/id").and_then(Value::as_str).is_some(),
                "has_error": value.get("error").is_some(),
            })),
            other => anyhow::bail!("unexpected responses stream event type {other}: {value}"),
        }
    }
    Ok(Value::Array(normalized))
}

fn assert_compat_security_headers(headers: &reqwest::header::HeaderMap) -> Result<()> {
    assert_eq!(
        required_header_value(headers, "cache-control")?,
        "no-store",
        "compat responses must disable cache persistence"
    );
    assert_eq!(
        required_header_value(headers, "x-content-type-options")?,
        "nosniff",
        "compat responses must set X-Content-Type-Options=nosniff"
    );
    assert_eq!(
        required_header_value(headers, "x-frame-options")?,
        "DENY",
        "compat responses must deny framing"
    );
    assert_eq!(
        required_header_value(headers, "referrer-policy")?,
        "no-referrer",
        "compat responses must not leak referrer values"
    );
    Ok(())
}

fn required_header_value(headers: &reqwest::header::HeaderMap, name: &str) -> Result<String> {
    headers
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing expected response header {name}"))?
        .to_str()
        .with_context(|| format!("header {name} contains invalid UTF-8"))
        .map(ToOwned::to_owned)
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
