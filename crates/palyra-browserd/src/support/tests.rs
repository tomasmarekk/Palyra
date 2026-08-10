//! Crate-internal integration tests for browserd: gRPC service flows, persistence, downloads,
//! navigation guards, and redaction. Compiled via `#[path]` from `lib.rs`; uses parity fixtures.

use super::chromium_security_incident_for_launch;
use super::{
    action_log_entry_to_proto, browser_v1, build_accessibility_tree_snapshot, build_dom_snapshot,
    build_state_store_from_env, chromium_active_tab_for_session,
    chromium_new_tab_error_is_retryable, chromium_tab_for_session,
    default_browserd_state_dir_from_env, derive_state_encryption_key, encrypt_state_blob,
    enforce_non_loopback_bind_auth, fetch_http_attachment_download_artifact, navigate_with_guards,
    parse_daemon_bind_socket, persisted_snapshot_hash, persisted_snapshot_legacy_hash,
    record_chromium_remote_ip_incident, reset_dns_validation_tracking_for_tests,
    run_chromium_blocking, sha256_hex, store_dns_nxdomain_cache, store_generated_artifact,
    update_profile_state_metadata_locked, validate_restored_snapshot_against_profile,
    validate_target_url, validate_target_url_blocking, Args, BrowserActionLogEntryInternal,
    BrowserEngineMode, BrowserProfileRecord, BrowserResilienceProfile, BrowserRuntimeState,
    BrowserServiceImpl, BrowserTabRecord, ChromiumPrivateTargetPolicy, ChromiumSessionProxy,
    DnsValidationCache, NetworkLogEntryInternal, NetworkLogHeaderInternal,
    PersistedSessionSnapshot, PersistedStateStore, SessionPermissionsInternal,
    AUTHORIZATION_HEADER, BROWSER_DIALOG_NAVIGATION_CLEANUP_REASON, BROWSER_RESILIENCE_PROFILE_ENV,
    CANONICAL_PROTOCOL_MAJOR, CHROMIUM_NEW_TAB_RETRY_DELAY_MS, CHROMIUM_PATH_ENV,
    DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS, DEFAULT_GRPC_PORT, DEFAULT_MAX_TABS_PER_SESSION,
    DOWNLOAD_MAX_FILE_BYTES, MAX_RELAY_PAYLOAD_BYTES, ONE_BY_ONE_PNG, PRINCIPAL_HEADER,
    PROFILE_RECORD_SCHEMA_VERSION, STATE_DIR_ENV, STATE_KEY_ENV, STATE_KEY_LEN, STATE_ROOT_ENV,
};
use crate::proto;
use crate::proto::palyra::browser::v1::browser_service_server::BrowserService;
use crate::security::auth::constant_time_eq_bytes;
use base64::Engine as _;
use reqwest::Url;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Request, Status};

const PARITY_DOWNLOAD_TRIGGER_HTML: &str =
    include_str!("../../../../fixtures/parity/download-trigger.html");
const PARITY_NETWORK_LOG_HTML: &str = include_str!("../../../../fixtures/parity/network-log.html");
const PARITY_REDIRECT_TOKEN_URL: &str =
    include_str!("../../../../fixtures/parity/redirect-token-url.txt");
const PARITY_TRICKY_DOM_HTML: &str = include_str!("../../../../fixtures/parity/tricky-dom.html");
const CHROMIUM_ENGINE_SOURCE: &str = include_str!("../engine/chromium.rs");

fn insert_bearer_auth<T>(request: &mut Request<T>, token: &str) {
    let value =
        format!("Bearer {token}").parse().expect("authorization header value should be valid");
    request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
}

fn insert_principal<T>(request: &mut Request<T>, principal: &str) {
    let value = principal.parse().expect("principal header value should be valid");
    request.metadata_mut().insert(PRINCIPAL_HEADER, value);
}

async fn create_session_with_retry_for_chromium_test(
    service: &BrowserServiceImpl,
    payload: browser_v1::CreateSessionRequest,
    max_attempts: usize,
) -> Result<browser_v1::CreateSessionResponse, Status> {
    let attempts = max_attempts.max(1);
    let mut last_status = None;
    for attempt in 1..=attempts {
        match service.create_session(Request::new(payload.clone())).await {
            Ok(response) => return Ok(response.into_inner()),
            Err(status)
                if attempt < attempts && chromium_new_tab_error_is_retryable(status.message()) =>
            {
                last_status = Some(status);
                tokio::time::sleep(Duration::from_millis(CHROMIUM_NEW_TAB_RETRY_DELAY_MS)).await;
            }
            Err(status) => return Err(status),
        }
    }
    Err(last_status.unwrap_or_else(|| Status::internal("chromium test session retry exhausted")))
}

fn resolve_chromium_path_for_tests() -> Option<PathBuf> {
    std::env::var(CHROMIUM_PATH_ENV)
        .ok()
        .map(PathBuf::from)
        .or_else(|| headless_chrome::browser::default_executable().ok())
}

struct EnvVarGuard {
    name: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(name);
        // The browserd env test mutex serializes process-wide environment mutation.
        unsafe {
            std::env::set_var(name, value);
        }
        Self { name, previous }
    }

    fn remove(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        // The browserd env test mutex serializes process-wide environment mutation.
        unsafe {
            std::env::remove_var(name);
        }
        Self { name, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // The browserd env test mutex serializes process-wide environment mutation.
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }
}

fn browserd_env_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static BROWSERD_ENV_TEST_LOCK: std::sync::OnceLock<StdMutex<()>> = std::sync::OnceLock::new();
    BROWSERD_ENV_TEST_LOCK.get_or_init(|| StdMutex::new(())).lock().expect("env test lock")
}

fn browser_runtime_state_for_tests(args: &Args) -> anyhow::Result<BrowserRuntimeState> {
    // Runtime initialization reads browserd persistence env, so keep it serialized with env-mutating tests.
    let _env_guard = browserd_env_test_guard();
    BrowserRuntimeState::new(args)
}

async fn chromium_integration_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static CHROMIUM_INTEGRATION_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    CHROMIUM_INTEGRATION_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(())).lock().await
}

fn simulated_runtime_for_tests() -> Arc<BrowserRuntimeState> {
    Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    )
}

#[test]
fn browser_resilience_profile_is_a_strict_separate_rollout() {
    let _env_guard = browserd_env_test_guard();
    let profile_guard = EnvVarGuard::set(BROWSER_RESILIENCE_PROFILE_ENV, "resilient");
    let enabled =
        BrowserResilienceProfile::from_env().expect("resilient profile should be accepted");
    assert!(enabled.automatic_reconnect);
    assert_eq!(enabled.name(), "resilient");

    drop(profile_guard);
    let _invalid = EnvVarGuard::set(BROWSER_RESILIENCE_PROFILE_ENV, "shadow-ish");
    let error = BrowserResilienceProfile::from_env()
        .expect_err("unknown resilience profile must fail closed");
    assert!(error.to_string().contains(BROWSER_RESILIENCE_PROFILE_ENV));
}

async fn create_test_session(
    service: &BrowserServiceImpl,
    principal: &str,
) -> browser_v1::CreateSessionResponse {
    create_test_session_with_private_targets(service, principal, true).await
}

async fn create_test_session_with_private_targets(
    service: &BrowserServiceImpl,
    principal: &str,
    allow_private_targets: bool,
) -> browser_v1::CreateSessionResponse {
    service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: principal.to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner()
}

async fn click_permission_check_and_wait_for_text(
    service: &BrowserServiceImpl,
    session_id: proto::palyra::common::v1::CanonicalId,
    text: &str,
) {
    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#check".to_owned(),
            max_retries: 1,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("permission check click should execute")
        .into_inner();
    assert!(click.success, "permission check click should succeed: {}", click.error);

    let observed = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id),
            selector: "#status".to_owned(),
            text: text.to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("permission status wait should execute")
        .into_inner();
    assert!(observed.success, "permission status should become '{text}': {}", observed.error);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_health_reports_engine_capabilities() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };

    let health = service
        .health(Request::new(browser_v1::BrowserHealthRequest { v: 1 }))
        .await
        .expect("health should execute")
        .into_inner();

    assert_eq!(health.status, "ok");
    assert_eq!(health.engine_mode, "simulated");
    assert!(!health.javascript_execution_enabled);
    assert!(!health.subresource_loading_enabled);
    assert!(!health.dom_interaction_enabled);
    assert_eq!(health.resilience_profile, "disabled");
    assert!(!health.automatic_reconnect_enabled);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_records_failed_navigation_in_action_log() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session_with_private_targets(&service, "user:ops", false).await;
    let session_id = created.session_id.expect("session id should be present");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: "file:///tmp/index.html".to_owned(),
            timeout_ms: 1_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: false,
        }))
        .await
        .expect("navigate should return a failure response")
        .into_inner();

    assert!(!navigate.success, "file navigation should fail closed");
    let mut inspect = Request::new(browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: false,
        include_storage: false,
        include_action_log: true,
        include_network_log: false,
        include_page_snapshot: false,
        include_console_log: true,
        include_page_diagnostics: false,
        max_cookie_bytes: 0,
        max_storage_bytes: 0,
        max_action_log_entries: 10,
        max_network_log_entries: 0,
        max_network_log_bytes: 0,
        max_dom_snapshot_bytes: 0,
        max_visible_text_bytes: 0,
        max_console_log_entries: 10,
        max_console_log_bytes: 1024,
    });
    insert_principal(&mut inspect, "user:ops");
    let inspected = service
        .inspect_session(inspect)
        .await
        .expect("inspect_session should include action log")
        .into_inner();

    let entry = inspected
        .action_log
        .iter()
        .find(|entry| entry.action_name == "navigate")
        .expect("failed navigate should be recorded as an action log entry");
    assert!(!entry.success);
    assert_eq!(entry.outcome, "policy_blocked");
    assert!(entry.error.contains("blocked URL scheme"));
    assert!(
        inspected.console_log.iter().any(|entry| entry.message.contains("navigate failed")),
        "failed navigation should also appear in diagnostics"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_pdf_zero_max_bytes_uses_session_budget() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    let mut default_budget_request = Request::new(browser_v1::ExportPdfRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        max_bytes: 0,
    });
    insert_principal(&mut default_budget_request, "user:ops");
    let pdf = service
        .export_pdf(default_budget_request)
        .await
        .expect("export_pdf should execute")
        .into_inner();
    assert!(pdf.success, "zero max_bytes should use session budget: {}", pdf.error);
    assert!(!pdf.pdf_bytes.is_empty(), "PDF bytes should be returned");

    let mut tiny_limit_request = Request::new(browser_v1::ExportPdfRequest {
        v: 1,
        session_id: Some(session_id),
        max_bytes: 1,
    });
    insert_principal(&mut tiny_limit_request, "user:ops");
    let limited = service
        .export_pdf(tiny_limit_request)
        .await
        .expect("export_pdf should execute")
        .into_inner();
    assert!(!limited.success, "explicit max_bytes should still be enforced");
    assert!(limited.error.contains("pdf output exceeds max_bytes"));
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_sets_file_input_in_simulated_mode() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session = sessions.get_mut(session_id.ulid.as_str()).expect("session should exist");
        let tab = session.active_tab_mut().expect("active tab should exist");
        tab.last_url = Some("https://example.test/upload".to_owned());
        tab.last_page_body =
            r#"<html><body><input id="upload" type="file"></body></html>"#.to_owned();
    }

    let upload = service
        .set_file_input(Request::new(browser_v1::SetFileInputRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#upload".to_owned(),
            file_name: "input.csv".to_owned(),
            file_bytes: b"name,score\nalice,9\n".to_vec(),
            timeout_ms: 1_000,
            capture_failure_screenshot: false,
            max_failure_screenshot_bytes: 0,
        }))
        .await
        .expect("set_file_input should return")
        .into_inner();

    assert!(upload.success, "file input upload should succeed: {}", upload.error);
    assert_eq!(upload.uploaded_file_name, "input.csv");
    assert_eq!(upload.uploaded_file_bytes, 19);
    assert_eq!(
        upload.action_log.as_ref().map(|entry| entry.action_name.as_str()),
        Some("set_file_input")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_get_download_artifact_returns_content() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let content = b"name,score\nalice,9\n";
    let artifact = store_generated_artifact(
        runtime.as_ref(),
        session_id.ulid.as_str(),
        None,
        "https://example.test/report.csv",
        "report.csv",
        "text/csv",
        content,
    )
    .await
    .expect("artifact should be stored");

    let mut request = Request::new(browser_v1::GetDownloadArtifactRequest {
        v: 1,
        session_id: Some(session_id),
        artifact_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: artifact.artifact_id.clone(),
        }),
        max_bytes: DOWNLOAD_MAX_FILE_BYTES,
    });
    insert_principal(&mut request, "user:ops");
    let fetched = service
        .get_download_artifact(request)
        .await
        .expect("get_download_artifact should return")
        .into_inner();

    assert!(fetched.success, "download artifact fetch should succeed: {}", fetched.error);
    assert_eq!(fetched.content, content);
    assert_eq!(
        fetched.artifact.and_then(|artifact| artifact.artifact_id).map(|id| id.ulid),
        Some(artifact.artifact_id)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_get_download_artifact_refuses_active_quarantined_content() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let artifact = store_generated_artifact(
        runtime.as_ref(),
        session_id.ulid.as_str(),
        None,
        "https://example.test/payload.html",
        "payload.html",
        "text/html",
        b"<script>alert(1)</script>",
    )
    .await
    .expect("active artifact should be stored in quarantine");
    assert!(artifact.quarantined, "active HTML artifact must be quarantined");
    assert!(
        artifact.quarantine_reason.contains("extension_not_allowlisted"),
        "active artifact should carry extension quarantine reason: {}",
        artifact.quarantine_reason
    );
    assert!(
        artifact.quarantine_reason.contains("mime_type_not_allowlisted"),
        "active artifact should carry MIME quarantine reason: {}",
        artifact.quarantine_reason
    );

    let mut request = Request::new(browser_v1::GetDownloadArtifactRequest {
        v: 1,
        session_id: Some(session_id),
        artifact_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: artifact.artifact_id.clone(),
        }),
        max_bytes: DOWNLOAD_MAX_FILE_BYTES,
    });
    insert_principal(&mut request, "user:ops");
    let fetched = service
        .get_download_artifact(request)
        .await
        .expect("get_download_artifact should return a denial response")
        .into_inner();

    assert!(!fetched.success, "quarantined active artifact must not be released");
    assert!(
        fetched.error.contains("quarantined"),
        "denial should identify quarantine boundary: {}",
        fetched.error
    );
    assert!(fetched.content.is_empty(), "quarantined content bytes must not be returned");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_get_download_artifact_returns_bounded_prefix_for_large_content() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let content = b"name,score\nalice,9\nbob,8\n";
    let artifact = store_generated_artifact(
        runtime.as_ref(),
        session_id.ulid.as_str(),
        None,
        "https://example.test/report.csv",
        "report.csv",
        "text/csv",
        content,
    )
    .await
    .expect("artifact should be stored");

    let mut request = Request::new(browser_v1::GetDownloadArtifactRequest {
        v: 1,
        session_id: Some(session_id),
        artifact_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: artifact.artifact_id.clone(),
        }),
        max_bytes: 5,
    });
    insert_principal(&mut request, "user:ops");
    let fetched = service
        .get_download_artifact(request)
        .await
        .expect("get_download_artifact should return")
        .into_inner();

    assert!(fetched.success, "download artifact prefix should succeed: {}", fetched.error);
    assert_eq!(fetched.content, b"name,");
    assert!(fetched.content_truncated);
    assert_eq!(fetched.content_offset_bytes, 0);
    assert_eq!(fetched.content_limit_bytes, 5);
    assert_eq!(
        fetched.artifact.and_then(|artifact| artifact.artifact_id).map(|id| id.ulid),
        Some(artifact.artifact_id)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_captures_http_attachment_download_artifact() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let (url, handle) = spawn_attachment_fixture_http_server(
        "/export",
        "attachment; filename=\"palyra-orders-export.csv\"",
        "text/csv",
        b"sku,name,quantity,price\nPAL-1,Palyra mug,1,12.00\n",
    );
    let artifact = fetch_http_attachment_download_artifact(
        runtime.as_ref(),
        session_id.as_str(),
        None,
        url.as_str(),
        "export",
        true,
        2_000,
    )
    .await
    .expect("attachment fetch should execute")
    .expect("attachment response should be captured");
    assert_eq!(artifact.file_name, "palyra-orders-export.csv");
    assert_eq!(artifact.mime_type, "text/csv");
    assert!(!artifact.quarantined);

    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut list_request, "user:ops");
    let listed = service
        .list_download_artifacts(list_request)
        .await
        .expect("list_download_artifacts should execute")
        .into_inner();
    assert_eq!(listed.artifacts.len(), 1);
    assert_eq!(listed.artifacts[0].file_name, "palyra-orders-export.csv");
    handle.join().expect("attachment server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_ordinary_click_ignores_unrelated_attachment_log_entry() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session =
            sessions.get_mut(session_id.ulid.as_str()).expect("session should exist for seeding");
        let tab = session.active_tab_mut().expect("active tab should exist");
        tab.last_url = Some("https://app.example.test/settings".to_owned());
        tab.last_page_body =
            r#"<html><body><button id="save">Save settings</button></body></html>"#.to_owned();
        tab.network_log.push_back(NetworkLogEntryInternal {
            request_url: "http://127.0.0.1:9/unrelated-secret.csv".to_owned(),
            status_code: 200,
            timing_bucket: "lt_100ms".to_owned(),
            latency_ms: 10,
            captured_at_unix_ms: 1,
            headers: vec![NetworkLogHeaderInternal {
                name: "content-disposition".to_owned(),
                value: "attachment; filename=\"secret.csv\"".to_owned(),
            }],
        });
    }

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id),
            selector: "#save".to_owned(),
            max_retries: 0,
            timeout_ms: 250,
            capture_failure_screenshot: false,
            max_failure_screenshot_bytes: 0,
        }))
        .await
        .expect("ordinary click should execute")
        .into_inner();

    assert!(click.success, "ordinary click should not fetch a logged attachment: {}", click.error);
    assert!(click.artifact.is_none(), "unrelated network entries must not become artifacts");
    assert_eq!(click.action_log.as_ref().map(|entry| entry.outcome.as_str()), Some("clicked"));
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_redacts_navigation_url_selector_in_action_log() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let (base_url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Callback</title></head><body>ok</body></html>",
    );
    let sensitive_url = format!(
        "{base_url}callback?code=oauthCODE123&state=csrfSTATE456&access_token=tok789&signature=sig000&safe=1#fragment"
    );

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: sensitive_url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed: {}", navigate.error);

    let stored_selector = {
        let sessions = runtime.sessions.lock().await;
        sessions
            .get(session_id.ulid.as_str())
            .and_then(|session| session.action_log.back())
            .map(|entry| entry.selector.clone())
            .expect("navigate action should be recorded")
    };
    assert_navigation_selector_redacted(stored_selector.as_str());

    let mut inspect = Request::new(browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: false,
        include_storage: false,
        include_action_log: true,
        include_network_log: false,
        include_page_snapshot: false,
        include_console_log: false,
        include_page_diagnostics: false,
        max_cookie_bytes: 0,
        max_storage_bytes: 0,
        max_action_log_entries: 10,
        max_network_log_entries: 0,
        max_network_log_bytes: 0,
        max_dom_snapshot_bytes: 0,
        max_visible_text_bytes: 0,
        max_console_log_entries: 0,
        max_console_log_bytes: 0,
    });
    insert_principal(&mut inspect, "user:ops");
    let inspected = service
        .inspect_session(inspect)
        .await
        .expect("inspect_session should include action log")
        .into_inner();
    let entry = inspected
        .action_log
        .iter()
        .find(|entry| entry.action_name == "navigate")
        .expect("navigate action should be serialized");
    assert_navigation_selector_redacted(entry.selector.as_str());

    handle.join().expect("test server thread should exit");
}

#[test]
fn query_redaction_treats_oauth_code_and_state_as_sensitive() {
    let redacted = super::redact_query_pairs("code=oauth123&state=abc123&safe=1");
    assert!(redacted.contains("code=<redacted>"), "oauth code must be redacted: {redacted}");
    assert!(redacted.contains("state=<redacted>"), "oauth state must be redacted: {redacted}");
    assert!(
        redacted.contains("safe=1"),
        "non-sensitive parameters should be preserved: {redacted}"
    );
    assert!(
        !redacted.contains("oauth123") && !redacted.contains("abc123"),
        "sensitive values must not leak: {redacted}"
    );
}

#[test]
fn action_log_entry_to_proto_redacts_url_selector_query_secrets() {
    let entry = BrowserActionLogEntryInternal {
        action_id: ulid::Ulid::new().to_string(),
        action_name: "navigate".to_owned(),
        selector: "https://idp.example/callback?code=oauthCODE123&state=csrfSTATE456&access_token=tok789&signature=sig000&safe=1#fragment".to_owned(),
        success: true,
        outcome: "loaded".to_owned(),
        error: String::new(),
        started_at_unix_ms: 1,
        completed_at_unix_ms: 2,
        attempts: 1,
        page_url: String::new(),
    };

    let proto = action_log_entry_to_proto(&entry);

    assert_navigation_selector_redacted(proto.selector.as_str());
}

#[test]
fn action_log_entry_to_proto_preserves_password_field_css_selector() {
    let entry = BrowserActionLogEntryInternal {
        action_id: ulid::Ulid::new().to_string(),
        action_name: "type".to_owned(),
        selector: "#password".to_owned(),
        success: true,
        outcome: "typed".to_owned(),
        error: String::new(),
        started_at_unix_ms: 1,
        completed_at_unix_ms: 2,
        attempts: 1,
        page_url: String::new(),
    };

    let proto = action_log_entry_to_proto(&entry);

    assert_eq!(proto.selector, "#password");
}

fn assert_navigation_selector_redacted(selector: &str) {
    assert!(selector.contains("code=<redacted>"), "oauth code must be redacted: {selector}");
    assert!(selector.contains("state=<redacted>"), "oauth state must be redacted: {selector}");
    assert!(
        selector.contains("access_token=<redacted>"),
        "access token must be redacted: {selector}"
    );
    assert!(selector.contains("signature=<redacted>"), "signature must be redacted: {selector}");
    assert!(
        selector.contains("safe=1"),
        "non-sensitive query parameters should be preserved: {selector}"
    );
    assert!(
        !selector.contains("oauthCODE123")
            && !selector.contains("csrfSTATE456")
            && !selector.contains("tok789")
            && !selector.contains("sig000")
            && !selector.contains("fragment"),
        "selector must not expose raw URL secrets or fragments: {selector}"
    );
}

#[test]
fn observe_snapshots_preserve_case_sensitive_selector_attributes() {
    let html = r#"
        <html>
          <body>
            <button id="btnStart" class="primaryAction" aria-label="Start Flow">Start</button>
            <input id="stateValue" name="workflowState" placeholder="Queued">
          </body>
        </html>
    "#;

    let (dom_snapshot, dom_truncated) = build_dom_snapshot(html, 8 * 1024);
    assert!(!dom_truncated, "small DOM snapshot should not truncate");
    assert!(
        dom_snapshot.contains(r#"id="btnStart""#),
        "DOM snapshot must preserve id value case: {dom_snapshot}"
    );
    assert!(
        dom_snapshot.contains(r#"class="primaryAction""#),
        "DOM snapshot must preserve class value case: {dom_snapshot}"
    );
    assert!(
        dom_snapshot.contains(r#"name="workflowState""#),
        "DOM snapshot must preserve name value case: {dom_snapshot}"
    );
    assert!(
        !dom_snapshot.contains("btnstart") && !dom_snapshot.contains("workflowstate"),
        "DOM snapshot must not expose lowercased reusable selectors: {dom_snapshot}"
    );

    let (accessibility_tree, accessibility_truncated) =
        build_accessibility_tree_snapshot(html, 8 * 1024);
    assert!(!accessibility_truncated, "small accessibility tree should not truncate");
    assert!(
        accessibility_tree.contains("selector=#btnStart"),
        "accessibility selector must remain click-compatible: {accessibility_tree}"
    );
    assert!(
        accessibility_tree.contains("selector=#stateValue"),
        "accessibility selector must preserve input id case: {accessibility_tree}"
    );
    assert!(
        !accessibility_tree.contains("#btnstart") && !accessibility_tree.contains("#statevalue"),
        "accessibility tree must not suggest lowercased ids: {accessibility_tree}"
    );
}

#[test]
fn default_browserd_state_dir_prefers_state_root_override() {
    let resolved = default_browserd_state_dir_from_env(
        Some(OsString::from("state-root")),
        None,
        None,
        None,
        None,
    )
    .expect("state root override should resolve");
    assert_eq!(
        resolved,
        PathBuf::from("state-root").join("browserd"),
        "PALYRA_STATE_ROOT should take precedence for browserd defaults"
    );
}

#[test]
fn build_state_store_uses_configured_state_dir_without_resolving_default() {
    let _env_guard = browserd_env_test_guard();
    let temp = tempfile::tempdir().expect("tempdir should be available");
    let state_dir = temp.path().join("configured-state");
    let encoded_key = base64::engine::general_purpose::STANDARD.encode([7_u8; STATE_KEY_LEN]);
    let _state_key = EnvVarGuard::set(STATE_KEY_ENV, encoded_key);
    let _state_dir = EnvVarGuard::set(STATE_DIR_ENV, state_dir.as_os_str());
    let _state_root = EnvVarGuard::remove(STATE_ROOT_ENV);
    let _appdata = EnvVarGuard::remove("APPDATA");
    let _local_appdata = EnvVarGuard::remove("LOCALAPPDATA");
    let _xdg_state_home = EnvVarGuard::remove("XDG_STATE_HOME");
    let _home = EnvVarGuard::remove("HOME");

    let store = build_state_store_from_env()
        .expect("explicit browserd state dir should not require default env vars")
        .expect("state key should enable persistence");

    assert_eq!(store.root_dir, state_dir);
}

#[cfg(windows)]
#[test]
fn default_browserd_state_dir_uses_appdata_on_windows() {
    let resolved = default_browserd_state_dir_from_env(
        None,
        Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
        Some(OsString::from(r"C:\Users\Test\AppData\Local")),
        None,
        None,
    )
    .expect("APPDATA fallback should resolve on windows");
    assert_eq!(
        resolved,
        PathBuf::from(r"C:\Users\Test\AppData\Roaming").join("Palyra").join("browserd")
    );
}

#[cfg(target_os = "macos")]
#[test]
fn default_browserd_state_dir_uses_macos_application_support() {
    let resolved = default_browserd_state_dir_from_env(
        None,
        None,
        None,
        None,
        Some(OsString::from("/Users/tester")),
    )
    .expect("HOME fallback should resolve on macOS");
    assert_eq!(
        resolved,
        PathBuf::from("/Users/tester")
            .join("Library")
            .join("Application Support")
            .join("Palyra")
            .join("browserd")
    );
}

#[cfg(all(not(windows), not(target_os = "macos")))]
#[test]
fn default_browserd_state_dir_uses_xdg_or_home_on_unix() {
    let xdg = default_browserd_state_dir_from_env(
        None,
        None,
        None,
        Some(OsString::from("/tmp/xdg-state")),
        Some(OsString::from("/home/tester")),
    )
    .expect("XDG_STATE_HOME fallback should resolve");
    assert_eq!(xdg, PathBuf::from("/tmp/xdg-state").join("palyra").join("browserd"));

    let home = default_browserd_state_dir_from_env(
        None,
        None,
        None,
        None,
        Some(OsString::from("/home/tester")),
    )
    .expect("HOME fallback should resolve");
    assert_eq!(
        home,
        PathBuf::from("/home/tester").join(".local").join("state").join("palyra").join("browserd")
    );
}

#[cfg(unix)]
#[test]
fn persisted_state_store_rejects_symlink_root_dir() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("tempdir should be available");
    let actual = temp.path().join("actual-state");
    let symlink_path = temp.path().join("state-link");
    std::fs::create_dir_all(actual.as_path()).expect("actual state dir should be created");
    symlink(actual.as_path(), symlink_path.as_path()).expect("state symlink should be created");

    let error = PersistedStateStore::new(symlink_path, [7_u8; STATE_KEY_LEN])
        .expect_err("symlink root should fail closed");
    let message = error.to_string();
    assert!(
        message.contains("must not be a symlink"),
        "error should explain symlink fail-closed policy: {message}"
    );
}

#[cfg(unix)]
#[test]
fn persisted_state_store_enforces_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir should be available");
    let store = PersistedStateStore::new(temp.path().join("state"), [7_u8; STATE_KEY_LEN])
        .expect("state store should initialize");
    store
        .save_profile_registry(&super::BrowserProfileRegistryDocument::default())
        .expect("registry save should persist encrypted state");

    let root_mode = std::fs::metadata(store.root_dir.as_path())
        .expect("root metadata should load")
        .permissions()
        .mode()
        & 0o777;
    let registry_mode = std::fs::metadata(store.root_dir.join(super::PROFILE_REGISTRY_FILE_NAME))
        .expect("registry metadata should load")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(root_mode, 0o700, "state dir should be owner-only on unix");
    assert_eq!(registry_mode, 0o600, "registry file should be owner-only on unix");
}

#[cfg(unix)]
#[test]
fn persisted_state_store_rejects_symlink_profile_registry_file() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("tempdir should be available");
    let store = PersistedStateStore::new(temp.path().join("state"), [7_u8; STATE_KEY_LEN])
        .expect("state store should initialize");
    let attacker_target = temp.path().join("attacker-profiles.enc");
    std::fs::write(attacker_target.as_path(), b"attacker-controlled")
        .expect("attacker target should be written");
    let registry_path = store.root_dir.join(super::PROFILE_REGISTRY_FILE_NAME);
    symlink(attacker_target.as_path(), registry_path.as_path())
        .expect("registry symlink should be created");

    let error = store.load_profile_registry().expect_err("symlinked registry should fail closed");
    let message = error.to_string();
    assert!(
        message.contains("must not be a symlink"),
        "error should explain symlink rejection: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_blocks_file_scheme_without_private_target_opt_in() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let fixture = temp.path().join("index.html");
    std::fs::write(fixture.as_path(), "<!doctype html><title>Local</title>")
        .expect("fixture should be written");
    let url = Url::from_file_path(fixture.as_path()).expect("file URL should be built");

    let outcome = navigate_with_guards(url.as_str(), 1_000, true, 3, false, 1024, None).await;

    assert!(!outcome.success, "file scheme must be blocked without explicit opt-in");
    assert!(
        outcome.error.contains("requires allow_private_targets=true"),
        "error should explain blocked scheme: {}",
        outcome.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_allows_local_file_with_private_target_opt_in() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let fixture = temp.path().join("release-notes.html");
    std::fs::write(
        fixture.as_path(),
        "<!doctype html><html><head><title>Release Notes</title></head><body><table><tr><td>1.4.0</td></tr></table></body></html>",
    )
    .expect("fixture should be written");
    let url = Url::from_file_path(fixture.as_path()).expect("file URL should be built");

    let outcome = navigate_with_guards(url.as_str(), 1_000, true, 3, true, 4096, None).await;

    assert!(outcome.success, "file scheme should be allowed with explicit local opt-in");
    assert_eq!(outcome.final_url, url.as_str());
    assert!(outcome.page_body.contains("1.4.0"));
    assert_eq!(outcome.status_code, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_blocks_http_redirect_to_local_file() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let fixture = temp.path().join("secret.txt");
    std::fs::write(fixture.as_path(), "sensitive").expect("fixture should be written");
    let file_url = Url::from_file_path(fixture.as_path()).expect("file URL should be built");
    let (url, handle) = spawn_redirect_http_server(file_url.as_str());

    let outcome = navigate_with_guards(url.as_str(), 2_000, true, 3, true, 4_096, None).await;
    handle.join().expect("test server thread should exit");

    assert!(!outcome.success, "http redirect to file:// must be blocked");
    assert_eq!(outcome.status_code, 302);
    assert!(
        outcome.error.contains("redirect to file:// URL is blocked"),
        "navigation should fail closed for http-to-file redirects: final_url={} error={}",
        outcome.final_url,
        outcome.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_does_not_replay_cookie_header_to_cross_host_redirect() {
    let (target_url, target_handle) = spawn_cookie_capture_http_server("localhost");
    let (url, redirect_handle) = spawn_redirect_http_server(target_url.as_str());

    let outcome =
        navigate_with_guards(url.as_str(), 2_000, true, 3, true, 4_096, Some("session=abc123"))
            .await;
    redirect_handle.join().expect("redirect server should exit");
    let target_request = target_handle.join().expect("target server should exit");

    assert!(outcome.success, "redirected navigation should succeed: {}", outcome.error);
    assert!(
        !target_request.to_ascii_lowercase().contains("cookie:"),
        "cross-host redirect target must not receive original cookie header: {target_request}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_truncates_oversized_successful_response() {
    let (url, handle) = spawn_chunked_http_server(
        200,
        &["<html><head><title>Oversized</title></head>", "<body>very ", "large</body></html>"],
    );
    let outcome = navigate_with_guards(url.as_str(), 2_000, true, 3, true, 16, None).await;
    assert!(outcome.success, "oversized successful page should still navigate");
    assert!(
        outcome.error.contains("max_response_bytes"),
        "size limit warning should be explicit: {}",
        outcome.error
    );
    assert!(outcome.error.contains("truncated"), "warning should say the body was bounded");
    assert!(outcome.page_body.len() <= 16, "page body should stay bounded");
    assert!(
        outcome.body_bytes > 16,
        "reported body bytes should reflect the first oversized chunk boundary"
    );
    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_allows_response_exactly_at_size_limit() {
    let body = "<html><head><title>Exact</title></head><body>1234</body></html>";
    let (url, handle) = spawn_chunked_http_server(
        200,
        &["<html><head><title>Exact</title></head>", "<body>1234</body></html>"],
    );
    let outcome =
        navigate_with_guards(url.as_str(), 2_000, true, 3, true, body.len() as u64, None).await;
    assert!(outcome.success, "payload at the cap must succeed");
    assert_eq!(outcome.body_bytes, body.len() as u64);
    assert_eq!(outcome.page_body, body);
    assert_eq!(outcome.title, "Exact");
    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_blocks_private_target_by_default() {
    let outcome =
        navigate_with_guards("http://127.0.0.1:8080/", 1_000, true, 3, false, 1024, None).await;
    assert!(!outcome.success, "private targets should be blocked by default");
    assert!(
        outcome.error.contains("private/local"),
        "error should explain private target block: {}",
        outcome.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_blocks_remote_private_ip_after_cached_dns_mismatch() {
    reset_dns_validation_tracking_for_tests();
    let target = "http://localhost:8080/";

    let outcome = navigate_with_guards(target, 2_000, true, 3, false, 8 * 1024, None).await;
    assert!(!outcome.success, "private DNS target must be blocked before request dispatch");
    assert!(
        outcome.error.contains("private/local"),
        "error should explain private target policy guard: {}",
        outcome.error
    );

    reset_dns_validation_tracking_for_tests();
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_target_url_pins_dns_resolution_for_hostnames() {
    let target = Url::parse("http://localhost:8080/").expect("URL should parse");
    let validated =
        validate_target_url(&target, true).await.expect("localhost should validate with opt-in");
    assert_eq!(validated.host.as_deref(), Some("localhost"));
    assert!(
        !validated.resolved_socket_addrs.is_empty(),
        "validated hostnames should return at least one pinned socket address"
    );
    assert!(
        validated.resolved_socket_addrs.iter().all(|addr| addr.port() == 8080),
        "pinned socket addresses should preserve the URL port"
    );
}

#[test]
fn validate_target_url_blocking_rejects_non_canonical_ipv4_literals() {
    for url in ["http://2130706433/", "http://0x7f000001/", "http://0177.0.0.1/", "http://127.1/"] {
        let error =
            validate_target_url_blocking(url, false).expect_err("non-canonical host must fail");
        assert!(
            error.contains("non-canonical IPv4 literal") || error.contains("private/local"),
            "error should keep fail-closed host guard semantics for {url}: {error}"
        );
    }
}

#[test]
fn dns_validation_cache_prunes_lru_entries() {
    let now = Instant::now();
    let mut cache = DnsValidationCache::new(2, Duration::from_secs(10));

    cache.insert_nxdomain("alpha.example".to_owned(), now);
    cache.insert_nxdomain("beta.example".to_owned(), now);
    assert!(
        cache.contains("alpha.example", now),
        "most recently touched key should remain in LRU cache"
    );
    cache.insert_nxdomain("gamma.example".to_owned(), now);

    assert!(
        cache.contains("alpha.example", now),
        "most recently touched key should remain in LRU cache"
    );
    assert!(
        !cache.contains("beta.example", now),
        "least recently used key should be evicted when capacity is exceeded"
    );
    assert!(cache.contains("gamma.example", now), "newly inserted key should be retained");
}

#[test]
fn dns_validation_cache_short_circuits_cached_nxdomain() {
    reset_dns_validation_tracking_for_tests();
    let host = "cached-nxdomain.invalid";
    let target = format!("http://{host}/");
    store_dns_nxdomain_cache(host);
    let second_error = validate_target_url_blocking(target.as_str(), false)
        .expect_err("cached NXDOMAIN validation should fail");
    assert!(
        second_error.contains("cached NXDOMAIN")
            || second_error.contains("DNS resolution failed for host 'cached-nxdomain.invalid'"),
        "failure should remain fail-closed for cached NXDOMAIN host: {second_error}"
    );
}

#[test]
fn constant_time_eq_bytes_requires_exact_match() {
    assert!(
        constant_time_eq_bytes(b"Bearer same-token", b"Bearer same-token"),
        "exactly matching tokens should compare as equal"
    );
    assert!(
        !constant_time_eq_bytes(b"Bearer same-token", b"Bearer same-tokem"),
        "single-byte difference should compare as non-equal"
    );
    assert!(
        !constant_time_eq_bytes(b"Bearer short", b"Bearer much-longer"),
        "different-length tokens should compare as non-equal"
    );
}

#[test]
fn chromium_new_tab_retryable_error_classifier_matches_transient_protocol_races() {
    assert!(
        chromium_new_tab_error_is_retryable("Event waited for never came: Target.targetCreated"),
        "target-created startup race should be retryable"
    );
    assert!(
        chromium_new_tab_error_is_retryable(
            "WebSocket protocol error: Sending after closing is not allowed"
        ),
        "transient websocket close race should be retryable"
    );
    assert!(
        chromium_new_tab_error_is_retryable(
            "Unable to make method calls because underlying connection is closed"
        ),
        "transient connection-close race should be retryable"
    );
    assert!(
        !chromium_new_tab_error_is_retryable(
            "browser.new_tab denied by policy: disallowed target origin"
        ),
        "non-transient policy failures must remain non-retryable"
    );
}

#[test]
fn chromium_remote_ip_guard_records_incident_for_private_addresses() {
    let incident = Arc::new(StdMutex::new(None::<String>));
    record_chromium_remote_ip_incident(
        Some("http://127.0.0.1/"),
        Some("127.0.0.1"),
        false,
        &incident,
    );
    let message = incident
        .lock()
        .expect("guard should lock after IPv4 incident")
        .clone()
        .expect("private IPv4 response IP should record an incident");
    assert!(
        message.contains("127.0.0.1"),
        "incident should include violating IPv4 address: {message}"
    );

    let incident = Arc::new(StdMutex::new(None::<String>));
    record_chromium_remote_ip_incident(Some("http://[::1]/"), Some("[::1]"), false, &incident);
    let message = incident
        .lock()
        .expect("guard should lock after IPv6 incident")
        .clone()
        .expect("private IPv6 response IP should record an incident");
    assert!(message.contains("::1"), "incident should include violating IPv6 address: {message}");
}

#[test]
fn chromium_process_replacement_retains_pending_remote_ip_incidents() {
    let incident = Arc::new(StdMutex::new(None::<String>));
    let replacement = chromium_security_incident_for_launch(Some(&incident));

    assert!(Arc::ptr_eq(&incident, &replacement));
    record_chromium_remote_ip_incident(
        Some("http://10.0.0.8/"),
        Some("10.0.0.8"),
        false,
        &incident,
    );
    assert!(
        replacement
            .lock()
            .expect("replacement incident guard")
            .as_deref()
            .is_some_and(|reason| reason.contains("10.0.0.8")),
        "replacement runtime must observe incidents recorded through the prior runtime"
    );

    let fresh = chromium_security_incident_for_launch(None);
    assert!(fresh.lock().expect("fresh incident guard").is_none());
}

#[test]
fn chromium_process_reconnect_enforces_incidents_around_launch() {
    let reconnect = CHROMIUM_ENGINE_SOURCE
        .split_once("async fn reconnect_chromium_process_runtime(")
        .map(|(_, source)| source)
        .and_then(|source| {
            source.split_once("/// Looks up the live tab handle").map(|(body, _)| body)
        })
        .expect("Chromium process reconnect body");
    let launch =
        reconnect.find("launch_chromium_session_runtime(").expect("Chromium replacement launch");
    let guard_checks = reconnect
        .match_indices("enforce_chromium_remote_ip_guard(runtime, session_id).await?;")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();

    assert_eq!(guard_checks.len(), 2, "process reconnect must enforce exactly around replacement");
    assert!(guard_checks[0] < launch, "pending incidents must terminate before replacement launch");
    assert!(guard_checks[1] > launch, "incidents racing replacement must terminate before success");
}

#[test]
fn chromium_remote_ip_guard_ignores_public_and_opted_in_private_targets() {
    let incident = Arc::new(StdMutex::new(None::<String>));
    record_chromium_remote_ip_incident(None, Some("93.184.216.34"), false, &incident);
    assert!(
        incident.lock().expect("guard should lock after public response IP check").is_none(),
        "public response IP should not produce a remote IP guard incident"
    );

    record_chromium_remote_ip_incident(
        Some("http://127.0.0.1/"),
        Some("127.0.0.1"),
        true,
        &incident,
    );
    assert!(
        incident.lock().expect("guard should lock after private-target opt-in check").is_none(),
        "private-target opt-in should bypass remote IP guard incidents"
    );
}

#[test]
fn chromium_remote_ip_guard_ignores_local_proxy_hop_for_public_response_url() {
    let incident = Arc::new(StdMutex::new(None::<String>));
    record_chromium_remote_ip_incident(
        Some("https://93.184.216.34/"),
        Some("127.0.0.1"),
        false,
        &incident,
    );
    assert!(
        incident.lock().expect("guard should lock after local proxy response IP check").is_none(),
        "loopback SOCKS5 proxy hop should not block an otherwise public response URL"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn chromium_session_proxy_blocks_private_targets_without_opt_in() {
    let proxy = ChromiumSessionProxy::spawn(false)
        .await
        .expect("proxy should start for private-target deny policy");
    let proxy_addr = proxy
        .proxy_uri
        .strip_prefix("socks5://")
        .expect("proxy uri should use socks5 scheme")
        .to_owned();
    let mut stream = tokio::net::TcpStream::connect(proxy_addr.as_str())
        .await
        .expect("test client should connect to SOCKS5 proxy");

    stream.write_all(&[0x05, 0x01, 0x00]).await.expect("proxy handshake should write greeting");
    let mut method_reply = [0_u8; 2];
    stream
        .read_exact(&mut method_reply)
        .await
        .expect("proxy handshake should read selected method");
    assert_eq!(method_reply, [0x05, 0x00], "proxy should accept SOCKS5 no-auth handshake");

    stream
        .write_all(&[0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1, 0, 80])
        .await
        .expect("proxy request should send CONNECT target");
    let mut connect_reply = [0_u8; 10];
    stream
        .read_exact(&mut connect_reply)
        .await
        .expect("proxy should return CONNECT policy decision");
    assert_eq!(
        connect_reply[1], 0x02,
        "private localhost target must be denied when allow_private_targets=false"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn chromium_session_proxy_allows_private_targets_after_session_opt_in() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fixture listener should bind on loopback");
    let target_port = listener.local_addr().expect("fixture listener addr should resolve").port();
    let fixture_server = tokio::spawn(async move {
        let (mut inbound, _) =
            listener.accept().await.expect("fixture server should accept proxied connection");
        let mut request = [0_u8; 4];
        inbound
            .read_exact(&mut request)
            .await
            .expect("fixture server should read tunneled payload");
        assert_eq!(&request, b"ping", "proxy tunnel should forward payload bytes");
        inbound.write_all(b"pong").await.expect("fixture server should write tunneled response");
    });

    let proxy = ChromiumSessionProxy::spawn(true)
        .await
        .expect("proxy should start for private-target opt-in policy");
    let proxy_addr = proxy
        .proxy_uri
        .strip_prefix("socks5://")
        .expect("proxy uri should use socks5 scheme")
        .to_owned();
    let mut stream = tokio::net::TcpStream::connect(proxy_addr.as_str())
        .await
        .expect("test client should connect to SOCKS5 proxy");

    stream.write_all(&[0x05, 0x01, 0x00]).await.expect("proxy handshake should write greeting");
    let mut method_reply = [0_u8; 2];
    stream
        .read_exact(&mut method_reply)
        .await
        .expect("proxy handshake should read selected method");
    assert_eq!(method_reply, [0x05, 0x00], "proxy should accept SOCKS5 no-auth handshake");

    let target_port_bytes = target_port.to_be_bytes();
    stream
        .write_all(&[
            0x05,
            0x01,
            0x00,
            0x01,
            127,
            0,
            0,
            1,
            target_port_bytes[0],
            target_port_bytes[1],
        ])
        .await
        .expect("proxy request should send CONNECT target");
    let mut connect_reply = [0_u8; 10];
    stream.read_exact(&mut connect_reply).await.expect("proxy should return CONNECT decision");
    assert_eq!(
        connect_reply[1], 0x00,
        "opted-in session should allow loopback target through proxy"
    );

    stream.write_all(b"ping").await.expect("proxy tunnel should forward request payload");
    let mut response = [0_u8; 4];
    stream.read_exact(&mut response).await.expect("proxy tunnel should forward response payload");
    assert_eq!(&response, b"pong");

    fixture_server.await.expect("fixture server task should complete successfully");
}

#[test]
fn chromium_private_target_policy_scopes_navigation_override_to_tab_target() {
    let policy = Arc::new(ChromiumPrivateTargetPolicy::new(false));
    assert!(
        !policy.allows_host_port("127.0.0.1", 7143),
        "private target should start denied without a scoped override"
    );

    let scoped = policy
        .scoped_url_allowance("tab-a", "http://127.0.0.1:7143/status")
        .expect("scoped private-target allowance should parse")
        .expect("network URL should create scoped allowance");
    assert!(
        policy.allows_tab_url("tab-a", "http://127.0.0.1:7143/status"),
        "owning tab should be allowed for the exact scoped URL"
    );
    assert!(
        !policy.allows_tab_url("tab-a", "http://127.0.0.1:7143/next"),
        "passive URL checks must not widen the scoped navigation URL"
    );
    assert!(
        policy.allows_tab_request_target("tab-a", "http://127.0.0.1:7143/styles.css"),
        "response guard should allow same-target subresources during active navigation"
    );
    assert!(
        !policy.allows_tab_url("tab-b", "http://127.0.0.1:7143/status"),
        "another tab must not inherit the scoped target"
    );
    assert!(
        !policy.allows_host_port("127.0.0.1", 7143),
        "SOCKS5 proxy must not allow the target until request interception authorizes it"
    );
    assert!(
        policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/status"),
        "owning tab request should arm one proxy CONNECT allowance"
    );
    assert!(
        policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/styles.css"),
        "same-origin subresource should be authorized while the navigation scope is active"
    );
    assert!(
        policy.allows_host_port("127.0.0.1", 7143),
        "SOCKS5 proxy should consume the armed target allowance"
    );
    assert!(
        policy.allows_host_port("127.0.0.1", 7143),
        "second same-target request should arm its own one-shot proxy allowance"
    );
    assert!(
        !policy.allows_host_port("127.0.0.1", 7143),
        "scoped proxy allowances must be one-shot"
    );
    assert!(
        !policy.allows_host_port("127.0.0.1", 7144),
        "SOCKS5 proxy should not inherit the override for another port"
    );

    drop(scoped);
    assert!(
        !policy.allows_tab_url("tab-a", "http://127.0.0.1:7143/styles.css"),
        "dropping the scoped guard should revoke the allowance"
    );
}

#[test]
fn chromium_private_target_policy_revokes_target_after_navigation_scope() {
    let policy = Arc::new(ChromiumPrivateTargetPolicy::new(false));
    let scoped = policy
        .scoped_url_allowance("tab-a", "http://127.0.0.1:7143/")
        .expect("navigation allowance should parse")
        .expect("private navigation should create scoped allowance");
    assert!(
        policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/"),
        "navigation request should be allowed by the scoped URL"
    );
    assert!(
        policy.allows_host_port("127.0.0.1", 7143),
        "navigation request should arm one proxy CONNECT allowance"
    );
    assert!(
        policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/mock-data.json"),
        "same-target subresources should remain available during the navigation scope"
    );
    assert!(
        policy.allows_host_port("127.0.0.1", 7143),
        "authorized subresource should arm its own proxy CONNECT allowance"
    );
    drop(scoped);
    assert!(
        !policy.allows_tab_url("tab-a", "http://127.0.0.1:7143/mock-data.json"),
        "temporary navigation scope must not widen after guard release"
    );
    assert!(
        !policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/mock-data.json"),
        "the tab must not retain private-target access after navigation completes"
    );
    assert!(
        !policy.authorize_tab_request_url("tab-b", "http://127.0.0.1:7143/mock-data.json"),
        "another tab must not inherit the expired private target"
    );
    assert!(
        !policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7144/mock-data.json"),
        "same tab must not inherit another private target"
    );
}

#[test]
fn chromium_private_target_policy_revokes_unconsumed_proxy_grant_with_scope() {
    let policy = Arc::new(ChromiumPrivateTargetPolicy::new(false));
    let scoped = policy
        .scoped_url_allowance("tab-a", "http://127.0.0.1:7143/")
        .expect("navigation allowance should parse")
        .expect("private navigation should create scoped allowance");
    assert!(
        policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/"),
        "navigation request should arm its tab-bound proxy allowance"
    );

    drop(scoped);

    assert!(
        !policy.allows_host_port("127.0.0.1", 7143),
        "an unconsumed proxy allowance must not survive its navigation scope"
    );

    let scoped_a = policy
        .scoped_url_allowance("tab-a", "http://127.0.0.1:7143/")
        .expect("first tab allowance should parse")
        .expect("first private navigation should create scoped allowance");
    let scoped_b = policy
        .scoped_url_allowance("tab-b", "http://127.0.0.1:7143/")
        .expect("second tab allowance should parse")
        .expect("second private navigation should create scoped allowance");
    assert!(policy.authorize_tab_request_url("tab-a", "http://127.0.0.1:7143/"));
    assert!(policy.authorize_tab_request_url("tab-b", "http://127.0.0.1:7143/"));

    drop(scoped_a);

    assert!(
        policy.allows_host_port("127.0.0.1", 7143),
        "the second tab's live scope must retain its own proxy allowance"
    );
    assert!(
        !policy.allows_host_port("127.0.0.1", 7143),
        "the expired tab's proxy allowance must be removed independently"
    );
    drop(scoped_b);
}

#[tokio::test(flavor = "multi_thread")]
async fn chromium_session_proxy_scoped_private_override_rejects_unrelated_target() {
    let allowed_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("allowed fixture listener should bind on loopback");
    let allowed_port =
        allowed_listener.local_addr().expect("allowed listener addr should resolve").port();
    let blocked_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("blocked fixture listener should bind on loopback");
    let blocked_port =
        blocked_listener.local_addr().expect("blocked listener addr should resolve").port();
    assert_ne!(allowed_port, blocked_port, "fixture ports should differ");

    let proxy = ChromiumSessionProxy::spawn(false)
        .await
        .expect("proxy should start for scoped private-target policy");
    let scoped_url = format!("http://127.0.0.1:{allowed_port}/");
    let _scoped = proxy
        .private_target_policy()
        .scoped_url_allowance("tab-a", scoped_url.as_str())
        .expect("scoped private-target allowance should parse")
        .expect("network URL should create scoped allowance");
    assert!(
        proxy.private_target_policy().authorize_tab_request_url("tab-a", scoped_url.as_str()),
        "owning tab request should arm only its scoped target"
    );

    let proxy_addr = proxy
        .proxy_uri
        .strip_prefix("socks5://")
        .expect("proxy uri should use socks5 scheme")
        .to_owned();
    let mut stream = tokio::net::TcpStream::connect(proxy_addr.as_str())
        .await
        .expect("test client should connect to SOCKS5 proxy");

    stream.write_all(&[0x05, 0x01, 0x00]).await.expect("proxy handshake should write greeting");
    let mut method_reply = [0_u8; 2];
    stream
        .read_exact(&mut method_reply)
        .await
        .expect("proxy handshake should read selected method");
    assert_eq!(method_reply, [0x05, 0x00], "proxy should accept SOCKS5 no-auth handshake");

    let blocked_port_bytes = blocked_port.to_be_bytes();
    stream
        .write_all(&[
            0x05,
            0x01,
            0x00,
            0x01,
            127,
            0,
            0,
            1,
            blocked_port_bytes[0],
            blocked_port_bytes[1],
        ])
        .await
        .expect("proxy request should send CONNECT target");
    let mut connect_reply = [0_u8; 10];
    stream
        .read_exact(&mut connect_reply)
        .await
        .expect("proxy should return CONNECT policy decision");
    assert_eq!(
        connect_reply[1], 0x02,
        "scoped override for one private target must not allow another target"
    );
}

#[test]
fn non_loopback_bind_requires_auth_token() {
    let admin = parse_daemon_bind_socket("0.0.0.0", 7143).expect("admin address should parse");
    let grpc = parse_daemon_bind_socket("127.0.0.1", DEFAULT_GRPC_PORT)
        .expect("grpc address should parse");
    let error = enforce_non_loopback_bind_auth(admin, grpc, false)
        .expect_err("non-loopback bind without auth token must fail closed");
    assert!(
        error.to_string().contains("auth token is required"),
        "error should explain startup auth requirement: {error}"
    );
}

#[test]
fn loopback_binds_allow_missing_auth_token() {
    let admin = parse_daemon_bind_socket("127.0.0.1", 7143).expect("admin address should parse");
    let grpc =
        parse_daemon_bind_socket("::1", DEFAULT_GRPC_PORT).expect("grpc address should parse");
    enforce_non_loopback_bind_auth(admin, grpc, false)
        .expect("loopback-only binds may run without auth token");
}

#[test]
fn non_loopback_bind_allows_when_auth_is_enabled() {
    let admin = parse_daemon_bind_socket("0.0.0.0", 7143).expect("admin address should parse");
    let grpc =
        parse_daemon_bind_socket("0.0.0.0", DEFAULT_GRPC_PORT).expect("grpc address should parse");
    enforce_non_loopback_bind_auth(admin, grpc, true)
        .expect("configured auth token should allow non-loopback bind");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_roundtrip_navigate_and_screenshot() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Integration Title</title></head><body>ok</body></html>",
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };

    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");
    assert_eq!(navigate.title, "Integration Title");

    let screenshot = service
        .screenshot(Request::new(browser_v1::ScreenshotRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            max_bytes: 1024,
            format: "png".to_owned(),
        }))
        .await
        .expect("screenshot should succeed")
        .into_inner();
    assert!(screenshot.success, "screenshot should succeed");
    assert_eq!(screenshot.image_bytes, ONE_BY_ONE_PNG);

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_wait_for_requires_requested_selector_and_text() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><body><div id='card-error'>Card declined</div></body></html>",
    );

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed: {}", navigate.error);

    let missing_text = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#card-error".to_owned(),
            text: "not valid".to_owned(),
            timeout_ms: 75,
            poll_interval_ms: 25,
            capture_failure_screenshot: false,
            max_failure_screenshot_bytes: 0,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(!missing_text.success, "selector-only match must not satisfy requested text");
    assert!(missing_text.matched_selector.is_empty());
    assert!(missing_text.matched_text.is_empty());
    assert!(missing_text.error.contains("not satisfied"), "{}", missing_text.error);

    let matched = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id),
            selector: "#card-error".to_owned(),
            text: "Card declined".to_owned(),
            timeout_ms: 75,
            poll_interval_ms: 25,
            capture_failure_screenshot: false,
            max_failure_screenshot_bytes: 0,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(matched.success, "selector and text should match: {}", matched.error);
    assert_eq!(matched.matched_selector, "#card-error");
    assert_eq!(matched.matched_text, "Card declined");

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_engine_executes_real_dom_actions() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>Chromium Fixture</title><script>
function markClicked(){
  document.getElementById('status').textContent='clicked';
  setTimeout(() => { throw new Error('late click diagnostic'); }, 50);
}
function markTyped(value){document.getElementById('typed-status').textContent=value;}
function markFiltered(){document.getElementById('filter-status').textContent='filtered:active';}
</script></head><body>
<input id='name-input' oninput='markTyped(this.value)' />
<button id='submit-btn' onclick='markClicked()'>Submit</button>
<button class='filter' data-filter='active' onclick='markFiltered()'>Active</button>
<div id='current-user'>Ada</div>
<div id='typed-status'>empty</div>
<div id='filter-status'>filter:idle</div>
<div id='status'>idle</div>
</body></html>"#,
        8,
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium mode");
    let session_id = created.session_id.expect("session id should exist");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);
    assert_eq!(navigate.title, "Chromium Fixture");

    let typed = service
        .r#type(Request::new(browser_v1::TypeRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#name-input".to_owned(),
            text: "hello chromium".to_owned(),
            clear_existing: true,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("type should execute")
        .into_inner();
    assert!(typed.success, "chromium type should succeed: {}", typed.error);

    let typed_wait = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#typed-status".to_owned(),
            text: "hello chromium".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for typed input side-effect should execute")
        .into_inner();
    assert!(
        typed_wait.success,
        "chromium wait_for should observe DOM input event after type: {}",
        typed_wait.error
    );

    let filter_ready = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "button.filter[data-filter=\"active\"]".to_owned(),
            text: String::new(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for action selector should execute")
        .into_inner();
    assert!(
        filter_ready.success,
        "chromium wait_for should resolve the action selector before click: {}",
        filter_ready.error
    );

    let filter_click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "button.filter[data-filter=\"active\"]".to_owned(),
            max_retries: 2,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("CSS selector click should execute")
        .into_inner();
    assert!(
        filter_click.success,
        "chromium click should use the same live CSS selector semantics as wait_for: {}",
        filter_click.error
    );

    let filter_wait = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#filter-status".to_owned(),
            text: "filtered:active".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for filter side-effect should execute")
        .into_inner();
    assert!(
        filter_wait.success,
        "chromium wait_for should observe DOM change after CSS selector click: {}",
        filter_wait.error
    );

    let highlight = service
        .highlight(Request::new(browser_v1::HighlightRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#current-user".to_owned(),
            timeout_ms: 3_000,
            duration_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("highlight should execute")
        .into_inner();
    assert!(highlight.success, "chromium highlight should succeed: {}", highlight.error);

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#submit-btn".to_owned(),
            max_retries: 2,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "chromium click should succeed: {}", click.error);
    {
        let sessions = service.runtime.sessions.lock().await;
        let session = sessions.get(session_id.ulid.as_str()).expect("session should remain active");
        let active_tab = session.active_tab().expect("active tab should remain available");
        assert!(
            active_tab.console_log.iter().any(|entry| entry.kind == "page_error"
                && entry.message.contains("late click diagnostic")),
            "chromium click should return after late click diagnostics settle: {:?}",
            active_tab.console_log
        );
    }

    let waited = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: String::new(),
            text: "clicked".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(
        waited.success,
        "chromium wait_for should observe DOM change after click: {}",
        waited.error
    );

    let screenshot = service
        .screenshot(Request::new(browser_v1::ScreenshotRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            max_bytes: 220 * 1024,
            format: "png".to_owned(),
        }))
        .await
        .expect("screenshot should execute")
        .into_inner();
    assert!(screenshot.success, "chromium screenshot should succeed: {}", screenshot.error);
    assert!(
        screenshot.image_bytes.starts_with(&[137, 80, 78, 71]),
        "chromium screenshot must return PNG payload"
    );
    let layout_metrics =
        screenshot.layout_metrics.as_ref().expect("screenshot should include layout metrics");
    assert!(
        layout_metrics.viewport_width > 0 && layout_metrics.viewport_height > 0,
        "layout metrics should expose effective viewport dimensions"
    );
    assert!(
        layout_metrics.document_scroll_width >= layout_metrics.document_client_width,
        "layout metrics should expose document overflow inputs"
    );

    let observed = service
        .observe(Request::new(browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            include_dom_snapshot: true,
            include_accessibility_tree: true,
            include_visible_text: true,
            max_dom_snapshot_bytes: 32 * 1024,
            max_accessibility_tree_bytes: 32 * 1024,
            max_visible_text_bytes: 8 * 1024,
            capture_selectors: Vec::new(),
            computed_style_properties: Vec::new(),
            max_capture_text_bytes: 0,
        }))
        .await
        .expect("observe should execute")
        .into_inner();
    assert!(observed.success, "chromium observe should succeed: {}", observed.error);
    assert!(
        observed.visible_text.contains("clicked"),
        "observe visible text should reflect click side-effect from real DOM"
    );

    let failed_click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id),
            selector: "#missing-action-target".to_owned(),
            max_retries: 1,
            timeout_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 220 * 1024,
        }))
        .await
        .expect("failed click should execute")
        .into_inner();
    assert!(!failed_click.success, "missing selector click should fail");
    assert!(
        failed_click.failure_screenshot_bytes.starts_with(&[137, 80, 78, 71]),
        "chromium failure screenshot must return PNG payload"
    );
    assert_ne!(
        failed_click.failure_screenshot_bytes, ONE_BY_ONE_PNG,
        "chromium failure screenshot should capture the active page, not the simulated placeholder"
    );

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_handles_native_dialogs_with_generation_fence() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>Dialog Fixture</title></head><body>
<div id="result">idle</div>
</body></html>"#,
        8,
    );
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 128 * 1024,
        max_response_bytes: 128 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Chromium,
        chromium_path: Some(chromium_path),
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("chromium runtime should initialize");
    runtime_state.resilience_profile = BrowserResilienceProfile {
        dialog_timeout_ms: 3_000,
        ..BrowserResilienceProfile::resilient_for_tests()
    };
    let runtime = std::sync::Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium mode");
    let session_id = created.session_id.expect("session id should exist");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: url.clone(),
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let (_, tab) = chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
        .await
        .expect("active Chromium tab should exist");
    run_chromium_blocking("schedule test prompt", move || {
        tab.evaluate(
            "setTimeout(() => { const value = prompt('Choose a bounded value', 'default'); document.getElementById('result').textContent = value ?? 'dismissed'; }, 0);",
            false,
        )
        .map(|_| ())
        .map_err(|error| format!("failed to schedule test prompt: {error}"))
    })
    .await
    .expect("prompt should be scheduled");

    let actions_before_inspection = {
        let sessions = runtime.sessions.lock().await;
        sessions.get(session_id.ulid.as_str()).expect("dialog session should exist").action_count
    };
    let mut inspected = None;
    for _ in 0..40 {
        let response = service
            .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                action: browser_v1::BrowserDialogAction::Inspect.into(),
                expected_generation: 0,
                prompt_text: String::new(),
            }))
            .await
            .expect("dialog inspection should execute")
            .into_inner();
        if response.present {
            inspected = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let inspected = inspected.expect("native prompt should become observable");
    assert!(inspected.success);
    let first_event = inspected.event.expect("dialog event should be returned");
    assert_eq!(first_event.dialog_type, "prompt");
    assert_eq!(first_event.message, "Choose a bounded value");
    {
        let mut sessions = runtime.sessions.lock().await;
        let session =
            sessions.get_mut(session_id.ulid.as_str()).expect("dialog session should exist");
        assert_eq!(
            session.action_count, actions_before_inspection,
            "read-only dialog inspection must not consume the mutation budget"
        );
        session.budget.max_actions_per_session = session.action_count;
    }

    let blocked = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Respond.into(),
            expected_generation: first_event.generation,
            prompt_text: "blocked".to_owned(),
        }))
        .await
        .expect("blocked dialog response should execute")
        .into_inner();
    assert!(!blocked.success);
    assert_eq!(blocked.error_code, "dialog_action_blocked");
    assert!(blocked.error.contains("session action budget exceeded"));
    {
        let mut sessions = runtime.sessions.lock().await;
        let session =
            sessions.get_mut(session_id.ulid.as_str()).expect("dialog session should exist");
        assert_eq!(session.action_count, actions_before_inspection);
        session.budget.max_actions_per_session = session.action_count.saturating_add(16);
    }

    let responded = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Respond.into(),
            expected_generation: first_event.generation,
            prompt_text: "handled".to_owned(),
        }))
        .await
        .expect("dialog response should execute")
        .into_inner();
    assert!(responded.success, "prompt response should succeed: {}", responded.error);
    assert!(responded.mutated_page);
    {
        let sessions = runtime.sessions.lock().await;
        let session = sessions.get(session_id.ulid.as_str()).expect("dialog session should exist");
        assert_eq!(session.action_count, actions_before_inspection.saturating_add(1));
        let entry = session.action_log.back().expect("dialog mutation should be audited");
        assert_eq!(entry.action_name, "dialog_respond");
        assert!(entry.success);
        assert_eq!(entry.outcome, "dialog_handled");
    }

    let updated = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#result".to_owned(),
            text: "handled".to_owned(),
            timeout_ms: 3_000,
            poll_interval_ms: 25,
            capture_failure_screenshot: false,
            max_failure_screenshot_bytes: 0,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(updated.success, "prompt result should reach the live DOM: {}", updated.error);

    let (_, tab) = chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
        .await
        .expect("active Chromium tab should still exist");
    run_chromium_blocking("schedule test confirmation", move || {
        tab.evaluate("setTimeout(() => confirm('Continue safely?'), 0);", false)
            .map(|_| ())
            .map_err(|error| format!("failed to schedule test confirmation: {error}"))
    })
    .await
    .expect("confirmation should be scheduled");
    let mut second_event = None;
    for _ in 0..40 {
        let response = service
            .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                action: browser_v1::BrowserDialogAction::Inspect.into(),
                expected_generation: 0,
                prompt_text: String::new(),
            }))
            .await
            .expect("second dialog inspection should execute")
            .into_inner();
        if response.present {
            second_event = response.event;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let second_event = second_event.expect("confirmation should become observable");
    assert!(second_event.generation > first_event.generation);

    let stale = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Accept.into(),
            expected_generation: first_event.generation,
            prompt_text: String::new(),
        }))
        .await
        .expect("stale dialog action should execute")
        .into_inner();
    assert!(!stale.success);
    assert_eq!(stale.error_code, "stale_dialog_generation");
    assert!(stale.present);

    let dismissed = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Dismiss.into(),
            expected_generation: second_event.generation,
            prompt_text: String::new(),
        }))
        .await
        .expect("current dialog dismissal should execute")
        .into_inner();
    assert!(dismissed.success, "current dialog dismissal should succeed");
    assert!(dismissed.mutated_page);

    let (_, tab) = chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
        .await
        .expect("active Chromium tab should still exist");
    run_chromium_blocking("schedule secret-bearing alert", move || {
        tab.evaluate("setTimeout(() => alert('authorization: Bearer secret-value'), 0);", false)
            .map(|_| ())
            .map_err(|error| format!("failed to schedule alert: {error}"))
    })
    .await
    .expect("alert should be scheduled");
    let mut alert_event = None;
    for _ in 0..12 {
        let response = service
            .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                action: browser_v1::BrowserDialogAction::Inspect.into(),
                expected_generation: 0,
                prompt_text: String::new(),
            }))
            .await
            .expect("alert inspection should execute")
            .into_inner();
        if response.present {
            alert_event = response.event;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let alert_event = alert_event.expect("native alert should become observable");
    assert_eq!(alert_event.dialog_type, "alert");
    assert_eq!(alert_event.message, "<redacted>");
    let alert_dismissed = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Dismiss.into(),
            expected_generation: alert_event.generation,
            prompt_text: String::new(),
        }))
        .await
        .expect("alert dismissal should execute")
        .into_inner();
    assert!(alert_dismissed.success);

    let (_, tab) = chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
        .await
        .expect("active Chromium tab should still exist");
    run_chromium_blocking("schedule expiring alert", move || {
        tab.evaluate("setTimeout(() => alert('Timeout safely'), 0);", false)
            .map(|_| ())
            .map_err(|error| format!("failed to schedule timeout alert: {error}"))
    })
    .await
    .expect("timeout alert should be scheduled");
    let mut timeout_event = None;
    for _ in 0..12 {
        let response = service
            .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                action: browser_v1::BrowserDialogAction::Inspect.into(),
                expected_generation: 0,
                prompt_text: String::new(),
            }))
            .await
            .expect("timeout alert inspection should execute")
            .into_inner();
        if response.present {
            timeout_event = response.event;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let timeout_event = timeout_event.expect("expiring alert should become observable");
    tokio::time::sleep(Duration::from_millis(3_300)).await;
    let timed_out = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Accept.into(),
            expected_generation: timeout_event.generation,
            prompt_text: String::new(),
        }))
        .await
        .expect("timed-out dialog response should execute")
        .into_inner();
    assert!(!timed_out.success);
    assert!(timed_out.timed_out);
    assert_eq!(timed_out.error_code, "dialog_timed_out_safe_dismiss");

    let (_, tab) = chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
        .await
        .expect("active Chromium tab should still exist");
    run_chromium_blocking("schedule navigation cleanup confirmation", move || {
        tab.evaluate("setTimeout(() => confirm('Clear before navigation?'), 0);", false)
            .map(|_| ())
            .map_err(|error| format!("failed to schedule cleanup confirmation: {error}"))
    })
    .await
    .expect("cleanup confirmation should be scheduled");
    let mut cleanup_event = None;
    for _ in 0..12 {
        let response = service
            .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                action: browser_v1::BrowserDialogAction::Inspect.into(),
                expected_generation: 0,
                prompt_text: String::new(),
            }))
            .await
            .expect("cleanup dialog inspection should execute")
            .into_inner();
        if response.present {
            cleanup_event = response.event;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let cleanup_event = cleanup_event.expect("cleanup confirmation should become observable");
    let renavigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigation cleanup should execute")
        .into_inner();
    assert!(renavigate.success, "navigation should clean pending dialog: {}", renavigate.error);
    let stale_after_navigation = service
        .handle_dialog(Request::new(browser_v1::HandleDialogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            action: browser_v1::BrowserDialogAction::Accept.into(),
            expected_generation: cleanup_event.generation,
            prompt_text: String::new(),
        }))
        .await
        .expect("cleaned dialog generation should remain observable")
        .into_inner();
    assert!(!stale_after_navigation.success);
    assert_eq!(stale_after_navigation.error_code, "stale_dialog_generation");

    let health = runtime
        .browser_session_health
        .lock()
        .await
        .get(session_id.ulid.as_str())
        .cloned()
        .expect("session health should exist");
    let snapshot = health.lock().expect("session health lock").snapshot();
    assert_eq!(snapshot.dialog_timeout_count, 1);
    assert_eq!(snapshot.dialog_navigation_cleanup_count, 1);
    assert_eq!(snapshot.reason_code, BROWSER_DIALOG_NAVIGATION_CLEANUP_REASON);

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_recovers_target_and_process_with_health_evidence() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 128 * 1024,
        max_response_bytes: 128 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Chromium,
        chromium_path: Some(chromium_path),
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("chromium runtime should initialize");
    runtime_state.resilience_profile = BrowserResilienceProfile::resilient_for_tests();
    let runtime = Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for Chromium mode");
    let session_id = created.session_id.expect("session id should exist");
    let tab_id = runtime
        .sessions
        .lock()
        .await
        .get(session_id.ulid.as_str())
        .expect("logical session should exist")
        .active_tab_id
        .clone();

    let original_tab =
        chromium_tab_for_session(runtime.as_ref(), session_id.ulid.as_str(), tab_id.as_str())
            .await
            .expect("original target should be healthy");
    let original_target_id = original_tab.get_target_id().to_string();
    run_chromium_blocking("close target for reconnect test", move || {
        original_tab
            .close(false)
            .map(|_| ())
            .map_err(|error| format!("failed to close target for reconnect test: {error}"))
    })
    .await
    .expect("target crash simulation should succeed");
    let recovered_target =
        chromium_tab_for_session(runtime.as_ref(), session_id.ulid.as_str(), tab_id.as_str())
            .await
            .expect("resilient profile should replace a closed target");
    assert_ne!(recovered_target.get_target_id().to_string(), original_target_id);

    let removed_runtime = runtime.chromium_sessions.lock().await.remove(session_id.ulid.as_str());
    drop(removed_runtime);
    let recovered_after_process_loss =
        chromium_tab_for_session(runtime.as_ref(), session_id.ulid.as_str(), tab_id.as_str())
            .await
            .expect("resilient profile should replace a missing process runtime");
    recovered_after_process_loss
        .get_target_info()
        .expect("recovered process should expose a live target");

    let health = service
        .health(Request::new(browser_v1::BrowserHealthRequest { v: 1 }))
        .await
        .expect("health diagnostics should execute")
        .into_inner();
    assert_eq!(health.resilience_profile, "resilient");
    assert!(health.automatic_reconnect_enabled);
    assert_eq!(health.healthy_sessions, 1);
    assert_eq!(health.target_reconnect_count, 1);
    assert_eq!(health.process_reconnect_count, 1);

    let mut inspect = Request::new(browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: false,
        include_storage: false,
        include_action_log: false,
        include_network_log: false,
        include_page_snapshot: false,
        include_console_log: false,
        include_page_diagnostics: false,
        max_cookie_bytes: 0,
        max_storage_bytes: 0,
        max_action_log_entries: 0,
        max_network_log_entries: 0,
        max_network_log_bytes: 0,
        max_dom_snapshot_bytes: 0,
        max_visible_text_bytes: 0,
        max_console_log_entries: 0,
        max_console_log_bytes: 0,
    });
    insert_principal(&mut inspect, "user:ops");
    let inspected = service
        .inspect_session(inspect)
        .await
        .expect("session diagnostics should execute")
        .into_inner();
    let session_health = inspected.session_health.expect("explicit session health should exist");
    assert_eq!(session_health.state, "ready");
    assert_eq!(session_health.target_reconnect_count, 1);
    assert_eq!(session_health.process_reconnect_count, 1);
    assert_eq!(session_health.reason_code, "browser.process.reconnected");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_captures_blob_download_artifacts() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>Blob Download Fixture</title><script>
window.addEventListener('DOMContentLoaded', () => {
  const staleBlob = new Blob(['unrelated,secret\n'], { type: 'text/csv' });
  const staleUrl = URL.createObjectURL(staleBlob);
  const staleAnchor = document.createElement('a');
  staleAnchor.href = staleUrl;
  staleAnchor.download = 'unrelated-secret.csv';
  staleAnchor.click();
  URL.revokeObjectURL(staleUrl);
});
function exportCsv(){
  const blob = new Blob(['id,name\n1001,Ada Lovelace\n1002,Grace Hopper\n'], { type: 'text/csv;charset=utf-8' });
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = objectUrl;
  anchor.download = 'upload-export.csv';
  anchor.click();
  URL.revokeObjectURL(objectUrl);
  document.getElementById('status').textContent = 'Export ready: upload-export.csv';
}
</script></head><body><button id="export" onclick="exportCsv()">Export</button><div id="status">idle</div></body></html>"#,
        8,
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium blob download mode");
    let session_id = created.session_id.expect("session id should exist");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#export".to_owned(),
            max_retries: 1,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "blob download click should succeed: {}", click.error);
    let click_artifact = click.artifact.expect("blob download should return artifact metadata");
    assert_eq!(click_artifact.file_name, "upload-export.csv");
    assert_eq!(click_artifact.mime_type, "text/csv");
    assert!(!click_artifact.quarantined);

    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut list_request, "user:ops");
    let listed = service
        .list_download_artifacts(list_request)
        .await
        .expect("list_download_artifacts should execute")
        .into_inner();
    assert_eq!(listed.artifacts.len(), 1, "blob artifact should be registered");

    let mut get_request = Request::new(browser_v1::GetDownloadArtifactRequest {
        v: 1,
        session_id: Some(session_id),
        artifact_id: click_artifact.artifact_id,
        max_bytes: DOWNLOAD_MAX_FILE_BYTES,
    });
    insert_principal(&mut get_request, "user:ops");
    let fetched = service
        .get_download_artifact(get_request)
        .await
        .expect("get_download_artifact should execute")
        .into_inner();
    assert!(fetched.success, "blob artifact fetch should succeed: {}", fetched.error);
    assert!(String::from_utf8_lossy(fetched.content.as_slice()).contains("Grace Hopper"));

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_profile_persistence_restores_local_storage() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>Cart Fixture</title><script>
function render(){document.getElementById('cart').textContent='cart:'+(localStorage.getItem('cart')||'0');}
function addCart(){localStorage.setItem('cart','1');render();}
window.addEventListener('DOMContentLoaded',render);
</script></head><body><button id="add" onclick="addCart()">Add</button><div id="cart">cart:loading</div></body></html>"#,
        8,
    );
    let state_dir = tempfile::tempdir().expect("state temp dir should be available");
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 256 * 1024,
        max_response_bytes: 256 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Chromium,
        chromium_path: Some(chromium_path),
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("chromium runtime should initialize");
    runtime_state.state_store = Some(
        PersistedStateStore::new(state_dir.path().join("state"), [9_u8; STATE_KEY_LEN])
            .expect("state store should initialize"),
    );
    let runtime = std::sync::Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime };

    let profile = service
        .create_profile(Request::new(browser_v1::CreateProfileRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            name: "Cart".to_owned(),
            theme_color: "#0f766e".to_owned(),
            persistence_enabled: true,
            private_profile: false,
        }))
        .await
        .expect("create_profile should succeed")
        .into_inner()
        .profile
        .expect("profile should be present");
    let profile_id = profile.profile_id.expect("profile id should be present");

    let first = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(profile_id.clone()),
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("first create_session should succeed");
    let first_session_id = first.session_id.expect("first session id should be present");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(first_session_id.clone()),
            url: url.clone(),
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("first navigate should execute")
        .into_inner();
    assert!(navigate.success, "first navigate should succeed: {}", navigate.error);

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(first_session_id.clone()),
            selector: "#add".to_owned(),
            max_retries: 2,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "cart click should succeed: {}", click.error);

    let waited = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(first_session_id.clone()),
            selector: "#cart".to_owned(),
            text: "cart:1".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(waited.success, "cart state should update before close: {}", waited.error);

    let closed = service
        .close_session(Request::new(browser_v1::CloseSessionRequest {
            v: 1,
            session_id: Some(first_session_id),
        }))
        .await
        .expect("close_session should execute")
        .into_inner();
    assert!(closed.closed, "first session should close cleanly");

    let second = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(profile_id),
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("second create_session should succeed");
    assert!(second.state_restored, "second session should restore profile snapshot");
    let second_session_id = second.session_id.expect("second session id should be present");

    let restored = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(second_session_id),
            selector: "#cart".to_owned(),
            text: "cart:1".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for restored state should execute")
        .into_inner();
    assert!(
        restored.success,
        "restored persistent profile should expose live localStorage-backed cart state without manual navigation: {}",
        restored.error
    );

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_click_type_and_wait_for_on_fixture_page() {
    let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Actions</title></head><body><input id=\"email\" name=\"email\" /><button id=\"submit\">Submit</button></body></html>",
        );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            selector: "#submit".to_owned(),
            max_retries: 2,
            timeout_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "click action should succeed");
    assert_eq!(click.action_log.as_ref().map(|value| value.action_name.as_str()), Some("click"));

    let typed = service
        .r#type(Request::new(browser_v1::TypeRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            selector: "#email".to_owned(),
            text: "agent@example.com".to_owned(),
            clear_existing: true,
            timeout_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 1024,
        }))
        .await
        .expect("type should execute")
        .into_inner();
    assert!(typed.success, "type action should succeed");
    assert_eq!(typed.typed_bytes, "agent@example.com".len() as u64);
    assert_eq!(typed.action_log.as_ref().map(|value| value.action_name.as_str()), Some("type"));
    {
        let sessions = service.runtime.sessions.lock().await;
        let session = sessions.get(session_id.as_str()).expect("session should remain active");
        let active_tab = session.active_tab().expect("active tab should remain available");
        assert_eq!(
            active_tab.typed_inputs.get("#email").map(String::as_str),
            Some("agent@example.com"),
            "typed control state should remain available as form interaction state"
        );
        assert!(
            session.storage_entries.values().all(|entries| !entries.contains_key("#email")),
            "typed control selectors must not be reported as page-owned storage entries"
        );
    }

    let viewport = service
        .set_viewport(Request::new(browser_v1::SetViewportRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            width: 375,
            height: 667,
            device_scale_factor: 2.0,
            mobile: true,
            timeout_ms: 500,
        }))
        .await
        .expect("viewport should execute")
        .into_inner();
    assert!(viewport.success, "viewport action should succeed: {}", viewport.error);
    assert_eq!(viewport.width, 375);
    assert_eq!(viewport.height, 667);
    assert_eq!(viewport.device_scale_factor, 2.0);
    assert!(viewport.mobile);
    assert_eq!(
        viewport.action_log.as_ref().map(|value| value.action_name.as_str()),
        Some("viewport")
    );

    let waited = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            selector: "#submit".to_owned(),
            text: String::new(),
            timeout_ms: 300,
            poll_interval_ms: 25,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 1024,
        }))
        .await
        .expect("wait_for should execute")
        .into_inner();
    assert!(waited.success, "wait_for should match existing selector");
    assert_eq!(waited.matched_selector, "#submit");

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_viewport_rejects_excessive_pixel_area() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");
    let (url, handle) = spawn_static_http_server(200, "<html><body>viewport</body></html>");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 1_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "navigate should succeed before viewport action: {}", navigate.error);

    let accepted_4k = service
        .set_viewport(Request::new(browser_v1::SetViewportRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            width: 3840,
            height: 2160,
            device_scale_factor: 2.0,
            mobile: false,
            timeout_ms: 500,
        }))
        .await
        .expect("4K at 2x should remain allowed")
        .into_inner();
    assert!(accepted_4k.success, "4K viewport should succeed: {}", accepted_4k.error);

    let css_area_status = service
        .set_viewport(Request::new(browser_v1::SetViewportRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            width: 10_000,
            height: 10_000,
            device_scale_factor: 1.0,
            mobile: false,
            timeout_ms: 500,
        }))
        .await
        .expect_err("extreme CSS area should be rejected before browser execution");
    assert_eq!(css_area_status.code(), tonic::Code::InvalidArgument);
    assert!(css_area_status.message().contains("CSS pixels"), "{}", css_area_status.message());

    let effective_area_status = service
        .set_viewport(Request::new(browser_v1::SetViewportRequest {
            v: 1,
            session_id: Some(session_id),
            width: 5000,
            height: 3000,
            device_scale_factor: 2.0,
            mobile: false,
            timeout_ms: 500,
        }))
        .await
        .expect_err("large scaled viewport should be rejected before browser execution");
    assert_eq!(effective_area_status.code(), tonic::Code::InvalidArgument);
    assert!(
        effective_area_status.message().contains("device pixels"),
        "{}",
        effective_area_status.message()
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_network_log_includes_same_origin_fetch_failures() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium network-log test");
    let session_id = created.session_id.expect("session id should exist");

    let (url, handle) = spawn_fetch_failure_http_server();
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#loadProfile".to_owned(),
            max_retries: 1,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "chromium click should succeed: {}", click.error);

    let waited = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#status".to_owned(),
            text: "profile failed".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for fetch failure marker should execute")
        .into_inner();
    assert!(waited.success, "fetch failure marker should render: {}", waited.error);

    let mut network_log_request = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(session_id),
        limit: 20,
        include_headers: false,
        max_payload_bytes: 32 * 1024,
    });
    insert_principal(&mut network_log_request, "user:ops");
    let network_log = service
        .network_log(network_log_request)
        .await
        .expect("network_log should execute")
        .into_inner();
    assert!(network_log.success, "network log call should succeed: {}", network_log.error);
    assert!(
        network_log.entries.iter().any(|entry| {
            entry.request_url.ends_with("/api/profile") && entry.status_code == 500
        }),
        "network log should include same-origin fetch 500 entries: {:?}",
        network_log.entries
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_preserves_navigated_private_origin_for_user_fetches() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: false,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium private-origin fetch test");
    let session_id = created.session_id.expect("session id should exist");

    let (url, handle) = spawn_click_fetch_http_server();
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#loadData".to_owned(),
            max_retries: 1,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(click.success, "chromium click should succeed: {}", click.error);

    let waited = service
        .wait_for(Request::new(browser_v1::WaitForRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: String::new(),
            text: "Atlas".to_owned(),
            timeout_ms: 5_000,
            poll_interval_ms: 50,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("wait_for same-origin fetch result should execute")
        .into_inner();
    assert!(
        waited.success,
        "same-origin local fetch after navigation should render fetched data: {}",
        waited.error
    );

    let network_log_deadline = Instant::now() + Duration::from_secs(5);
    let network_log = loop {
        let mut network_log_request = Request::new(browser_v1::NetworkLogRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            limit: 20,
            include_headers: false,
            max_payload_bytes: 32 * 1024,
        });
        insert_principal(&mut network_log_request, "user:ops");
        let network_log = service
            .network_log(network_log_request)
            .await
            .expect("network_log should execute")
            .into_inner();
        assert!(network_log.success, "network log call should succeed: {}", network_log.error);
        // Chromium can report status 0 for a same-origin fetch after page JS has
        // consumed the 200 response; `wait_for` above proves the data rendered.
        let has_completed_json_fetch = network_log.entries.iter().any(|entry| {
            entry.request_url.ends_with("/mock-data.json")
                && (entry.status_code == 200 || entry.status_code == 0)
        });
        if has_completed_json_fetch || Instant::now() >= network_log_deadline {
            break network_log;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        network_log.entries.iter().any(|entry| {
            entry.request_url.ends_with("/mock-data.json")
                && (entry.status_code == 200 || entry.status_code == 0)
        }),
        "network log should include same-origin JSON fetch entries: {:?}",
        network_log.entries
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_allows_initial_private_css_subresource() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: false,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium CSS subresource test");
    let session_id = created.session_id.expect("session id should exist");

    let (url, handle) = spawn_css_subresource_http_server();
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(
        navigate.success,
        "chromium navigate should allow initial stylesheet load: {}",
        navigate.error
    );

    let observed = service
        .observe(Request::new(browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            include_dom_snapshot: false,
            include_accessibility_tree: false,
            include_visible_text: false,
            max_dom_snapshot_bytes: 0,
            max_accessibility_tree_bytes: 0,
            max_visible_text_bytes: 0,
            capture_selectors: vec![".cta".to_owned()],
            computed_style_properties: vec![
                "display".to_owned(),
                "padding-top".to_owned(),
                "background-color".to_owned(),
            ],
            max_capture_text_bytes: 128,
        }))
        .await
        .expect("observe should execute")
        .into_inner();
    assert!(observed.success, "observe should succeed: {}", observed.error);
    let capture = observed
        .element_captures
        .iter()
        .find(|capture| capture.selector == ".cta")
        .expect("observe should return .cta capture");
    assert!(capture.found, "capture should find the styled CTA: {capture:?}");
    let style_value = |name: &str| {
        capture
            .computed_styles
            .iter()
            .find(|style| style.name == name)
            .map(|style| style.value.as_str())
    };
    assert_eq!(style_value("display"), Some("block"));
    assert_eq!(style_value("padding-top"), Some("14px"));
    assert_eq!(style_value("background-color"), Some("rgb(31, 77, 255)"));

    let mut network_log_request = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(session_id),
        limit: 20,
        include_headers: false,
        max_payload_bytes: 32 * 1024,
    });
    insert_principal(&mut network_log_request, "user:ops");
    let network_log = service
        .network_log(network_log_request)
        .await
        .expect("network_log should execute")
        .into_inner();
    assert!(
        network_log.entries.iter().any(|entry| entry.request_url.ends_with("/styles.css")),
        "network log should include the initial stylesheet request: {:?}",
        network_log.entries
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_salvages_timeout_when_dom_is_reached() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: false,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium timeout salvage test");
    let session_id = created.session_id.expect("session id should exist");

    let (url, handle) = spawn_hanging_subresource_http_server();
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: url.clone(),
            timeout_ms: 300,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(
        navigate.success,
        "navigate should salvage usable DOM after subresource timeout: {}",
        navigate.error
    );
    assert_eq!(navigate.final_url, url);
    assert!(
        navigate.error.contains("timed out"),
        "salvaged timeout should keep diagnostic warning: {}",
        navigate.error
    );
    let observed = service
        .observe(Request::new(browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(session_id),
            include_dom_snapshot: false,
            include_accessibility_tree: false,
            include_visible_text: true,
            max_dom_snapshot_bytes: 0,
            max_accessibility_tree_bytes: 0,
            max_visible_text_bytes: 4 * 1024,
            capture_selectors: Vec::new(),
            computed_style_properties: Vec::new(),
            max_capture_text_bytes: 0,
        }))
        .await
        .expect("observe should execute")
        .into_inner();
    assert!(observed.success, "observe should succeed after salvaged navigate");
    assert!(
        observed.visible_text.contains("usable dom"),
        "observe should read live DOM after salvaged timeout: {}",
        observed.visible_text
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_refreshes_snapshot_before_allowlisted_actions() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime: std::sync::Arc::clone(&runtime) };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: vec!["127.0.0.1".to_owned()],
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium allowlist test");
    let session_id = created.session_id.expect("session id should exist");
    {
        let mut sessions = runtime.sessions.lock().await;
        let session = sessions
            .get_mut(session_id.ulid.as_str())
            .expect("created chromium test session should exist");
        let active_tab = session
            .tabs
            .get_mut(session.active_tab_id.as_str())
            .expect("created chromium test session should have an active tab record");
        active_tab.last_url = Some("http://127.0.0.1/allowed".to_owned());
        active_tab.last_page_body = "<html><body>ok</body></html>".to_owned();
        active_tab.last_title = "Allowed Fixture".to_owned();
    }

    let (_tab_id, tab) =
        chromium_active_tab_for_session(runtime.as_ref(), session_id.ulid.as_str())
            .await
            .expect("active chromium tab should exist");
    run_chromium_blocking("chromium stale allowlist test navigate", move || {
        tab.navigate_to(
            "data:text/html,<html><body><button id='blocked'>Blocked</button></body></html>",
        )
        .map_err(|error| format!("failed to navigate Chromium tab to blocked page: {error}"))?;
        tab.wait_until_navigated()
            .map_err(|error| format!("Chromium blocked-page navigation failed: {error}"))?;
        Ok(())
    })
    .await
    .expect("direct Chromium navigation should succeed without refreshing the session snapshot");

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id),
            selector: "#blocked".to_owned(),
            max_retries: 0,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("click should execute")
        .into_inner();
    assert!(
        !click.success,
        "stale Chromium snapshots must not let action allowlists authorize the redirected page"
    );
    assert!(
        click.error.contains("action domain allowlist")
            || click.error.contains("failed to resolve host"),
        "allowlist refresh should reject stale Chromium redirects: {}",
        click.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_clamps_untrusted_session_budgets() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let default_budget = runtime.default_budget.clone();
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: Some(browser_v1::SessionBudget {
                max_navigation_timeout_ms: u64::MAX,
                max_session_lifetime_ms: u64::MAX,
                max_screenshot_bytes: u64::MAX,
                max_response_bytes: u64::MAX,
                max_action_timeout_ms: u64::MAX,
                max_type_input_bytes: u64::MAX,
                max_actions_per_session: u64::MAX,
                max_actions_per_window: u64::MAX,
                action_rate_window_ms: u64::MAX,
                max_action_log_entries: u64::MAX,
                max_observe_snapshot_bytes: u64::MAX,
                max_visible_text_bytes: u64::MAX,
                max_network_log_entries: u64::MAX,
                max_network_log_bytes: u64::MAX,
            }),
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let effective_budget = created.effective_budget.expect("effective budget should be returned");
    assert_eq!(
        effective_budget.max_navigation_timeout_ms, default_budget.max_navigation_timeout_ms,
        "untrusted session budgets must not widen max_navigation_timeout_ms"
    );
    assert_eq!(
        effective_budget.max_session_lifetime_ms, default_budget.max_session_lifetime_ms,
        "untrusted session budgets must not widen max_session_lifetime_ms"
    );
    assert_eq!(
        effective_budget.max_screenshot_bytes, default_budget.max_screenshot_bytes,
        "untrusted session budgets must not widen max_screenshot_bytes"
    );
    assert_eq!(
        effective_budget.max_response_bytes, default_budget.max_response_bytes,
        "untrusted session budgets must not widen max_response_bytes"
    );
    assert_eq!(
        effective_budget.max_action_timeout_ms, default_budget.max_action_timeout_ms,
        "untrusted session budgets must not widen max_action_timeout_ms"
    );
    assert_eq!(
        effective_budget.max_type_input_bytes, default_budget.max_type_input_bytes,
        "untrusted session budgets must not widen max_type_input_bytes"
    );
    assert_eq!(
        effective_budget.max_actions_per_session, default_budget.max_actions_per_session,
        "untrusted session budgets must not widen max_actions_per_session"
    );
    assert_eq!(
        effective_budget.max_actions_per_window, default_budget.max_actions_per_window,
        "untrusted session budgets must not widen max_actions_per_window"
    );
    assert_eq!(
        effective_budget.action_rate_window_ms, default_budget.action_rate_window_ms,
        "untrusted session budgets must not widen action_rate_window_ms"
    );
    assert_eq!(
        effective_budget.max_action_log_entries, default_budget.max_action_log_entries as u64,
        "untrusted session budgets must not widen max_action_log_entries"
    );
    assert_eq!(
        effective_budget.max_observe_snapshot_bytes, default_budget.max_observe_snapshot_bytes,
        "untrusted session budgets must not widen max_observe_snapshot_bytes"
    );
    assert_eq!(
        effective_budget.max_visible_text_bytes, default_budget.max_visible_text_bytes,
        "untrusted session budgets must not widen max_visible_text_bytes"
    );
    assert_eq!(
        effective_budget.max_network_log_entries, default_budget.max_network_log_entries as u64,
        "untrusted session budgets must not widen max_network_log_entries"
    );
    assert_eq!(
        effective_budget.max_network_log_bytes, default_budget.max_network_log_bytes,
        "untrusted session budgets must not widen max_network_log_bytes"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_rejects_oversized_type_input() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><body><input id=\"name\" name=\"name\" /></body></html>",
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: Some(browser_v1::SessionBudget {
                max_navigation_timeout_ms: 0,
                max_session_lifetime_ms: 0,
                max_screenshot_bytes: 0,
                max_response_bytes: 0,
                max_action_timeout_ms: 0,
                max_type_input_bytes: 4,
                max_actions_per_session: 0,
                max_actions_per_window: 0,
                action_rate_window_ms: 0,
                max_action_log_entries: 0,
                max_observe_snapshot_bytes: 0,
                max_visible_text_bytes: 0,
                max_network_log_entries: 0,
                max_network_log_bytes: 0,
            }),
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let typed = service
        .r#type(Request::new(browser_v1::TypeRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            selector: "#name".to_owned(),
            text: "abcdef".to_owned(),
            clear_existing: false,
            timeout_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 1024,
        }))
        .await
        .expect("type request should complete")
        .into_inner();
    assert!(!typed.success, "oversized type payload should fail");
    assert!(
        typed.error.contains("max_type_input_bytes"),
        "error should contain explicit budget context: {}",
        typed.error
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_blocks_download_click_when_disabled() {
    let (url, handle) = spawn_static_http_server(200, PARITY_DOWNLOAD_TRIGGER_HTML);
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            selector: "#download-link".to_owned(),
            max_retries: 0,
            timeout_ms: 500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 1024,
        }))
        .await
        .expect("click request should complete")
        .into_inner();
    assert!(!click.success, "download-like click should be blocked by default");
    assert!(
        click.error.contains("allow_downloads=false"),
        "error should identify explicit download policy: {}",
        click.error
    );
    assert_eq!(
        click.failure_screenshot_bytes, ONE_BY_ONE_PNG,
        "blocked click should include bounded failure screenshot"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_observe_returns_stable_sanitized_snapshot() {
    let (url, handle) = spawn_static_http_server(200, PARITY_TRICKY_DOM_HTML);
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: format!("{url}?access_token=topsecret&lang=en"),
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let observed = service
        .observe(Request::new(browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            include_dom_snapshot: true,
            include_accessibility_tree: true,
            include_visible_text: true,
            max_dom_snapshot_bytes: 8 * 1024,
            max_accessibility_tree_bytes: 8 * 1024,
            max_visible_text_bytes: 2 * 1024,
            capture_selectors: Vec::new(),
            computed_style_properties: Vec::new(),
            max_capture_text_bytes: 0,
        }))
        .await
        .expect("observe should execute")
        .into_inner();
    assert!(observed.success, "observe should succeed");
    assert!(
        observed.dom_snapshot.contains("<form"),
        "dom snapshot should include structural elements"
    );
    assert!(
        observed.dom_snapshot.contains("token=<redacted>")
            || observed.dom_snapshot.contains("access_token=<redacted>"),
        "dom snapshot should redact sensitive URL query params: {}",
        observed.dom_snapshot
    );
    assert!(
        !observed.dom_snapshot.contains("topsecret"),
        "sensitive query values must be redacted from dom snapshot: {}",
        observed.dom_snapshot
    );
    assert!(
        observed.accessibility_tree.contains("role=button"),
        "accessibility tree should include semantic roles: {}",
        observed.accessibility_tree
    );
    assert!(
        observed.visible_text.contains("Portal"),
        "visible text extraction should include visible text content"
    );
    assert!(
        observed.page_url.contains("access_token=<redacted>"),
        "observed page URL should be redacted: {}",
        observed.page_url
    );

    handle.join().expect("test server thread should exit");
}

#[test]
fn dom_snapshot_redacts_form_values_and_preserves_state_flags() {
    let html = r#"
<html><body>
  <input id="project" name="project" type="text" value="Palyra Portal">
  <input id="owner" name="owner" type="email" value="owner@example.test">
  <input id="remember" name="remember" type="checkbox" value="yes" checked="true">
  <select id="region" name="region" value="eu"><option value="us">US</option><option value="eu" selected="true">EU</option></select>
</body></html>
"#;

    let (snapshot, truncated) = build_dom_snapshot(html, 8 * 1024);

    assert!(!truncated);
    assert!(
        snapshot.contains("id=\"owner\"") && snapshot.contains("value=\"<redacted>\""),
        "form values should be redacted in observe snapshots: {snapshot}"
    );
    assert!(
        !snapshot.contains("owner@example.test") && !snapshot.contains("Palyra Portal"),
        "form values must not leak in observe snapshots: {snapshot}"
    );
    assert!(
        snapshot.contains("checked=\"true\"") && snapshot.contains("selected=\"true\""),
        "form checked/selected state should be visible in observe snapshots: {snapshot}"
    );
}

#[test]
fn dom_snapshot_redacts_sensitive_form_values() {
    let html = r#"
<html><body>
  <input id="password" name="password" type="password" value="supersecret">
  <input id="csrf-token" name="csrf_token" type="hidden" value="token=supersecret">
  <input id="query" name="query" type="text" value="safe text">
</body></html>
"#;

    let (snapshot, truncated) = build_dom_snapshot(html, 8 * 1024);

    assert!(!truncated);
    assert!(
        snapshot.contains("id=\"query\"") && snapshot.contains("value=\"<redacted>\""),
        "form values should be redacted even when the field name is not sensitive: {snapshot}"
    );
    assert!(
        snapshot.contains("id=\"password\"") && snapshot.contains("value=\"<redacted>\""),
        "password values should be redacted: {snapshot}"
    );
    assert!(
        !snapshot.contains("supersecret") && !snapshot.contains("safe text"),
        "form values must not leak: {snapshot}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_observe_truncates_deterministically_when_oversized() {
    let large_body = format!(
            "<html><body><main>{}</main></body></html>",
            (0..80)
                .map(|index| format!("<section id=\"section-{index}\"><button id=\"btn-{index}\">Run {index}</button></section>"))
                .collect::<String>()
        );
    let (url, handle) = spawn_static_http_server(200, large_body.as_str());
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let request = browser_v1::ObserveRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        include_dom_snapshot: true,
        include_accessibility_tree: true,
        include_visible_text: true,
        max_dom_snapshot_bytes: 64,
        max_accessibility_tree_bytes: 64,
        max_visible_text_bytes: 48,
        capture_selectors: Vec::new(),
        computed_style_properties: Vec::new(),
        max_capture_text_bytes: 0,
    };
    let first = service
        .observe(Request::new(request.clone()))
        .await
        .expect("first observe should execute")
        .into_inner();
    let second = service
        .observe(Request::new(request))
        .await
        .expect("second observe should execute")
        .into_inner();
    assert!(
        first.dom_truncated && first.accessibility_tree_truncated && first.visible_text_truncated,
        "all observe channels should report truncation for oversized snapshots"
    );
    assert_eq!(first.dom_snapshot, second.dom_snapshot, "dom truncation must be deterministic");
    assert_eq!(
        first.accessibility_tree, second.accessibility_tree,
        "a11y truncation must be deterministic"
    );
    assert_eq!(
        first.visible_text, second.visible_text,
        "visible text truncation must be deterministic"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_network_log_redacts_sensitive_values() {
    let (url, handle) = spawn_static_http_server_with_headers(
        200,
        PARITY_NETWORK_LOG_HTML,
        &[
            ("Set-Cookie", "session=abc123; HttpOnly"),
            ("X-Api-Key", "secret-key"),
            ("Location", PARITY_REDIRECT_TOKEN_URL.trim()),
        ],
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: format!("{url}?access_token=supersecret&safe=1"),
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let mut without_headers_request = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
        limit: 10,
        include_headers: false,
        max_payload_bytes: 8 * 1024,
    });
    insert_principal(&mut without_headers_request, "user:ops");
    let without_headers = service
        .network_log(without_headers_request)
        .await
        .expect("network_log without headers should execute")
        .into_inner();
    assert!(without_headers.success, "network log call should succeed");
    assert!(!without_headers.entries.is_empty(), "network log should contain entries");
    assert!(
        without_headers.entries.iter().all(|entry| entry.headers.is_empty()),
        "headers must be excluded unless explicitly requested"
    );

    let mut with_headers_request = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        limit: 10,
        include_headers: true,
        max_payload_bytes: 8 * 1024,
    });
    insert_principal(&mut with_headers_request, "user:ops");
    let with_headers = service
        .network_log(with_headers_request)
        .await
        .expect("network_log with headers should execute")
        .into_inner();
    assert!(with_headers.success, "network log call should succeed");
    let entry = with_headers.entries.last().expect("network log should include at least one entry");
    assert!(
        entry.request_url.contains("access_token=<redacted>"),
        "network log URLs should redact sensitive query values: {}",
        entry.request_url
    );
    assert!(
        !entry.request_url.contains("supersecret"),
        "network log must not leak original sensitive URL values: {}",
        entry.request_url
    );
    assert!(
        entry
            .headers
            .iter()
            .any(|header| { header.name == "set-cookie" && header.value == "<redacted>" }),
        "set-cookie header should be redacted"
    );
    assert!(
        entry.headers.iter().any(|header| {
            header.name == "location" && header.value.contains("token=<redacted>")
        }),
        "location header URLs should be normalized and redacted"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_reset_state_clears_cookie_jar_for_fixture_domain() {
    let (url, handle) = spawn_cookie_state_http_server();
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let first = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: url.clone(),
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("first navigate should execute")
        .into_inner();
    assert!(first.success, "first navigation should succeed");

    let second = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: url.clone(),
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("second navigate should execute")
        .into_inner();
    assert!(second.success, "second navigation should replay cookie and succeed");

    let mut reset_request = Request::new(browser_v1::ResetStateRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
        clear_cookies: true,
        clear_storage: false,
        reset_tabs: false,
        reset_permissions: false,
    });
    insert_principal(&mut reset_request, "user:ops");
    let reset =
        service.reset_state(reset_request).await.expect("reset_state should execute").into_inner();
    assert!(reset.success, "reset_state should succeed");
    assert!(reset.cookies_cleared >= 1, "at least one cookie should be removed during reset");

    let third = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("third navigate should execute")
        .into_inner();
    assert!(
        !third.success && third.status_code == 401,
        "third navigation should fail after reset because cookie was cleared"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_reset_state_clears_network_log_baseline() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session =
            sessions.get_mut(session_id.ulid.as_str()).expect("session should exist for seeding");
        let active_tab =
            session.active_tab_mut().expect("session should retain an active tab for seeding");
        active_tab.network_log.push_back(NetworkLogEntryInternal {
            request_url: "https://example.com/api/stale".to_owned(),
            status_code: 200,
            timing_bucket: "lt_100ms".to_owned(),
            latency_ms: 12,
            captured_at_unix_ms: 1,
            headers: Vec::new(),
        });
    }

    let mut reset_request = Request::new(browser_v1::ResetStateRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        clear_cookies: false,
        clear_storage: false,
        reset_tabs: false,
        reset_permissions: false,
    });
    insert_principal(&mut reset_request, "user:ops");
    let reset =
        service.reset_state(reset_request).await.expect("reset_state should execute").into_inner();
    assert!(reset.success, "reset_state should succeed");

    let mut network_request = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(session_id),
        limit: 10,
        include_headers: false,
        max_payload_bytes: 8 * 1024,
    });
    insert_principal(&mut network_request, "user:ops");
    let network_log = service
        .network_log(network_request)
        .await
        .expect("network_log should execute")
        .into_inner();

    assert!(network_log.success, "network_log should succeed");
    assert!(
        network_log.entries.is_empty(),
        "reset_state should clear stale network evidence: {:?}",
        network_log.entries
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_permissions_default_to_deny() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");
    let permissions = service
        .get_permissions(Request::new(browser_v1::GetPermissionsRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        }))
        .await
        .expect("get_permissions should execute")
        .into_inner();
    assert!(permissions.success, "permission query should succeed");
    let effective = permissions.permissions.expect("permissions should be returned");
    assert_eq!(effective.camera, 1, "camera permission should default to deny");
    assert_eq!(effective.microphone, 1, "microphone permission should default to deny");
    assert_eq!(effective.location, 1, "location permission should default to deny");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_set_permissions_updates_session_state() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    let updated = service
        .set_permissions(Request::new(browser_v1::SetPermissionsRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            camera: 0,
            microphone: 0,
            location: 2,
            reset_to_default: false,
        }))
        .await
        .expect("set_permissions should execute")
        .into_inner();

    assert!(updated.success, "permission update should succeed");
    let effective = updated.permissions.expect("permissions should be returned");
    assert_eq!(effective.camera, 1, "camera should remain denied by default");
    assert_eq!(effective.microphone, 1, "microphone should remain denied by default");
    assert_eq!(effective.location, 2, "location should be allowed");

    let reset = service
        .set_permissions(Request::new(browser_v1::SetPermissionsRequest {
            v: 1,
            session_id: Some(session_id),
            camera: 0,
            microphone: 0,
            location: 0,
            reset_to_default: true,
        }))
        .await
        .expect("permission reset should execute")
        .into_inner();

    assert!(reset.success, "permission reset should succeed");
    let effective = reset.permissions.expect("reset permissions should be returned");
    assert_eq!(effective.camera, 1, "camera should reset to deny");
    assert_eq!(effective.microphone, 1, "microphone should reset to deny");
    assert_eq!(effective.location, 1, "location should reset to deny");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_permissions_set_updates_page_permissions_api() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>Permission Fixture</title><script>
async function checkGeo(){
  const status = await navigator.permissions.query({ name: 'geolocation' });
  document.getElementById('status').textContent = 'Permission: ' + status.state;
}
</script></head><body onload="checkGeo()"><button id="check" onclick="checkGeo()">Check</button><div id="status">loading</div></body></html>"#,
        8,
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium permissions mode");
    let session_id = created.session_id.expect("session id should exist");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let denied = service
        .set_permissions(Request::new(browser_v1::SetPermissionsRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            camera: 0,
            microphone: 0,
            location: 1,
            reset_to_default: false,
        }))
        .await
        .expect("deny permission update should execute")
        .into_inner();
    assert!(denied.success, "deny should apply to Chromium: {}", denied.error);
    click_permission_check_and_wait_for_text(&service, session_id.clone(), "Permission: denied")
        .await;

    let allowed = service
        .set_permissions(Request::new(browser_v1::SetPermissionsRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            camera: 0,
            microphone: 0,
            location: 2,
            reset_to_default: false,
        }))
        .await
        .expect("allow permission update should execute")
        .into_inner();
    assert!(allowed.success, "allow should apply to Chromium: {}", allowed.error);
    click_permission_check_and_wait_for_text(&service, session_id, "Permission: granted").await;

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_reset_permissions_revokes_prior_origin_grants() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let permission_fixture = r#"<html><head><title>Permission Fixture</title><script>
async function checkGeo(){
  const status = await navigator.permissions.query({ name: 'geolocation' });
  document.getElementById('status').textContent = 'Permission: ' + status.state;
}
</script></head><body onload="checkGeo()"><button id="check" onclick="checkGeo()">Check</button><div id="status">loading</div></body></html>"#;
    let (first_origin_url, first_origin_handle) =
        spawn_static_http_server_with_request_budget(200, permission_fixture, 32);
    let (second_origin_url, second_origin_handle) =
        spawn_static_http_server_with_request_budget(200, permission_fixture, 32);
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium permissions mode");
    let session_id = created.session_id.expect("session id should exist");

    let first_navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: first_origin_url.clone(),
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("first-origin navigate should execute")
        .into_inner();
    assert!(
        first_navigate.success,
        "first-origin navigate should succeed: {}",
        first_navigate.error
    );

    let allowed = service
        .set_permissions(Request::new(browser_v1::SetPermissionsRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            camera: 0,
            microphone: 0,
            location: 2,
            reset_to_default: false,
        }))
        .await
        .expect("allow permission update should execute")
        .into_inner();
    assert!(allowed.success, "allow should apply to Chromium: {}", allowed.error);
    click_permission_check_and_wait_for_text(&service, session_id.clone(), "Permission: granted")
        .await;

    let second_navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: second_origin_url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("second-origin navigate should execute")
        .into_inner();
    assert!(
        second_navigate.success,
        "second-origin navigate should succeed: {}",
        second_navigate.error
    );

    let mut reset_request = Request::new(browser_v1::ResetStateRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        clear_cookies: false,
        clear_storage: false,
        reset_tabs: false,
        reset_permissions: true,
    });
    insert_principal(&mut reset_request, "user:ops");
    let reset = service
        .reset_state(reset_request)
        .await
        .expect("permission reset_state should execute")
        .into_inner();
    assert!(reset.success, "reset should revoke Chromium permissions: {}", reset.error);
    let permissions = reset.permissions.expect("reset permissions should be returned");
    assert_eq!(permissions.location, 1, "reset should report denied location");
    click_permission_check_and_wait_for_text(&service, session_id.clone(), "Permission: denied")
        .await;

    let return_to_first = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url: first_origin_url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("return to first origin should execute")
        .into_inner();
    assert!(
        return_to_first.success,
        "return to first origin should succeed: {}",
        return_to_first.error
    );
    click_permission_check_and_wait_for_text(&service, session_id, "Permission: denied").await;

    drop(first_origin_handle);
    drop(second_origin_handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_chromium_click_registers_window_open_tab() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let _guard = chromium_integration_test_guard().await;
    let (url, handle) = spawn_static_http_server_with_request_budget(
        200,
        r#"<html><head><title>MockID App</title><script>
function login(){ window.open('/callback.html', '_blank'); }
</script></head><body><button id="login" onclick="login()">Login with MockID</button></body></html>"#,
        8,
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Chromium,
            chromium_path: Some(chromium_path),
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("chromium runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = create_session_with_retry_for_chromium_test(
        &service,
        browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        },
        3,
    )
    .await
    .expect("create_session should succeed for chromium popup mode");
    let session_id = created.session_id.expect("session id should exist");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            url,
            timeout_ms: 8_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);

    let initial_tabs = service
        .list_tabs(Request::new(browser_v1::ListTabsRequest {
            v: 1,
            session_id: Some(session_id.clone()),
        }))
        .await
        .expect("initial tabs list should execute")
        .into_inner();
    assert_eq!(initial_tabs.tabs.len(), 1, "fixture should start with one session tab");

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(session_id.clone()),
            selector: "#login".to_owned(),
            max_retries: 1,
            timeout_ms: 3_000,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 16 * 1024,
        }))
        .await
        .expect("login click should execute")
        .into_inner();
    assert!(click.success, "login click should succeed: {}", click.error);
    assert_eq!(
        click.action_log.as_ref().map(|entry| entry.outcome.as_str()),
        Some("clicked_new_tab")
    );

    let tabs = service
        .list_tabs(Request::new(browser_v1::ListTabsRequest { v: 1, session_id: Some(session_id) }))
        .await
        .expect("tabs list after popup should execute")
        .into_inner();
    assert!(tabs.success, "tabs list after popup should succeed");
    assert_eq!(
        tabs.tabs.len(),
        2,
        "window.open _blank should register a second tab: {:?}",
        tabs.tabs
    );
    assert!(
        tabs.tabs.iter().any(|tab| tab.url.contains("/callback.html")),
        "registered tabs should include the callback URL: {:?}",
        tabs.tabs
    );

    drop(handle);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_tabs_keep_independent_state() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Secondary Tab</title></head><body>tab-two</body></html>",
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let initial_tabs = service
        .list_tabs(Request::new(browser_v1::ListTabsRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
        }))
        .await
        .expect("list_tabs should execute")
        .into_inner();
    assert!(initial_tabs.success, "list_tabs should succeed");
    let first_tab_id = initial_tabs
        .tabs
        .iter()
        .find_map(|tab| tab.tab_id.as_ref().map(|value| value.ulid.clone()))
        .expect("first tab should be present");

    let opened = service
        .open_tab(Request::new(browser_v1::OpenTabRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: url.clone(),
            activate: true,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("open_tab should execute")
        .into_inner();
    assert!(opened.success, "open_tab should succeed");
    let second_tab_id = opened
        .tab
        .as_ref()
        .and_then(|tab| tab.tab_id.as_ref())
        .map(|value| value.ulid.clone())
        .expect("opened tab id should be present");

    let active_title = service
        .get_title(Request::new(browser_v1::GetTitleRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            max_title_bytes: 1024,
        }))
        .await
        .expect("get_title should execute")
        .into_inner();
    assert_eq!(active_title.title, "Secondary Tab");

    let switched = service
        .switch_tab(Request::new(browser_v1::SwitchTabRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: first_tab_id }),
        }))
        .await
        .expect("switch_tab should execute")
        .into_inner();
    assert!(switched.success, "switch_tab should succeed");

    let first_tab_title = service
        .get_title(Request::new(browser_v1::GetTitleRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            max_title_bytes: 1024,
        }))
        .await
        .expect("get_title on first tab should execute")
        .into_inner();
    assert!(
        first_tab_title.title.is_empty(),
        "first tab should keep independent state and remain blank"
    );

    let switched_back = service
        .switch_tab(Request::new(browser_v1::SwitchTabRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: second_tab_id }),
        }))
        .await
        .expect("switch_tab back should execute")
        .into_inner();
    assert!(switched_back.success, "switch back should succeed");
    let second_tab_title = service
        .get_title(Request::new(browser_v1::GetTitleRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            max_title_bytes: 1024,
        }))
        .await
        .expect("get_title on second tab should execute")
        .into_inner();
    assert_eq!(second_tab_title.title, "Secondary Tab");

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_open_tab_enforces_session_tab_limit() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    for _ in 0..(DEFAULT_MAX_TABS_PER_SESSION - 1) {
        let opened = service
            .open_tab(Request::new(browser_v1::OpenTabRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: String::new(),
                activate: false,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("open_tab should execute")
            .into_inner();
        assert!(opened.success, "open_tab should succeed before tab limit");
    }

    let rejected = service
        .open_tab(Request::new(browser_v1::OpenTabRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            url: String::new(),
            activate: false,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("open_tab should execute")
        .into_inner();
    assert!(!rejected.success, "open_tab should fail at tab limit");
    assert_eq!(rejected.error, "tab_limit_reached");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_profile_persistence_roundtrip_restores_state() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Persisted Profile</title></head><body><p>persisted</p></body></html>",
    );
    let state_dir = tempfile::tempdir().expect("state temp dir should be available");
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 128 * 1024,
        max_response_bytes: 128 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Simulated,
        chromium_path: None,
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("runtime should initialize");
    runtime_state.state_store = Some(
        PersistedStateStore::new(state_dir.path().join("state"), [7_u8; STATE_KEY_LEN])
            .expect("state store should initialize"),
    );
    let runtime = std::sync::Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime };

    let profile = service
        .create_profile(Request::new(browser_v1::CreateProfileRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            name: "Ops".to_owned(),
            theme_color: "#1f2937".to_owned(),
            persistence_enabled: true,
            private_profile: false,
        }))
        .await
        .expect("create_profile should succeed")
        .into_inner()
        .profile
        .expect("profile should be present");
    let profile_id = profile
        .profile_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("profile id should be present");

    let first_session = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(proto::palyra::common::v1::CanonicalId { ulid: profile_id.clone() }),
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("first create_session should succeed")
        .into_inner();
    let first_session_id = first_session
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("first session id should be present");
    assert!(first_session.persistence_enabled, "profile should enable persistence");
    assert_eq!(
        first_session.profile_id.as_ref().map(|value| value.ulid.as_str()),
        Some(profile_id.as_str())
    );

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId {
                ulid: first_session_id.clone(),
            }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should execute")
        .into_inner();
    assert!(navigate.success, "navigation should succeed");

    let closed = service
        .close_session(Request::new(browser_v1::CloseSessionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: first_session_id }),
        }))
        .await
        .expect("close_session should execute")
        .into_inner();
    assert!(closed.closed, "first session should close cleanly");

    let second_session = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(proto::palyra::common::v1::CanonicalId { ulid: profile_id.clone() }),
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("second create_session should succeed")
        .into_inner();
    let second_session_id = second_session
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("second session id should be present");
    assert!(second_session.state_restored, "second session should restore persisted state");
    assert_eq!(
        second_session.profile_id.as_ref().map(|value| value.ulid.as_str()),
        Some(profile_id.as_str())
    );

    let title = service
        .get_title(Request::new(browser_v1::GetTitleRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: second_session_id }),
            max_title_bytes: 1_024,
        }))
        .await
        .expect("get_title should execute")
        .into_inner();
    assert!(title.success, "title lookup should succeed after restore");
    assert_eq!(title.title, "Persisted Profile");

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_profile_session_create_uses_profile_id_as_persistence_id() {
    let state_dir = tempfile::tempdir().expect("state temp dir should be available");
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 128 * 1024,
        max_response_bytes: 128 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Simulated,
        chromium_path: None,
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("runtime should initialize");
    runtime_state.state_store = Some(
        PersistedStateStore::new(state_dir.path().join("state"), [17_u8; STATE_KEY_LEN])
            .expect("state store should initialize"),
    );
    let runtime = std::sync::Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime };

    let profile = service
        .create_profile(Request::new(browser_v1::CreateProfileRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            name: "Ops".to_owned(),
            theme_color: "#1f2937".to_owned(),
            persistence_enabled: true,
            private_profile: false,
        }))
        .await
        .expect("create_profile should succeed")
        .into_inner()
        .profile
        .expect("profile should be present");
    let profile_id = profile.profile_id.expect("profile id should be present");

    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: true,
            persistence_id: String::new(),
            profile_id: Some(profile_id.clone()),
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should accept profile persistence without persistence_id")
        .into_inner();

    assert!(created.persistence_enabled, "persistent profile should enable session persistence");
    assert_eq!(
        created.persistence_id, profile_id.ulid,
        "profile-backed persistence should use the canonical profile id as the state id"
    );
    assert_eq!(
        created.profile_id.as_ref().map(|value| value.ulid.as_str()),
        Some(created.persistence_id.as_str())
    );

    let active_profile_created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: true,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("active persistent profile should also supply the persistence id")
        .into_inner();
    assert_eq!(
        active_profile_created.persistence_id, created.persistence_id,
        "active profile fallback should use the same canonical profile state id"
    );
}

#[test]
fn validate_restored_snapshot_against_profile_accepts_legacy_hash_for_revision_zero() {
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: "user:ops".to_owned(),
        channel: None,
        tabs: vec![BrowserTabRecord::new(ulid::Ulid::new().to_string())],
        tab_order: Vec::new(),
        active_tab_id: String::new(),
        permissions: SessionPermissionsInternal::default(),
        cookie_jar: HashMap::new(),
        storage_entries: HashMap::new(),
        state_revision: 0,
        saved_at_unix_ms: 1_737_000_000_000,
    };
    let legacy_hash =
        persisted_snapshot_legacy_hash(&snapshot).expect("legacy hash generation should succeed");
    let profile = BrowserProfileRecord {
        profile_id: ulid::Ulid::new().to_string(),
        principal: "user:ops".to_owned(),
        name: "Ops".to_owned(),
        theme_color: None,
        created_at_unix_ms: 1_737_000_000_000,
        updated_at_unix_ms: 1_737_000_000_000,
        last_used_unix_ms: 1_737_000_000_000,
        persistence_enabled: true,
        private_profile: false,
        state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
        state_revision: 0,
        state_hash_sha256: Some(legacy_hash),
        record_hash_sha256: String::new(),
    };
    validate_restored_snapshot_against_profile(&snapshot, None, &profile)
        .expect("legacy hash path should stay backward compatible");
}

fn test_session_record() -> super::BrowserSessionRecord {
    super::BrowserSessionRecord::with_defaults(super::BrowserSessionInit {
        principal: "user:ops".to_owned(),
        channel: None,
        now: Instant::now(),
        idle_ttl: Duration::from_secs(60),
        budget: super::SessionBudget {
            max_navigation_timeout_ms: 5_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            max_action_timeout_ms: 5_000,
            max_type_input_bytes: 4 * 1024,
            max_actions_per_session: 256,
            max_actions_per_window: 20,
            action_rate_window_ms: 1_000,
            max_action_log_entries: 64,
            max_observe_snapshot_bytes: 64 * 1024,
            max_visible_text_bytes: 16 * 1024,
            max_network_log_entries: 64,
            max_network_log_bytes: 64 * 1024,
            max_tabs_per_session: 8,
        },
        allow_private_targets: false,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        profile_id: None,
        private_profile: false,
        persistence: super::SessionPersistenceState::default(),
    })
}

#[test]
fn apply_cookie_updates_enforces_domain_and_cookie_quotas() {
    let mut session = test_session_record();
    for idx in 0..(super::MAX_COOKIE_DOMAINS_PER_SESSION + 8) {
        super::apply_cookie_updates(
            &mut session,
            &[super::CookieUpdate {
                domain: format!("d{idx}.example.com"),
                name: "sid".to_owned(),
                value: format!("v{idx}"),
            }],
        );
    }
    assert_eq!(
        session.cookie_jar.len(),
        super::MAX_COOKIE_DOMAINS_PER_SESSION,
        "domain quota should cap growth"
    );

    let mut capped_domain_session = test_session_record();
    let capped_domain = "quota.example.com".to_owned();
    for idx in 0..(super::MAX_COOKIES_PER_DOMAIN + 8) {
        super::apply_cookie_updates(
            &mut capped_domain_session,
            &[super::CookieUpdate {
                domain: capped_domain.clone(),
                name: format!("c{idx}"),
                value: format!("v{idx}"),
            }],
        );
    }
    let cookies = capped_domain_session
        .cookie_jar
        .get(capped_domain.as_str())
        .expect("quota test domain should exist");
    assert_eq!(
        cookies.len(),
        super::MAX_COOKIES_PER_DOMAIN,
        "per-domain cookie quota should cap growth"
    );

    super::apply_cookie_updates(
        &mut capped_domain_session,
        &[super::CookieUpdate {
            domain: capped_domain.clone(),
            name: "c0".to_owned(),
            value: "updated".to_owned(),
        }],
    );
    assert_eq!(
        capped_domain_session
            .cookie_jar
            .get(capped_domain.as_str())
            .and_then(|domain| domain.get("c0"))
            .map(String::as_str),
        Some("updated"),
        "existing cookies should remain mutable at quota"
    );

    super::apply_cookie_updates(
        &mut capped_domain_session,
        &[super::CookieUpdate {
            domain: capped_domain.clone(),
            name: "c0".to_owned(),
            value: String::new(),
        }],
    );
    assert!(
        capped_domain_session
            .cookie_jar
            .get(capped_domain.as_str())
            .is_some_and(|domain| !domain.contains_key("c0")),
        "delete updates should still remove existing cookies"
    );
}

#[test]
fn apply_storage_entry_update_enforces_origin_key_and_value_quotas() {
    let mut session = test_session_record();
    for idx in 0..(super::MAX_STORAGE_ORIGINS_PER_SESSION + 8) {
        super::apply_storage_entry_update(
            &mut session,
            format!("https://o{idx}.example.com").as_str(),
            "field",
            "value",
            true,
        );
    }
    assert_eq!(
        session.storage_entries.len(),
        super::MAX_STORAGE_ORIGINS_PER_SESSION,
        "origin quota should cap growth"
    );

    let mut capped_origin_session = test_session_record();
    let origin = "https://quota.example.com";
    for idx in 0..(super::MAX_STORAGE_ENTRIES_PER_ORIGIN + 8) {
        super::apply_storage_entry_update(
            &mut capped_origin_session,
            origin,
            format!("f{idx}").as_str(),
            "v",
            true,
        );
    }
    let storage =
        capped_origin_session.storage_entries.get(origin).expect("quota test origin should exist");
    assert_eq!(
        storage.len(),
        super::MAX_STORAGE_ENTRIES_PER_ORIGIN,
        "per-origin storage quota should cap growth"
    );

    super::apply_storage_entry_update(&mut capped_origin_session, origin, "f0", "updated", true);
    assert_eq!(
        capped_origin_session
            .storage_entries
            .get(origin)
            .and_then(|entries| entries.get("f0"))
            .map(String::as_str),
        Some("updated"),
        "existing storage keys should remain mutable at quota"
    );

    let mut append_session = test_session_record();
    let append_origin = "https://append.example.com";
    super::apply_storage_entry_update(&mut append_session, append_origin, "appended", "a", true);
    for _ in 0..(super::MAX_STORAGE_ENTRY_VALUE_BYTES + 64) {
        super::apply_storage_entry_update(
            &mut append_session,
            append_origin,
            "appended",
            "a",
            false,
        );
    }
    assert_eq!(
        append_session
            .storage_entries
            .get(append_origin)
            .and_then(|entries| entries.get("appended"))
            .map(String::len),
        Some(super::MAX_STORAGE_ENTRY_VALUE_BYTES),
        "storage entry values should be truncated across repeated appends"
    );
}

#[test]
fn replace_storage_entries_for_origin_replaces_and_removes_deleted_keys() {
    let mut session = test_session_record();
    let origin = "https://app.example.com";
    super::apply_storage_entry_update(&mut session, origin, "cart", "1", true);
    super::apply_storage_entry_update(&mut session, origin, "stale", "remove", true);

    super::replace_storage_entries_for_origin(
        &mut session,
        origin,
        HashMap::from([("cart".to_owned(), "2".to_owned())]),
    );

    let entries =
        session.storage_entries.get(origin).expect("origin should remain after replacement");
    assert_eq!(entries.get("cart").map(String::as_str), Some("2"));
    assert!(
        !entries.contains_key("stale"),
        "replacement should remove keys no longer present in browser localStorage"
    );

    super::replace_storage_entries_for_origin(&mut session, origin, HashMap::new());

    assert!(
        !session.storage_entries.contains_key(origin),
        "empty browser localStorage should clear the persisted origin snapshot"
    );
}

#[test]
fn replace_network_log_entries_for_navigation_removes_stale_entries() {
    let mut tab = BrowserTabRecord::new("tab-1".to_owned());
    tab.network_log.push_back(NetworkLogEntryInternal {
        request_url: "https://app.example.com/api/alerts?include=all".to_owned(),
        status_code: 200,
        timing_bucket: "gt_2s".to_owned(),
        latency_ms: 21_000,
        captured_at_unix_ms: 1_000,
        headers: Vec::new(),
    });

    super::replace_network_log_entries_for_navigation(
        &mut tab,
        &[NetworkLogEntryInternal {
            request_url: "https://app.example.com/api/alerts?include=summary".to_owned(),
            status_code: 200,
            timing_bucket: "lt_2s".to_owned(),
            latency_ms: 1_450,
            captured_at_unix_ms: 2_000,
            headers: vec![NetworkLogHeaderInternal {
                name: "content-type".to_owned(),
                value: "application/json".to_owned(),
            }],
        }],
        16,
        16 * 1024,
    );

    assert_eq!(tab.network_log.len(), 1);
    let entry = tab.network_log.front().expect("replacement entry should remain");
    assert_eq!(
        entry.request_url, "https://app.example.com/api/alerts?include=summary",
        "navigation network log should be scoped to the latest navigation boundary"
    );
}

#[test]
fn apply_snapshot_clamps_cookie_and_storage_state() {
    let mut session = test_session_record();
    let mut cookie_jar = HashMap::new();
    for domain_idx in 0..(super::MAX_COOKIE_DOMAINS_PER_SESSION + 4) {
        let mut cookies = HashMap::new();
        for cookie_idx in 0..(super::MAX_COOKIES_PER_DOMAIN + 4) {
            cookies.insert(format!("c{cookie_idx}"), "v".repeat(16));
        }
        cookie_jar.insert(format!("d{domain_idx}.example.com"), cookies);
    }
    let mut storage_entries = HashMap::new();
    for origin_idx in 0..(super::MAX_STORAGE_ORIGINS_PER_SESSION + 4) {
        let mut entries = HashMap::new();
        for entry_idx in 0..(super::MAX_STORAGE_ENTRIES_PER_ORIGIN + 4) {
            entries.insert(
                format!("k{entry_idx}"),
                "x".repeat(super::MAX_STORAGE_ENTRY_VALUE_BYTES + 32),
            );
        }
        storage_entries.insert(format!("https://o{origin_idx}.example.com"), entries);
    }
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: "user:ops".to_owned(),
        channel: None,
        tabs: vec![BrowserTabRecord::new(ulid::Ulid::new().to_string())],
        tab_order: Vec::new(),
        active_tab_id: String::new(),
        permissions: SessionPermissionsInternal::default(),
        cookie_jar,
        storage_entries,
        state_revision: 1,
        saved_at_unix_ms: 1_737_000_000_000,
    };

    session.apply_snapshot(snapshot);

    assert_eq!(
        session.cookie_jar.len(),
        super::MAX_COOKIE_DOMAINS_PER_SESSION,
        "restored cookie domains should be clamped"
    );
    assert!(
        session.cookie_jar.values().all(|cookies| cookies.len() <= super::MAX_COOKIES_PER_DOMAIN),
        "restored cookies per domain should be clamped"
    );
    assert_eq!(
        session.storage_entries.len(),
        super::MAX_STORAGE_ORIGINS_PER_SESSION,
        "restored storage origins should be clamped"
    );
    assert!(
        session
            .storage_entries
            .values()
            .all(|entries| entries.len() <= super::MAX_STORAGE_ENTRIES_PER_ORIGIN),
        "restored storage keys per origin should be clamped"
    );
    assert!(
        session.storage_entries.values().all(|entries| {
            entries.values().all(|value| value.len() <= super::MAX_STORAGE_ENTRY_VALUE_BYTES)
        }),
        "restored storage values should be truncated"
    );
}

#[test]
fn apply_snapshot_drops_network_log_and_preserves_missing_tab_append() {
    let mut session = test_session_record();
    session.budget.max_network_log_entries = 4;
    let first_tab_id = ulid::Ulid::new().to_string();
    let second_tab_id = ulid::Ulid::new().to_string();

    let mut first_tab = BrowserTabRecord::new(first_tab_id.clone());
    let retained_entry = NetworkLogEntryInternal {
        request_url: "https://example.com/api/retained".to_owned(),
        status_code: 200,
        timing_bucket: "lt_100ms".to_owned(),
        latency_ms: 10,
        captured_at_unix_ms: 2,
        headers: vec![NetworkLogHeaderInternal {
            name: "x-request-id".to_owned(),
            value: "req-retained".to_owned(),
        }],
    };
    let newest_entry = NetworkLogEntryInternal {
        request_url: "https://example.com/api/newest".to_owned(),
        status_code: 200,
        timing_bucket: "lt_100ms".to_owned(),
        latency_ms: 12,
        captured_at_unix_ms: 3,
        headers: vec![NetworkLogHeaderInternal {
            name: "x-request-id".to_owned(),
            value: "req-newest".to_owned(),
        }],
    };
    let trimmed_entry = NetworkLogEntryInternal {
        request_url: "https://example.com/api/trimmed".to_owned(),
        status_code: 200,
        timing_bucket: "lt_100ms".to_owned(),
        latency_ms: 8,
        captured_at_unix_ms: 1,
        headers: vec![NetworkLogHeaderInternal {
            name: "x-request-id".to_owned(),
            value: "req-trimmed".to_owned(),
        }],
    };
    first_tab.network_log.extend([
        trimmed_entry.clone(),
        retained_entry.clone(),
        newest_entry.clone(),
    ]);
    session.budget.max_network_log_bytes =
        (super::estimate_network_log_entry_internal_bytes(&retained_entry)
            + super::estimate_network_log_entry_internal_bytes(&newest_entry)) as u64;

    let second_tab = BrowserTabRecord::new(second_tab_id.clone());
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: "user:ops".to_owned(),
        channel: None,
        tabs: vec![first_tab, second_tab],
        tab_order: vec![second_tab_id.clone(), second_tab_id.clone()],
        active_tab_id: second_tab_id.clone(),
        permissions: SessionPermissionsInternal::default(),
        cookie_jar: HashMap::new(),
        storage_entries: HashMap::new(),
        state_revision: 1,
        saved_at_unix_ms: 1_737_000_000_000,
    };

    session.apply_snapshot(snapshot);

    let restored_first_tab =
        session.tabs.get(first_tab_id.as_str()).expect("first tab should be restored");
    assert_eq!(
        restored_first_tab.network_log.len(),
        0,
        "restored browser sessions should not replay historical network logs"
    );
    assert_eq!(
        session.tab_order,
        vec![second_tab_id.clone(), second_tab_id, first_tab_id],
        "restore should keep persisted tab order entries and append only missing tabs"
    );
}

#[test]
fn persisted_snapshot_hash_is_stable_for_equivalent_hashmap_content() {
    let mut first_tab = BrowserTabRecord::new("tab-1".to_owned());
    first_tab.typed_inputs.insert("search".to_owned(), "palyra".to_owned());
    first_tab.typed_inputs.insert("theme".to_owned(), "dark".to_owned());

    let mut second_tab = BrowserTabRecord::new("tab-1".to_owned());
    second_tab.typed_inputs.insert("theme".to_owned(), "dark".to_owned());
    second_tab.typed_inputs.insert("search".to_owned(), "palyra".to_owned());

    let mut first_cookie_inner = HashMap::new();
    first_cookie_inner.insert("theme".to_owned(), "dark".to_owned());
    first_cookie_inner.insert("session".to_owned(), "abc".to_owned());
    let mut first_cookie_jar = HashMap::new();
    first_cookie_jar.insert("https://example.com".to_owned(), first_cookie_inner);

    let mut second_cookie_inner = HashMap::new();
    second_cookie_inner.insert("session".to_owned(), "abc".to_owned());
    second_cookie_inner.insert("theme".to_owned(), "dark".to_owned());
    let mut second_cookie_jar = HashMap::new();
    second_cookie_jar.insert("https://example.com".to_owned(), second_cookie_inner);

    let mut first_storage_inner = HashMap::new();
    first_storage_inner.insert("locale".to_owned(), "en".to_owned());
    first_storage_inner.insert("layout".to_owned(), "compact".to_owned());
    let mut first_storage_entries = HashMap::new();
    first_storage_entries.insert("https://example.com".to_owned(), first_storage_inner);

    let mut second_storage_inner = HashMap::new();
    second_storage_inner.insert("layout".to_owned(), "compact".to_owned());
    second_storage_inner.insert("locale".to_owned(), "en".to_owned());
    let mut second_storage_entries = HashMap::new();
    second_storage_entries.insert("https://example.com".to_owned(), second_storage_inner);

    let snapshot_one = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: "user:ops".to_owned(),
        channel: None,
        tabs: vec![first_tab],
        tab_order: vec!["tab-1".to_owned()],
        active_tab_id: "tab-1".to_owned(),
        permissions: SessionPermissionsInternal::default(),
        cookie_jar: first_cookie_jar,
        storage_entries: first_storage_entries,
        state_revision: 5,
        saved_at_unix_ms: 1_737_000_000_000,
    };
    let snapshot_two = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: "user:ops".to_owned(),
        channel: None,
        tabs: vec![second_tab],
        tab_order: vec!["tab-1".to_owned()],
        active_tab_id: "tab-1".to_owned(),
        permissions: SessionPermissionsInternal::default(),
        cookie_jar: second_cookie_jar,
        storage_entries: second_storage_entries,
        state_revision: 5,
        saved_at_unix_ms: 1_737_000_000_000,
    };

    let hash_one =
        persisted_snapshot_hash(&snapshot_one).expect("first hash generation should succeed");
    let hash_two =
        persisted_snapshot_hash(&snapshot_two).expect("second hash generation should succeed");

    assert_eq!(
        hash_one, hash_two,
        "hash should remain stable when only HashMap insertion order changes"
    );
}

#[test]
fn validate_restored_snapshot_against_profile_accepts_raw_persisted_hash() {
    let state_dir = tempfile::tempdir().expect("state temp dir should be available");
    let store = PersistedStateStore::new(state_dir.path().join("state"), [3_u8; STATE_KEY_LEN])
        .expect("state store should initialize");
    let profile_id = ulid::Ulid::new().to_string();
    let raw_json = format!(
            concat!(
                "{{",
                "\"v\":{},",
                "\"principal\":\"user:ops\",",
                "\"channel\":null,",
                "\"tabs\":[{{",
                "\"tab_id\":\"tab-1\",",
                "\"last_title\":\"\",",
                "\"last_url\":null,",
                "\"last_page_body\":\"\",",
                "\"scroll_x\":0,",
                "\"scroll_y\":0,",
                "\"typed_inputs\":{{\"theme\":\"dark\",\"search\":\"palyra\"}},",
                "\"network_log\":[]",
                "}}],",
                "\"tab_order\":[\"tab-1\"],",
                "\"active_tab_id\":\"tab-1\",",
                "\"permissions\":{{\"camera\":\"Deny\",\"microphone\":\"Deny\",\"location\":\"Deny\"}},",
                "\"cookie_jar\":{{\"https://example.com\":{{\"theme\":\"dark\",\"session\":\"abc\"}}}},",
                "\"storage_entries\":{{\"https://example.com\":{{\"layout\":\"compact\",\"locale\":\"en\"}}}},",
                "\"state_revision\":1,",
                "\"saved_at_unix_ms\":1737000000000",
                "}}"
            ),
            CANONICAL_PROTOCOL_MAJOR
        );
    let encrypted = encrypt_state_blob(
        &derive_state_encryption_key(&store.key, Some(profile_id.as_str())),
        raw_json.as_bytes(),
    )
    .expect("snapshot should encrypt");
    std::fs::write(store.snapshot_path(profile_id.as_str()), encrypted)
        .expect("snapshot should persist");

    let loaded = store
        .load_snapshot(profile_id.as_str(), Some(profile_id.as_str()))
        .expect("snapshot load should succeed")
        .expect("snapshot should be present");
    let expected_raw_hash = sha256_hex(raw_json.as_bytes());
    assert_eq!(
        loaded.raw_hash_sha256, expected_raw_hash,
        "load_snapshot should preserve the stored raw payload hash"
    );
    assert_ne!(
        persisted_snapshot_hash(&loaded.snapshot).expect("canonical hash should compute"),
        expected_raw_hash,
        "test fixture should differ from canonical ordering so raw hash compatibility is exercised"
    );
    let profile = BrowserProfileRecord {
        profile_id,
        principal: "user:ops".to_owned(),
        name: "Ops".to_owned(),
        theme_color: None,
        created_at_unix_ms: 1_737_000_000_000,
        updated_at_unix_ms: 1_737_000_000_000,
        last_used_unix_ms: 1_737_000_000_000,
        persistence_enabled: true,
        private_profile: false,
        state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
        state_revision: 1,
        state_hash_sha256: Some(expected_raw_hash),
        record_hash_sha256: String::new(),
    };

    validate_restored_snapshot_against_profile(
        &loaded.snapshot,
        Some(loaded.raw_hash_sha256.as_str()),
        &profile,
    )
    .expect("raw persisted hash should keep older snapshots restorable");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_profile_restore_rejects_snapshot_revision_rollback() {
    let state_dir = tempfile::tempdir().expect("state temp dir should be available");
    let mut runtime_state = browser_runtime_state_for_tests(&Args {
        bind: "127.0.0.1".to_owned(),
        port: 7143,
        grpc_bind: "127.0.0.1".to_owned(),
        grpc_port: 7543,
        auth_token: None,
        session_idle_ttl_ms: 60_000,
        max_sessions: 16,
        max_navigation_timeout_ms: 10_000,
        max_session_lifetime_ms: 60_000,
        max_screenshot_bytes: 128 * 1024,
        max_response_bytes: 128 * 1024,
        max_title_bytes: 4 * 1024,
        engine_mode: BrowserEngineMode::Simulated,
        chromium_path: None,
        chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
    })
    .expect("runtime should initialize");
    runtime_state.state_store = Some(
        PersistedStateStore::new(state_dir.path().join("state"), [9_u8; STATE_KEY_LEN])
            .expect("state store should initialize"),
    );
    let runtime = std::sync::Arc::new(runtime_state);
    let service = BrowserServiceImpl { runtime: runtime.clone() };

    let profile = service
        .create_profile(Request::new(browser_v1::CreateProfileRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            name: "Ops".to_owned(),
            theme_color: "#1f2937".to_owned(),
            persistence_enabled: true,
            private_profile: false,
        }))
        .await
        .expect("create_profile should succeed")
        .into_inner()
        .profile
        .expect("profile should be present");
    let profile_id = profile.profile_id.expect("profile id should be present").ulid;

    let session = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(proto::palyra::common::v1::CanonicalId { ulid: profile_id.clone() }),
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = session.session_id.expect("session id should be present").ulid;

    service
        .close_session(Request::new(browser_v1::CloseSessionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        }))
        .await
        .expect("close_session should execute");

    let store = runtime
        .state_store
        .as_ref()
        .expect("state store should remain configured for rollback test");
    let loaded_snapshot = store
        .load_snapshot(profile_id.as_str(), Some(profile_id.as_str()))
        .expect("snapshot load should succeed")
        .expect("snapshot should be present after persisted profile session");
    let snapshot = loaded_snapshot.snapshot;
    assert!(snapshot.state_revision >= 1, "snapshot revision should advance after first persist");
    let expected_hash = persisted_snapshot_hash(&snapshot).expect("snapshot hash should compute");
    let mut rollback_snapshot = snapshot.clone();
    rollback_snapshot.state_revision = snapshot.state_revision.saturating_sub(1);
    store
        .save_snapshot(profile_id.as_str(), Some(profile_id.as_str()), &rollback_snapshot)
        .expect("rollback snapshot write should succeed");
    {
        let _profile_registry_guard = runtime.profile_registry_lock.lock().await;
        update_profile_state_metadata_locked(
            store,
            profile_id.as_str(),
            PROFILE_RECORD_SCHEMA_VERSION,
            snapshot.state_revision,
            expected_hash.as_str(),
        )
        .expect("profile metadata update should succeed");
    }

    let rollback_attempt = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: Some(proto::palyra::common::v1::CanonicalId { ulid: profile_id }),
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect_err("rollbacked snapshot should be rejected");
    assert_eq!(
        rollback_attempt.code(),
        tonic::Code::FailedPrecondition,
        "rollback guard should fail with failed_precondition"
    );
    assert!(
        rollback_attempt.message().contains("snapshot revision"),
        "error should explain revision rollback guard: {}",
        rollback_attempt.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_rejects_unsupported_action_kind() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let relay = service
        .relay_action(Request::new(browser_v1::RelayActionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            extension_id: "com.palyra.extension".to_owned(),
            action: 999,
            payload: None,
            max_payload_bytes: 4_096,
        }))
        .await
        .expect("relay action should return response")
        .into_inner();
    assert!(!relay.success, "unsupported relay action should fail closed");
    assert!(
        relay.error.contains("unsupported relay action"),
        "error should explain unsupported action: {}",
        relay.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_rejects_oversized_payload_budget() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let status = service
        .relay_action(Request::new(browser_v1::RelayActionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            extension_id: "com.palyra.extension".to_owned(),
            action: browser_v1::RelayActionKind::CaptureSelection as i32,
            payload: Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                browser_v1::RelayCaptureSelectionPayload {
                    selector: "body".to_owned(),
                    max_selection_bytes: 512,
                },
            )),
            max_payload_bytes: MAX_RELAY_PAYLOAD_BYTES + 1,
        }))
        .await
        .expect_err("oversized relay payload budget must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("max_payload_bytes exceeds"),
        "error should explain relay payload bound: {}",
        status.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_capture_selection_reports_exact_limit_without_truncation() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Selection</title></head><body>selection body</body></html>",
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("navigate should succeed")
        .into_inner();
    assert!(navigate.success, "navigate should succeed before relay capture_selection");

    let exact_limit = "<body>".len() as u64;
    let exact_response = service
        .relay_action(Request::new(browser_v1::RelayActionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            extension_id: "com.palyra.extension".to_owned(),
            action: browser_v1::RelayActionKind::CaptureSelection as i32,
            payload: Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                browser_v1::RelayCaptureSelectionPayload {
                    selector: "body".to_owned(),
                    max_selection_bytes: exact_limit,
                },
            )),
            max_payload_bytes: 4_096,
        }))
        .await
        .expect("relay capture_selection should return response")
        .into_inner();
    let Some(browser_v1::relay_action_response::Result::Selection(exact_selection)) =
        exact_response.result
    else {
        panic!("capture_selection should return selection payload");
    };
    assert_eq!(exact_selection.selected_text, "<body>");
    assert!(
        !exact_selection.truncated,
        "selection at the exact byte cap must not be marked truncated"
    );

    let truncated_response = service
        .relay_action(Request::new(browser_v1::RelayActionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            extension_id: "com.palyra.extension".to_owned(),
            action: browser_v1::RelayActionKind::CaptureSelection as i32,
            payload: Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                browser_v1::RelayCaptureSelectionPayload {
                    selector: "body".to_owned(),
                    max_selection_bytes: exact_limit.saturating_sub(1),
                },
            )),
            max_payload_bytes: 4_096,
        }))
        .await
        .expect("relay capture_selection should return response")
        .into_inner();
    let Some(browser_v1::relay_action_response::Result::Selection(truncated_selection)) =
        truncated_response.result
    else {
        panic!("capture_selection should return selection payload");
    };
    assert!(
        truncated_selection.truncated,
        "selection below the exact byte cap must report truncation"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_rejects_unsupported_action_kind_with_auth_token() {
    const AUTH_TOKEN: &str = "test-token";
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: Some(AUTH_TOKEN.to_owned()),
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let mut create_request = Request::new(browser_v1::CreateSessionRequest {
        v: 1,
        principal: "user:ops".to_owned(),
        idle_ttl_ms: 10_000,
        budget: None,
        allow_private_targets: true,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        persistence_enabled: false,
        persistence_id: String::new(),
        profile_id: None,
        private_profile: false,
        channel: String::new(),
    });
    insert_bearer_auth(&mut create_request, AUTH_TOKEN);
    let created = service
        .create_session(create_request)
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let mut relay_request = Request::new(browser_v1::RelayActionRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        extension_id: "com.palyra.extension".to_owned(),
        action: 999,
        payload: None,
        max_payload_bytes: 4_096,
    });
    insert_bearer_auth(&mut relay_request, AUTH_TOKEN);
    let relay = service
        .relay_action(relay_request)
        .await
        .expect("relay action should return response")
        .into_inner();
    assert!(!relay.success, "unsupported relay action should fail closed");
    assert!(
        relay.error.contains("unsupported relay action"),
        "error should explain unsupported action: {}",
        relay.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_rejects_oversized_payload_budget_with_auth_token() {
    const AUTH_TOKEN: &str = "test-token";
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: Some(AUTH_TOKEN.to_owned()),
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let mut create_request = Request::new(browser_v1::CreateSessionRequest {
        v: 1,
        principal: "user:ops".to_owned(),
        idle_ttl_ms: 10_000,
        budget: None,
        allow_private_targets: true,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        persistence_enabled: false,
        persistence_id: String::new(),
        profile_id: None,
        private_profile: false,
        channel: String::new(),
    });
    insert_bearer_auth(&mut create_request, AUTH_TOKEN);
    let created = service
        .create_session(create_request)
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let mut relay_request = Request::new(browser_v1::RelayActionRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        extension_id: "com.palyra.extension".to_owned(),
        action: browser_v1::RelayActionKind::CaptureSelection as i32,
        payload: Some(browser_v1::relay_action_request::Payload::CaptureSelection(
            browser_v1::RelayCaptureSelectionPayload {
                selector: "body".to_owned(),
                max_selection_bytes: 512,
            },
        )),
        max_payload_bytes: MAX_RELAY_PAYLOAD_BYTES + 1,
    });
    insert_bearer_auth(&mut relay_request, AUTH_TOKEN);
    let status = service
        .relay_action(relay_request)
        .await
        .expect_err("oversized relay payload budget must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("max_payload_bytes exceeds"),
        "error should explain relay payload bound: {}",
        status.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_requires_valid_bearer_token_when_auth_enabled() {
    const AUTH_TOKEN: &str = "test-token";
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: Some(AUTH_TOKEN.to_owned()),
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let mut create_request = Request::new(browser_v1::CreateSessionRequest {
        v: 1,
        principal: "user:ops".to_owned(),
        idle_ttl_ms: 10_000,
        budget: None,
        allow_private_targets: true,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        persistence_enabled: false,
        persistence_id: String::new(),
        profile_id: None,
        private_profile: false,
        channel: String::new(),
    });
    insert_bearer_auth(&mut create_request, AUTH_TOKEN);
    let created = service
        .create_session(create_request)
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let missing_token_status = service
        .relay_action(Request::new(browser_v1::RelayActionRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            extension_id: "com.palyra.extension".to_owned(),
            action: browser_v1::RelayActionKind::OpenTab as i32,
            payload: Some(browser_v1::relay_action_request::Payload::OpenTab(
                browser_v1::RelayOpenTabPayload {
                    url: "https://example.com".to_owned(),
                    activate: true,
                    timeout_ms: 1_000,
                },
            )),
            max_payload_bytes: 4_096,
        }))
        .await
        .expect_err("relay_action without bearer token must be rejected");
    assert_eq!(missing_token_status.code(), tonic::Code::Unauthenticated);

    let mut wrong_token_request = Request::new(browser_v1::RelayActionRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        extension_id: "com.palyra.extension".to_owned(),
        action: browser_v1::RelayActionKind::OpenTab as i32,
        payload: Some(browser_v1::relay_action_request::Payload::OpenTab(
            browser_v1::RelayOpenTabPayload {
                url: "https://example.com".to_owned(),
                activate: true,
                timeout_ms: 1_000,
            },
        )),
        max_payload_bytes: 4_096,
    });
    insert_bearer_auth(&mut wrong_token_request, "wrong-token");
    let wrong_token_status = service
        .relay_action(wrong_token_request)
        .await
        .expect_err("relay_action with wrong bearer token must be rejected");
    assert_eq!(wrong_token_status.code(), tonic::Code::Unauthenticated);
}

#[test]
fn resolve_download_target_preserves_original_case_for_href_and_filename() {
    let tag =
        r#"<A HREF="https://example.com/Artifacts/Report.PDF?Sig=AbC123" DOWNLOAD="Report.PDF">"#;
    let (resolved_url, file_name) =
        super::resolve_download_target(tag, None).expect("download target should parse");
    assert_eq!(resolved_url, "https://example.com/Artifacts/Report.PDF?Sig=AbC123");
    assert_eq!(file_name, "Report.PDF");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_open_tab_blocks_private_targets_even_with_auth_token() {
    const AUTH_TOKEN: &str = "test-token";
    let url = "http://127.0.0.1:8080/".to_owned();
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: Some(AUTH_TOKEN.to_owned()),
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let mut create_request = Request::new(browser_v1::CreateSessionRequest {
        v: 1,
        principal: "user:ops".to_owned(),
        idle_ttl_ms: 10_000,
        budget: None,
        allow_private_targets: true,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        persistence_enabled: false,
        persistence_id: String::new(),
        profile_id: None,
        private_profile: false,
        channel: String::new(),
    });
    insert_bearer_auth(&mut create_request, AUTH_TOKEN);
    let created = service
        .create_session(create_request)
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let mut relay_request = Request::new(browser_v1::RelayActionRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        extension_id: "com.palyra.extension".to_owned(),
        action: browser_v1::RelayActionKind::OpenTab as i32,
        payload: Some(browser_v1::relay_action_request::Payload::OpenTab(
            browser_v1::RelayOpenTabPayload { url, activate: true, timeout_ms: 1_500 },
        )),
        max_payload_bytes: 4_096,
    });
    insert_bearer_auth(&mut relay_request, AUTH_TOKEN);
    let relay = service
        .relay_action(relay_request)
        .await
        .expect("relay open_tab should return response")
        .into_inner();
    assert!(
        !relay.success,
        "relay open_tab should fail closed for private targets even when the session allows them"
    );
    assert!(
        relay.error.contains("private/local"),
        "relay open_tab should explain private-target denial: {}",
        relay.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_relay_send_snapshot_succeeds_with_auth_token() {
    const AUTH_TOKEN: &str = "test-token";
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Relay Snapshot</title></head><body>relay snapshot text</body></html>",
    );
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: Some(AUTH_TOKEN.to_owned()),
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };
    let mut create_request = Request::new(browser_v1::CreateSessionRequest {
        v: 1,
        principal: "user:ops".to_owned(),
        idle_ttl_ms: 10_000,
        budget: None,
        allow_private_targets: true,
        allow_downloads: false,
        action_allowed_domains: Vec::new(),
        persistence_enabled: false,
        persistence_id: String::new(),
        profile_id: None,
        private_profile: false,
        channel: String::new(),
    });
    insert_bearer_auth(&mut create_request, AUTH_TOKEN);
    let created = service
        .create_session(create_request)
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let mut navigate_request = Request::new(browser_v1::NavigateRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
        url,
        timeout_ms: 2_000,
        allow_redirects: true,
        max_redirects: 3,
        allow_private_targets: true,
    });
    insert_bearer_auth(&mut navigate_request, AUTH_TOKEN);
    let navigate =
        service.navigate(navigate_request).await.expect("navigate should execute").into_inner();
    assert!(navigate.success, "navigate should succeed before snapshot relay");

    let mut relay_request = Request::new(browser_v1::RelayActionRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        extension_id: "com.palyra.extension".to_owned(),
        action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
        payload: Some(browser_v1::relay_action_request::Payload::PageSnapshot(
            browser_v1::RelayPageSnapshotPayload {
                include_dom_snapshot: true,
                include_visible_text: true,
                max_dom_snapshot_bytes: 16 * 1024,
                max_visible_text_bytes: 4 * 1024,
            },
        )),
        max_payload_bytes: 4_096,
    });
    insert_bearer_auth(&mut relay_request, AUTH_TOKEN);
    let relay = service
        .relay_action(relay_request)
        .await
        .expect("relay send_page_snapshot should return response")
        .into_inner();
    assert!(relay.success, "relay send_page_snapshot should succeed with auth enabled");
    let snapshot = match relay.result {
        Some(browser_v1::relay_action_response::Result::Snapshot(snapshot)) => snapshot,
        _ => panic!("relay snapshot action should return snapshot payload"),
    };
    assert!(
        snapshot.visible_text.contains("relay snapshot text"),
        "snapshot visible text should contain served page content"
    );

    handle.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_download_allowlist_and_quarantine_artifacts() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };

    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let (allowlist_url, allowlist_handle) =
        spawn_download_fixture_http_server("/report.csv", "text/csv", b"name,score\nalice,9\n");
    let navigate_allowlist = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: allowlist_url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("allowlist navigate should execute")
        .into_inner();
    assert!(navigate_allowlist.success, "allowlist fixture navigation should succeed");

    let allowlisted_click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            selector: "#download-link".to_owned(),
            max_retries: 0,
            timeout_ms: 1_500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 2 * 1024,
        }))
        .await
        .expect("allowlist click should execute")
        .into_inner();
    assert!(allowlisted_click.success, "allowlisted download click should succeed");
    let allowlisted_artifact =
        allowlisted_click.artifact.expect("allowlisted download should return artifact metadata");
    assert!(!allowlisted_artifact.quarantined, "allowlisted artifact should not be quarantined");
    assert_eq!(allowlisted_artifact.file_name, "report.csv");
    allowlist_handle.join().expect("allowlist server thread should exit");

    let (quarantine_url, quarantine_handle) = spawn_download_fixture_http_server(
        "/payload.exe",
        "application/octet-stream",
        b"MZ\x90\x00suspicious",
    );
    let navigate_quarantine = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: quarantine_url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("quarantine navigate should execute")
        .into_inner();
    assert!(navigate_quarantine.success, "quarantine fixture navigation should succeed");

    let quarantined_click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            selector: "#download-link".to_owned(),
            max_retries: 0,
            timeout_ms: 1_500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 2 * 1024,
        }))
        .await
        .expect("quarantine click should execute")
        .into_inner();
    assert!(quarantined_click.success, "quarantined download still records click success");
    assert_eq!(
        quarantined_click.action_log.as_ref().map(|entry| entry.outcome.as_str()),
        Some("download_quarantined")
    );
    let quarantined_artifact =
        quarantined_click.artifact.expect("quarantined download should return artifact metadata");
    assert!(quarantined_artifact.quarantined, "suspicious file should be quarantined");
    assert!(
        quarantined_artifact.quarantine_reason.contains("extension_not_allowlisted"),
        "quarantine reason should include extension allowlist signal: {}",
        quarantined_artifact.quarantine_reason
    );
    quarantine_handle.join().expect("quarantine server thread should exit");

    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut list_request, "user:ops");
    let listed = service
        .list_download_artifacts(list_request)
        .await
        .expect("list_download_artifacts should execute")
        .into_inner();
    assert_eq!(listed.artifacts.len(), 2, "both artifacts should be registered");
    assert!(
        listed.artifacts.iter().any(|artifact| artifact.quarantined),
        "download artifact list should include quarantined entries"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_rejects_downloads_that_exceed_max_file_bytes() {
    let runtime = std::sync::Arc::new(
        browser_runtime_state_for_tests(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 256 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize"),
    );
    let service = BrowserServiceImpl { runtime };

    let created = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:ops".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let session_id = created
        .session_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .expect("session id should be present");

    let (oversized_url, oversized_handle) = spawn_streaming_download_fixture_http_server(
        "/oversized.csv",
        "text/csv",
        (DOWNLOAD_MAX_FILE_BYTES as usize).saturating_add(1),
    );
    let navigate = service
        .navigate(Request::new(browser_v1::NavigateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            url: oversized_url,
            timeout_ms: 2_000,
            allow_redirects: true,
            max_redirects: 3,
            allow_private_targets: true,
        }))
        .await
        .expect("oversized navigate should execute")
        .into_inner();
    assert!(navigate.success, "oversized fixture navigation should succeed");

    let click = service
        .click(Request::new(browser_v1::ClickRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            selector: "#download-link".to_owned(),
            max_retries: 0,
            timeout_ms: 1_500,
            capture_failure_screenshot: true,
            max_failure_screenshot_bytes: 2 * 1024,
        }))
        .await
        .expect("oversized click should execute")
        .into_inner();
    assert!(!click.success, "oversized download should fail closed");
    assert_eq!(
        click.action_log.as_ref().map(|entry| entry.outcome.as_str()),
        Some("download_failed")
    );
    assert!(
        click.error.contains("download exceeds max file bytes"),
        "oversized download failure should explain the size guard: {}",
        click.error
    );
    assert!(click.artifact.is_none(), "oversized download must not register an artifact");
    oversized_handle.join().expect("oversized server thread should exit");

    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut list_request, "user:ops");
    let listed = service
        .list_download_artifacts(list_request)
        .await
        .expect("list_download_artifacts should execute")
        .into_inner();
    assert!(listed.artifacts.is_empty(), "failed oversized download must not leave artifacts");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_lists_empty_downloads_for_existing_session_without_artifacts() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    let mut list_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(session_id),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut list_request, "user:ops");
    let listed = service
        .list_download_artifacts(list_request)
        .await
        .expect("existing session without downloads should list successfully")
        .into_inner();

    assert!(listed.artifacts.is_empty());
    assert!(!listed.truncated);
    assert!(listed.error.is_empty(), "empty download list must not report an error");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_download_listing_hides_cross_principal_session_existence() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let no_download_session = create_test_session(&service, "user:owner").await;
    let no_download_session_id =
        no_download_session.session_id.expect("session id should be present");
    let download_session = service
        .create_session(Request::new(browser_v1::CreateSessionRequest {
            v: 1,
            principal: "user:owner".to_owned(),
            idle_ttl_ms: 10_000,
            budget: None,
            allow_private_targets: true,
            allow_downloads: true,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            profile_id: None,
            private_profile: false,
            channel: String::new(),
        }))
        .await
        .expect("create_session should succeed")
        .into_inner();
    let download_session_id = download_session.session_id.expect("session id should be present");
    let missing_session_id =
        proto::palyra::common::v1::CanonicalId { ulid: ulid::Ulid::new().to_string() };

    let mut missing_request = Request::new(browser_v1::ListDownloadArtifactsRequest {
        v: 1,
        session_id: Some(missing_session_id),
        limit: 10,
        quarantined_only: false,
    });
    insert_principal(&mut missing_request, "user:other");
    let Err(missing_status) = service.list_download_artifacts(missing_request).await else {
        panic!("missing session should not list downloads");
    };

    for session_id in [no_download_session_id, download_session_id] {
        let mut request = Request::new(browser_v1::ListDownloadArtifactsRequest {
            v: 1,
            session_id: Some(session_id),
            limit: 10,
            quarantined_only: false,
        });
        insert_principal(&mut request, "user:other");
        let Err(status) = service.list_download_artifacts(request).await else {
            panic!("cross-principal session should not list downloads");
        };
        assert_eq!(status.code(), missing_status.code());
        assert_eq!(status.message(), missing_status.message());
        assert_eq!(status.code(), tonic::Code::NotFound);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_lists_and_gets_session_details() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };

    let first = create_test_session(&service, "user:alpha").await;
    let first_id = first.session_id.expect("first session id should be present");
    let second = create_test_session(&service, "user:alpha").await;
    let second_id = second.session_id.expect("second session id should be present");

    {
        let now = Instant::now();
        let mut sessions = runtime.sessions.lock().await;
        let first_session = sessions
            .get_mut(first_id.ulid.as_str())
            .expect("first session should exist for inspection");
        first_session.last_active = now - Duration::from_secs(5);
        first_session.channel = Some("alpha-channel".to_owned());
        first_session.action_allowed_domains = vec!["example.com".to_owned()];
        {
            let first_tab =
                first_session.active_tab_mut().expect("first session should have an active tab");
            first_tab.last_url = Some("https://example.com/alpha".to_owned());
            first_tab.last_title = "Alpha Session".to_owned();
        }

        let second_session = sessions
            .get_mut(second_id.ulid.as_str())
            .expect("second session should exist for ordering");
        second_session.last_active = now;
    }

    let mut list_request =
        Request::new(browser_v1::ListSessionsRequest { v: 1, principal: String::new(), limit: 1 });
    insert_principal(&mut list_request, "user:alpha");
    let listed = service
        .list_sessions(list_request)
        .await
        .expect("list_sessions should execute")
        .into_inner();
    assert!(listed.truncated, "listing with limit=1 should report truncation");
    assert_eq!(listed.sessions.len(), 1, "listing should clamp to requested limit");
    assert_eq!(
        listed.sessions[0].session_id.as_ref().map(|value| value.ulid.as_str()),
        Some(second_id.ulid.as_str()),
        "most recently active session should be listed first"
    );

    let mut filtered_request = Request::new(browser_v1::ListSessionsRequest {
        v: 1,
        principal: "user:alpha".to_owned(),
        limit: 10,
    });
    insert_principal(&mut filtered_request, "user:alpha");
    let filtered = service
        .list_sessions(filtered_request)
        .await
        .expect("filtered list_sessions should execute")
        .into_inner();
    assert_eq!(filtered.sessions.len(), 2, "matching principal should see both owned sessions");
    let summary = filtered
        .sessions
        .iter()
        .find(|session| {
            session.session_id.as_ref().map(|value| value.ulid.as_str())
                == Some(first_id.ulid.as_str())
        })
        .expect("filtered sessions should include the first owned session");
    assert_eq!(summary.principal, "user:alpha");
    assert_eq!(summary.channel, "alpha-channel");
    assert_eq!(summary.active_tab_title, "Alpha Session");
    assert_eq!(summary.action_allowed_domains, vec!["example.com".to_owned()]);

    let mut get_request =
        Request::new(browser_v1::GetSessionRequest { v: 1, session_id: Some(first_id) });
    insert_principal(&mut get_request, "user:alpha");
    let detailed =
        service.get_session(get_request).await.expect("get_session should execute").into_inner();
    assert!(detailed.success, "get_session should succeed for an active session");
    let detail = detailed.session.expect("session detail should be returned");
    let detail_summary = detail.summary.expect("session detail should include summary");
    assert_eq!(detail_summary.principal, "user:alpha");
    assert_eq!(detail_summary.channel, "alpha-channel");
    assert_eq!(detail.tabs.len(), 1, "fresh sessions should expose their single active tab");
    assert_eq!(
        detail
            .effective_budget
            .expect("effective budget should be returned")
            .max_network_log_entries,
        runtime.default_budget.max_network_log_entries as u64
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_session_diagnostics_require_matching_principal() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime };
    let created = create_test_session(&service, "user:alpha").await;
    let session_id = created.session_id.expect("session id should be present");

    let missing_principal = service
        .list_sessions(Request::new(browser_v1::ListSessionsRequest {
            v: 1,
            principal: String::new(),
            limit: 10,
        }))
        .await
        .expect_err("list_sessions without caller principal must fail");
    assert_eq!(missing_principal.code(), tonic::Code::Unauthenticated);

    let mut mismatched_list = Request::new(browser_v1::ListSessionsRequest {
        v: 1,
        principal: "user:alpha".to_owned(),
        limit: 10,
    });
    insert_principal(&mut mismatched_list, "user:beta");
    let mismatched_list_status = service
        .list_sessions(mismatched_list)
        .await
        .expect_err("list_sessions should reject principal mismatch");
    assert_eq!(mismatched_list_status.code(), tonic::Code::PermissionDenied);

    let mut mismatched_get =
        Request::new(browser_v1::GetSessionRequest { v: 1, session_id: Some(session_id.clone()) });
    insert_principal(&mut mismatched_get, "user:beta");
    let mismatched_get_status = service
        .get_session(mismatched_get)
        .await
        .expect_err("get_session should reject cross-principal access");
    assert_eq!(mismatched_get_status.code(), tonic::Code::PermissionDenied);

    let mut mismatched_network_log = Request::new(browser_v1::NetworkLogRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        limit: 10,
        include_headers: false,
        max_payload_bytes: 8 * 1024,
    });
    insert_principal(&mut mismatched_network_log, "user:beta");
    let mismatched_network_log_status = service
        .network_log(mismatched_network_log)
        .await
        .expect_err("network_log should reject cross-principal access");
    assert_eq!(mismatched_network_log_status.code(), tonic::Code::PermissionDenied);

    let mut mismatched_inspect = Request::new(browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: false,
        include_storage: false,
        include_action_log: false,
        include_network_log: false,
        include_page_snapshot: false,
        include_console_log: false,
        include_page_diagnostics: false,
        max_cookie_bytes: 0,
        max_storage_bytes: 0,
        max_action_log_entries: 0,
        max_network_log_entries: 0,
        max_network_log_bytes: 0,
        max_dom_snapshot_bytes: 0,
        max_visible_text_bytes: 0,
        max_console_log_entries: 0,
        max_console_log_bytes: 0,
    });
    insert_principal(&mut mismatched_inspect, "user:beta");
    let mismatched_inspect_status = service
        .inspect_session(mismatched_inspect)
        .await
        .expect_err("inspect_session should reject cross-principal access");
    assert_eq!(mismatched_inspect_status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_session_actions_require_matching_principal() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:alpha").await;
    let session_id = created.session_id.expect("session id should be present");

    let mut mismatched_navigate = Request::new(browser_v1::NavigateRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        url: "https://example.com".to_owned(),
        timeout_ms: 1_000,
        allow_redirects: false,
        max_redirects: 0,
        allow_private_targets: false,
    });
    insert_principal(&mut mismatched_navigate, "user:beta");
    let navigate_status = service
        .navigate(mismatched_navigate)
        .await
        .expect_err("navigate should hide cross-principal sessions");
    assert_eq!(navigate_status.code(), tonic::Code::NotFound);

    let mut mismatched_close = Request::new(browser_v1::CloseSessionRequest {
        v: 1,
        session_id: Some(session_id.clone()),
    });
    insert_principal(&mut mismatched_close, "user:beta");
    let close_status = service
        .close_session(mismatched_close)
        .await
        .expect_err("close_session should hide cross-principal sessions");
    assert_eq!(close_status.code(), tonic::Code::NotFound);
    assert!(
        runtime.sessions.lock().await.contains_key(session_id.ulid.as_str()),
        "rejected close must preserve the owner's session"
    );

    let mut owner_close =
        Request::new(browser_v1::CloseSessionRequest { v: 1, session_id: Some(session_id) });
    insert_principal(&mut owner_close, "user:alpha");
    let response =
        service.close_session(owner_close).await.expect("owner close should execute").into_inner();
    assert!(response.closed);
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_reset_state_requires_matching_principal() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:alpha").await;
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session = sessions
            .get_mut(session_id.ulid.as_str())
            .expect("session should exist for storage seeding");
        session.storage_entries.insert(
            "https://example.com".to_owned(),
            HashMap::from([("theme".to_owned(), "dark".to_owned())]),
        );
    }

    let mut mismatched_reset = Request::new(browser_v1::ResetStateRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        clear_cookies: false,
        clear_storage: true,
        reset_tabs: false,
        reset_permissions: false,
    });
    insert_principal(&mut mismatched_reset, "user:beta");
    let mismatched_status = service
        .reset_state(mismatched_reset)
        .await
        .expect_err("reset_state should reject cross-principal access");
    assert_eq!(mismatched_status.code(), tonic::Code::PermissionDenied);
    {
        let sessions = runtime.sessions.lock().await;
        let session = sessions
            .get(session_id.ulid.as_str())
            .expect("session should remain after rejected reset");
        assert!(
            session.storage_entries.contains_key("https://example.com"),
            "rejected reset must not clear victim storage"
        );
    }

    let mut owner_reset = Request::new(browser_v1::ResetStateRequest {
        v: 1,
        session_id: Some(session_id.clone()),
        clear_cookies: false,
        clear_storage: true,
        reset_tabs: false,
        reset_permissions: false,
    });
    insert_principal(&mut owner_reset, "user:alpha");
    let response = service
        .reset_state(owner_reset)
        .await
        .expect("owner reset_state should execute")
        .into_inner();
    assert!(response.success, "owner reset should succeed: {}", response.error);
    assert_eq!(response.storage_entries_cleared, 1);
    let sessions = runtime.sessions.lock().await;
    let session =
        sessions.get(session_id.ulid.as_str()).expect("session should remain after owner reset");
    assert!(session.storage_entries.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_inspect_session_redacts_debug_state() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session = sessions
            .get_mut(session_id.ulid.as_str())
            .expect("session should exist for debug-state seeding");
        {
            let active_tab = session
                .active_tab_mut()
                .expect("session should retain an active tab for inspection");
            active_tab.last_url =
                Some("https://example.com/app?access_token=topsecret&safe=1".to_owned());
            active_tab.last_title = "Debug Session".to_owned();
            active_tab.last_page_body = "<html><body><main><button id=\"save\">Save</button><div>Visible debug text</div><form action=\"https://example.com/login?token=abc123\"></form></main></body></html>".to_owned();
            active_tab.network_log.push_back(NetworkLogEntryInternal {
                request_url: "https://example.com/api?access_token=topsecret&safe=1".to_owned(),
                status_code: 200,
                timing_bucket: "lt_100ms".to_owned(),
                latency_ms: 42,
                captured_at_unix_ms: 1,
                headers: vec![NetworkLogHeaderInternal {
                    name: "set-cookie".to_owned(),
                    value: "session=abc123".to_owned(),
                }],
            });
        }
        session.cookie_jar.insert(
            "example.com".to_owned(),
            HashMap::from([
                ("session".to_owned(), "abc123".to_owned()),
                ("theme".to_owned(), "light".to_owned()),
            ]),
        );
        session.storage_entries.insert(
            "https://example.com".to_owned(),
            HashMap::from([
                ("token".to_owned(), "supersecret".to_owned()),
                ("#email".to_owned(), "operator@example.com".to_owned()),
            ]),
        );
        session.action_log.push_back(BrowserActionLogEntryInternal {
            action_id: ulid::Ulid::new().to_string(),
            action_name: "navigate".to_owned(),
            selector: String::new(),
            success: false,
            outcome: "navigation_failed".to_owned(),
            error: "token=supersecret".to_owned(),
            started_at_unix_ms: 1,
            completed_at_unix_ms: 2,
            attempts: 1,
            page_url: "https://example.com/app?access_token=topsecret&safe=1".to_owned(),
        });
    }

    let mut inspect_request = Request::new(browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: true,
        include_storage: true,
        include_action_log: true,
        include_network_log: true,
        include_page_snapshot: true,
        include_console_log: true,
        include_page_diagnostics: true,
        max_cookie_bytes: 2 * 1024,
        max_storage_bytes: 2 * 1024,
        max_action_log_entries: 10,
        max_network_log_entries: 10,
        max_network_log_bytes: 4 * 1024,
        max_dom_snapshot_bytes: 4 * 1024,
        max_visible_text_bytes: 512,
        max_console_log_entries: 10,
        max_console_log_bytes: 2 * 1024,
    });
    insert_principal(&mut inspect_request, "user:ops");
    let inspected = service
        .inspect_session(inspect_request)
        .await
        .expect("inspect_session should execute")
        .into_inner();
    assert!(inspected.success, "inspect_session should succeed for seeded session state");
    let inspected_summary = inspected
        .session
        .as_ref()
        .and_then(|detail| detail.summary.as_ref())
        .expect("inspect response should include session summary");
    assert_eq!(inspected_summary.active_tab_title, "Debug Session");

    let cookie_domains = inspected
        .cookies
        .iter()
        .find(|domain| domain.domain == "example.com")
        .expect("cookie inspection should include example.com");
    assert!(
        cookie_domains
            .cookies
            .iter()
            .any(|cookie| cookie.name == "session" && cookie.value == "<redacted>"),
        "session cookie values must be redacted"
    );
    assert!(
        cookie_domains
            .cookies
            .iter()
            .any(|cookie| cookie.name == "theme" && cookie.value == "light"),
        "non-sensitive cookie values should remain visible"
    );

    let storage_origin = inspected
        .storage
        .iter()
        .find(|origin| origin.origin == "https://example.com")
        .expect("storage inspection should include the typed origin");
    assert!(
        storage_origin
            .entries
            .iter()
            .any(|entry| entry.key == "token" && entry.value == "<redacted>"),
        "sensitive storage keys must be redacted"
    );
    assert!(
        storage_origin
            .entries
            .iter()
            .any(|entry| entry.key == "#email" && entry.value == "operator@example.com"),
        "non-sensitive storage values should remain visible"
    );

    assert_eq!(
        inspected.action_log.first().map(|entry| entry.error.as_str()),
        Some("<redacted>"),
        "sensitive action log errors must be redacted"
    );
    let network_entry = inspected
        .network_log
        .first()
        .expect("network log inspection should include the seeded entry");
    assert!(
        network_entry.request_url.contains("access_token=<redacted>"),
        "network log URLs should be normalized and redacted: {}",
        network_entry.request_url
    );
    assert!(
        network_entry
            .headers
            .iter()
            .any(|header| header.name == "set-cookie" && header.value == "<redacted>"),
        "sensitive network log headers must be redacted"
    );
    assert!(
        inspected.dom_snapshot.contains("token=<redacted>"),
        "page snapshot should redact sensitive query parameters: {}",
        inspected.dom_snapshot
    );
    assert!(
        !inspected.dom_snapshot.contains("abc123") && !inspected.dom_snapshot.contains("topsecret"),
        "page snapshot must not leak sensitive values: {}",
        inspected.dom_snapshot
    );
    assert!(
        inspected.visible_text.contains("Visible debug text"),
        "visible text should expose useful debug context"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_inspect_session_truncates_deterministically() {
    let runtime = simulated_runtime_for_tests();
    let service = BrowserServiceImpl { runtime: Arc::clone(&runtime) };
    let created = create_test_session(&service, "user:ops").await;
    let session_id = created.session_id.expect("session id should be present");

    {
        let mut sessions = runtime.sessions.lock().await;
        let session = sessions
            .get_mut(session_id.ulid.as_str())
            .expect("session should exist for truncation seeding");
        {
            let active_tab = session
                .active_tab_mut()
                .expect("session should retain an active tab for truncation test");
            active_tab.last_url = Some("https://example.com/dashboard".to_owned());
            active_tab.last_title = "Truncation Fixture".to_owned();
            active_tab.last_page_body = format!(
                "<html><body><main>{}</main></body></html>",
                (0..24)
                    .map(|index| format!(
                        "<section id=\"section-{index}\"><button id=\"action-{index}\">Button {index}</button><p>Repeated truncation content {index}</p></section>"
                    ))
                    .collect::<String>()
            );
            for index in 0..4 {
                active_tab.network_log.push_back(NetworkLogEntryInternal {
                    request_url: format!("https://example.com/api/items/{index}?safe={index}"),
                    status_code: 200,
                    timing_bucket: "lt_100ms".to_owned(),
                    latency_ms: 10 + index,
                    captured_at_unix_ms: index,
                    headers: vec![NetworkLogHeaderInternal {
                        name: "x-request-id".to_owned(),
                        value: format!("req-{index}"),
                    }],
                });
            }
        }
        session.cookie_jar.insert(
            "example.com".to_owned(),
            HashMap::from([
                ("cookie-a".to_owned(), "a".repeat(48)),
                ("cookie-b".to_owned(), "b".repeat(48)),
                ("cookie-c".to_owned(), "c".repeat(48)),
            ]),
        );
        session.storage_entries.insert(
            "https://example.com".to_owned(),
            HashMap::from([
                ("field-a".to_owned(), "alpha".repeat(24)),
                ("field-b".to_owned(), "beta".repeat(24)),
                ("field-c".to_owned(), "gamma".repeat(24)),
            ]),
        );
        for index in 0..3 {
            session.action_log.push_back(BrowserActionLogEntryInternal {
                action_id: ulid::Ulid::new().to_string(),
                action_name: format!("action-{index}"),
                selector: format!("#selector-{index}"),
                success: true,
                outcome: "completed".to_owned(),
                error: String::new(),
                started_at_unix_ms: index,
                completed_at_unix_ms: index + 1,
                attempts: 1,
                page_url: "https://example.com/dashboard".to_owned(),
            });
        }
    }

    let request = browser_v1::InspectSessionRequest {
        v: 1,
        session_id: Some(session_id),
        include_cookies: true,
        include_storage: true,
        include_action_log: true,
        include_network_log: true,
        include_page_snapshot: true,
        include_console_log: true,
        include_page_diagnostics: true,
        max_cookie_bytes: 96,
        max_storage_bytes: 128,
        max_action_log_entries: 1,
        max_network_log_entries: 2,
        max_network_log_bytes: 128,
        max_dom_snapshot_bytes: 64,
        max_visible_text_bytes: 32,
        max_console_log_entries: 2,
        max_console_log_bytes: 128,
    };
    let mut first_request = Request::new(request.clone());
    insert_principal(&mut first_request, "user:ops");
    let first = service
        .inspect_session(first_request)
        .await
        .expect("first inspect_session should execute")
        .into_inner();
    let mut second_request = Request::new(request);
    insert_principal(&mut second_request, "user:ops");
    let second = service
        .inspect_session(second_request)
        .await
        .expect("second inspect_session should execute")
        .into_inner();

    assert!(first.cookies_truncated, "cookie payload should report truncation");
    assert!(first.storage_truncated, "storage payload should report truncation");
    assert!(first.action_log_truncated, "action log should report truncation");
    assert!(first.network_log_truncated, "network log should report truncation");
    assert!(first.dom_truncated, "DOM snapshot should report truncation");
    assert!(first.visible_text_truncated, "visible text should report truncation");
    assert_eq!(first.cookies, second.cookies, "cookie truncation must be deterministic");
    assert_eq!(first.storage, second.storage, "storage truncation must be deterministic");
    assert_eq!(first.action_log, second.action_log, "action log truncation must be deterministic");
    assert_eq!(
        first.network_log, second.network_log,
        "network log truncation must be deterministic"
    );
    assert_eq!(first.dom_snapshot, second.dom_snapshot, "DOM truncation must be deterministic");
    assert_eq!(
        first.visible_text, second.visible_text,
        "visible-text truncation must be deterministic"
    );
}

fn spawn_static_http_server(status_code: u16, body: &str) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let body = body.to_owned();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let _ = read_http_request(&mut stream);
        let response = format!(
                "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        stream.write_all(response.as_bytes()).expect("server should write response");
        stream.flush().expect("server should flush response");
    });
    (format!("http://{address}/"), handle)
}

fn spawn_chunked_http_server(
    status_code: u16,
    chunks: &[&str],
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let chunks = chunks.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let _ = read_http_request(&mut stream);
        let headers = format!(
                "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            );
        if !write_chunked_test_bytes(
            &mut stream,
            headers.as_bytes(),
            "server should write response headers",
        ) {
            return;
        }
        if !flush_chunked_test_stream(&mut stream, "server should flush response headers") {
            return;
        }
        for chunk in chunks {
            let prefix = format!("{:X}\r\n", chunk.len());
            if !write_chunked_test_bytes(
                &mut stream,
                prefix.as_bytes(),
                "server should write chunk length",
            ) {
                return;
            }
            if !write_chunked_test_bytes(
                &mut stream,
                chunk.as_bytes(),
                "server should write chunk body",
            ) {
                return;
            }
            if !write_chunked_test_bytes(&mut stream, b"\r\n", "server should terminate chunk") {
                return;
            }
            if !flush_chunked_test_stream(&mut stream, "server should flush chunk") {
                return;
            }
        }
        let _ = write_chunked_test_bytes(
            &mut stream,
            b"0\r\n\r\n",
            "server should write chunked terminator",
        );
        let _ = flush_chunked_test_stream(&mut stream, "server should flush chunked terminator");
    });
    (format!("http://{address}/"), handle)
}

fn spawn_redirect_http_server(location: &str) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let location = location.to_owned();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let _ = read_http_request(&mut stream);
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(response.as_bytes()).expect("server should write response");
        stream.flush().expect("server should flush response");
    });
    (format!("http://{address}/"), handle)
}

fn spawn_cookie_capture_http_server(url_host: &str) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let url = format!("http://{}:{}/", url_host, address.port());
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let request = read_http_request(&mut stream);
        let body = "<html><body>redirected</body></html>";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).expect("server should write response");
        stream.flush().expect("server should flush response");
        request
    });
    (url, handle)
}

fn write_chunked_test_bytes(stream: &mut TcpStream, bytes: &[u8], context: &str) -> bool {
    match stream.write_all(bytes) {
        Ok(()) => true,
        Err(error) if is_expected_chunked_test_disconnect(&error) => false,
        Err(error) => panic!("{context}: {error}"),
    }
}

fn flush_chunked_test_stream(stream: &mut TcpStream, context: &str) -> bool {
    match stream.flush() {
        Ok(()) => true,
        Err(error) if is_expected_chunked_test_disconnect(&error) => false,
        Err(error) => panic!("{context}: {error}"),
    }
}

fn is_expected_chunked_test_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::UnexpectedEof
    )
}

fn spawn_static_http_server_with_request_budget(
    status_code: u16,
    body: &str,
    max_requests: usize,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let body = body.to_owned();
    let handle = thread::spawn(move || {
        for _ in 0..max_requests {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let _ = read_http_request(&mut stream);
            let response = format!(
                    "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            stream.write_all(response.as_bytes()).expect("server should write response");
            stream.flush().expect("server should flush response");
        }
    });
    (format!("http://{address}/"), handle)
}

fn spawn_fetch_failure_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    listener.set_nonblocking(true).expect("listener should become nonblocking");
    let address = listener.local_addr().expect("listener local address should resolve");
    let handle = thread::spawn(move || {
        let started_at = std::time::Instant::now();
        let mut root_requests = 0usize;
        let mut api_seen = false;
        while started_at.elapsed() < Duration::from_secs(30) && !(api_seen && root_requests >= 1) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).expect("accepted stream should become blocking");
                    let request = read_http_request(&mut stream);
                    let path = http_request_path(request.as_str());
                    if path.starts_with("/api/profile")
                        || path.contains("/api/profile")
                        || request.contains("/api/profile")
                    {
                        api_seen = true;
                        let body = r#"{"error":"profile unavailable"}"#;
                        let response = format!(
                            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write API response");
                        stream.flush().expect("server should flush API response");
                        continue;
                    }
                    root_requests = root_requests.saturating_add(1);
                    let body = "<html><head><title>Fetch Failure</title></head><body><button id='loadProfile'>Load profile</button><div id='status'>idle</div><script>document.getElementById('loadProfile').addEventListener('click', () => { document.getElementById('status').textContent = 'loading'; fetch('/api/profile').then((response) => { document.getElementById('status').textContent = response.ok ? 'profile ok' : 'profile failed'; }).catch(() => { document.getElementById('status').textContent = 'profile failed'; }); });</script></body></html>";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("server should write page response");
                    stream.flush().expect("server should flush page response");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("listener accept failed: {error}"),
            }
        }
        assert!(api_seen, "fixture should receive the same-origin fetch request before shutdown");
    });
    (format!("http://{address}/"), handle)
}

fn spawn_click_fetch_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    listener.set_nonblocking(true).expect("listener should become nonblocking");
    let address = listener.local_addr().expect("listener local address should resolve");
    let handle = thread::spawn(move || {
        let started_at = std::time::Instant::now();
        let mut root_seen = false;
        let mut data_seen = false;
        while started_at.elapsed() < Duration::from_secs(20) && !(root_seen && data_seen) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).expect("accepted stream should become blocking");
                    let request = read_http_request(&mut stream);
                    if request.trim().is_empty() {
                        continue;
                    }
                    let path = http_request_path(request.as_str());
                    if path.starts_with("/mock-data.json")
                        || path.contains("/mock-data.json")
                        || request.contains("/mock-data.json")
                    {
                        data_seen = true;
                        let body = r#"[{"name":"Atlas"},{"name":"Beacon"},{"name":"Cobalt"}]"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write JSON response");
                        stream.flush().expect("server should flush JSON response");
                        continue;
                    }
                    if path == "/" || path.starts_with("/index.html") {
                        root_seen = true;
                        let body = r#"<html><head><title>Fetch After Click</title></head><body><button id="loadData">Load</button><div id="items">empty</div><script>
document.getElementById("loadData").addEventListener("click", async () => {
  try {
    const response = await fetch("./mock-data.json");
    const items = await response.json();
    document.getElementById("items").textContent = items.map((item) => item.name).join(", ");
  } catch (error) {
    document.getElementById("items").textContent = "fetch failed";
  }
});
</script></body></html>"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write page response");
                        stream.flush().expect("server should flush page response");
                        continue;
                    }
                    let body = "not found";
                    let response = format!(
                        "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("server should write fallback response");
                    stream.flush().expect("server should flush fallback response");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("listener accept failed: {error}"),
            }
        }
        assert!(root_seen, "fixture should receive the initial page request before shutdown");
        assert!(
            data_seen,
            "fixture should receive the click-triggered JSON request before shutdown"
        );
    });
    (format!("http://{address}/"), handle)
}

fn spawn_css_subresource_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    listener.set_nonblocking(true).expect("listener should become nonblocking");
    let address = listener.local_addr().expect("listener local address should resolve");
    let handle = thread::spawn(move || {
        let started_at = std::time::Instant::now();
        let mut root_requests = 0usize;
        let mut css_seen = false;
        while started_at.elapsed() < Duration::from_secs(20) && !(css_seen && root_requests >= 2) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).expect("accepted stream should become blocking");
                    let request = read_http_request(&mut stream);
                    if request.trim().is_empty() {
                        continue;
                    }
                    let path = http_request_path(request.as_str());
                    if path.starts_with("/styles.css") {
                        css_seen = true;
                        let body = ".cta { display: block; padding: 14px 28px; background-color: rgb(31, 77, 255); }";
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/css\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write CSS response");
                        stream.flush().expect("server should flush CSS response");
                        continue;
                    }
                    if path == "/" || path.starts_with("/index.html") {
                        root_requests = root_requests.saturating_add(1);
                        let body = r##"<html><head><title>Styled CTA</title><link rel="stylesheet" href="/styles.css" /></head><body><a class="cta" href="#">Continue</a></body></html>"##;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write page response");
                        stream.flush().expect("server should flush page response");
                        continue;
                    }
                    let body = "not found";
                    let response = format!(
                        "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).expect("server should write fallback");
                    stream.flush().expect("server should flush fallback");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("listener accept failed: {error}"),
            }
        }
        assert!(css_seen, "fixture should receive the initial stylesheet request before shutdown");
    });
    (format!("http://{address}/index.html"), handle)
}

fn spawn_hanging_subresource_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    listener.set_nonblocking(true).expect("listener should become nonblocking");
    let address = listener.local_addr().expect("listener local address should resolve");
    let handle = thread::spawn(move || {
        let started_at = std::time::Instant::now();
        let mut root_requests = 0usize;
        let mut hanging_seen = false;
        while started_at.elapsed() < Duration::from_secs(20)
            && !(hanging_seen && root_requests >= 2)
        {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).expect("accepted stream should become blocking");
                    let request = read_http_request(&mut stream);
                    if request.trim().is_empty() {
                        continue;
                    }
                    let path = http_request_path(request.as_str());
                    if path.starts_with("/slow.png") {
                        hanging_seen = true;
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    if path == "/" || path.starts_with("/index.html") {
                        root_requests = root_requests.saturating_add(1);
                        let body = r#"<html><head><title>Slow Subresource</title></head><body><main>usable dom</main><img src="/slow.png" alt="" /></body></html>"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("server should write page response");
                        stream.flush().expect("server should flush page response");
                        continue;
                    }
                    let body = "not found";
                    let response = format!(
                        "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).expect("server should write fallback");
                    stream.flush().expect("server should flush fallback");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("listener accept failed: {error}"),
            }
        }
        assert!(hanging_seen, "fixture should receive the hanging subresource request");
    });
    (format!("http://{address}/index.html"), handle)
}

fn spawn_static_http_server_with_headers(
    status_code: u16,
    body: &str,
    headers: &[(&str, &str)],
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let body = body.to_owned();
    let headers = headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let _ = read_http_request(&mut stream);
        let mut response = format!(
            "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (name, value) in headers {
            response.push_str(format!("{name}: {value}\r\n").as_str());
        }
        response.push_str("Connection: close\r\n\r\n");
        response.push_str(body.as_str());
        stream.write_all(response.as_bytes()).expect("server should write response");
        stream.flush().expect("server should flush response");
    });
    (format!("http://{address}/"), handle)
}

fn spawn_cookie_state_http_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let handle = thread::spawn(move || {
        for index in 0..3 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let request = read_http_request(&mut stream);
            let has_cookie = request.to_ascii_lowercase().contains("cookie: session=abc123");
            let (status_code, body, headers) = match index {
                0 => (200, "seed", vec!["Set-Cookie: session=abc123; Path=/"]),
                1 => {
                    if has_cookie {
                        (200, "cookie_replayed", Vec::new())
                    } else {
                        (401, "cookie_missing", Vec::new())
                    }
                }
                _ => {
                    if has_cookie {
                        (200, "cookie_still_present", Vec::new())
                    } else {
                        (401, "cookie_cleared", Vec::new())
                    }
                }
            };
            let mut response = format!(
                "HTTP/1.1 {status_code} OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n",
                body.len()
            );
            for header in headers {
                response.push_str(format!("{header}\r\n").as_str());
            }
            response.push_str("Connection: close\r\n\r\n");
            response.push_str(body);
            stream.write_all(response.as_bytes()).expect("server should write response");
            stream.flush().expect("server should flush response");
        }
    });
    (format!("http://{address}/"), handle)
}

fn spawn_download_fixture_http_server(
    file_path: &str,
    file_content_type: &str,
    file_body: &[u8],
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let file_path = file_path.to_owned();
    let file_content_type = file_content_type.to_owned();
    let file_body = file_body.to_vec();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let request = read_http_request(&mut stream);
            let path = http_request_path(request.as_str());
            if path == "/" {
                let body = format!(
                        "<!doctype html><html><body><a id=\"download-link\" href=\"{file_path}\" download>Download</a></body></html>"
                    );
                let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                stream.write_all(response.as_bytes()).expect("server should write HTML response");
                stream.flush().expect("server should flush HTML response");
                continue;
            }
            if path == file_path {
                let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {file_content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        file_body.len()
                    );
                stream
                    .write_all(headers.as_bytes())
                    .expect("server should write file response headers");
                stream
                    .write_all(file_body.as_slice())
                    .expect("server should write file response body");
                stream.flush().expect("server should flush file response");
                continue;
            }
            let fallback = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot_found";
            stream.write_all(fallback.as_bytes()).expect("server should write fallback response");
            stream.flush().expect("server should flush fallback response");
        }
    });
    (format!("http://{address}/"), handle)
}

fn spawn_attachment_fixture_http_server(
    file_path: &str,
    content_disposition: &str,
    file_content_type: &str,
    file_body: &[u8],
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let file_path = file_path.to_owned();
    let url_path = file_path.clone();
    let content_disposition = content_disposition.to_owned();
    let file_content_type = file_content_type.to_owned();
    let file_body = file_body.to_vec();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("listener should accept request");
        let request = read_http_request(&mut stream);
        let path = http_request_path(request.as_str());
        if path == file_path {
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {file_content_type}\r\nContent-Disposition: {content_disposition}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                file_body.len()
            );
            stream.write_all(headers.as_bytes()).expect("server should write file headers");
            stream.write_all(file_body.as_slice()).expect("server should write file body");
            stream.flush().expect("server should flush file response");
            return;
        }
        let response =
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot found";
        stream.write_all(response.as_bytes()).expect("server should write 404 response");
        stream.flush().expect("server should flush 404 response");
    });
    (format!("http://{address}{url_path}"), handle)
}

fn spawn_streaming_download_fixture_http_server(
    file_path: &str,
    file_content_type: &str,
    file_size: usize,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener local address should resolve");
    let file_path = file_path.to_owned();
    let file_content_type = file_content_type.to_owned();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let request = read_http_request(&mut stream);
            let path = http_request_path(request.as_str());
            if path == "/" {
                let body = format!(
                    "<!doctype html><html><body><a id=\"download-link\" href=\"{file_path}\" download>Download</a></body></html>"
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("server should write HTML response");
                stream.flush().expect("server should flush HTML response");
                continue;
            }
            if path == file_path {
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {file_content_type}\r\nContent-Length: {file_size}\r\nConnection: close\r\n\r\n",
                );
                stream
                    .write_all(headers.as_bytes())
                    .expect("server should write file response headers");
                let chunk = vec![b'a'; 64 * 1024];
                let mut remaining = file_size;
                while remaining > 0 {
                    let to_write = remaining.min(chunk.len());
                    if stream.write_all(&chunk[..to_write]).is_err() {
                        break;
                    }
                    remaining -= to_write;
                }
                let _ = stream.flush();
                continue;
            }
            let fallback = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot_found";
            stream.write_all(fallback.as_bytes()).expect("server should write fallback response");
            stream.flush().expect("server should flush fallback response");
        }
    });
    (format!("http://{address}/"), handle)
}

fn http_request_path(request: &str) -> String {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "/".to_owned())
}

fn read_http_request(stream: &mut TcpStream) -> String {
    stream
        // Chromium can open idle preconnect sockets before sending a request; keep fixture
        // servers from blocking long enough to starve the real request behind that socket.
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("read timeout should be configured");
    let mut output = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                output.extend_from_slice(&buffer[..read]);
                if output.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(output.as_slice()).to_string()
}
