use super::{
    browser_v1, chromium_active_tab_for_session, chromium_new_tab_error_is_retryable,
    default_browserd_state_dir_from_env, derive_state_encryption_key, encrypt_state_blob,
    enforce_non_loopback_bind_auth, navigate_with_guards, parse_daemon_bind_socket,
    persisted_snapshot_hash, persisted_snapshot_legacy_hash, record_chromium_remote_ip_incident,
    reset_dns_validation_tracking_for_tests, run_chromium_blocking, sha256_hex,
    store_dns_nxdomain_cache, update_profile_state_metadata,
    validate_restored_snapshot_against_profile, validate_target_url, validate_target_url_blocking,
    Args, BrowserEngineMode, BrowserProfileRecord, BrowserRuntimeState, BrowserServiceImpl,
    BrowserTabRecord, ChromiumSessionProxy, DnsValidationCache, PersistedSessionSnapshot,
    PersistedStateStore, SessionPermissionsInternal, AUTHORIZATION_HEADER,
    CANONICAL_PROTOCOL_MAJOR, CHROMIUM_NEW_TAB_RETRY_DELAY_MS, CHROMIUM_PATH_ENV,
    DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS, DEFAULT_GRPC_PORT, DEFAULT_MAX_TABS_PER_SESSION,
    MAX_RELAY_PAYLOAD_BYTES, ONE_BY_ONE_PNG, PROFILE_RECORD_SCHEMA_VERSION, STATE_KEY_LEN,
};
use crate::proto;
use crate::proto::palyra::browser::v1::browser_service_server::BrowserService;
use crate::security::auth::constant_time_eq_bytes;
use reqwest::Url;
use std::collections::HashMap;
use std::ffi::OsString;
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

fn insert_bearer_auth<T>(request: &mut Request<T>, token: &str) {
    let value =
        format!("Bearer {token}").parse().expect("authorization header value should be valid");
    request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
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
async fn navigate_with_guards_blocks_file_scheme() {
    let outcome =
        navigate_with_guards("file:///tmp/index.html", 1_000, true, 3, false, 1024, None).await;
    assert!(!outcome.success, "file scheme must be blocked");
    assert!(
        outcome.error.contains("blocked URL scheme"),
        "error should explain blocked scheme: {}",
        outcome.error
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn navigate_with_guards_enforces_response_size_limit() {
    let (url, handle) = spawn_chunked_http_server(
        200,
        &["<html><head><title>Oversized</title></head>", "<body>very ", "large</body></html>"],
    );
    let outcome = navigate_with_guards(url.as_str(), 2_000, true, 3, true, 16, None).await;
    assert!(!outcome.success, "oversized payload must fail");
    assert!(
        outcome.error.contains("max_response_bytes"),
        "size limit error should be explicit: {}",
        outcome.error
    );
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
    record_chromium_remote_ip_incident(Some("127.0.0.1"), false, &incident);
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
    record_chromium_remote_ip_incident(Some("[::1]"), false, &incident);
    let message = incident
        .lock()
        .expect("guard should lock after IPv6 incident")
        .clone()
        .expect("private IPv6 response IP should record an incident");
    assert!(message.contains("::1"), "incident should include violating IPv6 address: {message}");
}

#[test]
fn chromium_remote_ip_guard_ignores_public_and_opted_in_private_targets() {
    let incident = Arc::new(StdMutex::new(None::<String>));
    record_chromium_remote_ip_incident(Some("93.184.216.34"), false, &incident);
    assert!(
        incident.lock().expect("guard should lock after public response IP check").is_none(),
        "public response IP should not produce a remote IP guard incident"
    );

    record_chromium_remote_ip_incident(Some("127.0.0.1"), true, &incident);
    assert!(
        incident.lock().expect("guard should lock after private-target opt-in check").is_none(),
        "private-target opt-in should bypass remote IP guard incidents"
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
async fn chromium_session_proxy_allows_private_targets_when_opted_in() {
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
        BrowserRuntimeState::new(&Args {
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
async fn browser_service_chromium_engine_executes_real_dom_actions() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let (url, handle) = spawn_static_http_server_with_request_budget(
            200,
            "<html><head><title>Chromium Fixture</title><script>function markClicked(){document.getElementById('status').textContent='clicked';}</script></head><body><input id='name-input' /><button id='submit-btn' onclick='markClicked()'>Submit</button><div id='status'>idle</div></body></html>",
            8,
        );
    let runtime = std::sync::Arc::new(
        BrowserRuntimeState::new(&Args {
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

    let observed = service
        .observe(Request::new(browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(session_id),
            include_dom_snapshot: true,
            include_accessibility_tree: true,
            include_visible_text: true,
            max_dom_snapshot_bytes: 32 * 1024,
            max_accessibility_tree_bytes: 32 * 1024,
            max_visible_text_bytes: 8 * 1024,
        }))
        .await
        .expect("observe should execute")
        .into_inner();
    assert!(observed.success, "chromium observe should succeed: {}", observed.error);
    assert!(
        observed.visible_text.contains("clicked"),
        "observe visible text should reflect click side-effect from real DOM"
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
        BrowserRuntimeState::new(&Args {
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
async fn browser_service_chromium_refreshes_snapshot_before_allowlisted_actions() {
    let Some(chromium_path) = resolve_chromium_path_for_tests() else {
        return;
    };
    let runtime = std::sync::Arc::new(
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
                max_navigation_timeout_ms: 0,
                max_session_lifetime_ms: 0,
                max_screenshot_bytes: 0,
                max_response_bytes: u64::MAX,
                max_action_timeout_ms: 0,
                max_type_input_bytes: u64::MAX,
                max_actions_per_session: u64::MAX,
                max_actions_per_window: 0,
                action_rate_window_ms: 0,
                max_action_log_entries: u64::MAX,
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
    let effective_budget = created.effective_budget.expect("effective budget should be returned");
    assert_eq!(
        effective_budget.max_response_bytes, default_budget.max_response_bytes,
        "untrusted session budgets must not widen max_response_bytes"
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
        effective_budget.max_action_log_entries, default_budget.max_action_log_entries as u64,
        "untrusted session budgets must not widen max_action_log_entries"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_service_rejects_oversized_type_input() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><body><input id=\"name\" name=\"name\" /></body></html>",
    );
    let runtime = std::sync::Arc::new(
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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

    let without_headers = service
        .network_log(Request::new(browser_v1::NetworkLogRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            limit: 10,
            include_headers: false,
            max_payload_bytes: 8 * 1024,
        }))
        .await
        .expect("network_log without headers should execute")
        .into_inner();
    assert!(without_headers.success, "network log call should succeed");
    assert!(!without_headers.entries.is_empty(), "network log should contain entries");
    assert!(
        without_headers.entries.iter().all(|entry| entry.headers.is_empty()),
        "headers must be excluded unless explicitly requested"
    );

    let with_headers = service
        .network_log(Request::new(browser_v1::NetworkLogRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            limit: 10,
            include_headers: true,
            max_payload_bytes: 8 * 1024,
        }))
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
        BrowserRuntimeState::new(&Args {
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

    let reset = service
        .reset_state(Request::new(browser_v1::ResetStateRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.clone() }),
            clear_cookies: true,
            clear_storage: false,
            reset_tabs: false,
            reset_permissions: false,
        }))
        .await
        .expect("reset_state should execute")
        .into_inner();
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
async fn browser_service_permissions_default_to_deny() {
    let runtime = std::sync::Arc::new(
        BrowserRuntimeState::new(&Args {
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
async fn browser_service_tabs_keep_independent_state() {
    let (url, handle) = spawn_static_http_server(
        200,
        "<html><head><title>Secondary Tab</title></head><body>tab-two</body></html>",
    );
    let runtime = std::sync::Arc::new(
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
    let mut runtime_state = BrowserRuntimeState::new(&Args {
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
    let mut runtime_state = BrowserRuntimeState::new(&Args {
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
    update_profile_state_metadata(
        store,
        profile_id.as_str(),
        PROFILE_RECORD_SCHEMA_VERSION,
        snapshot.state_revision,
        expected_hash.as_str(),
    )
    .expect("profile metadata update should succeed");

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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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
        BrowserRuntimeState::new(&Args {
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

    let listed = service
        .list_download_artifacts(Request::new(browser_v1::ListDownloadArtifactsRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            limit: 10,
            quarantined_only: false,
        }))
        .await
        .expect("list_download_artifacts should execute")
        .into_inner();
    assert_eq!(listed.artifacts.len(), 2, "both artifacts should be registered");
    assert!(
        listed.artifacts.iter().any(|artifact| artifact.quarantined),
        "download artifact list should include quarantined entries"
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
        stream.write_all(headers.as_bytes()).expect("server should write response headers");
        stream.flush().expect("server should flush response headers");
        for chunk in chunks {
            let prefix = format!("{:X}\r\n", chunk.len());
            stream.write_all(prefix.as_bytes()).expect("server should write chunk length");
            stream.write_all(chunk.as_bytes()).expect("server should write chunk body");
            stream.write_all(b"\r\n").expect("server should terminate chunk");
            stream.flush().expect("server should flush chunk");
        }
        stream.write_all(b"0\r\n\r\n").expect("server should write chunked terminator");
        stream.flush().expect("server should flush chunked terminator");
    });
    (format!("http://{address}/"), handle)
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
        .set_read_timeout(Some(Duration::from_secs(2)))
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
