use crate::{
    compute_backoff_ms, load_secret_utf8, normalize_optional_text, normalize_token_endpoint,
    persist_secret_utf8, validate_runtime_token_endpoint_with_resolver, AuthCredential,
    AuthCredentialType, AuthProfileEligibility, AuthProfileError, AuthProfileFailureKind,
    AuthProfileListFilter, AuthProfileRegistry, AuthProfileScope, AuthProfileSelectionRequest,
    AuthProfileSetRequest, AuthProvider, AuthProviderKind, AuthTokenExpiryState,
    HttpOAuthRefreshAdapter, OAuthRefreshAdapter, OAuthRefreshError, OAuthRefreshOutcomeKind,
    OAuthRefreshRequest, OAuthRefreshResponse, OAuthRefreshState,
};
use palyra_vault::Vault;
use palyra_vault::{
    BackendPreference as VaultBackendPreference, VaultConfig as VaultConfigOptions,
};
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier, Condvar, Mutex,
};
use std::thread;
use std::time::Duration;

struct StubRefreshAdapter {
    response: Result<OAuthRefreshResponse, OAuthRefreshError>,
    call_count: Arc<Mutex<u64>>,
}

impl OAuthRefreshAdapter for StubRefreshAdapter {
    fn refresh_access_token(
        &self,
        _request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
        let mut guard = self.call_count.lock().expect("test mutex should be available");
        *guard = guard.saturating_add(1);
        self.response.clone()
    }
}

struct RacingRefreshAdapter {
    barrier: Arc<Barrier>,
    call_count: Arc<AtomicUsize>,
    success_persisted: Arc<(Mutex<bool>, Condvar)>,
}

impl OAuthRefreshAdapter for RacingRefreshAdapter {
    fn refresh_access_token(
        &self,
        _request: &OAuthRefreshRequest,
    ) -> Result<OAuthRefreshResponse, OAuthRefreshError> {
        let call_index = self.call_count.fetch_add(1, Ordering::SeqCst);
        self.barrier.wait();
        if call_index == 0 {
            return Ok(OAuthRefreshResponse {
                access_token: "race-access-token".to_owned(),
                refresh_token: None,
                expires_in_seconds: Some(60),
            });
        }
        let (lock, signal) = &*self.success_persisted;
        let guard = lock.lock().expect("test mutex should be available");
        let wait_result = signal
            .wait_timeout_while(guard, Duration::from_secs(5), |persisted| !*persisted)
            .expect("test condvar should be available");
        if !*wait_result.0 {
            return Err(OAuthRefreshError::Transport(
                "timed out waiting for successful refresh to persist".to_owned(),
            ));
        }
        Err(OAuthRefreshError::Transport("simulated transport fault".to_owned()))
    }
}

fn open_test_vault(root: &Path, identity_root: &Path) -> Vault {
    Vault::open_with_config(VaultConfigOptions {
        root: Some(root.to_path_buf()),
        identity_store_root: Some(identity_root.to_path_buf()),
        backend_preference: VaultBackendPreference::EncryptedFile,
        ..VaultConfigOptions::default()
    })
    .expect("test vault should initialize")
}

fn auth_registry_lock_path(identity_root: &Path) -> PathBuf {
    let state_root = identity_root.parent().unwrap_or(identity_root);
    state_root.join("auth_profiles.toml.lock")
}

fn sample_oauth_profile_request(
    token_endpoint: String,
    expires_at_unix_ms: Option<i64>,
    refresh_state: OAuthRefreshState,
) -> AuthProfileSetRequest {
    AuthProfileSetRequest {
        profile_id: "openai-default".to_owned(),
        provider: AuthProvider::known(AuthProviderKind::Openai),
        profile_name: "default".to_owned(),
        scope: AuthProfileScope::Global,
        credential: AuthCredential::Oauth {
            access_token_vault_ref: "global/auth_openai_access".to_owned(),
            refresh_token_vault_ref: "global/auth_openai_refresh".to_owned(),
            token_endpoint,
            client_id: Some("test-client".to_owned()),
            client_secret_vault_ref: Some("global/auth_openai_client_secret".to_owned()),
            scopes: vec!["chat:read".to_owned()],
            expires_at_unix_ms,
            refresh_state,
        },
    }
}

fn sample_oauth_profile_request_with_identity(
    profile_id: &str,
    profile_name: &str,
) -> AuthProfileSetRequest {
    let mut request = sample_oauth_profile_request(
        "https://example.test/token".to_owned(),
        None,
        OAuthRefreshState::default(),
    );
    request.profile_id = profile_id.to_owned();
    request.profile_name = profile_name.to_owned();
    request
}

fn spawn_oauth_server(response_body: String) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
    let address = listener.local_addr().expect("test server should resolve local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test server should accept request");
        let mut request_buffer = [0_u8; 2048];
        let _ = stream.read(&mut request_buffer);
        let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
        stream.write_all(headers.as_bytes()).expect("test server should write response headers");
        stream.write_all(response_body.as_bytes()).expect("test server should write response body");
        stream.flush().expect("test server should flush response");
    });
    (format!("http://{address}/oauth/token"), handle)
}

fn spawn_redirect_server(location: String) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
    let address = listener.local_addr().expect("test server should resolve local addr");
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test server should accept request");
        let mut request_buffer = [0_u8; 2048];
        let _ = stream.read(&mut request_buffer);
        let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        stream.write_all(response.as_bytes()).expect("test server should write redirect response");
        stream.flush().expect("test server should flush redirect response");
    });
    (format!("http://{address}/oauth/token"), handle)
}

#[test]
fn compute_backoff_grows_exponentially_and_caps_per_provider() {
    let provider = AuthProvider::known(AuthProviderKind::Openai);
    assert_eq!(compute_backoff_ms(&provider, 1), 15_000);
    assert_eq!(compute_backoff_ms(&provider, 2), 30_000);
    assert_eq!(compute_backoff_ms(&provider, 3), 60_000);
    assert_eq!(compute_backoff_ms(&provider, 20), 300_000);
}

#[test]
fn list_profiles_resumes_from_after_profile_id() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");

    for (profile_id, profile_name) in
        [("openai-alpha", "alpha"), ("openai-beta", "beta"), ("openai-gamma", "gamma")]
    {
        registry
            .set_profile(sample_oauth_profile_request_with_identity(profile_id, profile_name))
            .expect("profile should persist");
    }

    let first_page = registry
        .list_profiles(AuthProfileListFilter {
            after_profile_id: None,
            limit: Some(2),
            provider: None,
            scope: None,
        })
        .expect("first page should load");
    assert_eq!(
        first_page.profiles.iter().map(|profile| profile.profile_id.as_str()).collect::<Vec<_>>(),
        vec!["openai-alpha", "openai-beta"]
    );
    assert_eq!(first_page.next_after_profile_id.as_deref(), Some("openai-beta"));

    let second_page = registry
        .list_profiles(AuthProfileListFilter {
            after_profile_id: first_page.next_after_profile_id.clone(),
            limit: Some(2),
            provider: None,
            scope: None,
        })
        .expect("second page should load");
    assert_eq!(
        second_page.profiles.iter().map(|profile| profile.profile_id.as_str()).collect::<Vec<_>>(),
        vec!["openai-gamma"]
    );
    assert_eq!(second_page.next_after_profile_id, None);
}

#[test]
fn list_profiles_rejects_unknown_after_profile_id() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");

    registry
        .set_profile(sample_oauth_profile_request_with_identity("openai-alpha", "alpha"))
        .expect("profile should persist");

    let error = registry
        .list_profiles(AuthProfileListFilter {
            after_profile_id: Some("missing-profile".to_owned()),
            limit: Some(10),
            provider: None,
            scope: None,
        })
        .expect_err("unknown cursor should fail");
    assert!(matches!(
        error,
        AuthProfileError::InvalidField { field, message }
            if field == "after_profile_id"
                && message == "cursor does not exist in current result set"
    ));
}

#[test]
fn list_profiles_rejects_deleted_after_profile_id() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");

    for (profile_id, profile_name) in
        [("openai-alpha", "alpha"), ("openai-beta", "beta"), ("openai-gamma", "gamma")]
    {
        registry
            .set_profile(sample_oauth_profile_request_with_identity(profile_id, profile_name))
            .expect("profile should persist");
    }

    let first_page = registry
        .list_profiles(AuthProfileListFilter {
            after_profile_id: None,
            limit: Some(2),
            provider: None,
            scope: None,
        })
        .expect("first page should load");
    let stale_cursor =
        first_page.next_after_profile_id.expect("first page should return continuation cursor");
    assert!(
        registry
            .delete_profile(stale_cursor.as_str())
            .expect("cursor profile deletion should succeed"),
        "stale cursor profile should be deleted"
    );

    let error = registry
        .list_profiles(AuthProfileListFilter {
            after_profile_id: Some(stale_cursor),
            limit: Some(2),
            provider: None,
            scope: None,
        })
        .expect_err("deleted cursor should fail");
    assert!(matches!(
        error,
        AuthProfileError::InvalidField { field, message }
            if field == "after_profile_id"
                && message == "cursor does not exist in current result set"
    ));
}

#[test]
fn normalize_optional_text_truncates_on_utf8_boundary() {
    let normalized = normalize_optional_text(Some("A🙂B".to_owned()), 3)
        .expect("non-empty input should remain present");
    assert_eq!(normalized, "A");
}

#[test]
fn normalize_token_endpoint_rejects_non_loopback_http_hosts() {
    let error = normalize_token_endpoint("http://example.test/oauth/token")
        .expect_err("non-loopback http endpoint must be rejected");
    assert!(matches!(
        error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("loopback")
    ));
}

#[test]
fn normalize_token_endpoint_allows_loopback_http_hosts() {
    let ipv4 = normalize_token_endpoint("http://127.0.0.1:8080/oauth/token")
        .expect("loopback ipv4 endpoint should be accepted");
    let host = normalize_token_endpoint("http://localhost:8080/oauth/token")
        .expect("localhost endpoint should be accepted");
    let ipv6 = normalize_token_endpoint("http://[::1]:8080/oauth/token")
        .expect("loopback ipv6 endpoint should be accepted");
    assert_eq!(ipv4, "http://127.0.0.1:8080/oauth/token");
    assert_eq!(host, "http://localhost:8080/oauth/token");
    assert_eq!(ipv6, "http://[::1]:8080/oauth/token");
}

#[test]
fn normalize_token_endpoint_rejects_https_private_targets() {
    let localhost_error = normalize_token_endpoint("https://localhost/oauth/token")
        .expect_err("https localhost token endpoint must be rejected");
    let private_ip_error = normalize_token_endpoint("https://127.0.0.1/oauth/token")
        .expect_err("https private ip token endpoint must be rejected");
    assert!(matches!(
        localhost_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("localhost/private network")
    ));
    assert!(matches!(
        private_ip_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("localhost/private network")
    ));
}

#[test]
fn normalize_token_endpoint_rejects_userinfo_components() {
    let username_error = normalize_token_endpoint("https://user@example.test/oauth/token")
        .expect_err("username in URL should be rejected");
    let password_error = normalize_token_endpoint("https://user:secret@example.test/oauth/token")
        .expect_err("username/password in URL should be rejected");
    assert!(matches!(
        username_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("username/password")
    ));
    assert!(matches!(
        password_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("username/password")
    ));
}

#[test]
fn normalize_token_endpoint_rejects_query_and_fragment_components() {
    let query_error = normalize_token_endpoint("https://example.test/oauth/token?token=secret")
        .expect_err("query-bearing token endpoint must be rejected");
    let fragment_error = normalize_token_endpoint("https://example.test/oauth/token#secret")
        .expect_err("fragment-bearing token endpoint must be rejected");
    assert!(matches!(
        query_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("query or fragment")
    ));
    assert!(matches!(
        fragment_error,
        AuthProfileError::InvalidField { field, message }
            if field == "oauth.token_endpoint"
                && message.contains("query or fragment")
    ));
}

#[test]
fn oauth_refresh_adapter_does_not_follow_redirects() {
    let (token_endpoint, redirect_thread) =
        spawn_redirect_server("http://127.0.0.1:0/oauth/token".to_owned());
    let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
        .expect("HTTP adapter should initialize");
    let request = OAuthRefreshRequest {
        provider: AuthProvider::known(AuthProviderKind::Openai),
        token_endpoint,
        client_id: Some("test-client".to_owned()),
        client_secret: Some("test-secret".to_owned()),
        refresh_token: "refresh-token".to_owned(),
        scopes: vec!["chat:read".to_owned()],
    };
    let result = adapter.refresh_access_token(&request);
    assert!(matches!(
        result,
        Err(OAuthRefreshError::HttpStatus { status }) if status == 307
    ));
    redirect_thread.join().expect("redirect test server thread should exit cleanly");
}

#[test]
fn oauth_refresh_adapter_rejects_query_bearing_endpoint() {
    let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
        .expect("HTTP adapter should initialize");
    let request = OAuthRefreshRequest {
        provider: AuthProvider::known(AuthProviderKind::Openai),
        token_endpoint: "https://example.test/oauth/token?token=secret".to_owned(),
        client_id: Some("test-client".to_owned()),
        client_secret: Some("test-secret".to_owned()),
        refresh_token: "refresh-token".to_owned(),
        scopes: vec!["chat:read".to_owned()],
    };
    let result = adapter.refresh_access_token(&request);
    assert!(matches!(
        result,
        Err(OAuthRefreshError::Transport(message))
            if message.contains("query or fragment")
    ));
}

#[test]
fn oauth_refresh_runtime_validation_rejects_https_hostnames_resolving_private_addresses() {
    let result = validate_runtime_token_endpoint_with_resolver(
        "https://auth.example.test/oauth/token",
        |_host, _port| Ok(vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]),
    );
    assert!(matches!(
        result,
        Err(OAuthRefreshError::Transport(message))
            if message.contains("network policy")
                || message.contains("private/local")
    ));
}

#[test]
fn refresh_skips_when_cooldown_is_active() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let now = 1_730_000_000_000_i64;
    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_add(30_000)),
            OAuthRefreshState {
                failure_count: 2,
                last_error: Some("oauth refresh transport failure".to_owned()),
                last_attempt_unix_ms: Some(now.saturating_sub(1_000)),
                last_success_unix_ms: None,
                next_allowed_refresh_unix_ms: Some(now.saturating_add(120_000)),
            },
        ))
        .expect("profile should persist");

    let calls = Arc::new(Mutex::new(0_u64));
    let adapter = StubRefreshAdapter {
        response: Ok(OAuthRefreshResponse {
            access_token: "new-access".to_owned(),
            refresh_token: None,
            expires_in_seconds: Some(60),
        }),
        call_count: Arc::clone(&calls),
    };
    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("cooldown check should succeed");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::SkippedCooldown);
    assert_eq!(
        *calls.lock().expect("call counter should be available"),
        0,
        "adapter must not be called when cooldown is active"
    );
}

#[test]
fn set_profile_keeps_in_memory_state_when_registry_write_fails() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");

    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            None,
            OAuthRefreshState::default(),
        ))
        .expect("initial profile write should succeed");

    let lock_path = auth_registry_lock_path(identity_root.as_path());
    fs::write(&lock_path, "busy").expect("lock file should be created");
    let mut updated_request = sample_oauth_profile_request(
        "https://example.test/token".to_owned(),
        None,
        OAuthRefreshState::default(),
    );
    updated_request.profile_name = "updated-profile".to_owned();
    let error =
        registry.set_profile(updated_request).expect_err("persist lock should force write failure");
    assert!(
        matches!(error, AuthProfileError::WriteRegistry { .. }),
        "set_profile should fail with write-registry error when lock is held"
    );
    fs::remove_file(&lock_path).expect("lock file should be removed");

    let stored = registry
        .get_profile("openai-default")
        .expect("profile lookup should succeed")
        .expect("profile should still exist");
    assert_eq!(
        stored.profile_name, "default",
        "in-memory state must remain unchanged when persist fails"
    );
}

#[test]
fn oauth_refresh_integration_updates_vault_secret() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let now = 1_730_000_000_000_i64;
    let (token_endpoint, server_thread) = spawn_oauth_server(
        r#"{"access_token":"access-new","refresh_token":"refresh-new","expires_in":120}"#
            .to_owned(),
    );

    registry
        .set_profile(sample_oauth_profile_request(
            token_endpoint,
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");
    let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
        .expect("HTTP adapter should initialize");
    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("refresh should succeed");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Succeeded);

    let access = load_secret_utf8(&vault, "global/auth_openai_access")
        .expect("access secret should be readable");
    let refresh = load_secret_utf8(&vault, "global/auth_openai_refresh")
        .expect("refresh secret should be readable");
    assert_eq!(access, "access-new");
    assert_eq!(refresh, "refresh-new");
    server_thread.join().expect("test server thread should exit cleanly");
}

#[test]
fn oauth_refresh_integration_accepts_expired_in_alias() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let now = 1_730_000_000_000_i64;
    let (token_endpoint, server_thread) = spawn_oauth_server(
        r#"{"access_token":"access-new","refresh_token":"refresh-new","expired_in":"120"}"#
            .to_owned(),
    );

    registry
        .set_profile(sample_oauth_profile_request(
            token_endpoint,
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");
    let adapter = HttpOAuthRefreshAdapter::with_timeout(Duration::from_secs(2))
        .expect("HTTP adapter should initialize");
    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("refresh should succeed");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Succeeded);

    let profile = registry
        .get_profile("openai-default")
        .expect("profile lookup should succeed")
        .expect("profile should exist");
    let AuthCredential::Oauth { expires_at_unix_ms, .. } = profile.credential else {
        panic!("profile should keep oauth credential type");
    };
    assert_eq!(expires_at_unix_ms, Some(now.saturating_add(120_000)));
    server_thread.join().expect("test server thread should exit cleanly");
}

#[test]
fn refresh_fails_when_client_secret_reference_is_missing() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-static")
        .expect("refresh secret should persist");

    let now = 1_730_000_000_000_i64;
    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");

    let call_count = Arc::new(Mutex::new(0_u64));
    let adapter = StubRefreshAdapter {
        response: Ok(OAuthRefreshResponse {
            access_token: "unused-access".to_owned(),
            refresh_token: None,
            expires_in_seconds: Some(120),
        }),
        call_count: Arc::clone(&call_count),
    };

    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("missing client secret should produce persisted failure");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Failed);
    assert_eq!(
        outcome.reason, "client secret reference is missing or unreadable",
        "failure reason should explain missing secret reference"
    );
    assert_eq!(
        *call_count.lock().expect("call counter should be available"),
        0,
        "adapter must not be called when configured client secret cannot be loaded"
    );
}

#[test]
fn concurrent_refresh_stale_failure_does_not_override_success_state() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry = Arc::new(
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize"),
    );
    let vault = Arc::new(open_test_vault(vault_root.as_path(), identity_root.as_path()));
    persist_secret_utf8(vault.as_ref(), "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(vault.as_ref(), "global/auth_openai_refresh", "refresh-old")
        .expect("refresh secret should persist");
    persist_secret_utf8(vault.as_ref(), "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let now = 1_730_000_000_000_i64;
    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");

    let adapter = Arc::new(RacingRefreshAdapter {
        barrier: Arc::new(Barrier::new(2)),
        call_count: Arc::new(AtomicUsize::new(0)),
        success_persisted: Arc::new((Mutex::new(false), Condvar::new())),
    });
    let success_persisted_left = Arc::clone(&adapter.success_persisted);
    let registry_left = Arc::clone(&registry);
    let vault_left = Arc::clone(&vault);
    let adapter_left = Arc::clone(&adapter);
    let worker_left = thread::spawn(move || {
        let outcome = registry_left.refresh_oauth_profile_with_clock(
            "openai-default",
            vault_left.as_ref(),
            adapter_left.as_ref(),
            now,
        );
        if let Ok(value) = outcome.as_ref() {
            if value.kind == OAuthRefreshOutcomeKind::Succeeded {
                let (lock, signal) = &*success_persisted_left;
                let mut persisted = lock.lock().expect("test mutex should be available");
                *persisted = true;
                signal.notify_all();
            }
        }
        outcome
    });
    let success_persisted_right = Arc::clone(&adapter.success_persisted);
    let registry_right = Arc::clone(&registry);
    let vault_right = Arc::clone(&vault);
    let adapter_right = Arc::clone(&adapter);
    let worker_right = thread::spawn(move || {
        let outcome = registry_right.refresh_oauth_profile_with_clock(
            "openai-default",
            vault_right.as_ref(),
            adapter_right.as_ref(),
            now,
        );
        if let Ok(value) = outcome.as_ref() {
            if value.kind == OAuthRefreshOutcomeKind::Succeeded {
                let (lock, signal) = &*success_persisted_right;
                let mut persisted = lock.lock().expect("test mutex should be available");
                *persisted = true;
                signal.notify_all();
            }
        }
        outcome
    });

    let left_outcome = worker_left
        .join()
        .expect("left worker thread should join")
        .expect("left refresh call should complete");
    let right_outcome = worker_right
        .join()
        .expect("right worker thread should join")
        .expect("right refresh call should complete");
    let kinds = [left_outcome.kind, right_outcome.kind];
    assert!(
        kinds.contains(&OAuthRefreshOutcomeKind::Succeeded),
        "one concurrent refresh should succeed"
    );
    assert!(
        kinds.contains(&OAuthRefreshOutcomeKind::SkippedCooldown),
        "stale failure result should be ignored instead of overwriting success"
    );

    let profile = registry
        .get_profile("openai-default")
        .expect("profile lookup should succeed")
        .expect("profile should exist");
    let AuthCredential::Oauth { refresh_state, .. } = profile.credential else {
        panic!("profile should keep oauth credential type");
    };
    assert_eq!(
        refresh_state.failure_count, 0,
        "stale concurrent failure must not increment failure count"
    );
    assert!(
        refresh_state.last_error.is_none(),
        "stale concurrent failure must not write last_error"
    );
    let access = load_secret_utf8(vault.as_ref(), "global/auth_openai_access")
        .expect("access token should be readable");
    assert_eq!(access, "race-access-token");
}

#[test]
fn refresh_failure_reason_is_sanitized_and_does_not_leak_secret_material() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-top-secret")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let now = 1_730_000_000_000_i64;
    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");

    let adapter = StubRefreshAdapter {
        response: Err(OAuthRefreshError::InvalidResponse(
            "response contains refresh_token=refresh-top-secret".to_owned(),
        )),
        call_count: Arc::new(Mutex::new(0)),
    };
    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("refresh failure should be persisted");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Failed);
    assert!(
        !outcome.reason.contains("refresh-top-secret"),
        "sanitized refresh reason must not leak secret values"
    );

    let profile = registry
        .get_profile("openai-default")
        .expect("profile lookup should succeed")
        .expect("profile should exist");
    let AuthCredential::Oauth { refresh_state, .. } = profile.credential else {
        panic!("profile should keep oauth credential type");
    };
    let stored_error = refresh_state.last_error.unwrap_or_default();
    assert!(
        !stored_error.contains("refresh-top-secret"),
        "persisted refresh error should not leak refresh token values"
    );
}

#[test]
fn health_report_state_survives_registry_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let now = 1_730_000_000_000_i64;

    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-old")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");

    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");
    drop(registry);

    let reopened = AuthProfileRegistry::open(identity_root.as_path())
        .expect("registry should reopen from persisted file");
    let report = reopened
        .health_report_with_clock(&vault, None, now, 15 * 60 * 1_000)
        .expect("health report should compute");
    assert_eq!(report.summary.total, 1);
    assert_eq!(report.summary.expired, 1);
}

#[test]
fn health_report_serialization_does_not_include_secret_values() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let now = 1_730_000_000_000_i64;

    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    let access_secret = "access-super-secret-token";
    let refresh_secret = "refresh-super-secret-token";
    let client_secret = "client-super-secret";
    persist_secret_utf8(&vault, "global/auth_openai_access", access_secret)
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", refresh_secret)
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", client_secret)
        .expect("client secret should persist");

    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_add(60_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");

    let report = registry
        .health_report_with_clock(&vault, None, now, 15 * 60 * 1_000)
        .expect("health report should compute");
    let serialized =
        serde_json::to_string(&report).expect("health report should serialize as JSON");

    assert!(
        !serialized.contains(access_secret)
            && !serialized.contains(refresh_secret)
            && !serialized.contains(client_secret),
        "health report payload must not expose secret values loaded from vault refs"
    );
}

#[test]
fn invalid_profile_id_is_rejected() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let request = AuthProfileSetRequest {
        profile_id: "bad profile".to_owned(),
        provider: AuthProvider::known(AuthProviderKind::Openai),
        profile_name: "default".to_owned(),
        scope: AuthProfileScope::Global,
        credential: AuthCredential::ApiKey {
            api_key_vault_ref: "global/openai_api_key".to_owned(),
        },
    };
    let error = registry.set_profile(request).expect_err("invalid id should fail");
    assert!(
        matches!(error, AuthProfileError::InvalidField { field, .. } if field == "profile_id"),
        "invalid profile_id should return field validation error"
    );
}

#[test]
fn refresh_due_profiles_marks_transport_failure_without_retry_spam() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-old")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-old")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-secret")
        .expect("client secret should persist");
    let now = 1_730_000_000_000_i64;

    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");
    let calls = Arc::new(Mutex::new(0_u64));
    let adapter = StubRefreshAdapter {
        response: Err(OAuthRefreshError::Transport("connection reset".to_owned())),
        call_count: Arc::clone(&calls),
    };
    let first = registry
        .refresh_due_oauth_profiles_with_clock(&vault, &adapter, None, now, 5 * 60 * 1_000)
        .expect("refresh sweep should complete");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind, OAuthRefreshOutcomeKind::Failed);
    let second = registry
        .refresh_due_oauth_profiles_with_clock(
            &vault,
            &adapter,
            None,
            now.saturating_add(1_000),
            5 * 60 * 1_000,
        )
        .expect("refresh sweep should complete");
    assert_eq!(second[0].kind, OAuthRefreshOutcomeKind::SkippedCooldown);
    assert_eq!(
        *calls.lock().expect("call counter should be available"),
        1,
        "cooldown should suppress immediate repeated refresh attempts"
    );
}

#[test]
fn runtime_state_persists_refresh_failure_without_secret_material() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/auth_openai_access", "access-top-secret")
        .expect("access secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_refresh", "refresh-top-secret")
        .expect("refresh secret should persist");
    persist_secret_utf8(&vault, "global/auth_openai_client_secret", "client-top-secret")
        .expect("client secret should persist");
    let now = 1_730_000_000_000_i64;

    registry
        .set_profile(sample_oauth_profile_request(
            "https://example.test/token".to_owned(),
            Some(now.saturating_sub(1_000)),
            OAuthRefreshState::default(),
        ))
        .expect("profile should persist");
    let adapter = StubRefreshAdapter {
        response: Err(OAuthRefreshError::Transport(
            "response contained access-top-secret refresh-top-secret".to_owned(),
        )),
        call_count: Arc::new(Mutex::new(0_u64)),
    };
    let outcome = registry
        .refresh_oauth_profile_with_clock("openai-default", &vault, &adapter, now)
        .expect("refresh failure should persist");
    assert_eq!(outcome.kind, OAuthRefreshOutcomeKind::Failed);

    let records = registry
        .runtime_records_for_agent_with_clock(&vault, None, now.saturating_add(1), 15 * 60 * 1_000)
        .expect("runtime records should load");
    let record = records.first().expect("runtime record should exist");
    assert_eq!(record.profile_id, "openai-default");
    assert_eq!(record.failure_count, 1);
    assert_eq!(record.last_failure_kind, Some(AuthProfileFailureKind::RefreshFailed));
    assert_eq!(record.eligibility, AuthProfileEligibility::CoolingDown);
    assert_eq!(record.token_expiry_state, AuthTokenExpiryState::Expired);
    assert!(record.cooldown_until_unix_ms.is_some());

    let serialized = fs::read_to_string(tempdir.path().join("auth_profile_runtime_state.toml"))
        .expect("runtime state should be readable");
    assert!(!serialized.contains("access-top-secret"));
    assert!(!serialized.contains("refresh-top-secret"));
    assert!(!serialized.contains("client-top-secret"));
}

#[test]
fn selector_respects_explicit_order_cooldown_and_least_recently_used() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/openai_a", "key-a").expect("key-a should persist");
    persist_secret_utf8(&vault, "global/openai_b", "key-b").expect("key-b should persist");
    for (profile_id, vault_ref) in
        [("openai-a", "global/openai_a"), ("openai-b", "global/openai_b")]
    {
        registry
            .set_profile(AuthProfileSetRequest {
                profile_id: profile_id.to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: profile_id.to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey { api_key_vault_ref: vault_ref.to_owned() },
            })
            .expect("api key profile should persist");
    }
    let now = 1_730_000_000_000_i64;
    registry
        .record_profile_success_with_clock("openai-a", now.saturating_sub(10_000))
        .expect("success state should persist");

    let lru = registry
        .select_auth_profile_with_clock(
            &vault,
            AuthProfileSelectionRequest {
                provider: Some(AuthProvider::known(AuthProviderKind::Openai)),
                agent_id: None,
                explicit_profile_order: Vec::new(),
                allowed_credential_types: vec![AuthCredentialType::ApiKey],
                policy_denied_profile_ids: Vec::new(),
            },
            now,
        )
        .expect("selector should run");
    assert_eq!(lru.selected_profile_id.as_deref(), Some("openai-b"));

    let explicit = registry
        .select_auth_profile_with_clock(
            &vault,
            AuthProfileSelectionRequest {
                provider: Some(AuthProvider::known(AuthProviderKind::Openai)),
                agent_id: None,
                explicit_profile_order: vec!["openai-a".to_owned(), "openai-b".to_owned()],
                allowed_credential_types: vec![AuthCredentialType::ApiKey],
                policy_denied_profile_ids: Vec::new(),
            },
            now,
        )
        .expect("selector should honor explicit order");
    assert_eq!(explicit.selected_profile_id.as_deref(), Some("openai-a"));

    registry
        .record_profile_failure_with_clock("openai-a", AuthProfileFailureKind::RateLimit, now)
        .expect("cooldown state should persist");
    let failover = registry
        .select_auth_profile_with_clock(
            &vault,
            AuthProfileSelectionRequest {
                provider: Some(AuthProvider::known(AuthProviderKind::Openai)),
                agent_id: None,
                explicit_profile_order: vec!["openai-a".to_owned(), "openai-b".to_owned()],
                allowed_credential_types: vec![AuthCredentialType::ApiKey],
                policy_denied_profile_ids: Vec::new(),
            },
            now.saturating_add(1),
        )
        .expect("selector should skip cooldown profile");
    assert_eq!(failover.selected_profile_id.as_deref(), Some("openai-b"));
    assert_eq!(failover.candidates[0].profile_id, "openai-a");
    assert_eq!(failover.candidates[0].reason_code, "cooldown_active");
}

#[test]
fn persisted_profile_order_drives_selector_without_explicit_order() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/openai_a", "key-a").expect("key-a should persist");
    persist_secret_utf8(&vault, "global/openai_b", "key-b").expect("key-b should persist");
    for (profile_id, vault_ref) in
        [("openai-a", "global/openai_a"), ("openai-b", "global/openai_b")]
    {
        registry
            .set_profile(AuthProfileSetRequest {
                profile_id: profile_id.to_owned(),
                provider: AuthProvider::known(AuthProviderKind::Openai),
                profile_name: profile_id.to_owned(),
                scope: AuthProfileScope::Global,
                credential: AuthCredential::ApiKey { api_key_vault_ref: vault_ref.to_owned() },
            })
            .expect("api key profile should persist");
    }

    let order = registry
        .set_profile_order_with_clock(
            Some(AuthProvider::known(AuthProviderKind::Openai)),
            None,
            vec!["openai-a".to_owned(), "openai-b".to_owned()],
            1_730_000_000_000,
        )
        .expect("profile order should persist");
    assert_eq!(order.profile_ids, vec!["openai-a", "openai-b"]);

    let selected = registry
        .select_auth_profile_with_clock(
            &vault,
            AuthProfileSelectionRequest {
                provider: Some(AuthProvider::known(AuthProviderKind::Openai)),
                agent_id: None,
                explicit_profile_order: Vec::new(),
                allowed_credential_types: vec![AuthCredentialType::ApiKey],
                policy_denied_profile_ids: Vec::new(),
            },
            1_730_000_000_001,
        )
        .expect("selector should use persisted profile order");
    assert_eq!(selected.selected_profile_id.as_deref(), Some("openai-a"));

    let reopened =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should reopen");
    let persisted_order = reopened
        .profile_order(Some(&AuthProvider::known(AuthProviderKind::Openai)), None)
        .expect("profile order lookup should succeed")
        .expect("profile order should exist");
    assert_eq!(persisted_order.profile_ids, vec!["openai-a", "openai-b"]);
}

#[test]
fn readonly_runtime_records_do_not_persist_audit_materialization() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let identity_root = tempdir.path().join("identity");
    let vault_root = tempdir.path().join("vault");
    let registry =
        AuthProfileRegistry::open(identity_root.as_path()).expect("registry should initialize");
    let vault = open_test_vault(vault_root.as_path(), identity_root.as_path());
    persist_secret_utf8(&vault, "global/openai_audit", "key-audit").expect("key should persist");
    registry
        .set_profile(AuthProfileSetRequest {
            profile_id: "openai-audit".to_owned(),
            provider: AuthProvider::known(AuthProviderKind::Openai),
            profile_name: "audit".to_owned(),
            scope: AuthProfileScope::Global,
            credential: AuthCredential::ApiKey {
                api_key_vault_ref: "global/openai_audit".to_owned(),
            },
        })
        .expect("api key profile should persist");
    let runtime_path = tempdir.path().join("auth_profile_runtime_state.toml");
    let before = fs::read_to_string(runtime_path.as_path())
        .expect("runtime state should be initialized by registry open");

    let readonly_records = registry
        .runtime_records_for_agent_readonly_with_clock(&vault, None, 1_730_000_000_000, 15 * 60_000)
        .expect("readonly runtime records should load");
    assert_eq!(readonly_records.len(), 1);
    assert_eq!(readonly_records[0].eligibility, AuthProfileEligibility::Eligible);
    let after_readonly = fs::read_to_string(runtime_path.as_path())
        .expect("runtime state should still be readable after readonly load");
    assert_eq!(
        after_readonly, before,
        "readonly audit materialization must not mutate runtime state persistence"
    );

    registry
        .runtime_records_for_agent_with_clock(&vault, None, 1_730_000_000_001, 15 * 60_000)
        .expect("runtime record materialization should persist");
    let after_materialized = fs::read_to_string(runtime_path.as_path())
        .expect("runtime state should be readable after materialization");
    assert_ne!(
        after_materialized, before,
        "non-readonly materialization should still persist health records"
    );
    assert!(after_materialized.contains("openai-audit"));
}
