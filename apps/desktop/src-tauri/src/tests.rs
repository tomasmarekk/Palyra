    use std::{
        ffi::OsString,
        io::{Read, Write},
        net::TcpListener,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, OnceLock,
        },
        time::Duration,
    };

    use serde_json::json;

    use super::{
        build_snapshot_from_inputs, collect_redacted_errors, compute_backoff_ms,
        executable_file_name, load_or_initialize_state_file, mpsc,
        parse_discord_status, parse_remote_dashboard_base_url, resolve_binary_path,
        resolve_dashboard_access_target, sanitize_log_line, try_enqueue_log_event,
        BrowserStatusSnapshot, Client, ControlCenter, DashboardAccessMode, DesktopSecretStore,
        DesktopStateFile, LogEvent, LogStream, ManagedService, RuntimeConfig, ServiceKind, Ulid,
        LOG_EVENT_CHANNEL_CAPACITY,
    };
    use super::commands::initialize_control_center;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: tests serialize environment mutations with env_lock().
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                // SAFETY: tests serialize environment mutations with env_lock().
                unsafe {
                    std::env::set_var(self.key, previous);
                }
            } else {
                // SAFETY: tests serialize environment mutations with env_lock().
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    struct ScopedCurrentDir {
        previous: PathBuf,
    }

    impl ScopedCurrentDir {
        fn set(path: &Path) -> Self {
            let previous = std::env::current_dir().expect("current directory should resolve");
            std::env::set_current_dir(path).expect("current directory should be set");
            Self { previous }
        }
    }

    impl Drop for ScopedCurrentDir {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(self.previous.as_path());
        }
    }

    struct TempFixtureDir {
        root: PathBuf,
    }

    impl TempFixtureDir {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("palyra-desktop-fixture-{}", Ulid::new()));
            std::fs::create_dir_all(root.as_path()).expect("fixture directory should be created");
            Self { root }
        }

        fn path(&self) -> &Path {
            self.root.as_path()
        }
    }

    impl Drop for TempFixtureDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.root.as_path());
        }
    }

    fn write_config_file(root: &Path, content: &str) -> PathBuf {
        let path = root.join("palyra.toml");
        std::fs::write(path.as_path(), content).expect("fixture config should be written");
        path
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
        }
        std::fs::write(path, content).expect("fixture file should be written");
    }

    fn build_test_control_center(root: &Path) -> ControlCenter {
        let runtime_root = root.join("runtime");
        let support_bundle_dir = root.join("support-bundles");
        std::fs::create_dir_all(runtime_root.as_path())
            .expect("runtime root should be created for test control center");
        std::fs::create_dir_all(support_bundle_dir.as_path())
            .expect("support bundle directory should be created for test control center");
        let runtime = RuntimeConfig::default();
        let gateway = ManagedService::new(vec![
            runtime.gateway_admin_port,
            runtime.gateway_grpc_port,
            runtime.gateway_quic_port,
        ]);
        let browserd = ManagedService::new(vec![runtime.browser_health_port, runtime.browser_grpc_port]);
        let http_client = Client::builder()
            .cookie_store(true)
            .timeout(Duration::from_secs(4))
            .build()
            .expect("HTTP client should initialize for test control center");
        let (log_tx, log_rx) = mpsc::channel(LOG_EVENT_CHANNEL_CAPACITY);
        ControlCenter {
            runtime_root,
            support_bundle_dir,
            state_file_path: root.join("state.json"),
            persisted: DesktopStateFile::new_default(),
            admin_token: format!("test-admin-{}", Ulid::new()),
            browser_auth_token: format!("test-browser-{}", Ulid::new()),
            runtime,
            gateway,
            browserd,
            http_client,
            log_tx,
            log_rx,
            dropped_log_events: Arc::new(AtomicU64::new(0)),
        }
    }

    #[test]
    fn backoff_uses_exponential_growth_with_cap() {
        assert_eq!(compute_backoff_ms(0), 1_000);
        assert_eq!(compute_backoff_ms(1), 2_000);
        assert_eq!(compute_backoff_ms(2), 4_000);
        assert_eq!(compute_backoff_ms(5), 30_000);
        assert_eq!(compute_backoff_ms(9), 30_000);
    }

    #[test]
    fn diagnostics_error_collection_deduplicates_and_respects_limit() {
        let payload = json!({
            "errors": ["auth token=abcdef", "auth token=abcdef", "network timeout"],
            "details": {
                "failure_reason": "Bearer top-secret"
            }
        });
        let collected = collect_redacted_errors(&payload, 2);
        assert_eq!(collected.len(), 2);
        assert!(collected.iter().all(|entry| !entry.contains("abcdef")));
        assert!(collected.iter().all(|entry| !entry.contains("top-secret")));
    }

    #[test]
    fn sanitize_log_line_redacts_sensitive_assignments_and_url_query_tokens() {
        let sanitized = sanitize_log_line(
            "failed auth authorization=very-secret url=https://local.test/cb?token=abc&mode=ok",
        );
        assert!(!sanitized.contains("very-secret"));
        assert!(!sanitized.contains("token=abc"));
        assert!(sanitized.contains("token=<redacted>"));
        assert!(sanitized.contains("mode=ok"));
    }

    #[test]
    fn discord_snapshot_uses_runtime_error_fallback() {
        let payload = json!({
            "connector": {
                "connector_id": "discord:default",
                "enabled": true,
                "readiness": "auth_failed",
                "liveness": "running"
            },
            "runtime": {
                "last_error": "authorization=super-secret"
            }
        });
        let snapshot = parse_discord_status(Some(&payload));
        assert!(snapshot.enabled);
        assert!(!snapshot.authenticated);
        assert_eq!(snapshot.readiness, "auth_failed");
        assert!(snapshot.last_error.is_some());
        assert!(!snapshot.last_error.unwrap_or_default().contains("super-secret"));
    }

    #[test]
    fn browser_disabled_status_is_treated_as_healthy_for_overall_checks() {
        let snapshot = BrowserStatusSnapshot {
            enabled: false,
            healthy: true,
            status: "disabled".to_owned(),
            uptime_seconds: None,
            last_error: None,
        };
        assert!(!snapshot.enabled);
        assert!(snapshot.healthy);
    }

    #[test]
    fn remote_dashboard_url_parser_accepts_https_without_sensitive_parts() {
        let parsed = parse_remote_dashboard_base_url(
            "https://dashboard.example.com/path",
            "gateway_access.remote_base_url",
        )
        .expect("https remote URL should be accepted");
        assert_eq!(parsed, "https://dashboard.example.com/path");
    }

    #[test]
    fn remote_dashboard_url_parser_rejects_non_https_and_credentials() {
        let non_https = parse_remote_dashboard_base_url(
            "http://dashboard.example.com",
            "gateway_access.remote_base_url",
        )
        .expect_err("non-https URL must be rejected");
        assert!(non_https.to_string().contains("must use https://"));

        let credentials = parse_remote_dashboard_base_url(
            "https://user:pass@dashboard.example.com",
            "gateway_access.remote_base_url",
        )
        .expect_err("URL with embedded credentials must be rejected");
        assert!(credentials.to_string().contains("must not include embedded credentials"));
    }

    #[test]
    fn dashboard_access_target_prefers_remote_url_when_configured() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let fixture = TempFixtureDir::new();
        let config_path = write_config_file(
            fixture.path(),
            r#"
version = 1
[gateway_access]
remote_base_url = "https://dashboard.example.com/"
"#,
        );
        let _config_var =
            ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
        let target = resolve_dashboard_access_target(7142)
            .expect("dashboard access target should resolve from configured remote URL");
        assert_eq!(target.url, "https://dashboard.example.com/");
        assert_eq!(target.mode, DashboardAccessMode::Remote);
    }

    #[test]
    fn dashboard_access_target_uses_local_daemon_bind_when_remote_url_is_missing() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let fixture = TempFixtureDir::new();
        let config_path = write_config_file(
            fixture.path(),
            r#"
version = 1
[daemon]
bind_addr = "0.0.0.0"
port = 9911
"#,
        );
        let _config_var =
            ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
        let target = resolve_dashboard_access_target(7142)
            .expect("dashboard access target should resolve from daemon bind");
        assert_eq!(target.url, "http://127.0.0.1:9911/");
        assert_eq!(target.mode, DashboardAccessMode::Local);
    }

    #[test]
    fn state_file_migration_moves_plaintext_tokens_to_secret_store() {
        let fixture = TempFixtureDir::new();
        let state_path = fixture.path().join("state.json");
        let legacy_admin_token = format!("legacy-admin-{}", Ulid::new());
        let legacy_browser_token = format!("legacy-browser-{}", Ulid::new());
        let legacy = json!({
            "schema_version": 1_u32,
            "admin_token": legacy_admin_token,
            "browser_auth_token": legacy_browser_token,
            "browser_service_enabled": false,
        });
        write_file(
            state_path.as_path(),
            serde_json::to_string_pretty(&legacy)
                .expect("legacy desktop state fixture should serialize")
                .as_str(),
        );

        let secret_store =
            DesktopSecretStore::open(fixture.path()).expect("secret store should initialize");
        let loaded = load_or_initialize_state_file(state_path.as_path(), &secret_store)
            .expect("legacy desktop state should migrate");
        assert_eq!(
            loaded.admin_token,
            legacy["admin_token"]
                .as_str()
                .expect("legacy admin token fixture should be string")
        );
        assert_eq!(
            loaded.browser_auth_token,
            legacy["browser_auth_token"]
                .as_str()
                .expect("legacy browser token fixture should be string")
        );
        assert!(!loaded.persisted.browser_service_enabled);

        let rewritten = std::fs::read_to_string(state_path.as_path())
            .expect("rewritten desktop state should be readable");
        assert!(!rewritten.contains(legacy["admin_token"].as_str().unwrap_or_default()));
        assert!(!rewritten.contains(legacy["browser_auth_token"].as_str().unwrap_or_default()));

        let persisted_json: serde_json::Value =
            serde_json::from_str(rewritten.as_str()).expect("rewritten state should parse");
        assert!(persisted_json.get("admin_token").is_none());
        assert!(persisted_json.get("browser_auth_token").is_none());
        assert_eq!(persisted_json["browser_service_enabled"], json!(false));

        let loaded_again = load_or_initialize_state_file(state_path.as_path(), &secret_store)
            .expect("migrated desktop state should load from secret store");
        assert_eq!(loaded_again.admin_token, loaded.admin_token);
        assert_eq!(loaded_again.browser_auth_token, loaded.browser_auth_token);
    }

    #[test]
    fn state_file_initialization_never_writes_plaintext_tokens() {
        let fixture = TempFixtureDir::new();
        let state_path = fixture.path().join("state.json");
        let secret_store =
            DesktopSecretStore::open(fixture.path()).expect("secret store should initialize");
        let loaded = load_or_initialize_state_file(state_path.as_path(), &secret_store)
            .expect("desktop state should initialize");
        let persisted_raw =
            std::fs::read_to_string(state_path.as_path()).expect("desktop state should be readable");
        assert!(!persisted_raw.contains(loaded.admin_token.as_str()));
        assert!(!persisted_raw.contains(loaded.browser_auth_token.as_str()));
        assert!(!persisted_raw.contains("admin_token"));
        assert!(!persisted_raw.contains("browser_auth_token"));
    }

    #[test]
    fn resolve_binary_path_accepts_absolute_env_override_file() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let fixture = TempFixtureDir::new();
        let binary_name = "palyra-resolver-override";
        let binary_path = fixture.path().join(executable_file_name(binary_name));
        write_file(binary_path.as_path(), "binary");

        let override_path = binary_path.to_string_lossy().into_owned();
        let _override = ScopedEnvVar::set("PALYRA_TEST_RESOLVE_BIN", override_path.as_str());
        let resolved = resolve_binary_path(binary_name, "PALYRA_TEST_RESOLVE_BIN")
            .expect("absolute env override should be accepted");
        let expected = std::fs::canonicalize(binary_path.as_path())
            .expect("canonicalized override path should resolve");
        assert_eq!(resolved, expected);
    }

    #[test]
    fn resolve_binary_path_rejects_relative_env_override() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let _override = ScopedEnvVar::set("PALYRA_TEST_RESOLVE_BIN", "relative/path/to/palyrad");
        let error = resolve_binary_path("palyrad", "PALYRA_TEST_RESOLVE_BIN")
            .expect_err("relative env override must be rejected");
        assert!(error.to_string().contains("must be an absolute path"));
    }

    #[test]
    fn resolve_binary_path_rejects_cwd_target_fallback() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let fixture = TempFixtureDir::new();
        let binary_name = format!("palyra-cwd-{}", Ulid::new());
        let binary_path = fixture
            .path()
            .join("target")
            .join("debug")
            .join(executable_file_name(binary_name.as_str()));
        write_file(binary_path.as_path(), "binary");
        let _cwd = ScopedCurrentDir::set(fixture.path());
        let _override = ScopedEnvVar::set("PALYRA_TEST_RESOLVE_BIN", "");

        let error = resolve_binary_path(binary_name.as_str(), "PALYRA_TEST_RESOLVE_BIN")
            .expect_err("cwd fallback should be rejected");
        assert!(error.to_string().contains("unable to locate"));
    }

    #[test]
    fn resolve_binary_path_rejects_path_only_binary() {
        let _env_guard = env_lock().lock().expect("env lock should be available");
        let fixture = TempFixtureDir::new();
        let binary_name = format!("palyra-path-{}", Ulid::new());
        let binary_path = fixture.path().join(executable_file_name(binary_name.as_str()));
        write_file(binary_path.as_path(), "binary");
        let path_value = fixture.path().to_string_lossy().into_owned();
        let _path = ScopedEnvVar::set("PATH", path_value.as_str());
        let _override = ScopedEnvVar::set("PALYRA_TEST_RESOLVE_BIN", "");

        let error = resolve_binary_path(binary_name.as_str(), "PALYRA_TEST_RESOLVE_BIN")
            .expect_err("PATH fallback should be rejected");
        assert!(error.to_string().contains("unable to locate"));
    }

    #[test]
    fn try_enqueue_log_event_tracks_drops_when_queue_is_full() {
        let (sender, mut receiver) = mpsc::channel(1);
        let dropped_counter = AtomicU64::new(0);
        let first = LogEvent {
            unix_ms: 1,
            service: ServiceKind::Gateway,
            stream: LogStream::Stdout,
            line: "first".to_owned(),
        };
        assert!(try_enqueue_log_event(&sender, &dropped_counter, first));
        assert_eq!(dropped_counter.load(Ordering::Relaxed), 0);

        let second = LogEvent {
            unix_ms: 2,
            service: ServiceKind::Gateway,
            stream: LogStream::Stdout,
            line: "second".to_owned(),
        };
        assert!(
            try_enqueue_log_event(&sender, &dropped_counter, second),
            "enqueue helper should keep producer alive when queue is full"
        );
        assert_eq!(dropped_counter.load(Ordering::Relaxed), 1);

        let drained = receiver.try_recv().expect("first event should still be queued");
        assert_eq!(drained.line, "first");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_diagnostics_surface_dropped_log_event_count() {
        let fixture = TempFixtureDir::new();
        let mut control_center = build_test_control_center(fixture.path());
        control_center.persisted.browser_service_enabled = false;
        control_center.dropped_log_events.store(7, Ordering::Relaxed);

        let inputs = control_center.capture_snapshot_inputs();
        assert_eq!(inputs.dropped_log_events_total, 7);
        let snapshot =
            build_snapshot_from_inputs(inputs).await.expect("snapshot should build with dropped logs");
        assert_eq!(snapshot.diagnostics.dropped_log_events_total, 7);
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning.contains("dropped 7 log event")),
            "snapshot warnings should surface queue overflow summary"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_build_reuses_console_session_until_expiry() {
        fn write_http_response(
            stream: &mut std::net::TcpStream,
            status_line: &str,
            body: &str,
            extra_headers: &[&str],
        ) {
            let mut response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for header in extra_headers {
                response.push_str(header);
                response.push_str("\r\n");
            }
            response.push_str("\r\n");
            response.push_str(body);
            stream.write_all(response.as_bytes()).expect("response should be written");
            stream.flush().expect("response should be flushed");
        }

        let fixture = TempFixtureDir::new();
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let port = listener.local_addr().expect("listener address should resolve").port();
        let login_requests = Arc::new(AtomicU64::new(0));
        let session_requests = Arc::new(AtomicU64::new(0));
        let login_requests_server = Arc::clone(&login_requests);
        let session_requests_server = Arc::clone(&session_requests);
        let server = std::thread::spawn(move || {
            for _ in 0..8 {
                let (mut stream, _) = listener.accept().expect("listener should accept request");
                let mut buffer = [0_u8; 4096];
                let read = stream.read(&mut buffer).expect("request should be readable");
                let request = String::from_utf8_lossy(&buffer[..read]);
                let request_line = request.lines().next().unwrap_or_default().to_owned();
                let has_cookie = request.contains("palyra_console_session=session-1");

                if request_line.starts_with("GET /health ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"status":"ok","version":"test","git_hash":"hash","uptime_seconds":1}"#,
                        &[],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/auth/session ") {
                    let prior_attempts = session_requests_server.fetch_add(1, Ordering::Relaxed);
                    if prior_attempts == 0 {
                        write_http_response(
                            &mut stream,
                            "403 Forbidden",
                            r#"{"error":"console session cookie is missing"}"#,
                            &[],
                        );
                    } else {
                        assert!(
                            has_cookie,
                            "session reuse request should include cached console session cookie"
                        );
                        write_http_response(
                            &mut stream,
                            "200 OK",
                            r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                            &[],
                        );
                    }
                    continue;
                }
                if request_line.starts_with("POST /console/v1/auth/login ") {
                    login_requests_server.fetch_add(1, Ordering::Relaxed);
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                        &["Set-Cookie: palyra_console_session=session-1; Path=/; HttpOnly; SameSite=Strict"],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/diagnostics ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"generated_at_unix_ms":123,"errors":[]}"#,
                        &[],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"connector":{"connector_id":"discord:default","enabled":true,"readiness":"ready","liveness":"running"}}"#,
                        &[],
                    );
                    continue;
                }
                panic!("unexpected desktop snapshot request: {request_line}");
            }
        });

        let mut control_center = build_test_control_center(fixture.path());
        control_center.runtime.gateway_admin_port = port;
        control_center.persisted.browser_service_enabled = false;

        let first_inputs = control_center.capture_snapshot_inputs();
        build_snapshot_from_inputs(first_inputs)
            .await
            .expect("first snapshot should bootstrap console session");

        let second_inputs = control_center.capture_snapshot_inputs();
        build_snapshot_from_inputs(second_inputs)
            .await
            .expect("second snapshot should reuse cached console session");

        assert_eq!(
            login_requests.load(Ordering::Relaxed),
            1,
            "desktop snapshot polling should not re-login when the console session is still valid"
        );
        assert_eq!(
            session_requests.load(Ordering::Relaxed),
            2,
            "desktop snapshot polling should probe existing console session before deciding to log in"
        );
        server.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_build_redacts_console_and_connector_diagnostics() {
        fn write_http_response(
            stream: &mut std::net::TcpStream,
            status_line: &str,
            body: &str,
            extra_headers: &[&str],
        ) {
            let mut response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for header in extra_headers {
                response.push_str(header);
                response.push_str("\r\n");
            }
            response.push_str("\r\n");
            response.push_str(body);
            stream.write_all(response.as_bytes()).expect("response should be written");
            stream.flush().expect("response should be flushed");
        }

        let fixture = TempFixtureDir::new();
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let port = listener.local_addr().expect("listener address should resolve").port();
        let server = std::thread::spawn(move || {
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().expect("listener should accept request");
                let mut buffer = [0_u8; 4096];
                let read = stream.read(&mut buffer).expect("request should be readable");
                let request = String::from_utf8_lossy(&buffer[..read]);
                let request_line = request.lines().next().unwrap_or_default().to_owned();
                if request_line.starts_with("GET /health ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"status":"ok","version":"test","git_hash":"hash","uptime_seconds":1}"#,
                        &[],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/auth/session ") {
                    write_http_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                    continue;
                }
                if request_line.starts_with("POST /console/v1/auth/login ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                        &["Set-Cookie: palyra_console_session=session-1; Path=/; HttpOnly; SameSite=Strict"],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/diagnostics ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{
                            "generated_at_unix_ms":123,
                            "errors":["provider token=alpha"],
                            "browserd":{"last_error":"Bearer browser-secret"},
                            "auth_profiles":{"profiles":[{"refresh_failure_reason":"refresh_token=beta"}]}
                        }"#,
                        &[],
                    );
                    continue;
                }
                if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        r#"{
                            "connector":{"connector_id":"discord:default","enabled":true,"readiness":"degraded","liveness":"running"},
                            "runtime":{"last_error":"discord send failed authorization=discord-secret url=https://discord.test/api/webhooks/1?token=hook-secret&mode=ok"}
                        }"#,
                        &[],
                    );
                    continue;
                }
                panic!("unexpected desktop snapshot request: {request_line}");
            }
        });

        let mut control_center = build_test_control_center(fixture.path());
        control_center.runtime.gateway_admin_port = port;
        control_center.persisted.browser_service_enabled = false;

        let snapshot = build_snapshot_from_inputs(control_center.capture_snapshot_inputs())
            .await
            .expect("snapshot should build");
        assert!(
            snapshot
                .diagnostics
                .errors
                .iter()
                .any(|entry| entry.contains("<redacted>")),
            "desktop diagnostics should preserve redaction markers"
        );
        assert!(
            snapshot
                .diagnostics
                .errors
                .iter()
                .all(|entry| {
                    !entry.contains("alpha")
                        && !entry.contains("browser-secret")
                        && !entry.contains("beta")
                }),
            "desktop diagnostics must not leak raw secret values: {:?}",
            snapshot.diagnostics.errors
        );
        let discord_error = snapshot.quick_facts.discord.last_error.unwrap_or_default();
        assert!(
            discord_error.contains("authorization=<redacted>")
                && discord_error.contains("token=<redacted>")
                && !discord_error.contains("discord-secret")
                && !discord_error.contains("hook-secret"),
            "desktop connector snapshot must sanitize connector diagnostics: {discord_error}"
        );
        server.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_build_releases_supervisor_lock_while_waiting_on_http() {
        let fixture = TempFixtureDir::new();

        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let port = listener.local_addr().expect("listener address should resolve").port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 1024];
            let _ = stream.read(&mut buffer);
            std::thread::sleep(Duration::from_millis(600));
            let body = r#"{"status":"ok","version":"test","git_hash":"hash","uptime_seconds":1}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("response should be written");
            stream.flush().expect("response should be flushed");
        });

        let mut control_center = build_test_control_center(fixture.path());
        control_center.runtime.gateway_admin_port = port;
        control_center.persisted.browser_service_enabled = false;
        control_center.gateway.desired_running = true;
        control_center.gateway.next_restart_unix_ms = Some(i64::MAX);
        let supervisor = Arc::new(tokio::sync::Mutex::new(control_center));

        let snapshot_supervisor = supervisor.clone();
        let snapshot_task = tokio::spawn(async move {
            let inputs = {
                let mut guard = snapshot_supervisor.lock().await;
                guard.capture_snapshot_inputs()
            };
            build_snapshot_from_inputs(inputs).await
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        let lock_attempt = tokio::time::timeout(Duration::from_millis(250), supervisor.lock()).await;
        assert!(
            lock_attempt.is_ok(),
            "supervisor lock should remain available while snapshot is awaiting HTTP responses"
        );
        drop(lock_attempt);

        let snapshot = snapshot_task
            .await
            .expect("snapshot task should join")
            .expect("snapshot should build");
        assert_eq!(snapshot.gateway_process.service, "gateway");
        server.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn support_bundle_export_plan_capture_does_not_hold_supervisor_lock() {
        let fixture = TempFixtureDir::new();
        let control_center = build_test_control_center(fixture.path());
        let supervisor = Arc::new(tokio::sync::Mutex::new(control_center));
        let export_plan = {
            let guard = supervisor.lock().await;
            guard.prepare_support_bundle_export()
        };

        let lock_attempt = tokio::time::timeout(Duration::from_millis(100), supervisor.lock()).await;
        assert!(
            lock_attempt.is_ok(),
            "supervisor lock should remain available immediately after export plan capture"
        );
        assert!(
            export_plan
                .runtime_root
                .ends_with(Path::new("runtime")),
            "export plan should retain desktop runtime root from supervisor state"
        );
    }

    #[test]
    fn initialize_control_center_returns_sanitized_error_without_panicking() {
        let error = initialize_control_center(|| {
            Err(anyhow::anyhow!(
                "state initialization failed: admin_token=super-secret"
            ))
        })
        .expect_err("initialization helper should surface errors without panicking");
        assert!(
            error.contains("desktop initialization failed"),
            "initialization helper should prepend actionable startup context"
        );
        assert!(
            !error.contains("super-secret"),
            "initialization helper should sanitize sensitive values before reporting"
        );
    }
