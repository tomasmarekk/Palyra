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

use super::commands::initialize_control_center;
use super::discord_onboarding::{
    apply_discord_onboarding, run_discord_onboarding_preflight, verify_discord_connector,
    DiscordControlPlaneInputs, DiscordOnboardingRequest, DiscordVerificationRequest,
};
use super::openai_auth::{
    connect_openai_api_key, get_openai_oauth_callback_state, load_openai_auth_status,
    open_external_browser, reconnect_openai_oauth, refresh_openai_profile, revoke_openai_profile,
    set_openai_default_profile, start_openai_oauth_bootstrap, OpenAiApiKeyConnectRequest,
    OpenAiControlPlaneInputs, OpenAiOAuthBootstrapRequest, OpenAiOAuthCallbackStateRequest,
    OpenAiProfileActionRequest, OpenAiScopeInput,
};
use super::snapshot::resolve_dashboard_access_target;
use super::{
    build_onboarding_status, build_snapshot_from_inputs, collect_redacted_errors,
    compute_backoff_ms, executable_file_name, load_or_initialize_state_file, mpsc,
    parse_discord_status, parse_remote_dashboard_base_url, resolve_binary_path, sanitize_log_line,
    try_enqueue_log_event, BrowserStatusSnapshot, Client, ControlCenter, DashboardAccessMode,
    DesktopOnboardingStep, DesktopSecretStore, DesktopStateFile, LogEvent, LogStream,
    ManagedService, RuntimeConfig, ServiceKind, Ulid, LOG_EVENT_CHANNEL_CAPACITY,
};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    env_lock().lock().unwrap_or_else(|error| error.into_inner())
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
    let browserd =
        ManagedService::new(vec![runtime.browser_health_port, runtime.browser_grpc_port]);
    let http_client = Client::builder()
        .cookie_store(true)
        .timeout(Duration::from_secs(4))
        .build()
        .expect("HTTP client should initialize for test control center");
    let (log_tx, log_rx) = mpsc::channel(LOG_EVENT_CHANNEL_CAPACITY);
    ControlCenter {
        default_runtime_root: runtime_root.clone(),
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

fn build_test_openai_inputs(root: &Path, port: u16) -> OpenAiControlPlaneInputs {
    let mut control_center = build_test_control_center(root);
    control_center.runtime.gateway_admin_port = port;
    OpenAiControlPlaneInputs::capture(&control_center)
}

fn build_test_discord_inputs(root: &Path, port: u16) -> DiscordControlPlaneInputs {
    let mut control_center = build_test_control_center(root);
    control_center.runtime.gateway_admin_port = port;
    DiscordControlPlaneInputs::capture(&control_center)
}

fn write_json_response(
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
fn discord_snapshot_surfaces_recovery_diagnostics_from_operations_payload() {
    let payload = json!({
        "connector": {
            "connector_id": "discord:default",
            "enabled": true,
            "readiness": "ready",
            "liveness": "running"
        },
        "operations": {
            "queue": {
                "pending_outbox": 4,
                "due_outbox": 2,
                "claimed_outbox": 1,
                "dead_letters": 3,
                "paused": true,
                "pause_reason": "operator token rotation"
            },
            "saturation": {
                "state": "paused"
            },
            "last_auth_failure": "authorization=super-secret",
            "discord": {
                "last_permission_failure": "Missing Send Messages in #ops",
                "health_refresh_hint": "Run health refresh with verify_channel_id"
            }
        },
        "health_refresh": {
            "supported": true,
            "refreshed": false,
            "message": "token=discord-secret",
            "warnings": ["Missing View Channels"]
        }
    });

    let snapshot = parse_discord_status(Some(&payload));

    assert!(snapshot.authenticated);
    assert_eq!(snapshot.saturation_state, "paused");
    assert!(snapshot.queue_paused);
    assert_eq!(snapshot.pending_outbox, 4);
    assert_eq!(snapshot.due_outbox, 2);
    assert_eq!(snapshot.claimed_outbox, 1);
    assert_eq!(snapshot.dead_letters, 3);
    assert_eq!(snapshot.pause_reason.as_deref(), Some("operator token rotation"));
    assert_eq!(snapshot.permission_gap_hint.as_deref(), Some("Missing Send Messages in #ops"));
    assert_eq!(snapshot.health_refresh_status, "degraded");
    assert_eq!(snapshot.health_refresh_warning_count, 1);
    let auth_hint = snapshot.auth_failure_hint.unwrap_or_default();
    assert!(
        auth_hint.contains("authorization=<redacted>") && !auth_hint.contains("super-secret"),
        "auth hint should be redacted: {auth_hint}"
    );
    let health_detail = snapshot.health_refresh_detail.unwrap_or_default();
    assert!(
        health_detail.contains("token=<redacted>") && !health_detail.contains("discord-secret"),
        "health refresh detail should be redacted: {health_detail}"
    );
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
    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let config_path = write_config_file(
        fixture.path(),
        r#"
version = 1
[gateway_access]
remote_base_url = "https://dashboard.example.com/"
"#,
    );
    let _config_var = ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
    let target = resolve_dashboard_access_target(7142)
        .expect("dashboard access target should resolve from configured remote URL");
    assert_eq!(target.url, "https://dashboard.example.com/");
    assert_eq!(target.mode, DashboardAccessMode::Remote);
}

#[test]
fn dashboard_access_target_uses_local_daemon_bind_when_remote_url_is_missing() {
    let _env_guard = lock_env();
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
    let _config_var = ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
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
        legacy["admin_token"].as_str().expect("legacy admin token fixture should be string")
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
fn state_file_initialization_seeds_onboarding_defaults() {
    let fixture = TempFixtureDir::new();
    let state_path = fixture.path().join("state.json");
    let secret_store =
        DesktopSecretStore::open(fixture.path()).expect("secret store should initialize");
    let loaded = load_or_initialize_state_file(state_path.as_path(), &secret_store)
        .expect("desktop state should initialize");
    assert!(loaded.persisted.runtime_state_root.is_none());
    assert!(loaded.persisted.onboarding.welcome_acknowledged_at_unix_ms.is_none());
    assert!(!loaded.persisted.onboarding.flow_id.trim().is_empty());
    assert_eq!(loaded.persisted.onboarding.discord.account_id, "default");
    assert_eq!(loaded.persisted.onboarding.discord.broadcast_strategy, "deny");
    assert!(loaded.persisted.onboarding.recent_events.is_empty());
    assert!(loaded.persisted.onboarding.failure_step_counts.is_empty());
    assert_eq!(loaded.persisted.onboarding.support_bundle_export_attempts, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn onboarding_status_advances_to_gateway_init_after_preflight_and_state_root_confirmation() {
    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let config_path = write_config_file(
        fixture.path(),
        r#"
version = 1
"#,
    );
    let gateway_binary = fixture.path().join(executable_file_name("palyrad"));
    let browser_binary = fixture.path().join(executable_file_name("palyra-browserd"));
    let cli_binary = fixture.path().join(executable_file_name("palyra"));
    write_file(gateway_binary.as_path(), "binary");
    write_file(browser_binary.as_path(), "binary");
    write_file(cli_binary.as_path(), "binary");
    let _config_override =
        ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
    let _gateway_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRAD_BIN", gateway_binary.to_string_lossy().as_ref());
    let _browser_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_BROWSERD_BIN", browser_binary.to_string_lossy().as_ref());
    let _cli_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRA_BIN", cli_binary.to_string_lossy().as_ref());

    let mut control_center = build_test_control_center(fixture.path());
    control_center.runtime.gateway_admin_port = 0;
    control_center.runtime.gateway_grpc_port = 0;
    control_center.runtime.gateway_quic_port = 0;
    control_center.runtime.browser_health_port = 0;
    control_center.runtime.browser_grpc_port = 0;
    control_center.gateway.bound_ports = vec![0, 0, 0];
    control_center.browserd.bound_ports = vec![0, 0];
    control_center
        .mark_onboarding_welcome_acknowledged()
        .expect("welcome acknowledgement should persist");
    control_center
        .set_runtime_state_root_override(None, true)
        .expect("state root confirmation should persist");

    let status = build_onboarding_status(control_center.capture_onboarding_status_inputs())
        .await
        .expect("onboarding status should build");
    assert_eq!(status.current_step, DesktopOnboardingStep::GatewayInit);
    assert_eq!(status.preflight.blocked_count, 0);
    assert!(status.state_root_confirmed);
    assert_eq!(status.phase, "onboarding");
    assert!(
        status
            .steps
            .iter()
            .any(|step| step.key == DesktopOnboardingStep::Environment && step.status == "complete"),
        "environment step should be marked complete once preflight passes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn onboarding_status_surfaces_flow_id_failure_counts_and_bundle_metrics() {
    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let config_path = write_config_file(
        fixture.path(),
        r#"
version = 1
"#,
    );
    let gateway_binary = fixture.path().join(executable_file_name("palyrad"));
    let browser_binary = fixture.path().join(executable_file_name("palyra-browserd"));
    let cli_binary = fixture.path().join(executable_file_name("palyra"));
    write_file(gateway_binary.as_path(), "binary");
    write_file(browser_binary.as_path(), "binary");
    write_file(cli_binary.as_path(), "binary");
    let _config_override =
        ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
    let _gateway_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRAD_BIN", gateway_binary.to_string_lossy().as_ref());
    let _browser_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_BROWSERD_BIN", browser_binary.to_string_lossy().as_ref());
    let _cli_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRA_BIN", cli_binary.to_string_lossy().as_ref());

    let mut control_center = build_test_control_center(fixture.path());
    control_center.runtime.gateway_admin_port = 0;
    control_center.runtime.gateway_grpc_port = 0;
    control_center.runtime.gateway_quic_port = 0;
    control_center.runtime.browser_health_port = 0;
    control_center.runtime.browser_grpc_port = 0;
    control_center.gateway.bound_ports = vec![0, 0, 0];
    control_center.browserd.bound_ports = vec![0, 0];
    control_center
        .mark_onboarding_welcome_acknowledged()
        .expect("welcome acknowledgement should persist");
    control_center
        .set_runtime_state_root_override(None, true)
        .expect("state root confirmation should persist");
    control_center
        .record_onboarding_failure(
            DesktopOnboardingStep::Environment,
            "Missing gateway binary".to_owned(),
        )
        .expect("first onboarding failure should persist");
    control_center
        .record_onboarding_failure(
            DesktopOnboardingStep::Environment,
            "Missing gateway binary".to_owned(),
        )
        .expect("second onboarding failure should persist");
    control_center
        .record_support_bundle_export_result(true, Some("bundle-one.json".to_owned()))
        .expect("bundle success should persist");
    control_center
        .record_support_bundle_export_result(false, Some("bundle failed".to_owned()))
        .expect("bundle failure should persist");

    let status = build_onboarding_status(control_center.capture_onboarding_status_inputs())
        .await
        .expect("onboarding status should build");
    assert!(!status.flow_id.trim().is_empty());
    assert_eq!(
        status
            .failure_step_counts
            .iter()
            .find(|entry| entry.step == DesktopOnboardingStep::Environment.as_str())
            .map(|entry| entry.failures),
        Some(2)
    );
    assert_eq!(status.support_bundle_exports.attempts, 2);
    assert_eq!(status.support_bundle_exports.successes, 1);
    assert_eq!(status.support_bundle_exports.failures, 1);
    assert_eq!(status.support_bundle_exports.success_rate_bps, 5_000);
}

#[test]
fn resolve_binary_path_accepts_absolute_env_override_file() {
    let _env_guard = lock_env();
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
    let _env_guard = lock_env();
    let _override = ScopedEnvVar::set("PALYRA_TEST_RESOLVE_BIN", "relative/path/to/palyrad");
    let error = resolve_binary_path("palyrad", "PALYRA_TEST_RESOLVE_BIN")
        .expect_err("relative env override must be rejected");
    assert!(error.to_string().contains("must be an absolute path"));
}

#[test]
fn resolve_binary_path_rejects_cwd_target_fallback() {
    let _env_guard = lock_env();
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
    let _env_guard = lock_env();
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
        snapshot.warnings.iter().any(|warning| warning.contains("dropped 7 log event")),
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
        for _ in 0..9 {
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
                            "observability":{
                                "provider_auth":{
                                    "state":"degraded",
                                    "attempts":8,
                                    "failures":2,
                                    "failure_rate_bps":2500,
                                    "refresh_failures":1
                                },
                                "dashboard":{
                                    "attempts":12,
                                    "failures":2,
                                    "failure_rate_bps":1666
                                },
                                "connector":{
                                    "queue_depth":6,
                                    "dead_letters":3,
                                    "degraded_connectors":1,
                                    "upload_failures":1,
                                    "upload_failure_rate_bps":10000
                                },
                                "browser":{
                                    "relay_actions":{
                                        "attempts":5,
                                        "failures":1,
                                        "failure_rate_bps":2000
                                    }
                                },
                                "support_bundle":{
                                    "attempts":4,
                                    "successes":3,
                                    "failures":1,
                                    "success_rate_bps":7500
                                },
                                "failure_classes":{
                                    "config_failure":1,
                                    "upstream_provider_failure":2,
                                    "product_failure":1
                                },
                                "recent_failures":[
                                    {
                                        "operation":"provider_auth_refresh",
                                        "failure_class":"upstream_provider_failure",
                                        "message":"provider auth request failed with http 502"
                                    }
                                ]
                            },
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
                            "runtime":{"last_error":"discord send failed authorization=discord-secret url=https://discord.test/api/webhooks/1?token=hook-secret&mode=ok"},
                            "operations":{
                                "queue":{
                                    "pending_outbox":5,
                                    "due_outbox":2,
                                    "claimed_outbox":1,
                                    "dead_letters":3,
                                    "paused":true,
                                    "pause_reason":"operator token rotation"
                                },
                                "saturation":{"state":"paused"},
                                "last_auth_failure":"authorization=queue-secret",
                                "discord":{
                                    "last_permission_failure":"Missing Send Messages",
                                    "health_refresh_hint":"Run health refresh with verify_channel_id"
                                }
                            },
                            "health_refresh":{
                                "supported":true,
                                "refreshed":false,
                                "message":"token=refresh-secret",
                                "warnings":["Missing View Channels"]
                            }
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
        snapshot.diagnostics.errors.iter().any(|entry| entry.contains("<redacted>")),
        "desktop diagnostics should preserve redaction markers"
    );
    assert!(
        snapshot.diagnostics.errors.iter().all(|entry| {
            !entry.contains("alpha") && !entry.contains("browser-secret") && !entry.contains("beta")
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
    assert!(snapshot.quick_facts.discord.queue_paused);
    assert_eq!(snapshot.quick_facts.discord.pending_outbox, 5);
    assert_eq!(snapshot.quick_facts.discord.dead_letters, 3);
    assert_eq!(snapshot.diagnostics.observability.provider_auth.failures, 2);
    assert_eq!(
        snapshot.diagnostics.observability.provider_auth.failure_rate_bps,
        2_500
    );
    assert_eq!(snapshot.diagnostics.observability.dashboard.failures, 2);
    assert_eq!(snapshot.diagnostics.observability.connector.queue_depth, 6);
    assert_eq!(snapshot.diagnostics.observability.browser.relay_failures, 1);
    assert_eq!(
        snapshot.diagnostics.observability.support_bundle.success_rate_bps,
        7_500
    );
    assert_eq!(
        snapshot
            .diagnostics
            .observability
            .failure_classes
            .upstream_provider_failure,
        2
    );
    assert_eq!(snapshot.diagnostics.observability.recent_failure_count, 1);
    assert_eq!(snapshot.quick_facts.discord.saturation_state, "paused");
    assert_eq!(
        snapshot.quick_facts.discord.permission_gap_hint.as_deref(),
        Some("Missing Send Messages")
    );
    let auth_hint = snapshot.quick_facts.discord.auth_failure_hint.clone().unwrap_or_default();
    assert!(
        auth_hint.contains("authorization=<redacted>") && !auth_hint.contains("queue-secret"),
        "desktop snapshot should redact auth failure hints: {auth_hint}"
    );
    let health_detail =
        snapshot.quick_facts.discord.health_refresh_detail.clone().unwrap_or_default();
    assert_eq!(snapshot.quick_facts.discord.health_refresh_status, "degraded");
    assert_eq!(snapshot.quick_facts.discord.health_refresh_warning_count, 1);
    assert!(
        health_detail.contains("token=<redacted>") && !health_detail.contains("refresh-secret"),
        "desktop snapshot should redact health refresh detail: {health_detail}"
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

    let snapshot =
        snapshot_task.await.expect("snapshot task should join").expect("snapshot should build");
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
        export_plan.runtime_root.ends_with(Path::new("runtime")),
        "export plan should retain desktop runtime root from supervisor state"
    );
}

#[test]
fn initialize_control_center_returns_sanitized_error_without_panicking() {
    let error = initialize_control_center(|| {
        Err(anyhow::anyhow!("state initialization failed: admin_token=super-secret"))
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

#[tokio::test(flavor = "current_thread")]
async fn openai_auth_status_loads_profiles_and_redacts_refresh_state() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            if request_line.starts_with("GET /console/v1/auth/session ") {
                write_json_response(
                    &mut stream,
                    "403 Forbidden",
                    r#"{"error":"console session cookie is missing"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-openai","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-openai; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/auth/providers/openai ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","oauth_supported":true,"bootstrap_supported":true,"callback_supported":true,"reconnect_supported":true,"revoke_supported":true,"default_selection_supported":true,"default_profile_id":"openai-oauth","available_profile_ids":["openai-api","openai-oauth"],"state":"connected","note":"OpenAI auth is ready."}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/auth/health?include_profiles=true ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"summary":{"total":2,"ok":0,"expiring":1,"expired":0,"missing":0,"static_count":1},"expiry_distribution":{},"profiles":[{"profile_id":"openai-api","provider":"openai","profile_name":"OpenAI API","scope":"global","credential_type":"api_key","state":"static","reason":"API key stored in vault.","expires_at_unix_ms":null},{"profile_id":"openai-oauth","provider":"openai","profile_name":"OpenAI OAuth","scope":"agent:assistant","credential_type":"oauth","state":"expiring","reason":"Token expires soon.","expires_at_unix_ms":1700000000000}],"refresh_metrics":{"attempts":3,"successes":1,"failures":2,"by_provider":[{"provider":"openai","attempts":3,"successes":1,"failures":2}]}}"#,
                    &[],
                );
                continue;
            }
            if request_line
                .starts_with("GET /console/v1/auth/profiles?provider_kind=openai&limit=100 ")
            {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"profiles":[{"profile_id":"openai-api","provider":{"kind":"openai"},"profile_name":"OpenAI API","scope":{"kind":"global"},"credential":{"type":"api_key","api_key_vault_ref":"vault://openai/api"},"created_at_unix_ms":11,"updated_at_unix_ms":22},{"profile_id":"openai-oauth","provider":{"kind":"openai"},"profile_name":"OpenAI OAuth","scope":{"kind":"agent","agent_id":"assistant"},"credential":{"type":"oauth","access_token_vault_ref":"vault://openai/access","refresh_token_vault_ref":"vault://openai/refresh","token_endpoint":"https://auth.openai.com/oauth/token","client_id":"desktop-client","client_secret_vault_ref":"vault://openai/client-secret","scopes":["openid","profile","offline_access"],"expires_at_unix_ms":1700000000000,"refresh_state":{"failure_count":2,"last_error":"refresh_token=super-secret","last_attempt_unix_ms":1700000000100,"last_success_unix_ms":1700000000200,"next_allowed_refresh_unix_ms":1700000000300}},"created_at_unix_ms":33,"updated_at_unix_ms":44}],"page":{"limit":100,"returned":2,"next_cursor":null,"has_more":false}}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected OpenAI auth status request: {request_line}");
        }
    });

    let status = load_openai_auth_status(build_test_openai_inputs(fixture.path(), port))
        .await
        .expect("OpenAI auth status should load");
    assert!(status.available);
    assert_eq!(status.default_profile_id.as_deref(), Some("openai-oauth"));
    assert_eq!(status.summary.total, 2);
    assert_eq!(status.refresh_metrics.failures, 2);

    let api_profile = status
        .profiles
        .iter()
        .find(|profile| profile.profile_id == "openai-api")
        .expect("API key profile should be present");
    assert_eq!(api_profile.credential_type, "api_key");
    assert!(api_profile.can_rotate_api_key);
    assert_eq!(api_profile.health_state, "static");

    let oauth_profile = status
        .profiles
        .iter()
        .find(|profile| profile.profile_id == "openai-oauth")
        .expect("OAuth profile should be present");
    assert!(oauth_profile.is_default);
    assert_eq!(oauth_profile.scope_label, "agent:assistant");
    assert_eq!(oauth_profile.health_state, "expiring");
    assert!(oauth_profile.can_reconnect);
    assert_eq!(oauth_profile.refresh_state.as_ref().map(|value| value.failure_count), Some(2));
    let refresh_error = oauth_profile
        .refresh_state
        .as_ref()
        .and_then(|value| value.last_error.as_deref())
        .unwrap_or_default();
    assert!(
        refresh_error.contains("<redacted>") && !refresh_error.contains("super-secret"),
        "OAuth refresh error should stay redacted: {refresh_error}"
    );

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn openai_api_key_connect_bootstraps_console_session_and_posts_payload() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            if request_line.starts_with("GET /console/v1/auth/session ") {
                write_json_response(
                    &mut stream,
                    "403 Forbidden",
                    r#"{"error":"console session cookie is missing"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-api","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-api; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/api-key ") {
                assert!(
                    request.contains("x-palyra-csrf-token: csrf-api"),
                    "API key connect must include CSRF header"
                );
                assert!(request.contains("\"profile_name\":\"Rotated OpenAI\""));
                assert!(request.contains("\"api_key\":\"sk-live-test\""));
                assert!(request.contains("\"kind\":\"agent\""));
                assert!(request.contains("\"agent_id\":\"assistant\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","action":"api_key","state":"connected","message":"OpenAI API key connected.","profile_id":"openai-api"}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected OpenAI API key request: {request_line}");
        }
    });

    let response = connect_openai_api_key(
        build_test_openai_inputs(fixture.path(), port),
        OpenAiApiKeyConnectRequest {
            profile_id: Some("openai-api".to_owned()),
            profile_name: "Rotated OpenAI".to_owned(),
            scope: Some(OpenAiScopeInput {
                kind: "agent".to_owned(),
                agent_id: Some("assistant".to_owned()),
            }),
            api_key: "sk-live-test".to_owned(),
            set_default: true,
        },
    )
    .await
    .expect("OpenAI API key connect should succeed");
    assert_eq!(response.message, "OpenAI API key connected.");
    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn openai_oauth_bootstrap_and_callback_state_reuse_console_session() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..5 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-oauth");
            if request_line.starts_with("GET /console/v1/auth/session ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-oauth","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                        &[],
                    );
                } else {
                    write_json_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                }
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-oauth","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-oauth; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/bootstrap ") {
                assert!(
                    request.contains("x-palyra-csrf-token: csrf-oauth"),
                    "OAuth bootstrap must include CSRF header"
                );
                assert!(request.contains("\"client_id\":\"desktop-client\""));
                assert!(request.contains("\"client_secret\":\"desktop-secret\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","attempt_id":"attempt-1","authorization_url":"https://auth.openai.example/authorize?attempt=1","expires_at_unix_ms":1700000000000,"profile_id":"openai-oauth","message":"OpenAI OAuth authorization URL issued."}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with(
                "GET /console/v1/auth/providers/openai/callback-state?attempt_id=attempt-1 ",
            ) {
                assert!(
                    has_cookie,
                    "callback-state polling should reuse the authenticated console session cookie"
                );
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","attempt_id":"attempt-1","state":"failed","message":"User denied the OpenAI authorization request.","profile_id":"openai-oauth","completed_at_unix_ms":1700000000200,"expires_at_unix_ms":1700000000000}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected OpenAI OAuth request: {request_line}");
        }
    });

    let inputs = build_test_openai_inputs(fixture.path(), port);
    let bootstrap = start_openai_oauth_bootstrap(
        inputs.clone(),
        OpenAiOAuthBootstrapRequest {
            profile_id: None,
            profile_name: Some("OpenAI OAuth".to_owned()),
            scope: Some(OpenAiScopeInput { kind: "global".to_owned(), agent_id: None }),
            client_id: Some("desktop-client".to_owned()),
            client_secret: Some("desktop-secret".to_owned()),
            scopes_text: "openid, profile, offline_access".to_owned(),
            set_default: true,
        },
    )
    .await
    .expect("OpenAI OAuth bootstrap should succeed");
    assert_eq!(bootstrap.attempt_id, "attempt-1");
    assert_eq!(bootstrap.profile_id.as_deref(), Some("openai-oauth"));

    let callback_state = get_openai_oauth_callback_state(
        inputs,
        OpenAiOAuthCallbackStateRequest { attempt_id: "attempt-1".to_owned() },
    )
    .await
    .expect("OpenAI OAuth callback state should load");
    assert!(callback_state.is_terminal);
    assert_eq!(callback_state.state, "failed");
    assert_eq!(callback_state.profile_id.as_deref(), Some("openai-oauth"));

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn openai_profile_actions_hit_expected_routes_including_reconnect() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..9 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-actions");
            if request_line.starts_with("GET /console/v1/auth/session ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-actions","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                        &[],
                    );
                } else {
                    write_json_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                }
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-actions","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-actions; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/refresh ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-actions"));
                assert!(request.contains("\"profile_id\":\"openai-oauth\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","action":"refresh","state":"ok","message":"OpenAI profile refreshed.","profile_id":"openai-oauth"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/default-profile ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-actions"));
                assert!(request.contains("\"profile_id\":\"openai-oauth\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","action":"default_profile","state":"ok","message":"OpenAI default profile updated.","profile_id":"openai-oauth"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/revoke ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-actions"));
                assert!(request.contains("\"profile_id\":\"openai-oauth\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","action":"revoke","state":"revoked","message":"OpenAI profile revoked.","profile_id":"openai-oauth"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/reconnect ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-actions"));
                assert!(request.contains("\"profile_id\":\"openai-oauth\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"contract":{"contract_version":"control-plane.v1"},"provider":"openai","attempt_id":"attempt-reconnect","authorization_url":"https://auth.openai.example/authorize?attempt=reconnect","expires_at_unix_ms":1700000000500,"profile_id":"openai-oauth","message":"OpenAI OAuth authorization URL issued."}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected OpenAI profile action request: {request_line}");
        }
    });

    let inputs = build_test_openai_inputs(fixture.path(), port);
    let request = OpenAiProfileActionRequest { profile_id: "openai-oauth".to_owned() };

    let refresh = refresh_openai_profile(inputs.clone(), request.clone())
        .await
        .expect("refresh action should succeed");
    assert_eq!(refresh.message, "OpenAI profile refreshed.");

    let default_profile = set_openai_default_profile(inputs.clone(), request.clone())
        .await
        .expect("default selection should succeed");
    assert_eq!(default_profile.message, "OpenAI default profile updated.");

    let revoke = revoke_openai_profile(inputs.clone(), request.clone())
        .await
        .expect("revoke action should succeed");
    assert_eq!(revoke.message, "OpenAI profile revoked.");

    let reconnect =
        reconnect_openai_oauth(inputs, request).await.expect("reconnect action should succeed");
    assert_eq!(reconnect.attempt_id, "attempt-reconnect");
    assert_eq!(reconnect.profile_id.as_deref(), Some("openai-oauth"));

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn discord_onboarding_preflight_apply_and_verify_use_console_session_and_csrf() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..9 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-discord");
            if request_line.starts_with("GET /console/v1/auth/session ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-discord","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                        &[],
                    );
                } else {
                    write_json_response(
                        &mut stream,
                        "403 Forbidden",
                        r#"{"error":"console session cookie is missing"}"#,
                        &[],
                    );
                }
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/login ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-discord","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-discord; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/channels/discord/onboarding/probe ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-discord"));
                assert!(request.contains("\"token\":\"discord-live-token\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"connector_id":"discord:default","account_id":"default","bot":{"id":"123","username":"palyra-bot"},"invite_url_template":"https://discord.com/oauth2/authorize?client_id=123","required_permissions":["send_messages"],"security_defaults":["require_mention=true"],"warnings":["message content intent is required"],"policy_warnings":[],"inbound_alive":false}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/channels/discord/onboarding/apply ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-discord"));
                assert!(request.contains("\"verify_channel_id\":\"123456789012345678\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"preflight":{"connector_id":"discord:default","account_id":"default","warnings":[],"policy_warnings":[]},"applied":{"connector_id":"discord:default","config_path":"C:\\palyra\\discord.toml","config_created":true,"connector_enabled":true,"token_vault_ref":"vault://discord/default/token"},"status":{"readiness":"ready","liveness":"running"},"inbound_alive":true,"inbound_monitor_warnings":[]}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/channels/discord%3Adefault/test-send ") {
                assert!(request.contains("x-palyra-csrf-token: csrf-discord"));
                assert!(request.contains("\"target\":\"channel:123456789012345678\""));
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"dispatch":{"connector_id":"discord:default","target":"channel:123456789012345678","delivered":1},"status":{"readiness":"ready","liveness":"running"}}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected Discord onboarding request: {request_line}");
        }
    });

    let request = DiscordOnboardingRequest {
        account_id: Some("default".to_owned()),
        token: "discord-live-token".to_owned(),
        mode: Some("local".to_owned()),
        inbound_scope: Some("dm_only".to_owned()),
        allow_from: Vec::new(),
        deny_from: Vec::new(),
        require_mention: Some(true),
        mention_patterns: Vec::new(),
        concurrency_limit: Some(2),
        direct_message_policy: None,
        broadcast_strategy: Some("deny".to_owned()),
        confirm_open_guild_channels: Some(false),
        verify_channel_id: Some("123456789012345678".to_owned()),
    };

    let preflight = run_discord_onboarding_preflight(
        build_test_discord_inputs(fixture.path(), port),
        request.clone(),
    )
    .await
    .expect("Discord preflight should succeed");
    assert_eq!(preflight.connector_id, "discord:default");
    assert_eq!(preflight.bot_username.as_deref(), Some("palyra-bot"));

    let applied =
        apply_discord_onboarding(build_test_discord_inputs(fixture.path(), port), request)
            .await
            .expect("Discord apply should succeed");
    assert!(applied.connector_enabled);
    assert_eq!(applied.readiness.as_deref(), Some("ready"));
    assert_eq!(applied.token_vault_ref.as_deref(), Some("vault://discord/default/token"));

    let verify = verify_discord_connector(
        build_test_discord_inputs(fixture.path(), port),
        DiscordVerificationRequest {
            connector_id: "discord:default".to_owned(),
            target: "channel:123456789012345678".to_owned(),
            text: Some("hello discord".to_owned()),
        },
    )
    .await
    .expect("Discord verification should succeed");
    assert_eq!(verify.connector_id, "discord:default");
    assert_eq!(verify.delivered, Some(1));

    server.join().expect("test server thread should exit");
}

#[test]
fn openai_browser_handoff_requires_http_or_https() {
    let invalid = open_external_browser("file:///tmp/openai", |_url| Ok::<(), std::io::Error>(()));
    assert!(invalid
        .expect_err("browser handoff should reject non-http URLs")
        .to_string()
        .contains("only supports http:// and https:// URLs"));

    let mut opened_url = String::new();
    open_external_browser("https://auth.openai.example/authorize", |url| {
        opened_url = url.to_owned();
        Ok::<(), std::io::Error>(())
    })
    .expect("https browser handoff should succeed");
    assert_eq!(opened_url, "https://auth.openai.example/authorize");
}
