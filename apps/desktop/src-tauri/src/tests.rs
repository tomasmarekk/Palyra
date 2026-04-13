use std::{
    ffi::OsString,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use serde_json::json;

mod dashboard_access;
mod support;

use support::{
    build_test_control_center, build_test_discord_inputs, build_test_openai_inputs,
    write_cli_profiles_file, write_config_file, write_file, write_json_response, TempFixtureDir,
};

use crate::companion::{build_companion_snapshot, DesktopCompanionRolloutRequest};

use super::commands::initialize_control_center;
use super::features::onboarding::connectors::discord::{
    apply_discord_onboarding, run_discord_onboarding_preflight, verify_discord_connector,
    DiscordOnboardingRequest, DiscordVerificationRequest,
};
use super::openai_auth::{
    connect_openai_api_key, get_openai_oauth_callback_state, load_openai_auth_status,
    open_external_browser, reconnect_openai_oauth, refresh_openai_profile, revoke_openai_profile,
    set_openai_default_profile, start_openai_oauth_bootstrap, OpenAiApiKeyConnectRequest,
    OpenAiOAuthBootstrapRequest, OpenAiOAuthCallbackStateRequest, OpenAiProfileActionRequest,
    OpenAiScopeInput,
};
use super::snapshot::resolve_dashboard_access_target;
use super::{
    bootstrap_portable_install_environment_for_executable, build_desktop_refresh_payload,
    build_onboarding_status, build_snapshot_from_inputs, collect_redacted_errors,
    compute_backoff_ms, executable_file_name, load_or_initialize_state_file, load_runtime_secrets,
    migrate_legacy_runtime_secrets_from_state_file, mpsc, parse_discord_status,
    parse_remote_dashboard_base_url, prepare_control_center_for_launch, resolve_binary_path,
    resolve_desktop_state_root, sanitize_log_line, try_enqueue_log_event,
    validate_runtime_state_root_override, BrowserStatusSnapshot, DashboardAccessMode,
    DesktopOnboardingStep, DesktopSecretStore, DesktopStateFile, LogEvent, LogStream, ServiceKind,
    Ulid,
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

    fn unset(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        // SAFETY: tests serialize environment mutations with env_lock().
        unsafe {
            std::env::remove_var(key);
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

#[test]
fn backoff_uses_exponential_growth_with_cap() {
    assert_eq!(compute_backoff_ms(0), 1_000);
    assert_eq!(compute_backoff_ms(1), 2_000);
    assert_eq!(compute_backoff_ms(2), 4_000);
    assert_eq!(compute_backoff_ms(5), 30_000);
    assert_eq!(compute_backoff_ms(9), 30_000);
}

#[test]
fn launch_preparation_requests_gateway_and_enabled_browser_start() {
    let fixture = TempFixtureDir::new();
    let mut control_center = build_test_control_center(fixture.path());
    control_center.persisted.browser_service_enabled = true;

    prepare_control_center_for_launch(&mut control_center);

    assert!(control_center.gateway.desired_running, "launch should request gateway start");
    assert!(control_center.browserd.desired_running, "launch should request browserd start");
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
fn companion_offline_draft_queueing_respects_rollout_toggle() {
    let fixture = TempFixtureDir::new();
    let mut control_center = build_test_control_center(fixture.path());
    assert!(
        control_center.companion_offline_drafts_enabled(),
        "offline drafts should be enabled by default"
    );
    control_center
        .update_companion_rollout(&DesktopCompanionRolloutRequest {
            companion_shell_enabled: None,
            desktop_notifications_enabled: None,
            offline_drafts_enabled: Some(false),
            voice_capture_enabled: None,
            tts_playback_enabled: None,
            release_channel: None,
        })
        .expect("rollout update should persist");
    assert!(
        !control_center.companion_offline_drafts_enabled(),
        "offline draft queueing should disable when rollout toggle is false"
    );
}

#[test]
fn companion_rollout_persists_voice_and_tts_flags() {
    let fixture = TempFixtureDir::new();
    let mut control_center = build_test_control_center(fixture.path());

    control_center
        .update_companion_rollout(&DesktopCompanionRolloutRequest {
            companion_shell_enabled: None,
            desktop_notifications_enabled: None,
            offline_drafts_enabled: None,
            voice_capture_enabled: Some(true),
            tts_playback_enabled: Some(true),
            release_channel: Some("voice-preview".to_owned()),
        })
        .expect("voice rollout update should persist");

    let rollout = &control_center.persisted.active_companion().rollout;
    assert!(rollout.voice_capture_enabled, "voice capture flag should persist");
    assert!(rollout.tts_playback_enabled, "tts playback flag should persist");
    assert_eq!(rollout.release_channel, "voice-preview");
}

#[test]
fn profile_switch_isolates_companion_preferences_and_drafts() {
    let fixture = TempFixtureDir::new();
    let _profiles = write_cli_profiles_file(
        fixture.path(),
        r#"
version = 1
default_profile = "review"

[profiles.review]
label = "Review"
mode = "remote"
environment = "staging"
color = "amber"
risk_level = "elevated"
"#,
    );

    let mut control_center = build_test_control_center(fixture.path());
    control_center
        .update_companion_preferences(&super::companion::DesktopCompanionPreferencesRequest {
            active_section: Some(super::DesktopCompanionSection::Chat),
            active_session_id: Some("desktop-local-session".to_owned()),
            active_device_id: None,
            last_run_id: Some("desktop-local-run".to_owned()),
        })
        .expect("desktop-local preferences should persist");
    let local_draft_id = control_center
        .record_companion_offline_draft(
            Some("desktop-local-session"),
            "Retry this when local runtime returns",
            "local runtime offline",
        )
        .expect("desktop-local draft should persist");

    control_center.gateway.desired_running = true;
    let message = control_center
        .switch_active_profile("review", false)
        .expect("profile switch should succeed");

    assert!(message.contains("runtime was paused"));
    assert_eq!(control_center.persisted.active_profile_name(), "review");
    assert_eq!(control_center.active_profile.context.name, "review");
    assert!(!control_center.gateway.desired_running);
    assert!(control_center.persisted.active_companion().active_session_id.is_none());
    assert!(control_center.persisted.active_companion().offline_drafts.is_empty());

    control_center
        .update_companion_preferences(&super::companion::DesktopCompanionPreferencesRequest {
            active_section: Some(super::DesktopCompanionSection::Approvals),
            active_session_id: Some("review-session".to_owned()),
            active_device_id: Some("review-device".to_owned()),
            last_run_id: Some("review-run".to_owned()),
        })
        .expect("review preferences should persist");
    let review_draft_id = control_center
        .record_companion_offline_draft(
            Some("review-session"),
            "Resume review workflow",
            "review profile disconnected",
        )
        .expect("review draft should persist");

    control_center
        .switch_active_profile("desktop-local", false)
        .expect("switch back to desktop-local should succeed");

    let companion = control_center.persisted.active_companion();
    assert_eq!(companion.active_session_id.as_deref(), Some("desktop-local-session"));
    assert_eq!(companion.last_run_id.as_deref(), Some("desktop-local-run"));
    assert_eq!(companion.offline_drafts.len(), 1);
    assert_eq!(companion.offline_drafts[0].draft_id, local_draft_id);
    assert_ne!(companion.offline_drafts[0].draft_id, review_draft_id);
}

#[test]
fn profile_switch_requires_explicit_ack_for_strict_profiles() {
    let fixture = TempFixtureDir::new();
    let _profiles = write_cli_profiles_file(
        fixture.path(),
        r#"
version = 1

[profiles.production]
label = "Production"
strict_mode = true
mode = "remote"
environment = "production"
color = "red"
risk_level = "high"
"#,
    );

    let mut control_center = build_test_control_center(fixture.path());
    let error = control_center
        .switch_active_profile("production", false)
        .expect_err("strict profile switch must require explicit confirmation");

    assert!(error.to_string().contains("requires explicit confirmation"));
    assert_eq!(control_center.persisted.active_profile_name(), "desktop-local");
}

#[test]
fn profile_switch_persists_active_profile_for_restore() {
    let fixture = TempFixtureDir::new();
    let _profiles = write_cli_profiles_file(
        fixture.path(),
        r#"
version = 1

[profiles.review]
label = "Review"
mode = "remote"
"#,
    );

    let mut control_center = build_test_control_center(fixture.path());
    control_center
        .switch_active_profile("review", false)
        .expect("profile switch should persist state");
    control_center.save_state_file().expect("desktop state should save after profile switch");

    let reloaded = load_or_initialize_state_file(control_center.state_file_path.as_path())
        .expect("desktop state should reload after switch");

    assert_eq!(reloaded.active_profile_name(), "review");
    assert!(
        reloaded.recent_profile_names().iter().any(|name| name == "review"),
        "recent profile list should include the switched profile"
    );
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

    let query = parse_remote_dashboard_base_url(
        "https://dashboard.example.com/?token=secret",
        "gateway_access.remote_base_url",
    )
    .expect_err("URL with a query string must be rejected");
    assert!(query.to_string().contains("must not include query or fragment"));

    let fragment = parse_remote_dashboard_base_url(
        "https://dashboard.example.com/#state=secret",
        "gateway_access.remote_base_url",
    )
    .expect_err("URL with a fragment must be rejected");
    assert!(fragment.to_string().contains("must not include query or fragment"));
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
    assert_eq!(target.url, "http://127.0.0.1:9911/#/control/overview");
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
    migrate_legacy_runtime_secrets_from_state_file(state_path.as_path(), &secret_store)
        .expect("legacy desktop secrets should migrate");
    let loaded = load_or_initialize_state_file(state_path.as_path())
        .expect("legacy desktop state should load");
    let runtime_secrets =
        load_runtime_secrets(&secret_store).expect("runtime secrets should load after migration");
    assert_eq!(
        runtime_secrets.admin_token,
        legacy["admin_token"].as_str().expect("legacy admin token fixture should be string")
    );
    assert_eq!(
        runtime_secrets.browser_auth_token,
        legacy["browser_auth_token"]
            .as_str()
            .expect("legacy browser token fixture should be string")
    );
    assert!(!loaded.browser_service_enabled);

    let rewritten = std::fs::read_to_string(state_path.as_path())
        .expect("rewritten desktop state should be readable");
    assert!(!rewritten.contains(legacy["admin_token"].as_str().unwrap_or_default()));
    assert!(!rewritten.contains(legacy["browser_auth_token"].as_str().unwrap_or_default()));

    let persisted_json: serde_json::Value =
        serde_json::from_str(rewritten.as_str()).expect("rewritten state should parse");
    assert!(persisted_json.get("admin_token").is_none());
    assert!(persisted_json.get("browser_auth_token").is_none());
    assert_eq!(persisted_json["browser_service_enabled"], json!(false));

    migrate_legacy_runtime_secrets_from_state_file(state_path.as_path(), &secret_store)
        .expect("legacy desktop secrets should reload idempotently");
    let loaded_again = load_or_initialize_state_file(state_path.as_path())
        .expect("migrated desktop state should load from secret store");
    let runtime_secrets_again = load_runtime_secrets(&secret_store)
        .expect("runtime secrets should reload from secret store");
    assert_eq!(runtime_secrets_again.admin_token, runtime_secrets.admin_token);
    assert_eq!(runtime_secrets_again.browser_auth_token, runtime_secrets.browser_auth_token);
    assert_eq!(loaded_again.browser_service_enabled, loaded.browser_service_enabled);
}

#[test]
fn state_file_initialization_never_writes_plaintext_tokens() {
    let fixture = TempFixtureDir::new();
    let state_path = fixture.path().join("state.json");
    let secret_store =
        DesktopSecretStore::open(fixture.path()).expect("secret store should initialize");
    let loaded = load_or_initialize_state_file(state_path.as_path())
        .expect("desktop state should initialize");
    let runtime_secrets =
        load_runtime_secrets(&secret_store).expect("runtime secrets should initialize");
    let persisted_raw =
        std::fs::read_to_string(state_path.as_path()).expect("desktop state should be readable");
    assert!(!persisted_raw.contains(runtime_secrets.admin_token.as_str()));
    assert!(!persisted_raw.contains(runtime_secrets.browser_auth_token.as_str()));
    assert!(!persisted_raw.contains("admin_token"));
    assert!(!persisted_raw.contains("browser_auth_token"));
    assert_eq!(loaded.active_profile_name(), "desktop-local");
}

#[test]
fn state_file_initialization_seeds_onboarding_defaults() {
    let fixture = TempFixtureDir::new();
    let state_path = fixture.path().join("state.json");
    let loaded = load_or_initialize_state_file(state_path.as_path())
        .expect("desktop state should initialize");
    assert_eq!(loaded.active_profile_name(), "desktop-local");
    assert!(loaded.normalized_runtime_state_root().is_none());
    assert!(loaded.active_onboarding().welcome_acknowledged_at_unix_ms.is_none());
    assert!(!loaded.active_onboarding().flow_id.trim().is_empty());
    assert_eq!(loaded.active_onboarding().discord.account_id, "default");
    assert_eq!(loaded.active_onboarding().discord.broadcast_strategy, "deny");
    assert!(loaded.active_onboarding().recent_events.is_empty());
    assert!(loaded.active_onboarding().failure_step_counts.is_empty());
    assert_eq!(loaded.active_onboarding().support_bundle_export_attempts, 0);
}

#[test]
fn desktop_state_root_uses_absolute_palyra_state_root_override() {
    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let _state_root_override =
        ScopedEnvVar::set("PALYRA_STATE_ROOT", fixture.path().to_string_lossy().as_ref());

    let resolved = resolve_desktop_state_root().expect("desktop state root should resolve");
    assert_eq!(resolved, fixture.path().to_path_buf());
}

#[test]
fn desktop_state_root_rejects_relative_palyra_state_root_override() {
    let _env_guard = lock_env();
    let _state_root_override = ScopedEnvVar::set("PALYRA_STATE_ROOT", "relative-state-root");

    let error =
        resolve_desktop_state_root().expect_err("relative PALYRA_STATE_ROOT should be rejected");
    assert!(error.to_string().contains("must be an absolute path"));
}

#[test]
fn portable_install_metadata_bootstraps_missing_env_paths() {
    let _env_guard = lock_env();
    let _state_root_override = ScopedEnvVar::unset("PALYRA_STATE_ROOT");
    let _config_override = ScopedEnvVar::unset("PALYRA_CONFIG");
    let fixture = TempFixtureDir::new();
    let install_root = fixture.path().join("install");
    let state_root = fixture.path().join("portable-state");
    let config_path = state_root.join("palyra.toml");
    let executable_path = install_root.join(executable_file_name("palyra-desktop-control-center"));

    std::fs::create_dir_all(install_root.as_path())
        .expect("install root should exist for metadata bootstrap test");
    std::fs::create_dir_all(state_root.as_path())
        .expect("state root should exist for metadata bootstrap test");
    std::fs::write(executable_path.as_path(), b"desktop")
        .expect("desktop executable stub should be created");
    std::fs::write(config_path.as_path(), "daemon.bind = \"127.0.0.1\"")
        .expect("config stub should be created");
    std::fs::write(
        install_root.join("install-metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "artifact_kind": "desktop",
            "state_root": state_root,
            "config_path": config_path,
        }))
        .expect("install metadata should serialize"),
    )
    .expect("install metadata should be written");

    bootstrap_portable_install_environment_for_executable(executable_path.as_path())
        .expect("portable install bootstrap should succeed");

    assert_eq!(std::env::var_os("PALYRA_STATE_ROOT").map(PathBuf::from), Some(state_root.clone()));
    assert_eq!(std::env::var_os("PALYRA_CONFIG").map(PathBuf::from), Some(config_path));
}

#[test]
fn portable_install_metadata_does_not_override_explicit_env_paths() {
    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let install_root = fixture.path().join("install");
    let state_root = fixture.path().join("portable-state");
    let config_path = state_root.join("palyra.toml");
    let explicit_state_root = fixture.path().join("explicit-state");
    let explicit_config_path = fixture.path().join("explicit.toml");
    let executable_path = install_root.join(executable_file_name("palyra-desktop-control-center"));
    let _state_root_override =
        ScopedEnvVar::set("PALYRA_STATE_ROOT", explicit_state_root.to_string_lossy().as_ref());
    let _config_override =
        ScopedEnvVar::set("PALYRA_CONFIG", explicit_config_path.to_string_lossy().as_ref());

    std::fs::create_dir_all(install_root.as_path())
        .expect("install root should exist for metadata bootstrap test");
    std::fs::create_dir_all(state_root.as_path())
        .expect("state root should exist for metadata bootstrap test");
    std::fs::write(executable_path.as_path(), b"desktop")
        .expect("desktop executable stub should be created");
    std::fs::write(config_path.as_path(), "daemon.bind = \"127.0.0.1\"")
        .expect("config stub should be created");
    std::fs::write(
        install_root.join("install-metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "artifact_kind": "desktop",
            "state_root": state_root,
            "config_path": config_path,
        }))
        .expect("install metadata should serialize"),
    )
    .expect("install metadata should be written");

    bootstrap_portable_install_environment_for_executable(executable_path.as_path())
        .expect("portable install bootstrap should succeed");

    assert_eq!(std::env::var_os("PALYRA_STATE_ROOT").map(PathBuf::from), Some(explicit_state_root));
    assert_eq!(std::env::var_os("PALYRA_CONFIG").map(PathBuf::from), Some(explicit_config_path));
}

#[test]
fn runtime_state_root_override_accepts_desktop_managed_subdirectory() {
    let fixture = TempFixtureDir::new();
    let default_runtime_root = fixture.path().join("runtime");
    let custom_runtime_root = fixture.path().join("runtime-alt");
    std::fs::create_dir_all(default_runtime_root.as_path())
        .expect("default runtime root should exist for validation");

    let resolved = validate_runtime_state_root_override(
        Some(custom_runtime_root.to_string_lossy().as_ref()),
        default_runtime_root.as_path(),
    )
    .expect("desktop-managed runtime subdirectory should be accepted");

    assert_eq!(resolved, custom_runtime_root);
}

#[test]
fn runtime_state_root_override_rejects_paths_outside_desktop_state_directory() {
    let fixture = TempFixtureDir::new();
    let default_runtime_root = fixture.path().join("runtime");
    let escaped_runtime_root =
        std::env::temp_dir().join(format!("palyra-desktop-escape-{}", Ulid::new()));
    std::fs::create_dir_all(default_runtime_root.as_path())
        .expect("default runtime root should exist for validation");

    let error = validate_runtime_state_root_override(
        Some(escaped_runtime_root.to_string_lossy().as_ref()),
        default_runtime_root.as_path(),
    )
    .expect_err("paths outside the desktop state directory must be rejected");

    assert!(
        error.to_string().contains("must stay within the desktop state directory"),
        "unexpected error: {error}"
    );
}

#[test]
fn desktop_state_file_rejects_runtime_root_escape_on_reload() {
    let fixture = TempFixtureDir::new();
    let default_runtime_root = fixture.path().join("runtime");
    let escaped_runtime_root =
        std::env::temp_dir().join(format!("palyra-desktop-reload-escape-{}", Ulid::new()));
    std::fs::create_dir_all(default_runtime_root.as_path())
        .expect("default runtime root should exist for reload validation");

    let state_file = DesktopStateFile { ..DesktopStateFile::default() };
    let mut state_file = state_file;
    state_file.active_profile_state_mut().runtime_state_root =
        Some(escaped_runtime_root.to_string_lossy().into_owned());
    let error = state_file
        .resolve_runtime_root(default_runtime_root.as_path())
        .expect_err("persisted runtime root escapes must be rejected on reload");

    assert!(
        error.to_string().contains("must stay within the desktop state directory"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)]
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
#[allow(clippy::await_holding_lock)]
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

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)]
async fn desktop_refresh_payload_reuses_single_snapshot_build_for_home_and_onboarding_views() {
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

    let payload = build_desktop_refresh_payload(control_center.capture_onboarding_status_inputs())
        .await
        .expect("desktop refresh payload should build");

    assert_eq!(payload.snapshot.quick_facts.dashboard_url, payload.onboarding_status.dashboard_url);
    assert_eq!(
        payload.snapshot.quick_facts.dashboard_access_mode,
        payload.onboarding_status.dashboard_access_mode
    );
    assert_eq!(
        payload.openai_status.default_profile_id,
        payload.onboarding_status.openai_default_profile_id
    );
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
        listener.set_nonblocking(true).expect("listener should support nonblocking mode");
        let mut idle_since = Instant::now();
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => {
                    idle_since = Instant::now();
                    connection
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if idle_since.elapsed() >= Duration::from_millis(250) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("listener should accept request: {error}"),
            };
            stream
                .set_nonblocking(false)
                .expect("accepted stream should switch back to blocking mode");
            let mut buffer = [0_u8; 4096];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-1");

            if request_line.starts_with("GET /healthz ") {
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
        0,
        "desktop snapshot polling should use the cookie-backed read path without forcing a console login when reads already succeed"
    );
    assert_eq!(
        session_requests.load(Ordering::Relaxed),
        0,
        "desktop snapshot polling should not probe the auth/session endpoint while the cookie-backed read path remains healthy"
    );
    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn companion_snapshot_polling_does_not_rebootstrap_console_auth_after_first_refresh() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let login_requests = Arc::new(AtomicU64::new(0));
    let session_requests = Arc::new(AtomicU64::new(0));
    let login_requests_server = Arc::clone(&login_requests);
    let session_requests_server = Arc::clone(&session_requests);

    let server = std::thread::spawn(move || {
        listener.set_nonblocking(true).expect("listener should support nonblocking mode");
        let mut idle_since = Instant::now();
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => {
                    idle_since = Instant::now();
                    connection
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if idle_since.elapsed() >= Duration::from_millis(400) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("listener should accept request: {error}"),
            };
            stream
                .set_nonblocking(false)
                .expect("accepted stream should switch back to blocking mode");

            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-companion");

            if request_line.starts_with("GET /healthz ") {
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"status":"ok","version":"0.1.0","git_hash":"desktop-test","uptime_seconds":42}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET / ") {
                write_json_response(&mut stream, "200 OK", r#"{"ok":true}"#, &[]);
                continue;
            }
            if request_line.starts_with("GET /console/v1/auth/session ") {
                session_requests_server.fetch_add(1, Ordering::Relaxed);
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-companion","issued_at_unix_ms":1,"expires_at_unix_ms":4102444800000}"#,
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
                login_requests_server.fetch_add(1, Ordering::Relaxed);
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-companion","issued_at_unix_ms":1,"expires_at_unix_ms":4102444800000}"#,
                    &["Set-Cookie: palyra_console_session=session-companion; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/diagnostics ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"generated_at_unix_ms":1700000000000,"observability":{},"errors":[]}"#,
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
            if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                if has_cookie {
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"connector":{"connector_id":"discord:default","enabled":false,"readiness":"disabled","liveness":"stopped"}}"#,
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
            if request_line.starts_with("GET /console/v1/auth/providers/openai ") {
                assert!(has_cookie, "OpenAI provider state should reuse the console session");
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"provider":"openai","state":"connected","note":"OpenAI is ready.","default_profile_id":"openai-default","oauth_supported":true,"bootstrap_supported":true,"callback_supported":true,"reconnect_supported":true,"revoke_supported":true,"default_selection_supported":true}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/auth/health?include_profiles=true ") {
                assert!(has_cookie, "OpenAI auth health should reuse the console session");
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"profiles":[],"refresh_metrics":{"attempts":0,"successes":0,"failures":0,"by_provider":[{"provider":"openai","attempts":0,"successes":0,"failures":0}]}}"#,
                    &[],
                );
                continue;
            }
            if request_line
                .starts_with("GET /console/v1/auth/profiles?provider_kind=openai&limit=100 ")
            {
                assert!(has_cookie, "OpenAI profile listing should reuse the console session");
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"profiles":[],"page":{"limit":100,"returned":0,"next_cursor":null,"has_more":false}}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/sessions?") {
                assert!(has_cookie, "session catalog should reuse the console session");
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"sessions":[],"summary":{"active_sessions":0,"archived_sessions":0,"sessions_with_pending_approvals":0,"sessions_with_active_runs":0},"page":{"limit":16,"returned":0,"next_cursor":null,"has_more":false}}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/approvals?limit=24 ") {
                assert!(has_cookie, "approvals listing should reuse the console session");
                write_json_response(&mut stream, "200 OK", r#"{"approvals":[]}"#, &[]);
                continue;
            }
            if request_line.starts_with("GET /console/v1/inventory ") {
                assert!(has_cookie, "inventory listing should reuse the console session");
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"generated_at_unix_ms":1700000000000,"summary":{"devices":0,"trusted_devices":0,"pending_pairings":0,"ok_devices":0,"stale_devices":0,"degraded_devices":0,"offline_devices":0,"ok_instances":0,"stale_instances":0,"degraded_instances":0,"offline_instances":0},"devices":[]}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected companion snapshot request: {request_line}");
        }
    });

    let mut control_center = build_test_control_center(fixture.path());
    control_center.persisted.browser_service_enabled = false;
    control_center.runtime.gateway_admin_port = port;

    let _first = build_companion_snapshot(control_center.capture_companion_inputs())
        .await
        .expect("first companion snapshot should build");
    let first_session_requests = session_requests.load(Ordering::Relaxed);
    let first_login_requests = login_requests.load(Ordering::Relaxed);

    let _second = build_companion_snapshot(control_center.capture_companion_inputs())
        .await
        .expect("second companion snapshot should build");

    assert_eq!(
        session_requests.load(Ordering::Relaxed),
        first_session_requests,
        "subsequent companion refreshes should not hit /console/v1/auth/session once the desktop session is cached",
    );
    assert_eq!(
        login_requests.load(Ordering::Relaxed),
        first_login_requests,
        "subsequent companion refreshes should not re-login while the cached desktop session remains valid",
    );

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn onboarding_preflight_accepts_healthy_runtime_already_bound_to_expected_ports() {
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

    let _env_guard = lock_env();
    let fixture = TempFixtureDir::new();
    let config_path = write_config_file(
        fixture.path(),
        r#"
version = 1
"#,
    );
    let gateway_binary = fixture.path().join(executable_file_name("palyrad"));
    let cli_binary = fixture.path().join(executable_file_name("palyra"));
    write_file(gateway_binary.as_path(), "binary");
    write_file(cli_binary.as_path(), "binary");
    let _config_override =
        ScopedEnvVar::set("PALYRA_CONFIG", config_path.to_string_lossy().as_ref());
    let _gateway_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRAD_BIN", gateway_binary.to_string_lossy().as_ref());
    let _cli_override =
        ScopedEnvVar::set("PALYRA_DESKTOP_PALYRA_BIN", cli_binary.to_string_lossy().as_ref());

    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let server = std::thread::spawn(move || {
        for _ in 0..6 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 4096];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            if request_line.starts_with("GET /healthz ") {
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
                    &["Set-Cookie: palyra_console_session=session-healthy; Path=/; HttpOnly; SameSite=Strict"],
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
            if request_line.starts_with("GET /console/v1/auth/providers/openai ") {
                write_http_response(
                    &mut stream,
                    "200 OK",
                    r#"{"provider":"openai","summary":{"total":0,"ok":0,"static":0,"expiring":0,"expired":0},"profiles":[],"default_profile_id":null,"default_profile_name":null,"connect":{"mode":"idle"},"refresh":{"attempts":0,"success":0,"failed":0},"oauth":{"available":true},"status":"not_configured"}"#,
                    &[],
                );
                continue;
            }
            if request_line.starts_with("GET /console/v1/channels/discord%3Adefault ") {
                write_http_response(
                    &mut stream,
                    "200 OK",
                    r#"{"connector":{"connector_id":"discord:default","enabled":false,"readiness":"unknown","liveness":"unknown"}}"#,
                    &[],
                );
                continue;
            }
            panic!("unexpected desktop snapshot request: {request_line}");
        }
    });

    let mut control_center = build_test_control_center(fixture.path());
    control_center.runtime.gateway_admin_port = port;
    control_center.gateway.bound_ports = vec![port];
    control_center.persisted.browser_service_enabled = false;
    control_center
        .mark_onboarding_welcome_acknowledged()
        .expect("welcome acknowledgement should persist");

    let payload = build_desktop_refresh_payload(control_center.capture_onboarding_status_inputs())
        .await
        .expect("desktop refresh payload should build");
    let gateway_ports = payload
        .onboarding_status
        .preflight
        .checks
        .iter()
        .find(|check| check.key == "gateway_ports")
        .expect("gateway port preflight should exist");

    assert_eq!(gateway_ports.status, "ok");
    assert!(
        gateway_ports.detail.contains("already responding"),
        "healthy runtime detail should explain the port reuse case: {}",
        gateway_ports.detail
    );
    assert_eq!(payload.snapshot.overall_status, super::snapshot::OverallStatus::Healthy);

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
        listener.set_nonblocking(true).expect("listener should support nonblocking mode");
        let mut idle_since = Instant::now();
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => {
                    idle_since = Instant::now();
                    connection
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if idle_since.elapsed() >= Duration::from_millis(250) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("listener should accept request: {error}"),
            };
            stream
                .set_nonblocking(false)
                .expect("accepted stream should switch back to blocking mode");
            let mut buffer = [0_u8; 4096];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            if request_line.starts_with("GET /healthz ") {
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
                            "canvas_experiments":{
                                "structured_contract":"a2ui.v1",
                                "fail_closed":true,
                                "requires_console_diagnostics":true,
                                "native_canvas":{
                                    "track_id":"native-canvas-preview",
                                    "enabled":false,
                                    "feature_flag":"canvas_host.enabled",
                                    "rollout_stage":"disabled",
                                    "ambient_mode":"disabled",
                                    "consent_required":false,
                                    "support_summary":"Native canvas stays behind the bounded canvas host.",
                                    "security_review":[
                                        "Preserve CSP and token-scoped access."
                                    ],
                                    "exit_criteria":[
                                        "Disable if diagnostics regress."
                                    ],
                                    "limits":{
                                        "max_state_bytes":8192,
                                        "max_bundle_bytes":65536,
                                        "max_assets_per_bundle":8,
                                        "max_updates_per_minute":30
                                    }
                                }
                            },
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
    assert_eq!(snapshot.diagnostics.observability.provider_auth.failure_rate_bps, 2_500);
    assert_eq!(snapshot.diagnostics.observability.dashboard.failures, 2);
    assert_eq!(snapshot.diagnostics.observability.connector.queue_depth, 6);
    assert_eq!(snapshot.diagnostics.observability.browser.relay_failures, 1);
    assert_eq!(snapshot.diagnostics.observability.support_bundle.success_rate_bps, 7_500);
    assert_eq!(snapshot.diagnostics.observability.failure_classes.upstream_provider_failure, 2);
    assert_eq!(snapshot.diagnostics.observability.recent_failure_count, 1);
    assert_eq!(snapshot.diagnostics.experiments.structured_contract, "a2ui.v1");
    assert!(snapshot.diagnostics.experiments.fail_closed);
    assert_eq!(snapshot.diagnostics.experiments.native_canvas.feature_flag, "canvas_host.enabled");
    assert_eq!(snapshot.diagnostics.experiments.native_canvas.limits.max_state_bytes, 8_192);
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
    let session_requests = Arc::new(AtomicU64::new(0));
    let login_requests = Arc::new(AtomicU64::new(0));
    let callback_state_requests = Arc::new(AtomicU64::new(0));
    let session_requests_server = Arc::clone(&session_requests);
    let login_requests_server = Arc::clone(&login_requests);
    let callback_state_requests_server = Arc::clone(&callback_state_requests);
    let server = std::thread::spawn(move || {
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-oauth");
            if request_line.starts_with("GET /console/v1/auth/session ") {
                session_requests_server.fetch_add(1, Ordering::Relaxed);
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
                login_requests_server.fetch_add(1, Ordering::Relaxed);
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
                callback_state_requests_server.fetch_add(1, Ordering::Relaxed);
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
    assert_eq!(
        session_requests.load(Ordering::Relaxed),
        1,
        "OAuth bootstrap should establish the console session once and reuse the cached session for callback polling",
    );
    assert_eq!(
        login_requests.load(Ordering::Relaxed),
        1,
        "OAuth bootstrap should only login once per flow",
    );
    assert_eq!(
        callback_state_requests.load(Ordering::Relaxed),
        1,
        "callback-state polling should hit the control plane exactly once",
    );

    server.join().expect("test server thread should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn openai_profile_actions_hit_expected_routes_including_reconnect() {
    let fixture = TempFixtureDir::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let port = listener.local_addr().expect("listener address should resolve").port();
    let session_requests = Arc::new(AtomicU64::new(0));
    let login_requests = Arc::new(AtomicU64::new(0));
    let refresh_requests = Arc::new(AtomicU64::new(0));
    let default_profile_requests = Arc::new(AtomicU64::new(0));
    let revoke_requests = Arc::new(AtomicU64::new(0));
    let reconnect_requests = Arc::new(AtomicU64::new(0));
    let session_requests_server = Arc::clone(&session_requests);
    let login_requests_server = Arc::clone(&login_requests);
    let refresh_requests_server = Arc::clone(&refresh_requests);
    let default_profile_requests_server = Arc::clone(&default_profile_requests);
    let revoke_requests_server = Arc::clone(&revoke_requests);
    let reconnect_requests_server = Arc::clone(&reconnect_requests);
    let server = std::thread::spawn(move || {
        for _ in 0..6 {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let mut buffer = [0_u8; 8192];
            let read = stream.read(&mut buffer).expect("request should be readable");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or_default().to_owned();
            let has_cookie = request.contains("palyra_console_session=session-actions");
            if request_line.starts_with("GET /console/v1/auth/session ") {
                session_requests_server.fetch_add(1, Ordering::Relaxed);
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
                login_requests_server.fetch_add(1, Ordering::Relaxed);
                write_json_response(
                    &mut stream,
                    "200 OK",
                    r#"{"principal":"admin:desktop-control-center","device_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","csrf_token":"csrf-actions","issued_at_unix_ms":1,"expires_at_unix_ms":2}"#,
                    &["Set-Cookie: palyra_console_session=session-actions; Path=/; HttpOnly; SameSite=Strict"],
                );
                continue;
            }
            if request_line.starts_with("POST /console/v1/auth/providers/openai/refresh ") {
                refresh_requests_server.fetch_add(1, Ordering::Relaxed);
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
                default_profile_requests_server.fetch_add(1, Ordering::Relaxed);
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
                revoke_requests_server.fetch_add(1, Ordering::Relaxed);
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
                reconnect_requests_server.fetch_add(1, Ordering::Relaxed);
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
    assert_eq!(
        session_requests.load(Ordering::Relaxed),
        1,
        "profile actions should establish the console session once and reuse the cached session afterwards",
    );
    assert_eq!(login_requests.load(Ordering::Relaxed), 1);
    assert_eq!(refresh_requests.load(Ordering::Relaxed), 1);
    assert_eq!(default_profile_requests.load(Ordering::Relaxed), 1);
    assert_eq!(revoke_requests.load(Ordering::Relaxed), 1);
    assert_eq!(reconnect_requests.load(Ordering::Relaxed), 1);

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
