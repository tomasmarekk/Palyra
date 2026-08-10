use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Duration,
};

use super::super::features::onboarding::connectors::discord::DiscordControlPlaneInputs;
use super::super::openai_auth::OpenAiControlPlaneInputs;
use super::super::profile_registry::{implicit_profile, DesktopProfileCatalog};
use super::super::supervisor::{
    BrowserStateEncryptionKey, ConsolePayloadCache, DesktopConfigReloadWatchState,
};
use super::super::{
    mpsc, Client, ControlCenter, DesktopInstanceLock, DesktopRuntimeSecrets, DesktopStateFile,
    ManagedService, RuntimeConfig, Ulid, CONSOLE_PRINCIPAL, LOG_EVENT_CHANNEL_CAPACITY,
};

pub(super) struct TempFixtureDir {
    root: PathBuf,
}

impl TempFixtureDir {
    pub(super) fn new() -> Self {
        let root = std::env::temp_dir().join(format!("palyra-desktop-fixture-{}", Ulid::new()));
        std::fs::create_dir_all(root.as_path()).expect("fixture directory should be created");
        Self { root }
    }

    pub(super) fn path(&self) -> &Path {
        self.root.as_path()
    }
}

impl Drop for TempFixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.root.as_path());
    }
}

pub(super) fn write_config_file(root: &Path, content: &str) -> PathBuf {
    let path = root.join("palyra.toml");
    std::fs::write(path.as_path(), content).expect("fixture config should be written");
    path
}

pub(super) fn write_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("fixture parent directory should be created");
    }
    std::fs::write(path, content).expect("fixture file should be written");
}

pub(super) fn write_cli_profiles_file(root: &Path, content: &str) -> PathBuf {
    let path = root.join("cli").join("profiles.toml");
    write_file(path.as_path(), content);
    path
}

pub(super) fn build_test_control_center(root: &Path) -> ControlCenter {
    let state_dir = root.join("desktop-control-center");
    let runtime_root = state_dir.join("runtime");
    let support_bundle_dir = state_dir.join("support-bundles");
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
    let node_host = ManagedService::new(Vec::new());
    let http_client = Client::builder()
        .cookie_store(true)
        .timeout(Duration::from_secs(4))
        .build()
        .expect("HTTP client should initialize for test control center");
    let (log_tx, log_rx) = mpsc::channel(LOG_EVENT_CHANNEL_CAPACITY);
    let persisted = DesktopStateFile::new_default();
    let active_profile = implicit_profile(persisted.active_profile_name());
    let config_reload_watch = DesktopConfigReloadWatchState::from_profile(&active_profile);
    let profile_catalog = DesktopProfileCatalog::load(root).expect("profile catalog should load");
    let runtime_secret_fallbacks = DesktopRuntimeSecrets {
        admin_token: format!("test-admin-{}", Ulid::new()),
        browser_auth_token: format!("test-browser-{}", Ulid::new()),
        browser_state_encryption_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
    };
    ControlCenter {
        desktop_state_root: root.to_path_buf(),
        state_dir: state_dir.clone(),
        _instance_lock: DesktopInstanceLock::acquire(state_dir.as_path())
            .expect("test control center instance lock should succeed"),
        default_runtime_root: runtime_root.clone(),
        runtime_root,
        support_bundle_dir,
        state_file_path: state_dir.join("state.json"),
        persisted,
        active_profile,
        profile_catalog,
        admin_token: runtime_secret_fallbacks.admin_token.clone(),
        admin_bound_principal: CONSOLE_PRINCIPAL.to_owned(),
        browser_auth_token: runtime_secret_fallbacks.browser_auth_token.clone(),
        browser_state_encryption_key: Some(
            BrowserStateEncryptionKey::parse(
                runtime_secret_fallbacks.browser_state_encryption_key.clone(),
                "test browser state key",
            )
            .expect("test browser state key should parse"),
        ),
        runtime_secret_fallbacks,
        runtime,
        gateway,
        browserd,
        node_host,
        config_reload_watch,
        http_client,
        console_session_cache: Arc::new(Mutex::new(None)),
        console_payload_cache: Arc::new(Mutex::new(ConsolePayloadCache::default())),
        log_tx,
        log_rx,
        dropped_log_events: Arc::new(AtomicU64::new(0)),
    }
}

pub(super) fn build_test_openai_inputs(root: &Path, port: u16) -> OpenAiControlPlaneInputs {
    let mut control_center = build_test_control_center(root);
    control_center.runtime.gateway_admin_port = port;
    OpenAiControlPlaneInputs::capture(&control_center)
}

pub(super) fn build_test_discord_inputs(root: &Path, port: u16) -> DiscordControlPlaneInputs {
    let mut control_center = build_test_control_center(root);
    control_center.runtime.gateway_admin_port = port;
    DiscordControlPlaneInputs::capture(&control_center)
}

pub(super) fn write_json_response(
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
