use std::{
    collections::VecDeque,
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultError, VaultScope};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use ulid::Ulid;

const SUPERVISOR_TICK_MS: u64 = 500;
const MAX_LOG_LINES_PER_SERVICE: usize = 400;
const LOG_EVENT_CHANNEL_CAPACITY: usize = 2_048;
const MAX_DIAGNOSTIC_ERRORS: usize = 25;
const DASHBOARD_SCHEME: &str = "http";
const LOOPBACK_HOST: &str = "127.0.0.1";
const CONSOLE_PRINCIPAL: &str = "admin:desktop-control-center";
const CONSOLE_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DESKTOP_STATE_SCHEMA_VERSION: u32 = 2;
const DESKTOP_SECRET_MAX_BYTES: usize = 4_096;
const DESKTOP_SECRET_KEY_ADMIN_TOKEN: &str = "desktop_admin_token";
const DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN: &str = "desktop_browser_auth_token";

const GATEWAY_ADMIN_PORT: u16 = 7142;
mod commands;
mod snapshot;

use snapshot::sanitize_log_line;

#[cfg(test)]
pub(crate) use snapshot::{
    BrowserStatusSnapshot, DashboardAccessMode, build_snapshot_from_inputs,
    collect_redacted_errors, parse_discord_status, parse_remote_dashboard_base_url,
    resolve_dashboard_access_target,
};

const GATEWAY_GRPC_PORT: u16 = 7443;
const GATEWAY_QUIC_PORT: u16 = 7444;
const BROWSER_HEALTH_PORT: u16 = 7143;
const BROWSER_GRPC_PORT: u16 = 7543;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServiceKind {
    Gateway,
    Browserd,
}

impl ServiceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Browserd => "browserd",
        }
    }

    const fn display_name(self) -> &'static str {
        match self {
            Self::Gateway => "palyrad",
            Self::Browserd => "palyra-browserd",
        }
    }

    const fn binary_name(self) -> &'static str {
        self.display_name()
    }

    const fn env_override(self) -> &'static str {
        match self {
            Self::Gateway => "PALYRA_DESKTOP_PALYRAD_BIN",
            Self::Browserd => "PALYRA_DESKTOP_BROWSERD_BIN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LogStream {
    Stdout,
    Stderr,
    Supervisor,
}

impl LogStream {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Supervisor => "supervisor",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogLine {
    unix_ms: i64,
    service: String,
    stream: String,
    line: String,
}

#[derive(Debug)]
struct LogEvent {
    unix_ms: i64,
    service: ServiceKind,
    stream: LogStream,
    line: String,
}

#[derive(Debug)]
struct ManagedService {
    desired_running: bool,
    child: Option<Child>,
    pid: Option<u32>,
    last_start_unix_ms: Option<i64>,
    restart_attempt: u32,
    next_restart_unix_ms: Option<i64>,
    last_exit: Option<String>,
    logs: VecDeque<LogLine>,
    bound_ports: Vec<u16>,
}

impl ManagedService {
    fn new(bound_ports: Vec<u16>) -> Self {
        Self {
            desired_running: false,
            child: None,
            pid: None,
            last_start_unix_ms: None,
            restart_attempt: 0,
            next_restart_unix_ms: None,
            last_exit: None,
            logs: VecDeque::new(),
            bound_ports,
        }
    }

    fn running(&self) -> bool {
        self.child.is_some()
    }

    fn liveness(&self) -> &'static str {
        if self.running() {
            return "running";
        }
        if self.desired_running {
            return "restarting";
        }
        "stopped"
    }
}

#[derive(Debug, Clone)]
struct RuntimeConfig {
    gateway_admin_port: u16,
    gateway_grpc_port: u16,
    gateway_quic_port: u16,
    browser_health_port: u16,
    browser_grpc_port: u16,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            gateway_admin_port: GATEWAY_ADMIN_PORT,
            gateway_grpc_port: GATEWAY_GRPC_PORT,
            gateway_quic_port: GATEWAY_QUIC_PORT,
            browser_health_port: BROWSER_HEALTH_PORT,
            browser_grpc_port: BROWSER_GRPC_PORT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DesktopStateFile {
    schema_version: u32,
    browser_service_enabled: bool,
}

impl DesktopStateFile {
    fn new_default() -> Self {
        Self {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyDesktopStateFile {
    #[serde(default = "default_legacy_schema_version")]
    schema_version: u32,
    #[serde(default)]
    admin_token: String,
    #[serde(default)]
    browser_auth_token: String,
    #[serde(default = "default_browser_service_enabled")]
    browser_service_enabled: bool,
}

impl LegacyDesktopStateFile {
    fn into_state(self) -> DesktopStateFile {
        let _ = self.schema_version;
        DesktopStateFile {
            schema_version: DESKTOP_STATE_SCHEMA_VERSION,
            browser_service_enabled: self.browser_service_enabled,
        }
    }
}

#[derive(Debug, Clone)]
struct LoadedDesktopState {
    persisted: DesktopStateFile,
    admin_token: String,
    browser_auth_token: String,
}

struct DesktopSecretStore {
    vault: Vault,
}

impl DesktopSecretStore {
    fn open(state_dir: &Path) -> Result<Self> {
        let backend_preference = if cfg!(test) {
            BackendPreference::EncryptedFile
        } else {
            BackendPreference::Auto
        };
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(state_dir.join("vault")),
            identity_store_root: Some(state_dir.join("identity")),
            backend_preference,
            max_secret_bytes: DESKTOP_SECRET_MAX_BYTES,
        })
        .map_err(|error| anyhow!("failed to initialize desktop secret store: {error}"))?;
        Ok(Self { vault })
    }

    fn load_or_create_secret(&self, key: &str, legacy_value: Option<&str>) -> Result<String> {
        let scope = VaultScope::Global;
        if let Some(value) = self.read_secret_utf8(&scope, key)? {
            return Ok(value);
        }

        let value = normalize_optional_text(legacy_value.unwrap_or_default())
            .map(ToOwned::to_owned)
            .unwrap_or_else(generate_secret_token);
        self.vault
            .put_secret(&scope, key, value.as_bytes())
            .map_err(|error| anyhow!("failed to persist desktop secret '{key}': {error}"))?;
        Ok(value)
    }

    fn read_secret_utf8(&self, scope: &VaultScope, key: &str) -> Result<Option<String>> {
        match self.vault.get_secret(scope, key) {
            Ok(raw) => {
                let decoded = String::from_utf8(raw).with_context(|| {
                    format!("desktop secret '{key}' contains non UTF-8 bytes")
                })?;
                if decoded.trim().is_empty() {
                    return Ok(None);
                }
                Ok(Some(decoded))
            }
            Err(VaultError::NotFound) => Ok(None),
            Err(error) => Err(anyhow!("failed to read desktop secret '{key}': {error}")),
        }
    }
}

const fn default_legacy_schema_version() -> u32 {
    1
}

const fn default_browser_service_enabled() -> bool {
    true
}

#[derive(Debug, Serialize)]
struct ServiceProcessSnapshot {
    service: String,
    desired_running: bool,
    running: bool,
    liveness: String,
    pid: Option<u32>,
    last_start_unix_ms: Option<i64>,
    last_exit: Option<String>,
    restart_attempt: u32,
    next_restart_unix_ms: Option<i64>,
    bound_ports: Vec<u16>,
}

#[derive(Debug, Deserialize)]
struct HealthEndpointPayload {
    status: String,
    version: String,
    git_hash: String,
    uptime_seconds: u64,
}

#[derive(Debug)]
struct ControlCenter {
    runtime_root: PathBuf,
    support_bundle_dir: PathBuf,
    state_file_path: PathBuf,
    persisted: DesktopStateFile,
    admin_token: String,
    browser_auth_token: String,
    runtime: RuntimeConfig,
    gateway: ManagedService,
    browserd: ManagedService,
    http_client: Client,
    log_tx: mpsc::Sender<LogEvent>,
    log_rx: mpsc::Receiver<LogEvent>,
    dropped_log_events: Arc<AtomicU64>,
}

impl ControlCenter {
    fn new() -> Result<Self> {
        let state_root = resolve_desktop_state_root()?;
        let state_dir = state_root.join("desktop-control-center");
        let runtime_root = state_dir.join("runtime");
        let support_bundle_dir = state_dir.join("support-bundles");
        fs::create_dir_all(runtime_root.as_path()).with_context(|| {
            format!("failed to create desktop runtime dir {}", runtime_root.display())
        })?;
        fs::create_dir_all(support_bundle_dir.as_path()).with_context(|| {
            format!("failed to create support bundle output dir {}", support_bundle_dir.display())
        })?;

        let state_file_path = state_dir.join("state.json");
        let secret_store = DesktopSecretStore::open(state_dir.as_path())?;
        let loaded = load_or_initialize_state_file(state_file_path.as_path(), &secret_store)?;

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
            .context("failed to build desktop HTTP client")?;

        let (log_tx, log_rx) = mpsc::channel(LOG_EVENT_CHANNEL_CAPACITY);
        let dropped_log_events = Arc::new(AtomicU64::new(0));

        Ok(Self {
            runtime_root,
            support_bundle_dir,
            state_file_path,
            persisted: loaded.persisted,
            admin_token: loaded.admin_token,
            browser_auth_token: loaded.browser_auth_token,
            runtime,
            gateway,
            browserd,
            http_client,
            log_tx,
            log_rx,
            dropped_log_events,
        })
    }

    fn save_state_file(&self) -> Result<()> {
        let encoded = serde_json::to_string_pretty(&self.persisted)
            .context("failed to encode desktop state file")?;
        fs::write(self.state_file_path.as_path(), encoded).with_context(|| {
            format!("failed to persist desktop state file {}", self.state_file_path.display())
        })
    }

    fn start_all(&mut self) {
        self.gateway.desired_running = true;
        self.browserd.desired_running = self.persisted.browser_service_enabled;
        self.gateway.next_restart_unix_ms = Some(unix_ms_now());
        self.browserd.next_restart_unix_ms = Some(unix_ms_now());
        self.append_supervisor_log(ServiceKind::Gateway, "start requested for gateway sidecar");
        if self.persisted.browser_service_enabled {
            self.append_supervisor_log(
                ServiceKind::Browserd,
                "start requested for browser sidecar",
            );
        }
    }

    fn stop_all(&mut self) {
        self.stop_service(ServiceKind::Browserd);
        self.stop_service(ServiceKind::Gateway);
    }

    fn restart_all(&mut self) {
        self.stop_all();
        self.start_all();
    }

    fn stop_service(&mut self, kind: ServiceKind) {
        let mut log_message: Option<String> = None;
        {
            let service = self.service_mut(kind);
            service.desired_running = false;
            service.next_restart_unix_ms = None;
            service.restart_attempt = 0;
            if let Some(child) = service.child.as_mut() {
                log_message = Some(match child.start_kill() {
                    Ok(()) => "stop signal sent to child process".to_owned(),
                    Err(error) => sanitize_log_line(
                        format!("failed to stop {} process: {error}", kind.display_name()).as_str(),
                    ),
                });
            }
        }
        if let Some(message) = log_message {
            self.append_supervisor_log(kind, message.as_str());
        }
    }

    fn set_browser_service_enabled(&mut self, enabled: bool) -> Result<()> {
        if self.persisted.browser_service_enabled == enabled {
            return Ok(());
        }
        self.persisted.browser_service_enabled = enabled;
        self.save_state_file()?;

        if !enabled {
            self.stop_service(ServiceKind::Browserd);
        }

        if self.gateway.desired_running {
            self.restart_all();
        }
        Ok(())
    }

    fn refresh_runtime_state(&mut self) {
        self.drain_log_events();
        self.check_process_exit(ServiceKind::Gateway);
        self.check_process_exit(ServiceKind::Browserd);
        self.reconcile_service(ServiceKind::Gateway);
        self.reconcile_service(ServiceKind::Browserd);
    }

    fn drain_log_events(&mut self) {
        while let Ok(event) = self.log_rx.try_recv() {
            self.append_log_line(event.service, event.stream, event.unix_ms, event.line);
        }
    }
    fn check_process_exit(&mut self, kind: ServiceKind) {
        let now_unix_ms = unix_ms_now();
        let exit = {
            let service = self.service_mut(kind);
            let Some(child) = service.child.as_mut() else {
                return;
            };

            match child.try_wait() {
                Ok(Some(status)) => Some(Ok(status)),
                Ok(None) => None,
                Err(error) => Some(Err(error.to_string())),
            }
        };

        let Some(exit) = exit else {
            return;
        };

        let log_message: String;

        {
            let service = self.service_mut(kind);
            service.child = None;
            service.pid = None;

            let (status_message, was_success) = match exit {
                Ok(status) => {
                    let message = if let Some(code) = status.code() {
                        format!("{} exited with code {code}", kind.display_name())
                    } else {
                        format!("{} exited without a numeric status code", kind.display_name())
                    };
                    (message, status.success())
                }
                Err(error) => {
                    let message = format!("{} exit check failed: {error}", kind.display_name());
                    (message, false)
                }
            };

            service.last_exit = Some(sanitize_log_line(status_message.as_str()));

            if !service.desired_running {
                service.restart_attempt = 0;
                service.next_restart_unix_ms = None;
                log_message = "process stopped".to_owned();
            } else {
                let recent_start = service
                    .last_start_unix_ms
                    .map(|started| now_unix_ms.saturating_sub(started))
                    .unwrap_or_default();

                if was_success || recent_start > 60_000 {
                    service.restart_attempt = 0;
                }

                let backoff_ms = compute_backoff_ms(service.restart_attempt);
                service.restart_attempt = service.restart_attempt.saturating_add(1);
                service.next_restart_unix_ms = Some(now_unix_ms.saturating_add(backoff_ms as i64));
                log_message = format!(
                    "{} crashed or exited unexpectedly; restart in {} ms",
                    kind.display_name(),
                    backoff_ms
                );
            }
        }

        self.append_supervisor_log(kind, log_message.as_str());
    }

    fn reconcile_service(&mut self, kind: ServiceKind) {
        let now_unix_ms = unix_ms_now();
        let should_spawn = {
            let service = self.service_mut(kind);
            if !service.desired_running || service.running() {
                false
            } else {
                service.next_restart_unix_ms.map(|deadline| now_unix_ms >= deadline).unwrap_or(true)
            }
        };

        if !should_spawn {
            return;
        }

        if let Err(error) = self.spawn_service(kind) {
            let message;
            {
                let service = self.service_mut(kind);
                let backoff_ms = compute_backoff_ms(service.restart_attempt);
                service.restart_attempt = service.restart_attempt.saturating_add(1);
                service.next_restart_unix_ms = Some(now_unix_ms.saturating_add(backoff_ms as i64));
                message = format!(
                    "failed to spawn {}: {}; retry in {} ms",
                    kind.display_name(),
                    sanitize_log_line(error.to_string().as_str()),
                    backoff_ms
                );
            }
            self.append_supervisor_log(kind, message.as_str());
        }
    }

    fn spawn_service(&mut self, kind: ServiceKind) -> Result<()> {
        let binary_path = resolve_binary_path(kind.binary_name(), kind.env_override())?;
        let mut command = Command::new(binary_path.as_path());
        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());

        match kind {
            ServiceKind::Gateway => {
                command
                    .arg("--bind")
                    .arg(LOOPBACK_HOST)
                    .arg("--port")
                    .arg(self.runtime.gateway_admin_port.to_string())
                    .arg("--grpc-bind")
                    .arg(LOOPBACK_HOST)
                    .arg("--grpc-port")
                    .arg(self.runtime.gateway_grpc_port.to_string());
                for (key, value) in self.gateway_env() {
                    command.env(key, value);
                }
            }
            ServiceKind::Browserd => {
                command
                    .arg("--bind")
                    .arg(LOOPBACK_HOST)
                    .arg("--port")
                    .arg(self.runtime.browser_health_port.to_string())
                    .arg("--grpc-bind")
                    .arg(LOOPBACK_HOST)
                    .arg("--grpc-port")
                    .arg(self.runtime.browser_grpc_port.to_string())
                    .arg("--auth-token")
                    .arg(self.browser_auth_token.as_str());
                for (key, value) in self.browserd_env() {
                    command.env(key, value);
                }
            }
        }

        let mut child =
            command.spawn().with_context(|| format!("failed to spawn {}", kind.display_name()))?;

        let pid = child.id();
        let now = unix_ms_now();

        if let Some(stdout) = child.stdout.take() {
            spawn_log_reader(
                stdout,
                kind,
                LogStream::Stdout,
                self.log_tx.clone(),
                self.dropped_log_events.clone(),
                kind.display_name(),
            );
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_reader(
                stderr,
                kind,
                LogStream::Stderr,
                self.log_tx.clone(),
                self.dropped_log_events.clone(),
                kind.display_name(),
            );
        }

        {
            let service = self.service_mut(kind);
            service.child = Some(child);
            service.pid = pid;
            service.last_start_unix_ms = Some(now);
            service.next_restart_unix_ms = None;
        }

        let message = format!(
            "{} started{}",
            kind.display_name(),
            pid.map(|value| format!(" (pid={value})")).unwrap_or_default()
        );
        self.append_supervisor_log(kind, message.as_str());

        Ok(())
    }

    fn gateway_env(&self) -> Vec<(String, String)> {
        let browser_enabled = self.persisted.browser_service_enabled;
        vec![
            ("PALYRA_DEPLOYMENT_MODE".to_owned(), "local_desktop".to_owned()),
            ("PALYRA_ADMIN_REQUIRE_AUTH".to_owned(), "true".to_owned()),
            ("PALYRA_ADMIN_TOKEN".to_owned(), self.admin_token.clone()),
            ("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), CONSOLE_PRINCIPAL.to_owned()),
            ("PALYRA_STATE_ROOT".to_owned(), self.runtime_root.to_string_lossy().into_owned()),
            ("PALYRA_DAEMON_BIND_ADDR".to_owned(), LOOPBACK_HOST.to_owned()),
            ("PALYRA_DAEMON_PORT".to_owned(), self.runtime.gateway_admin_port.to_string()),
            ("PALYRA_GATEWAY_GRPC_BIND_ADDR".to_owned(), LOOPBACK_HOST.to_owned()),
            ("PALYRA_GATEWAY_GRPC_PORT".to_owned(), self.runtime.gateway_grpc_port.to_string()),
            ("PALYRA_GATEWAY_QUIC_BIND_ADDR".to_owned(), LOOPBACK_HOST.to_owned()),
            ("PALYRA_GATEWAY_QUIC_PORT".to_owned(), self.runtime.gateway_quic_port.to_string()),
            ("PALYRA_BROWSER_SERVICE_ENABLED".to_owned(), browser_enabled.to_string()),
            (
                "PALYRA_BROWSER_SERVICE_ENDPOINT".to_owned(),
                format!("http://{LOOPBACK_HOST}:{}", self.runtime.browser_grpc_port),
            ),
            (
                "PALYRA_BROWSER_SERVICE_AUTH_TOKEN".to_owned(),
                self.browser_auth_token.clone(),
            ),
        ]
    }

    fn browserd_env(&self) -> Vec<(String, String)> {
        vec![
            ("PALYRA_STATE_ROOT".to_owned(), self.runtime_root.to_string_lossy().into_owned()),
            ("PALYRA_BROWSERD_AUTH_TOKEN".to_owned(), self.browser_auth_token.clone()),
        ]
    }

    fn service_mut(&mut self, kind: ServiceKind) -> &mut ManagedService {
        match kind {
            ServiceKind::Gateway => &mut self.gateway,
            ServiceKind::Browserd => &mut self.browserd,
        }
    }

    fn service_ref(&self, kind: ServiceKind) -> &ManagedService {
        match kind {
            ServiceKind::Gateway => &self.gateway,
            ServiceKind::Browserd => &self.browserd,
        }
    }

    fn append_supervisor_log(&mut self, kind: ServiceKind, line: &str) {
        self.append_log_line(kind, LogStream::Supervisor, unix_ms_now(), line.to_owned());
    }

    fn append_log_line(
        &mut self,
        kind: ServiceKind,
        stream: LogStream,
        unix_ms: i64,
        line: String,
    ) {
        let sanitized = sanitize_log_line(line.as_str());
        if sanitized.trim().is_empty() {
            return;
        }
        let service = self.service_mut(kind);
        service.logs.push_back(LogLine {
            unix_ms,
            service: kind.as_str().to_owned(),
            stream: stream.as_str().to_owned(),
            line: sanitized,
        });
        while service.logs.len() > MAX_LOG_LINES_PER_SERVICE {
            service.logs.pop_front();
        }
    }

    fn process_snapshot(&self, kind: ServiceKind) -> ServiceProcessSnapshot {
        let service = self.service_ref(kind);
        ServiceProcessSnapshot {
            service: kind.as_str().to_owned(),
            desired_running: service.desired_running,
            running: service.running(),
            liveness: service.liveness().to_owned(),
            pid: service.pid,
            last_start_unix_ms: service.last_start_unix_ms,
            last_exit: service.last_exit.clone(),
            restart_attempt: service.restart_attempt,
            next_restart_unix_ms: service.next_restart_unix_ms,
            bound_ports: service.bound_ports.clone(),
        }
    }

    fn collect_logs(&self) -> Vec<LogLine> {
        let mut combined = Vec::new();
        combined.extend(self.gateway.logs.iter().cloned());
        combined.extend(self.browserd.logs.iter().cloned());
        combined.sort_by(|left, right| right.unix_ms.cmp(&left.unix_ms));
        combined.truncate(250);
        combined
    }

    fn open_dashboard(&self) -> Result<String> {
        let url = self.dashboard_access_target()?.url;
        webbrowser::open(url.as_str())
            .context("failed to open dashboard URL in default browser")?;
        Ok(url)
    }
}

fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() { None } else { Some(trimmed) }
}

fn compute_backoff_ms(attempt: u32) -> u64 {
    let exponent = attempt.min(5);
    let scaled = 1_000_u64.saturating_mul(1_u64 << exponent);
    scaled.min(30_000)
}

fn resolve_binary_path(binary_name: &str, env_override: &str) -> Result<PathBuf> {
    if let Ok(value) = env::var(env_override) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            let candidate = PathBuf::from(trimmed);
            if !candidate.is_absolute() {
                bail!("{env_override} must be an absolute path");
            }
            return canonicalize_explicit_binary_path(candidate.as_path(), env_override);
        }
    }

    let executable_name = executable_file_name(binary_name);
    let mut candidates = Vec::<PathBuf>::new();

    if let Ok(current_exe) = env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join(executable_name.as_str()));
        }
        for ancestor in current_exe.ancestors().take(8) {
            candidates.push(ancestor.join("target").join("debug").join(executable_name.as_str()));
            candidates.push(ancestor.join("target").join("release").join(executable_name.as_str()));
        }
    }

    for candidate in candidates {
        if let Some(canonical) = canonicalize_binary_candidate(candidate.as_path()) {
            return Ok(canonical);
        }
    }

    bail!(
        "unable to locate '{}'; set {} to an absolute path or place the binary next to the desktop executable",
        binary_name,
        env_override
    )
}

fn canonicalize_explicit_binary_path(candidate: &Path, env_override: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(candidate).with_context(|| {
        format!("{env_override} points to '{}', but that file does not exist", candidate.display())
    })?;
    let metadata = fs::metadata(canonical.as_path()).with_context(|| {
        format!(
            "{env_override} points to '{}', but file metadata could not be read",
            canonical.display()
        )
    })?;
    if !metadata.is_file() {
        bail!(
            "{env_override} points to '{}', but that path is not a file",
            canonical.display()
        );
    }
    Ok(canonical)
}

fn canonicalize_binary_candidate(candidate: &Path) -> Option<PathBuf> {
    let canonical = fs::canonicalize(candidate).ok()?;
    let metadata = fs::metadata(canonical.as_path()).ok()?;
    if metadata.is_file() { Some(canonical) } else { None }
}

fn executable_file_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_owned()
    }
}

fn resolve_desktop_state_root() -> Result<PathBuf> {
    palyra_common::default_state_root().map_err(|error| {
        anyhow!("failed to resolve default state root for desktop control center: {}", error)
    })
}

fn load_or_initialize_state_file(
    path: &Path,
    secret_store: &DesktopSecretStore,
) -> Result<LoadedDesktopState> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create desktop state directory {}", parent.display())
        })?;
    }

    if path.exists() {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read desktop state file {}", path.display()))?;
        let legacy_state: LegacyDesktopStateFile = serde_json::from_str(raw.as_str())
            .with_context(|| format!("failed to parse desktop state file {}", path.display()))?;
        let admin_token = secret_store
            .load_or_create_secret(DESKTOP_SECRET_KEY_ADMIN_TOKEN, Some(legacy_state.admin_token.as_str()))?;
        let browser_auth_token = secret_store.load_or_create_secret(
            DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN,
            Some(legacy_state.browser_auth_token.as_str()),
        )?;
        let persisted = legacy_state.into_state();
        persist_desktop_state_file(path, &persisted, "normalized")?;
        return Ok(LoadedDesktopState { persisted, admin_token, browser_auth_token });
    }

    let persisted = DesktopStateFile::new_default();
    let admin_token = secret_store.load_or_create_secret(DESKTOP_SECRET_KEY_ADMIN_TOKEN, None)?;
    let browser_auth_token =
        secret_store.load_or_create_secret(DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN, None)?;
    persist_desktop_state_file(path, &persisted, "default")?;
    Ok(LoadedDesktopState { persisted, admin_token, browser_auth_token })
}

fn persist_desktop_state_file(path: &Path, state: &DesktopStateFile, label: &str) -> Result<()> {
    let encoded = serde_json::to_string_pretty(state)
        .with_context(|| format!("failed to encode {label} desktop state file"))?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to persist {label} desktop state file {}", path.display()))
}

fn generate_secret_token() -> String {
    format!("{}{}", Ulid::new(), Ulid::new())
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn spawn_log_reader(
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    service: ServiceKind,
    stream: LogStream,
    sender: mpsc::Sender<LogEvent>,
    dropped_counter: Arc<AtomicU64>,
    process_name: &'static str,
) {
    tauri::async_runtime::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let event = LogEvent { unix_ms: unix_ms_now(), service, stream, line };
                    if !try_enqueue_log_event(&sender, dropped_counter.as_ref(), event) {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let event = LogEvent {
                        unix_ms: unix_ms_now(),
                        service,
                        stream: LogStream::Supervisor,
                        line: format!("{process_name} log stream read failed: {error}"),
                    };
                    let _ = try_enqueue_log_event(&sender, dropped_counter.as_ref(), event);
                    break;
                }
            }
        }
    });
}

fn try_enqueue_log_event(
    sender: &mpsc::Sender<LogEvent>,
    dropped_counter: &AtomicU64,
    event: LogEvent,
) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            dropped_counter.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

#[cfg(test)]
mod tests;

pub fn run() {
    commands::run();
}
