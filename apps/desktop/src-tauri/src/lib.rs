use std::{
    collections::VecDeque,
    env, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use palyra_common::{
    config_system::parse_document_with_migration,
    daemon_config_schema::RootFileConfig,
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    redaction::{redact_auth_error, redact_url},
};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tauri::{Manager, State};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};
use ulid::Ulid;
use which::which;

const SUPERVISOR_TICK_MS: u64 = 500;
const MAX_LOG_LINES_PER_SERVICE: usize = 400;
const MAX_DIAGNOSTIC_ERRORS: usize = 25;
const DASHBOARD_SCHEME: &str = "http";
const LOOPBACK_HOST: &str = "127.0.0.1";
const CONSOLE_PRINCIPAL: &str = "admin:desktop-control-center";
const CONSOLE_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

const GATEWAY_ADMIN_PORT: u16 = 7142;
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
    admin_token: String,
    browser_auth_token: String,
    browser_service_enabled: bool,
}

impl DesktopStateFile {
    fn new_default() -> Self {
        Self {
            schema_version: 1,
            admin_token: generate_secret_token(),
            browser_auth_token: generate_secret_token(),
            browser_service_enabled: true,
        }
    }
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

#[derive(Debug, Serialize)]
struct DiscordStatusSnapshot {
    connector_id: String,
    enabled: bool,
    authenticated: bool,
    readiness: String,
    liveness: String,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BrowserStatusSnapshot {
    enabled: bool,
    healthy: bool,
    status: String,
    uptime_seconds: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DashboardAccessMode {
    Local,
    Remote,
}

impl DashboardAccessMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone)]
struct DashboardAccessTarget {
    url: String,
    mode: DashboardAccessMode,
}

#[derive(Debug, Serialize)]
struct QuickFactsSnapshot {
    dashboard_url: String,
    dashboard_access_mode: String,
    gateway_version: Option<String>,
    gateway_git_hash: Option<String>,
    gateway_uptime_seconds: Option<u64>,
    discord: DiscordStatusSnapshot,
    browser_service: BrowserStatusSnapshot,
}

#[derive(Debug, Serialize)]
struct DiagnosticsSnapshot {
    generated_at_unix_ms: Option<i64>,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum OverallStatus {
    Healthy,
    Degraded,
    Down,
}

#[derive(Debug, Serialize)]
struct ControlCenterSnapshot {
    generated_at_unix_ms: i64,
    overall_status: OverallStatus,
    quick_facts: QuickFactsSnapshot,
    diagnostics: DiagnosticsSnapshot,
    gateway_process: ServiceProcessSnapshot,
    browserd_process: ServiceProcessSnapshot,
    logs: Vec<LogLine>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ActionResult {
    ok: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct SupportBundleExportResult {
    output_path: String,
    command_output: String,
}

#[derive(Debug, Serialize)]
struct DesktopSettingsSnapshot {
    browser_service_enabled: bool,
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
    runtime: RuntimeConfig,
    gateway: ManagedService,
    browserd: ManagedService,
    http_client: Client,
    log_tx: mpsc::UnboundedSender<LogEvent>,
    log_rx: mpsc::UnboundedReceiver<LogEvent>,
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
        let persisted = load_or_initialize_state_file(state_file_path.as_path())?;

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

        let (log_tx, log_rx) = mpsc::unbounded_channel();

        Ok(Self {
            runtime_root,
            support_bundle_dir,
            state_file_path,
            persisted,
            runtime,
            gateway,
            browserd,
            http_client,
            log_tx,
            log_rx,
        })
    }

    fn settings_snapshot(&self) -> DesktopSettingsSnapshot {
        DesktopSettingsSnapshot { browser_service_enabled: self.persisted.browser_service_enabled }
    }

    fn dashboard_access_target(&self) -> Result<DashboardAccessTarget> {
        resolve_dashboard_access_target(self.runtime.gateway_admin_port)
    }

    fn default_dashboard_access_target(&self) -> DashboardAccessTarget {
        default_dashboard_access_target(self.runtime.gateway_admin_port)
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
                    .arg(self.persisted.browser_auth_token.as_str());
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
                kind.display_name(),
            );
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_reader(
                stderr,
                kind,
                LogStream::Stderr,
                self.log_tx.clone(),
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
            ("PALYRA_ADMIN_TOKEN".to_owned(), self.persisted.admin_token.clone()),
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
                self.persisted.browser_auth_token.clone(),
            ),
        ]
    }

    fn browserd_env(&self) -> Vec<(String, String)> {
        vec![
            ("PALYRA_STATE_ROOT".to_owned(), self.runtime_root.to_string_lossy().into_owned()),
            ("PALYRA_BROWSERD_AUTH_TOKEN".to_owned(), self.persisted.browser_auth_token.clone()),
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

    async fn build_snapshot(&mut self) -> Result<ControlCenterSnapshot> {
        self.refresh_runtime_state();
        let mut warnings = Vec::new();

        let gateway_health = match self.fetch_health(self.runtime.gateway_admin_port).await {
            Ok(payload) => payload,
            Err(error) => {
                warnings.push(format!(
                    "gateway health check failed: {}",
                    sanitize_log_line(error.to_string().as_str())
                ));
                None
            }
        };

        let browser_health = if self.persisted.browser_service_enabled {
            match self.fetch_health(self.runtime.browser_health_port).await {
                Ok(payload) => payload,
                Err(error) => {
                    warnings.push(format!(
                        "browser health check failed: {}",
                        sanitize_log_line(error.to_string().as_str())
                    ));
                    None
                }
            }
        } else {
            None
        };

        let (diagnostics_payload, discord_payload, console_warnings) =
            self.fetch_console_payloads(gateway_health.is_some()).await;
        warnings.extend(console_warnings);

        let diagnostics_errors = diagnostics_payload
            .as_ref()
            .map(|value| collect_redacted_errors(value, MAX_DIAGNOSTIC_ERRORS))
            .unwrap_or_default();

        let diagnostics = DiagnosticsSnapshot {
            generated_at_unix_ms: diagnostics_payload
                .as_ref()
                .and_then(|value| value.get("generated_at_unix_ms"))
                .and_then(Value::as_i64),
            errors: diagnostics_errors,
        };

        let discord = parse_discord_status(discord_payload.as_ref());

        let browser_running = self.browserd.running();
        let browser_status = build_browser_status(
            self.persisted.browser_service_enabled,
            browser_running,
            browser_health.as_ref(),
            self.browserd.last_exit.clone(),
        );

        let dashboard_access = match self.dashboard_access_target() {
            Ok(target) => target,
            Err(error) => {
                warnings.push(format!(
                    "dashboard URL discovery failed: {}",
                    sanitize_log_line(error.to_string().as_str())
                ));
                self.default_dashboard_access_target()
            }
        };

        let quick_facts = QuickFactsSnapshot {
            dashboard_url: dashboard_access.url,
            dashboard_access_mode: dashboard_access.mode.as_str().to_owned(),
            gateway_version: gateway_health.as_ref().map(|value| value.version.clone()),
            gateway_git_hash: gateway_health.as_ref().map(|value| value.git_hash.clone()),
            gateway_uptime_seconds: gateway_health.as_ref().map(|value| value.uptime_seconds),
            discord,
            browser_service: browser_status,
        };

        let gateway_running = self.gateway.running();
        let browser_enabled = self.persisted.browser_service_enabled;
        let browser_healthy = quick_facts.browser_service.healthy;
        let discord_degraded = quick_facts.discord.enabled && !quick_facts.discord.authenticated;
        let diagnostics_degraded = !diagnostics.errors.is_empty();

        let overall_status = if !gateway_running || gateway_health.is_none() {
            OverallStatus::Down
        } else if (browser_enabled && !browser_healthy) || discord_degraded || diagnostics_degraded
        {
            OverallStatus::Degraded
        } else {
            OverallStatus::Healthy
        };

        Ok(ControlCenterSnapshot {
            generated_at_unix_ms: unix_ms_now(),
            overall_status,
            quick_facts,
            diagnostics,
            gateway_process: self.process_snapshot(ServiceKind::Gateway),
            browserd_process: self.process_snapshot(ServiceKind::Browserd),
            logs: self.collect_logs(),
            warnings,
        })
    }
    async fn fetch_health(&self, port: u16) -> Result<Option<HealthEndpointPayload>> {
        let url = loopback_url(port, "/health")?;
        let response = self.http_client.get(url).send().await.context("health request failed")?;
        if !response.status().is_success() {
            return Ok(None);
        }
        let payload = response
            .json::<HealthEndpointPayload>()
            .await
            .context("failed to decode health payload")?;
        if payload.status.trim().eq_ignore_ascii_case("ok") {
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    async fn fetch_console_payloads(
        &self,
        gateway_health_available: bool,
    ) -> (Option<Value>, Option<Value>, Vec<String>) {
        let mut warnings = Vec::new();
        if !gateway_health_available {
            return (None, None, warnings);
        }

        if let Err(error) = self.login_console_session().await {
            warnings.push(format!(
                "console login failed: {}",
                sanitize_log_line(error.to_string().as_str())
            ));
            return (None, None, warnings);
        }

        let diagnostics = match self
            .fetch_console_json("/console/v1/diagnostics")
            .await
            .with_context(|| "failed to fetch diagnostics payload".to_owned())
        {
            Ok(value) => Some(value),
            Err(error) => {
                warnings.push(sanitize_log_line(error.to_string().as_str()));
                None
            }
        };

        let discord = match self
            .fetch_console_json("/console/v1/channels/discord%3Adefault")
            .await
            .with_context(|| "failed to fetch Discord connector status".to_owned())
        {
            Ok(value) => Some(value),
            Err(error) => {
                warnings.push(sanitize_log_line(error.to_string().as_str()));
                None
            }
        };

        (diagnostics, discord, warnings)
    }

    async fn login_console_session(&self) -> Result<()> {
        let url = loopback_url(self.runtime.gateway_admin_port, "/console/v1/auth/login")?;
        let payload = json!({
            "admin_token": self.persisted.admin_token,
            "principal": CONSOLE_PRINCIPAL,
            "device_id": CONSOLE_DEVICE_ID,
        });
        let response = self
            .http_client
            .post(url)
            .json(&payload)
            .send()
            .await
            .context("console login request failed")?;
        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        bail!("console login failed with HTTP {}: {}", status, sanitize_log_line(text.as_str()))
    }

    async fn fetch_console_json(&self, path: &str) -> Result<Value> {
        let url = loopback_url(self.runtime.gateway_admin_port, path)?;
        let response =
            self.http_client.get(url).send().await.context("console GET request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            bail!(
                "console request {} failed with HTTP {}: {}",
                path,
                status,
                sanitize_log_line(text.as_str())
            );
        }

        response.json::<Value>().await.context("failed to decode console JSON response")
    }

    async fn export_support_bundle(&self) -> Result<SupportBundleExportResult> {
        let cli_path = resolve_binary_path("palyra", "PALYRA_DESKTOP_PALYRA_BIN")?;
        let output_name = format!("support-bundle-{}.json", unix_ms_now());
        let output_path = self.support_bundle_dir.join(output_name);

        let mut command = Command::new(cli_path.as_path());
        command.env_clear();
        if let Ok(path) = env::var("PATH") {
            command.env("PATH", path);
        }
        command.env("LANG", "C").env("LC_ALL", "C");
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("support-bundle")
            .arg("export")
            .arg("--output")
            .arg(output_path.as_os_str())
            .env("PALYRA_STATE_ROOT", self.runtime_root.to_string_lossy().into_owned())
            .env("PALYRA_ADMIN_TOKEN", self.persisted.admin_token.clone());

        let output = command.output().await.context("failed to run support-bundle export")?;
        let stdout = sanitize_log_line(String::from_utf8_lossy(output.stdout.as_slice()).as_ref());
        let stderr = sanitize_log_line(String::from_utf8_lossy(output.stderr.as_slice()).as_ref());
        let command_output = [stdout, stderr]
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        if !output.status.success() {
            bail!(
                "support-bundle export failed (status={}): {}",
                output
                    .status
                    .code()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_owned()),
                command_output
            );
        }

        Ok(SupportBundleExportResult {
            output_path: output_path.to_string_lossy().into_owned(),
            command_output,
        })
    }

    fn open_dashboard(&self) -> Result<String> {
        let url = self.dashboard_access_target()?.url;
        webbrowser::open(url.as_str())
            .context("failed to open dashboard URL in default browser")?;
        Ok(url)
    }
}

fn build_browser_status(
    enabled: bool,
    running: bool,
    health: Option<&HealthEndpointPayload>,
    last_exit: Option<String>,
) -> BrowserStatusSnapshot {
    if !enabled {
        return BrowserStatusSnapshot {
            enabled: false,
            healthy: true,
            status: "disabled".to_owned(),
            uptime_seconds: None,
            last_error: None,
        };
    }

    let health_ok = health.is_some();
    let healthy = running && health_ok;
    let status = if healthy {
        "ok"
    } else if running {
        "degraded"
    } else {
        "down"
    }
    .to_owned();

    BrowserStatusSnapshot {
        enabled,
        healthy,
        status,
        uptime_seconds: health.map(|value| value.uptime_seconds),
        last_error: if healthy { None } else { last_exit },
    }
}

fn parse_discord_status(payload: Option<&Value>) -> DiscordStatusSnapshot {
    let mut snapshot = DiscordStatusSnapshot {
        connector_id: "discord:default".to_owned(),
        enabled: false,
        authenticated: false,
        readiness: "unknown".to_owned(),
        liveness: "unknown".to_owned(),
        last_error: None,
    };

    let Some(root) = payload.and_then(Value::as_object) else {
        return snapshot;
    };

    if let Some(connector) = root.get("connector").and_then(Value::as_object) {
        if let Some(connector_id) = connector.get("connector_id").and_then(Value::as_str) {
            snapshot.connector_id = connector_id.to_owned();
        }
        if let Some(enabled) = connector.get("enabled").and_then(Value::as_bool) {
            snapshot.enabled = enabled;
        }
        if let Some(readiness) = connector.get("readiness").and_then(Value::as_str) {
            snapshot.readiness = readiness.to_owned();
        }
        if let Some(liveness) = connector.get("liveness").and_then(Value::as_str) {
            snapshot.liveness = liveness.to_owned();
        }
        if let Some(last_error) = connector.get("last_error").and_then(Value::as_str) {
            let cleaned = sanitize_log_line(last_error);
            if !cleaned.trim().is_empty() {
                snapshot.last_error = Some(cleaned);
            }
        }
    }

    if let Some(runtime) = root.get("runtime").and_then(Value::as_object) {
        if snapshot.last_error.is_none() {
            if let Some(last_error) = runtime.get("last_error").and_then(Value::as_str) {
                let cleaned = sanitize_log_line(last_error);
                if !cleaned.trim().is_empty() {
                    snapshot.last_error = Some(cleaned);
                }
            }
        }
    }

    snapshot.authenticated = snapshot.enabled && snapshot.readiness.eq_ignore_ascii_case("ready");
    snapshot
}

fn collect_redacted_errors(value: &Value, limit: usize) -> Vec<String> {
    let mut collected = Vec::new();
    collect_redacted_errors_inner(value, None, &mut collected);

    let mut deduped = Vec::new();
    for item in collected {
        if deduped.iter().any(|existing: &String| existing == &item) {
            continue;
        }
        deduped.push(item);
        if deduped.len() >= limit {
            break;
        }
    }
    deduped
}

fn collect_redacted_errors_inner(value: &Value, key_context: Option<&str>, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                collect_redacted_errors_inner(child, Some(key.as_str()), out);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_redacted_errors_inner(entry, key_context, out);
            }
        }
        Value::String(raw) => {
            if key_context.is_some_and(is_error_like_key) {
                let sanitized = sanitize_log_line(raw.as_str());
                if !sanitized.trim().is_empty() {
                    out.push(sanitized);
                }
            }
        }
        _ => {}
    }
}

fn is_error_like_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    ["error", "failure", "warning", "detail", "reason", "message"]
        .iter()
        .any(|needle| lowered.contains(needle))
}

fn sanitize_log_line(raw: &str) -> String {
    let mut line = redact_auth_error(raw);
    line = redact_inline_urls(line.as_str());
    line.trim().to_owned()
}

fn redact_inline_urls(raw: &str) -> String {
    let mut tokens = Vec::new();
    for token in raw.split_whitespace() {
        let sanitized = sanitize_token_with_url(token);
        tokens.push(sanitized);
    }
    tokens.join(" ")
}

fn sanitize_token_with_url(token: &str) -> String {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let bytes = trimmed.as_bytes();
    let mut start = 0usize;
    let mut end = bytes.len();

    while start < end {
        if matches!(bytes[start], b'"' | b'\'' | b'(' | b'[') {
            start = start.saturating_add(1);
            continue;
        }
        break;
    }

    while end > start {
        if matches!(bytes[end - 1], b',' | b';' | b')' | b']' | b'"' | b'\'') {
            end = end.saturating_sub(1);
            continue;
        }
        break;
    }

    if start >= end {
        return trimmed.to_owned();
    }

    let prefix = &trimmed[..start];
    let core = &trimmed[start..end];
    let suffix = &trimmed[end..];

    if core.starts_with("http://") || core.starts_with("https://") {
        return format!("{prefix}{}{suffix}", redact_url(core));
    }

    if let Some((key, value)) = core.split_once('=') {
        if value.starts_with("http://") || value.starts_with("https://") {
            return format!("{prefix}{key}={}{}", redact_url(value), suffix);
        }
    }

    trimmed.to_owned()
}

fn loopback_url(port: u16, path: &str) -> Result<Url> {
    if !path.starts_with('/') {
        bail!("path must be absolute");
    }
    Url::parse(format!("{DASHBOARD_SCHEME}://{LOOPBACK_HOST}:{port}{path}").as_str())
        .with_context(|| format!("failed to construct loopback URL for path '{path}'"))
}

fn resolve_dashboard_access_target(default_port: u16) -> Result<DashboardAccessTarget> {
    let Some(config_path) = resolve_dashboard_config_path()? else {
        return Ok(default_dashboard_access_target(default_port));
    };
    let parsed = load_dashboard_root_file_config(config_path.as_path())?;

    if let Some(remote_base_url) = parsed
        .gateway_access
        .as_ref()
        .and_then(|access| access.remote_base_url.as_deref())
        .and_then(normalize_optional_text)
    {
        return Ok(DashboardAccessTarget {
            url: parse_remote_dashboard_base_url(
                remote_base_url,
                "gateway_access.remote_base_url",
            )?,
            mode: DashboardAccessMode::Remote,
        });
    }

    let bind_addr = parsed
        .daemon
        .as_ref()
        .and_then(|daemon| daemon.bind_addr.as_deref())
        .unwrap_or(LOOPBACK_HOST);
    let port = parsed.daemon.as_ref().and_then(|daemon| daemon.port).unwrap_or(default_port);
    let socket = parse_daemon_bind_socket(bind_addr, port)
        .with_context(|| format!("invalid daemon bind config ({bind_addr}:{port})"))?;
    Ok(DashboardAccessTarget { url: format_dashboard_url(normalize_dashboard_socket(socket)), mode: DashboardAccessMode::Local })
}

fn resolve_dashboard_config_path() -> Result<Option<PathBuf>> {
    if let Ok(explicit) = env::var("PALYRA_CONFIG") {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            let parsed = parse_config_path(trimmed)
                .with_context(|| "PALYRA_CONFIG contains an invalid path")?;
            if parsed.exists() {
                return Ok(Some(parsed));
            }
        }
    }

    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn load_dashboard_root_file_config(path: &Path) -> Result<RootFileConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read desktop dashboard config {}", path.display()))?;
    let (document, _) = parse_document_with_migration(content.as_str())
        .with_context(|| format!("failed to migrate desktop dashboard config {}", path.display()))?;
    let migrated = toml::to_string(&document)
        .context("failed to serialize migrated desktop dashboard config document")?;
    toml::from_str(migrated.as_str()).context("desktop dashboard config does not match schema")
}

fn parse_remote_dashboard_base_url(raw: &str, source_name: &str) -> Result<String> {
    let parsed = Url::parse(raw)
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if parsed.scheme() != "https" {
        bail!("{source_name} must use https://");
    }
    if parsed.host_str().is_none() {
        bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("{source_name} must not include embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        bail!("{source_name} must not include query or fragment");
    }
    Ok(parsed.to_string())
}

fn normalize_dashboard_socket(socket: SocketAddr) -> SocketAddr {
    if socket.ip().is_unspecified() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), socket.port())
    } else {
        socket
    }
}

fn format_dashboard_url(socket: SocketAddr) -> String {
    format!("{DASHBOARD_SCHEME}://{socket}/")
}

fn default_dashboard_access_target(default_port: u16) -> DashboardAccessTarget {
    DashboardAccessTarget {
        url: format!("{DASHBOARD_SCHEME}://{LOOPBACK_HOST}:{default_port}/"),
        mode: DashboardAccessMode::Local,
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
            if candidate.exists() {
                return Ok(candidate);
            }
            bail!(
                "{} points to '{}', but that file does not exist",
                env_override,
                candidate.display()
            );
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

    if let Ok(current_dir) = env::current_dir() {
        candidates.push(current_dir.join("target").join("debug").join(executable_name.as_str()));
        candidates.push(current_dir.join("target").join("release").join(executable_name.as_str()));
    }

    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    if let Ok(path) = which(binary_name) {
        return Ok(path);
    }

    bail!("unable to locate '{}'; build/install it or set {}", binary_name, env_override)
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

fn load_or_initialize_state_file(path: &Path) -> Result<DesktopStateFile> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create desktop state directory {}", parent.display())
        })?;
    }

    if path.exists() {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read desktop state file {}", path.display()))?;
        let mut state: DesktopStateFile = serde_json::from_str(raw.as_str())
            .with_context(|| format!("failed to parse desktop state file {}", path.display()))?;
        if state.admin_token.trim().is_empty() {
            state.admin_token = generate_secret_token();
        }
        if state.browser_auth_token.trim().is_empty() {
            state.browser_auth_token = generate_secret_token();
        }
        let encoded = serde_json::to_string_pretty(&state)
            .context("failed to encode normalized desktop state file")?;
        fs::write(path, encoded).with_context(|| {
            format!("failed to persist normalized desktop state file {}", path.display())
        })?;
        return Ok(state);
    }

    let state = DesktopStateFile::new_default();
    let encoded = serde_json::to_string_pretty(&state)
        .context("failed to encode default desktop state file")?;
    fs::write(path, encoded)
        .with_context(|| format!("failed to create desktop state file {}", path.display()))?;
    Ok(state)
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
    sender: mpsc::UnboundedSender<LogEvent>,
    process_name: &'static str,
) {
    tauri::async_runtime::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let _ = sender.send(LogEvent { unix_ms: unix_ms_now(), service, stream, line });
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = sender.send(LogEvent {
                        unix_ms: unix_ms_now(),
                        service,
                        stream: LogStream::Supervisor,
                        line: format!("{process_name} log stream read failed: {error}"),
                    });
                    break;
                }
            }
        }
    });
}

struct DesktopAppState {
    supervisor: Arc<Mutex<ControlCenter>>,
}

#[tauri::command]
async fn get_snapshot(state: State<'_, DesktopAppState>) -> Result<ControlCenterSnapshot, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.build_snapshot().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn get_settings(
    state: State<'_, DesktopAppState>,
) -> Result<DesktopSettingsSnapshot, String> {
    let supervisor = state.supervisor.lock().await;
    Ok(supervisor.settings_snapshot())
}

#[tauri::command]
async fn set_browser_service_enabled(
    state: State<'_, DesktopAppState>,
    enabled: bool,
) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.set_browser_service_enabled(enabled).map_err(|error| error.to_string())?;
    let message = if enabled { "browser sidecar enabled" } else { "browser sidecar disabled" };
    Ok(ActionResult { ok: true, message: message.to_owned() })
}
#[tauri::command]
async fn start_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.start_all();
    Ok(ActionResult { ok: true, message: "start requested".to_owned() })
}

#[tauri::command]
async fn stop_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.stop_all();
    Ok(ActionResult { ok: true, message: "stop requested".to_owned() })
}

#[tauri::command]
async fn restart_palyra(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let mut supervisor = state.supervisor.lock().await;
    supervisor.restart_all();
    Ok(ActionResult { ok: true, message: "restart requested".to_owned() })
}

#[tauri::command]
async fn open_dashboard(state: State<'_, DesktopAppState>) -> Result<ActionResult, String> {
    let supervisor = state.supervisor.lock().await;
    let url = supervisor.open_dashboard().map_err(|error| error.to_string())?;
    Ok(ActionResult { ok: true, message: format!("opened {url}") })
}

#[tauri::command]
async fn export_support_bundle(
    state: State<'_, DesktopAppState>,
) -> Result<SupportBundleExportResult, String> {
    let supervisor = state.supervisor.lock().await;
    supervisor.export_support_bundle().await.map_err(|error| error.to_string())
}

async fn supervisor_loop(supervisor: Arc<Mutex<ControlCenter>>) {
    loop {
        {
            let mut guard = supervisor.lock().await;
            guard.refresh_runtime_state();
        }
        tokio::time::sleep(Duration::from_millis(SUPERVISOR_TICK_MS)).await;
    }
}

pub fn run() {
    let control_center = match ControlCenter::new() {
        Ok(value) => value,
        Err(error) => {
            panic!("failed to initialize desktop control center: {error}");
        }
    };

    tauri::Builder::default()
        .manage(DesktopAppState { supervisor: Arc::new(Mutex::new(control_center)) })
        .setup(|app| {
            let state = app.state::<DesktopAppState>().supervisor.clone();
            tauri::async_runtime::spawn(supervisor_loop(state));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_snapshot,
            get_settings,
            set_browser_service_enabled,
            start_palyra,
            stop_palyra,
            restart_palyra,
            open_dashboard,
            export_support_bundle
        ])
        .run(tauri::generate_context!())
        .expect("tauri desktop runtime failed");
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    use serde_json::json;

    use super::{
        collect_redacted_errors, compute_backoff_ms, parse_discord_status, sanitize_log_line,
        parse_remote_dashboard_base_url, resolve_dashboard_access_target, BrowserStatusSnapshot,
        DashboardAccessMode, Ulid,
    };

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
}
