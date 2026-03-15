use std::{
    collections::VecDeque,
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use super::{
    load_or_initialize_state_file, sanitize_log_line, DesktopOnboardingStep, DesktopSecretStore,
    DesktopStateFile, BROWSER_GRPC_PORT, BROWSER_HEALTH_PORT, CONSOLE_PRINCIPAL,
    GATEWAY_ADMIN_PORT, GATEWAY_GRPC_PORT, GATEWAY_QUIC_PORT, LOG_EVENT_CHANNEL_CAPACITY,
    LOOPBACK_HOST, MAX_LOG_LINES_PER_SERVICE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceKind {
    Gateway,
    Browserd,
}

impl ServiceKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Browserd => "browserd",
        }
    }

    pub(crate) const fn display_name(self) -> &'static str {
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
pub(crate) enum LogStream {
    Stdout,
    Stderr,
    Supervisor,
}

impl LogStream {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Supervisor => "supervisor",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogLine {
    pub(crate) unix_ms: i64,
    pub(crate) service: String,
    pub(crate) stream: String,
    pub(crate) line: String,
}

#[derive(Debug)]
pub(crate) struct LogEvent {
    pub(crate) unix_ms: i64,
    pub(crate) service: ServiceKind,
    pub(crate) stream: LogStream,
    pub(crate) line: String,
}

#[derive(Debug)]
pub(crate) struct ManagedService {
    pub(crate) desired_running: bool,
    pub(crate) child: Option<Child>,
    pub(crate) pid: Option<u32>,
    pub(crate) last_start_unix_ms: Option<i64>,
    pub(crate) restart_attempt: u32,
    pub(crate) next_restart_unix_ms: Option<i64>,
    pub(crate) last_exit: Option<String>,
    pub(crate) logs: VecDeque<LogLine>,
    pub(crate) bound_ports: Vec<u16>,
}

impl ManagedService {
    pub(crate) fn new(bound_ports: Vec<u16>) -> Self {
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

    pub(crate) fn running(&self) -> bool {
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
pub(crate) struct RuntimeConfig {
    pub(crate) gateway_admin_port: u16,
    pub(crate) gateway_grpc_port: u16,
    pub(crate) gateway_quic_port: u16,
    pub(crate) browser_health_port: u16,
    pub(crate) browser_grpc_port: u16,
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

#[derive(Debug, Clone)]
pub(crate) struct ConsoleSessionCache {
    pub(crate) csrf_token: String,
    pub(crate) expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CachedConsolePayload {
    pub(crate) payload: Option<Value>,
    pub(crate) fetched_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ConsolePayloadCache {
    pub(crate) diagnostics: CachedConsolePayload,
    pub(crate) discord: CachedConsolePayload,
}

#[derive(Debug, Serialize)]
pub(crate) struct ServiceProcessSnapshot {
    pub(crate) service: String,
    pub(crate) desired_running: bool,
    pub(crate) running: bool,
    pub(crate) liveness: String,
    pub(crate) pid: Option<u32>,
    pub(crate) last_start_unix_ms: Option<i64>,
    pub(crate) last_exit: Option<String>,
    pub(crate) restart_attempt: u32,
    pub(crate) next_restart_unix_ms: Option<i64>,
    pub(crate) bound_ports: Vec<u16>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealthEndpointPayload {
    pub(crate) status: String,
    pub(crate) version: String,
    pub(crate) git_hash: String,
    pub(crate) uptime_seconds: u64,
}

#[derive(Debug)]
pub(crate) struct ControlCenter {
    pub(crate) default_runtime_root: PathBuf,
    pub(crate) runtime_root: PathBuf,
    pub(crate) support_bundle_dir: PathBuf,
    pub(crate) state_file_path: PathBuf,
    pub(crate) persisted: DesktopStateFile,
    pub(crate) admin_token: String,
    pub(crate) browser_auth_token: String,
    pub(crate) runtime: RuntimeConfig,
    pub(crate) gateway: ManagedService,
    pub(crate) browserd: ManagedService,
    pub(crate) http_client: Client,
    pub(crate) console_session_cache: Arc<Mutex<Option<ConsoleSessionCache>>>,
    pub(crate) console_payload_cache: Arc<Mutex<ConsolePayloadCache>>,
    pub(crate) log_tx: mpsc::Sender<LogEvent>,
    pub(crate) log_rx: mpsc::Receiver<LogEvent>,
    pub(crate) dropped_log_events: Arc<AtomicU64>,
}

impl ControlCenter {
    pub(crate) fn new() -> Result<Self> {
        let state_root = super::resolve_desktop_state_root()?;
        let state_dir = state_root.join("desktop-control-center");
        let default_runtime_root = state_dir.join("runtime");
        let support_bundle_dir = state_dir.join("support-bundles");
        fs::create_dir_all(support_bundle_dir.as_path()).with_context(|| {
            format!("failed to create support bundle output dir {}", support_bundle_dir.display())
        })?;

        let state_file_path = state_dir.join("state.json");
        let secret_store = DesktopSecretStore::open(state_dir.as_path())?;
        let loaded = load_or_initialize_state_file(state_file_path.as_path(), &secret_store)?;
        let runtime_root = loaded.persisted.resolve_runtime_root(default_runtime_root.as_path())?;
        fs::create_dir_all(runtime_root.as_path()).with_context(|| {
            format!("failed to create desktop runtime dir {}", runtime_root.display())
        })?;

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
            default_runtime_root,
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
            console_session_cache: Arc::new(Mutex::new(None)),
            console_payload_cache: Arc::new(Mutex::new(ConsolePayloadCache::default())),
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

    pub(crate) fn record_onboarding_event(
        &mut self,
        kind: impl Into<String>,
        detail: Option<String>,
    ) -> Result<()> {
        self.persisted.onboarding.ensure_flow_id();
        self.persisted.onboarding.push_event(kind, detail, unix_ms_now());
        self.save_state_file()
    }

    pub(crate) fn clear_onboarding_failure(&mut self) -> Result<()> {
        if self.persisted.onboarding.last_failure.is_none() {
            return Ok(());
        }
        self.persisted.onboarding.last_failure = None;
        self.save_state_file()
    }

    pub(crate) fn record_onboarding_failure(
        &mut self,
        step: DesktopOnboardingStep,
        message: String,
    ) -> Result<()> {
        let sanitized = sanitize_log_line(message.as_str());
        self.persisted.onboarding.ensure_flow_id();
        self.persisted.onboarding.record_failure_step(step);
        self.persisted.onboarding.last_failure =
            Some(super::desktop_state::DesktopOnboardingFailureState {
                step,
                message: sanitized.clone(),
                recorded_at_unix_ms: unix_ms_now(),
            });
        self.persisted.onboarding.push_event(
            "failure",
            Some(format!("{}: {}", step.as_str(), sanitized)),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn record_support_bundle_export_result(
        &mut self,
        success: bool,
        detail: Option<String>,
    ) -> Result<()> {
        self.persisted.onboarding.ensure_flow_id();
        self.persisted.onboarding.record_support_bundle_export_result(success);
        self.persisted.onboarding.push_event(
            if success {
                "support_bundle_export_succeeded"
            } else {
                "support_bundle_export_failed"
            },
            detail,
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn mark_onboarding_welcome_acknowledged(&mut self) -> Result<()> {
        if self.persisted.onboarding.welcome_acknowledged_at_unix_ms.is_none() {
            self.persisted.onboarding.welcome_acknowledged_at_unix_ms = Some(unix_ms_now());
        }
        self.persisted.onboarding.push_event(
            "welcome_acknowledged",
            Some("Desktop first-run onboarding started.".to_owned()),
            unix_ms_now(),
        );
        self.persisted.onboarding.last_failure = None;
        self.save_state_file()
    }

    pub(crate) fn set_runtime_state_root_override(
        &mut self,
        candidate: Option<&str>,
        confirm_selection: bool,
    ) -> Result<PathBuf> {
        let runtime_root =
            validate_runtime_state_root_override(candidate, self.default_runtime_root.as_path())?;
        fs::create_dir_all(runtime_root.as_path()).with_context(|| {
            format!("failed to create desktop runtime root {}", runtime_root.display())
        })?;
        self.persisted.runtime_state_root = if runtime_root == self.default_runtime_root {
            None
        } else {
            Some(runtime_root.to_string_lossy().into_owned())
        };
        self.runtime_root = runtime_root.clone();
        if confirm_selection && self.persisted.onboarding.state_root_confirmed_at_unix_ms.is_none()
        {
            self.persisted.onboarding.state_root_confirmed_at_unix_ms = Some(unix_ms_now());
        }
        self.persisted.onboarding.push_event(
            "state_root_selected",
            Some(self.runtime_root.to_string_lossy().into_owned()),
            unix_ms_now(),
        );
        self.persisted.onboarding.last_failure = None;
        self.save_state_file()?;
        Ok(runtime_root)
    }

    pub(crate) fn mark_openai_connected(
        &mut self,
        preferred_method: Option<&str>,
        profile_id: Option<&str>,
    ) -> Result<()> {
        if let Some(method) = preferred_method.and_then(normalize_optional_text) {
            self.persisted.onboarding.openai.preferred_method = Some(method.to_ascii_lowercase());
        }
        if let Some(profile_id) = profile_id.and_then(normalize_optional_text) {
            self.persisted.onboarding.openai.last_profile_id = Some(profile_id.to_owned());
        }
        self.persisted.onboarding.openai.last_connected_at_unix_ms = Some(unix_ms_now());
        self.persisted.onboarding.last_failure = None;
        self.persisted.onboarding.push_event(
            "openai_connected",
            profile_id.map(str::to_owned).or_else(|| preferred_method.map(str::to_owned)),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn update_discord_onboarding_metadata(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        let discord = &mut self.persisted.onboarding.discord;
        discord.account_id =
            normalize_optional_text(request.account_id.as_deref().unwrap_or_default())
                .unwrap_or("default")
                .to_ascii_lowercase();
        discord.mode = request.mode.clone().unwrap_or_else(|| "local".to_owned());
        discord.inbound_scope =
            request.inbound_scope.clone().unwrap_or_else(|| "dm_only".to_owned());
        discord.allow_from = request.allow_from.clone();
        discord.deny_from = request.deny_from.clone();
        discord.require_mention = request.require_mention.unwrap_or(true);
        discord.concurrency_limit = request.concurrency_limit.unwrap_or(2).clamp(1, 32);
        discord.broadcast_strategy =
            request.broadcast_strategy.clone().unwrap_or_else(|| "deny".to_owned());
        discord.confirm_open_guild_channels = request.confirm_open_guild_channels.unwrap_or(false);
        discord.verify_channel_id = request
            .verify_channel_id
            .as_deref()
            .and_then(normalize_optional_text)
            .map(str::to_owned);
        discord.last_connector_id = Some(discord.connector_id());
        self.save_state_file()
    }

    pub(crate) fn mark_discord_preflight(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        self.update_discord_onboarding_metadata(request)?;
        self.persisted.onboarding.last_failure = None;
        self.persisted.onboarding.push_event(
            "discord_preflight",
            self.persisted.onboarding.discord.verify_channel_id.clone(),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn mark_discord_applied(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        self.update_discord_onboarding_metadata(request)?;
        self.persisted.onboarding.last_failure = None;
        self.persisted.onboarding.push_event(
            "discord_applied",
            Some(self.persisted.onboarding.discord.connector_id()),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn mark_discord_verified(&mut self, connector_id: &str, target: &str) -> Result<()> {
        self.persisted.onboarding.discord.last_connector_id =
            normalize_optional_text(connector_id).map(str::to_owned);
        self.persisted.onboarding.discord.last_verified_target =
            normalize_optional_text(target).map(str::to_owned);
        self.persisted.onboarding.discord.last_verified_at_unix_ms = Some(unix_ms_now());
        self.persisted.onboarding.last_failure = None;
        self.persisted.onboarding.push_event(
            "discord_verified",
            Some(format!("{connector_id}:{target}")),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn mark_dashboard_handoff_complete(&mut self) -> Result<()> {
        if self.persisted.onboarding.dashboard_handoff_at_unix_ms.is_none() {
            self.persisted.onboarding.dashboard_handoff_at_unix_ms = Some(unix_ms_now());
        }
        self.persisted.onboarding.last_failure = None;
        self.persisted.onboarding.push_event(
            "dashboard_opened",
            Some("Dashboard handoff completed from desktop.".to_owned()),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    pub(crate) fn mark_onboarding_complete(&mut self) -> Result<()> {
        if self.persisted.onboarding.completed_at_unix_ms.is_none() {
            self.persisted.onboarding.completed_at_unix_ms = Some(unix_ms_now());
            self.persisted.onboarding.push_event(
                "onboarding_completed",
                Some("Desktop onboarding completed.".to_owned()),
                unix_ms_now(),
            );
        }
        self.persisted.onboarding.last_failure = None;
        self.save_state_file()
    }

    pub(crate) fn start_all(&mut self) {
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

    pub(crate) fn stop_all(&mut self) {
        self.stop_service(ServiceKind::Browserd);
        self.stop_service(ServiceKind::Gateway);
    }

    pub(crate) fn restart_all(&mut self) {
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

    pub(crate) fn set_browser_service_enabled(&mut self, enabled: bool) -> Result<()> {
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

    pub(crate) fn refresh_runtime_state(&mut self) {
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
        super::configure_background_command(&mut command);
        command.kill_on_drop(true);
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
            ("PALYRA_BROWSER_SERVICE_AUTH_TOKEN".to_owned(), self.browser_auth_token.clone()),
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

    pub(crate) fn process_snapshot(&self, kind: ServiceKind) -> ServiceProcessSnapshot {
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

    pub(crate) fn collect_logs(&self) -> Vec<LogLine> {
        let mut combined = Vec::new();
        combined.extend(self.gateway.logs.iter().cloned());
        combined.extend(self.browserd.logs.iter().cloned());
        combined.sort_by(|left, right| right.unix_ms.cmp(&left.unix_ms));
        combined.truncate(250);
        combined
    }

    pub(crate) fn open_dashboard(&self, url: &str) -> Result<String> {
        open_url_in_default_browser(url)?;
        Ok(url.to_owned())
    }
}

impl Drop for ControlCenter {
    fn drop(&mut self) {
        self.stop_all();
    }
}

pub(crate) fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn open_url_in_default_browser(url: &str) -> Result<()> {
    let normalized_url = normalize_browser_open_url(url)?;
    webbrowser::open(normalized_url.as_str())
        .context("failed to open dashboard URL in default browser")?;
    Ok(())
}

fn normalize_browser_open_url(raw: &str) -> Result<String> {
    let parsed =
        Url::parse(raw).with_context(|| "dashboard browser open requires a valid absolute URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        bail!("dashboard browser open only supports http:// and https:// URLs");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("dashboard browser open URL must not include embedded credentials");
    }
    Ok(parsed.to_string())
}

pub(crate) fn compute_backoff_ms(attempt: u32) -> u64 {
    let exponent = attempt.min(5);
    let scaled = 1_000_u64.saturating_mul(1_u64 << exponent);
    scaled.min(30_000)
}

#[cfg(test)]
mod tests {
    use super::normalize_browser_open_url;

    #[test]
    fn normalize_browser_open_url_accepts_http_and_https_urls() {
        let http = normalize_browser_open_url("http://127.0.0.1:7142/console")
            .expect("http dashboard URL should be accepted");
        let https = normalize_browser_open_url("https://example.test/console")
            .expect("https dashboard URL should be accepted");

        assert_eq!(http, "http://127.0.0.1:7142/console");
        assert_eq!(https, "https://example.test/console");
    }

    #[test]
    fn normalize_browser_open_url_rejects_embedded_credentials() {
        let error = normalize_browser_open_url("https://operator:secret@example.test/console")
            .expect_err("embedded credentials should be rejected");

        assert!(
            error
                .to_string()
                .contains("must not include embedded credentials"),
            "unexpected error: {error}"
        );
    }
}

pub(crate) fn validate_runtime_state_root_override(
    candidate: Option<&str>,
    default_root: &Path,
) -> Result<PathBuf> {
    let Some(raw) = candidate.and_then(normalize_optional_text) else {
        return Ok(default_root.to_path_buf());
    };
    let parsed = PathBuf::from(raw);
    if !parsed.is_absolute() {
        bail!("desktop runtime state root must be an absolute path");
    }
    Ok(parsed)
}

pub(crate) fn resolve_binary_path(binary_name: &str, env_override: &str) -> Result<PathBuf> {
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
        bail!("{env_override} points to '{}', but that path is not a file", canonical.display());
    }
    Ok(canonical)
}

fn canonicalize_binary_candidate(candidate: &Path) -> Option<PathBuf> {
    let canonical = fs::canonicalize(candidate).ok()?;
    let metadata = fs::metadata(canonical.as_path()).ok()?;
    if metadata.is_file() {
        Some(canonical)
    } else {
        None
    }
}

pub(crate) fn executable_file_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_owned()
    }
}

pub(crate) fn unix_ms_now() -> i64 {
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

pub(crate) fn try_enqueue_log_event(
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
