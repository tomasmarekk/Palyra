use std::{
    env, fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::Ordering,
};

use anyhow::{anyhow, bail, Context, Result};
use palyra_common::{
    config_system::parse_document_with_migration,
    daemon_config_schema::RootFileConfig,
    default_config_search_paths, parse_config_path, parse_daemon_bind_socket,
    redaction::{redact_auth_error, redact_url},
};
use palyra_control_plane::{self as control_plane, ControlPlaneClient, ControlPlaneClientConfig};
use reqwest::{Client, Url};
use serde::Serialize;
use serde_json::Value;
use tokio::process::Command;

use super::{
    unix_ms_now, ControlCenter, HealthEndpointPayload, LogLine, RuntimeConfig, ServiceKind,
    ServiceProcessSnapshot, CONSOLE_DEVICE_ID, CONSOLE_PRINCIPAL, DASHBOARD_SCHEME,
    LOOPBACK_HOST, MAX_DIAGNOSTIC_ERRORS, normalize_optional_text, resolve_binary_path,
};

#[derive(Debug, Serialize)]
pub(crate) struct DiscordStatusSnapshot {
    pub(crate) connector_id: String,
    pub(crate) enabled: bool,
    pub(crate) authenticated: bool,
    pub(crate) readiness: String,
    pub(crate) liveness: String,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BrowserStatusSnapshot {
    pub(crate) enabled: bool,
    pub(crate) healthy: bool,
    pub(crate) status: String,
    pub(crate) uptime_seconds: Option<u64>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DashboardAccessMode {
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
pub(crate) struct DashboardAccessTarget {
    pub(crate) url: String,
    pub(crate) mode: DashboardAccessMode,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuickFactsSnapshot {
    pub(crate) dashboard_url: String,
    pub(crate) dashboard_access_mode: String,
    pub(crate) gateway_version: Option<String>,
    pub(crate) gateway_git_hash: Option<String>,
    pub(crate) gateway_uptime_seconds: Option<u64>,
    pub(crate) discord: DiscordStatusSnapshot,
    pub(crate) browser_service: BrowserStatusSnapshot,
}

#[derive(Debug, Serialize)]
pub(crate) struct DiagnosticsSnapshot {
    pub(crate) generated_at_unix_ms: Option<i64>,
    pub(crate) errors: Vec<String>,
    pub(crate) dropped_log_events_total: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OverallStatus {
    Healthy,
    Degraded,
    Down,
}

#[derive(Debug, Serialize)]
pub(crate) struct ControlCenterSnapshot {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) overall_status: OverallStatus,
    pub(crate) quick_facts: QuickFactsSnapshot,
    pub(crate) diagnostics: DiagnosticsSnapshot,
    pub(crate) gateway_process: ServiceProcessSnapshot,
    pub(crate) browserd_process: ServiceProcessSnapshot,
    pub(crate) logs: Vec<LogLine>,
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ActionResult {
    pub(crate) ok: bool,
    pub(crate) message: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SupportBundleExportResult {
    pub(crate) output_path: String,
    pub(crate) command_output: String,
}

#[derive(Debug)]
pub(crate) struct SnapshotBuildInputs {
    pub(crate) runtime: RuntimeConfig,
    pub(crate) browser_service_enabled: bool,
    pub(crate) admin_token: String,
    pub(crate) browser_last_exit: Option<String>,
    pub(crate) dropped_log_events_total: u64,
    pub(crate) gateway_running: bool,
    pub(crate) browser_running: bool,
    pub(crate) gateway_process: ServiceProcessSnapshot,
    pub(crate) browserd_process: ServiceProcessSnapshot,
    pub(crate) logs: Vec<LogLine>,
    pub(crate) http_client: Client,
}

#[derive(Debug, Clone)]
pub(crate) struct SupportBundleExportPlan {
    pub(crate) runtime_root: PathBuf,
    pub(crate) support_bundle_dir: PathBuf,
    pub(crate) admin_token: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct DesktopSettingsSnapshot {
    pub(crate) browser_service_enabled: bool,
}


impl ControlCenter {
    pub(crate) fn settings_snapshot(&self) -> DesktopSettingsSnapshot {
        DesktopSettingsSnapshot { browser_service_enabled: self.persisted.browser_service_enabled }
    }

    pub(crate) fn dashboard_access_target(&self) -> Result<DashboardAccessTarget> {
        resolve_dashboard_access_target(self.runtime.gateway_admin_port)
    }

    pub(crate) fn capture_snapshot_inputs(&mut self) -> SnapshotBuildInputs {
        self.refresh_runtime_state();
        SnapshotBuildInputs {
            runtime: self.runtime.clone(),
            browser_service_enabled: self.persisted.browser_service_enabled,
            admin_token: self.admin_token.clone(),
            browser_last_exit: self.browserd.last_exit.clone(),
            dropped_log_events_total: self.dropped_log_events.load(Ordering::Relaxed),
            gateway_running: self.gateway.running(),
            browser_running: self.browserd.running(),
            gateway_process: self.process_snapshot(ServiceKind::Gateway),
            browserd_process: self.process_snapshot(ServiceKind::Browserd),
            logs: self.collect_logs(),
            http_client: self.http_client.clone(),
        }
    }

    pub(crate) fn prepare_support_bundle_export(&self) -> SupportBundleExportPlan {
        SupportBundleExportPlan {
            runtime_root: self.runtime_root.clone(),
            support_bundle_dir: self.support_bundle_dir.clone(),
            admin_token: self.admin_token.clone(),
        }
    }

}

pub(crate) fn build_browser_status(
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

pub(crate) fn parse_discord_status(payload: Option<&Value>) -> DiscordStatusSnapshot {
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

pub(crate) fn collect_redacted_errors(value: &Value, limit: usize) -> Vec<String> {
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

pub(crate) fn collect_redacted_errors_inner(value: &Value, key_context: Option<&str>, out: &mut Vec<String>) {
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

pub(crate) async fn build_snapshot_from_inputs(
    inputs: SnapshotBuildInputs,
) -> Result<ControlCenterSnapshot> {
    let SnapshotBuildInputs {
        runtime,
        browser_service_enabled,
        admin_token,
        browser_last_exit,
        dropped_log_events_total,
        gateway_running,
        browser_running,
        gateway_process,
        browserd_process,
        logs,
        http_client,
    } = inputs;
    let mut warnings = Vec::new();

    let gateway_health = match fetch_health(&http_client, runtime.gateway_admin_port).await {
        Ok(payload) => payload,
        Err(error) => {
            warnings.push(format!(
                "gateway health check failed: {}",
                sanitize_log_line(error.to_string().as_str())
            ));
            None
        }
    };

    let browser_health = if browser_service_enabled {
        match fetch_health(&http_client, runtime.browser_health_port).await {
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

    let (diagnostics_payload, discord_payload, console_warnings) = fetch_console_payloads(
        &http_client,
        &runtime,
        admin_token.as_str(),
        gateway_health.is_some(),
    )
    .await;
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
        dropped_log_events_total,
    };
    if dropped_log_events_total > 0 {
        warnings.push(format!(
            "desktop log queue overflowed; dropped {dropped_log_events_total} log event(s)"
        ));
    }

    let discord = parse_discord_status(discord_payload.as_ref());
    let browser_status = build_browser_status(
        browser_service_enabled,
        browser_running,
        browser_health.as_ref(),
        browser_last_exit,
    );

    let dashboard_access = match resolve_dashboard_access_target(runtime.gateway_admin_port) {
        Ok(target) => target,
        Err(error) => {
            warnings.push(format!(
                "dashboard URL discovery failed: {}",
                sanitize_log_line(error.to_string().as_str())
            ));
            default_dashboard_access_target(runtime.gateway_admin_port)
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
    let browser_healthy = quick_facts.browser_service.healthy;
    let discord_degraded = quick_facts.discord.enabled && !quick_facts.discord.authenticated;
    let diagnostics_degraded = !diagnostics.errors.is_empty();

    let overall_status = if !gateway_running || gateway_health.is_none() {
        OverallStatus::Down
    } else if (browser_service_enabled && !browser_healthy) || discord_degraded || diagnostics_degraded
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
        gateway_process,
        browserd_process,
        logs,
        warnings,
    })
}

async fn fetch_health(http_client: &Client, port: u16) -> Result<Option<HealthEndpointPayload>> {
    let url = loopback_url(port, "/health")?;
    let response = http_client.get(url).send().await.context("health request failed")?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let payload =
        response.json::<HealthEndpointPayload>().await.context("failed to decode health payload")?;
    if payload.status.trim().eq_ignore_ascii_case("ok") {
        Ok(Some(payload))
    } else {
        Ok(None)
    }
}

async fn fetch_console_payloads(
    http_client: &Client,
    runtime: &RuntimeConfig,
    admin_token: &str,
    gateway_health_available: bool,
) -> (Option<Value>, Option<Value>, Vec<String>) {
    let mut warnings = Vec::new();
    if !gateway_health_available {
        return (None, None, warnings);
    }

    let mut control_plane = match build_control_plane_client(http_client.clone(), runtime) {
        Ok(client) => client,
        Err(error) => {
            warnings.push(format!(
                "control-plane client initialization failed: {}",
                sanitize_log_line(error.to_string().as_str())
            ));
            return (None, None, warnings);
        }
    };

    if let Err(error) = ensure_console_session(&mut control_plane, admin_token).await {
        warnings.push(format!(
            "console session bootstrap failed: {}",
            sanitize_log_line(error.to_string().as_str())
        ));
        return (None, None, warnings);
    }

    let diagnostics = match control_plane.get_diagnostics().await {
        Ok(value) => Some(value),
        Err(error) => {
            warnings.push(format!(
                "failed to fetch diagnostics payload: {}",
                sanitize_log_line(error.to_string().as_str())
            ));
            None
        }
    };

    let discord = match fetch_console_json(
        http_client,
        runtime,
        "/console/v1/channels/discord%3Adefault",
    )
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

pub(crate) fn build_control_plane_client(http_client: Client, runtime: &RuntimeConfig) -> Result<ControlPlaneClient> {
    let config = ControlPlaneClientConfig::new(format!(
        "{DASHBOARD_SCHEME}://{LOOPBACK_HOST}:{}/",
        runtime.gateway_admin_port
    ));
    ControlPlaneClient::with_client(config, http_client)
        .map_err(|error| anyhow!("failed to build control-plane client: {error}"))
}

async fn ensure_console_session(control_plane: &mut ControlPlaneClient, admin_token: &str) -> Result<()> {
    match control_plane.get_session().await {
        Ok(_) => Ok(()),
        Err(control_plane::ControlPlaneClientError::Http { status, .. })
            if matches!(status, 401 | 403) =>
        {
            control_plane
                .login(&control_plane::ConsoleLoginRequest {
                    admin_token: admin_token.to_owned(),
                    principal: CONSOLE_PRINCIPAL.to_owned(),
                    device_id: CONSOLE_DEVICE_ID.to_owned(),
                    channel: None,
                })
                .await
                .map(|_| ())
                .map_err(|error| anyhow!("console login failed: {error}"))
        }
        Err(error) => Err(anyhow!("console session request failed: {error}")),
    }
}

async fn fetch_console_json(http_client: &Client, runtime: &RuntimeConfig, path: &str) -> Result<Value> {
    let url = loopback_url(runtime.gateway_admin_port, path)?;
    let response = http_client.get(url).send().await.context("console GET request failed")?;
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

pub(crate) async fn run_support_bundle_export(
    plan: SupportBundleExportPlan,
) -> Result<SupportBundleExportResult> {
    let cli_path = resolve_binary_path("palyra", "PALYRA_DESKTOP_PALYRA_BIN")?;
    let output_name = format!("support-bundle-{}.json", unix_ms_now());
    let output_path = plan.support_bundle_dir.join(output_name);

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
        .env("PALYRA_STATE_ROOT", plan.runtime_root.to_string_lossy().into_owned())
        .env("PALYRA_ADMIN_TOKEN", plan.admin_token);

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

pub(crate) fn is_error_like_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    ["error", "failure", "warning", "detail", "reason", "message"]
        .iter()
        .any(|needle| lowered.contains(needle))
}

pub(crate) fn sanitize_log_line(raw: &str) -> String {
    let mut line = redact_auth_error(raw);
    line = redact_inline_urls(line.as_str());
    line.trim().to_owned()
}

pub(crate) fn redact_inline_urls(raw: &str) -> String {
    let mut tokens = Vec::new();
    for token in raw.split_whitespace() {
        let sanitized = sanitize_token_with_url(token);
        tokens.push(sanitized);
    }
    tokens.join(" ")
}

pub(crate) fn sanitize_token_with_url(token: &str) -> String {
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

pub(crate) fn loopback_url(port: u16, path: &str) -> Result<Url> {
    if !path.starts_with('/') {
        bail!("path must be absolute");
    }
    Url::parse(format!("{DASHBOARD_SCHEME}://{LOOPBACK_HOST}:{port}{path}").as_str())
        .with_context(|| format!("failed to construct loopback URL for path '{path}'"))
}

pub(crate) fn resolve_dashboard_access_target(default_port: u16) -> Result<DashboardAccessTarget> {
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

pub(crate) fn resolve_dashboard_config_path() -> Result<Option<PathBuf>> {
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

pub(crate) fn load_dashboard_root_file_config(path: &Path) -> Result<RootFileConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read desktop dashboard config {}", path.display()))?;
    let (document, _) = parse_document_with_migration(content.as_str())
        .with_context(|| format!("failed to migrate desktop dashboard config {}", path.display()))?;
    let migrated = toml::to_string(&document)
        .context("failed to serialize migrated desktop dashboard config document")?;
    toml::from_str(migrated.as_str()).context("desktop dashboard config does not match schema")
}

pub(crate) fn parse_remote_dashboard_base_url(raw: &str, source_name: &str) -> Result<String> {
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

pub(crate) fn normalize_dashboard_socket(socket: SocketAddr) -> SocketAddr {
    if socket.ip().is_unspecified() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), socket.port())
    } else {
        socket
    }
}

pub(crate) fn format_dashboard_url(socket: SocketAddr) -> String {
    format!("{DASHBOARD_SCHEME}://{socket}/")
}

pub(crate) fn default_dashboard_access_target(default_port: u16) -> DashboardAccessTarget {
    DashboardAccessTarget {
        url: format!("{DASHBOARD_SCHEME}://{LOOPBACK_HOST}:{default_port}/"),
        mode: DashboardAccessMode::Local,
    }
}
