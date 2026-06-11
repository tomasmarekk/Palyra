//! Desktop control-center supervisor for local Palyra sidecars.
//!
//! The supervisor owns process lifecycle, runtime configuration, profile
//! activation, config reload watching, log buffering, and the single-instance
//! guard used by the Tauri desktop shell.

use std::{
    collections::VecDeque,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use palyra_common::config_system::{get_value_at_path, set_value_at_path};
use palyra_vault::{BackendPreference as VaultBackendPreference, Vault, VaultConfig, VaultRef};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use super::companion::DesktopCompanionProfileRecord;
pub(crate) use super::console_cache::{
    CachedConsolePayload, ConsolePayloadCache, ConsoleSessionCache,
};
use super::profile_registry::{DesktopProfileCatalog, DesktopResolvedProfile};
use super::{
    load_or_initialize_state_file, load_runtime_secrets,
    migrate_legacy_runtime_secrets_from_state_file, sanitize_log_line,
    validate_runtime_state_root_override, DesktopOnboardingStep, DesktopRuntimeSecrets,
    DesktopSecretStore, DesktopStateFile, BROWSER_GRPC_PORT, BROWSER_HEALTH_PORT,
    CONSOLE_PRINCIPAL, GATEWAY_ADMIN_PORT, GATEWAY_GRPC_PORT, GATEWAY_QUIC_PORT,
    LOG_EVENT_CHANNEL_CAPACITY, LOOPBACK_HOST, MAX_LOG_LINES_PER_SERVICE,
};

const NODE_HOST_STATE_DIR: &str = "node-host";
const NODE_HOST_CONFIG_FILE_NAME: &str = "node-host.json";
const DESKTOP_CONFIG_RELOAD_MODE_ENV: &str = "PALYRA_DESKTOP_CONFIG_RELOAD_MODE";
const BROWSERD_STATE_ENCRYPTION_KEY_ENV: &str = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY";

/// Supervised service kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServiceKind {
    Gateway,
    Browserd,
    NodeHost,
}

impl ServiceKind {
    /// Returns the stable machine-readable service name.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Browserd => "browserd",
            Self::NodeHost => "node_host",
        }
    }

    /// Returns the human-facing sidecar name.
    pub(crate) const fn display_name(self) -> &'static str {
        match self {
            Self::Gateway => "palyrad",
            Self::Browserd => "palyra-browserd",
            Self::NodeHost => "palyra node host",
        }
    }

    const fn binary_name(self) -> &'static str {
        self.display_name()
    }

    const fn env_override(self) -> &'static str {
        match self {
            Self::Gateway => "PALYRA_DESKTOP_PALYRAD_BIN",
            Self::Browserd => "PALYRA_DESKTOP_BROWSERD_BIN",
            Self::NodeHost => "PALYRA_DESKTOP_PALYRA_BIN",
        }
    }
}

/// Origin stream for one buffered service log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LogStream {
    Stdout,
    Stderr,
    Supervisor,
}

impl LogStream {
    /// Returns the stable machine-readable stream name.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Supervisor => "supervisor",
        }
    }
}

/// Sanitized log line stored for desktop diagnostics.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogLine {
    pub(crate) unix_ms: i64,
    pub(crate) service: String,
    pub(crate) stream: String,
    pub(crate) line: String,
}

/// Internal log event sent from async process readers to the supervisor loop.
#[derive(Debug)]
pub(crate) struct LogEvent {
    pub(crate) unix_ms: i64,
    pub(crate) service: ServiceKind,
    pub(crate) stream: LogStream,
    pub(crate) line: String,
}

/// Mutable lifecycle state for one supervised process.
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

/// Config file reload mode for the desktop supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopConfigReloadMode {
    Auto,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigFileSignature {
    modified_unix_ms: i64,
    len: u64,
}

/// Watched root config state used to restart supervised sidecars safely.
#[derive(Debug, Clone)]
pub(crate) struct DesktopConfigReloadWatchState {
    mode: DesktopConfigReloadMode,
    path: Option<PathBuf>,
    observed_once: bool,
    last_signature: Option<ConfigFileSignature>,
    last_error: Option<String>,
}

enum ConfigReloadWatchOutcome {
    Changed { path: PathBuf },
    Idle,
}

impl ManagedService {
    /// Creates a stopped service with known bound ports.
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

    /// Returns whether a child process handle is currently attached.
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

/// Runtime ports used by the supervised gateway and browser daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeConfig {
    pub(crate) gateway_admin_port: u16,
    pub(crate) gateway_grpc_port: u16,
    pub(crate) gateway_quic_port: u16,
    pub(crate) browser_health_port: u16,
    pub(crate) browser_grpc_port: u16,
}

impl RuntimeConfig {
    fn gateway_bound_ports(&self) -> Vec<u16> {
        vec![self.gateway_admin_port, self.gateway_grpc_port, self.gateway_quic_port]
    }

    fn browser_bound_ports(&self) -> Vec<u16> {
        vec![self.browser_health_port, self.browser_grpc_port]
    }
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

/// Process snapshot returned to the desktop UI.
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

/// Health endpoint payload returned by supervised sidecars.
#[derive(Debug, Deserialize)]
pub(crate) struct HealthEndpointPayload {
    pub(crate) status: String,
    pub(crate) version: String,
    pub(crate) git_hash: String,
    pub(crate) uptime_seconds: u64,
}

/// Main desktop supervisor state.
#[derive(Debug)]
pub(crate) struct ControlCenter {
    pub(crate) desktop_state_root: PathBuf,
    pub(crate) state_dir: PathBuf,
    pub(crate) _instance_lock: DesktopInstanceLock,
    pub(crate) default_runtime_root: PathBuf,
    pub(crate) runtime_root: PathBuf,
    pub(crate) support_bundle_dir: PathBuf,
    pub(crate) state_file_path: PathBuf,
    pub(crate) persisted: DesktopStateFile,
    pub(crate) active_profile: DesktopResolvedProfile,
    pub(crate) profile_catalog: DesktopProfileCatalog,
    pub(crate) runtime_secret_fallbacks: DesktopRuntimeSecrets,
    pub(crate) admin_token: String,
    pub(crate) admin_bound_principal: Option<String>,
    pub(crate) browser_auth_token: String,
    pub(crate) browser_state_encryption_key: Option<BrowserStateEncryptionKey>,
    pub(crate) runtime: RuntimeConfig,
    pub(crate) gateway: ManagedService,
    pub(crate) browserd: ManagedService,
    pub(crate) node_host: ManagedService,
    pub(crate) config_reload_watch: DesktopConfigReloadWatchState,
    pub(crate) http_client: Client,
    pub(crate) console_session_cache: Arc<Mutex<Option<ConsoleSessionCache>>>,
    pub(crate) console_payload_cache: Arc<Mutex<ConsolePayloadCache>>,
    pub(crate) log_tx: mpsc::Sender<LogEvent>,
    pub(crate) log_rx: mpsc::Receiver<LogEvent>,
    pub(crate) dropped_log_events: Arc<AtomicU64>,
}

impl DesktopConfigReloadWatchState {
    /// Creates a watch state for the active desktop profile.
    pub(crate) fn from_profile(profile: &DesktopResolvedProfile) -> Self {
        let last_signature =
            profile.config_path.as_ref().and_then(|path| config_file_signature(path).ok());
        Self {
            mode: desktop_config_reload_mode_from_env(),
            path: profile.config_path.clone(),
            observed_once: profile.config_path.is_some(),
            last_signature,
            last_error: None,
        }
    }

    fn observe(&mut self) -> ConfigReloadWatchOutcome {
        if self.mode == DesktopConfigReloadMode::Manual {
            return ConfigReloadWatchOutcome::Idle;
        }
        let Some(path) = self.path.clone() else {
            return ConfigReloadWatchOutcome::Idle;
        };
        let signature = match config_file_signature(path.as_path()) {
            Ok(signature) => {
                self.last_error = None;
                signature
            }
            Err(error) => {
                let sanitized = sanitize_log_line(error.to_string().as_str());
                if self.last_error.as_deref() != Some(sanitized.as_str()) {
                    self.last_error = Some(sanitized);
                }
                self.observed_once = true;
                return ConfigReloadWatchOutcome::Idle;
            }
        };
        let changed =
            self.last_signature.map(|previous| previous != signature).unwrap_or(self.observed_once);
        self.observed_once = true;
        self.last_signature = Some(signature);
        if changed {
            ConfigReloadWatchOutcome::Changed { path }
        } else {
            ConfigReloadWatchOutcome::Idle
        }
    }
}

fn desktop_config_reload_mode_from_env() -> DesktopConfigReloadMode {
    match env::var(DESKTOP_CONFIG_RELOAD_MODE_ENV)
        .ok()
        .as_deref()
        .and_then(normalize_optional_text)
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("manual" | "off" | "disabled" | "false" | "0") => DesktopConfigReloadMode::Manual,
        _ => DesktopConfigReloadMode::Auto,
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ConfigRuntimeTokenOverrides {
    admin_token: Option<String>,
    admin_bound_principal: Option<String>,
    browser_auth_token: Option<String>,
    browser_state_encryption_key: Option<BrowserStateEncryptionKey>,
    config_file_present: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ConfigRuntimePortOverrides {
    gateway_admin_port: Option<u16>,
    gateway_grpc_port: Option<u16>,
    gateway_quic_port: Option<u16>,
    browser_health_port: Option<u16>,
    browser_grpc_port: Option<u16>,
}

#[derive(Debug, PartialEq, Eq)]
struct DesktopRuntimeAuthConfig {
    admin_token: String,
    admin_bound_principal: Option<String>,
    browser_auth_token: String,
    browser_state_encryption_key: Option<BrowserStateEncryptionKey>,
}

/// Browserd state encryption key wrapper that redacts debug output.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct BrowserStateEncryptionKey(String);

impl BrowserStateEncryptionKey {
    /// Parses a non-empty browser state encryption key from config or env.
    pub(crate) fn parse(raw: impl Into<String>, source: &str) -> Result<Self> {
        let raw = raw.into();
        let value = normalize_optional_text(raw.as_str())
            .with_context(|| format!("{source} resolved to an empty browser state key"))?;
        Ok(Self(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for BrowserStateEncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BrowserStateEncryptionKey(<redacted>)")
    }
}

fn runtime_auth_with_config_overrides(
    fallback: &DesktopRuntimeSecrets,
    config_path: Option<&Path>,
) -> Result<DesktopRuntimeAuthConfig> {
    let overrides = config_runtime_token_overrides(config_path)?;
    let admin_bound_principal = overrides
        .admin_bound_principal
        .or_else(|| (!overrides.config_file_present).then(|| CONSOLE_PRINCIPAL.to_owned()));
    Ok(DesktopRuntimeAuthConfig {
        admin_token: overrides.admin_token.unwrap_or_else(|| fallback.admin_token.clone()),
        admin_bound_principal,
        browser_auth_token: overrides
            .browser_auth_token
            .unwrap_or_else(|| fallback.browser_auth_token.clone()),
        browser_state_encryption_key: match overrides.browser_state_encryption_key {
            Some(value) => Some(value),
            None => Some(BrowserStateEncryptionKey::parse(
                fallback.browser_state_encryption_key.clone(),
                "desktop browser state encryption key",
            )?),
        },
    })
}

fn runtime_config_with_config_overrides(
    fallback: RuntimeConfig,
    config_path: Option<&Path>,
) -> Result<RuntimeConfig> {
    let overrides = config_runtime_port_overrides(config_path)?;
    Ok(RuntimeConfig {
        gateway_admin_port: overrides.gateway_admin_port.unwrap_or(fallback.gateway_admin_port),
        gateway_grpc_port: overrides.gateway_grpc_port.unwrap_or(fallback.gateway_grpc_port),
        gateway_quic_port: overrides.gateway_quic_port.unwrap_or(fallback.gateway_quic_port),
        browser_health_port: overrides.browser_health_port.unwrap_or(fallback.browser_health_port),
        browser_grpc_port: overrides.browser_grpc_port.unwrap_or(fallback.browser_grpc_port),
    })
}

#[cfg(test)]
fn runtime_secrets_with_config_overrides(
    fallback: &DesktopRuntimeSecrets,
    config_path: Option<&Path>,
) -> Result<DesktopRuntimeSecrets> {
    let auth = runtime_auth_with_config_overrides(fallback, config_path)?;
    Ok(DesktopRuntimeSecrets {
        admin_token: auth.admin_token,
        browser_auth_token: auth.browser_auth_token,
        browser_state_encryption_key: auth
            .browser_state_encryption_key
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default(),
    })
}

fn config_runtime_port_overrides(config_path: Option<&Path>) -> Result<ConfigRuntimePortOverrides> {
    let Some(config_path) = config_path else {
        return Ok(ConfigRuntimePortOverrides::default());
    };
    if !config_path.is_file() {
        return Ok(ConfigRuntimePortOverrides::default());
    }

    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read daemon config {}", config_path.display()))?;
    let document: toml::Value = toml::from_str(raw.as_str())
        .with_context(|| format!("failed to parse daemon config {}", config_path.display()))?;
    let browser_grpc_port = toml_url_port_at_path(&document, "tool_call.browser_service.endpoint");
    let browser_health_port =
        toml_url_port_at_path(&document, "tool_call.browser_service.health_base_url")
            .or_else(|| browser_grpc_port.map(derive_browser_health_port_from_grpc_port));
    Ok(ConfigRuntimePortOverrides {
        gateway_admin_port: toml_u16_at_path(&document, "daemon.port"),
        gateway_grpc_port: toml_u16_at_path(&document, "gateway.grpc_port"),
        gateway_quic_port: toml_u16_at_path(&document, "gateway.quic_port"),
        browser_health_port,
        browser_grpc_port,
    })
}

fn config_runtime_token_overrides(
    config_path: Option<&Path>,
) -> Result<ConfigRuntimeTokenOverrides> {
    let Some(config_path) = config_path else {
        return Ok(ConfigRuntimeTokenOverrides::default());
    };
    if !config_path.is_file() {
        return Ok(ConfigRuntimeTokenOverrides::default());
    }

    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read daemon config {}", config_path.display()))?;
    let document: toml::Value = toml::from_str(raw.as_str())
        .with_context(|| format!("failed to parse daemon config {}", config_path.display()))?;
    Ok(ConfigRuntimeTokenOverrides {
        admin_token: toml_string_at_path(&document, "admin.auth_token"),
        admin_bound_principal: toml_string_at_path(&document, "admin.bound_principal"),
        browser_auth_token: toml_string_at_path(&document, "tool_call.browser_service.auth_token"),
        browser_state_encryption_key: browser_state_encryption_key_from_config(
            &document,
            config_path.parent(),
        )?,
        config_file_present: true,
    })
}

fn browser_state_encryption_key_from_config(
    document: &toml::Value,
    config_dir: Option<&Path>,
) -> Result<Option<BrowserStateEncryptionKey>> {
    let Some(raw_vault_ref) =
        toml_string_at_path(document, "tool_call.browser_service.state_key_vault_ref")
    else {
        return Ok(None);
    };
    let parsed = VaultRef::parse(raw_vault_ref.as_str()).with_context(|| {
        "failed to parse tool_call.browser_service.state_key_vault_ref".to_owned()
    })?;
    let vault = Vault::open_with_config(VaultConfig {
        root: toml_path_at_path(document, "storage.vault_dir", config_dir),
        identity_store_root: toml_path_at_path(document, "gateway.identity_store_dir", config_dir),
        backend_preference: VaultBackendPreference::Auto,
        ..Default::default()
    })
    .context("failed to initialize vault runtime for browser state key")?;
    let secret = vault
        .get_secret(&parsed.scope, parsed.key.as_str())
        .context("failed to read browser state key vault ref")?;
    let secret =
        String::from_utf8(secret).context("browser state key vault ref must be UTF-8 text")?;
    BrowserStateEncryptionKey::parse(secret, "browser state key vault ref").map(Some)
}

fn toml_string_at_path(document: &toml::Value, path: &str) -> Option<String> {
    let mut current = document;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    normalize_optional_text(current.as_str().unwrap_or_default()).map(str::to_owned)
}

fn toml_path_at_path(
    document: &toml::Value,
    path: &str,
    config_dir: Option<&Path>,
) -> Option<PathBuf> {
    let path = PathBuf::from(toml_string_at_path(document, path)?);
    if path.is_absolute() {
        Some(path)
    } else {
        config_dir.map(|dir| dir.join(path))
    }
}

fn toml_u16_at_path(document: &toml::Value, path: &str) -> Option<u16> {
    get_value_at_path(document, path)
        .ok()
        .flatten()
        .and_then(toml::Value::as_integer)
        .and_then(|value| u16::try_from(value).ok())
}

fn toml_url_port_at_path(document: &toml::Value, path: &str) -> Option<u16> {
    let url = toml_string_at_path(document, path)?;
    Url::parse(url.as_str()).ok().and_then(|parsed| parsed.port_or_known_default())
}

fn derive_browser_health_port_from_grpc_port(grpc_port: u16) -> u16 {
    if grpc_port == palyra_common::local_runtime_ports::DEFAULT_BROWSER_GRPC_PORT {
        palyra_common::local_runtime_ports::DEFAULT_BROWSER_HEALTH_PORT
    } else {
        grpc_port.saturating_sub(1).max(1)
    }
}

fn format_port_availability_summary(
    unavailable: &[palyra_common::local_runtime_ports::PortAvailability],
) -> String {
    unavailable
        .iter()
        .map(|port| {
            format!("{}: {}", port.port, port.error.as_deref().unwrap_or("port is not bindable"))
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn config_file_signature(path: &Path) -> Result<ConfigFileSignature> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to stat watched config file {}", path.display()))?;
    let modified = metadata.modified().with_context(|| {
        format!("failed to read modified timestamp for watched config file {}", path.display())
    })?;
    let modified_unix_ms = system_time_to_unix_ms(modified);
    Ok(ConfigFileSignature { modified_unix_ms, len: metadata.len() })
}

fn browserd_env_values(
    runtime_root: &Path,
    browser_auth_token: &str,
    browser_state_encryption_key: Option<&BrowserStateEncryptionKey>,
) -> Vec<(String, String)> {
    let mut env = vec![
        ("PALYRA_STATE_ROOT".to_owned(), runtime_root.to_string_lossy().into_owned()),
        ("PALYRA_BROWSERD_AUTH_TOKEN".to_owned(), browser_auth_token.to_owned()),
    ];
    if let Some(state_key) = browser_state_encryption_key {
        env.push((BROWSERD_STATE_ENCRYPTION_KEY_ENV.to_owned(), state_key.as_str().to_owned()));
    }
    env
}

impl ControlCenter {
    /// Initializes desktop state, profile, secrets, runtime ports, and sidecar state.
    pub(crate) fn new() -> Result<Self> {
        let state_root = super::resolve_desktop_state_root()?;
        let state_dir = state_root.join("desktop-control-center");
        let support_bundle_dir = state_dir.join("support-bundles");
        fs::create_dir_all(support_bundle_dir.as_path()).with_context(|| {
            format!("failed to create support bundle output dir {}", support_bundle_dir.display())
        })?;
        let instance_lock = DesktopInstanceLock::acquire(state_dir.as_path())?;

        let state_file_path = state_dir.join("state.json");
        let secret_store = DesktopSecretStore::open(state_dir.as_path())?;
        migrate_legacy_runtime_secrets_from_state_file(state_file_path.as_path(), &secret_store)?;
        let mut persisted = load_or_initialize_state_file(state_file_path.as_path())?;
        let profile_catalog = DesktopProfileCatalog::load(state_root.as_path())?;
        let active_profile = resolve_active_profile(&persisted, &profile_catalog);
        persisted.activate_profile(active_profile.context.name.as_str());
        let runtime_secret_fallbacks = load_runtime_secrets(&secret_store)?;
        let runtime_auth = runtime_auth_with_config_overrides(
            &runtime_secret_fallbacks,
            active_profile.config_path.as_deref(),
        )?;
        let default_runtime_root = default_runtime_root_for_profile(
            state_root.as_path(),
            state_dir.as_path(),
            &active_profile,
        );
        let runtime_root = resolve_runtime_root_for_profile(
            &persisted,
            &active_profile,
            default_runtime_root.as_path(),
        )?;
        fs::create_dir_all(runtime_root.as_path()).with_context(|| {
            format!("failed to create desktop runtime dir {}", runtime_root.display())
        })?;
        apply_profile_process_env(&active_profile);

        let runtime = runtime_config_with_config_overrides(
            RuntimeConfig::default(),
            active_profile.config_path.as_deref(),
        )?;
        let gateway = ManagedService::new(runtime.gateway_bound_ports());
        let browserd = ManagedService::new(runtime.browser_bound_ports());
        let node_host = ManagedService::new(Vec::new());
        let config_reload_watch = DesktopConfigReloadWatchState::from_profile(&active_profile);

        let http_client = build_http_client()?;

        let (log_tx, log_rx) = mpsc::channel(LOG_EVENT_CHANNEL_CAPACITY);
        let dropped_log_events = Arc::new(AtomicU64::new(0));

        Ok(Self {
            desktop_state_root: state_root,
            state_dir,
            _instance_lock: instance_lock,
            default_runtime_root,
            runtime_root,
            support_bundle_dir,
            state_file_path,
            persisted,
            active_profile,
            profile_catalog,
            runtime_secret_fallbacks,
            admin_token: runtime_auth.admin_token,
            admin_bound_principal: runtime_auth.admin_bound_principal,
            browser_auth_token: runtime_auth.browser_auth_token,
            browser_state_encryption_key: runtime_auth.browser_state_encryption_key,
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
            dropped_log_events,
        })
    }

    /// Persists the current desktop state file.
    pub(crate) fn save_state_file(&self) -> Result<()> {
        let encoded = serde_json::to_string_pretty(&self.persisted)
            .context("failed to encode desktop state file")?;
        fs::write(self.state_file_path.as_path(), encoded).with_context(|| {
            format!("failed to persist desktop state file {}", self.state_file_path.display())
        })
    }

    /// Records a bounded onboarding event in the active profile.
    pub(crate) fn record_onboarding_event(
        &mut self,
        kind: impl Into<String>,
        detail: Option<String>,
    ) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.ensure_flow_id();
        onboarding.push_event(kind, detail, unix_ms_now());
        self.save_state_file()
    }

    /// Clears the last onboarding failure when one is present.
    pub(crate) fn clear_onboarding_failure(&mut self) -> Result<()> {
        if self.persisted.active_onboarding().last_failure.is_none() {
            return Ok(());
        }
        self.persisted.active_onboarding_mut().last_failure = None;
        self.save_state_file()
    }

    /// Records an onboarding failure with sanitized UI-facing detail.
    pub(crate) fn record_onboarding_failure(
        &mut self,
        step: DesktopOnboardingStep,
        message: String,
    ) -> Result<()> {
        let sanitized = sanitize_log_line(message.as_str());
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.ensure_flow_id();
        onboarding.record_failure_step(step);
        onboarding.last_failure = Some(super::desktop_state::DesktopOnboardingFailureState {
            step,
            message: sanitized.clone(),
            recorded_at_unix_ms: unix_ms_now(),
        });
        onboarding.push_event(
            "failure",
            Some(format!("{}: {}", step.as_str(), sanitized)),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Records the outcome of a support-bundle export attempt.
    pub(crate) fn record_support_bundle_export_result(
        &mut self,
        success: bool,
        detail: Option<String>,
    ) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.ensure_flow_id();
        onboarding.record_support_bundle_export_result(success);
        onboarding.push_event(
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

    /// Marks the welcome step acknowledged for the active profile.
    pub(crate) fn mark_onboarding_welcome_acknowledged(&mut self) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        if onboarding.welcome_acknowledged_at_unix_ms.is_none() {
            onboarding.welcome_acknowledged_at_unix_ms = Some(unix_ms_now());
        }
        onboarding.push_event(
            "welcome_acknowledged",
            Some("Desktop first-run onboarding started.".to_owned()),
            unix_ms_now(),
        );
        onboarding.last_failure = None;
        self.save_state_file()
    }

    /// Applies and persists a runtime state-root override.
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
        self.persisted.active_profile_state_mut().runtime_state_root =
            if runtime_root == self.default_runtime_root {
                None
            } else {
                Some(runtime_root.to_string_lossy().into_owned())
            };
        self.runtime_root = runtime_root.clone();
        let onboarding = self.persisted.active_onboarding_mut();
        if confirm_selection && onboarding.state_root_confirmed_at_unix_ms.is_none() {
            onboarding.state_root_confirmed_at_unix_ms = Some(unix_ms_now());
        }
        onboarding.push_event(
            "state_root_selected",
            Some(self.runtime_root.to_string_lossy().into_owned()),
            unix_ms_now(),
        );
        onboarding.last_failure = None;
        self.save_state_file()?;
        Ok(runtime_root)
    }

    /// Records successful OpenAI provider onboarding.
    pub(crate) fn mark_openai_connected(
        &mut self,
        preferred_method: Option<&str>,
        profile_id: Option<&str>,
    ) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        if let Some(method) = preferred_method.and_then(normalize_optional_text) {
            onboarding.openai.preferred_method = Some(method.to_ascii_lowercase());
        }
        if let Some(profile_id) = profile_id.and_then(normalize_optional_text) {
            onboarding.openai.last_profile_id = Some(profile_id.to_owned());
        }
        onboarding.openai.last_connected_at_unix_ms = Some(unix_ms_now());
        onboarding.last_failure = None;
        onboarding.push_event(
            "openai_connected",
            profile_id.map(str::to_owned).or_else(|| preferred_method.map(str::to_owned)),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Stores Discord onboarding request metadata in active profile state.
    pub(crate) fn update_discord_onboarding_metadata(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        let discord = &mut self.persisted.active_onboarding_mut().discord;
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

    /// Marks Discord onboarding preflight as completed.
    pub(crate) fn mark_discord_preflight(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        self.update_discord_onboarding_metadata(request)?;
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.last_failure = None;
        onboarding.push_event(
            "discord_preflight",
            onboarding.discord.verify_channel_id.clone(),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Marks Discord connector settings as applied.
    pub(crate) fn mark_discord_applied(
        &mut self,
        request: &super::DiscordOnboardingRequest,
    ) -> Result<()> {
        self.update_discord_onboarding_metadata(request)?;
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.last_failure = None;
        onboarding.push_event(
            "discord_applied",
            Some(onboarding.discord.connector_id()),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Marks Discord verification as completed.
    pub(crate) fn mark_discord_verified(&mut self, connector_id: &str, target: &str) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        onboarding.discord.last_connector_id =
            normalize_optional_text(connector_id).map(str::to_owned);
        onboarding.discord.last_verified_target =
            normalize_optional_text(target).map(str::to_owned);
        onboarding.discord.last_verified_at_unix_ms = Some(unix_ms_now());
        onboarding.last_failure = None;
        onboarding.push_event(
            "discord_verified",
            Some(format!("{connector_id}:{target}")),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Marks the dashboard handoff step as completed.
    pub(crate) fn mark_dashboard_handoff_complete(&mut self) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        if onboarding.dashboard_handoff_at_unix_ms.is_none() {
            onboarding.dashboard_handoff_at_unix_ms = Some(unix_ms_now());
        }
        onboarding.last_failure = None;
        onboarding.push_event(
            "dashboard_opened",
            Some("Dashboard handoff completed from desktop.".to_owned()),
            unix_ms_now(),
        );
        self.save_state_file()
    }

    /// Marks desktop onboarding completed for the active profile.
    pub(crate) fn mark_onboarding_complete(&mut self) -> Result<()> {
        let onboarding = self.persisted.active_onboarding_mut();
        if onboarding.completed_at_unix_ms.is_none() {
            onboarding.completed_at_unix_ms = Some(unix_ms_now());
            onboarding.push_event(
                "onboarding_completed",
                Some("Desktop onboarding completed.".to_owned()),
                unix_ms_now(),
            );
        }
        onboarding.last_failure = None;
        self.save_state_file()
    }

    /// Returns the active profile row for companion UI.
    pub(crate) fn current_profile_record(&self) -> DesktopCompanionProfileRecord {
        DesktopCompanionProfileRecord::from_resolved_profile(
            &self.active_profile,
            true,
            true,
            self.active_profile.context.name.as_str() == self.persisted.active_profile_name(),
        )
    }

    /// Returns ordered profile rows for the companion profile switcher.
    pub(crate) fn profile_records(&self) -> Vec<DesktopCompanionProfileRecord> {
        let active_name = self.persisted.active_profile_name();
        let recent_names = self.persisted.recent_profile_names();
        let mut ordered_names = recent_names.to_vec();
        for name in self.profile_catalog.profiles.keys() {
            if !ordered_names.iter().any(|candidate| candidate == name) {
                ordered_names.push(name.clone());
            }
        }

        ordered_names
            .into_iter()
            .filter_map(|name| {
                self.profile_catalog.profiles.get(name.as_str()).map(|profile| {
                    DesktopCompanionProfileRecord::from_resolved_profile(
                        profile,
                        recent_names.iter().any(|candidate| candidate == &name),
                        false,
                        profile.context.name == active_name,
                    )
                })
            })
            .collect()
    }

    /// Switches the active desktop profile and resets profile-bound runtime state.
    pub(crate) fn switch_active_profile(
        &mut self,
        profile_name: &str,
        allow_strict_switch: bool,
    ) -> Result<String> {
        let profile_name = normalize_optional_text(profile_name)
            .ok_or_else(|| anyhow::anyhow!("desktop profile switch requires a profile name"))?;
        let Some(next_profile) = self.profile_catalog.profiles.get(profile_name).cloned() else {
            bail!("desktop profile '{profile_name}' is not available");
        };
        if next_profile.context.name == self.active_profile.context.name {
            return Ok(format!(
                "desktop companion is already using profile {}",
                next_profile.context.label
            ));
        }
        if next_profile.context.strict_mode && !allow_strict_switch {
            bail!(
                "switching to strict desktop profile '{}' requires explicit confirmation",
                next_profile.context.label
            );
        }

        let runtime_was_active = self.gateway.desired_running
            || self.gateway.running()
            || self.browserd.desired_running
            || self.browserd.running();
        self.stop_all();
        self.reset_profile_bound_runtime_state()?;

        self.persisted.activate_profile(next_profile.context.name.as_str());
        self.active_profile = next_profile;
        self.config_reload_watch =
            DesktopConfigReloadWatchState::from_profile(&self.active_profile);
        self.default_runtime_root = default_runtime_root_for_profile(
            self.desktop_state_root.as_path(),
            self.state_dir.as_path(),
            &self.active_profile,
        );
        self.runtime_root = resolve_runtime_root_for_profile(
            &self.persisted,
            &self.active_profile,
            self.default_runtime_root.as_path(),
        )?;
        fs::create_dir_all(self.runtime_root.as_path()).with_context(|| {
            format!("failed to create desktop runtime dir {}", self.runtime_root.display())
        })?;
        apply_profile_process_env(&self.active_profile);
        self.refresh_runtime_tokens_from_active_config()?;
        self.record_onboarding_event(
            "profile_switched",
            Some(self.active_profile.context.name.clone()),
        )?;

        let suffix =
            if runtime_was_active { "; runtime was paused for safe profile isolation" } else { "" };
        Ok(format!(
            "switched desktop companion to profile {}{}",
            self.active_profile.context.label, suffix
        ))
    }

    /// Requests all enabled supervised services to start.
    pub(crate) fn start_all(&mut self) {
        self.gateway.desired_running = true;
        self.browserd.desired_running = self.persisted.browser_service_enabled;
        self.node_host.desired_running = self.node_host_installed();
        self.gateway.next_restart_unix_ms = Some(unix_ms_now());
        self.browserd.next_restart_unix_ms = Some(unix_ms_now());
        self.node_host.next_restart_unix_ms =
            self.node_host.desired_running.then_some(unix_ms_now());
        self.append_supervisor_log(ServiceKind::Gateway, "start requested for gateway sidecar");
        if self.persisted.browser_service_enabled {
            self.append_supervisor_log(
                ServiceKind::Browserd,
                "start requested for browser sidecar",
            );
        }
        if self.node_host.desired_running {
            self.append_supervisor_log(
                ServiceKind::NodeHost,
                "start requested for desktop node host sidecar",
            );
        }
    }

    /// Stops all supervised services in dependency order.
    pub(crate) fn stop_all(&mut self) {
        self.stop_service(ServiceKind::NodeHost);
        self.stop_service(ServiceKind::Browserd);
        self.stop_service(ServiceKind::Gateway);
    }

    /// Restarts all supervised services.
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

    /// Enables or disables the browser sidecar and persists the setting.
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

    /// Advances process lifecycle, log drains, config reloads, and restarts.
    pub(crate) fn refresh_runtime_state(&mut self) {
        self.drain_log_events();
        self.check_process_exit(ServiceKind::Gateway);
        self.check_process_exit(ServiceKind::Browserd);
        self.check_process_exit(ServiceKind::NodeHost);
        self.reconcile_config_reload_watch();
        self.reconcile_service(ServiceKind::Gateway);
        self.reconcile_service(ServiceKind::Browserd);
        self.reconcile_service(ServiceKind::NodeHost);
    }

    /// Restarts supervised runtime when the watched config file changes.
    pub(crate) fn reconcile_config_reload_watch(&mut self) {
        let ConfigReloadWatchOutcome::Changed { path } = self.config_reload_watch.observe() else {
            return;
        };
        if !self.gateway.desired_running && !self.browserd.desired_running {
            self.append_supervisor_log(
                ServiceKind::Gateway,
                format!(
                    "watched config changed at {}, but runtime is in manual stopped mode",
                    path.display()
                )
                .as_str(),
            );
            return;
        }
        self.append_supervisor_log(
            ServiceKind::Gateway,
            format!("watched config changed at {}; restarting supervised runtime", path.display())
                .as_str(),
        );
        match self.refresh_runtime_tokens_from_config_path(Some(path.as_path())) {
            Ok(true) => self.append_supervisor_log(
                ServiceKind::Gateway,
                "watched config runtime settings refreshed for supervised runtime",
            ),
            Ok(false) => {}
            Err(error) => self.append_supervisor_log(
                ServiceKind::Gateway,
                format!(
                    "watched config runtime refresh failed: {}",
                    sanitize_log_line(error.to_string().as_str())
                )
                .as_str(),
            ),
        }
        self.restart_all();
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

    fn ensure_service_ports_available_before_spawn(&mut self, kind: ServiceKind) -> Result<()> {
        match kind {
            ServiceKind::Gateway => {
                let unavailable = palyra_common::local_runtime_ports::unavailable_ports(
                    LOOPBACK_HOST,
                    self.runtime.gateway_bound_ports().as_slice(),
                );
                if unavailable.is_empty() {
                    return Ok(());
                }
                let ports =
                    palyra_common::local_runtime_ports::select_available_gateway_runtime_ports(
                        LOOPBACK_HOST,
                    )
                    .map_err(anyhow::Error::msg)?;
                self.runtime.gateway_admin_port = ports.admin;
                self.runtime.gateway_grpc_port = ports.grpc;
                self.runtime.gateway_quic_port = ports.quic;
                self.persist_runtime_port_selection(ServiceKind::Gateway)?;
                self.sync_service_bound_ports();
                self.append_supervisor_log(
                    ServiceKind::Gateway,
                    format!(
                        "gateway port collision detected ({}); selected free loopback ports admin={} grpc={} quic={}",
                        format_port_availability_summary(unavailable.as_slice()),
                        ports.admin,
                        ports.grpc,
                        ports.quic
                    )
                    .as_str(),
                );
                Ok(())
            }
            ServiceKind::Browserd => {
                let unavailable = palyra_common::local_runtime_ports::unavailable_ports(
                    LOOPBACK_HOST,
                    self.runtime.browser_bound_ports().as_slice(),
                );
                if unavailable.is_empty() {
                    return Ok(());
                }
                let ports =
                    palyra_common::local_runtime_ports::select_available_browser_runtime_ports(
                        LOOPBACK_HOST,
                    )
                    .map_err(anyhow::Error::msg)?;
                self.runtime.browser_health_port = ports.health;
                self.runtime.browser_grpc_port = ports.grpc;
                self.persist_runtime_port_selection(ServiceKind::Browserd)?;
                self.sync_service_bound_ports();
                self.append_supervisor_log(
                    ServiceKind::Browserd,
                    format!(
                        "browserd port collision detected ({}); selected free loopback ports health={} grpc={}",
                        format_port_availability_summary(unavailable.as_slice()),
                        ports.health,
                        ports.grpc
                    )
                    .as_str(),
                );
                Ok(())
            }
            ServiceKind::NodeHost => Ok(()),
        }
    }

    fn persist_runtime_port_selection(&self, kind: ServiceKind) -> Result<()> {
        let Some(config_path) = self.active_profile.config_path.as_deref() else {
            return Ok(());
        };
        if !config_path.is_file() {
            return Ok(());
        }

        let raw = fs::read_to_string(config_path)
            .with_context(|| format!("failed to read daemon config {}", config_path.display()))?;
        let mut document: toml::Value = toml::from_str(raw.as_str())
            .with_context(|| format!("failed to parse daemon config {}", config_path.display()))?;
        match kind {
            ServiceKind::Gateway => {
                set_value_at_path(
                    &mut document,
                    "daemon.port",
                    toml::Value::Integer(i64::from(self.runtime.gateway_admin_port)),
                )?;
                set_value_at_path(
                    &mut document,
                    "gateway.grpc_port",
                    toml::Value::Integer(i64::from(self.runtime.gateway_grpc_port)),
                )?;
                set_value_at_path(
                    &mut document,
                    "gateway.quic_port",
                    toml::Value::Integer(i64::from(self.runtime.gateway_quic_port)),
                )?;
                set_value_at_path(
                    &mut document,
                    "canvas_host.public_base_url",
                    toml::Value::String(format!(
                        "http://{LOOPBACK_HOST}:{}",
                        self.runtime.gateway_admin_port
                    )),
                )?;
            }
            ServiceKind::Browserd => {
                set_value_at_path(
                    &mut document,
                    "tool_call.browser_service.endpoint",
                    toml::Value::String(format!(
                        "http://{LOOPBACK_HOST}:{}",
                        self.runtime.browser_grpc_port
                    )),
                )?;
                set_value_at_path(
                    &mut document,
                    "tool_call.browser_service.health_base_url",
                    toml::Value::String(format!(
                        "http://{LOOPBACK_HOST}:{}",
                        self.runtime.browser_health_port
                    )),
                )?;
            }
            ServiceKind::NodeHost => {}
        }
        let rendered =
            toml::to_string_pretty(&document).context("failed to serialize daemon config")?;
        fs::write(config_path, rendered)
            .with_context(|| format!("failed to persist daemon config {}", config_path.display()))
    }

    fn spawn_service(&mut self, kind: ServiceKind) -> Result<()> {
        self.ensure_service_ports_available_before_spawn(kind)?;
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
                    .arg(self.runtime.browser_grpc_port.to_string());
                for (key, value) in self.browserd_env() {
                    command.env(key, value);
                }
            }
            ServiceKind::NodeHost => {
                command.arg("node").arg("run").arg("--json");
                command.env("PALYRA_STATE_ROOT", self.runtime_root.to_string_lossy().into_owned());
                command.env(
                    "PALYRA_GATEWAY_IDENTITY_STORE_DIR",
                    self.runtime_root.join("identity").to_string_lossy().into_owned(),
                );
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
        let mut env = vec![
            ("PALYRA_DEPLOYMENT_MODE".to_owned(), "local_desktop".to_owned()),
            ("PALYRA_ADMIN_REQUIRE_AUTH".to_owned(), "true".to_owned()),
            ("PALYRA_ADMIN_TOKEN".to_owned(), self.admin_token.clone()),
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
        ];
        if let Some(principal) = self.admin_bound_principal.as_deref() {
            env.push(("PALYRA_ADMIN_BOUND_PRINCIPAL".to_owned(), principal.to_owned()));
        }
        env
    }

    fn browserd_env(&self) -> Vec<(String, String)> {
        browserd_env_values(
            self.runtime_root.as_path(),
            self.browser_auth_token.as_str(),
            self.browser_state_encryption_key.as_ref(),
        )
    }

    fn refresh_runtime_tokens_from_active_config(&mut self) -> Result<bool> {
        let config_path = self.active_profile.config_path.clone();
        match config_path {
            Some(path) => self.refresh_runtime_tokens_from_config_path(Some(path.as_path())),
            None => self.refresh_runtime_tokens_from_config_path(None),
        }
    }

    fn refresh_runtime_tokens_from_config_path(
        &mut self,
        config_path: Option<&Path>,
    ) -> Result<bool> {
        let runtime_auth =
            runtime_auth_with_config_overrides(&self.runtime_secret_fallbacks, config_path)?;
        let runtime_config =
            runtime_config_with_config_overrides(RuntimeConfig::default(), config_path)?;
        let auth_changed = self.admin_token != runtime_auth.admin_token
            || self.admin_bound_principal != runtime_auth.admin_bound_principal
            || self.browser_auth_token != runtime_auth.browser_auth_token
            || self.browser_state_encryption_key != runtime_auth.browser_state_encryption_key;
        let runtime_changed = self.runtime != runtime_config;
        if auth_changed {
            self.admin_token = runtime_auth.admin_token;
            self.admin_bound_principal = runtime_auth.admin_bound_principal;
            self.browser_auth_token = runtime_auth.browser_auth_token;
            self.browser_state_encryption_key = runtime_auth.browser_state_encryption_key;
            if let Ok(mut session_cache) = self.console_session_cache.lock() {
                *session_cache = None;
            }
            if let Ok(mut payload_cache) = self.console_payload_cache.lock() {
                *payload_cache = ConsolePayloadCache::default();
            }
        }
        if runtime_changed {
            self.runtime = runtime_config;
            self.sync_service_bound_ports();
        }
        Ok(auth_changed || runtime_changed)
    }

    fn sync_service_bound_ports(&mut self) {
        self.gateway.bound_ports = self.runtime.gateway_bound_ports();
        self.browserd.bound_ports = self.runtime.browser_bound_ports();
    }

    fn reset_profile_bound_runtime_state(&mut self) -> Result<()> {
        self.http_client = build_http_client()?;
        if let Ok(mut session_cache) = self.console_session_cache.lock() {
            *session_cache = None;
        }
        if let Ok(mut payload_cache) = self.console_payload_cache.lock() {
            *payload_cache = ConsolePayloadCache::default();
        }
        self.gateway.logs.clear();
        self.browserd.logs.clear();
        self.node_host.logs.clear();
        self.gateway.last_exit = None;
        self.browserd.last_exit = None;
        self.node_host.last_exit = None;
        self.gateway.pid = None;
        self.browserd.pid = None;
        self.node_host.pid = None;
        self.gateway.last_start_unix_ms = None;
        self.browserd.last_start_unix_ms = None;
        self.node_host.last_start_unix_ms = None;
        self.gateway.restart_attempt = 0;
        self.browserd.restart_attempt = 0;
        self.node_host.restart_attempt = 0;
        self.gateway.next_restart_unix_ms = None;
        self.browserd.next_restart_unix_ms = None;
        self.node_host.next_restart_unix_ms = None;
        Ok(())
    }

    fn service_mut(&mut self, kind: ServiceKind) -> &mut ManagedService {
        match kind {
            ServiceKind::Gateway => &mut self.gateway,
            ServiceKind::Browserd => &mut self.browserd,
            ServiceKind::NodeHost => &mut self.node_host,
        }
    }

    fn service_ref(&self, kind: ServiceKind) -> &ManagedService {
        match kind {
            ServiceKind::Gateway => &self.gateway,
            ServiceKind::Browserd => &self.browserd,
            ServiceKind::NodeHost => &self.node_host,
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

    /// Builds a process snapshot for one supervised service.
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

    /// Returns recent logs across all supervised services.
    pub(crate) fn collect_logs(&self) -> Vec<LogLine> {
        let mut combined = Vec::new();
        combined.extend(self.gateway.logs.iter().cloned());
        combined.extend(self.browserd.logs.iter().cloned());
        combined.extend(self.node_host.logs.iter().cloned());
        combined.sort_by_key(|entry| std::cmp::Reverse(entry.unix_ms));
        combined.truncate(250);
        combined
    }

    /// Synchronizes node-host desired state with gateway and enrollment state.
    pub(crate) fn sync_node_host_desired_state(&mut self) {
        let should_run = self.gateway.desired_running && self.node_host_installed();
        self.node_host.desired_running = should_run;
        if should_run && !self.node_host.running() {
            self.node_host.next_restart_unix_ms = Some(unix_ms_now());
        }
        if !should_run {
            self.node_host.next_restart_unix_ms = None;
        }
    }

    /// Returns whether desktop node-host enrollment config exists.
    pub(crate) fn node_host_installed(&self) -> bool {
        self.runtime_root.join(NODE_HOST_STATE_DIR).join(NODE_HOST_CONFIG_FILE_NAME).is_file()
    }

    /// Stops only the desktop node-host sidecar.
    pub(crate) fn stop_node_host(&mut self) {
        self.stop_service(ServiceKind::NodeHost);
    }

    /// Opens a validated dashboard URL in the default browser.
    pub(crate) fn open_dashboard(&self, url: &str) -> Result<String> {
        open_url_in_default_browser(url)?;
        Ok(url.to_owned())
    }
}

/// File-based guard that prevents multiple desktop instances per state dir.
#[derive(Debug)]
pub(crate) struct DesktopInstanceLock {
    path: PathBuf,
}

impl DesktopInstanceLock {
    /// Acquires the single-instance lock, recovering stale locks when possible.
    pub(crate) fn acquire(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join("instance.lock");
        let mut file = match create_instance_lock_file(path.as_path()) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if recover_stale_instance_lock(path.as_path())? {
                    create_instance_lock_file(path.as_path()).with_context(|| {
                        format!(
                            "failed to recreate desktop instance lock {} after stale recovery",
                            path.display()
                        )
                    })?
                } else {
                    bail!(
                        "desktop control center is already running for state root {}",
                        state_dir.display()
                    );
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to create {}", path.display()));
            }
        };
        writeln!(file, "pid={}\nstarted_at_unix_ms={}", std::process::id(), unix_ms_now())
            .with_context(|| format!("failed to write desktop instance lock {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush desktop instance lock {}", path.display()))?;
        Ok(Self { path })
    }
}

fn create_instance_lock_file(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new().write(true).create_new(true).open(path)
}

fn recover_stale_instance_lock(path: &Path) -> Result<bool> {
    if !instance_lock_is_stale(path)? {
        return Ok(false);
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error).with_context(|| {
            format!("failed to remove stale desktop instance lock {}", path.display())
        }),
    }
}

fn instance_lock_is_stale(path: &Path) -> Result<bool> {
    let Some(pid) = read_instance_lock_pid(path)? else {
        return Ok(true);
    };
    Ok(!desktop_process_is_alive(pid)?)
}

fn read_instance_lock_pid(path: &Path) -> Result<Option<u32>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read desktop instance lock {}", path.display()))?;
    let Some(raw_pid) = contents.lines().find_map(|line| line.strip_prefix("pid=").map(str::trim))
    else {
        return Ok(None);
    };
    let pid = raw_pid.parse::<u32>().with_context(|| {
        format!("failed to parse PID from desktop instance lock {}", path.display())
    })?;
    Ok(Some(pid))
}

#[cfg(windows)]
fn desktop_process_is_alive(pid: u32) -> Result<bool> {
    palyra_common::windows_security::process_is_alive(pid)
        .with_context(|| format!("failed to probe desktop instance lock PID {pid}"))
}

#[cfg(unix)]
fn desktop_process_is_alive(pid: u32) -> Result<bool> {
    let pid = i32::try_from(pid).context("desktop instance lock PID exceeds platform range")?;
    let result = unsafe { libc::kill(pid, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error).with_context(|| format!("failed to probe desktop instance lock PID {pid}")),
    }
}

#[cfg(not(any(windows, unix)))]
fn desktop_process_is_alive(_pid: u32) -> Result<bool> {
    Ok(true)
}

impl Drop for DesktopInstanceLock {
    fn drop(&mut self) {
        match fs::remove_file(self.path.as_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
}

#[cfg(test)]
mod instance_lock_tests {
    use super::DesktopInstanceLock;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_state_dir() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("palyra-desktop-instance-lock-{unique}"));
        fs::create_dir_all(dir.as_path()).expect("temp state dir should be created");
        dir
    }

    #[test]
    fn desktop_instance_lock_rejects_parallel_acquire_for_same_state_dir() {
        let state_dir = temp_state_dir();
        let _guard = DesktopInstanceLock::acquire(state_dir.as_path())
            .expect("first desktop instance lock should succeed");

        let error = DesktopInstanceLock::acquire(state_dir.as_path())
            .expect_err("second desktop instance lock should fail");

        assert!(
            error.to_string().contains("already running"),
            "parallel desktop instance error should explain the single-instance guard: {error}"
        );
        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn desktop_instance_lock_releases_on_drop() {
        let state_dir = temp_state_dir();
        {
            let _guard = DesktopInstanceLock::acquire(state_dir.as_path())
                .expect("desktop instance lock should succeed");
        }

        DesktopInstanceLock::acquire(state_dir.as_path())
            .expect("lock should be reacquirable after prior guard drops");
        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn desktop_instance_lock_recovers_stale_lock_for_missing_process() {
        let state_dir = temp_state_dir();
        let lock_path = state_dir.join("instance.lock");
        fs::write(lock_path.as_path(), "pid=2147483647\nstarted_at_unix_ms=1\n")
            .expect("stale desktop instance lock should be written");

        let _guard = DesktopInstanceLock::acquire(state_dir.as_path())
            .expect("stale desktop instance lock should be recovered");

        let contents = fs::read_to_string(lock_path.as_path())
            .expect("recovered desktop instance lock should be readable");
        assert!(
            contents.contains(format!("pid={}", std::process::id()).as_str()),
            "recovered desktop instance lock should be rewritten for the current process: {contents}"
        );
        let _ = fs::remove_dir_all(state_dir);
    }
}

impl Drop for ControlCenter {
    fn drop(&mut self) {
        self.stop_all();
    }
}

/// Returns trimmed text when it is non-empty.
pub(crate) fn normalize_optional_text(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn build_http_client() -> Result<Client> {
    Client::builder()
        .cookie_store(true)
        .timeout(Duration::from_secs(4))
        .build()
        .context("failed to build desktop HTTP client")
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

/// Computes capped exponential restart backoff in milliseconds.
pub(crate) fn compute_backoff_ms(attempt: u32) -> u64 {
    let exponent = attempt.min(5);
    let scaled = 1_000_u64.saturating_mul(1_u64 << exponent);
    scaled.min(30_000)
}

fn resolve_active_profile(
    persisted: &DesktopStateFile,
    catalog: &DesktopProfileCatalog,
) -> DesktopResolvedProfile {
    let active_name = persisted.active_profile_name();
    if let Some(profile) = catalog.profiles.get(active_name) {
        return profile.clone();
    }
    if let Some(default_name) = catalog.default_profile_name.as_deref() {
        if let Some(profile) = catalog.profiles.get(default_name) {
            return profile.clone();
        }
    }
    catalog
        .profiles
        .get(crate::desktop_state::IMPLICIT_DESKTOP_PROFILE_NAME)
        .cloned()
        .or_else(|| catalog.profiles.values().next().cloned())
        .expect("desktop profile catalog must contain at least one profile")
}

fn default_runtime_root_for_profile(
    base_state_root: &Path,
    state_dir: &Path,
    profile: &DesktopResolvedProfile,
) -> PathBuf {
    if let Some(state_root) = profile.state_root.as_ref() {
        return state_root.clone();
    }
    if profile.implicit {
        return state_dir.join("runtime");
    }
    base_state_root.join("profiles").join(profile.context.name.as_str()).join("state")
}

fn resolve_runtime_root_for_profile(
    persisted: &DesktopStateFile,
    profile: &DesktopResolvedProfile,
    default_runtime_root: &Path,
) -> Result<PathBuf> {
    if let Some(state_root) = profile.state_root.as_ref() {
        if !state_root.is_absolute() {
            bail!("desktop profile '{}' must use an absolute state root", profile.context.name);
        }
        return Ok(state_root.clone());
    }
    persisted.resolve_runtime_root(default_runtime_root)
}

fn apply_profile_process_env(profile: &DesktopResolvedProfile) {
    // SAFETY: desktop app serializes control-center mutations through a single async mutex.
    unsafe {
        std::env::set_var("PALYRA_CLI_PROFILE", profile.context.name.as_str());
        if let Some(config_path) = profile.config_path.as_ref() {
            std::env::set_var("PALYRA_CONFIG", config_path.as_os_str());
        } else {
            std::env::remove_var("PALYRA_CONFIG");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultScope};

    use super::{
        browserd_env_values, normalize_browser_open_url, runtime_auth_with_config_overrides,
        runtime_config_with_config_overrides, runtime_secrets_with_config_overrides,
        BrowserStateEncryptionKey, ConfigReloadWatchOutcome, DesktopConfigReloadWatchState,
    };
    use crate::desktop_state::DesktopRuntimeSecrets;
    use crate::profile_registry::implicit_profile;
    use crate::RuntimeConfig;

    const TEST_BROWSER_STATE_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

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
            error.to_string().contains("must not include embedded credentials"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn runtime_secrets_with_config_overrides_prefers_cli_visible_tokens() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-token-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        fs::write(
            config_path.as_path(),
            r#"
version = 1

[admin]
auth_token = "config-admin-token"

[tool_call.browser_service]
auth_token = "config-browser-token"
"#,
        )
        .expect("config fixture should be written");

        let fallback = DesktopRuntimeSecrets {
            admin_token: "desktop-admin-token".to_owned(),
            browser_auth_token: "desktop-browser-token".to_owned(),
            browser_state_encryption_key: TEST_BROWSER_STATE_KEY.to_owned(),
        };
        let effective =
            runtime_secrets_with_config_overrides(&fallback, Some(config_path.as_path()))
                .expect("config token overrides should load");

        assert_eq!(effective.admin_token, "config-admin-token");
        assert_eq!(effective.browser_auth_token, "config-browser-token");
        assert_eq!(effective.browser_state_encryption_key, TEST_BROWSER_STATE_KEY);
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn runtime_secrets_with_config_overrides_falls_back_when_tokens_are_missing() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-token-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        fs::write(config_path.as_path(), "version = 1\n")
            .expect("config fixture should be written");

        let fallback = DesktopRuntimeSecrets {
            admin_token: "desktop-admin-token".to_owned(),
            browser_auth_token: "desktop-browser-token".to_owned(),
            browser_state_encryption_key: TEST_BROWSER_STATE_KEY.to_owned(),
        };
        let effective =
            runtime_secrets_with_config_overrides(&fallback, Some(config_path.as_path()))
                .expect("fallback token resolution should load");

        assert_eq!(effective.admin_token, "desktop-admin-token");
        assert_eq!(effective.browser_auth_token, "desktop-browser-token");
        assert_eq!(effective.browser_state_encryption_key, TEST_BROWSER_STATE_KEY);
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn runtime_auth_with_config_overrides_reads_browser_state_key_vault_ref() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-browser-key-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let identity_root = fixture.join("identity");
        let vault_root = fixture.join("vault");
        let browser_state_key = TEST_BROWSER_STATE_KEY;
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(vault_root.clone()),
            identity_store_root: Some(identity_root.clone()),
            backend_preference: BackendPreference::EncryptedFile,
            ..Default::default()
        })
        .expect("fixture vault should open");
        vault
            .put_secret(&VaultScope::Global, "browser_state_key", browser_state_key.as_bytes())
            .expect("browser state key should be stored");

        let config_path = fixture.join("palyra.toml");
        fs::write(
            config_path.as_path(),
            format!(
                r#"
version = 1

[gateway]
identity_store_dir = '{}'

[storage]
vault_dir = '{}'

[tool_call.browser_service]
state_key_vault_ref = "global/browser_state_key"
"#,
                identity_root.display(),
                vault_root.display()
            ),
        )
        .expect("config fixture should be written");

        let fallback = DesktopRuntimeSecrets {
            admin_token: "desktop-admin-token".to_owned(),
            browser_auth_token: "desktop-browser-token".to_owned(),
            browser_state_encryption_key: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=".to_owned(),
        };
        let effective = runtime_auth_with_config_overrides(&fallback, Some(config_path.as_path()))
            .expect("browser state key vault ref should resolve");

        assert_eq!(
            effective.browser_state_encryption_key.as_ref().map(|key| key.as_str()),
            Some(browser_state_key)
        );
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn browserd_env_values_include_resolved_profile_state_key() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-browser-env-test-{}", ulid::Ulid::new()));
        let browser_state_key =
            BrowserStateEncryptionKey::parse(TEST_BROWSER_STATE_KEY, "fixture browser state key")
                .expect("fixture browser state key should parse");

        let env =
            browserd_env_values(fixture.as_path(), "browser-auth-token", Some(&browser_state_key));

        assert_eq!(
            env.iter()
                .find(|(key, _)| key == super::BROWSERD_STATE_ENCRYPTION_KEY_ENV)
                .map(|(_, value)| value.as_str()),
            Some(browser_state_key.as_str())
        );
    }

    #[test]
    fn runtime_config_with_config_overrides_uses_configured_ports() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-port-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        fs::write(
            config_path.as_path(),
            r#"
version = 1

[daemon]
port = 7310

[gateway]
grpc_port = 7311
quic_port = 7312

[tool_call.browser_service]
endpoint = "http://127.0.0.1:7314"
health_base_url = "http://127.0.0.1:7313"
"#,
        )
        .expect("config fixture should be written");

        let effective = runtime_config_with_config_overrides(
            RuntimeConfig::default(),
            Some(config_path.as_path()),
        )
        .expect("config port overrides should load");

        assert_eq!(effective.gateway_admin_port, 7310);
        assert_eq!(effective.gateway_grpc_port, 7311);
        assert_eq!(effective.gateway_quic_port, 7312);
        assert_eq!(effective.browser_health_port, 7313);
        assert_eq!(effective.browser_grpc_port, 7314);
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn runtime_auth_with_config_overrides_uses_config_principal_binding() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-auth-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        fs::write(
            config_path.as_path(),
            r#"
version = 1

[admin]
auth_token = "config-admin-token"
bound_principal = "admin:local"
"#,
        )
        .expect("config fixture should be written");

        let fallback = DesktopRuntimeSecrets {
            admin_token: "desktop-admin-token".to_owned(),
            browser_auth_token: "desktop-browser-token".to_owned(),
            browser_state_encryption_key: TEST_BROWSER_STATE_KEY.to_owned(),
        };
        let effective = runtime_auth_with_config_overrides(&fallback, Some(config_path.as_path()))
            .expect("config auth overrides should load");

        assert_eq!(effective.admin_token, "config-admin-token");
        assert_eq!(effective.admin_bound_principal.as_deref(), Some("admin:local"));
        assert_eq!(effective.browser_auth_token, "desktop-browser-token");
        assert_eq!(
            effective.browser_state_encryption_key.as_ref().map(|key| key.as_str()),
            Some(TEST_BROWSER_STATE_KEY)
        );
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn runtime_auth_with_missing_config_keeps_desktop_principal_binding() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-auth-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("missing-palyra.toml");
        let fallback = DesktopRuntimeSecrets {
            admin_token: "desktop-admin-token".to_owned(),
            browser_auth_token: "desktop-browser-token".to_owned(),
            browser_state_encryption_key: TEST_BROWSER_STATE_KEY.to_owned(),
        };

        let effective = runtime_auth_with_config_overrides(&fallback, Some(config_path.as_path()))
            .expect("fallback auth should load");

        assert_eq!(effective.admin_token, "desktop-admin-token");
        assert_eq!(effective.admin_bound_principal.as_deref(), Some(super::CONSOLE_PRINCIPAL));
        assert_eq!(effective.browser_auth_token, "desktop-browser-token");
        assert_eq!(
            effective.browser_state_encryption_key.as_ref().map(|key| key.as_str()),
            Some(TEST_BROWSER_STATE_KEY)
        );
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn config_reload_watch_treats_first_config_creation_as_change() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-config-watch-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        let mut profile = implicit_profile("desktop-local");
        profile.config_path = Some(config_path.clone());
        let mut watch = DesktopConfigReloadWatchState::from_profile(&profile);

        fs::write(config_path.as_path(), "version = 1\n")
            .expect("config fixture should be written");

        match watch.observe() {
            ConfigReloadWatchOutcome::Changed { path } => assert_eq!(path, config_path),
            ConfigReloadWatchOutcome::Idle => panic!("first config creation should trigger reload"),
        }
        assert!(matches!(watch.observe(), ConfigReloadWatchOutcome::Idle));
        let _ = fs::remove_dir_all(fixture.as_path());
    }

    #[test]
    fn config_reload_watch_uses_existing_config_as_startup_baseline() {
        let fixture =
            env::temp_dir().join(format!("palyra-desktop-config-watch-test-{}", ulid::Ulid::new()));
        fs::create_dir_all(fixture.as_path()).expect("fixture dir should be created");
        let config_path = fixture.join("palyra.toml");
        fs::write(config_path.as_path(), "version = 1\n")
            .expect("config fixture should be written");
        let mut profile = implicit_profile("desktop-local");
        profile.config_path = Some(config_path);
        let mut watch = DesktopConfigReloadWatchState::from_profile(&profile);

        assert!(matches!(watch.observe(), ConfigReloadWatchOutcome::Idle));
        let _ = fs::remove_dir_all(fixture.as_path());
    }
}

/// Resolves a sidecar binary from an explicit env override or local build paths.
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

/// Returns the platform-specific executable file name.
pub(crate) fn executable_file_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_owned()
    }
}

/// Returns the current Unix time in milliseconds.
pub(crate) fn unix_ms_now() -> i64 {
    system_time_to_unix_ms(SystemTime::now())
}

fn system_time_to_unix_ms(value: SystemTime) -> i64 {
    value
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

/// Enqueues a log event or records an overflow without blocking readers.
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
