use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use palyra_common::{
    config_system::get_value_at_path, default_config_search_paths, default_state_root,
    parse_config_path, parse_daemon_bind_socket, IdentityStorePathError,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    args::{LogLevelArg, OutputFormatArg, RootOptions},
    load_document_from_existing_path, normalize_client_socket, AgentConnection, DEFAULT_CHANNEL,
    DEFAULT_DAEMON_BIND_ADDR, DEFAULT_DAEMON_PORT, DEFAULT_DAEMON_URL, DEFAULT_DEVICE_ID,
    DEFAULT_GATEWAY_GRPC_BIND_ADDR, DEFAULT_GATEWAY_GRPC_PORT,
};

const CLI_PROFILE_ENV: &str = "PALYRA_CLI_PROFILE";
const CLI_PROFILES_PATH_ENV: &str = "PALYRA_CLI_PROFILES_PATH";
const CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const CLI_PROFILE_SCHEMA_VERSION: u32 = 1;
const PROFILE_STATE_ROOT_RELATIVE_PREFIX: &str = "profiles";
const PROFILE_STATE_ROOT_RELATIVE_SUFFIX: &str = "state";
const PROFILE_CONFIG_RELATIVE_DIR: &str = "config";
const PROFILE_CONFIG_FILE_NAME: &str = "palyra.toml";

#[derive(Debug, Clone)]
pub(crate) struct RootCommandContext {
    cli_state_root: PathBuf,
    state_root: PathBuf,
    config_path: Option<PathBuf>,
    profile_name: Option<String>,
    output_format: OutputFormatArg,
    log_level: LogLevelArg,
    no_color: bool,
    trace_id: String,
    profile: Option<CliConnectionProfile>,
    config_defaults: ConfigConnectionDefaults,
    pub(crate) allow_strict_profile_actions: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionDefaults {
    pub principal: &'static str,
    pub device_id: &'static str,
    pub channel: &'static str,
}

impl ConnectionDefaults {
    pub(crate) const USER: Self =
        Self { principal: "user:local", device_id: DEFAULT_DEVICE_ID, channel: DEFAULT_CHANNEL };

    pub(crate) const ADMIN: Self =
        Self { principal: "admin:local", device_id: DEFAULT_DEVICE_ID, channel: DEFAULT_CHANNEL };
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectionOverrides {
    pub daemon_url: Option<String>,
    pub grpc_url: Option<String>,
    pub token: Option<String>,
    pub principal: Option<String>,
    pub device_id: Option<String>,
    pub channel: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct HttpConnection {
    pub base_url: String,
    pub token: Option<String>,
    pub principal: String,
    pub device_id: String,
    pub channel: String,
    pub trace_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CliProfilesDocument {
    pub(crate) version: Option<u32>,
    pub(crate) default_profile: Option<String>,
    #[serde(default)]
    pub(crate) profiles: BTreeMap<String, CliConnectionProfile>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CliConnectionProfile {
    pub(crate) config_path: Option<String>,
    pub(crate) state_root: Option<String>,
    pub(crate) daemon_url: Option<String>,
    pub(crate) grpc_url: Option<String>,
    pub(crate) admin_token: Option<String>,
    pub(crate) admin_token_env: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) device_id: Option<String>,
    pub(crate) channel: Option<String>,
    pub(crate) label: Option<String>,
    pub(crate) environment: Option<String>,
    pub(crate) color: Option<String>,
    pub(crate) risk_level: Option<String>,
    #[serde(default)]
    pub(crate) strict_mode: bool,
    pub(crate) mode: Option<String>,
    pub(crate) created_at_unix_ms: Option<i64>,
    pub(crate) updated_at_unix_ms: Option<i64>,
    pub(crate) last_used_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ActiveProfileContext {
    pub(crate) name: String,
    pub(crate) label: String,
    pub(crate) environment: String,
    pub(crate) color: String,
    pub(crate) risk_level: String,
    pub(crate) strict_mode: bool,
    pub(crate) mode: String,
}

#[derive(Debug, Clone, Default)]
struct ConfigConnectionDefaults {
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token: Option<String>,
    principal: Option<String>,
}

#[derive(Debug, Clone)]
struct ConnectionEnvironment {
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    grpc_bind_addr: String,
    grpc_port: u16,
    admin_token: Option<String>,
    admin_bound_principal: Option<String>,
    profile_admin_token: Option<String>,
}

static ROOT_CONTEXT: OnceLock<Mutex<Option<RootCommandContext>>> = OnceLock::new();

fn context_cell() -> &'static Mutex<Option<RootCommandContext>> {
    ROOT_CONTEXT.get_or_init(|| Mutex::new(None))
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum ExplicitConfigPathPolicy {
    #[default]
    RequireExisting,
    AllowMissingForBootstrap,
}

#[cfg(test)]
pub(crate) fn test_env_lock_for_tests() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn install_root_context(root: RootOptions) -> Result<RootCommandContext> {
    install_root_context_with_policy(root, ExplicitConfigPathPolicy::RequireExisting)
}

pub(crate) fn install_root_context_with_policy(
    root: RootOptions,
    explicit_config_path_policy: ExplicitConfigPathPolicy,
) -> Result<RootCommandContext> {
    let context = build_root_context(root, explicit_config_path_policy)?;
    let mut guard =
        context_cell().lock().map_err(|_| anyhow::anyhow!("CLI root context lock poisoned"))?;
    *guard = Some(context.clone());
    Ok(context)
}

pub(crate) fn current_root_context() -> Option<RootCommandContext> {
    context_cell().lock().ok().and_then(|guard| guard.as_ref().cloned())
}

#[cfg(test)]
pub(crate) fn clear_root_context_for_tests() {
    if let Ok(mut guard) = context_cell().lock() {
        *guard = None;
    }
}

impl RootCommandContext {
    pub(crate) fn output_format(&self) -> OutputFormatArg {
        self.output_format
    }

    pub(crate) fn log_level(&self) -> LogLevelArg {
        self.log_level
    }

    pub(crate) fn no_color(&self) -> bool {
        self.no_color
    }

    pub(crate) fn trace_id(&self) -> &str {
        self.trace_id.as_str()
    }

    pub(crate) fn profile_name(&self) -> Option<&str> {
        self.profile_name.as_deref()
    }

    pub(crate) fn active_profile_context(&self) -> Option<ActiveProfileContext> {
        build_active_profile_context(self.profile_name.as_deref(), self.profile.as_ref())
    }

    pub(crate) fn strict_profile_mode(&self) -> bool {
        self.active_profile_context().is_some_and(|profile| profile.strict_mode)
    }

    pub(crate) fn state_root(&self) -> &Path {
        self.state_root.as_path()
    }

    pub(crate) fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    pub(crate) fn prefers_json(&self) -> bool {
        matches!(self.output_format, OutputFormatArg::Json)
    }

    pub(crate) fn prefers_ndjson(&self) -> bool {
        matches!(self.output_format, OutputFormatArg::Ndjson)
    }

    pub(crate) fn resolve_grpc_connection(
        &self,
        overrides: ConnectionOverrides,
        defaults: ConnectionDefaults,
    ) -> Result<AgentConnection> {
        Ok(AgentConnection {
            grpc_url: self.resolve_grpc_url(overrides.grpc_url)?,
            token: self.resolve_token(overrides.token),
            principal: self.resolve_principal(overrides.principal, defaults),
            device_id: self.resolve_device_id(overrides.device_id, defaults),
            channel: self.resolve_channel(overrides.channel, defaults),
            trace_id: self.trace_id.clone(),
        })
    }

    pub(crate) fn resolve_http_connection(
        &self,
        overrides: ConnectionOverrides,
        defaults: ConnectionDefaults,
    ) -> Result<HttpConnection> {
        Ok(HttpConnection {
            base_url: self.resolve_daemon_url(overrides.daemon_url)?,
            token: self.resolve_token(overrides.token),
            principal: self.resolve_principal(overrides.principal, defaults),
            device_id: self.resolve_device_id(overrides.device_id, defaults),
            channel: self.resolve_channel(overrides.channel, defaults),
            trace_id: self.trace_id.clone(),
        })
    }

    fn resolve_daemon_url(&self, override_url: Option<String>) -> Result<String> {
        if let Some(url) = normalize_optional_text(override_url.as_deref()) {
            return Ok(url.to_owned());
        }
        if let Some(url) = self
            .profile
            .as_ref()
            .and_then(|profile| normalize_optional_text(profile.daemon_url.as_deref()))
        {
            return Ok(url.to_owned());
        }
        if let Some(url) = self.config_defaults.daemon_url.as_deref() {
            return Ok(url.to_owned());
        }
        let connection_env = ConnectionEnvironment::read(self.profile.as_ref());
        if let Some(url) = connection_env.daemon_url.as_deref() {
            return Ok(url.to_owned());
        }
        Ok(DEFAULT_DAEMON_URL.to_owned())
    }

    fn resolve_grpc_url(&self, override_url: Option<String>) -> Result<String> {
        if let Some(url) = normalize_optional_text(override_url.as_deref()) {
            return Ok(url.to_owned());
        }
        if let Some(url) = self
            .profile
            .as_ref()
            .and_then(|profile| normalize_optional_text(profile.grpc_url.as_deref()))
        {
            return Ok(url.to_owned());
        }
        if let Some(url) = self.config_defaults.grpc_url.as_deref() {
            return Ok(url.to_owned());
        }
        let connection_env = ConnectionEnvironment::read(self.profile.as_ref());
        if let Some(url) = connection_env.grpc_url.as_deref() {
            return Ok(url.to_owned());
        }
        let socket = parse_daemon_bind_socket(
            connection_env.grpc_bind_addr.as_str(),
            connection_env.grpc_port,
        )
        .context("invalid gateway gRPC bind config")?;
        let socket = normalize_client_socket(socket);
        Ok(format!("http://{socket}"))
    }

    fn resolve_token(&self, override_token: Option<String>) -> Option<String> {
        let connection_env = ConnectionEnvironment::read(self.profile.as_ref());
        normalize_owned_text(override_token)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.admin_token.clone()))
            })
            .or(connection_env.profile_admin_token)
            .or_else(|| normalize_owned_text(self.config_defaults.admin_token.clone()))
            .or(connection_env.admin_token)
    }

    fn resolve_principal(
        &self,
        override_principal: Option<String>,
        defaults: ConnectionDefaults,
    ) -> String {
        let connection_env = ConnectionEnvironment::read(self.profile.as_ref());
        normalize_owned_text(override_principal)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.principal.clone()))
            })
            .or_else(|| normalize_owned_text(self.config_defaults.principal.clone()))
            .or(connection_env.admin_bound_principal)
            .unwrap_or_else(|| defaults.principal.to_owned())
    }

    fn resolve_device_id(
        &self,
        override_device_id: Option<String>,
        defaults: ConnectionDefaults,
    ) -> String {
        normalize_owned_text(override_device_id)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.device_id.clone()))
            })
            .unwrap_or_else(|| defaults.device_id.to_owned())
    }

    fn resolve_channel(
        &self,
        override_channel: Option<String>,
        defaults: ConnectionDefaults,
    ) -> String {
        normalize_owned_text(override_channel)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.channel.clone()))
            })
            .unwrap_or_else(|| defaults.channel.to_owned())
    }
}

fn build_root_context(
    root: RootOptions,
    explicit_config_path_policy: ExplicitConfigPathPolicy,
) -> Result<RootCommandContext> {
    let bootstrap_state_root = resolve_cli_state_root(root.state_root.as_deref())?;
    let profiles_path = resolve_profiles_path(&bootstrap_state_root)?;
    let profiles = load_profiles_document(profiles_path.as_deref())?;
    let profile_name = resolve_active_profile_name(&root, &profiles);
    let expected_profile_name = normalize_owned_text(root.expect_profile.clone());
    let profile = resolve_profile(profile_name.as_deref(), &profiles)?;
    let state_root = resolve_final_state_root(&root, profile.as_ref())?;
    let config_path = resolve_config_path(&root, profile.as_ref(), explicit_config_path_policy)?;
    let config_defaults = load_config_defaults(config_path.as_deref())?;
    validate_expected_profile(
        expected_profile_name.as_deref(),
        profile_name.as_deref(),
        root.allow_profile_mismatch,
    )?;

    Ok(RootCommandContext {
        cli_state_root: bootstrap_state_root,
        output_format: resolve_output_format(&root),
        log_level: resolve_log_level(&root),
        no_color: root.no_color,
        trace_id: format!("cli:{}", Ulid::new()),
        state_root,
        config_path,
        profile_name,
        profile,
        config_defaults,
        allow_strict_profile_actions: root.allow_strict_profile_actions,
    })
}

fn resolve_output_format(root: &RootOptions) -> OutputFormatArg {
    if root.plain {
        OutputFormatArg::Text
    } else {
        root.output_format
    }
}

fn resolve_log_level(root: &RootOptions) -> LogLevelArg {
    match root.verbose {
        0 => root.log_level,
        1 => LogLevelArg::Debug,
        _ => LogLevelArg::Trace,
    }
}

fn resolve_profiles_path(base_state_root: &Path) -> Result<Option<PathBuf>> {
    if let Ok(raw) = env::var(CLI_PROFILES_PATH_ENV) {
        let Some(path) = normalize_optional_text(Some(raw.as_str())) else {
            return Ok(None);
        };
        let parsed = parse_config_path(path)
            .with_context(|| format!("{CLI_PROFILES_PATH_ENV} contains an invalid path"))?;
        return Ok(Some(parsed));
    }

    let default_path = base_state_root.join(CLI_PROFILES_RELATIVE_PATH);
    if default_path.exists() {
        Ok(Some(default_path))
    } else {
        Ok(None)
    }
}

fn load_profiles_document(path: Option<&Path>) -> Result<CliProfilesDocument> {
    let Some(path) = path else {
        return Ok(CliProfilesDocument::default());
    };
    if !path.exists() {
        return Ok(CliProfilesDocument::default());
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read CLI profiles {}", path.display()))?;
    let document: CliProfilesDocument = toml::from_str(content.as_str())
        .with_context(|| format!("failed to parse CLI profiles {}", path.display()))?;
    if let Some(version) = document.version {
        if version != CLI_PROFILE_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported CLI profile schema version {version}; expected {CLI_PROFILE_SCHEMA_VERSION}"
            );
        }
    }
    Ok(document)
}

pub(crate) fn cli_profiles_registry_path() -> Result<PathBuf> {
    let base_state_root = current_root_context()
        .map(|context| context.cli_state_root.to_path_buf())
        .unwrap_or(resolve_cli_state_root(None)?);
    resolve_profiles_storage_path(base_state_root.as_path())
}

fn resolve_profiles_storage_path(base_state_root: &Path) -> Result<PathBuf> {
    if let Ok(raw) = env::var(CLI_PROFILES_PATH_ENV) {
        let Some(path) = normalize_optional_text(Some(raw.as_str())) else {
            return Ok(base_state_root.join(CLI_PROFILES_RELATIVE_PATH));
        };
        return parse_config_path(path)
            .with_context(|| format!("{CLI_PROFILES_PATH_ENV} contains an invalid path"));
    }
    Ok(base_state_root.join(CLI_PROFILES_RELATIVE_PATH))
}

pub(crate) fn load_cli_profiles_registry() -> Result<(PathBuf, CliProfilesDocument)> {
    let path = cli_profiles_registry_path()?;
    let document = load_profiles_document(Some(path.as_path()))?;
    Ok((path, document))
}

pub(crate) fn persist_cli_profiles_registry(
    path: &Path,
    document: &CliProfilesDocument,
) -> Result<()> {
    let mut persisted = document.clone();
    persisted.version = Some(CLI_PROFILE_SCHEMA_VERSION);
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create CLI profiles directory {}", parent.display())
        })?;
    }
    let rendered =
        toml::to_string_pretty(&persisted).context("failed to serialize CLI profiles registry")?;
    fs::write(path, rendered.as_bytes())
        .with_context(|| format!("failed to write CLI profiles {}", path.display()))
}

pub(crate) fn default_profile_state_root(profile_name: &str) -> Result<PathBuf> {
    let base_state_root = resolve_cli_state_root(None)?;
    Ok(base_state_root
        .join(PROFILE_STATE_ROOT_RELATIVE_PREFIX)
        .join(profile_name)
        .join(PROFILE_STATE_ROOT_RELATIVE_SUFFIX))
}

pub(crate) fn default_profile_config_path(profile_name: &str) -> Result<PathBuf> {
    let base_state_root = resolve_cli_state_root(None)?;
    Ok(base_state_root
        .join(PROFILE_STATE_ROOT_RELATIVE_PREFIX)
        .join(profile_name)
        .join(PROFILE_CONFIG_RELATIVE_DIR)
        .join(PROFILE_CONFIG_FILE_NAME))
}

pub(crate) fn validate_profile_name(raw: &str) -> Result<String> {
    let normalized = raw.trim();
    if normalized.is_empty() {
        anyhow::bail!("profile name cannot be empty");
    }
    if normalized.len() > 64 {
        anyhow::bail!("profile name must be 64 characters or fewer");
    }
    let is_valid = normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.'));
    if !is_valid {
        anyhow::bail!(
            "profile name must use only lowercase ASCII letters, digits, '.', '-', or '_'"
        );
    }
    Ok(normalized.to_owned())
}

pub(crate) fn normalized_profile_text(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|candidate| !candidate.is_empty()).map(ToOwned::to_owned)
}

pub(crate) fn update_active_profile_paths(
    config_path: Option<&Path>,
    state_root: Option<&Path>,
) -> Result<()> {
    let Some(context) = current_root_context() else {
        return Ok(());
    };
    let Some(profile_name) = context.profile_name().map(ToOwned::to_owned) else {
        return Ok(());
    };
    let (path, mut document) = load_cli_profiles_registry()?;
    let Some(profile) = document.profiles.get_mut(profile_name.as_str()) else {
        return Ok(());
    };
    if let Some(config_path) = config_path {
        profile.config_path = Some(config_path.display().to_string());
    }
    if let Some(state_root) = state_root {
        profile.state_root = Some(state_root.display().to_string());
    }
    profile.updated_at_unix_ms = Some(current_unix_timestamp_ms()?);
    profile.last_used_at_unix_ms = profile.updated_at_unix_ms;
    persist_cli_profiles_registry(path.as_path(), &document)
}

fn resolve_active_profile_name(
    root: &RootOptions,
    profiles: &CliProfilesDocument,
) -> Option<String> {
    normalize_owned_text(root.profile.clone())
        .or_else(|| {
            env::var(CLI_PROFILE_ENV).ok().and_then(|value| normalize_owned_text(Some(value)))
        })
        .or_else(|| normalize_owned_text(profiles.default_profile.clone()))
}

fn resolve_profile(
    profile_name: Option<&str>,
    profiles: &CliProfilesDocument,
) -> Result<Option<CliConnectionProfile>> {
    let Some(profile_name) = profile_name else {
        return Ok(None);
    };
    let Some(profile) = profiles.profiles.get(profile_name) else {
        anyhow::bail!("CLI profile not found: {profile_name}");
    };
    Ok(Some(profile.clone()))
}

fn validate_expected_profile(
    expected_profile_name: Option<&str>,
    actual_profile_name: Option<&str>,
    allow_profile_mismatch: bool,
) -> Result<()> {
    let Some(expected_profile_name) = expected_profile_name else {
        return Ok(());
    };
    if allow_profile_mismatch {
        return Ok(());
    }
    match actual_profile_name {
        Some(actual_profile_name) if actual_profile_name == expected_profile_name => Ok(()),
        Some(actual_profile_name) => anyhow::bail!(
            "active CLI profile mismatch: expected `{expected_profile_name}` but resolved `{actual_profile_name}`; re-run with --profile {expected_profile_name} or acknowledge the mismatch with --allow-profile-mismatch"
        ),
        None => anyhow::bail!(
            "active CLI profile mismatch: expected `{expected_profile_name}` but no active profile was resolved; re-run with --profile {expected_profile_name} or acknowledge the mismatch with --allow-profile-mismatch"
        ),
    }
}

fn build_active_profile_context(
    profile_name: Option<&str>,
    profile: Option<&CliConnectionProfile>,
) -> Option<ActiveProfileContext> {
    let profile_name = profile_name?;
    let profile = profile?;
    let mode = normalize_optional_text(profile.mode.as_deref()).unwrap_or_else(|| {
        if normalize_optional_text(profile.daemon_url.as_deref())
            .is_some_and(|value| !value.contains("127.0.0.1") && !value.contains("localhost"))
        {
            "remote"
        } else {
            "local"
        }
    });
    let strict_mode = profile.strict_mode;
    let environment =
        normalize_optional_text(profile.environment.as_deref()).unwrap_or_else(|| {
            if strict_mode || mode.eq_ignore_ascii_case("remote") {
                "production"
            } else {
                "local"
            }
        });
    let risk_level = normalize_optional_text(profile.risk_level.as_deref()).unwrap_or_else(|| {
        if strict_mode {
            "high"
        } else if mode.eq_ignore_ascii_case("remote") {
            "elevated"
        } else {
            "low"
        }
    });
    let color = normalize_optional_text(profile.color.as_deref()).unwrap_or(match risk_level {
        "critical" | "high" => "red",
        "elevated" => "amber",
        _ => "green",
    });
    let label = normalize_optional_text(profile.label.as_deref()).unwrap_or(profile_name);
    Some(ActiveProfileContext {
        name: profile_name.to_owned(),
        label: label.to_owned(),
        environment: environment.to_owned(),
        color: color.to_owned(),
        risk_level: risk_level.to_owned(),
        strict_mode,
        mode: mode.to_owned(),
    })
}

pub(crate) fn resolve_cli_state_root(explicit: Option<&str>) -> Result<PathBuf> {
    resolve_cli_state_root_with(explicit, env::var("PALYRA_STATE_ROOT").ok(), default_state_root)
}

fn resolve_cli_state_root_with<F>(
    explicit: Option<&str>,
    env_state_root: Option<String>,
    default_state_root_resolver: F,
) -> Result<PathBuf>
where
    F: FnOnce() -> Result<PathBuf, IdentityStorePathError>,
{
    if let Some(explicit) = normalize_optional_text(explicit) {
        return parse_config_path(explicit)
            .with_context(|| format!("state root path is invalid: {explicit}"));
    }
    if let Some(raw) = normalize_optional_text(env_state_root.as_deref()) {
        return parse_config_path(raw)
            .with_context(|| "PALYRA_STATE_ROOT contains an invalid path");
    }
    default_state_root_resolver().map_err(missing_default_state_root_error)
}

#[cfg(windows)]
fn missing_default_state_root_error(error: IdentityStorePathError) -> anyhow::Error {
    match error {
        IdentityStorePathError::AppDataNotSet => anyhow::anyhow!(
            "LOCALAPPDATA and APPDATA are unset; set PALYRA_STATE_ROOT or pass --state-root explicitly"
        ),
    }
}

#[cfg(not(windows))]
fn missing_default_state_root_error(error: IdentityStorePathError) -> anyhow::Error {
    match error {
        IdentityStorePathError::HomeNotSet => {
            anyhow::anyhow!("HOME is unset; set PALYRA_STATE_ROOT or pass --state-root explicitly")
        }
    }
}

fn resolve_final_state_root(
    root: &RootOptions,
    profile: Option<&CliConnectionProfile>,
) -> Result<PathBuf> {
    if let Some(explicit) = normalize_optional_text(root.state_root.as_deref()) {
        return parse_config_path(explicit)
            .with_context(|| format!("state root path is invalid: {explicit}"));
    }
    if let Some(raw) = normalize_optional_text(env::var("PALYRA_STATE_ROOT").ok().as_deref()) {
        return parse_config_path(raw)
            .with_context(|| "PALYRA_STATE_ROOT contains an invalid path");
    }
    if let Some(profile_state_root) =
        profile.and_then(|profile| normalize_optional_text(profile.state_root.as_deref()))
    {
        return parse_config_path(profile_state_root)
            .with_context(|| format!("profile state_root is invalid: {profile_state_root}"));
    }
    resolve_cli_state_root(None)
}

fn resolve_config_path(
    root: &RootOptions,
    profile: Option<&CliConnectionProfile>,
    explicit_config_path_policy: ExplicitConfigPathPolicy,
) -> Result<Option<PathBuf>> {
    if let Some(explicit) = normalize_optional_text(root.config_path.as_deref()) {
        let parsed = parse_config_path(explicit)
            .with_context(|| format!("config path is invalid: {explicit}"))?;
        if !parsed.exists() {
            if matches!(
                explicit_config_path_policy,
                ExplicitConfigPathPolicy::AllowMissingForBootstrap
            ) {
                return Ok(Some(parsed));
            }
            anyhow::bail!("config file does not exist: {}", parsed.display());
        }
        return Ok(Some(parsed));
    }

    if let Ok(raw) = env::var("PALYRA_CONFIG") {
        if let Some(raw) = normalize_optional_text(Some(raw.as_str())) {
            let parsed =
                parse_config_path(raw).with_context(|| "PALYRA_CONFIG contains an invalid path")?;
            if parsed.exists() {
                return Ok(Some(parsed));
            }
        }
    }

    if let Some(profile_path) =
        profile.and_then(|profile| normalize_optional_text(profile.config_path.as_deref()))
    {
        let parsed = parse_config_path(profile_path)
            .with_context(|| format!("profile config_path is invalid: {profile_path}"))?;
        if !parsed.exists() {
            anyhow::bail!("profile config file does not exist: {}", parsed.display());
        }
        return Ok(Some(parsed));
    }

    Ok(default_config_search_paths().into_iter().find(|candidate| candidate.exists()))
}

fn load_config_defaults(config_path: Option<&Path>) -> Result<ConfigConnectionDefaults> {
    let Some(config_path) = config_path else {
        return Ok(ConfigConnectionDefaults::default());
    };
    if !config_path.exists() {
        return Ok(ConfigConnectionDefaults::default());
    }
    let (document, _) = load_document_from_existing_path(config_path)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;

    Ok(ConfigConnectionDefaults {
        daemon_url: Some(resolve_daemon_url_from_document(&document)?),
        grpc_url: Some(resolve_grpc_url_from_document(&document)?),
        admin_token: config_string(&document, "admin.auth_token")?
            .or(config_string(&document, "gateway.admin_token")?),
        principal: config_string(&document, "admin.bound_principal")?,
    })
}

fn resolve_daemon_url_from_document(document: &toml::Value) -> Result<String> {
    let bind = config_string(document, "daemon.bind_addr")?
        .unwrap_or_else(|| DEFAULT_DAEMON_BIND_ADDR.to_owned());
    let port = config_u16(document, "daemon.port")?.unwrap_or(DEFAULT_DAEMON_PORT);
    let socket = parse_daemon_bind_socket(bind.as_str(), port)
        .with_context(|| format!("invalid daemon bind config ({bind}:{port})"))?;
    let socket = normalize_client_socket(socket);
    Ok(format!("http://{socket}"))
}

fn resolve_grpc_url_from_document(document: &toml::Value) -> Result<String> {
    let bind = config_string(document, "gateway.grpc_bind_addr")?
        .unwrap_or_else(|| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned());
    let port = config_u16(document, "gateway.grpc_port")?.unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
    let socket = parse_daemon_bind_socket(bind.as_str(), port)
        .with_context(|| format!("invalid gateway gRPC bind config ({bind}:{port})"))?;
    let socket = normalize_client_socket(socket);
    Ok(format!("http://{socket}"))
}

fn config_string(document: &toml::Value, path: &str) -> Result<Option<String>> {
    Ok(get_value_at_path(document, path)?
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned))
}

fn config_u16(document: &toml::Value, path: &str) -> Result<Option<u16>> {
    Ok(get_value_at_path(document, path)?
        .and_then(|value| value.as_integer())
        .and_then(|value| u16::try_from(value).ok()))
}

fn normalize_optional_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalize_owned_text(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn current_unix_timestamp_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is set before UNIX_EPOCH")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported timestamp range")
}

impl ConnectionEnvironment {
    fn read(profile: Option<&CliConnectionProfile>) -> Self {
        Self {
            daemon_url: read_normalized_env_var("PALYRA_DAEMON_URL"),
            grpc_url: read_normalized_env_var("PALYRA_GATEWAY_GRPC_URL"),
            grpc_bind_addr: env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR")
                .unwrap_or_else(|_| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned()),
            grpc_port: env::var("PALYRA_GATEWAY_GRPC_PORT")
                .ok()
                .and_then(|raw| raw.parse::<u16>().ok())
                .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT),
            admin_token: read_normalized_env_var("PALYRA_ADMIN_TOKEN"),
            admin_bound_principal: read_normalized_env_var("PALYRA_ADMIN_BOUND_PRINCIPAL"),
            profile_admin_token: profile
                .and_then(|profile| normalize_optional_text(profile.admin_token_env.as_deref()))
                .and_then(read_normalized_env_var),
        }
    }
}

fn read_normalized_env_var(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| normalize_owned_text(Some(value)))
}

#[cfg(test)]
mod tests {
    use super::{
        build_active_profile_context, build_root_context, cli_profiles_registry_path, context_cell,
        CliConnectionProfile, ConnectionDefaults, ConnectionOverrides, ExplicitConfigPathPolicy,
        RootOptions, CLI_PROFILES_PATH_ENV, CLI_PROFILES_RELATIVE_PATH, CLI_PROFILE_ENV,
    };
    use crate::args::{LogLevelArg, OutputFormatArg};
    use anyhow::Result;
    use palyra_common::IdentityStorePathError;
    use std::{env, fs};
    use tempfile::tempdir;

    fn clear_env() {
        for key in [
            "PALYRA_STATE_ROOT",
            "PALYRA_CONFIG",
            "PALYRA_DAEMON_URL",
            "PALYRA_GATEWAY_GRPC_URL",
            "PALYRA_ADMIN_TOKEN",
            "PALYRA_ADMIN_BOUND_PRINCIPAL",
            "PALYRA_PROFILE_ADMIN_TOKEN",
            CLI_PROFILE_ENV,
            CLI_PROFILES_PATH_ENV,
        ] {
            env::remove_var(key);
        }
        if let Ok(mut guard) = context_cell().lock() {
            *guard = None;
        }
    }

    #[test]
    fn explicit_root_options_override_profile_and_env_values() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let profile_path = temp.path().join("profiles.toml");
        fs::write(
            &profile_path,
            r#"
version = 1
[profiles.ops]
daemon_url = "http://127.0.0.1:8100"
grpc_url = "http://127.0.0.1:8101"
admin_token = "profile-token"
principal = "user:profile"
device_id = "01ARZ3NDEKTSV4RRFFQ69G5FB0"
channel = "profile"
"#,
        )?;
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);
        env::set_var("PALYRA_DAEMON_URL", "http://127.0.0.1:9999");

        let context = build_root_context(
            RootOptions {
                profile: Some("ops".to_owned()),
                expect_profile: None,
                config_path: None,
                state_root: Some(state_root.display().to_string()),
                verbose: 1,
                log_level: LogLevelArg::Info,
                output_format: OutputFormatArg::Json,
                plain: false,
                no_color: true,
                allow_profile_mismatch: false,
                allow_strict_profile_actions: false,
            },
            ExplicitConfigPathPolicy::RequireExisting,
        )?;

        let grpc = context.resolve_grpc_connection(
            ConnectionOverrides {
                grpc_url: Some("http://127.0.0.1:7445".to_owned()),
                token: Some("explicit-token".to_owned()),
                principal: Some("user:explicit".to_owned()),
                device_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FB1".to_owned()),
                channel: Some("explicit".to_owned()),
                daemon_url: None,
            },
            ConnectionDefaults::USER,
        )?;

        assert_eq!(context.state_root(), state_root.as_path());
        assert_eq!(context.output_format(), OutputFormatArg::Json);
        assert_eq!(context.log_level(), LogLevelArg::Debug);
        assert!(context.no_color());
        assert_eq!(grpc.grpc_url, "http://127.0.0.1:7445");
        assert_eq!(grpc.token.as_deref(), Some("explicit-token"));
        assert_eq!(grpc.principal, "user:explicit");
        assert_eq!(grpc.device_id, "01ARZ3NDEKTSV4RRFFQ69G5FB1");
        assert_eq!(grpc.channel, "explicit");
        Ok(())
    }

    #[test]
    fn profile_values_fill_connection_defaults_when_flags_are_absent() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let profile_path = temp.path().join("profiles.toml");
        fs::write(
            &profile_path,
            r#"
version = 1
default_profile = "staging"
[profiles.staging]
daemon_url = "http://127.0.0.1:8200"
grpc_url = "http://127.0.0.1:8201"
admin_token = "profile-token"
principal = "admin:staging"
device_id = "01ARZ3NDEKTSV4RRFFQ69G5FB2"
channel = "staging"
"#,
        )?;
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);

        let context =
            build_root_context(RootOptions::default(), ExplicitConfigPathPolicy::RequireExisting)?;
        let http = context
            .resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;
        let grpc = context
            .resolve_grpc_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;

        assert_eq!(context.profile_name(), Some("staging"));
        assert_eq!(http.base_url, "http://127.0.0.1:8200");
        assert_eq!(grpc.grpc_url, "http://127.0.0.1:8201");
        assert_eq!(http.token.as_deref(), Some("profile-token"));
        assert_eq!(http.principal, "admin:staging");
        assert_eq!(grpc.device_id, "01ARZ3NDEKTSV4RRFFQ69G5FB2");
        assert_eq!(grpc.channel, "staging");
        Ok(())
    }

    #[test]
    fn env_config_path_overrides_default_profile_config_path() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let profile_path = temp.path().join("profiles.toml");
        let profile_config = temp.path().join("profile").join("palyra.toml");
        let env_config = temp.path().join("installed").join("palyra.toml");
        let profile_config_literal = profile_config.display().to_string().replace('\\', "\\\\");
        fs::create_dir_all(profile_config.parent().expect("profile config parent"))?;
        fs::create_dir_all(env_config.parent().expect("env config parent"))?;
        fs::write(&profile_path, format!(
            r#"
version = 1
default_profile = "installed"
[profiles.installed]
config_path = "{}"
"#,
            profile_config_literal
        ))?;
        fs::write(
            &profile_config,
            r#"
[daemon]
bind_addr = "127.0.0.1"
port = 8110
"#,
        )?;
        fs::write(
            &env_config,
            r#"
[daemon]
bind_addr = "127.0.0.1"
port = 9222
"#,
        )?;
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);
        env::set_var("PALYRA_CONFIG", &env_config);

        let context =
            build_root_context(RootOptions::default(), ExplicitConfigPathPolicy::RequireExisting)?;

        assert_eq!(context.config_path(), Some(env_config.as_path()));
        let http = context
            .resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;
        assert_eq!(http.base_url, "http://127.0.0.1:9222");
        Ok(())
    }

    #[test]
    fn env_state_root_overrides_default_profile_state_root() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let profile_path = temp.path().join("profiles.toml");
        let profile_state_root = temp.path().join("profile-state");
        let env_state_root = temp.path().join("installed-state");
        let profile_state_root_literal =
            profile_state_root.display().to_string().replace('\\', "\\\\");
        fs::create_dir_all(&profile_state_root)?;
        fs::create_dir_all(&env_state_root)?;
        fs::write(&profile_path, format!(
            r#"
version = 1
default_profile = "installed"
[profiles.installed]
state_root = "{}"
"#,
            profile_state_root_literal
        ))?;
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);
        env::set_var("PALYRA_STATE_ROOT", &env_state_root);

        let context =
            build_root_context(RootOptions::default(), ExplicitConfigPathPolicy::RequireExisting)?;

        assert_eq!(context.state_root(), env_state_root.as_path());
        Ok(())
    }

    #[test]
    fn profile_admin_token_env_overrides_global_env_fallback() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let profile_path = temp.path().join("profiles.toml");
        fs::write(
            &profile_path,
            r#"
version = 1
[profiles.ops]
admin_token_env = "PALYRA_PROFILE_ADMIN_TOKEN"
"#,
        )?;
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);
        env::set_var(CLI_PROFILE_ENV, "ops");
        env::set_var("PALYRA_PROFILE_ADMIN_TOKEN", "profile-env-token");
        env::set_var("PALYRA_ADMIN_TOKEN", "global-env-token");

        let context =
            build_root_context(RootOptions::default(), ExplicitConfigPathPolicy::RequireExisting)?;
        let http = context
            .resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;

        assert_eq!(http.token.as_deref(), Some("profile-env-token"));
        Ok(())
    }

    #[test]
    fn config_bound_principal_overrides_user_defaults_when_flags_are_absent() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        fs::create_dir_all(&state_root)?;
        let config_path = temp.path().join("palyra.toml");
        fs::write(
            &config_path,
            r#"
[daemon]
bind_addr = "127.0.0.1"
port = 7142

[gateway]
grpc_bind_addr = "127.0.0.1"
grpc_port = 50051

[admin]
auth_token = "config-token"
bound_principal = "admin:local"
"#,
        )?;

        let context = build_root_context(
            RootOptions {
                profile: None,
                expect_profile: None,
                config_path: Some(config_path.display().to_string()),
                state_root: Some(state_root.display().to_string()),
                verbose: 0,
                log_level: LogLevelArg::Info,
                output_format: OutputFormatArg::Text,
                plain: false,
                no_color: true,
                allow_profile_mismatch: false,
                allow_strict_profile_actions: false,
            },
            ExplicitConfigPathPolicy::RequireExisting,
        )?;

        let grpc = context
            .resolve_grpc_connection(ConnectionOverrides::default(), ConnectionDefaults::USER)?;
        let http = context
            .resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::USER)?;

        assert_eq!(grpc.token.as_deref(), Some("config-token"));
        assert_eq!(grpc.principal, "admin:local");
        assert_eq!(http.token.as_deref(), Some("config-token"));
        assert_eq!(http.principal, "admin:local");
        Ok(())
    }

    #[test]
    fn expected_profile_fails_closed_on_mismatch() {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir().expect("tempdir");
        let profile_path = temp.path().join("profiles.toml");
        fs::write(
            &profile_path,
            r#"
version = 1
default_profile = "staging"
[profiles.staging]
daemon_url = "http://127.0.0.1:8200"
"#,
        )
        .expect("profile registry should be written");
        env::set_var(CLI_PROFILES_PATH_ENV, &profile_path);

        let error = build_root_context(
            RootOptions { expect_profile: Some("prod".to_owned()), ..RootOptions::default() },
            ExplicitConfigPathPolicy::RequireExisting,
        )
        .expect_err("profile mismatch should fail closed");

        assert!(
            error.to_string().contains("active CLI profile mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn cli_profiles_registry_path_prefers_installed_root_context_state_root() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let state_root = temp.path().join("portable-state");
        let context = build_root_context(
            RootOptions {
                state_root: Some(state_root.display().to_string()),
                ..RootOptions::default()
            },
            ExplicitConfigPathPolicy::RequireExisting,
        )?;
        let mut guard = context_cell().lock().expect("context lock");
        *guard = Some(context);
        drop(guard);

        let registry_path = cli_profiles_registry_path()?;
        assert_eq!(registry_path, state_root.join(CLI_PROFILES_RELATIVE_PATH));
        Ok(())
    }

    #[test]
    fn bootstrap_policy_accepts_missing_explicit_config_path() -> Result<()> {
        let _guard = super::test_env_lock_for_tests().lock().expect("env lock");
        clear_env();
        let temp = tempdir()?;
        let missing_config_path = temp.path().join("config").join("palyra.toml");

        let context = build_root_context(
            RootOptions {
                config_path: Some(missing_config_path.display().to_string()),
                ..RootOptions::default()
            },
            ExplicitConfigPathPolicy::AllowMissingForBootstrap,
        )?;

        assert_eq!(context.config_path(), Some(missing_config_path.as_path()));
        Ok(())
    }

    #[test]
    fn active_profile_context_derives_visible_posture_defaults() {
        let profile = CliConnectionProfile {
            label: Some("Production".to_owned()),
            mode: Some("remote".to_owned()),
            strict_mode: true,
            ..CliConnectionProfile::default()
        };

        let context = build_active_profile_context(Some("prod"), Some(&profile))
            .expect("profile context should resolve");

        assert_eq!(context.name, "prod");
        assert_eq!(context.label, "Production");
        assert_eq!(context.environment, "production");
        assert_eq!(context.risk_level, "high");
        assert_eq!(context.color, "red");
        assert!(context.strict_mode);
    }

    #[cfg(windows)]
    #[test]
    fn state_root_requires_explicit_override_when_windows_appdata_is_missing() {
        let error = super::resolve_cli_state_root_with(None, None, || {
            Err(IdentityStorePathError::AppDataNotSet)
        })
        .expect_err("missing appdata should require an explicit state root");
        assert!(error.to_string().contains("PALYRA_STATE_ROOT"), "unexpected error: {error}");
    }

    #[cfg(not(windows))]
    #[test]
    fn state_root_requires_explicit_override_when_home_is_missing() {
        let error = super::resolve_cli_state_root_with(None, None, || {
            Err(IdentityStorePathError::HomeNotSet)
        })
        .expect_err("missing HOME should require an explicit state root");
        assert!(error.to_string().contains("PALYRA_STATE_ROOT"), "unexpected error: {error}");
    }
}
