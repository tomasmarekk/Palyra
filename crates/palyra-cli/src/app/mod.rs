use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use anyhow::{Context, Result};
use palyra_common::{
    config_system::get_value_at_path, default_config_search_paths, default_state_root,
    parse_config_path, parse_daemon_bind_socket,
};
use serde::Deserialize;
use ulid::Ulid;

use crate::{
    args::{LogLevelArg, OutputFormatArg, RootOptions},
    load_document_from_existing_path, normalize_client_socket, AgentConnection,
    DEFAULT_CHANNEL, DEFAULT_DAEMON_BIND_ADDR, DEFAULT_DAEMON_PORT, DEFAULT_DAEMON_URL,
    DEFAULT_DEVICE_ID, DEFAULT_GATEWAY_GRPC_BIND_ADDR, DEFAULT_GATEWAY_GRPC_PORT,
};

const CLI_PROFILE_ENV: &str = "PALYRA_CLI_PROFILE";
const CLI_PROFILES_PATH_ENV: &str = "PALYRA_CLI_PROFILES_PATH";
const CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const CLI_PROFILE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub(crate) struct RootCommandContext {
    state_root: PathBuf,
    profile_name: Option<String>,
    output_format: OutputFormatArg,
    log_level: LogLevelArg,
    no_color: bool,
    trace_id: String,
    profile: Option<CliConnectionProfile>,
    config_defaults: ConfigConnectionDefaults,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionDefaults {
    pub principal: &'static str,
    pub device_id: &'static str,
    pub channel: &'static str,
}

impl ConnectionDefaults {
    pub(crate) const USER: Self = Self {
        principal: "user:local",
        device_id: DEFAULT_DEVICE_ID,
        channel: DEFAULT_CHANNEL,
    };

    pub(crate) const ADMIN: Self = Self {
        principal: "admin:local",
        device_id: DEFAULT_DEVICE_ID,
        channel: DEFAULT_CHANNEL,
    };
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliProfilesDocument {
    version: Option<u32>,
    default_profile: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, CliConnectionProfile>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliConnectionProfile {
    config_path: Option<String>,
    state_root: Option<String>,
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token: Option<String>,
    admin_token_env: Option<String>,
    principal: Option<String>,
    device_id: Option<String>,
    channel: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ConfigConnectionDefaults {
    daemon_url: Option<String>,
    grpc_url: Option<String>,
    admin_token: Option<String>,
    principal: Option<String>,
}

static ROOT_CONTEXT: OnceLock<Mutex<Option<RootCommandContext>>> = OnceLock::new();

fn context_cell() -> &'static Mutex<Option<RootCommandContext>> {
    ROOT_CONTEXT.get_or_init(|| Mutex::new(None))
}

pub(crate) fn install_root_context(root: RootOptions) -> Result<RootCommandContext> {
    let context = build_root_context(root)?;
    let mut guard = context_cell()
        .lock()
        .map_err(|_| anyhow::anyhow!("CLI root context lock poisoned"))?;
    *guard = Some(context.clone());
    Ok(context)
}

pub(crate) fn current_root_context() -> Option<RootCommandContext> {
    context_cell()
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().cloned())
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

    pub(crate) fn state_root(&self) -> &Path {
        self.state_root.as_path()
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
        if let Ok(url) = env::var("PALYRA_DAEMON_URL") {
            if let Some(url) = normalize_optional_text(Some(url.as_str())) {
                return Ok(url.to_owned());
            }
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
        if let Ok(url) = env::var("PALYRA_GATEWAY_GRPC_URL") {
            if let Some(url) = normalize_optional_text(Some(url.as_str())) {
                return Ok(url.to_owned());
            }
        }
        let bind = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_GATEWAY_GRPC_BIND_ADDR.to_owned());
        let port = env::var("PALYRA_GATEWAY_GRPC_PORT")
            .ok()
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT);
        let socket = parse_daemon_bind_socket(bind.as_str(), port)
            .context("invalid gateway gRPC bind config")?;
        let socket = normalize_client_socket(socket);
        Ok(format!("http://{socket}"))
    }

    fn resolve_token(&self, override_token: Option<String>) -> Option<String> {
        normalize_owned_text(override_token)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.admin_token.clone()))
            })
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_optional_text(profile.admin_token_env.as_deref()))
                    .and_then(|env_name| env::var(env_name).ok())
                    .and_then(|value| normalize_owned_text(Some(value)))
            })
            .or_else(|| normalize_owned_text(self.config_defaults.admin_token.clone()))
            .or_else(|| {
                env::var("PALYRA_ADMIN_TOKEN")
                    .ok()
                    .and_then(|value| normalize_owned_text(Some(value)))
            })
    }

    fn resolve_principal(
        &self,
        override_principal: Option<String>,
        defaults: ConnectionDefaults,
    ) -> String {
        normalize_owned_text(override_principal)
            .or_else(|| {
                self.profile
                    .as_ref()
                    .and_then(|profile| normalize_owned_text(profile.principal.clone()))
            })
            .or_else(|| normalize_owned_text(self.config_defaults.principal.clone()))
            .or_else(|| {
                env::var("PALYRA_ADMIN_BOUND_PRINCIPAL")
                    .ok()
                    .and_then(|value| normalize_owned_text(Some(value)))
            })
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

fn build_root_context(root: RootOptions) -> Result<RootCommandContext> {
    let bootstrap_state_root = resolve_explicit_or_env_state_root(root.state_root.as_deref())?;
    let profiles_path = resolve_profiles_path(&bootstrap_state_root)?;
    let profiles = load_profiles_document(profiles_path.as_deref())?;
    let profile_name = resolve_active_profile_name(&root, &profiles);
    let profile = resolve_profile(profile_name.as_deref(), &profiles)?;
    let state_root = resolve_final_state_root(&root, profile.as_ref())?;
    let config_path = resolve_config_path(&root, profile.as_ref())?;
    let config_defaults = load_config_defaults(config_path.as_deref())?;

    Ok(RootCommandContext {
        output_format: resolve_output_format(&root),
        log_level: resolve_log_level(&root),
        no_color: root.no_color,
        trace_id: format!("cli:{}", Ulid::new()),
        state_root,
        profile_name,
        profile,
        config_defaults,
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
        if parsed.exists() {
            return Ok(Some(parsed));
        }
        anyhow::bail!("CLI profiles file does not exist: {}", parsed.display());
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

fn resolve_active_profile_name(root: &RootOptions, profiles: &CliProfilesDocument) -> Option<String> {
    normalize_owned_text(root.profile.clone())
        .or_else(|| {
            env::var(CLI_PROFILE_ENV)
                .ok()
                .and_then(|value| normalize_owned_text(Some(value)))
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

fn resolve_explicit_or_env_state_root(explicit: Option<&str>) -> Result<PathBuf> {
    if let Some(explicit) = normalize_optional_text(explicit) {
        return parse_config_path(explicit)
            .with_context(|| format!("state root path is invalid: {explicit}"));
    }
    if let Ok(raw) = env::var("PALYRA_STATE_ROOT") {
        if let Some(raw) = normalize_optional_text(Some(raw.as_str())) {
            return parse_config_path(raw).with_context(|| "PALYRA_STATE_ROOT contains an invalid path");
        }
    }
    default_state_root().context("failed to resolve default state root")
}

fn resolve_final_state_root(
    root: &RootOptions,
    profile: Option<&CliConnectionProfile>,
) -> Result<PathBuf> {
    if let Some(explicit) = normalize_optional_text(root.state_root.as_deref()) {
        return parse_config_path(explicit)
            .with_context(|| format!("state root path is invalid: {explicit}"));
    }
    if let Some(profile_state_root) = profile
        .and_then(|profile| normalize_optional_text(profile.state_root.as_deref()))
    {
        return parse_config_path(profile_state_root).with_context(|| {
            format!("profile state_root is invalid: {profile_state_root}")
        });
    }
    resolve_explicit_or_env_state_root(None)
}

fn resolve_config_path(
    root: &RootOptions,
    profile: Option<&CliConnectionProfile>,
) -> Result<Option<PathBuf>> {
    if let Some(explicit) = normalize_optional_text(root.config_path.as_deref()) {
        let parsed = parse_config_path(explicit)
            .with_context(|| format!("config path is invalid: {explicit}"))?;
        if !parsed.exists() {
            anyhow::bail!("config file does not exist: {}", parsed.display());
        }
        return Ok(Some(parsed));
    }

    if let Some(profile_path) = profile.and_then(|profile| normalize_optional_text(profile.config_path.as_deref())) {
        let parsed = parse_config_path(profile_path)
            .with_context(|| format!("profile config_path is invalid: {profile_path}"))?;
        if !parsed.exists() {
            anyhow::bail!("profile config file does not exist: {}", parsed.display());
        }
        return Ok(Some(parsed));
    }

    if let Ok(raw) = env::var("PALYRA_CONFIG") {
        if let Some(raw) = normalize_optional_text(Some(raw.as_str())) {
            let parsed = parse_config_path(raw)
                .with_context(|| "PALYRA_CONFIG contains an invalid path")?;
            if parsed.exists() {
                return Ok(Some(parsed));
            }
        }
    }

    Ok(default_config_search_paths().into_iter().find(|candidate| candidate.exists()))
}

fn load_config_defaults(config_path: Option<&Path>) -> Result<ConfigConnectionDefaults> {
    let Some(config_path) = config_path else {
        return Ok(ConfigConnectionDefaults::default());
    };
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
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{
        build_root_context, ConnectionDefaults, ConnectionOverrides, RootOptions, CLI_PROFILE_ENV,
        CLI_PROFILES_PATH_ENV,
    };
    use crate::args::{LogLevelArg, OutputFormatArg};
    use anyhow::Result;
    use std::{env, fs, sync::Mutex, sync::OnceLock};
    use tempfile::tempdir;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_env() {
        for key in [
            "PALYRA_STATE_ROOT",
            "PALYRA_CONFIG",
            "PALYRA_DAEMON_URL",
            "PALYRA_GATEWAY_GRPC_URL",
            "PALYRA_ADMIN_TOKEN",
            "PALYRA_ADMIN_BOUND_PRINCIPAL",
            CLI_PROFILE_ENV,
            CLI_PROFILES_PATH_ENV,
        ] {
            env::remove_var(key);
        }
    }

    #[test]
    fn explicit_root_options_override_profile_and_env_values() -> Result<()> {
        let _guard = env_lock().lock().expect("env lock");
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

        let context = build_root_context(RootOptions {
            profile: Some("ops".to_owned()),
            config_path: None,
            state_root: Some(state_root.display().to_string()),
            verbose: 1,
            log_level: LogLevelArg::Info,
            output_format: OutputFormatArg::Json,
            plain: false,
            no_color: true,
        })?;

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
        let _guard = env_lock().lock().expect("env lock");
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

        let context = build_root_context(RootOptions::default())?;
        let http = context.resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;
        let grpc = context.resolve_grpc_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;

        assert_eq!(context.profile_name(), Some("staging"));
        assert_eq!(http.base_url, "http://127.0.0.1:8200");
        assert_eq!(grpc.grpc_url, "http://127.0.0.1:8201");
        assert_eq!(http.token.as_deref(), Some("profile-token"));
        assert_eq!(http.principal, "admin:staging");
        assert_eq!(grpc.device_id, "01ARZ3NDEKTSV4RRFFQ69G5FB2");
        assert_eq!(grpc.channel, "staging");
        Ok(())
    }
}
