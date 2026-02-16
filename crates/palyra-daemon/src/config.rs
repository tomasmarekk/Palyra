use std::{
    env, fs,
    path::{Component, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    daemon_config_schema::RootFileConfig, default_config_search_paths, parse_config_path,
};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7142;
const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_GRPC_PORT: u16 = 7443;
const DEFAULT_QUIC_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_QUIC_PORT: u16 = 7444;
const DEFAULT_QUIC_ENABLED: bool = true;
const DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED: bool = false;
const DEFAULT_ADMIN_REQUIRE_AUTH: bool = true;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;
const DEFAULT_JOURNAL_DB_PATH: &str = "data/journal.sqlite3";
const DEFAULT_JOURNAL_HASH_CHAIN_ENABLED: bool = false;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub source: String,
    pub daemon: DaemonConfig,
    pub gateway: GatewayConfig,
    pub orchestrator: OrchestratorConfig,
    pub admin: AdminConfig,
    pub identity: IdentityConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub bind_addr: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub runloop_v1_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminConfig {
    pub require_auth: bool,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub journal_db_path: PathBuf,
    pub journal_hash_chain_enabled: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self { bind_addr: DEFAULT_BIND_ADDR.to_owned(), port: DEFAULT_PORT }
    }
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self { allow_insecure_node_rpc_without_mtls: DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            journal_db_path: PathBuf::from(DEFAULT_JOURNAL_DB_PATH),
            journal_hash_chain_enabled: DEFAULT_JOURNAL_HASH_CHAIN_ENABLED,
        }
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            grpc_bind_addr: DEFAULT_GRPC_BIND_ADDR.to_owned(),
            grpc_port: DEFAULT_GRPC_PORT,
            quic_bind_addr: DEFAULT_QUIC_BIND_ADDR.to_owned(),
            quic_port: DEFAULT_QUIC_PORT,
            quic_enabled: DEFAULT_QUIC_ENABLED,
        }
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self { runloop_v1_enabled: DEFAULT_ORCHESTRATOR_RUNLOOP_V1_ENABLED }
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self { require_auth: DEFAULT_ADMIN_REQUIRE_AUTH, auth_token: None }
    }
}

pub fn load_config() -> Result<LoadedConfig> {
    let mut daemon = DaemonConfig::default();
    let mut gateway = GatewayConfig::default();
    let mut orchestrator = OrchestratorConfig::default();
    let mut admin = AdminConfig::default();
    let mut identity = IdentityConfig::default();
    let mut storage = StorageConfig::default();
    let mut source = "defaults".to_owned();

    if let Some(path) = find_config_path()? {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let parsed: RootFileConfig = toml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        if let Some(file_daemon) = parsed.daemon {
            if let Some(bind_addr) = file_daemon.bind_addr {
                daemon.bind_addr = bind_addr;
            }
            if let Some(port) = file_daemon.port {
                daemon.port = port;
            }
        }
        if let Some(file_gateway) = parsed.gateway {
            if let Some(grpc_bind_addr) = file_gateway.grpc_bind_addr {
                gateway.grpc_bind_addr = grpc_bind_addr;
            }
            if let Some(grpc_port) = file_gateway.grpc_port {
                gateway.grpc_port = grpc_port;
            }
            if let Some(quic_bind_addr) = file_gateway.quic_bind_addr {
                gateway.quic_bind_addr = quic_bind_addr;
            }
            if let Some(quic_port) = file_gateway.quic_port {
                gateway.quic_port = quic_port;
            }
            if let Some(quic_enabled) = file_gateway.quic_enabled {
                gateway.quic_enabled = quic_enabled;
            }
        }
        if let Some(file_orchestrator) = parsed.orchestrator {
            if let Some(runloop_v1_enabled) = file_orchestrator.runloop_v1_enabled {
                orchestrator.runloop_v1_enabled = runloop_v1_enabled;
            }
        }
        if let Some(file_admin) = parsed.admin {
            if let Some(require_auth) = file_admin.require_auth {
                admin.require_auth = require_auth;
            }
            if let Some(auth_token) = file_admin.auth_token {
                admin.auth_token =
                    if auth_token.trim().is_empty() { None } else { Some(auth_token) };
            }
        }
        if let Some(file_identity) = parsed.identity {
            if let Some(allow_insecure) = file_identity.allow_insecure_node_rpc_without_mtls {
                identity.allow_insecure_node_rpc_without_mtls = allow_insecure;
            }
        }
        if let Some(file_storage) = parsed.storage {
            if let Some(path) = file_storage.journal_db_path {
                storage.journal_db_path = parse_journal_db_path(&path)?;
            }
            if let Some(hash_chain_enabled) = file_storage.journal_hash_chain_enabled {
                storage.journal_hash_chain_enabled = hash_chain_enabled;
            }
        }
        source = path.to_string_lossy().into_owned();
    }

    if let Ok(bind_addr) = env::var("PALYRA_DAEMON_BIND_ADDR") {
        daemon.bind_addr = bind_addr;
        source.push_str(" +env(PALYRA_DAEMON_BIND_ADDR)");
    }

    if let Ok(port) = env::var("PALYRA_DAEMON_PORT") {
        daemon.port = port.parse::<u16>().context("PALYRA_DAEMON_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_DAEMON_PORT)");
    }

    if let Ok(grpc_bind_addr) = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR") {
        gateway.grpc_bind_addr = grpc_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_BIND_ADDR)");
    }

    if let Ok(grpc_port) = env::var("PALYRA_GATEWAY_GRPC_PORT") {
        gateway.grpc_port =
            grpc_port.parse::<u16>().context("PALYRA_GATEWAY_GRPC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_PORT)");
    }

    if let Ok(quic_bind_addr) = env::var("PALYRA_GATEWAY_QUIC_BIND_ADDR") {
        gateway.quic_bind_addr = quic_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_BIND_ADDR)");
    }

    if let Ok(quic_port) = env::var("PALYRA_GATEWAY_QUIC_PORT") {
        gateway.quic_port =
            quic_port.parse::<u16>().context("PALYRA_GATEWAY_QUIC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_PORT)");
    }

    if let Ok(quic_enabled) = env::var("PALYRA_GATEWAY_QUIC_ENABLED") {
        gateway.quic_enabled = quic_enabled
            .parse::<bool>()
            .context("PALYRA_GATEWAY_QUIC_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_ENABLED)");
    }

    if let Ok(runloop_v1_enabled) = env::var("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED") {
        orchestrator.runloop_v1_enabled = runloop_v1_enabled
            .parse::<bool>()
            .context("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED)");
    }

    if let Ok(require_auth) = env::var("PALYRA_ADMIN_REQUIRE_AUTH") {
        admin.require_auth = require_auth
            .parse::<bool>()
            .context("PALYRA_ADMIN_REQUIRE_AUTH must be true or false")?;
        source.push_str(" +env(PALYRA_ADMIN_REQUIRE_AUTH)");
    }

    if let Ok(admin_token) = env::var("PALYRA_ADMIN_TOKEN") {
        admin.auth_token = if admin_token.trim().is_empty() { None } else { Some(admin_token) };
        source.push_str(" +env(PALYRA_ADMIN_TOKEN)");
    }

    if let Ok(allow_insecure) = env::var("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS") {
        identity.allow_insecure_node_rpc_without_mtls = allow_insecure
            .parse::<bool>()
            .context("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS must be true or false")?;
        source.push_str(" +env(PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS)");
    }

    if let Ok(path) = env::var("PALYRA_JOURNAL_DB_PATH") {
        storage.journal_db_path = parse_journal_db_path(&path)?;
        source.push_str(" +env(PALYRA_JOURNAL_DB_PATH)");
    }

    if let Ok(hash_chain_enabled) = env::var("PALYRA_JOURNAL_HASH_CHAIN_ENABLED") {
        storage.journal_hash_chain_enabled = hash_chain_enabled
            .parse::<bool>()
            .context("PALYRA_JOURNAL_HASH_CHAIN_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_JOURNAL_HASH_CHAIN_ENABLED)");
    }

    Ok(LoadedConfig { source, daemon, gateway, orchestrator, admin, identity, storage })
}

fn find_config_path() -> Result<Option<PathBuf>> {
    if let Ok(path) = env::var("PALYRA_CONFIG") {
        let path =
            parse_config_path(&path).context("PALYRA_CONFIG contains an invalid config path")?;
        if !path.exists() {
            anyhow::bail!("PALYRA_CONFIG points to a missing file: {}", path.to_string_lossy());
        }
        return Ok(Some(path));
    }

    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn parse_journal_db_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("journal db path cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("journal db path cannot contain embedded NUL byte");
    }
    let path = PathBuf::from(raw);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        anyhow::bail!("journal db path cannot contain parent traversal ('..')");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        parse_journal_db_path, AdminConfig, GatewayConfig, IdentityConfig, OrchestratorConfig,
        StorageConfig,
    };
    use palyra_common::daemon_config_schema::RootFileConfig;

    #[test]
    fn identity_config_defaults_to_secure_mode() {
        let config = IdentityConfig::default();
        assert!(!config.allow_insecure_node_rpc_without_mtls);
    }

    #[test]
    fn identity_config_parses_file_override() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [identity]
            allow_insecure_node_rpc_without_mtls = true
            "#,
        )
        .expect("toml should parse");
        let identity = parsed.identity.expect("identity config should be present");
        assert_eq!(identity.allow_insecure_node_rpc_without_mtls, Some(true));
    }

    #[test]
    fn gateway_config_defaults_to_quic_and_grpc_loopback() {
        let config = GatewayConfig::default();
        assert_eq!(config.grpc_bind_addr, "127.0.0.1");
        assert_eq!(config.grpc_port, 7443);
        assert_eq!(config.quic_bind_addr, "127.0.0.1");
        assert_eq!(config.quic_port, 7444);
        assert!(config.quic_enabled, "gateway transport should default to QUIC-enabled mode");
    }

    #[test]
    fn orchestrator_config_defaults_to_disabled_runloop() {
        let config = OrchestratorConfig::default();
        assert!(
            !config.runloop_v1_enabled,
            "orchestrator run loop should default disabled until explicitly enabled"
        );
    }

    #[test]
    fn admin_config_defaults_to_deny_when_token_missing() {
        let config = AdminConfig::default();
        assert!(config.require_auth, "admin auth should default to required");
        assert!(config.auth_token.is_none(), "admin token should default to missing");
    }

    #[test]
    fn storage_config_defaults_to_safe_journal_mode() {
        let config = StorageConfig::default();
        assert_eq!(config.journal_db_path, PathBuf::from("data/journal.sqlite3"));
        assert!(
            !config.journal_hash_chain_enabled,
            "hash chain must default to disabled until audit mode is explicitly enabled"
        );
    }

    #[test]
    fn config_rejects_unknown_top_level_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("unexpected=true\n[daemon]\nport=7142\n");
        assert!(result.is_err(), "unknown top-level keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_daemon_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[daemon]\nport=7142\nunexpected=true\n");
        assert!(result.is_err(), "unknown daemon keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_identity_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[identity]\nallow_insecure_node_rpc_without_mtls=true\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown identity keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_gateway_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[gateway]\ngrpc_port=7443\nunexpected=true\n");
        assert!(result.is_err(), "unknown gateway keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_orchestrator_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[orchestrator]\nrunloop_v1_enabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown orchestrator keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_admin_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[admin]\nrequire_auth=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown admin keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_storage_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[storage]\njournal_db_path='data/journal.sqlite3'\nunexpected=true\n");
        assert!(result.is_err(), "unknown storage keys must be rejected");
    }

    #[test]
    fn journal_db_path_rejects_parent_traversal() {
        let result = parse_journal_db_path("../secrets/journal.sqlite3");
        assert!(result.is_err(), "journal db path must reject parent traversal");
    }
}
