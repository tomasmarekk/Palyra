use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use palyra_common::{daemon_config_schema::RootFileConfig, parse_config_path};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7142;
const DEFAULT_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS: bool = false;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    pub source: String,
    pub daemon: DaemonConfig,
    pub identity: IdentityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub bind_addr: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: bool,
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

pub fn load_config() -> Result<LoadedConfig> {
    let mut daemon = DaemonConfig::default();
    let mut identity = IdentityConfig::default();
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
        if let Some(file_identity) = parsed.identity {
            if let Some(allow_insecure) = file_identity.allow_insecure_node_rpc_without_mtls {
                identity.allow_insecure_node_rpc_without_mtls = allow_insecure;
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

    if let Ok(allow_insecure) = env::var("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS") {
        identity.allow_insecure_node_rpc_without_mtls = allow_insecure
            .parse::<bool>()
            .context("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS must be true or false")?;
        source.push_str(" +env(PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS)");
    }

    Ok(LoadedConfig { source, daemon, identity })
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

    let search_paths = [
        PathBuf::from("palyra.toml"),
        PathBuf::from("Palyra.toml"),
        PathBuf::from("config/palyra.toml"),
    ];
    for candidate in search_paths {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::IdentityConfig;
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
}
