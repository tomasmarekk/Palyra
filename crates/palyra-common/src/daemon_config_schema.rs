use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootFileConfig {
    pub daemon: Option<FileDaemonConfig>,
    pub identity: Option<FileIdentityConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDaemonConfig {
    pub bind_addr: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileIdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: Option<bool>,
}
