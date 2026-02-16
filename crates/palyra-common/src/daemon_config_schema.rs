use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootFileConfig {
    pub daemon: Option<FileDaemonConfig>,
    pub gateway: Option<FileGatewayConfig>,
    pub admin: Option<FileAdminConfig>,
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
pub struct FileGatewayConfig {
    pub grpc_bind_addr: Option<String>,
    pub grpc_port: Option<u16>,
    pub quic_bind_addr: Option<String>,
    pub quic_port: Option<u16>,
    pub quic_enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAdminConfig {
    pub require_auth: Option<bool>,
    pub auth_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileIdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: Option<bool>,
}
