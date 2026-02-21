use serde::Deserialize;
use toml::Value;

const REDACTED_CONFIG_VALUE: &str = "<redacted>";

pub const SECRET_CONFIG_PATHS: &[&str] = &[
    "admin.auth_token",
    "model_provider.openai_api_key",
    "model_provider.openai_api_key_vault_ref",
    "gateway.admin_token",
];

#[must_use]
pub fn is_secret_config_path(path: &str) -> bool {
    let normalized = normalize_config_path(path);
    SECRET_CONFIG_PATHS.iter().any(|candidate| *candidate == normalized)
}

pub fn redact_secret_config_values(document: &mut Value) {
    for secret_path in SECRET_CONFIG_PATHS {
        redact_config_path(document, secret_path);
    }
}

fn redact_config_path(document: &mut Value, path: &str) {
    let mut segments = path.split('.').peekable();
    let mut cursor = document;
    while let Some(segment) = segments.next() {
        let Some(table) = cursor.as_table_mut() else {
            return;
        };
        if segments.peek().is_none() {
            if table.contains_key(segment) {
                table.insert(segment.to_owned(), Value::String(REDACTED_CONFIG_VALUE.to_owned()));
            }
            return;
        }
        let Some(next) = table.get_mut(segment) else {
            return;
        };
        cursor = next;
    }
}

fn normalize_config_path(path: &str) -> String {
    path.split('.')
        .filter_map(|segment| {
            let trimmed = segment.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_ascii_lowercase())
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootFileConfig {
    pub version: Option<u32>,
    pub daemon: Option<FileDaemonConfig>,
    pub gateway: Option<FileGatewayConfig>,
    pub cron: Option<FileCronConfig>,
    pub orchestrator: Option<FileOrchestratorConfig>,
    pub memory: Option<FileMemoryConfig>,
    pub model_provider: Option<FileModelProviderConfig>,
    pub tool_call: Option<FileToolCallConfig>,
    pub admin: Option<FileAdminConfig>,
    pub identity: Option<FileIdentityConfig>,
    pub storage: Option<FileStorageConfig>,
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
    pub allow_insecure_remote: Option<bool>,
    pub identity_store_dir: Option<String>,
    pub max_tape_entries_per_response: Option<u64>,
    pub max_tape_bytes_per_response: Option<u64>,
    pub tls: Option<FileGatewayTlsConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileGatewayTlsConfig {
    pub enabled: Option<bool>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub client_ca_path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCronConfig {
    pub timezone: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileOrchestratorConfig {
    pub runloop_v1_enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryConfig {
    pub max_item_bytes: Option<u64>,
    pub max_item_tokens: Option<u64>,
    pub default_ttl_ms: Option<i64>,
    pub auto_inject: Option<FileMemoryAutoInjectConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryAutoInjectConfig {
    pub enabled: Option<bool>,
    pub max_items: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileModelProviderConfig {
    pub kind: Option<String>,
    pub openai_base_url: Option<String>,
    pub allow_private_base_url: Option<bool>,
    pub openai_model: Option<String>,
    pub openai_api_key: Option<String>,
    pub openai_api_key_vault_ref: Option<String>,
    pub request_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub circuit_breaker_failure_threshold: Option<u32>,
    pub circuit_breaker_cooldown_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileToolCallConfig {
    pub allowed_tools: Option<Vec<String>>,
    pub max_calls_per_run: Option<u32>,
    pub execution_timeout_ms: Option<u64>,
    pub process_runner: Option<FileProcessRunnerConfig>,
    pub wasm_runtime: Option<FileWasmRuntimeConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProcessRunnerConfig {
    pub enabled: Option<bool>,
    pub workspace_root: Option<String>,
    pub allowed_executables: Option<Vec<String>>,
    pub allow_interpreters: Option<bool>,
    pub egress_enforcement_mode: Option<String>,
    pub allowed_egress_hosts: Option<Vec<String>>,
    pub allowed_dns_suffixes: Option<Vec<String>>,
    pub cpu_time_limit_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub max_output_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileWasmRuntimeConfig {
    pub enabled: Option<bool>,
    pub allow_inline_modules: Option<bool>,
    pub max_module_size_bytes: Option<u64>,
    pub fuel_budget: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_table_elements: Option<u64>,
    pub max_instances: Option<u64>,
    pub allowed_http_hosts: Option<Vec<String>>,
    pub allowed_secrets: Option<Vec<String>>,
    pub allowed_storage_prefixes: Option<Vec<String>>,
    pub allowed_channels: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAdminConfig {
    pub require_auth: Option<bool>,
    pub auth_token: Option<String>,
    pub bound_principal: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileIdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileStorageConfig {
    pub journal_db_path: Option<String>,
    pub journal_hash_chain_enabled: Option<bool>,
    pub max_journal_payload_bytes: Option<u64>,
    pub vault_dir: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{is_secret_config_path, redact_secret_config_values};

    #[test]
    fn secret_config_path_matching_is_case_insensitive() {
        assert!(is_secret_config_path("model_provider.openai_api_key"));
        assert!(is_secret_config_path("model_provider.OPENAI_API_KEY"));
        assert!(is_secret_config_path("model_provider.openai_api_key_vault_ref"));
        assert!(is_secret_config_path("gateway.admin_token"));
        assert!(is_secret_config_path(" admin.auth_token "));
        assert!(!is_secret_config_path("daemon.port"));
    }

    #[test]
    fn redaction_replaces_known_secret_fields() {
        let mut document: toml::Value = toml::from_str(
            r#"
            version = 1
            [admin]
            auth_token = "token-value"
            [model_provider]
            openai_api_key = "sk-secret"
            openai_api_key_vault_ref = "vault://global/openai_api_key"
            [gateway]
            admin_token = "legacy-token"
            "#,
        )
        .expect("config document should parse");

        redact_secret_config_values(&mut document);

        assert_eq!(
            document
                .get("admin")
                .and_then(|admin| admin.get("auth_token"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("model_provider")
                .and_then(|provider| provider.get("openai_api_key"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("model_provider")
                .and_then(|provider| provider.get("openai_api_key_vault_ref"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("gateway")
                .and_then(|gateway| gateway.get("admin_token"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
    }
}
