use serde::Deserialize;
use toml::Value;

use crate::secret_refs::SecretRef;

const REDACTED_CONFIG_VALUE: &str = "<redacted>";

pub const SECRET_CONFIG_PATHS: &[&str] = &[
    "admin.auth_token",
    "admin.auth_token_secret_ref",
    "admin.connector_token",
    "admin.connector_token_secret_ref",
    "model_provider.openai_api_key",
    "model_provider.openai_api_key_secret_ref",
    "model_provider.openai_api_key_vault_ref",
    "model_provider.anthropic_api_key",
    "model_provider.anthropic_api_key_secret_ref",
    "model_provider.anthropic_api_key_vault_ref",
    "gateway.admin_token",
    "tool_call.browser_service.auth_token",
    "tool_call.browser_service.auth_token_secret_ref",
    "tool_call.browser_service.state_key_secret_ref",
    "tool_call.browser_service.state_key_vault_ref",
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
    redact_provider_registry_secrets(document);
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

fn redact_provider_registry_secrets(document: &mut Value) {
    let Some(model_provider) = document.get_mut("model_provider") else {
        return;
    };
    let Some(model_provider_table) = model_provider.as_table_mut() else {
        return;
    };
    let Some(providers) = model_provider_table.get_mut("providers") else {
        return;
    };
    let Some(array) = providers.as_array_mut() else {
        return;
    };
    for entry in array {
        let Some(provider_table) = entry.as_table_mut() else {
            continue;
        };
        for secret_field in [
            "api_key",
            "api_key_secret_ref",
            "api_key_vault_ref",
            "openai_api_key",
            "openai_api_key_secret_ref",
            "openai_api_key_vault_ref",
            "anthropic_api_key",
            "anthropic_api_key_secret_ref",
            "anthropic_api_key_vault_ref",
        ] {
            if provider_table.contains_key(secret_field) {
                provider_table.insert(
                    secret_field.to_owned(),
                    Value::String(REDACTED_CONFIG_VALUE.to_owned()),
                );
            }
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootFileConfig {
    pub version: Option<u32>,
    pub deployment: Option<FileDeploymentConfig>,
    pub daemon: Option<FileDaemonConfig>,
    pub gateway: Option<FileGatewayConfig>,
    pub gateway_access: Option<FileGatewayAccessConfig>,
    pub feature_rollouts: Option<FileFeatureRolloutsConfig>,
    pub session_queue_policy: Option<FileSessionQueuePolicyConfig>,
    pub pruning_policy_matrix: Option<FilePruningPolicyMatrixConfig>,
    pub retrieval_dual_path: Option<FileRetrievalDualPathConfig>,
    pub auxiliary_executor: Option<FileAuxiliaryExecutorConfig>,
    pub flow_orchestration: Option<FileFlowOrchestrationConfig>,
    pub delivery_arbitration: Option<FileDeliveryArbitrationConfig>,
    pub replay_capture: Option<FileReplayCaptureConfig>,
    pub networked_workers: Option<FileNetworkedWorkersConfig>,
    pub cron: Option<FileCronConfig>,
    pub orchestrator: Option<FileOrchestratorConfig>,
    pub memory: Option<FileMemoryConfig>,
    pub media: Option<FileMediaConfig>,
    pub model_provider: Option<FileModelProviderConfig>,
    pub tool_call: Option<FileToolCallConfig>,
    pub channel_router: Option<FileChannelRouterConfig>,
    pub canvas_host: Option<FileCanvasHostConfig>,
    pub admin: Option<FileAdminConfig>,
    pub identity: Option<FileIdentityConfig>,
    pub storage: Option<FileStorageConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDeploymentConfig {
    pub mode: Option<String>,
    pub dangerous_remote_bind_ack: Option<bool>,
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
    pub bind_profile: Option<String>,
    pub allow_insecure_remote: Option<bool>,
    pub identity_store_dir: Option<String>,
    pub vault_get_approval_required_refs: Option<Vec<String>>,
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
pub struct FileGatewayAccessConfig {
    pub remote_base_url: Option<String>,
    pub pinned_server_cert_fingerprint_sha256: Option<String>,
    pub pinned_gateway_ca_fingerprint_sha256: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFeatureRolloutsConfig {
    pub dynamic_tool_builder: Option<bool>,
    pub context_engine: Option<bool>,
    pub execution_backend_remote_node: Option<bool>,
    pub execution_backend_networked_worker: Option<bool>,
    pub execution_backend_ssh_tunnel: Option<bool>,
    pub safety_boundary: Option<bool>,
    pub execution_gate_pipeline_v2: Option<bool>,
    pub session_queue_policy: Option<bool>,
    pub pruning_policy_matrix: Option<bool>,
    pub retrieval_dual_path: Option<bool>,
    pub auxiliary_executor: Option<bool>,
    pub flow_orchestration: Option<bool>,
    pub delivery_arbitration: Option<bool>,
    pub replay_capture: Option<bool>,
    pub networked_workers: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSessionQueuePolicyConfig {
    pub mode: Option<String>,
    pub max_depth: Option<u64>,
    pub merge_window_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePruningPolicyMatrixConfig {
    pub mode: Option<String>,
    pub manual_apply_enabled: Option<bool>,
    pub min_token_savings: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalDualPathConfig {
    pub mode: Option<String>,
    pub branch_timeout_ms: Option<u64>,
    pub prompt_budget_tokens: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAuxiliaryExecutorConfig {
    pub mode: Option<String>,
    pub max_tasks_per_session: Option<u64>,
    pub default_budget_tokens: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFlowOrchestrationConfig {
    pub mode: Option<String>,
    pub cancellation_gate_enabled: Option<bool>,
    pub max_retry_count: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDeliveryArbitrationConfig {
    pub mode: Option<String>,
    pub descendant_preference: Option<bool>,
    pub suppression_limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileReplayCaptureConfig {
    pub mode: Option<String>,
    pub capture_runtime_decisions: Option<bool>,
    pub max_events_per_run: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileNetworkedWorkersConfig {
    pub mode: Option<String>,
    pub lease_ttl_ms: Option<u64>,
    pub require_attestation: Option<bool>,
    pub expected_image_digest_sha256: Option<String>,
    pub expected_build_digest_sha256: Option<String>,
    pub expected_artifact_digest_sha256: Option<String>,
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
    pub retention: Option<FileMemoryRetentionConfig>,
    pub retrieval: Option<FileMemoryRetrievalConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryAutoInjectConfig {
    pub enabled: Option<bool>,
    pub max_items: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryRetentionConfig {
    pub max_entries: Option<u64>,
    pub max_bytes: Option<u64>,
    pub ttl_days: Option<u32>,
    pub vacuum_schedule: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryRetrievalConfig {
    pub backend: Option<FileRetrievalBackendConfig>,
    pub scoring: Option<FileRetrievalScoringConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalBackendConfig {
    pub kind: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalScoringConfig {
    pub phrase_match_bonus_bps: Option<u16>,
    pub default_profile: Option<FileRetrievalSourceScoringProfile>,
    pub memory: Option<FileRetrievalSourceScoringProfile>,
    pub workspace: Option<FileRetrievalSourceScoringProfile>,
    pub transcript: Option<FileRetrievalSourceScoringProfile>,
    pub checkpoint: Option<FileRetrievalSourceScoringProfile>,
    pub compaction: Option<FileRetrievalSourceScoringProfile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalSourceScoringProfile {
    pub lexical_bps: Option<u16>,
    pub vector_bps: Option<u16>,
    pub recency_bps: Option<u16>,
    pub source_quality_bps: Option<u16>,
    pub min_recency_bps: Option<u16>,
    pub min_source_quality_bps: Option<u16>,
    pub pinned_bonus_bps: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMediaConfig {
    pub download_enabled: Option<bool>,
    pub outbound_upload_enabled: Option<bool>,
    pub allow_http_fixture_urls: Option<bool>,
    pub max_attachments_per_message: Option<u64>,
    pub max_total_attachment_bytes_per_message: Option<u64>,
    pub max_download_bytes: Option<u64>,
    pub max_redirects: Option<u64>,
    pub allowed_source_hosts: Option<Vec<String>>,
    pub allowed_download_content_types: Option<Vec<String>>,
    pub vision_allowed_content_types: Option<Vec<String>>,
    pub vision_max_image_count: Option<u64>,
    pub vision_max_image_bytes: Option<u64>,
    pub vision_max_total_bytes: Option<u64>,
    pub vision_max_dimension_px: Option<u32>,
    pub outbound_allowed_content_types: Option<Vec<String>>,
    pub outbound_max_upload_bytes: Option<u64>,
    pub store_max_bytes: Option<u64>,
    pub store_max_artifacts: Option<u64>,
    pub retention_ttl_ms: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileModelProviderConfig {
    pub kind: Option<String>,
    pub openai_base_url: Option<String>,
    pub anthropic_base_url: Option<String>,
    pub allow_private_base_url: Option<bool>,
    pub openai_model: Option<String>,
    pub anthropic_model: Option<String>,
    pub openai_embeddings_model: Option<String>,
    pub openai_embeddings_dims: Option<u32>,
    pub openai_api_key: Option<String>,
    pub openai_api_key_secret_ref: Option<SecretRef>,
    pub openai_api_key_vault_ref: Option<String>,
    pub anthropic_api_key: Option<String>,
    pub anthropic_api_key_secret_ref: Option<SecretRef>,
    pub anthropic_api_key_vault_ref: Option<String>,
    pub auth_profile_id: Option<String>,
    pub auth_profile_ref: Option<String>,
    pub auth_provider_kind: Option<String>,
    pub request_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub circuit_breaker_failure_threshold: Option<u32>,
    pub circuit_breaker_cooldown_ms: Option<u64>,
    pub providers: Option<Vec<FileModelProviderRegistryEntry>>,
    pub models: Option<Vec<FileModelProviderRegistryModel>>,
    pub default_chat_model_id: Option<String>,
    pub default_embeddings_model_id: Option<String>,
    pub default_audio_transcription_model_id: Option<String>,
    pub failover_enabled: Option<bool>,
    pub response_cache_enabled: Option<bool>,
    pub response_cache_ttl_ms: Option<u64>,
    pub response_cache_max_entries: Option<u64>,
    pub discovery_ttl_ms: Option<u64>,
    pub health_ttl_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileModelProviderRegistryEntry {
    pub provider_id: Option<String>,
    pub display_name: Option<String>,
    pub kind: Option<String>,
    pub base_url: Option<String>,
    pub allow_private_base_url: Option<bool>,
    pub enabled: Option<bool>,
    pub auth_profile_id: Option<String>,
    pub auth_provider_kind: Option<String>,
    pub api_key: Option<String>,
    pub api_key_secret_ref: Option<SecretRef>,
    pub api_key_vault_ref: Option<String>,
    pub request_timeout_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub circuit_breaker_failure_threshold: Option<u32>,
    pub circuit_breaker_cooldown_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileModelProviderRegistryModel {
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub role: Option<String>,
    pub enabled: Option<bool>,
    pub metadata_source: Option<String>,
    pub operator_override: Option<bool>,
    pub tool_calls: Option<bool>,
    pub json_mode: Option<bool>,
    pub vision: Option<bool>,
    pub audio_transcribe: Option<bool>,
    pub embeddings: Option<bool>,
    pub max_context_tokens: Option<u32>,
    pub cost_tier: Option<String>,
    pub latency_tier: Option<String>,
    pub recommended_use_cases: Option<Vec<String>>,
    pub known_limitations: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileToolCallConfig {
    pub allowed_tools: Option<Vec<String>>,
    pub max_calls_per_run: Option<u32>,
    pub execution_timeout_ms: Option<u64>,
    pub process_runner: Option<FileProcessRunnerConfig>,
    pub wasm_runtime: Option<FileWasmRuntimeConfig>,
    pub http_fetch: Option<FileHttpFetchConfig>,
    pub browser_service: Option<FileBrowserServiceConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileHttpFetchConfig {
    pub allow_private_targets: Option<bool>,
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub max_response_bytes: Option<u64>,
    pub allow_redirects: Option<bool>,
    pub max_redirects: Option<u32>,
    pub allowed_content_types: Option<Vec<String>>,
    pub allowed_request_headers: Option<Vec<String>>,
    pub cache_enabled: Option<bool>,
    pub cache_ttl_ms: Option<u64>,
    pub max_cache_entries: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBrowserServiceConfig {
    pub enabled: Option<bool>,
    pub endpoint: Option<String>,
    pub auth_token: Option<String>,
    pub auth_token_secret_ref: Option<SecretRef>,
    pub state_dir: Option<String>,
    pub state_key_secret_ref: Option<SecretRef>,
    pub state_key_vault_ref: Option<String>,
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub max_screenshot_bytes: Option<u64>,
    pub max_title_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRouterConfig {
    pub enabled: Option<bool>,
    pub max_message_bytes: Option<u64>,
    pub max_retry_queue_depth_per_channel: Option<u64>,
    pub max_retry_attempts: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub default_response_prefix: Option<String>,
    pub routing: Option<FileChannelRoutingConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCanvasHostConfig {
    pub enabled: Option<bool>,
    pub public_base_url: Option<String>,
    pub token_ttl_ms: Option<u64>,
    pub max_state_bytes: Option<u64>,
    pub max_bundle_bytes: Option<u64>,
    pub max_assets_per_bundle: Option<u32>,
    pub max_updates_per_minute: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRoutingConfig {
    pub default_channel_enabled: Option<bool>,
    pub default_allow_direct_messages: Option<bool>,
    pub default_direct_message_policy: Option<String>,
    pub default_isolate_session_by_sender: Option<bool>,
    pub default_broadcast_strategy: Option<String>,
    pub default_concurrency_limit: Option<u64>,
    pub channels: Option<Vec<FileChannelRoutingRule>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRoutingRule {
    pub channel: Option<String>,
    pub enabled: Option<bool>,
    pub mention_patterns: Option<Vec<String>>,
    pub allow_from: Option<Vec<String>>,
    pub deny_from: Option<Vec<String>>,
    pub allow_direct_messages: Option<bool>,
    pub direct_message_policy: Option<String>,
    pub isolate_session_by_sender: Option<bool>,
    pub response_prefix: Option<String>,
    pub auto_ack_text: Option<String>,
    pub auto_reaction: Option<String>,
    pub broadcast_strategy: Option<String>,
    pub concurrency_limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProcessRunnerConfig {
    pub enabled: Option<bool>,
    pub tier: Option<String>,
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
    pub auth_token_secret_ref: Option<SecretRef>,
    pub connector_token: Option<String>,
    pub connector_token_secret_ref: Option<SecretRef>,
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
    pub max_journal_events: Option<u64>,
    pub vault_dir: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{is_secret_config_path, redact_secret_config_values, RootFileConfig};

    #[test]
    fn secret_config_path_matching_is_case_insensitive() {
        assert!(is_secret_config_path("model_provider.openai_api_key"));
        assert!(is_secret_config_path("model_provider.openai_api_key_secret_ref"));
        assert!(is_secret_config_path("model_provider.OPENAI_API_KEY"));
        assert!(is_secret_config_path("model_provider.openai_api_key_vault_ref"));
        assert!(is_secret_config_path("gateway.admin_token"));
        assert!(is_secret_config_path("tool_call.browser_service.auth_token"));
        assert!(is_secret_config_path("tool_call.browser_service.auth_token_secret_ref"));
        assert!(is_secret_config_path("tool_call.browser_service.state_key_secret_ref"));
        assert!(is_secret_config_path("tool_call.browser_service.state_key_vault_ref"));
        assert!(is_secret_config_path(" admin.auth_token "));
        assert!(is_secret_config_path("admin.auth_token_secret_ref"));
        assert!(is_secret_config_path("admin.connector_token"));
        assert!(is_secret_config_path("admin.connector_token_secret_ref"));
        assert!(!is_secret_config_path("daemon.port"));
    }

    #[test]
    fn redaction_replaces_known_secret_fields() {
        let mut document: toml::Value = toml::from_str(
            r#"
            version = 1
            [admin]
            auth_token = "token-value"
            connector_token = "connector-token-value"
            [model_provider]
            openai_api_key = "sk-secret"
            openai_api_key_vault_ref = "vault://global/openai_api_key"
            [model_provider.openai_api_key_secret_ref]
            kind = "env"
            variable = "PALYRA_OPENAI_API_KEY"
            [gateway]
            admin_token = "legacy-token"
            [admin.auth_token_secret_ref]
            kind = "file"
            path = "secrets/admin.txt"
            trusted_dirs = ["secrets"]
            [tool_call.browser_service]
            auth_token = "browserd-token"
            state_key_vault_ref = "global/browserd_state_key"
            [tool_call.browser_service.auth_token_secret_ref]
            kind = "exec"
            command = ["git", "--version"]
            [tool_call.browser_service.state_key_secret_ref]
            kind = "file"
            path = "secrets/browserd.key"
            trusted_dirs = ["secrets"]
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
                .get("admin")
                .and_then(|admin| admin.get("connector_token"))
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
                .get("model_provider")
                .and_then(|provider| provider.get("openai_api_key_secret_ref"))
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
        assert_eq!(
            document
                .get("admin")
                .and_then(|admin| admin.get("auth_token_secret_ref"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("tool_call")
                .and_then(|tool_call| tool_call.get("browser_service"))
                .and_then(|browser_service| browser_service.get("auth_token"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("tool_call")
                .and_then(|tool_call| tool_call.get("browser_service"))
                .and_then(|browser_service| browser_service.get("auth_token_secret_ref"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("tool_call")
                .and_then(|tool_call| tool_call.get("browser_service"))
                .and_then(|browser_service| browser_service.get("state_key_secret_ref"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
        assert_eq!(
            document
                .get("tool_call")
                .and_then(|tool_call| tool_call.get("browser_service"))
                .and_then(|browser_service| browser_service.get("state_key_vault_ref"))
                .and_then(toml::Value::as_str),
            Some("<redacted>")
        );
    }

    #[test]
    fn structured_secret_ref_fields_parse_in_config_schema() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [model_provider.openai_api_key_secret_ref]
            kind = "env"
            variable = "PALYRA_OPENAI_API_KEY"
            display_name = "OpenAI API key"
            [tool_call.browser_service.state_key_secret_ref]
            kind = "file"
            path = "secrets/browserd.key"
            trusted_dirs = ["secrets"]
            "#,
        )
        .expect("structured secret refs should parse");

        let model_provider = parsed.model_provider.expect("model_provider section should parse");
        assert_eq!(
            model_provider
                .openai_api_key_secret_ref
                .expect("secret ref should be present")
                .source_kind(),
            "env"
        );
        let tool_call = parsed.tool_call.expect("tool_call section should parse");
        assert_eq!(
            tool_call
                .browser_service
                .expect("browser service section should parse")
                .state_key_secret_ref
                .expect("browser state secret ref should be present")
                .source_kind(),
            "file"
        );
    }

    #[test]
    fn gateway_access_section_parses_expected_fields() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [gateway_access]
            remote_base_url = "https://console.example.com/palyra"
            pinned_server_cert_fingerprint_sha256 = "01ab"
            "#,
        )
        .expect("gateway access config should parse");

        let gateway_access =
            parsed.gateway_access.as_ref().expect("gateway_access section should be available");
        assert_eq!(
            gateway_access.remote_base_url.as_deref(),
            Some("https://console.example.com/palyra")
        );
        assert_eq!(gateway_access.pinned_server_cert_fingerprint_sha256.as_deref(), Some("01ab"));
        assert!(gateway_access.pinned_gateway_ca_fingerprint_sha256.is_none());
    }

    #[test]
    fn feature_rollouts_section_parses_expected_fields() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [feature_rollouts]
            dynamic_tool_builder = true
            context_engine = true
            execution_backend_remote_node = false
            execution_backend_networked_worker = true
            execution_backend_ssh_tunnel = true
            safety_boundary = true
            execution_gate_pipeline_v2 = false
            session_queue_policy = true
            pruning_policy_matrix = false
            retrieval_dual_path = true
            auxiliary_executor = true
            flow_orchestration = false
            delivery_arbitration = true
            replay_capture = true
            networked_workers = false
            "#,
        )
        .expect("feature_rollouts section should parse");

        let feature_rollouts =
            parsed.feature_rollouts.as_ref().expect("feature_rollouts section should be available");
        assert_eq!(feature_rollouts.dynamic_tool_builder, Some(true));
        assert_eq!(feature_rollouts.context_engine, Some(true));
        assert_eq!(feature_rollouts.execution_backend_remote_node, Some(false));
        assert_eq!(feature_rollouts.execution_backend_networked_worker, Some(true));
        assert_eq!(feature_rollouts.execution_backend_ssh_tunnel, Some(true));
        assert_eq!(feature_rollouts.safety_boundary, Some(true));
        assert_eq!(feature_rollouts.execution_gate_pipeline_v2, Some(false));
        assert_eq!(feature_rollouts.session_queue_policy, Some(true));
        assert_eq!(feature_rollouts.pruning_policy_matrix, Some(false));
        assert_eq!(feature_rollouts.retrieval_dual_path, Some(true));
        assert_eq!(feature_rollouts.auxiliary_executor, Some(true));
        assert_eq!(feature_rollouts.flow_orchestration, Some(false));
        assert_eq!(feature_rollouts.delivery_arbitration, Some(true));
        assert_eq!(feature_rollouts.replay_capture, Some(true));
        assert_eq!(feature_rollouts.networked_workers, Some(false));
    }

    #[test]
    fn runtime_preview_sections_parse_expected_fields() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [session_queue_policy]
            mode = "preview_only"
            max_depth = 12
            merge_window_ms = 2500

            [pruning_policy_matrix]
            mode = "enabled"
            manual_apply_enabled = true
            min_token_savings = 256

            [retrieval_dual_path]
            mode = "preview_only"
            branch_timeout_ms = 2200
            prompt_budget_tokens = 2048

            [auxiliary_executor]
            mode = "preview_only"
            max_tasks_per_session = 4
            default_budget_tokens = 1536

            [flow_orchestration]
            mode = "enabled"
            cancellation_gate_enabled = true
            max_retry_count = 2

            [delivery_arbitration]
            mode = "disabled"
            descendant_preference = true
            suppression_limit = 3

            [replay_capture]
            mode = "preview_only"
            capture_runtime_decisions = true
            max_events_per_run = 96

            [networked_workers]
            mode = "preview_only"
            lease_ttl_ms = 900000
            require_attestation = true
            expected_image_digest_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            expected_build_digest_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            expected_artifact_digest_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            "#,
        )
        .expect("runtime preview sections should parse");

        assert_eq!(
            parsed.session_queue_policy.as_ref().and_then(|value| value.mode.as_deref()),
            Some("preview_only")
        );
        assert_eq!(
            parsed.session_queue_policy.as_ref().and_then(|value| value.max_depth),
            Some(12)
        );
        assert_eq!(
            parsed.pruning_policy_matrix.as_ref().and_then(|value| value.min_token_savings),
            Some(256)
        );
        assert_eq!(
            parsed.retrieval_dual_path.as_ref().and_then(|value| value.prompt_budget_tokens),
            Some(2048)
        );
        assert_eq!(
            parsed.auxiliary_executor.as_ref().and_then(|value| value.default_budget_tokens),
            Some(1536)
        );
        assert_eq!(
            parsed.flow_orchestration.as_ref().and_then(|value| value.max_retry_count),
            Some(2)
        );
        assert_eq!(
            parsed.delivery_arbitration.as_ref().and_then(|value| value.suppression_limit),
            Some(3)
        );
        assert_eq!(
            parsed.replay_capture.as_ref().and_then(|value| value.max_events_per_run),
            Some(96)
        );
        assert_eq!(
            parsed.networked_workers.as_ref().and_then(|value| value.lease_ttl_ms),
            Some(900000)
        );
        assert_eq!(
            parsed
                .networked_workers
                .as_ref()
                .and_then(|value| value.expected_image_digest_sha256.as_deref()),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }
}
