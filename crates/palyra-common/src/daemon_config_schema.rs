//! Typed serde mirror of the daemon's TOML config file (`palyra.toml`).
//!
//! Every table uses `deny_unknown_fields` with all-optional fields: unknown
//! keys fail loudly while absent sections fall back to daemon defaults.
//! Field names are pinned by config import/export fixtures and the JSON
//! config contracts under `schemas/json`, so renames are breaking changes.
//! Also owns the secret config paths redacted from config exports.

use serde::{Deserialize, Serialize};
use toml::Value;

use crate::secret_refs::SecretRef;

const REDACTED_CONFIG_VALUE: &str = "<redacted>";

/// Dot-separated config paths whose values must never leave the daemon
/// unredacted.
///
/// Extend this list whenever a new secret-bearing field is added to the
/// schema; [`redact_secret_config_values`] and config-export surfaces
/// consume it.
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

/// Typed metadata for operator-facing config diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConfigSchemaEntry {
    pub path: &'static str,
    pub value_type: &'static str,
    pub default_value: Option<&'static str>,
    pub env_vars: &'static [&'static str],
    pub secret: bool,
    pub deprecated: bool,
    pub restart_required: bool,
    pub category: &'static str,
    pub description: &'static str,
}

/// Durable config metadata surfaced by `palyra config explain` and doctor
/// diagnostics. The list is intentionally compact and tracks operator-facing
/// keys with defaults, env overrides, secret posture, or rollout risk.
pub const CONFIG_SCHEMA_ENTRIES: &[ConfigSchemaEntry] = &[
    ConfigSchemaEntry {
        path: "deployment.mode",
        value_type: "enum(local_desktop|server|remote_agent)",
        default_value: Some("local_desktop"),
        env_vars: &["PALYRA_DEPLOYMENT_MODE"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "deployment",
        description: "Runtime deployment posture used by bind and remote-access checks.",
    },
    ConfigSchemaEntry {
        path: "deployment.dangerous_remote_bind_ack",
        value_type: "bool",
        default_value: Some("false"),
        env_vars: &["PALYRA_DEPLOYMENT_DANGEROUS_REMOTE_BIND_ACK"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "deployment",
        description: "Explicit config-side acknowledgement for non-loopback runtime binds.",
    },
    ConfigSchemaEntry {
        path: "daemon.bind_addr",
        value_type: "ip_addr",
        default_value: Some("127.0.0.1"),
        env_vars: &["PALYRA_DAEMON_BIND_ADDR"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "daemon",
        description: "HTTP/admin daemon bind address.",
    },
    ConfigSchemaEntry {
        path: "daemon.port",
        value_type: "u16",
        default_value: Some("7142"),
        env_vars: &["PALYRA_DAEMON_PORT"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "daemon",
        description: "HTTP/admin daemon port.",
    },
    ConfigSchemaEntry {
        path: "gateway.grpc_bind_addr",
        value_type: "ip_addr",
        default_value: Some("127.0.0.1"),
        env_vars: &["PALYRA_GATEWAY_GRPC_BIND_ADDR"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "gateway",
        description: "gRPC gateway bind address.",
    },
    ConfigSchemaEntry {
        path: "gateway.grpc_port",
        value_type: "u16",
        default_value: Some("7443"),
        env_vars: &["PALYRA_GATEWAY_GRPC_PORT"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "gateway",
        description: "gRPC gateway port.",
    },
    ConfigSchemaEntry {
        path: "gateway.allow_insecure_remote",
        value_type: "bool",
        default_value: Some("false"),
        env_vars: &["PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "gateway",
        description: "Escape hatch for remote gateway binds without the secure profile.",
    },
    ConfigSchemaEntry {
        path: "admin.auth_token",
        value_type: "string",
        default_value: None,
        env_vars: &["PALYRA_ADMIN_TOKEN"],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "admin",
        description: "Admin API bearer token.",
    },
    ConfigSchemaEntry {
        path: "admin.auth_token_secret_ref",
        value_type: "secret_ref",
        default_value: None,
        env_vars: &[],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "admin",
        description: "Structured secret reference for the admin API token.",
    },
    ConfigSchemaEntry {
        path: "model_provider.kind",
        value_type: "enum(deterministic|openai_compatible|anthropic)",
        default_value: Some("openai_compatible"),
        env_vars: &["PALYRA_MODEL_PROVIDER_KIND"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Default model-provider protocol family.",
    },
    ConfigSchemaEntry {
        path: "model_provider.openai_api_key",
        value_type: "string",
        default_value: None,
        env_vars: &["PALYRA_OPENAI_API_KEY"],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Inline OpenAI-compatible API key.",
    },
    ConfigSchemaEntry {
        path: "model_provider.openai_api_key_secret_ref",
        value_type: "secret_ref",
        default_value: None,
        env_vars: &[],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Structured secret reference for the OpenAI-compatible API key.",
    },
    ConfigSchemaEntry {
        path: "model_provider.openai_api_key_vault_ref",
        value_type: "vault_ref",
        default_value: None,
        env_vars: &[],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Vault reference for the OpenAI-compatible API key.",
    },
    ConfigSchemaEntry {
        path: "model_provider.anthropic_api_key",
        value_type: "string",
        default_value: None,
        env_vars: &["PALYRA_ANTHROPIC_API_KEY"],
        secret: true,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Inline Anthropic-compatible API key.",
    },
    ConfigSchemaEntry {
        path: "model_provider.auth_profile_id",
        value_type: "string",
        default_value: None,
        env_vars: &["PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "model_provider",
        description: "Auth profile id used by model-provider requests.",
    },
    ConfigSchemaEntry {
        path: "tool_call.allowed_tools",
        value_type: "array<string>",
        default_value: Some("[]"),
        env_vars: &["PALYRA_TOOL_CALL_ALLOWED_TOOLS"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "tool_call",
        description: "Explicit tool allowlist exposed to model runs.",
    },
    ConfigSchemaEntry {
        path: "tool_call.disabled_tools",
        value_type: "array<string>",
        default_value: Some("[]"),
        env_vars: &["PALYRA_TOOL_CALL_DISABLED_TOOLS"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "tool_call",
        description: "Tools removed from the effective tool catalog.",
    },
    ConfigSchemaEntry {
        path: "tool_call.max_calls_per_run",
        value_type: "u32",
        default_value: Some("0"),
        env_vars: &["PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN"],
        secret: false,
        deprecated: true,
        restart_required: true,
        category: "tool_call",
        description: "Legacy compatibility budget; must not terminate agent run loops.",
    },
    ConfigSchemaEntry {
        path: "tool_call.http_fetch.allow_private_targets",
        value_type: "bool",
        default_value: Some("false"),
        env_vars: &["PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "tool_call",
        description: "Escape hatch allowing HTTP fetches to private network targets.",
    },
    ConfigSchemaEntry {
        path: "networked_workers.mode",
        value_type: "enum(disabled|preview_only|enabled)",
        default_value: Some("disabled"),
        env_vars: &["PALYRA_NETWORKED_WORKERS_MODE"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "workers",
        description: "Networked worker runtime mode.",
    },
    ConfigSchemaEntry {
        path: "networked_workers.require_attestation",
        value_type: "bool",
        default_value: Some("true"),
        env_vars: &["PALYRA_NETWORKED_WORKERS_REQUIRE_ATTESTATION"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "workers",
        description: "Requires worker attestation before capability grants.",
    },
    ConfigSchemaEntry {
        path: "networked_workers.expected_image_digest_sha256",
        value_type: "sha256",
        default_value: None,
        env_vars: &["PALYRA_NETWORKED_WORKERS_EXPECTED_IMAGE_DIGEST_SHA256"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "workers",
        description: "Pinned expected worker image digest.",
    },
    ConfigSchemaEntry {
        path: "orchestrator.runloop_v1_enabled",
        value_type: "bool",
        default_value: Some("false"),
        env_vars: &["PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "orchestrator",
        description: "Enables the v1 orchestrator run loop.",
    },
    ConfigSchemaEntry {
        path: "identity.allow_insecure_node_rpc_without_mtls",
        value_type: "bool",
        default_value: Some("false"),
        env_vars: &["PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS"],
        secret: false,
        deprecated: false,
        restart_required: true,
        category: "identity",
        description: "Escape hatch for node RPC without mTLS.",
    },
];

/// Returns the stable config metadata catalog.
#[must_use]
pub fn config_schema_entries() -> &'static [ConfigSchemaEntry] {
    CONFIG_SCHEMA_ENTRIES
}

/// Finds a config metadata entry by dot-separated config path.
#[must_use]
pub fn config_schema_entry(path: &str) -> Option<&'static ConfigSchemaEntry> {
    let normalized = normalize_config_path(path);
    CONFIG_SCHEMA_ENTRIES.iter().find(|entry| entry.path == normalized)
}

/// Returns PALYRA_* environment variables known to the config schema catalog.
#[must_use]
pub fn known_config_env_vars() -> Vec<&'static str> {
    let mut env_vars = CONFIG_SCHEMA_ENTRIES
        .iter()
        .flat_map(|entry| entry.env_vars.iter().copied())
        .collect::<Vec<_>>();
    env_vars.extend([
        "PALYRA_CONFIG",
        "PALYRA_STATE_ROOT",
        "PALYRA_HOME",
        "PALYRA_DAEMON_URL",
        "PALYRA_CLI_PROFILE",
        "PALYRA_CLI_PROFILES_PATH",
        "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK",
        "PALYRA_EXPERIMENTAL_NETWORKED_WORKERS",
        "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER",
        "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE",
        "PALYRA_HTTP_FETCH_ALLOWED_HOSTS",
        "PALYRA_HTTP_FETCH_ALLOWLIST",
        "PALYRA_HTTP_FETCH_ALLOWED_CREDENTIAL_VAULT_REFS",
        "PALYRA_CHANNEL_DELIVERY_PIPELINE_MODE",
        "PALYRA_CHANNEL_ROUTER_GROUP_GUARDRAILS",
        "PALYRA_TOOL_CATALOG_EXPOSURE_MODE",
        "PALYRA_TOOL_CATALOG_COMPACT_THRESHOLD",
        "PALYRA_TOOL_CALL_DENIED_TOOLS",
        "PALYRA_MINIMAX_API_KEY",
        "PALYRA_XAI_API_KEY",
    ]);
    env_vars.sort_unstable();
    env_vars.dedup();
    env_vars
}

/// Reports whether a dot-separated config path names a secret value.
///
/// Matching is case-insensitive and ignores whitespace around segments.
#[must_use]
pub fn is_secret_config_path(path: &str) -> bool {
    let normalized = normalize_config_path(path);
    SECRET_CONFIG_PATHS.iter().any(|candidate| *candidate == normalized)
}

/// Replaces all known secret values in a parsed config document with a
/// redaction marker.
///
/// Covers the static [`SECRET_CONFIG_PATHS`] plus per-provider secret fields
/// inside `model_provider.providers` registry entries.
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

/// Root of the config file; one optional field per top-level TOML table.
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

/// `[deployment]`: bootstrap profile and remote-bind acknowledgement.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDeploymentConfig {
    pub profile: Option<String>,
    pub mode: Option<String>,
    pub dangerous_remote_bind_ack: Option<bool>,
}

/// `[daemon]`: HTTP bind address and port.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDaemonConfig {
    pub bind_addr: Option<String>,
    pub port: Option<u16>,
}

/// `[gateway]`: gRPC/QUIC bind posture, TLS, and response limits.
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

/// `[gateway.tls]`: gateway TLS material paths.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileGatewayTlsConfig {
    pub enabled: Option<bool>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub client_ca_path: Option<String>,
}

/// `[gateway_access]`: remote console URL and pinned certificate
/// fingerprints.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileGatewayAccessConfig {
    pub remote_base_url: Option<String>,
    pub pinned_server_cert_fingerprint_sha256: Option<String>,
    pub pinned_gateway_ca_fingerprint_sha256: Option<String>,
}

/// `[feature_rollouts]`: per-feature rollout switches.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFeatureRolloutsConfig {
    pub dynamic_tool_builder: Option<bool>,
    pub context_engine: Option<bool>,
    pub execution_backend_remote_node: Option<bool>,
    pub execution_backend_networked_worker: Option<bool>,
    pub execution_backend_docker: Option<bool>,
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
    pub tool_repair: Option<bool>,
    pub provider_stream_normalizer: Option<bool>,
    pub channel_turn_kernel: Option<bool>,
    pub agent_plan_state: Option<bool>,
    pub objective_judge: Option<bool>,
    pub verification_runtime: Option<bool>,
    pub progress_drafts: Option<bool>,
    pub compaction_safeguard: Option<bool>,
    pub attack_surface_audit: Option<bool>,
}

/// `[session_queue_policy]`: runtime-preview queue posture.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSessionQueuePolicyConfig {
    pub mode: Option<String>,
    pub max_depth: Option<u64>,
    pub merge_window_ms: Option<u64>,
}

/// `[pruning_policy_matrix]`: runtime-preview compaction/pruning posture.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePruningPolicyMatrixConfig {
    pub mode: Option<String>,
    pub manual_apply_enabled: Option<bool>,
    pub min_token_savings: Option<u64>,
}

/// `[retrieval_dual_path]`: runtime-preview split-retrieval posture.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalDualPathConfig {
    pub mode: Option<String>,
    pub branch_timeout_ms: Option<u64>,
    pub prompt_budget_tokens: Option<u64>,
}

/// `[auxiliary_executor]`: runtime-preview background-task limits.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAuxiliaryExecutorConfig {
    pub mode: Option<String>,
    pub max_tasks_per_session: Option<u64>,
    pub default_budget_tokens: Option<u64>,
}

/// `[flow_orchestration]`: runtime-preview flow transition/retry posture.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFlowOrchestrationConfig {
    pub mode: Option<String>,
    pub cancellation_gate_enabled: Option<bool>,
    pub max_retry_count: Option<u32>,
}

/// `[delivery_arbitration]`: runtime-preview descendant delivery policy.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDeliveryArbitrationConfig {
    pub mode: Option<String>,
    pub descendant_preference: Option<bool>,
    pub suppression_limit: Option<u32>,
}

/// `[replay_capture]`: runtime-preview replay bundle capture posture.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileReplayCaptureConfig {
    pub mode: Option<String>,
    pub capture_runtime_decisions: Option<bool>,
    pub max_events_per_run: Option<u64>,
}

/// `[networked_workers]`: worker leasing, attestation, and expected digests.
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

/// `[cron]`: scheduler timezone.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCronConfig {
    pub timezone: Option<String>,
}

/// `[orchestrator]`: orchestrator run-loop rollout switch.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileOrchestratorConfig {
    pub runloop_v1_enabled: Option<bool>,
}

/// `[memory]`: memory item limits, auto-inject, retention, and retrieval.
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

/// `[memory.auto_inject]`: automatic memory injection limits.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryAutoInjectConfig {
    pub enabled: Option<bool>,
    pub max_items: Option<u64>,
}

/// `[memory.retention]`: memory store retention and vacuum policy.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryRetentionConfig {
    pub max_entries: Option<u64>,
    pub max_bytes: Option<u64>,
    pub ttl_days: Option<u32>,
    pub vacuum_schedule: Option<String>,
}

/// `[memory.retrieval]`: retrieval backend and scoring configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMemoryRetrievalConfig {
    pub backend: Option<FileRetrievalBackendConfig>,
    pub scoring: Option<FileRetrievalScoringConfig>,
}

/// `[memory.retrieval.backend]`: retrieval backend selection.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRetrievalBackendConfig {
    pub kind: Option<String>,
}

/// `[memory.retrieval.scoring]`: per-source scoring profiles in basis points.
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

/// Scoring weights (basis points) for one retrieval source.
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

/// `[media]`: attachment download/upload limits and content-type allowlists.
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

/// `[model_provider]`: provider credentials, endpoints, retries, and the
/// provider/model registry.
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
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
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

/// One `[[model_provider.providers]]` registry entry.
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

/// One `[[model_provider.models]]` registry entry with capability metadata.
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
    pub reasoning: Option<bool>,
    pub reasoning_efforts: Option<Vec<String>>,
    pub service_tier: Option<bool>,
    pub service_tiers: Option<Vec<String>>,
    pub max_context_tokens: Option<u32>,
    pub cost_tier: Option<String>,
    pub latency_tier: Option<String>,
    pub recommended_use_cases: Option<Vec<String>>,
    pub known_limitations: Option<Vec<String>>,
}

/// `[tool_call]`: tool allowlist, budgets, and tool-runtime sub-sections.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileToolCallConfig {
    pub allowed_tools: Option<Vec<String>>,
    pub profiles: Option<Vec<String>>,
    pub extra_tools: Option<Vec<String>>,
    pub disabled_tools: Option<Vec<String>>,
    pub catalog_exposure_mode: Option<String>,
    pub compact_tool_threshold: Option<usize>,
    pub max_calls_per_run: Option<u32>,
    pub execution_timeout_ms: Option<u64>,
    pub process_runner: Option<FileProcessRunnerConfig>,
    pub code_intel: Option<FileCodeIntelConfig>,
    pub wasm_runtime: Option<FileWasmRuntimeConfig>,
    pub http_fetch: Option<FileHttpFetchConfig>,
    pub browser_service: Option<FileBrowserServiceConfig>,
}

/// `[tool_call.code_intel]`: bounded code diagnostics provider settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCodeIntelConfig {
    pub enabled: Option<bool>,
    pub workspace_root: Option<String>,
    pub rust_analyzer_binary: Option<String>,
    pub typescript_server_binary: Option<String>,
    pub pyright_binary: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_output_bytes: Option<u64>,
    pub max_items: Option<u64>,
    pub idle_reap_ms: Option<u64>,
}

/// `[tool_call.http_fetch]`: outbound HTTP fetch policy and limits.
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
    pub allowed_credential_vault_refs: Option<Vec<String>>,
    pub cache_enabled: Option<bool>,
    pub cache_ttl_ms: Option<u64>,
    pub max_cache_entries: Option<u64>,
}

/// `[tool_call.browser_service]`: browser daemon endpoint, auth, and limits.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBrowserServiceConfig {
    pub enabled: Option<bool>,
    pub endpoint: Option<String>,
    pub health_base_url: Option<String>,
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

/// `[channel_router]`: channel delivery limits, retries, and routing.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRouterConfig {
    pub enabled: Option<bool>,
    pub max_message_bytes: Option<u64>,
    pub max_retry_queue_depth_per_channel: Option<u64>,
    pub max_retry_attempts: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub default_response_prefix: Option<String>,
    pub inbound_coalescing: Option<FileInboundCoalescingConfig>,
    pub routing: Option<FileChannelRoutingConfig>,
}

/// `[channel_router.inbound_coalescing]`: inbound message debouncing.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileInboundCoalescingConfig {
    pub enabled: Option<bool>,
    pub debounce_ms: Option<u64>,
    pub max_tracked_keys: Option<u64>,
    pub bypass_commands: Option<bool>,
    pub bypass_media: Option<bool>,
}

/// `[canvas_host]`: canvas hosting limits and token TTL.
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

/// `[channel_router.routing]`: default routing posture and per-channel rules.
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

/// One `[[channel_router.routing.channels]]` rule.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRoutingRule {
    pub channel: Option<String>,
    pub enabled: Option<bool>,
    pub mention_patterns: Option<Vec<String>>,
    pub route_targets: Option<Vec<FileChannelRouteTargetRule>>,
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

/// One route target inside a `channel_router.routing.channels[*]` rule.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChannelRouteTargetRule {
    pub agent_id: Option<String>,
    pub mention_patterns: Option<Vec<String>>,
    pub required_sender_roles: Option<Vec<String>>,
}

/// `[tool_call.process_runner]`: sandboxed process execution policy and
/// limits.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProcessRunnerConfig {
    pub enabled: Option<bool>,
    pub tier: Option<String>,
    pub workspace_root: Option<String>,
    pub path_access_mode: Option<String>,
    pub allowed_executables: Option<Vec<String>>,
    pub allow_interpreters: Option<bool>,
    pub egress_enforcement_mode: Option<String>,
    pub allowed_egress_hosts: Option<Vec<String>>,
    pub allowed_dns_suffixes: Option<Vec<String>>,
    pub cpu_time_limit_ms: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub max_output_bytes: Option<u64>,
}

/// `[tool_call.wasm_runtime]`: Wasm plugin runtime limits and allowlists.
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

/// `[admin]`: admin/connector auth tokens and bound principal.
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

/// `[identity]`: identity and mTLS escape hatches.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileIdentityConfig {
    pub allow_insecure_node_rpc_without_mtls: Option<bool>,
}

/// `[storage]`: journal and vault storage paths and limits.
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
            execution_backend_docker = false
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
            tool_repair = true
            provider_stream_normalizer = false
            channel_turn_kernel = true
            agent_plan_state = false
            objective_judge = true
            verification_runtime = true
            progress_drafts = false
            compaction_safeguard = true
            attack_surface_audit = false
            "#,
        )
        .expect("feature_rollouts section should parse");

        let feature_rollouts =
            parsed.feature_rollouts.as_ref().expect("feature_rollouts section should be available");
        assert_eq!(feature_rollouts.dynamic_tool_builder, Some(true));
        assert_eq!(feature_rollouts.context_engine, Some(true));
        assert_eq!(feature_rollouts.execution_backend_remote_node, Some(false));
        assert_eq!(feature_rollouts.execution_backend_networked_worker, Some(true));
        assert_eq!(feature_rollouts.execution_backend_docker, Some(false));
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
        assert_eq!(feature_rollouts.tool_repair, Some(true));
        assert_eq!(feature_rollouts.provider_stream_normalizer, Some(false));
        assert_eq!(feature_rollouts.channel_turn_kernel, Some(true));
        assert_eq!(feature_rollouts.agent_plan_state, Some(false));
        assert_eq!(feature_rollouts.objective_judge, Some(true));
        assert_eq!(feature_rollouts.verification_runtime, Some(true));
        assert_eq!(feature_rollouts.progress_drafts, Some(false));
        assert_eq!(feature_rollouts.compaction_safeguard, Some(true));
        assert_eq!(feature_rollouts.attack_surface_audit, Some(false));
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

    #[test]
    fn code_intel_tool_call_section_parses_expected_fields() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [tool_call.code_intel]
            enabled = true
            workspace_root = "workspace"
            rust_analyzer_binary = "rust-analyzer"
            typescript_server_binary = "typescript-language-server"
            pyright_binary = "pyright-langserver"
            timeout_ms = 1500
            max_output_bytes = 32768
            max_items = 24
            idle_reap_ms = 60000
            "#,
        )
        .expect("code_intel section should parse");

        let code_intel = parsed
            .tool_call
            .as_ref()
            .and_then(|tool_call| tool_call.code_intel.as_ref())
            .expect("code_intel section should be present");
        assert_eq!(code_intel.enabled, Some(true));
        assert_eq!(code_intel.workspace_root.as_deref(), Some("workspace"));
        assert_eq!(code_intel.rust_analyzer_binary.as_deref(), Some("rust-analyzer"));
        assert_eq!(
            code_intel.typescript_server_binary.as_deref(),
            Some("typescript-language-server")
        );
        assert_eq!(code_intel.pyright_binary.as_deref(), Some("pyright-langserver"));
        assert_eq!(code_intel.timeout_ms, Some(1_500));
        assert_eq!(code_intel.max_output_bytes, Some(32_768));
        assert_eq!(code_intel.max_items, Some(24));
        assert_eq!(code_intel.idle_reap_ms, Some(60_000));
    }
}
