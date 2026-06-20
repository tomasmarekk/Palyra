//! Console and diagnostics snapshots for provider registry/runtime state.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::{ProviderCapabilitiesSnapshot, ProviderFailureSnapshot};
/// Configured retry budget: attempt count and base backoff delay.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRetryPolicySnapshot {
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

/// Point-in-time circuit breaker state for one provider.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderCircuitBreakerSnapshot {
    pub failure_threshold: u32,
    pub cooldown_ms: u64,
    pub consecutive_failures: u32,
    pub open: bool,
}

/// Aggregated request/error/latency/token counters for one provider runtime;
/// `error_rate_bps` is in basis points (1/10000).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRuntimeMetricsSnapshot {
    pub request_count: u64,
    pub error_count: u64,
    pub error_rate_bps: u32,
    pub total_retry_attempts: u64,
    pub total_prompt_tokens: u64,
    pub total_completion_tokens: u64,
    pub avg_prompt_tokens_per_run: u64,
    pub avg_completion_tokens_per_run: u64,
    pub last_latency_ms: u64,
    pub avg_latency_ms: u64,
    pub max_latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ProviderFailureSnapshot>,
}

/// Latest health probe outcome for a provider; `source` records whether the
/// probe was static, registry-derived, or runtime-observed.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderHealthProbeSnapshot {
    pub state: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checked_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub source: String,
}

/// Model discovery status for a provider, including the model ids currently
/// believed to be available.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderDiscoverySnapshot {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checked_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub discovered_model_ids: Vec<String>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Console-facing view of one registry provider: identity, configuration
/// flags, and live retry/circuit/health/discovery state. Secrets are reduced
/// to the `api_key_configured` boolean.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistryProviderSnapshot {
    pub provider_id: String,
    pub credential_id: String,
    pub display_name: String,
    pub kind: String,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_provider_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_source: Option<String>,
    pub api_key_configured: bool,
    pub retry_policy: ProviderRetryPolicySnapshot,
    pub circuit_breaker: ProviderCircuitBreakerSnapshot,
    pub runtime_metrics: ProviderRuntimeMetricsSnapshot,
    pub health: ProviderHealthProbeSnapshot,
    pub discovery: ProviderDiscoverySnapshot,
}

/// Union of capabilities across the enabled models reachable through one
/// credential.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderCredentialCapabilitySummary {
    pub chat: bool,
    pub embeddings: bool,
    pub audio_transcription: bool,
    pub vision: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_context_tokens: Option<u32>,
}

/// Console-facing view of one provider credential: availability state,
/// capability summary, and runtime health. Never carries secret material.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistryCredentialSnapshot {
    pub credential_id: String,
    pub provider_id: String,
    pub provider_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_provider_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_source: Option<String>,
    pub availability_state: String,
    pub capability_summary: ProviderCredentialCapabilitySummary,
    pub health: ProviderHealthProbeSnapshot,
    pub runtime_metrics: ProviderRuntimeMetricsSnapshot,
}

/// Console-facing view of one registry model entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistryModelSnapshot {
    pub model_id: String,
    pub provider_id: String,
    pub role: String,
    pub enabled: bool,
    pub capabilities: ProviderCapabilitiesSnapshot,
}

/// Console-facing view of the whole registry: role defaults, failover/cache
/// flags, and per-provider/credential/model snapshots.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistrySnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_chat_model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_embeddings_model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_audio_transcription_model_id: Option<String>,
    pub failover_enabled: bool,
    pub response_cache_enabled: bool,
    pub providers: Vec<ProviderRegistryProviderSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credentials: Vec<ProviderRegistryCredentialSnapshot>,
    pub models: Vec<ProviderRegistryModelSnapshot>,
}

/// Why one chat candidate was or was not routable at snapshot time.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRouteCandidateTrace {
    pub provider_id: String,
    pub credential_id: String,
    pub model_id: String,
    pub role: String,
    pub capability_state: String,
    pub health_state: String,
    pub selected: bool,
    pub reason_code: String,
}

/// Explainable routing trace: which model would be selected for chat and how
/// every other candidate was ranked or excluded.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRouteSelectionTrace {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model_id: Option<String>,
    pub failover_enabled: bool,
    pub generated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<ProviderRouteCandidateTrace>,
}

impl ProviderRouteSelectionTrace {
    /// Returns a trace with no candidates, timestamped now.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            default_model_id: None,
            failover_enabled: false,
            generated_at_unix_ms: snapshot_current_unix_ms().unwrap_or_default(),
            selected_provider_id: None,
            selected_model_id: None,
            candidates: Vec::new(),
        }
    }
}

/// Point-in-time response cache statistics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderResponseCacheSnapshot {
    pub enabled: bool,
    pub entry_count: usize,
    pub hit_count: u64,
    pub miss_count: u64,
}

/// Complete provider status surfaced over the console API: default provider
/// identity, capabilities, retry/circuit/cache/health/discovery state, the
/// full registry view, and the route selection trace.
///
/// The legacy `openai_*`/`anthropic_*` fields mirror the default provider so
/// older console clients keep working.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderStatusSnapshot {
    pub kind: String,
    pub provider_id: String,
    pub credential_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    pub capabilities: ProviderCapabilitiesSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_embeddings_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_embeddings_dims: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_provider_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_source: Option<String>,
    pub api_key_configured: bool,
    pub retry_policy: ProviderRetryPolicySnapshot,
    pub circuit_breaker: ProviderCircuitBreakerSnapshot,
    pub runtime_metrics: ProviderRuntimeMetricsSnapshot,
    pub response_cache: ProviderResponseCacheSnapshot,
    pub health: ProviderHealthProbeSnapshot,
    pub discovery: ProviderDiscoverySnapshot,
    pub registry: ProviderRegistrySnapshot,
    pub route_selection: ProviderRouteSelectionTrace,
}

fn snapshot_current_unix_ms() -> Result<i64, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}
