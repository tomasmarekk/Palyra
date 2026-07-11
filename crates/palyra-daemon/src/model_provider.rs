//! Model/provider orchestration: registry-backed routing across chat,
//! embeddings, and audio-transcription backends with retries, circuit
//! breaking, response caching, and console status snapshots.
//!
//! Provider abstraction model: [`ModelProvider`] (chat completion, audio
//! transcription, status) and [`EmbeddingsProvider`] are the runtime traits.
//! Concrete backends are a deterministic offline provider (fixtures, smoke
//! flows), an OpenAI-compatible HTTP provider, and an Anthropic-compatible
//! HTTP provider that also serves MiniMax via bearer auth.
//! [`RegistryBackedModelProvider`] composes per-provider runtimes from
//! [`ModelProviderRegistryConfig`] and fails over between providers when the
//! primary candidate errors.
//!
//! Streaming semantics: provider output is normalized into bounded
//! [`ProviderEvent::ModelToken`] preview events (see the `streaming` and
//! `contract` submodules) so the orchestrator consumes one uniform event
//! stream regardless of whether the upstream transport is streaming or
//! response-body based.
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use reqwest::{header::RETRY_AFTER, Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ulid::Ulid;

use crate::{
    application::tool_registry::ModelVisibleToolCatalogSnapshot, orchestrator::estimate_token_count,
};

mod adapters;
use adapters::{
    openai_responses_tool_wire_name_map, AnthropicCompatibleChatAdapter,
    OpenAiCompatibleChatAdapter, OpenAiResponsesChatAdapter, ProviderChatAdapter,
};
#[cfg(test)]
pub(crate) use palyra_model_providers::MAX_TOOL_ARGUMENT_BYTES;
pub(crate) use palyra_model_providers::{
    anthropic_compatible_uses_anthropic_oauth_headers, anthropic_compatible_uses_bearer_auth,
    bounded_provider_turn_output_for_persistence, coerce_raw_tool_call_markup,
    normalize_tool_arguments, normalize_tool_input_value, provider_events_from_output,
    redact_remote_secret_fragments, sanitize_remote_error,
};
#[allow(unused_imports)]
pub use palyra_model_providers::{
    assemble_canonical_tool_calls, validate_canonical_provider_stream, ProviderCanonicalEvent,
    ProviderError, ProviderErrorEnvelope, ProviderErrorKind, ProviderErrorSeverity,
    ProviderFailureAction, ProviderFailureCategory, ProviderFailureClass,
    ProviderFailureClassification, ProviderFailureSnapshot, ProviderRecoveryAction,
    ProviderRecoveryDecision, ProviderRecoveryDecisionKind, ProviderRecoveryPlanSnapshot,
    ProviderRetryability, ProviderStreamAccumulator, ProviderStreamEvent, ToolCallAssemblyPolicy,
    PROVIDER_CANONICAL_STREAM_AUDIT_EVENT, PROVIDER_RECOVERY_DECISION_EVENT,
    TOOL_CALL_ASSEMBLER_AUDIT_EVENT,
};
#[allow(unused_imports)]
pub use palyra_model_providers::{
    capability_defaults_for_kind, capability_defaults_for_provider, configured_model_id,
    model_id_supports_reasoning_effort, validate_model_provider_config,
    validate_openai_base_url_network_policy, validate_openai_base_url_network_policy_with_resolver,
    ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderCredentialSource,
    ModelProviderKind, ModelProviderRegistryConfig, ProviderCapabilitiesSnapshot, ProviderCostTier,
    ProviderLatencyTier, ProviderMetadataSource, ProviderModelEntryConfig, ProviderModelRole,
    ProviderRegistryEntryConfig,
};
use palyra_model_providers::{
    classify_http_provider_failure, classify_reqwest_provider_failure,
    classify_transport_provider_failure, fail_closed_provider_classification,
    failover_provider_classification, invalid_response_classification,
    provider_output_from_text_and_tools, provider_request_has_vision,
    qa_mock_provider_output_for_attempt, qa_mock_provider_turn_for_request,
    retry_provider_classification, retryable_invalid_response_classification,
    user_action_provider_classification, ANTHROPIC_API_VERSION, MAX_QA_MOCK_PROVIDER_ATTEMPTS,
    MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS, MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS,
    OPENAI_CODEX_BACKEND_BASE_URL as OPENAI_CODEX_RESPONSES_BASE_URL,
};
#[allow(unused_imports)]
pub use palyra_model_providers::{
    classify_terminal_outcome, AudioTranscriptionRequest, AudioTranscriptionResponse,
    AudioTranscriptionSegment, EmbeddingsRequest, EmbeddingsResponse, PromptCachePolicy,
    PromptCacheReport, PromptCacheStrategy, ProviderAttemptState, ProviderAttemptSummary,
    ProviderEvent, ProviderFinishReason, ProviderImageInput, ProviderMessage,
    ProviderMessageContentPart, ProviderMessageRole, ProviderMessageToolCall,
    ProviderOutputContentPart, ProviderPromptCacheHint, ProviderPromptSegment,
    ProviderPromptSegmentKind, ProviderRawProviderRefs, ProviderReasoningEffort,
    ProviderRedactionState, ProviderRequest, ProviderResponse, ProviderServiceTier,
    ProviderTurnOutput, ProviderUsage, TerminalOutcomeClass, TerminalOutcomeClassification,
};
#[allow(unused_imports)]
pub use palyra_model_providers::{
    decide_tool_repair_candidate, normalize_assistant_output_for_tool_repair,
    tool_repair_audit_events_for_decision, NormalizedAssistantOutput, ProviderNeutralStreamEvent,
    ProviderStreamSegment, ToolProposalCandidate, ToolRepairAuditEvent, ToolRepairBoundary,
    ToolRepairBoundaryState, ToolRepairCandidate, ToolRepairCandidateFormat, ToolRepairDecision,
    ToolRepairDecisionStatus, ToolRepairStreamNormalizer, DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
};
use palyra_model_providers::{
    parse_qa_mock_provider_fixture_yaml, qa_mock::MIN_QA_MOCK_PROVIDER_ATTEMPTS,
};
#[allow(unused_imports)]
pub use palyra_model_providers::{
    ProviderCircuitBreakerSnapshot, ProviderCredentialCapabilitySummary, ProviderDiscoverySnapshot,
    ProviderHealthProbeSnapshot, ProviderRegistryCredentialSnapshot, ProviderRegistryModelSnapshot,
    ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderResponseCacheSnapshot,
    ProviderRetryPolicySnapshot, ProviderRouteCandidateTrace, ProviderRouteSelectionTrace,
    ProviderRuntimeMetricsSnapshot, ProviderStatusSnapshot, QaMockProviderFixture,
};

const OPENAI_CHAT_COMPLETIONS_PATH: &str = "/chat/completions";
const OPENAI_CODEX_RESPONSES_PATH: &str = "/responses";
const OPENAI_CODEX_ORIGINATOR: &str = "codex_cli_rs";
const OPENAI_CODEX_USER_AGENT: &str = "codex_cli_rs/0.0.0 (Palyra)";
const OPENAI_CHATGPT_AUTH_CLAIM_NAMESPACE: &str = "https://api.openai.com/auth";
const OPENAI_CHATGPT_ACCOUNT_ID_CLAIM: &str = "chatgpt_account_id";
const OPENAI_EMBEDDINGS_PATH: &str = "/embeddings";
const OPENAI_AUDIO_TRANSCRIPTIONS_PATH: &str = "/audio/transcriptions";
const ANTHROPIC_MESSAGES_PATH: &str = "/v1/messages";
const ANTHROPIC_OAUTH_BETA_HEADER: &str = "claude-code-20250219,oauth-2025-04-20";
const ANTHROPIC_OAUTH_USER_AGENT: &str = "claude-cli/2.1.74 (external, cli)";
// Shared by all HTTP backends; 529 is the Anthropic/MiniMax overload status
// and must be retried like the other transient upstream codes.
const OPENAI_RETRYABLE_STATUS_CODES: &[u16] = &[429, 500, 502, 503, 504, 529];
const FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID: &str = "e2e-failing-openai";
const FAILOVER_SELF_CHECK_PRIMARY_MODEL_ID: &str = "e2e-failing-openai-chat";
const FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID: &str = "e2e-deterministic-fallback";
const FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID: &str = "e2e-deterministic-chat";
const FAILOVER_SELF_CHECK_PROMPT: &str = "palyra provider failover fixture";

/// Safety metadata for the synthetic provider failover self-check.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProviderFailoverSelfCheckSafety {
    pub label: String,
    pub uses_real_config: bool,
    pub uses_real_credentials: bool,
    pub performs_network_io: bool,
}

/// Operator-facing result of the synthetic provider failover self-check.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProviderFailoverSelfCheckReport {
    pub status: String,
    pub mode: String,
    pub safety: ProviderFailoverSelfCheckSafety,
    pub primary_provider_id: String,
    pub primary_model_id: String,
    pub fallback_provider_id: String,
    pub fallback_model_id: String,
    pub resolved_provider_id: String,
    pub resolved_model_id: String,
    pub failover_count: u32,
    pub attempt_count: usize,
    pub attempts: Vec<ProviderAttemptSummary>,
}
const MAX_EMBEDDINGS_BATCH_SIZE: usize = 64;
const MAX_EMBEDDINGS_INPUT_BYTES: usize = 256 * 1024;
const MAX_SINGLE_EMBEDDING_INPUT_BYTES: usize = 64 * 1024;
const CODEX_TEXT_REPLAY_FALLBACK_MAX_BYTES: usize =
    palyra_model_providers::MAX_PROVIDER_TURN_TEXT_BYTES;
const DEFAULT_DETERMINISTIC_EMBEDDINGS_DIMS: usize = 64;
const DETERMINISTIC_TOOL_FIXTURE_ID: &str = "deterministic-provider-tool-call-v1";
const DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH: &str = "reports/deterministic-provider.md";
const DETERMINISTIC_TOOL_FIXTURE_WRITE_CALL_ID: &str = "deterministic-fixture-write";
const DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID: &str = "deterministic-fixture-read";
const DETERMINISTIC_TOOL_FIXTURE_REPORT: &str = "# Deterministic Provider Fixture\n\nfixture_id: deterministic-provider-tool-call-v1\nstatus: passed\nprovider: deterministic\n";

fn empty_health_probe_snapshot(
    state: &str,
    message: &str,
    source: &str,
) -> ProviderHealthProbeSnapshot {
    ProviderHealthProbeSnapshot {
        state: state.to_owned(),
        message: message.to_owned(),
        checked_at_unix_ms: None,
        latency_ms: None,
        source: source.to_owned(),
    }
}

fn empty_discovery_snapshot(source: &str) -> ProviderDiscoverySnapshot {
    ProviderDiscoverySnapshot {
        status: "unknown".to_owned(),
        checked_at_unix_ms: None,
        expires_at_unix_ms: None,
        discovered_model_ids: Vec::new(),
        source: source.to_owned(),
        message: None,
    }
}

fn empty_runtime_metrics_snapshot() -> ProviderRuntimeMetricsSnapshot {
    ProviderRuntimeMetricsSnapshot {
        request_count: 0,
        error_count: 0,
        error_rate_bps: 0,
        total_retry_attempts: 0,
        total_prompt_tokens: 0,
        total_completion_tokens: 0,
        avg_prompt_tokens_per_run: 0,
        avg_completion_tokens_per_run: 0,
        last_latency_ms: 0,
        avg_latency_ms: 0,
        max_latency_ms: 0,
        last_used_at_unix_ms: None,
        last_success_at_unix_ms: None,
        last_error_at_unix_ms: None,
        last_error: None,
    }
}

fn empty_response_cache_snapshot(enabled: bool) -> ProviderResponseCacheSnapshot {
    ProviderResponseCacheSnapshot { enabled, entry_count: 0, hit_count: 0, miss_count: 0 }
}

fn response_cache_enabled_from_config(config: &ModelProviderConfig) -> bool {
    config.normalized_registry().map(|registry| registry.response_cache_enabled).unwrap_or(true)
}

fn registry_snapshot_from_config(
    config: &ModelProviderConfig,
    runtime_status: &ProviderStatusSnapshot,
) -> ProviderRegistrySnapshot {
    let Ok(registry) = config.normalized_registry() else {
        return ProviderRegistrySnapshot {
            default_chat_model_id: None,
            default_embeddings_model_id: None,
            default_audio_transcription_model_id: None,
            failover_enabled: false,
            response_cache_enabled: false,
            providers: Vec::new(),
            credentials: Vec::new(),
            models: Vec::new(),
        };
    };

    let providers = registry
        .providers
        .iter()
        .map(|provider| ProviderRegistryProviderSnapshot {
            provider_id: provider.provider_id.clone(),
            credential_id: normalized_provider_credential_id(
                provider.provider_id.as_str(),
                provider.auth_profile_id.as_deref(),
                provider.credential_source,
            ),
            display_name: provider
                .display_name
                .clone()
                .unwrap_or_else(|| provider.kind.as_str().replace('_', " ")),
            kind: provider.kind.as_str().to_owned(),
            enabled: provider.enabled,
            endpoint_base_url: provider.base_url.clone(),
            auth_profile_id: provider.auth_profile_id.clone(),
            auth_profile_provider_kind: provider
                .auth_profile_provider_kind
                .map(|kind| kind.as_str().to_owned()),
            credential_source: provider.credential_source.map(|source| source.as_str().to_owned()),
            api_key_configured: provider.api_key.is_some(),
            retry_policy: ProviderRetryPolicySnapshot {
                max_retries: provider.max_retries,
                retry_backoff_ms: provider.retry_backoff_ms,
            },
            circuit_breaker: if provider.provider_id == runtime_status.provider_id {
                runtime_status.circuit_breaker.clone()
            } else {
                ProviderCircuitBreakerSnapshot {
                    failure_threshold: provider.circuit_breaker_failure_threshold,
                    cooldown_ms: provider.circuit_breaker_cooldown_ms,
                    consecutive_failures: 0,
                    open: false,
                }
            },
            runtime_metrics: if provider.provider_id == runtime_status.provider_id {
                runtime_status.runtime_metrics.clone()
            } else {
                empty_runtime_metrics_snapshot()
            },
            health: if provider.provider_id == runtime_status.provider_id {
                runtime_status.health.clone()
            } else if provider.api_key.is_some() || provider.auth_profile_id.is_some() {
                empty_health_probe_snapshot("ok", "provider configured", "registry")
            } else {
                empty_health_probe_snapshot(
                    "missing_auth",
                    "provider has no credential reference",
                    "registry",
                )
            },
            discovery: if provider.provider_id == runtime_status.provider_id {
                runtime_status.discovery.clone()
            } else {
                empty_discovery_snapshot("registry")
            },
        })
        .collect::<Vec<_>>();
    let credentials = registry
        .providers
        .iter()
        .map(|provider| {
            let runtime_metrics = if provider.provider_id == runtime_status.provider_id {
                runtime_status.runtime_metrics.clone()
            } else {
                empty_runtime_metrics_snapshot()
            };
            let health = if provider.provider_id == runtime_status.provider_id {
                runtime_status.health.clone()
            } else if provider.api_key.is_some() || provider.auth_profile_id.is_some() {
                empty_health_probe_snapshot("ok", "provider configured", "registry")
            } else {
                empty_health_probe_snapshot(
                    "missing_auth",
                    "provider has no credential reference",
                    "registry",
                )
            };
            ProviderRegistryCredentialSnapshot {
                credential_id: normalized_provider_credential_id(
                    provider.provider_id.as_str(),
                    provider.auth_profile_id.as_deref(),
                    provider.credential_source,
                ),
                provider_id: provider.provider_id.clone(),
                provider_kind: provider.kind.as_str().to_owned(),
                auth_profile_id: provider.auth_profile_id.clone(),
                auth_profile_provider_kind: provider
                    .auth_profile_provider_kind
                    .map(|kind| kind.as_str().to_owned()),
                credential_source: provider
                    .credential_source
                    .map(|source| source.as_str().to_owned()),
                availability_state: credential_availability_state(
                    health.state.as_str(),
                    runtime_metrics.last_error.as_ref(),
                )
                .to_owned(),
                capability_summary: credential_capability_summary(
                    registry
                        .models
                        .iter()
                        .filter(|model| model.provider_id == provider.provider_id && model.enabled),
                ),
                health,
                runtime_metrics,
            }
        })
        .collect::<Vec<_>>();

    let models = registry
        .models
        .iter()
        .map(|model| ProviderRegistryModelSnapshot {
            model_id: model.model_id.clone(),
            provider_id: model.provider_id.clone(),
            role: model.role.as_str().to_owned(),
            enabled: model.enabled,
            capabilities: model.capabilities.clone(),
        })
        .collect::<Vec<_>>();

    ProviderRegistrySnapshot {
        default_chat_model_id: registry.default_chat_model_id,
        default_embeddings_model_id: registry.default_embeddings_model_id,
        default_audio_transcription_model_id: registry.default_audio_transcription_model_id,
        failover_enabled: registry.failover_enabled,
        response_cache_enabled: registry.response_cache_enabled,
        providers,
        credentials,
        models,
    }
}

fn normalized_provider_credential_id(
    provider_id: &str,
    auth_profile_id: Option<&str>,
    credential_source: Option<ModelProviderCredentialSource>,
) -> String {
    if let Some(profile_id) = auth_profile_id {
        return format!("auth-profile:{provider_id}:{profile_id}");
    }
    match credential_source {
        Some(source) => format!("config:{provider_id}:{}", source.as_str()),
        None => format!("config:{provider_id}:unbound"),
    }
}

fn credential_capability_summary<'a>(
    models: impl IntoIterator<Item = &'a ProviderModelEntryConfig>,
) -> ProviderCredentialCapabilitySummary {
    let mut summary = ProviderCredentialCapabilitySummary {
        chat: false,
        embeddings: false,
        audio_transcription: false,
        vision: false,
        max_context_tokens: None,
    };
    for model in models {
        match model.role {
            ProviderModelRole::Chat => summary.chat = true,
            ProviderModelRole::Embeddings => summary.embeddings = true,
            ProviderModelRole::AudioTranscription => summary.audio_transcription = true,
        }
        summary.vision |= model.capabilities.vision;
        summary.max_context_tokens =
            summary.max_context_tokens.max(model.capabilities.max_context_tokens);
    }
    summary
}

fn credential_availability_state(
    health_state: &str,
    last_error: Option<&ProviderFailureSnapshot>,
) -> &'static str {
    if let Some(last_error) = last_error {
        return match last_error.class.as_str() {
            "auth_invalid" => "auth_invalid",
            "auth_expired" => "auth_expired",
            "permission_denied" => "permission_denied",
            "rate_limit" | "rate_limited" => "rate_limited",
            "quota" | "quota_exceeded" => "quota_exceeded",
            "context_overflow" | "context_window_exceeded" | "content_policy_blocked" => {
                "available"
            }
            "network_unavailable"
            | "provider_unavailable"
            | "provider_timeout"
            | "transient_upstream"
            | "schema_rejected"
            | "bad_tool_arguments"
            | "truncated_tool_arguments"
            | "malformed_response"
            | "malformed_stream"
            | "empty_output"
            | "premature_final"
            | "payload_too_large"
            | "permanent_upstream" => "provider_degraded",
            _ => "degraded",
        };
    }
    match health_state {
        "ok" | "static" => "available",
        "missing_auth" => "missing_auth",
        "degraded" => "degraded",
        _ => "unknown",
    }
}

fn provider_effective_health_state(
    health: &ProviderHealthProbeSnapshot,
    metrics: &ProviderRuntimeMetricsSnapshot,
    circuit: &ProviderCircuitBreakerSnapshot,
) -> &'static str {
    if circuit.open {
        return "cooling_down";
    }
    if let Some(last_error) = metrics.last_error.as_ref() {
        return match last_error.class.as_str() {
            "auth_invalid" | "auth_expired" | "permission_denied" | "quota" | "quota_exceeded" => {
                "unavailable"
            }
            "rate_limit"
            | "rate_limited"
            | "network_unavailable"
            | "provider_unavailable"
            | "provider_timeout"
            | "transient_upstream"
            | "context_overflow"
            | "context_window_exceeded"
            | "schema_rejected"
            | "bad_tool_arguments"
            | "truncated_tool_arguments"
            | "malformed_response"
            | "malformed_stream"
            | "empty_output"
            | "premature_final"
            | "payload_too_large"
            | "permanent_upstream" => "degraded",
            _ => "degraded",
        };
    }
    match health.state.as_str() {
        "ok" | "static" => "healthy",
        "missing_auth" => "unavailable",
        "degraded" => "degraded",
        "cooling_down" => "cooling_down",
        "unavailable" => "unavailable",
        _ => "unknown",
    }
}

fn provider_route_reason(
    selected: bool,
    capability_state: &str,
    health_state: &str,
    failover_enabled: bool,
) -> String {
    if selected && capability_state == "eligible" && health_state != "unavailable" {
        return "selected_default".to_owned();
    }
    if capability_state != "eligible" {
        return capability_state.to_owned();
    }
    match health_state {
        "cooling_down" => "circuit_open".to_owned(),
        "unavailable" => "provider_unavailable".to_owned(),
        _ if failover_enabled => "failover_candidate".to_owned(),
        _ => "available_failover_disabled".to_owned(),
    }
}

/// Runtime contract every chat-capable model backend implements.
///
/// Implementations own their retry, circuit-breaker, and metrics policies;
/// callers see only the terminal [`ProviderResponse`]/[`ProviderError`].
/// Methods return boxed futures so the trait stays object-safe behind
/// `Arc<dyn ModelProvider>`.
pub trait ModelProvider: Send + Sync {
    /// Runs one chat completion turn, including any internal retries and the
    /// translation of provider output into uniform [`ProviderEvent`]s.
    ///
    /// # Errors
    /// Returns a [`ProviderError`] once the retry budget is exhausted or the
    /// request is rejected up front (missing credential, open circuit,
    /// unsupported capability).
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>;
    /// Transcribes an audio payload with the provider's transcription model.
    ///
    /// # Errors
    /// Returns a [`ProviderError`] when the backend does not support audio
    /// transcription or the upstream request ultimately fails.
    fn transcribe_audio<'a>(
        &'a self,
        request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>;
    /// Returns the current status snapshot for console/diagnostics surfaces.
    fn status_snapshot(&self) -> ProviderStatusSnapshot;
}

/// Runtime contract for text-embedding backends.
pub trait EmbeddingsProvider: Send + Sync {
    /// Embeds a validated batch of inputs, preserving input order.
    ///
    /// # Errors
    /// Returns [`ProviderError::InvalidEmbeddingsRequest`] for batches that
    /// violate size limits, and other [`ProviderError`] variants for
    /// credential or upstream failures.
    fn embed<'a>(
        &'a self,
        request: EmbeddingsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<EmbeddingsResponse, ProviderError>> + Send + 'a>>;
}

/// Builds the registry-backed model provider from validated configuration.
///
/// # Errors
/// Returns an error when the configuration fails validation (timeouts,
/// retry/circuit tuning, base URL network policy, or registry shape).
pub fn build_model_provider(config: &ModelProviderConfig) -> Result<Arc<dyn ModelProvider>> {
    validate_model_provider_config(config)?;
    Ok(Arc::new(RegistryBackedModelProvider::new(config.clone())?))
}

/// Runs an in-memory provider failover self-check without reading real config
/// or credentials.
///
/// # Errors
/// Returns an error when the synthetic runtime cannot be built or when the
/// primary missing-credential failure does not route to the deterministic
/// fallback exactly once.
pub(crate) async fn run_provider_failover_self_check() -> Result<ProviderFailoverSelfCheckReport> {
    let provider = build_model_provider(&provider_failover_self_check_config())
        .context("failed to build provider failover self-check runtime")?;
    let response = provider
        .complete(ProviderRequest::from_input_text(
            FAILOVER_SELF_CHECK_PROMPT.to_owned(),
            false,
            Vec::new(),
            None,
        ))
        .await
        .map_err(|error| {
            let envelope = error.envelope();
            anyhow::anyhow!(
                "provider failover self-check failed before fallback served request: {}",
                envelope.redacted_message
            )
        })?;
    ensure_provider_failover_self_check_response(&response)?;

    Ok(ProviderFailoverSelfCheckReport {
        status: "passed".to_owned(),
        mode: "in_memory_synthetic".to_owned(),
        safety: ProviderFailoverSelfCheckSafety {
            label: "no_real_config_no_real_credentials_no_network".to_owned(),
            uses_real_config: false,
            uses_real_credentials: false,
            performs_network_io: false,
        },
        primary_provider_id: FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID.to_owned(),
        primary_model_id: FAILOVER_SELF_CHECK_PRIMARY_MODEL_ID.to_owned(),
        fallback_provider_id: FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID.to_owned(),
        fallback_model_id: FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID.to_owned(),
        resolved_provider_id: response.provider_id.clone(),
        resolved_model_id: response.model_id.clone(),
        failover_count: response.failover_count,
        attempt_count: response.attempts.len(),
        attempts: response.attempts.clone(),
    })
}

fn provider_failover_self_check_config() -> ModelProviderConfig {
    ModelProviderConfig {
        kind: ModelProviderKind::Deterministic,
        registry: ModelProviderRegistryConfig {
            providers: vec![
                ProviderRegistryEntryConfig {
                    provider_id: FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID.to_owned(),
                    display_name: Some("E2E failing OpenAI-compatible provider".to_owned()),
                    kind: ModelProviderKind::OpenAiCompatible,
                    base_url: None,
                    allow_private_base_url: false,
                    enabled: true,
                    auth_profile_id: None,
                    auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Openai),
                    api_key: None,
                    api_key_secret_ref: None,
                    api_key_vault_ref: None,
                    credential_source: None,
                    request_timeout_ms: 500,
                    max_retries: 0,
                    retry_backoff_ms: 1,
                    circuit_breaker_failure_threshold: 1,
                    circuit_breaker_cooldown_ms: 60_000,
                },
                ProviderRegistryEntryConfig {
                    provider_id: FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID.to_owned(),
                    display_name: Some("E2E deterministic fallback provider".to_owned()),
                    kind: ModelProviderKind::Deterministic,
                    base_url: None,
                    allow_private_base_url: false,
                    enabled: true,
                    auth_profile_id: None,
                    auth_profile_provider_kind: None,
                    api_key: None,
                    api_key_secret_ref: None,
                    api_key_vault_ref: None,
                    credential_source: None,
                    request_timeout_ms: 500,
                    max_retries: 0,
                    retry_backoff_ms: 1,
                    circuit_breaker_failure_threshold: 1,
                    circuit_breaker_cooldown_ms: 60_000,
                },
            ],
            models: vec![
                ProviderModelEntryConfig {
                    model_id: FAILOVER_SELF_CHECK_PRIMARY_MODEL_ID.to_owned(),
                    provider_id: FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID.to_owned(),
                    role: ProviderModelRole::Chat,
                    enabled: true,
                    metadata_source: ProviderMetadataSource::Static,
                    operator_override: false,
                    capabilities: capability_defaults_for_kind(
                        ModelProviderKind::OpenAiCompatible,
                        ProviderModelRole::Chat,
                    ),
                },
                ProviderModelEntryConfig {
                    model_id: FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID.to_owned(),
                    provider_id: FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID.to_owned(),
                    role: ProviderModelRole::Chat,
                    enabled: true,
                    metadata_source: ProviderMetadataSource::Static,
                    operator_override: false,
                    capabilities: capability_defaults_for_kind(
                        ModelProviderKind::Deterministic,
                        ProviderModelRole::Chat,
                    ),
                },
            ],
            default_chat_model_id: Some(FAILOVER_SELF_CHECK_PRIMARY_MODEL_ID.to_owned()),
            failover_enabled: true,
            response_cache_enabled: false,
            response_cache_ttl_ms: 1_000,
            response_cache_max_entries: 1,
            ..ModelProviderRegistryConfig::default()
        },
        request_timeout_ms: 500,
        max_retries: 0,
        retry_backoff_ms: 1,
        circuit_breaker_failure_threshold: 1,
        circuit_breaker_cooldown_ms: 60_000,
        ..ModelProviderConfig::default()
    }
}

fn ensure_provider_failover_self_check_response(response: &ProviderResponse) -> Result<()> {
    let first_attempt =
        response.attempts.first().context("provider failover self-check recorded no attempts")?;
    let last_attempt =
        response.attempts.last().context("provider failover self-check recorded no attempts")?;
    let expected = response.provider_id == FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID
        && response.model_id == FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID
        && response.failover_count == 1
        && response.attempts.len() == 2
        && first_attempt.provider_id == FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID
        && first_attempt.model_id == FAILOVER_SELF_CHECK_PRIMARY_MODEL_ID
        && first_attempt.outcome == "error"
        && first_attempt.retryable
        && last_attempt.provider_id == FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID
        && last_attempt.model_id == FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID
        && last_attempt.outcome == "failover_success";
    if !expected {
        anyhow::bail!(
            "provider failover self-check observed unexpected routing: resolved_provider={} resolved_model={} failover_count={} attempts={}",
            response.provider_id,
            response.model_id,
            response.failover_count,
            response.attempts.len()
        );
    }
    Ok(())
}

/// Builds the embeddings provider matching `config.kind`.
///
/// # Errors
/// Returns an error when configuration validation fails, when the
/// OpenAI-compatible backend lacks an embeddings model, or when the kind
/// (Anthropic) exposes no embeddings adapter.
pub fn build_embeddings_provider(
    config: &ModelProviderConfig,
) -> Result<Arc<dyn EmbeddingsProvider>> {
    validate_model_provider_config(config)?;

    match config.kind {
        ModelProviderKind::Deterministic => {
            Ok(Arc::new(DeterministicEmbeddingsProvider::new(config.clone())))
        }
        ModelProviderKind::OpenAiCompatible => {
            Ok(Arc::new(OpenAiCompatibleEmbeddingsProvider::new(config)?))
        }
        ModelProviderKind::Anthropic => Err(anyhow::anyhow!(
            "anthropic provider does not expose embeddings through the built-in adapter"
        )),
    }
}

/// One instantiated provider backend paired with its registry entry.
struct RegistryProviderRuntime {
    entry: ProviderRegistryEntryConfig,
    provider: Arc<dyn ModelProvider>,
}

fn provider_attempt_index(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

fn registry_provider_credential_id(entry: &ProviderRegistryEntryConfig) -> String {
    normalized_provider_credential_id(
        entry.provider_id.as_str(),
        entry.auth_profile_id.as_deref(),
        entry.credential_source,
    )
}

fn provider_attempt_summary(
    provider_id: String,
    model_id: String,
    outcome: &str,
    retryable: bool,
    served_from_cache: bool,
    reason_code: Option<String>,
    state: Option<ProviderAttemptState>,
) -> ProviderAttemptSummary {
    ProviderAttemptSummary {
        provider_id,
        model_id,
        outcome: outcome.to_owned(),
        retryable,
        served_from_cache,
        reason_code,
        state,
    }
}

fn provider_attempt_success_state(
    attempt_index: usize,
    model: &ProviderModelEntryConfig,
    runtime: &RegistryProviderRuntime,
    response: &ProviderResponse,
    final_disposition: &str,
    served_from_cache: bool,
) -> ProviderAttemptState {
    let total_tokens = response.prompt_tokens.saturating_add(response.completion_tokens);
    ProviderAttemptState {
        attempt_index: provider_attempt_index(attempt_index),
        provider_profile_id: runtime.entry.provider_id.clone(),
        credential_id: registry_provider_credential_id(&runtime.entry),
        model_id: model.model_id.clone(),
        error_class: None,
        retry_after_ms: None,
        cooldown_until_unix_ms: None,
        prompt_tokens: response.prompt_tokens,
        output_tokens: response.completion_tokens,
        cache_tokens: if served_from_cache { total_tokens } else { 0 },
        estimated_cost_microusd: None,
        final_disposition: final_disposition.to_owned(),
        repair_hint: None,
    }
}

fn provider_attempt_error_state(
    attempt_index: usize,
    model: &ProviderModelEntryConfig,
    runtime: &RegistryProviderRuntime,
    error: &ProviderError,
    observed_at_unix_ms: i64,
) -> ProviderAttemptState {
    let failure = error.failure_snapshot();
    let retry_after_ms = failure.recovery.retry_after_ms;
    ProviderAttemptState {
        attempt_index: provider_attempt_index(attempt_index),
        provider_profile_id: runtime.entry.provider_id.clone(),
        credential_id: registry_provider_credential_id(&runtime.entry),
        model_id: model.model_id.clone(),
        error_class: Some(failure.class.as_str().to_owned()),
        retry_after_ms,
        cooldown_until_unix_ms: retry_after_ms.map(|value| {
            observed_at_unix_ms.saturating_add(i64::try_from(value).unwrap_or(i64::MAX))
        }),
        prompt_tokens: 0,
        output_tokens: 0,
        cache_tokens: 0,
        estimated_cost_microusd: None,
        final_disposition: provider_attempt_error_disposition(&failure).to_owned(),
        repair_hint: provider_attempt_repair_hint(&failure),
    }
}

fn provider_attempt_skipped_state(
    attempt_index: usize,
    model: &ProviderModelEntryConfig,
    runtime: &RegistryProviderRuntime,
    blocked_by: &ProviderAttemptState,
) -> ProviderAttemptState {
    ProviderAttemptState {
        attempt_index: provider_attempt_index(attempt_index),
        provider_profile_id: runtime.entry.provider_id.clone(),
        credential_id: registry_provider_credential_id(&runtime.entry),
        model_id: model.model_id.clone(),
        error_class: blocked_by.error_class.clone(),
        retry_after_ms: blocked_by.retry_after_ms,
        cooldown_until_unix_ms: blocked_by.cooldown_until_unix_ms,
        prompt_tokens: 0,
        output_tokens: 0,
        cache_tokens: 0,
        estimated_cost_microusd: None,
        final_disposition: "skipped_credential_cooldown".to_owned(),
        repair_hint: blocked_by.repair_hint.clone(),
    }
}

fn rebind_provider_attempts(
    attempts: &mut [ProviderAttemptSummary],
    attempt_offset: usize,
    model: &ProviderModelEntryConfig,
    runtime: &RegistryProviderRuntime,
) {
    let credential_id = registry_provider_credential_id(&runtime.entry);
    for (local_index, attempt) in attempts.iter_mut().enumerate() {
        attempt.provider_id = model.provider_id.clone();
        attempt.model_id = model.model_id.clone();
        if let Some(state) = attempt.state.as_mut() {
            state.attempt_index =
                provider_attempt_index(attempt_offset.saturating_add(local_index));
            state.provider_profile_id = model.provider_id.clone();
            state.credential_id.clone_from(&credential_id);
            state.model_id = model.model_id.clone();
        }
    }
}

fn provider_attempt_error_disposition(failure: &ProviderFailureSnapshot) -> &'static str {
    match failure.recovery.category.as_str() {
        "auth" => "credential_refresh_required",
        "rate_limit" => "retry_after_required",
        "quota" => "quota_action_required",
        "transient" => "retryable_failure",
        _ => "failed",
    }
}

fn provider_attempt_repair_hint(failure: &ProviderFailureSnapshot) -> Option<String> {
    match failure.recovery.category.as_str() {
        "auth" => Some(
            "refresh the provider credential or select another credential before retrying"
                .to_owned(),
        ),
        "rate_limit" => Some("respect retry_after before retrying this credential".to_owned()),
        "quota" => Some("increase quota or route to another provider profile".to_owned()),
        _ => None,
    }
}

fn provider_failure_blocks_credential(failure: &ProviderFailureSnapshot) -> bool {
    matches!(failure.recovery.category.as_str(), "auth" | "rate_limit" | "quota")
}

#[derive(Debug, Clone)]
struct CachedProviderResponse {
    inserted_seq: u64,
    expires_at: Instant,
    response: ProviderResponse,
}

#[derive(Debug, Default)]
struct ProviderResponseCacheState {
    entries: HashMap<String, CachedProviderResponse>,
    next_seq: u64,
    hit_count: u64,
    miss_count: u64,
}

/// Routing facade over all configured provider runtimes: per-request
/// candidate ordering, cross-provider failover, a TTL-bounded response
/// cache, and aggregate runtime metrics.
struct RegistryBackedModelProvider {
    config: ModelProviderConfig,
    registry: ModelProviderRegistryConfig,
    providers: HashMap<String, RegistryProviderRuntime>,
    models: HashMap<String, ProviderModelEntryConfig>,
    response_cache: Mutex<ProviderResponseCacheState>,
    runtime_metrics: Mutex<ProviderRuntimeMetrics>,
}

impl RegistryBackedModelProvider {
    fn new(config: ModelProviderConfig) -> Result<Self> {
        let registry = config.normalized_registry()?;
        let mut providers = HashMap::new();
        let mut default_models_by_provider = HashMap::<String, String>::new();
        for model in &registry.models {
            if model.role == ProviderModelRole::Chat && model.enabled {
                default_models_by_provider
                    .entry(model.provider_id.clone())
                    .or_insert_with(|| model.model_id.clone());
            }
        }
        for entry in &registry.providers {
            let provider = build_registry_provider_runtime(
                &config,
                &registry,
                entry,
                default_models_by_provider.get(entry.provider_id.as_str()).cloned(),
            )?;
            providers.insert(
                entry.provider_id.clone(),
                RegistryProviderRuntime { entry: entry.clone(), provider },
            );
        }
        let models = registry
            .models
            .iter()
            .cloned()
            .map(|model| (model.model_id.clone(), model))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            config,
            registry,
            providers,
            models,
            response_cache: Mutex::new(ProviderResponseCacheState::default()),
            runtime_metrics: Mutex::new(ProviderRuntimeMetrics::default()),
        })
    }

    fn record_runtime_metrics(
        &self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
        failure: Option<ProviderFailureSnapshot>,
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms, failure);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }

    fn response_cache_snapshot(&self) -> ProviderResponseCacheSnapshot {
        let cache = match self.response_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        ProviderResponseCacheSnapshot {
            enabled: self.registry.response_cache_enabled,
            entry_count: cache.entries.len(),
            hit_count: cache.hit_count,
            miss_count: cache.miss_count,
        }
    }

    fn compatible_chat_models(&self, request: &ProviderRequest) -> Vec<&ProviderModelEntryConfig> {
        self.registry
            .models
            .iter()
            .filter(|model| {
                model.enabled
                    && model.role == ProviderModelRole::Chat
                    && self
                        .providers
                        .get(model.provider_id.as_str())
                        .is_some_and(|provider| provider.entry.enabled)
                    && (!request.json_mode || model.capabilities.json_mode)
                    && (!provider_request_has_vision(request) || model.capabilities.vision)
            })
            .collect()
    }

    fn candidate_order<'a>(
        &'a self,
        request: &ProviderRequest,
    ) -> Result<Vec<&'a ProviderModelEntryConfig>, ProviderError> {
        let compatible = self.compatible_chat_models(request);
        if compatible.is_empty() {
            if provider_request_has_vision(request) {
                return Err(ProviderError::VisionUnsupported {
                    provider: self
                        .registry
                        .default_chat_model_id
                        .clone()
                        .unwrap_or_else(|| self.config.kind.as_str().to_owned()),
                });
            }
            return Err(ProviderError::RequestFailed {
                message: "no enabled chat model matches the requested capability envelope"
                    .to_owned(),
                retryable: false,
                retry_count: 0,
                classification: failover_provider_classification(
                    "registry_no_enabled_chat_model_matches_request",
                ),
            });
        }

        let requested_model_id = request
            .model_override
            .as_deref()
            .or(self.registry.default_chat_model_id.as_deref())
            .ok_or_else(|| ProviderError::RequestFailed {
                message: "provider registry does not define a default chat model".to_owned(),
                retryable: false,
                retry_count: 0,
                classification: fail_closed_provider_classification(
                    "registry_default_chat_model_missing",
                ),
            })?;
        let primary = compatible
            .iter()
            .find(|model| model.model_id == requested_model_id)
            .copied()
            .ok_or_else(|| ProviderError::RequestFailed {
                message: format!("requested chat model '{requested_model_id}' is unavailable"),
                retryable: false,
                retry_count: 0,
                classification: failover_provider_classification(
                    "registry_requested_chat_model_unavailable",
                ),
            })?;

        // Deterministic fallback order: cheapest first, then lowest latency,
        // then model id as a stable tiebreaker.
        let mut fallbacks = compatible
            .into_iter()
            .filter(|model| model.model_id != primary.model_id)
            .collect::<Vec<_>>();
        fallbacks.sort_by(|left, right| {
            fallback_cost_rank(left.capabilities.cost_tier.as_str())
                .cmp(&fallback_cost_rank(right.capabilities.cost_tier.as_str()))
                .then(
                    fallback_latency_rank(left.capabilities.latency_tier.as_str())
                        .cmp(&fallback_latency_rank(right.capabilities.latency_tier.as_str())),
                )
                .then_with(|| left.model_id.cmp(&right.model_id))
        });
        let mut ordered = vec![primary];
        // An explicit model override pins the request to that model: no
        // failover. Fallbacks on the primary's own provider are skipped too,
        // since a provider-level failure would hit them identically.
        if self.registry.failover_enabled && request.model_override.is_none() {
            ordered.extend(
                fallbacks.into_iter().filter(|model| model.provider_id != primary.provider_id),
            );
        }
        Ok(ordered)
    }

    // The key hashes only request fields that affect the model's answer.
    // Volatile tool catalog audit metadata (snapshot id, hash, timestamp) is
    // stripped first so re-issued identical requests still hit the cache.
    fn response_cache_key(
        &self,
        request: &ProviderRequest,
        model: &ProviderModelEntryConfig,
    ) -> String {
        let tool_catalog_snapshot = request
            .tool_catalog_snapshot
            .as_ref()
            .map(stable_tool_catalog_snapshot_for_response_cache);
        let payload = json!({
            "schema_version": 1,
            "provider_id": model.provider_id.as_str(),
            "model_id": model.model_id.as_str(),
            "input_text": request.input_text.as_str(),
            "json_mode": request.json_mode,
            "model_override": request.model_override.as_deref(),
            "messages": &request.messages,
            "tool_catalog_snapshot": tool_catalog_snapshot.as_ref(),
            "instruction_hash": request.instruction_hash.as_deref(),
            "context_trace_id": request.context_trace_id.as_deref(),
            "budget_profile": request.budget_profile.as_deref(),
            "max_output_tokens": request.max_output_tokens,
            "vision_inputs": &request.vision_inputs,
            "prompt_segments": &request.prompt_segments,
            "prompt_cache_policy": &request.prompt_cache_policy,
            "prompt_cache_report": request.prompt_cache_report.as_ref().map(|report| json!({
                "eligible_bytes": report.eligible_bytes,
                "invalidated_bytes": report.invalidated_bytes,
                "invalidation_reasons": report.invalidation_reasons,
                "requested_strategy": report.requested_strategy.as_str(),
                "applied_strategy": report.applied_strategy.as_str(),
                "breakpoint_count": report.breakpoint_count,
                "cacheable_tokens": report.cacheable_tokens,
                "prompt_cache_epoch": report.prompt_cache_epoch,
                "stable_prefix_hash": report.stable_prefix_hash.as_deref(),
                "cache_scope_hash": report.cache_scope_hash.as_deref(),
                "tool_catalog_hash": report.tool_catalog_hash.as_deref(),
                "memory_snapshot_hash": report.memory_snapshot_hash.as_deref(),
                "provider_cache_strategy": report.provider_cache_strategy.as_str(),
            })),
        });
        crate::sha256_hex(
            serde_json::to_vec(&payload).unwrap_or_else(|_| b"null".to_vec()).as_slice(),
        )
    }

    fn lookup_cached_response(
        &self,
        cache_key: &str,
        model: &ProviderModelEntryConfig,
        runtime: &RegistryProviderRuntime,
    ) -> Option<ProviderResponse> {
        if !self.registry.response_cache_enabled {
            return None;
        }
        let mut cache = match self.response_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(entry) = cache.entries.get(cache_key).cloned() else {
            cache.miss_count = cache.miss_count.saturating_add(1);
            return None;
        };
        if entry.expires_at <= Instant::now() {
            cache.entries.remove(cache_key);
            cache.miss_count = cache.miss_count.saturating_add(1);
            return None;
        }
        cache.hit_count = cache.hit_count.saturating_add(1);
        // Rewrite identity and attempt metadata: a cache hit must report the
        // current candidate and a clean cache_hit attempt trail, not the
        // retries/failovers of the original upstream call.
        let mut response = entry.response;
        response.provider_id = model.provider_id.clone();
        response.model_id = model.model_id.clone();
        response.retry_count = 0;
        response.served_from_cache = true;
        response.failover_count = 0;
        response.attempts = vec![provider_attempt_summary(
            model.provider_id.clone(),
            model.model_id.clone(),
            "cache_hit",
            false,
            true,
            Some("response_cache_hit".to_owned()),
            Some(provider_attempt_success_state(0, model, runtime, &response, "cache_hit", true)),
        )];
        Some(response)
    }

    fn insert_cached_response(&self, cache_key: String, response: &ProviderResponse) {
        // Responses carrying tool proposals are never cached: replaying one
        // would skip approval flows and re-execute side effects.
        if !self.registry.response_cache_enabled
            || response.served_from_cache
            || response
                .events
                .iter()
                .any(|event| matches!(event, ProviderEvent::ToolProposal { .. }))
        {
            return;
        }
        let mut cache = match self.response_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cache.next_seq = cache.next_seq.saturating_add(1);
        let inserted_seq = cache.next_seq;
        cache.entries.insert(
            cache_key,
            CachedProviderResponse {
                inserted_seq,
                expires_at: Instant::now()
                    + Duration::from_millis(self.registry.response_cache_ttl_ms),
                response: response.clone(),
            },
        );
        // FIFO eviction by insertion sequence: entries are short-lived (TTL
        // in the tens of seconds), so recency tracking would add complexity
        // without measurable benefit.
        while cache.entries.len() > self.registry.response_cache_max_entries {
            let Some(oldest_key) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_seq)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            cache.entries.remove(oldest_key.as_str());
        }
    }

    fn provider_statuses(&self) -> HashMap<String, ProviderStatusSnapshot> {
        self.providers
            .iter()
            .map(|(provider_id, runtime)| {
                let mut snapshot = runtime.provider.status_snapshot();
                snapshot.provider_id = provider_id.clone();
                if let Some(default_model_id) =
                    default_model_for_provider(&self.registry, provider_id.as_str())
                {
                    snapshot.model_id = Some(default_model_id.clone());
                    match runtime.entry.kind {
                        ModelProviderKind::OpenAiCompatible => {
                            snapshot.openai_model = Some(default_model_id);
                        }
                        ModelProviderKind::Anthropic => {
                            snapshot.anthropic_model = Some(default_model_id);
                        }
                        ModelProviderKind::Deterministic => {}
                    }
                } else {
                    snapshot.model_id = None;
                    snapshot.openai_model = None;
                    snapshot.anthropic_model = None;
                    snapshot.discovery.discovered_model_ids.clear();
                }
                if snapshot.discovery.discovered_model_ids.is_empty() {
                    snapshot.discovery.discovered_model_ids = self
                        .registry
                        .models
                        .iter()
                        .filter(|model| model.provider_id == *provider_id)
                        .map(|model| model.model_id.clone())
                        .collect();
                    snapshot.discovery.source = "registry".to_owned();
                    if snapshot.discovery.discovered_model_ids.is_empty() {
                        snapshot.discovery.status = "pending".to_owned();
                        snapshot.discovery.message =
                            Some("no provider-discovered models are configured yet".to_owned());
                    } else {
                        snapshot.discovery.status = "ok".to_owned();
                        snapshot.discovery.message =
                            Some("serving configured registry models".to_owned());
                    }
                }
                (provider_id.clone(), snapshot)
            })
            .collect()
    }

    fn route_selection_trace(
        &self,
        statuses: &HashMap<String, ProviderStatusSnapshot>,
        default_model_id: Option<&str>,
    ) -> ProviderRouteSelectionTrace {
        let selected_model = default_model_id.and_then(|model_id| self.models.get(model_id));
        let selected_provider_id = selected_model.map(|model| model.provider_id.clone());
        let mut candidates = self
            .registry
            .models
            .iter()
            .filter(|model| model.role == ProviderModelRole::Chat)
            .map(|model| {
                let provider = self.providers.get(model.provider_id.as_str());
                let provider_entry = provider.map(|runtime| &runtime.entry);
                let runtime_status = statuses.get(model.provider_id.as_str());
                let circuit = runtime_status
                    .map(|snapshot| snapshot.circuit_breaker.clone())
                    .unwrap_or_else(|| {
                        provider_entry.map_or(
                            ProviderCircuitBreakerSnapshot {
                                failure_threshold: self.config.circuit_breaker_failure_threshold,
                                cooldown_ms: self.config.circuit_breaker_cooldown_ms,
                                consecutive_failures: 0,
                                open: false,
                            },
                            |entry| ProviderCircuitBreakerSnapshot {
                                failure_threshold: entry.circuit_breaker_failure_threshold,
                                cooldown_ms: entry.circuit_breaker_cooldown_ms,
                                consecutive_failures: 0,
                                open: false,
                            },
                        )
                    });
                let metrics = runtime_status
                    .map(|snapshot| snapshot.runtime_metrics.clone())
                    .unwrap_or_else(empty_runtime_metrics_snapshot);
                let health =
                    runtime_status.map(|snapshot| snapshot.health.clone()).unwrap_or_else(|| {
                        empty_health_probe_snapshot(
                            "unknown",
                            "provider has not been probed yet",
                            "registry",
                        )
                    });
                let capability_state = if !model.enabled {
                    "model_disabled"
                } else if !provider_entry.is_some_and(|entry| entry.enabled) {
                    "provider_disabled"
                } else {
                    "eligible"
                };
                let health_state =
                    provider_effective_health_state(&health, &metrics, &circuit).to_owned();
                let selected = default_model_id.is_some_and(|model_id| model_id == model.model_id);
                ProviderRouteCandidateTrace {
                    provider_id: model.provider_id.clone(),
                    credential_id: provider_entry.map_or_else(
                        || {
                            normalized_provider_credential_id(
                                model.provider_id.as_str(),
                                None,
                                None,
                            )
                        },
                        |entry| {
                            normalized_provider_credential_id(
                                entry.provider_id.as_str(),
                                entry.auth_profile_id.as_deref(),
                                entry.credential_source,
                            )
                        },
                    ),
                    model_id: model.model_id.clone(),
                    role: model.role.as_str().to_owned(),
                    capability_state: capability_state.to_owned(),
                    reason_code: provider_route_reason(
                        selected,
                        capability_state,
                        health_state.as_str(),
                        self.registry.failover_enabled,
                    ),
                    health_state,
                    selected,
                }
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .selected
                .cmp(&left.selected)
                .then_with(|| left.provider_id.cmp(&right.provider_id))
                .then_with(|| left.model_id.cmp(&right.model_id))
        });
        ProviderRouteSelectionTrace {
            default_model_id: default_model_id.map(str::to_owned),
            failover_enabled: self.registry.failover_enabled,
            generated_at_unix_ms: current_unix_ms().unwrap_or_default(),
            selected_provider_id,
            selected_model_id: selected_model.map(|model| model.model_id.clone()),
            candidates,
        }
    }
}

impl ModelProvider for RegistryBackedModelProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let started_at = Instant::now();
            let candidates = match self.candidate_order(&request) {
                Ok(candidates) => candidates,
                Err(error) => {
                    self.record_runtime_metrics(
                        true,
                        0,
                        0,
                        error.retry_count(),
                        elapsed_millis_since(started_at),
                        Some(error.failure_snapshot()),
                    );
                    return Err(error);
                }
            };
            let mut attempts = Vec::new();
            let mut failover_count = 0_u32;
            let mut last_error = None;
            let mut blocked_credentials = HashMap::<String, ProviderAttemptState>::new();

            for (index, model) in candidates.iter().enumerate() {
                // Registry normalization guarantees every model references a
                // known provider; a miss here means internal state corruption.
                let runtime = self
                    .providers
                    .get(model.provider_id.as_str())
                    .ok_or(ProviderError::StatePoisoned)?;
                let credential_id = registry_provider_credential_id(&runtime.entry);
                if let Some(blocked_by) = blocked_credentials.get(credential_id.as_str()) {
                    let state = provider_attempt_skipped_state(index, model, runtime, blocked_by);
                    attempts.push(provider_attempt_summary(
                        model.provider_id.clone(),
                        model.model_id.clone(),
                        "skipped",
                        false,
                        false,
                        Some("credential_cooldown".to_owned()),
                        Some(state),
                    ));
                    if index + 1 < candidates.len() {
                        failover_count = failover_count.saturating_add(1);
                        continue;
                    }
                    break;
                }
                let cache_key = self.response_cache_key(&request, model);
                if let Some(mut cached) =
                    self.lookup_cached_response(cache_key.as_str(), model, runtime)
                {
                    cached.failover_count = failover_count;
                    rebind_provider_attempts(&mut cached.attempts, attempts.len(), model, runtime);
                    cached.attempts = attempts.into_iter().chain(cached.attempts).collect();
                    self.record_runtime_metrics(
                        false,
                        cached.prompt_tokens,
                        cached.completion_tokens,
                        cached.retry_count,
                        elapsed_millis_since(started_at),
                        None,
                    );
                    return Ok(cached);
                }

                let mut provider_request = request.clone();
                provider_request.model_override = Some(model.model_id.clone());
                match runtime.provider.complete(provider_request).await {
                    Ok(mut response) => {
                        response.provider_id = model.provider_id.clone();
                        response.model_id = model.model_id.clone();
                        response.served_from_cache = false;
                        response.failover_count = failover_count;
                        let outcome = if index == 0 { "success" } else { "failover_success" };
                        let mut provider_attempts = std::mem::take(&mut response.attempts);
                        if provider_attempts.len() > 1 {
                            let attempt_offset = attempts.len();
                            rebind_provider_attempts(
                                &mut provider_attempts,
                                attempt_offset,
                                model,
                                runtime,
                            );
                            let final_attempt_index = attempt_offset
                                .saturating_add(provider_attempts.len())
                                .saturating_sub(1);
                            if let Some(final_attempt) = provider_attempts.last_mut() {
                                final_attempt.outcome = outcome.to_owned();
                                final_attempt.reason_code =
                                    (index > 0).then(|| "failover_success".to_owned());
                                let mut state = provider_attempt_success_state(
                                    index, model, runtime, &response, outcome, false,
                                );
                                state.attempt_index = provider_attempt_index(final_attempt_index);
                                final_attempt.state = Some(state);
                            }
                            attempts.extend(provider_attempts);
                        } else {
                            attempts.push(provider_attempt_summary(
                                model.provider_id.clone(),
                                model.model_id.clone(),
                                outcome,
                                false,
                                false,
                                (index > 0).then(|| "failover_success".to_owned()),
                                Some(provider_attempt_success_state(
                                    index, model, runtime, &response, outcome, false,
                                )),
                            ));
                        }
                        response.attempts = attempts;
                        self.insert_cached_response(cache_key, &response);
                        self.record_runtime_metrics(
                            false,
                            response.prompt_tokens,
                            response.completion_tokens,
                            response.retry_count,
                            elapsed_millis_since(started_at),
                            None,
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        // Missing-credential errors count as retryable in the
                        // attempt record: the failure is provider-local and a
                        // failover candidate with its own credential can still
                        // serve the request.
                        let retryable = matches!(
                            error,
                            ProviderError::CircuitOpen { .. }
                                | ProviderError::RequestFailed { retryable: true, .. }
                                | ProviderError::MissingApiKey
                                | ProviderError::MissingAnthropicApiKey
                        );
                        let failure = error.failure_snapshot();
                        let state = provider_attempt_error_state(
                            index,
                            model,
                            runtime,
                            &error,
                            current_unix_ms().unwrap_or_default(),
                        );
                        let blocks_credential = provider_failure_blocks_credential(&failure);
                        attempts.push(provider_attempt_summary(
                            model.provider_id.clone(),
                            model.model_id.clone(),
                            "error",
                            retryable,
                            false,
                            Some(error.envelope().provider_trace_ref.unwrap_or_else(|| {
                                error.classification().class.as_str().to_owned()
                            })),
                            Some(state.clone()),
                        ));
                        if blocks_credential {
                            blocked_credentials.insert(state.credential_id.clone(), state);
                        }
                        last_error = Some(error);
                        if index + 1 < candidates.len() {
                            failover_count = failover_count.saturating_add(1);
                            continue;
                        }
                    }
                }
            }

            let error = last_error.unwrap_or(ProviderError::StatePoisoned);
            self.record_runtime_metrics(
                true,
                0,
                0,
                error.retry_count(),
                elapsed_millis_since(started_at),
                Some(error.failure_snapshot()),
            );
            Err(error)
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async move {
            let model_id =
                self.registry.default_audio_transcription_model_id.clone().ok_or_else(|| {
                    ProviderError::RequestFailed {
                        message: "provider registry does not define an audio transcription model"
                            .to_owned(),
                        retryable: false,
                        retry_count: 0,
                        classification: fail_closed_provider_classification(
                            "registry_default_audio_transcription_model_missing",
                        ),
                    }
                })?;
            let model = self.models.get(model_id.as_str()).ok_or(ProviderError::StatePoisoned)?;
            let runtime = self
                .providers
                .get(model.provider_id.as_str())
                .ok_or(ProviderError::StatePoisoned)?;
            runtime.provider.transcribe_audio(request).await
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let statuses = self.provider_statuses();
        let default_model_id = self.registry.default_chat_model_id.clone().or_else(|| {
            self.registry
                .models
                .iter()
                .find(|model| model.role == ProviderModelRole::Chat)
                .map(|model| model.model_id.clone())
        });
        let default_model =
            default_model_id.as_ref().and_then(|model_id| self.models.get(model_id.as_str()));
        let default_provider_id = default_model
            .map(|model| model.provider_id.clone())
            .or_else(|| {
                self.registry.providers.first().map(|provider| provider.provider_id.clone())
            })
            .unwrap_or_else(|| "unknown".to_owned());
        let default_provider_entry = self
            .registry
            .providers
            .iter()
            .find(|provider| provider.provider_id == default_provider_id);
        let default_provider_status = statuses.get(default_provider_id.as_str());
        let mut providers = Vec::new();
        for provider in &self.registry.providers {
            let runtime_status = statuses.get(provider.provider_id.as_str());
            providers.push(ProviderRegistryProviderSnapshot {
                provider_id: provider.provider_id.clone(),
                credential_id: normalized_provider_credential_id(
                    provider.provider_id.as_str(),
                    provider.auth_profile_id.as_deref(),
                    provider.credential_source,
                ),
                display_name: provider
                    .display_name
                    .clone()
                    .unwrap_or_else(|| provider.kind.as_str().replace('_', " ")),
                kind: provider.kind.as_str().to_owned(),
                enabled: provider.enabled,
                endpoint_base_url: provider.base_url.clone(),
                auth_profile_id: provider.auth_profile_id.clone(),
                auth_profile_provider_kind: provider
                    .auth_profile_provider_kind
                    .map(|kind| kind.as_str().to_owned()),
                credential_source: provider
                    .credential_source
                    .map(|source| source.as_str().to_owned()),
                api_key_configured: provider.api_key.is_some()
                    || provider.api_key_secret_ref.is_some()
                    || provider.api_key_vault_ref.is_some()
                    || provider.auth_profile_id.is_some(),
                retry_policy: ProviderRetryPolicySnapshot {
                    max_retries: provider.max_retries,
                    retry_backoff_ms: provider.retry_backoff_ms,
                },
                circuit_breaker: runtime_status
                    .map(|snapshot| snapshot.circuit_breaker.clone())
                    .unwrap_or(ProviderCircuitBreakerSnapshot {
                        failure_threshold: provider.circuit_breaker_failure_threshold,
                        cooldown_ms: provider.circuit_breaker_cooldown_ms,
                        consecutive_failures: 0,
                        open: false,
                    }),
                runtime_metrics: runtime_status
                    .map(|snapshot| snapshot.runtime_metrics.clone())
                    .unwrap_or_else(empty_runtime_metrics_snapshot),
                health: runtime_status.map(|snapshot| snapshot.health.clone()).unwrap_or_else(
                    || {
                        empty_health_probe_snapshot(
                            "unknown",
                            "provider has not been probed yet",
                            "registry",
                        )
                    },
                ),
                discovery: runtime_status
                    .map(|snapshot| snapshot.discovery.clone())
                    .unwrap_or_else(|| empty_discovery_snapshot("registry")),
            });
        }
        let credentials = self
            .registry
            .providers
            .iter()
            .map(|provider| {
                let runtime_status = statuses.get(provider.provider_id.as_str());
                let runtime_metrics = runtime_status
                    .map(|snapshot| snapshot.runtime_metrics.clone())
                    .unwrap_or_else(empty_runtime_metrics_snapshot);
                let health =
                    runtime_status.map(|snapshot| snapshot.health.clone()).unwrap_or_else(|| {
                        empty_health_probe_snapshot(
                            "unknown",
                            "provider has not been probed yet",
                            "registry",
                        )
                    });
                ProviderRegistryCredentialSnapshot {
                    credential_id: normalized_provider_credential_id(
                        provider.provider_id.as_str(),
                        provider.auth_profile_id.as_deref(),
                        provider.credential_source,
                    ),
                    provider_id: provider.provider_id.clone(),
                    provider_kind: provider.kind.as_str().to_owned(),
                    auth_profile_id: provider.auth_profile_id.clone(),
                    auth_profile_provider_kind: provider
                        .auth_profile_provider_kind
                        .map(|kind| kind.as_str().to_owned()),
                    credential_source: provider
                        .credential_source
                        .map(|source| source.as_str().to_owned()),
                    availability_state: credential_availability_state(
                        health.state.as_str(),
                        runtime_metrics.last_error.as_ref(),
                    )
                    .to_owned(),
                    capability_summary: credential_capability_summary(
                        self.registry.models.iter().filter(|model| {
                            model.provider_id == provider.provider_id && model.enabled
                        }),
                    ),
                    health,
                    runtime_metrics,
                }
            })
            .collect::<Vec<_>>();
        let models = self
            .registry
            .models
            .iter()
            .map(|model| ProviderRegistryModelSnapshot {
                model_id: model.model_id.clone(),
                provider_id: model.provider_id.clone(),
                role: model.role.as_str().to_owned(),
                enabled: model.enabled,
                capabilities: model.capabilities.clone(),
            })
            .collect::<Vec<_>>();
        let route_selection = self.route_selection_trace(&statuses, default_model_id.as_deref());

        ProviderStatusSnapshot {
            kind: default_provider_entry
                .map(|provider| provider.kind.as_str().to_owned())
                .unwrap_or_else(|| self.config.kind.as_str().to_owned()),
            provider_id: default_provider_id.clone(),
            credential_id: default_provider_entry
                .map(|provider| {
                    normalized_provider_credential_id(
                        provider.provider_id.as_str(),
                        provider.auth_profile_id.as_deref(),
                        provider.credential_source,
                    )
                })
                .unwrap_or_else(|| {
                    normalized_provider_credential_id(
                        default_provider_id.as_str(),
                        self.config.auth_profile_id.as_deref(),
                        self.config.credential_source,
                    )
                }),
            model_id: default_model_id.clone(),
            capabilities: default_model.map(|model| model.capabilities.clone()).unwrap_or_else(
                || {
                    let auth_provider_kind = default_provider_entry
                        .and_then(|provider| provider.auth_profile_provider_kind)
                        .or(self.config.auth_profile_provider_kind);
                    capability_defaults_for_provider(
                        self.config.kind,
                        ProviderModelRole::Chat,
                        auth_provider_kind,
                    )
                },
            ),
            openai_base_url: default_provider_entry
                .filter(|provider| provider.kind == ModelProviderKind::OpenAiCompatible)
                .and_then(|provider| provider.base_url.clone()),
            anthropic_base_url: default_provider_entry
                .filter(|provider| provider.kind == ModelProviderKind::Anthropic)
                .and_then(|provider| provider.base_url.clone()),
            openai_model: default_provider_entry
                .filter(|provider| provider.kind == ModelProviderKind::OpenAiCompatible)
                .and_then(|_| default_model_id.clone()),
            anthropic_model: default_provider_entry
                .filter(|provider| provider.kind == ModelProviderKind::Anthropic)
                .and_then(|_| default_model_id.clone()),
            openai_embeddings_model: self.registry.default_embeddings_model_id.clone(),
            openai_embeddings_dims: None,
            auth_profile_id: default_provider_entry
                .and_then(|provider| provider.auth_profile_id.clone()),
            auth_profile_provider_kind: default_provider_entry.and_then(|provider| {
                provider.auth_profile_provider_kind.map(|kind| kind.as_str().to_owned())
            }),
            credential_source: default_provider_entry.and_then(|provider| {
                provider.credential_source.map(|source| source.as_str().to_owned())
            }),
            api_key_configured: default_provider_entry.is_some_and(|provider| {
                provider.api_key.is_some()
                    || provider.api_key_secret_ref.is_some()
                    || provider.api_key_vault_ref.is_some()
                    || provider.auth_profile_id.is_some()
            }),
            retry_policy: default_provider_entry
                .map(|provider| ProviderRetryPolicySnapshot {
                    max_retries: provider.max_retries,
                    retry_backoff_ms: provider.retry_backoff_ms,
                })
                .unwrap_or(ProviderRetryPolicySnapshot {
                    max_retries: self.config.max_retries,
                    retry_backoff_ms: self.config.retry_backoff_ms,
                }),
            circuit_breaker: default_provider_status
                .map(|snapshot| snapshot.circuit_breaker.clone())
                .unwrap_or(ProviderCircuitBreakerSnapshot {
                    failure_threshold: self.config.circuit_breaker_failure_threshold,
                    cooldown_ms: self.config.circuit_breaker_cooldown_ms,
                    consecutive_failures: 0,
                    open: false,
                }),
            runtime_metrics: self.runtime_metrics_snapshot(),
            response_cache: self.response_cache_snapshot(),
            health: default_provider_status.map(|snapshot| snapshot.health.clone()).unwrap_or_else(
                || {
                    empty_health_probe_snapshot(
                        "unknown",
                        "provider has not been probed yet",
                        "registry",
                    )
                },
            ),
            discovery: default_provider_status
                .map(|snapshot| snapshot.discovery.clone())
                .unwrap_or_else(|| empty_discovery_snapshot("registry")),
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: self.registry.default_chat_model_id.clone(),
                default_embeddings_model_id: self.registry.default_embeddings_model_id.clone(),
                default_audio_transcription_model_id: self
                    .registry
                    .default_audio_transcription_model_id
                    .clone(),
                failover_enabled: self.registry.failover_enabled,
                response_cache_enabled: self.registry.response_cache_enabled,
                providers,
                credentials,
                models,
            },
            route_selection,
        }
    }
}

// Projects a tool catalog snapshot onto its semantically relevant fields for
// cache keying. Typed parse preferred; the untyped fallback only strips the
// known volatile audit fields.
fn stable_tool_catalog_snapshot_for_response_cache(snapshot: &Value) -> Value {
    if let Ok(snapshot) =
        serde_json::from_value::<ModelVisibleToolCatalogSnapshot>(snapshot.clone())
    {
        return json!({
            "schema_version": snapshot.schema_version,
            "provider_dialect": snapshot.provider_dialect.as_str(),
            "provider_kind": snapshot.provider_kind,
            "provider_model_id": snapshot.provider_model_id,
            "surface": snapshot.surface.as_str(),
            "principal_hash": snapshot.principal_hash,
            "channel_hash": snapshot.channel_hash,
            "remaining_tool_budget": snapshot.remaining_tool_budget,
            "profile_expansion": snapshot.profile_expansion,
            "exposure_mode": snapshot.exposure_mode.as_str(),
            "compact_tool_threshold": snapshot.compact_tool_threshold,
            "direct_tool_count": snapshot.direct_tool_count,
            "exposed_tool_count": snapshot.exposed_tool_count,
            "estimated_direct_tool_bytes": snapshot.estimated_direct_tool_bytes,
            "estimated_exposed_tool_bytes": snapshot.estimated_exposed_tool_bytes,
            "estimated_saved_bytes": snapshot.estimated_saved_bytes,
            "availability_probes": snapshot.availability_probes,
            "index": snapshot.index,
            "indexed_tools": snapshot.indexed_tools,
            "tools": snapshot.tools,
            "filtered_tools": snapshot.filtered_tools,
        });
    }

    let mut stable_snapshot = snapshot.clone();
    if let Value::Object(fields) = &mut stable_snapshot {
        fields.remove("snapshot_id");
        fields.remove("catalog_hash");
        fields.remove("created_at_unix_ms");
    }
    stable_snapshot
}

fn default_model_for_provider(
    registry: &ModelProviderRegistryConfig,
    provider_id: &str,
) -> Option<String> {
    if let Some(default_model_id) = registry.default_chat_model_id.as_deref() {
        if registry.models.iter().any(|model| {
            model.model_id == default_model_id
                && model.provider_id == provider_id
                && model.role == ProviderModelRole::Chat
                && model.enabled
        }) {
            return Some(default_model_id.to_owned());
        }
    }
    registry
        .models
        .iter()
        .find(|model| {
            model.provider_id == provider_id
                && model.role == ProviderModelRole::Chat
                && model.enabled
        })
        .map(|model| model.model_id.clone())
}

fn fallback_cost_rank(value: &str) -> u8 {
    match value {
        "low" => 0,
        "standard" => 1,
        "premium" => 2,
        _ => 3,
    }
}

fn fallback_latency_rank(value: &str) -> u8 {
    match value {
        "low" => 0,
        "standard" => 1,
        "high" => 2,
        _ => 3,
    }
}

fn default_provider_service_tiers() -> Vec<String> {
    [
        ProviderServiceTier::Auto,
        ProviderServiceTier::Default,
        ProviderServiceTier::Priority,
        ProviderServiceTier::Flex,
    ]
    .into_iter()
    .map(ProviderServiceTier::as_str)
    .map(ToOwned::to_owned)
    .collect()
}

fn build_registry_provider_runtime(
    base_config: &ModelProviderConfig,
    registry: &ModelProviderRegistryConfig,
    entry: &ProviderRegistryEntryConfig,
    default_chat_model_id: Option<String>,
) -> Result<Arc<dyn ModelProvider>> {
    let mut config = ModelProviderConfig {
        kind: entry.kind,
        openai_base_url: entry
            .base_url
            .clone()
            .unwrap_or_else(|| base_config.openai_base_url.clone()),
        anthropic_base_url: entry
            .base_url
            .clone()
            .unwrap_or_else(|| base_config.anthropic_base_url.clone()),
        allow_private_base_url: entry.allow_private_base_url,
        openai_model: default_chat_model_id
            .clone()
            .unwrap_or_else(|| base_config.openai_model.clone()),
        anthropic_model: default_chat_model_id
            .clone()
            .unwrap_or_else(|| base_config.anthropic_model.clone()),
        openai_embeddings_model: registry
            .models
            .iter()
            .find(|model| {
                model.provider_id == entry.provider_id
                    && model.role == ProviderModelRole::Embeddings
                    && model.enabled
            })
            .map(|model| model.model_id.clone()),
        openai_embeddings_dims: None,
        openai_api_key: None,
        openai_api_key_secret_ref: None,
        openai_api_key_vault_ref: None,
        anthropic_api_key: None,
        anthropic_api_key_secret_ref: None,
        anthropic_api_key_vault_ref: None,
        auth_profile_id: entry
            .auth_profile_id
            .clone()
            .or_else(|| base_config.auth_profile_id.clone()),
        auth_profile_provider_kind: entry
            .auth_profile_provider_kind
            .or(base_config.auth_profile_provider_kind),
        credential_source: entry.credential_source.or(base_config.credential_source),
        reasoning_effort: base_config.reasoning_effort,
        service_tier: base_config.service_tier,
        qa_mock_fixture_path: base_config.qa_mock_fixture_path.clone(),
        qa_mock_fixture_enabled: base_config.qa_mock_fixture_enabled,
        request_timeout_ms: entry.request_timeout_ms,
        max_retries: entry.max_retries,
        retry_backoff_ms: entry.retry_backoff_ms,
        circuit_breaker_failure_threshold: entry.circuit_breaker_failure_threshold,
        circuit_breaker_cooldown_ms: entry.circuit_breaker_cooldown_ms,
        registry: ModelProviderRegistryConfig::default(),
    };
    match entry.kind {
        ModelProviderKind::Deterministic => {
            Ok(Arc::new(DeterministicProvider::new(config)?) as Arc<dyn ModelProvider>)
        }
        ModelProviderKind::OpenAiCompatible => {
            config.openai_api_key = entry.api_key.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::OpenAiCompatible {
                    base_config.openai_api_key.clone()
                } else {
                    None
                }
            });
            config.openai_api_key_secret_ref = entry.api_key_secret_ref.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::OpenAiCompatible {
                    base_config.openai_api_key_secret_ref.clone()
                } else {
                    None
                }
            });
            config.openai_api_key_vault_ref = entry.api_key_vault_ref.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::OpenAiCompatible {
                    base_config.openai_api_key_vault_ref.clone()
                } else {
                    None
                }
            });
            Ok(Arc::new(OpenAiCompatibleProvider::new(&config)?) as Arc<dyn ModelProvider>)
        }
        ModelProviderKind::Anthropic => {
            config.anthropic_api_key = entry.api_key.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::Anthropic {
                    base_config.anthropic_api_key.clone()
                } else {
                    None
                }
            });
            config.anthropic_api_key_secret_ref = entry.api_key_secret_ref.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::Anthropic {
                    base_config.anthropic_api_key_secret_ref.clone()
                } else {
                    None
                }
            });
            config.anthropic_api_key_vault_ref = entry.api_key_vault_ref.clone().or_else(|| {
                if base_config.kind == ModelProviderKind::Anthropic {
                    base_config.anthropic_api_key_vault_ref.clone()
                } else {
                    None
                }
            });
            Ok(Arc::new(AnthropicProvider::new(&config)?) as Arc<dyn ModelProvider>)
        }
    }
}

/// Offline provider that echoes input (or replays the scripted tool-call
/// fixture) with estimated token usage; used for tests and smoke flows.
#[derive(Debug)]
struct DeterministicProvider {
    config: ModelProviderConfig,
    qa_mock_fixture: Option<QaMockProviderFixture>,
    runtime_metrics: Mutex<ProviderRuntimeMetrics>,
}

impl DeterministicProvider {
    fn new(config: ModelProviderConfig) -> Result<Self> {
        let qa_mock_fixture = load_qa_mock_provider_fixture(&config)?;
        Ok(Self {
            config,
            qa_mock_fixture,
            runtime_metrics: Mutex::new(ProviderRuntimeMetrics::default()),
        })
    }

    fn record_runtime_metrics(
        &self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
        failure: Option<ProviderFailureSnapshot>,
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms, failure);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }
}

fn load_qa_mock_provider_fixture(
    config: &ModelProviderConfig,
) -> Result<Option<QaMockProviderFixture>> {
    let Some(path) = config.qa_mock_fixture_path.as_deref() else {
        return Ok(None);
    };
    if !config.qa_mock_fixture_enabled {
        anyhow::bail!(
            "model_provider.qa_mock_fixture_path requires qa_lab.mode=preview_only or PALYRA_QA_LAB_MODE=preview_only"
        );
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read QA mock-provider fixture {}", path.display()))?;
    parse_qa_mock_provider_fixture_yaml(text.as_str())
        .with_context(|| format!("failed to parse QA mock-provider fixture {}", path.display()))
        .map(Some)
}

struct QaMockProviderExecution {
    output: ProviderTurnOutput,
    retry_count: u32,
    attempts: Vec<ProviderAttemptSummary>,
}

#[derive(Debug, Clone, Copy)]
struct QaMockProviderExecutionPolicy {
    request_timeout: Duration,
    max_retries: u32,
    retry_backoff_ms: u64,
}

impl QaMockProviderExecutionPolicy {
    fn from_config(config: &ModelProviderConfig) -> Self {
        Self {
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            max_retries: config.max_retries,
            retry_backoff_ms: config.retry_backoff_ms,
        }
    }

    fn backoff_for_retry(self, retry_index: u32) -> Duration {
        let exponent = retry_index.min(8);
        let multiplier = 1_u64 << exponent;
        Duration::from_millis(self.retry_backoff_ms.saturating_mul(multiplier))
    }
}

async fn execute_qa_mock_provider_turn(
    turn: &palyra_model_providers::QaMockProviderTurn,
    request: &ProviderRequest,
    model_id: &str,
    config: &ModelProviderConfig,
) -> Result<QaMockProviderExecution, ProviderError> {
    validate_qa_mock_provider_attempt_bounds(turn)?;
    let policy = QaMockProviderExecutionPolicy::from_config(config);
    let attempt_count = turn.attempt_count();
    let mut attempts = Vec::with_capacity(attempt_count);
    let provider_id = "deterministic-primary";
    let credential_id = normalized_provider_credential_id(
        provider_id,
        config.auth_profile_id.as_deref(),
        config.credential_source,
    );

    for attempt_index in 0..attempt_count {
        let retry_count = u32::try_from(attempt_index).unwrap_or(u32::MAX);
        let latency_ms = turn.attempt_latency_ms(attempt_index).ok_or_else(|| {
            qa_mock_provider_plan_error(retry_count, "attempt latency is unavailable")
        })?;
        let attempt_result = tokio::time::timeout(policy.request_timeout, async {
            if latency_ms > 0 {
                tokio::time::sleep(Duration::from_millis(latency_ms)).await;
            }
            qa_mock_provider_output_for_attempt(
                turn,
                attempt_index,
                request,
                Some(model_id.to_owned()),
            )
        })
        .await
        .unwrap_or_else(|_| Err(qa_mock_provider_timeout_error(retry_count)));

        match attempt_result {
            Ok(output) => {
                let state = qa_mock_provider_success_state(
                    attempt_index,
                    provider_id,
                    credential_id.as_str(),
                    model_id,
                    &output,
                );
                attempts.push(provider_attempt_summary(
                    provider_id.to_owned(),
                    model_id.to_owned(),
                    "success",
                    false,
                    false,
                    None,
                    Some(state),
                ));
                return Ok(QaMockProviderExecution { output, retry_count, attempts });
            }
            Err(error) => {
                let classification = error.classification();
                let retryable = classification.recommended_action == ProviderFailureAction::Retry;
                let reason_code = classification
                    .provider_detail
                    .clone()
                    .or_else(|| Some(classification.class.as_str().to_owned()));
                let state = qa_mock_provider_error_state(
                    attempt_index,
                    provider_id,
                    credential_id.as_str(),
                    model_id,
                    &error,
                    current_unix_ms().unwrap_or_default(),
                );
                attempts.push(provider_attempt_summary(
                    provider_id.to_owned(),
                    model_id.to_owned(),
                    "error",
                    retryable,
                    false,
                    reason_code,
                    Some(state),
                ));
                let has_followup = attempt_index.saturating_add(1) < attempt_count;
                if retryable && has_followup && retry_count < policy.max_retries {
                    tokio::time::sleep(policy.backoff_for_retry(retry_count)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }

    Err(qa_mock_provider_plan_error(
        u32::try_from(attempt_count).unwrap_or(u32::MAX),
        "attempt sequence completed without a terminal result",
    ))
}

fn validate_qa_mock_provider_attempt_bounds(
    turn: &palyra_model_providers::QaMockProviderTurn,
) -> Result<(), ProviderError> {
    let attempt_count = turn.attempt_count();
    if turn.has_explicit_attempt_sequence() && attempt_count < MIN_QA_MOCK_PROVIDER_ATTEMPTS {
        return Err(qa_mock_provider_plan_error(
            0,
            "explicit attempt sequence is shorter than the supported minimum",
        ));
    }
    if attempt_count == 0 || attempt_count > MAX_QA_MOCK_PROVIDER_ATTEMPTS {
        return Err(qa_mock_provider_plan_error(0, "attempt count exceeds the supported bound"));
    }

    let mut total_latency_ms = 0_u64;
    for attempt_index in 0..attempt_count {
        let latency_ms = turn
            .attempt_latency_ms(attempt_index)
            .ok_or_else(|| qa_mock_provider_plan_error(0, "attempt latency is unavailable"))?;
        if latency_ms > MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS {
            return Err(qa_mock_provider_plan_error(
                0,
                "attempt latency exceeds the supported bound",
            ));
        }
        total_latency_ms = total_latency_ms
            .checked_add(latency_ms)
            .ok_or_else(|| qa_mock_provider_plan_error(0, "attempt latency total overflowed"))?;
    }
    if total_latency_ms > MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS {
        return Err(qa_mock_provider_plan_error(
            0,
            "attempt latency total exceeds the supported bound",
        ));
    }
    Ok(())
}

fn qa_mock_provider_plan_error(retry_count: u32, reason: &str) -> ProviderError {
    ProviderError::InvalidResponse {
        message: format!("QA mock-provider fixture has an invalid attempt plan: {reason}"),
        retry_count,
        classification: invalid_response_classification("qa_mock_invalid_attempt_plan"),
    }
}

fn qa_mock_provider_timeout_error(retry_count: u32) -> ProviderError {
    ProviderError::RequestFailed {
        message: "QA mock-provider fixture attempt exceeded the configured request timeout"
            .to_owned(),
        retryable: true,
        retry_count,
        classification: classify_transport_provider_failure("qa_mock_fixture_attempt", true),
    }
}

fn qa_mock_provider_success_state(
    attempt_index: usize,
    provider_id: &str,
    credential_id: &str,
    model_id: &str,
    output: &ProviderTurnOutput,
) -> ProviderAttemptState {
    ProviderAttemptState {
        attempt_index: provider_attempt_index(attempt_index),
        provider_profile_id: provider_id.to_owned(),
        credential_id: credential_id.to_owned(),
        model_id: model_id.to_owned(),
        error_class: None,
        retry_after_ms: None,
        cooldown_until_unix_ms: None,
        prompt_tokens: output.usage.prompt_tokens,
        output_tokens: output.usage.completion_tokens,
        cache_tokens: 0,
        estimated_cost_microusd: None,
        final_disposition: "success".to_owned(),
        repair_hint: None,
    }
}

fn qa_mock_provider_error_state(
    attempt_index: usize,
    provider_id: &str,
    credential_id: &str,
    model_id: &str,
    error: &ProviderError,
    observed_at_unix_ms: i64,
) -> ProviderAttemptState {
    let failure = error.failure_snapshot();
    let retry_after_ms = failure.recovery.retry_after_ms;
    ProviderAttemptState {
        attempt_index: provider_attempt_index(attempt_index),
        provider_profile_id: provider_id.to_owned(),
        credential_id: credential_id.to_owned(),
        model_id: model_id.to_owned(),
        error_class: Some(failure.class.as_str().to_owned()),
        retry_after_ms,
        cooldown_until_unix_ms: retry_after_ms.map(|value| {
            observed_at_unix_ms.saturating_add(i64::try_from(value).unwrap_or(i64::MAX))
        }),
        prompt_tokens: 0,
        output_tokens: 0,
        cache_tokens: 0,
        estimated_cost_microusd: None,
        final_disposition: provider_attempt_error_disposition(&failure).to_owned(),
        repair_hint: provider_attempt_repair_hint(&failure),
    }
}

// Scripted three-turn tool-call fixture for offline regression: turn 1
// proposes a workspace patch write, turn 2 (after the write result) proposes
// a read-back, turn 3 (after the read result) emits the final text. The turn
// is inferred from which tool results are already present in the request.
fn deterministic_tool_fixture_output(
    request: &ProviderRequest,
    prompt_tokens: u64,
) -> Option<ProviderTurnOutput> {
    if request.json_mode || !deterministic_tool_fixture_requested(request) {
        return None;
    }

    if provider_request_has_tool_result(request, DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID) {
        let readback_verified =
            provider_request_tool_result_text(request, DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID)
                .is_some_and(|text| text.contains(DETERMINISTIC_TOOL_FIXTURE_ID));
        let final_text = if readback_verified {
            format!(
                "Deterministic provider fixture completed: wrote and read back {DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH} with fixture_id={DETERMINISTIC_TOOL_FIXTURE_ID}."
            )
        } else {
            format!(
                "Deterministic provider fixture completed with an unexpected read-back payload for {DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH}; inspect the read_file tool result."
            )
        };
        return Some(deterministic_text_output(
            final_text,
            prompt_tokens,
            request.model_override.clone(),
            "deterministic:tool-fixture-final",
        ));
    }

    if provider_request_has_tool_result(request, DETERMINISTIC_TOOL_FIXTURE_WRITE_CALL_ID) {
        if !provider_request_tool_catalog_contains(request, "palyra.fs.read_file") {
            return Some(deterministic_text_output(
                format!(
                    "Deterministic provider fixture wrote {DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH}, but palyra.fs.read_file is not visible in the tool catalog for read-back verification."
                ),
                prompt_tokens,
                request.model_override.clone(),
                "deterministic:tool-fixture-read-unavailable",
            ));
        }
        return Some(deterministic_tool_call_output(
            DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID,
            "palyra.fs.read_file",
            json!({
                "path": DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH,
                "max_bytes": 4096,
            }),
            prompt_tokens,
            request.model_override.clone(),
            "deterministic:tool-fixture-read",
        ));
    }

    if !provider_request_tool_catalog_contains(request, "palyra.fs.apply_patch") {
        return Some(deterministic_text_output(
            format!(
                "Deterministic provider tool-call fixture requested, but palyra.fs.apply_patch is not visible in the tool catalog; enable the workspace patch tool to run fixture {DETERMINISTIC_TOOL_FIXTURE_ID}."
            ),
            prompt_tokens,
            request.model_override.clone(),
            "deterministic:tool-fixture-write-unavailable",
        ));
    }

    Some(deterministic_tool_call_output(
        DETERMINISTIC_TOOL_FIXTURE_WRITE_CALL_ID,
        "palyra.fs.apply_patch",
        json!({
            "patch": deterministic_tool_fixture_patch(),
        }),
        prompt_tokens,
        request.model_override.clone(),
        "deterministic:tool-fixture-write",
    ))
}

fn deterministic_tool_fixture_requested(request: &ProviderRequest) -> bool {
    let input = request
        .user_visible_input_text
        .as_deref()
        .unwrap_or(request.input_text.as_str())
        .to_ascii_lowercase();
    input.contains(DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH)
        || input.contains("deterministic-provider.md")
}

fn deterministic_tool_fixture_patch() -> String {
    let mut patch =
        format!("*** Begin Patch\n*** Add File: {DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH}\n");
    for line in DETERMINISTIC_TOOL_FIXTURE_REPORT.lines() {
        if line.is_empty() {
            patch.push_str("+\n");
        } else {
            patch.push('+');
            patch.push_str(line);
            patch.push('\n');
        }
    }
    patch.push_str("*** End Patch");
    patch
}

fn provider_request_tool_catalog_contains(request: &ProviderRequest, tool_name: &str) -> bool {
    request
        .tool_catalog_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.get("tools"))
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| tool.get("name").and_then(Value::as_str) == Some(tool_name))
        })
}

fn provider_request_has_tool_result(request: &ProviderRequest, tool_call_id: &str) -> bool {
    provider_request_tool_result_text(request, tool_call_id).is_some()
}

fn provider_request_tool_result_text(
    request: &ProviderRequest,
    tool_call_id: &str,
) -> Option<String> {
    request
        .messages
        .iter()
        .find(|message| {
            message.role == ProviderMessageRole::Tool
                && message.tool_call_id.as_deref() == Some(tool_call_id)
        })
        .map(ProviderMessage::text_content)
}

fn deterministic_tool_call_output(
    proposal_id: &str,
    tool_name: &str,
    input_json: Value,
    prompt_tokens: u64,
    provider_model_id: Option<String>,
    provider_trace_ref: &str,
) -> ProviderTurnOutput {
    let completion_tokens = serde_json::to_string(&input_json)
        .ok()
        .map(|value| estimate_token_count(value.as_str()).max(1))
        .unwrap_or(1);
    ProviderTurnOutput {
        full_text: String::new(),
        content_parts: vec![ProviderOutputContentPart::ToolCall {
            proposal_id: proposal_id.to_owned(),
            tool_name: tool_name.to_owned(),
            input_json,
        }],
        finish_reason: ProviderFinishReason::ToolCalls,
        usage: ProviderUsage::new(prompt_tokens, completion_tokens, "estimated"),
        raw_provider_refs: deterministic_raw_provider_refs(provider_model_id, provider_trace_ref),
        redaction_state: ProviderRedactionState::default(),
    }
}

fn deterministic_text_output(
    text: String,
    prompt_tokens: u64,
    provider_model_id: Option<String>,
    provider_trace_ref: &str,
) -> ProviderTurnOutput {
    ProviderTurnOutput::text(
        text.clone(),
        ProviderFinishReason::Stop,
        ProviderUsage::new(prompt_tokens, estimate_token_count(text.as_str()).max(1), "estimated"),
        deterministic_raw_provider_refs(provider_model_id, provider_trace_ref),
    )
}

fn deterministic_raw_provider_refs(
    provider_model_id: Option<String>,
    provider_trace_ref: &str,
) -> ProviderRawProviderRefs {
    ProviderRawProviderRefs {
        provider_response_id: None,
        provider_model_id,
        system_fingerprint: None,
        provider_trace_ref: Some(provider_trace_ref.to_owned()),
        stream_spill_ref: None,
    }
}

impl ModelProvider for DeterministicProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let started_at = Instant::now();
            if provider_request_has_vision(&request) {
                let error =
                    ProviderError::VisionUnsupported { provider: "deterministic".to_owned() };
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    0,
                    elapsed_millis_since(started_at),
                    Some(error.failure_snapshot()),
                );
                return Err(error);
            }

            let prompt_tokens = estimate_token_count(request.input_text.as_str());
            let actual_model_id =
                request.model_override.clone().unwrap_or_else(|| "deterministic".to_owned());
            let (output, retry_count, attempts) = if let Some(fixture) =
                self.qa_mock_fixture.as_ref()
            {
                let Some(turn) = qa_mock_provider_turn_for_request(fixture, &request) else {
                    let error = ProviderError::RequestFailed {
                        message: format!(
                            "QA mock-provider fixture '{}' has no turn matching the request",
                            fixture.id
                        ),
                        retryable: false,
                        retry_count: 0,
                        classification: fail_closed_provider_classification(
                            "qa_mock_fixture_no_matching_turn",
                        ),
                    };
                    self.record_runtime_metrics(
                        true,
                        prompt_tokens,
                        0,
                        0,
                        elapsed_millis_since(started_at),
                        Some(error.failure_snapshot()),
                    );
                    return Err(error);
                };
                match execute_qa_mock_provider_turn(
                    turn,
                    &request,
                    actual_model_id.as_str(),
                    &self.config,
                )
                .await
                {
                    Ok(execution) => (execution.output, execution.retry_count, execution.attempts),
                    Err(error) => {
                        self.record_runtime_metrics(
                            true,
                            prompt_tokens,
                            0,
                            error.retry_count(),
                            elapsed_millis_since(started_at),
                            Some(error.failure_snapshot()),
                        );
                        return Err(error);
                    }
                }
            } else {
                let output = deterministic_tool_fixture_output(&request, prompt_tokens)
                    .unwrap_or_else(|| {
                        let completion_source = if request.json_mode {
                            r#"{"ack":"ok"}"#.to_owned()
                        } else if let Some(user_visible_input_text) = request
                            .user_visible_input_text
                            .as_ref()
                            .filter(|value| !value.trim().is_empty())
                        {
                            user_visible_input_text.clone()
                        } else {
                            request.input_text.clone()
                        };

                        deterministic_text_output(
                            if completion_source.trim().is_empty() {
                                "ack".to_owned()
                            } else {
                                completion_source
                            },
                            prompt_tokens,
                            request.model_override.clone(),
                            "deterministic",
                        )
                    });
                let attempts = vec![provider_attempt_summary(
                    "deterministic-primary".to_owned(),
                    actual_model_id.clone(),
                    "success",
                    false,
                    false,
                    None,
                    None,
                )];
                (output, 0, attempts)
            };
            let prompt_tokens = output.usage.prompt_tokens;
            let completion_tokens = output.usage.completion_tokens;
            let events = provider_events_from_output(&output);
            self.record_runtime_metrics(
                false,
                prompt_tokens,
                completion_tokens,
                retry_count,
                elapsed_millis_since(started_at),
                None,
            );
            Ok(ProviderResponse {
                output,
                events,
                prompt_tokens,
                completion_tokens,
                retry_count,
                provider_id: "deterministic-primary".to_owned(),
                model_id: actual_model_id,
                served_from_cache: false,
                failover_count: 0,
                attempts,
            })
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        _request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async move {
            Err(ProviderError::RequestFailed {
                message: "deterministic model provider does not support audio transcription"
                    .to_owned(),
                retryable: false,
                retry_count: 0,
                classification: failover_provider_classification(
                    "deterministic_audio_transcription_unsupported",
                ),
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "deterministic-primary".to_owned(),
            credential_id: normalized_provider_credential_id(
                "deterministic-primary",
                self.config.auth_profile_id.as_deref(),
                self.config.credential_source,
            ),
            model_id: Some("deterministic".to_owned()),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                reasoning: false,
                reasoning_efforts: Vec::new(),
                service_tier: false,
                service_tiers: Vec::new(),
                max_context_tokens: Some(8_192),
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec![
                    "offline testing".to_owned(),
                    "scripted tool-call regression".to_owned(),
                    "deterministic smoke flows".to_owned(),
                ],
                known_limitations: vec![
                    "scripted fixture responses only".to_owned(),
                    "vision unsupported".to_owned(),
                ],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            },
            openai_base_url: None,
            anthropic_base_url: None,
            openai_model: None,
            anthropic_model: None,
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: self.config.auth_profile_id.clone(),
            auth_profile_provider_kind: self
                .config
                .auth_profile_provider_kind
                .map(|kind| kind.as_str().to_owned()),
            credential_source: self
                .config
                .credential_source
                .map(|source| source.as_str().to_owned()),
            api_key_configured: false,
            retry_policy: ProviderRetryPolicySnapshot {
                max_retries: self.config.max_retries,
                retry_backoff_ms: self.config.retry_backoff_ms,
            },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: self.config.circuit_breaker_failure_threshold,
                cooldown_ms: self.config.circuit_breaker_cooldown_ms,
                consecutive_failures: 0,
                open: false,
            },
            runtime_metrics: self.runtime_metrics_snapshot(),
            response_cache: empty_response_cache_snapshot(response_cache_enabled_from_config(
                &self.config,
            )),
            health: empty_health_probe_snapshot(
                "ok",
                "deterministic provider is always available",
                "static",
            ),
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: None,
                expires_at_unix_ms: None,
                discovered_model_ids: vec!["deterministic".to_owned()],
                source: "static".to_owned(),
                message: None,
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some("deterministic".to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: false,
                response_cache_enabled: true,
                providers: Vec::new(),
                credentials: Vec::new(),
                models: Vec::new(),
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot.route_selection = route_selection_from_status_snapshot(&snapshot);
        snapshot
    }
}

/// Offline embeddings backend producing stable hash-derived unit vectors;
/// useful for deterministic memory/retrieval tests without network access.
#[derive(Debug)]
struct DeterministicEmbeddingsProvider {
    dimensions: usize,
    model_name: String,
}

impl DeterministicEmbeddingsProvider {
    fn new(config: ModelProviderConfig) -> Self {
        let dimensions = config
            .openai_embeddings_dims
            .map_or(DEFAULT_DETERMINISTIC_EMBEDDINGS_DIMS, |value| value as usize);
        let model_name =
            config.openai_embeddings_model.unwrap_or_else(|| "hash-embedding-v1".to_owned());
        Self { dimensions, model_name }
    }
}

impl EmbeddingsProvider for DeterministicEmbeddingsProvider {
    fn embed<'a>(
        &'a self,
        request: EmbeddingsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<EmbeddingsResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let normalized_inputs = normalize_embeddings_inputs(&request)?;
            let vectors = normalized_inputs
                .iter()
                .map(|input| hash_embed_text(input.as_str(), self.dimensions))
                .collect::<Vec<_>>();
            Ok(EmbeddingsResponse {
                model_name: self.model_name.clone(),
                dimensions: self.dimensions,
                vectors,
                retry_count: 0,
            })
        })
    }
}

/// HTTP backend for OpenAI-compatible chat-completions and audio
/// transcription endpoints, with retries and a per-provider circuit breaker.
#[derive(Debug)]
struct OpenAiCompatibleProvider {
    config: ModelProviderConfig,
    client: Client,
    circuit_state: Mutex<CircuitBreakerState>,
    runtime_metrics: Mutex<ProviderRuntimeMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CircuitBreakerState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

/// Failure of a single upstream attempt, before retry policy is applied.
/// `invalid_response` selects between [`ProviderError::InvalidResponse`] and
/// [`ProviderError::RequestFailed`] once retries are exhausted.
#[derive(Debug)]
struct AttemptError {
    message: String,
    retryable: bool,
    invalid_response: bool,
    classification: ProviderFailureClassification,
}

impl AttemptError {
    fn request_failed(
        message: String,
        retryable: bool,
        classification: ProviderFailureClassification,
    ) -> Self {
        Self { message, retryable, invalid_response: false, classification }
    }

    fn invalid_response(message: String, provider_detail: &str) -> Self {
        Self {
            message,
            retryable: false,
            invalid_response: true,
            classification: invalid_response_classification(provider_detail),
        }
    }

    fn retryable_invalid_response(message: String, provider_detail: &str) -> Self {
        Self {
            message,
            retryable: true,
            invalid_response: true,
            classification: retryable_invalid_response_classification(provider_detail),
        }
    }
}

fn retry_after_ms_from_response(response: &Response) -> Option<u64> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1_000))
}

fn classify_http_provider_failure_with_retry_after(
    status_code: u16,
    retryable: bool,
    provider_detail: &str,
    response_body: &str,
    retry_after_ms: Option<u64>,
) -> ProviderFailureClassification {
    classify_http_provider_failure(status_code, retryable, provider_detail, response_body)
        .with_retry_after_ms(retry_after_ms)
}

fn selected_chat_model_id<'a>(
    model_override: Option<&'a str>,
    configured_chat_model_id: &'a str,
    provider_label: &str,
    classification_detail: &str,
) -> Result<&'a str, AttemptError> {
    model_override
        .and_then(configured_model_id)
        .or_else(|| configured_model_id(configured_chat_model_id))
        .ok_or_else(|| {
            AttemptError::request_failed(
                format!("{provider_label} provider has no discovered chat model configured"),
                false,
                fail_closed_provider_classification(classification_detail),
            )
        })
}

// Wire-format DTOs for upstream responses. Fields default aggressively so
// partial vendor payloads still deserialize; validation happens afterwards.
#[derive(Debug, Deserialize)]
struct OpenAiChatCompletionResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    system_fingerprint: Option<String>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
    choices: Vec<OpenAiChatChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatChoice {
    message: OpenAiChatMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatMessage {
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    tool_calls: Vec<OpenAiToolCall>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<OpenAiToolFunction>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolFunction {
    name: String,
    #[serde(default)]
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponsesResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output_text: Option<String>,
    #[serde(default)]
    usage: Option<OpenAiResponsesUsage>,
    #[serde(default)]
    output: Vec<OpenAiResponsesOutputItem>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponsesOutputItem {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<Value>,
    #[serde(default)]
    content: Vec<OpenAiResponsesContentPart>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponsesContentPart {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponsesUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    input_tokens_details: Option<OpenAiTokenDetails>,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    prompt_tokens_details: Option<OpenAiTokenDetails>,
}

#[derive(Debug, Deserialize)]
struct OpenAiTokenDetails {
    #[serde(default)]
    cached_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingsResponse {
    #[serde(default)]
    data: Vec<OpenAiEmbeddingVector>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingVector {
    #[serde(default)]
    index: Option<usize>,
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct OpenAiAudioTranscriptionResponse {
    text: String,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    segments: Vec<OpenAiAudioTranscriptionSegment>,
}

#[derive(Debug, Deserialize)]
struct OpenAiAudioTranscriptionSegment {
    #[serde(default)]
    start: Option<f64>,
    #[serde(default)]
    end: Option<f64>,
    #[serde(default)]
    text: String,
    #[serde(default)]
    avg_logprob: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AnthropicMessagesResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
}

fn non_zero_cache_tokens(tokens: u64) -> Option<u64> {
    (tokens > 0).then_some(tokens)
}

#[derive(Debug, Deserialize)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

/// HTTP backend for OpenAI-compatible embeddings endpoints with retry and
/// strict response-shape validation (ordering, dimensions, counts).
#[derive(Debug)]
struct OpenAiCompatibleEmbeddingsProvider {
    config: ModelProviderConfig,
    client: Client,
}

impl OpenAiCompatibleEmbeddingsProvider {
    fn new(config: &ModelProviderConfig) -> Result<Self> {
        if config.openai_embeddings_model.is_none() {
            return Err(ProviderError::MissingEmbeddingsModel.into());
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .context("failed to build openai-compatible embeddings HTTP client")?;
        Ok(Self { config: config.clone(), client })
    }

    fn embeddings_endpoint(&self) -> String {
        format!("{}{}", self.config.openai_base_url.trim_end_matches('/'), OPENAI_EMBEDDINGS_PATH)
    }

    // Exponential backoff (base * 2^retry) with the exponent capped at 8 so
    // the multiplier stays bounded at 256x even for large retry budgets.
    fn backoff_for_retry(&self, retry_index: u32) -> Duration {
        let exponent = retry_index.min(8);
        let multiplier = 1_u64 << exponent;
        Duration::from_millis(self.config.retry_backoff_ms.saturating_mul(multiplier))
    }

    async fn request_once(
        &self,
        api_key: &str,
        inputs: &[String],
    ) -> Result<EmbeddingsResponse, AttemptError> {
        let model_name = self.config.openai_embeddings_model.as_ref().ok_or_else(|| {
            AttemptError::request_failed(
                ProviderError::MissingEmbeddingsModel.to_string(),
                false,
                fail_closed_provider_classification("openai_embeddings_model_missing"),
            )
        })?;
        let mut body = json!({
            "model": model_name,
            "input": inputs,
        });
        if let Some(dimensions) = self.config.openai_embeddings_dims {
            body["dimensions"] = json!(dimensions);
        }

        let endpoint = self.embeddings_endpoint();
        let response = self
            .client
            .post(endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                AttemptError::request_failed(
                    format!("openai-compatible embeddings request failed: {error}"),
                    true,
                    classify_reqwest_provider_failure(
                        "openai_compatible_embeddings_request",
                        &error,
                    ),
                )
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
            let retry_after_ms = retry_after_ms_from_response(&response);
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<openai-compatible error body unavailable>".to_owned());
            return Err(AttemptError::request_failed(
                format!(
                    "openai-compatible embeddings endpoint returned HTTP {status}: {}",
                    sanitize_remote_error(&body_text)
                ),
                retryable,
                classify_http_provider_failure_with_retry_after(
                    status,
                    retryable,
                    "openai_compatible_embeddings_http",
                    body_text.as_str(),
                    retry_after_ms,
                ),
            ));
        }

        let parsed = response.json::<OpenAiEmbeddingsResponse>().await.map_err(|error| {
            AttemptError::invalid_response(
                format!("openai-compatible embeddings response JSON parsing failed: {error}"),
                "openai_compatible_embeddings_response_json",
            )
        })?;
        if parsed.data.is_empty() {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response did not include vectors".to_owned(),
                "openai_compatible_embeddings_vectors_missing",
            ));
        }

        // Vectors may arrive out of order; reassemble by the reported index
        // (falling back to array position when omitted) and reject
        // out-of-range, duplicate, or missing slots so callers can rely on
        // input/vector alignment.
        let mut ordered_vectors: Vec<Option<Vec<f32>>> = vec![None; inputs.len()];
        for (position, item) in parsed.data.into_iter().enumerate() {
            let index = item.index.unwrap_or(position);
            if index >= ordered_vectors.len() {
                return Err(AttemptError::invalid_response(format!(
                    "openai-compatible embeddings response contained out-of-range vector index {index}"
                ), "openai_compatible_embeddings_vector_index_out_of_range"));
            }
            if ordered_vectors[index].is_some() {
                return Err(AttemptError::invalid_response(
                    format!(
                        "openai-compatible embeddings response duplicated vector index {index}"
                    ),
                    "openai_compatible_embeddings_vector_index_duplicate",
                ));
            }
            ordered_vectors[index] = Some(item.embedding);
        }

        let vectors = ordered_vectors
            .into_iter()
            .map(|vector| {
                vector.ok_or_else(|| {
                    AttemptError::invalid_response(
                        "openai-compatible embeddings response omitted one or more vectors"
                            .to_owned(),
                        "openai_compatible_embeddings_vectors_omitted",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dimensions = vectors.first().map_or(0, |vector| vector.len());
        if dimensions == 0 {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response vectors must be non-empty".to_owned(),
                "openai_compatible_embeddings_vectors_empty",
            ));
        }
        if vectors.iter().any(|vector| vector.len() != dimensions) {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response returned inconsistent vector dimensions"
                    .to_owned(),
                "openai_compatible_embeddings_dims_inconsistent",
            ));
        }
        if let Some(expected_dimensions) = self.config.openai_embeddings_dims {
            if dimensions != expected_dimensions as usize {
                return Err(AttemptError::invalid_response(
                    format!(
                    "openai-compatible embeddings response returned dims {dimensions}, expected {}",
                    expected_dimensions
                ),
                    "openai_compatible_embeddings_dims_mismatch",
                ));
            }
        }
        if vectors.len() != inputs.len() {
            return Err(AttemptError::invalid_response(
                format!(
                    "openai-compatible embeddings response returned {} vectors for {} inputs",
                    vectors.len(),
                    inputs.len()
                ),
                "openai_compatible_embeddings_vector_count_mismatch",
            ));
        }

        Ok(EmbeddingsResponse {
            model_name: parsed.model.unwrap_or_else(|| model_name.clone()),
            dimensions,
            vectors,
            retry_count: 0,
        })
    }
}

impl EmbeddingsProvider for OpenAiCompatibleEmbeddingsProvider {
    fn embed<'a>(
        &'a self,
        request: EmbeddingsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<EmbeddingsResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let normalized_inputs = normalize_embeddings_inputs(&request)?;
            let Some(api_key) = self.config.openai_api_key.as_ref() else {
                return Err(ProviderError::MissingApiKey);
            };
            if self.config.openai_embeddings_model.is_none() {
                return Err(ProviderError::MissingEmbeddingsModel);
            }

            let mut retry_count = 0_u32;
            for attempt in 0..=self.config.max_retries {
                match self.request_once(api_key.as_str(), normalized_inputs.as_slice()).await {
                    Ok(mut response) => {
                        response.retry_count = retry_count;
                        return Ok(response);
                    }
                    Err(error) => {
                        let can_retry = error.retryable && attempt < self.config.max_retries;
                        if can_retry {
                            tokio::time::sleep(self.backoff_for_retry(retry_count)).await;
                            retry_count = retry_count.saturating_add(1);
                            continue;
                        }
                        return Err(if error.invalid_response {
                            ProviderError::InvalidResponse {
                                message: error.message,
                                retry_count,
                                classification: error.classification,
                            }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                                classification: error.classification,
                            }
                        });
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "openai-compatible embeddings execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
                classification: retry_provider_classification(
                    "openai_compatible_embeddings_retries_exhausted",
                ),
            })
        })
    }
}

impl OpenAiCompatibleProvider {
    fn new(config: &ModelProviderConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .context("failed to build openai-compatible provider HTTP client")?;
        Ok(Self {
            config: config.clone(),
            client,
            circuit_state: Mutex::new(CircuitBreakerState {
                consecutive_failures: 0,
                open_until: None,
            }),
            runtime_metrics: Mutex::new(ProviderRuntimeMetrics::default()),
        })
    }

    fn record_runtime_metrics(
        &self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
        failure: Option<ProviderFailureSnapshot>,
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms, failure);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }

    // Once the cooldown elapses the breaker closes fully and the failure
    // count resets; there is no half-open probe state.
    fn ensure_circuit_closed(&self) -> Result<(), ProviderError> {
        let now = Instant::now();
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        if let Some(open_until) = state.open_until {
            if now < open_until {
                let retry_after_ms = open_until.saturating_duration_since(now).as_millis() as u64;
                return Err(ProviderError::CircuitOpen { retry_after_ms });
            }
            state.open_until = None;
            state.consecutive_failures = 0;
        }
        Ok(())
    }

    fn record_success(&self) -> Result<(), ProviderError> {
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        state.consecutive_failures = 0;
        state.open_until = None;
        Ok(())
    }

    fn record_failure(&self) -> Result<(), ProviderError> {
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.config.circuit_breaker_failure_threshold {
            state.open_until = Some(
                Instant::now() + Duration::from_millis(self.config.circuit_breaker_cooldown_ms),
            );
        }
        Ok(())
    }

    // Exponential backoff (base * 2^retry) with the exponent capped at 8 so
    // the multiplier stays bounded at 256x even for large retry budgets.
    fn backoff_for_retry(&self, retry_index: u32) -> Duration {
        let exponent = retry_index.min(8);
        let multiplier = 1_u64 << exponent;
        Duration::from_millis(self.config.retry_backoff_ms.saturating_mul(multiplier))
    }

    fn chat_completions_endpoint(&self) -> String {
        format!(
            "{}{}",
            self.config.openai_base_url.trim_end_matches('/'),
            OPENAI_CHAT_COMPLETIONS_PATH
        )
    }

    fn codex_responses_endpoint(&self) -> String {
        format!("{}{}", self.codex_responses_base_url(), OPENAI_CODEX_RESPONSES_PATH)
    }

    fn codex_responses_base_url(&self) -> String {
        let configured = self.config.openai_base_url.trim_end_matches('/');
        if configured.to_ascii_lowercase().contains("api.openai.com") {
            OPENAI_CODEX_RESPONSES_BASE_URL.to_owned()
        } else {
            configured.to_owned()
        }
    }

    fn audio_transcriptions_endpoint(&self) -> String {
        format!(
            "{}{}",
            self.config.openai_base_url.trim_end_matches('/'),
            OPENAI_AUDIO_TRANSCRIPTIONS_PATH
        )
    }

    fn transcription_model_name(&self) -> Option<&str> {
        configured_model_id(self.config.openai_model.as_str())
            .filter(|model_id| model_id.contains("transcribe"))
    }

    fn request_with_config_overrides(
        &self,
        request: &ProviderRequest,
    ) -> Result<ProviderRequest, AttemptError> {
        let mut effective = request.clone();
        if effective.reasoning_effort.is_none() {
            effective.reasoning_effort = self.config.reasoning_effort;
        }
        if effective.service_tier.is_none() {
            effective.service_tier = self.config.service_tier;
        }
        if let Some(service_tier) = effective.service_tier {
            if !self.openai_service_tier_supported() {
                if service_tier == ProviderServiceTier::Default {
                    effective.service_tier = None;
                } else {
                    return Err(AttemptError::request_failed(
                        format!(
                            "provider '{}' does not support service_tier={}",
                            self.config.kind.as_str(),
                            service_tier.as_str()
                        ),
                        false,
                        user_action_provider_classification("service_tier_unsupported"),
                    ));
                }
            }
        }
        Ok(effective)
    }

    fn openai_service_tier_supported(&self) -> bool {
        self.config
            .auth_profile_provider_kind
            .is_none_or(|kind| kind == ModelProviderAuthProviderKind::Openai)
            && (openai_base_url_supports_service_tier(self.config.openai_base_url.as_str())
                || self
                    .config
                    .openai_api_key
                    .as_deref()
                    .and_then(openai_chatgpt_oauth_claims)
                    .is_some())
    }

    async fn request_once(
        &self,
        api_key: &str,
        request: &ProviderRequest,
    ) -> Result<ProviderResponse, AttemptError> {
        let requested_model_id = selected_chat_model_id(
            request.model_override.as_deref(),
            self.config.openai_model.as_str(),
            "openai-compatible",
            "openai_compatible_chat_model_missing",
        )?;
        if self.uses_codex_responses_transport(api_key) {
            return self.request_once_codex_responses(api_key, request, requested_model_id).await;
        }

        let actual_model_id = requested_model_id.to_owned();
        let adapter = OpenAiCompatibleChatAdapter;
        let effective_request = self.request_with_config_overrides(request)?;
        let body = adapter.request_payload(&effective_request, actual_model_id.as_str());

        let endpoint = self.chat_completions_endpoint();
        let response = self
            .client
            .post(endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                AttemptError::request_failed(
                    format!("openai-compatible request failed: {error}"),
                    true,
                    classify_reqwest_provider_failure("openai_compatible_chat_request", &error),
                )
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
            let retry_after_ms = retry_after_ms_from_response(&response);
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<openai-compatible error body unavailable>".to_owned());
            return Err(AttemptError::request_failed(
                format!(
                    "openai-compatible endpoint returned HTTP {status}: {}",
                    sanitize_remote_error(&body_text)
                ),
                retryable,
                classify_http_provider_failure_with_retry_after(
                    status,
                    retryable,
                    "openai_compatible_chat_http",
                    body_text.as_str(),
                    retry_after_ms,
                ),
            ));
        }

        let parsed = response.json::<OpenAiChatCompletionResponse>().await.map_err(|error| {
            AttemptError::invalid_response(
                format!("openai-compatible response JSON parsing failed: {error}"),
                "openai_compatible_chat_response_json",
            )
        })?;
        let provider_response_id = parsed.id.clone();
        let provider_model_id = parsed.model.clone();
        let system_fingerprint = parsed.system_fingerprint.clone();
        let provider_usage = parsed.usage.as_ref().map(|usage| ProviderUsage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            // Some compatible vendors omit total_tokens; derive it rather
            // than reporting zero usage.
            total_tokens: if usage.total_tokens == 0 {
                usage.prompt_tokens.saturating_add(usage.completion_tokens)
            } else {
                usage.total_tokens
            },
            cache_read_tokens: usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| non_zero_cache_tokens(details.cached_tokens)),
            cache_write_tokens: None,
            source: "provider".to_owned(),
        });
        let choice = parsed.choices.into_iter().next().ok_or_else(|| {
            AttemptError::invalid_response(
                "openai-compatible response did not include choices".to_owned(),
                "openai_compatible_chat_choices_missing",
            )
        })?;

        let mut tool_events = Vec::new();
        for tool_call in choice.message.tool_calls {
            let Some(function) = tool_call.function else {
                continue;
            };
            if function.name.trim().is_empty() {
                continue;
            }
            let input_json =
                normalize_tool_arguments(function.arguments.as_str()).map_err(|error| {
                    AttemptError::invalid_response(
                        format!("openai-compatible tool arguments are invalid: {error}"),
                        "openai_compatible_chat_tool_arguments",
                    )
                })?;
            tool_events.push(ProviderEvent::ToolProposal {
                proposal_id: tool_call.id.unwrap_or_else(|| Ulid::new().to_string()),
                tool_name: function.name,
                input_json,
            });
        }

        let mut completion_text = extract_completion_text(choice.message.content);
        // Some models emit tool calls as inline markup in the text body
        // instead of the structured tool-call field. Recover those into real
        // proposals; malformed markup is retryable because a fresh sample
        // usually produces well-formed output.
        let coerced_raw_tool_markup = if tool_events.is_empty() {
            match coerce_raw_tool_call_markup(completion_text.as_str()).map_err(|error| {
                AttemptError::retryable_invalid_response(
                    format!("openai-compatible raw tool-call markup is invalid: {error}"),
                    "openai_compatible_raw_tool_call_markup",
                )
            })? {
                Some(extraction) => {
                    completion_text = extraction.cleaned_text;
                    tool_events.extend(extraction.tool_events);
                    true
                }
                None => false,
            }
        } else {
            false
        };
        // A blank completion with no tool calls would read as a dead turn
        // downstream; substitute a minimal acknowledgement instead.
        let full_text = if completion_text.trim().is_empty() && tool_events.is_empty() {
            "ack".to_owned()
        } else {
            completion_text
        };
        // Without provider-reported usage, fall back to local estimates and
        // label the source so token accounting can distinguish the two.
        let usage = provider_usage.unwrap_or_else(|| {
            ProviderUsage::new(
                estimate_token_count(request.input_text.as_str()),
                estimate_token_count(full_text.as_str()),
                "estimated",
            )
        });
        let output = provider_output_from_text_and_tools(
            full_text,
            tool_events,
            if coerced_raw_tool_markup {
                ProviderFinishReason::ToolCalls
            } else {
                ProviderFinishReason::from_openai(choice.finish_reason.as_deref())
            },
            usage,
            ProviderRawProviderRefs {
                provider_response_id,
                provider_model_id,
                system_fingerprint,
                provider_trace_ref: Some("openai_compatible_chat".to_owned()),
                stream_spill_ref: None,
            },
        );
        let events = provider_events_from_output(&output);

        Ok(ProviderResponse {
            prompt_tokens: output.usage.prompt_tokens,
            completion_tokens: output.usage.completion_tokens,
            output,
            events,
            retry_count: 0,
            provider_id: "openai-primary".to_owned(),
            model_id: actual_model_id.clone(),
            served_from_cache: false,
            failover_count: 0,
            attempts: vec![ProviderAttemptSummary {
                provider_id: "openai-primary".to_owned(),
                model_id: actual_model_id,
                outcome: "success".to_owned(),
                retryable: false,
                served_from_cache: false,
                reason_code: None,
                state: None,
            }],
        })
    }

    fn uses_codex_responses_transport(&self, api_key: &str) -> bool {
        self.config.auth_profile_provider_kind == Some(ModelProviderAuthProviderKind::Openai)
            && openai_chatgpt_oauth_claims(api_key).is_some()
    }

    async fn request_once_codex_responses(
        &self,
        api_key: &str,
        request: &ProviderRequest,
        requested_model_id: &str,
    ) -> Result<ProviderResponse, AttemptError> {
        let actual_model_id = openai_codex_runtime_model_id(requested_model_id);
        let adapter = OpenAiResponsesChatAdapter;
        let effective_request = self.request_with_config_overrides(request)?;
        let body = adapter.request_payload(&effective_request, actual_model_id.as_str());
        let body_text = match self.send_codex_responses_payload(api_key, &body).await {
            Ok(body_text) => body_text,
            Err(error)
                if codex_unsupported_content_type_retryable_with_text_replay(
                    &error,
                    &effective_request,
                ) =>
            {
                let fallback_request = codex_text_replay_fallback_request(&effective_request);
                let fallback_body =
                    adapter.request_payload(&fallback_request, actual_model_id.as_str());
                self.send_codex_responses_payload(api_key, &fallback_body)
                    .await
                    .map_err(|fallback_error| {
                        AttemptError::request_failed(
                            format!(
                                "openai-codex responses compatibility retry failed after unsupported content type: {}; original failure: {}",
                                fallback_error.message, error.message
                            ),
                            fallback_error.retryable,
                            fallback_error.classification,
                        )
                    })?
            }
            Err(error) => return Err(error),
        };
        let parsed = parse_openai_codex_sse_response(body_text.as_str()).map_err(|error| {
            AttemptError::invalid_response(
                format!("openai-codex responses SSE parsing failed: {error}"),
                "openai_codex_responses_sse",
            )
        })?;
        openai_codex_provider_response(parsed, request, actual_model_id)
    }

    async fn send_codex_responses_payload(
        &self,
        api_key: &str,
        body: &Value,
    ) -> Result<String, AttemptError> {
        let endpoint = self.codex_responses_endpoint();
        let mut builder = self
            .client
            .post(endpoint)
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Accept", "text/event-stream")
            .header("User-Agent", OPENAI_CODEX_USER_AGENT)
            .header("originator", OPENAI_CODEX_ORIGINATOR)
            .json(body);
        if let Some(account_id) = openai_chatgpt_account_id_from_token(api_key) {
            builder = builder.header("ChatGPT-Account-ID", account_id);
        }

        let response = builder.send().await.map_err(|error| {
            AttemptError::request_failed(
                format!("openai-codex responses request failed: {error}"),
                true,
                classify_reqwest_provider_failure("openai_codex_responses_request", &error),
            )
        })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
            let retry_after_ms = retry_after_ms_from_response(&response);
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<openai-codex error body unavailable>".to_owned());
            return Err(AttemptError::request_failed(
                format!(
                    "openai-codex responses endpoint returned HTTP {status}: {}",
                    sanitize_remote_error(&body_text)
                ),
                retryable,
                classify_http_provider_failure_with_retry_after(
                    status,
                    retryable,
                    "openai_codex_responses_http",
                    body_text.as_str(),
                    retry_after_ms,
                ),
            ));
        }

        response.text().await.map_err(|error| {
            AttemptError::request_failed(
                format!("openai-codex responses stream read failed: {error}"),
                true,
                classify_reqwest_provider_failure("openai_codex_responses_body", &error),
            )
        })
    }

    async fn transcribe_audio_once(
        &self,
        api_key: &str,
        request: &AudioTranscriptionRequest,
    ) -> Result<AudioTranscriptionResponse, AttemptError> {
        let transcription_model = self.transcription_model_name().ok_or_else(|| {
            AttemptError::request_failed(
                "openai-compatible provider has no discovered audio transcription model configured"
                    .to_owned(),
                false,
                fail_closed_provider_classification(
                    "openai_compatible_audio_transcription_model_missing",
                ),
            )
        })?;
        let file_part = reqwest::multipart::Part::bytes(request.bytes.clone())
            .file_name(request.file_name.clone())
            .mime_str(request.content_type.as_str())
            .map_err(|error| {
                AttemptError::request_failed(
                    format!("invalid audio transcription content type: {error}"),
                    false,
                    user_action_provider_classification(
                        "openai_compatible_audio_content_type_invalid",
                    ),
                )
            })?;
        let mut form = reqwest::multipart::Form::new()
            .text("model", transcription_model.to_owned())
            .text("response_format", "verbose_json".to_owned())
            .part("file", file_part);
        if let Some(language) =
            request.language.as_deref().map(str::trim).filter(|value| !value.is_empty())
        {
            form = form.text("language", language.to_owned());
        }
        if let Some(prompt) =
            request.prompt.as_deref().map(str::trim).filter(|value| !value.is_empty())
        {
            form = form.text("prompt", prompt.to_owned());
        }

        let response = self
            .client
            .post(self.audio_transcriptions_endpoint())
            .header("Authorization", format!("Bearer {api_key}"))
            .multipart(form)
            .send()
            .await
            .map_err(|error| {
                AttemptError::request_failed(
                    format!("openai-compatible audio transcription request failed: {error}"),
                    true,
                    classify_reqwest_provider_failure("openai_compatible_audio_request", &error),
                )
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
            let retry_after_ms = retry_after_ms_from_response(&response);
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<openai-compatible error body unavailable>".to_owned());
            return Err(AttemptError::request_failed(
                format!(
                    "openai-compatible audio transcription endpoint returned HTTP {status}: {}",
                    sanitize_remote_error(&body_text)
                ),
                retryable,
                classify_http_provider_failure_with_retry_after(
                    status,
                    retryable,
                    "openai_compatible_audio_http",
                    body_text.as_str(),
                    retry_after_ms,
                ),
            ));
        }

        let parsed =
            response.json::<OpenAiAudioTranscriptionResponse>().await.map_err(|error| {
                AttemptError::invalid_response(
                    format!(
                    "openai-compatible audio transcription response JSON parsing failed: {error}"
                ),
                    "openai_compatible_audio_response_json",
                )
            })?;
        let segments = parsed
            .segments
            .into_iter()
            .filter(|segment| !segment.text.trim().is_empty())
            .map(|segment| AudioTranscriptionSegment {
                start_ms: provider_seconds_to_millis(segment.start.unwrap_or_default()),
                end_ms: provider_seconds_to_millis(segment.end.unwrap_or_default()),
                text: segment.text,
                // OpenAI verbose_json reports avg_logprob; exp() maps it back
                // to an approximate per-segment probability in (0, 1].
                confidence: segment.avg_logprob.map(|value| value.exp()),
            })
            .collect::<Vec<_>>();
        Ok(AudioTranscriptionResponse {
            text: parsed.text,
            language: parsed.language,
            duration_ms: parsed.duration.map(provider_seconds_to_millis),
            model_name: transcription_model.to_owned(),
            retry_count: 0,
            segments,
        })
    }
}

fn openai_chatgpt_oauth_claims(token: &str) -> Option<Value> {
    let claims = decode_jwt_payload(token)?;
    if claims.get(OPENAI_CHATGPT_AUTH_CLAIM_NAMESPACE).is_some()
        || claims.get(OPENAI_CHATGPT_ACCOUNT_ID_CLAIM).is_some()
    {
        Some(claims)
    } else {
        None
    }
}

fn openai_chatgpt_account_id_from_token(token: &str) -> Option<String> {
    let claims = openai_chatgpt_oauth_claims(token)?;
    claims
        .get(OPENAI_CHATGPT_AUTH_CLAIM_NAMESPACE)
        .and_then(|namespace| namespace.get(OPENAI_CHATGPT_ACCOUNT_ID_CLAIM))
        .or_else(|| claims.get(OPENAI_CHATGPT_ACCOUNT_ID_CLAIM))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn openai_base_url_supports_service_tier(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("api.openai.com") {
        return true;
    }
    host.eq_ignore_ascii_case("chatgpt.com")
        && url.path().trim_end_matches('/').eq_ignore_ascii_case("/backend-api/codex")
}

fn decode_jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    if payload.is_empty() {
        return None;
    }
    let mut padded = payload.to_owned();
    match padded.len() % 4 {
        0 => {}
        2 => padded.push_str("=="),
        3 => padded.push('='),
        _ => return None,
    }
    let decoded = URL_SAFE.decode(padded.as_bytes()).ok()?;
    serde_json::from_slice(decoded.as_slice()).ok()
}

fn openai_codex_runtime_model_id(configured_model_id: &str) -> String {
    configured_model_id.rsplit('/').next().unwrap_or(configured_model_id).trim().to_owned()
}

fn parse_openai_codex_sse_response(body: &str) -> Result<OpenAiResponsesResponse> {
    let mut parsed = OpenAiResponsesResponse {
        id: None,
        model: None,
        status: None,
        output_text: None,
        usage: None,
        output: Vec::new(),
    };
    let mut streamed_text = String::new();

    for line in body.lines() {
        let line = line.trim();
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let event: Value = serde_json::from_str(data)
            .with_context(|| format!("invalid OpenAI Codex SSE data frame: {data}"))?;
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
        match event_type {
            "response.output_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    streamed_text.push_str(delta);
                }
            }
            "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    parsed.output.push(
                        serde_json::from_value(item.clone())
                            .context("invalid OpenAI Codex output item frame")?,
                    );
                }
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                if event_type == "response.incomplete" && parsed.status.is_none() {
                    parsed.status = Some("incomplete".to_owned());
                } else if event_type == "response.failed" && parsed.status.is_none() {
                    parsed.status = Some("failed".to_owned());
                }
                if let Some(response) = event.get("response") {
                    apply_openai_codex_terminal_response(&mut parsed, response)?;
                }
            }
            "error" => {
                let message = event
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("stream emitted error event");
                anyhow::bail!("OpenAI Codex SSE error event: {}", sanitize_remote_error(message));
            }
            _ => {}
        }
    }

    if !streamed_text.is_empty() {
        parsed.output_text = Some(streamed_text);
    }
    if parsed.status.is_none() && (parsed.output_text.is_some() || !parsed.output.is_empty()) {
        parsed.status = Some("completed".to_owned());
    }
    if parsed.output_text.is_none() && parsed.output.is_empty() {
        anyhow::bail!("OpenAI Codex SSE stream did not include output text or output items");
    }
    Ok(parsed)
}

fn apply_openai_codex_terminal_response(
    parsed: &mut OpenAiResponsesResponse,
    response: &Value,
) -> Result<()> {
    if parsed.id.is_none() {
        parsed.id = response.get("id").and_then(Value::as_str).map(ToOwned::to_owned);
    }
    if parsed.model.is_none() {
        parsed.model = response.get("model").and_then(Value::as_str).map(ToOwned::to_owned);
    }
    if parsed.status.is_none() {
        parsed.status = response.get("status").and_then(Value::as_str).map(ToOwned::to_owned);
    }
    if parsed.output_text.is_none() {
        parsed.output_text =
            response.get("output_text").and_then(Value::as_str).map(ToOwned::to_owned);
    }
    if parsed.usage.is_none() {
        parsed.usage = response
            .get("usage")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .context("invalid OpenAI Codex usage frame")?;
    }
    if parsed.output.is_empty() {
        if let Some(items) = response.get("output").and_then(Value::as_array) {
            for item in items {
                parsed.output.push(
                    serde_json::from_value(item.clone())
                        .context("invalid OpenAI Codex terminal output item")?,
                );
            }
        }
    }
    Ok(())
}

fn openai_codex_provider_response(
    parsed: OpenAiResponsesResponse,
    request: &ProviderRequest,
    actual_model_id: String,
) -> Result<ProviderResponse, AttemptError> {
    let provider_response_id = parsed.id.clone();
    let provider_model_id = parsed.model.clone();
    let finish_status = parsed.status.clone();
    let provider_usage = parsed.usage.as_ref().map(|usage| ProviderUsage {
        prompt_tokens: usage.input_tokens,
        completion_tokens: usage.output_tokens,
        total_tokens: if usage.total_tokens == 0 {
            usage.input_tokens.saturating_add(usage.output_tokens)
        } else {
            usage.total_tokens
        },
        cache_read_tokens: usage
            .input_tokens_details
            .as_ref()
            .and_then(|details| non_zero_cache_tokens(details.cached_tokens)),
        cache_write_tokens: None,
        source: "provider".to_owned(),
    });

    let original_tool_names = openai_codex_original_tool_names_by_wire_name(request);
    let mut text_parts = Vec::new();
    let mut tool_events = Vec::new();
    for item in parsed.output {
        match item.kind.as_str() {
            "message" => {
                for part in item.content {
                    if matches!(part.kind.as_str(), "output_text" | "text") {
                        if let Some(text) = part.text.filter(|text| !text.is_empty()) {
                            text_parts.push(text);
                        }
                    }
                }
            }
            "function_call" => {
                let Some(wire_tool_name) = item.name.map(|name| name.trim().to_owned()) else {
                    continue;
                };
                if wire_tool_name.is_empty() {
                    continue;
                }
                let tool_name = original_tool_names
                    .get(wire_tool_name.as_str())
                    .cloned()
                    .unwrap_or(wire_tool_name);
                let arguments = openai_responses_tool_arguments(item.arguments);
                let input_json = normalize_tool_arguments(arguments.as_str()).map_err(|error| {
                    AttemptError::invalid_response(
                        format!("openai-codex tool arguments are invalid: {error}"),
                        "openai_codex_responses_tool_arguments",
                    )
                })?;
                tool_events.push(ProviderEvent::ToolProposal {
                    proposal_id: item
                        .call_id
                        .or(item.id)
                        .unwrap_or_else(|| Ulid::new().to_string()),
                    tool_name,
                    input_json,
                });
            }
            _ => {}
        }
    }
    if text_parts.is_empty() {
        if let Some(output_text) = parsed.output_text.filter(|text| !text.trim().is_empty()) {
            text_parts.push(output_text);
        }
    }

    let completion_text = text_parts.join("");
    let full_text = if completion_text.trim().is_empty() && tool_events.is_empty() {
        "ack".to_owned()
    } else {
        completion_text
    };
    let usage = provider_usage.unwrap_or_else(|| {
        ProviderUsage::new(
            estimate_token_count(request.input_text.as_str()),
            estimate_token_count(full_text.as_str()),
            "estimated",
        )
    });
    let finish_reason = if tool_events.is_empty() {
        finish_reason_from_openai_responses_status(finish_status.as_deref())
    } else {
        ProviderFinishReason::ToolCalls
    };
    let output = provider_output_from_text_and_tools(
        full_text,
        tool_events,
        finish_reason,
        usage,
        ProviderRawProviderRefs {
            provider_response_id,
            provider_model_id,
            system_fingerprint: None,
            provider_trace_ref: Some("openai_codex_responses".to_owned()),
            stream_spill_ref: None,
        },
    );
    let events = provider_events_from_output(&output);

    Ok(ProviderResponse {
        prompt_tokens: output.usage.prompt_tokens,
        completion_tokens: output.usage.completion_tokens,
        output,
        events,
        retry_count: 0,
        provider_id: "openai-primary".to_owned(),
        model_id: actual_model_id.clone(),
        served_from_cache: false,
        failover_count: 0,
        attempts: vec![ProviderAttemptSummary {
            provider_id: "openai-primary".to_owned(),
            model_id: actual_model_id,
            outcome: "success".to_owned(),
            retryable: false,
            served_from_cache: false,
            reason_code: None,
            state: None,
        }],
    })
}

fn openai_codex_original_tool_names_by_wire_name(
    request: &ProviderRequest,
) -> HashMap<String, String> {
    let Some(snapshot) = request.tool_catalog_snapshot.as_ref() else {
        return HashMap::new();
    };
    openai_responses_tool_wire_name_map(snapshot)
        .into_iter()
        .map(|(original_name, wire_name)| (wire_name, original_name))
        .collect()
}

fn openai_responses_tool_arguments(arguments: Option<Value>) -> String {
    match arguments {
        Some(Value::String(arguments)) => {
            let trimmed = arguments.trim();
            if trimmed.is_empty() {
                "{}".to_owned()
            } else {
                trimmed.to_owned()
            }
        }
        Some(Value::Null) | None => "{}".to_owned(),
        Some(arguments) => serde_json::to_string(&arguments).unwrap_or_else(|_| "{}".to_owned()),
    }
}

fn codex_unsupported_content_type_retryable_with_text_replay(
    error: &AttemptError,
    request: &ProviderRequest,
) -> bool {
    if error.retryable || error.invalid_response {
        return false;
    }
    let message = error.message.to_ascii_lowercase();
    message.contains("openai-codex responses endpoint returned http 400")
        && message.contains("unsupported content type")
        && request.messages.iter().any(|message| message.role == ProviderMessageRole::Tool)
}

fn codex_text_replay_fallback_request(request: &ProviderRequest) -> ProviderRequest {
    let source_messages = request.effective_messages();
    let transcript = codex_text_replay_transcript(&source_messages);
    let mut messages = source_messages
        .iter()
        .filter(|message| {
            matches!(message.role, ProviderMessageRole::System | ProviderMessageRole::Developer)
        })
        .cloned()
        .collect::<Vec<_>>();
    messages.push(ProviderMessage::user_text(transcript.clone()));

    let mut fallback = request.clone();
    fallback.input_text = transcript;
    fallback.messages = messages;
    fallback.vision_inputs.clear();
    fallback
}

fn codex_text_replay_transcript(messages: &[ProviderMessage]) -> String {
    let mut transcript = String::new();
    let mut truncated = false;
    codex_append_bounded_text(
        &mut transcript,
        "Codex Responses strict tool-result replay returned HTTP 400 Unsupported content type. Continue from this model-visible transcript of the same conversation and successful tool evidence. Do not repeat completed side effects unless fresh verification is required.\n",
        &mut truncated,
    );
    codex_append_bounded_text(
        &mut transcript,
        "Original provider error class: HTTP 400 Unsupported content type.\n",
        &mut truncated,
    );
    codex_append_bounded_text(&mut transcript, "\n\n", &mut truncated);

    for message in messages {
        match message.role {
            ProviderMessageRole::System | ProviderMessageRole::Developer => {}
            ProviderMessageRole::User => {
                codex_append_bounded_text(&mut transcript, "[user]\n", &mut truncated);
                codex_append_bounded_text(
                    &mut transcript,
                    message.text_content().as_str(),
                    &mut truncated,
                );
                codex_append_bounded_text(&mut transcript, "\n\n", &mut truncated);
            }
            ProviderMessageRole::Assistant => {
                let text = message.text_content();
                if !text.trim().is_empty() {
                    codex_append_bounded_text(&mut transcript, "[assistant]\n", &mut truncated);
                    codex_append_bounded_text(&mut transcript, text.as_str(), &mut truncated);
                    codex_append_bounded_text(&mut transcript, "\n", &mut truncated);
                }
                for tool_call in &message.tool_calls {
                    let arguments = serde_json::to_string(&tool_call.input_json)
                        .unwrap_or_else(|_| "{}".to_owned());
                    codex_append_bounded_text(
                        &mut transcript,
                        format!(
                            "[assistant_tool_call call_id={} name={} arguments={}]\n",
                            tool_call.proposal_id, tool_call.tool_name, arguments
                        )
                        .as_str(),
                        &mut truncated,
                    );
                }
                if !message.tool_calls.is_empty() {
                    codex_append_bounded_text(&mut transcript, "\n", &mut truncated);
                }
            }
            ProviderMessageRole::Tool => {
                let call_id = message.tool_call_id.as_deref().unwrap_or("unknown");
                codex_append_bounded_text(
                    &mut transcript,
                    format!("[tool_result call_id={call_id}]\n").as_str(),
                    &mut truncated,
                );
                codex_append_bounded_text(
                    &mut transcript,
                    message.text_content().as_str(),
                    &mut truncated,
                );
                codex_append_bounded_text(&mut transcript, "\n\n", &mut truncated);
            }
        }
    }

    if truncated {
        let marker = "\n[transcript truncated at compatibility replay byte limit]\n";
        if transcript.len().saturating_add(marker.len()) <= CODEX_TEXT_REPLAY_FALLBACK_MAX_BYTES {
            transcript.push_str(marker);
        }
    }
    transcript
}

fn codex_append_bounded_text(target: &mut String, text: &str, truncated: &mut bool) {
    if *truncated || target.len() >= CODEX_TEXT_REPLAY_FALLBACK_MAX_BYTES {
        *truncated = true;
        return;
    }
    let remaining = CODEX_TEXT_REPLAY_FALLBACK_MAX_BYTES.saturating_sub(target.len());
    if text.len() <= remaining {
        target.push_str(text);
        return;
    }
    let mut boundary = remaining;
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    target.push_str(&text[..boundary]);
    *truncated = true;
}

fn finish_reason_from_openai_responses_status(status: Option<&str>) -> ProviderFinishReason {
    match status.unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "completed" => ProviderFinishReason::Stop,
        "incomplete" => ProviderFinishReason::Length,
        "failed" => ProviderFinishReason::Error,
        "cancelled" | "canceled" => ProviderFinishReason::Cancelled,
        _ => ProviderFinishReason::Unknown,
    }
}

impl ModelProvider for OpenAiCompatibleProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let started_at = Instant::now();
            let Some(api_key) = self.config.openai_api_key.as_ref() else {
                let error = ProviderError::MissingApiKey;
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    0,
                    elapsed_millis_since(started_at),
                    Some(error.failure_snapshot()),
                );
                return Err(error);
            };
            if let Err(error) = self.ensure_circuit_closed() {
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    error.retry_count(),
                    elapsed_millis_since(started_at),
                    Some(error.failure_snapshot()),
                );
                return Err(error);
            }

            let mut retry_count = 0_u32;
            for attempt in 0..=self.config.max_retries {
                match self.request_once(api_key.as_str(), &request).await {
                    Ok(mut response) => {
                        self.record_success()?;
                        response.retry_count = retry_count;
                        self.record_runtime_metrics(
                            false,
                            response.prompt_tokens,
                            response.completion_tokens,
                            response.retry_count,
                            elapsed_millis_since(started_at),
                            None,
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        let can_retry = error.retryable && attempt < self.config.max_retries;
                        if can_retry {
                            tokio::time::sleep(self.backoff_for_retry(retry_count)).await;
                            retry_count = retry_count.saturating_add(1);
                            continue;
                        }

                        self.record_failure()?;
                        let provider_error = if error.invalid_response {
                            ProviderError::InvalidResponse {
                                message: error.message,
                                retry_count,
                                classification: error.classification,
                            }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                                classification: error.classification,
                            }
                        };
                        self.record_runtime_metrics(
                            true,
                            0,
                            0,
                            provider_error.retry_count(),
                            elapsed_millis_since(started_at),
                            Some(provider_error.failure_snapshot()),
                        );
                        return Err(provider_error);
                    }
                }
            }

            let exhausted_error = ProviderError::RequestFailed {
                message: "openai-compatible execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
                classification: retry_provider_classification(
                    "openai_compatible_chat_retries_exhausted",
                ),
            };
            self.record_runtime_metrics(
                true,
                0,
                0,
                exhausted_error.retry_count(),
                elapsed_millis_since(started_at),
                Some(exhausted_error.failure_snapshot()),
            );
            Err(exhausted_error)
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(api_key) = self.config.openai_api_key.as_ref() else {
                return Err(ProviderError::MissingApiKey);
            };
            self.ensure_circuit_closed()?;

            let mut retry_count = 0_u32;
            for attempt in 0..=self.config.max_retries {
                match self.transcribe_audio_once(api_key.as_str(), &request).await {
                    Ok(mut response) => {
                        self.record_success()?;
                        response.retry_count = retry_count;
                        return Ok(response);
                    }
                    Err(error) => {
                        let can_retry = error.retryable && attempt < self.config.max_retries;
                        if can_retry {
                            tokio::time::sleep(self.backoff_for_retry(retry_count)).await;
                            retry_count = retry_count.saturating_add(1);
                            continue;
                        }
                        self.record_failure()?;
                        return Err(if error.invalid_response {
                            ProviderError::InvalidResponse {
                                message: error.message,
                                retry_count,
                                classification: error.classification,
                            }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                                classification: error.classification,
                            }
                        });
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "openai-compatible audio transcription exhausted retries".to_owned(),
                retryable: true,
                retry_count,
                classification: retry_provider_classification(
                    "openai_compatible_audio_retries_exhausted",
                ),
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let (consecutive_failures, open) = self
            .circuit_state
            .lock()
            .map(|state| {
                let now = Instant::now();
                let open = state.open_until.is_some_and(|until| now < until);
                (state.consecutive_failures, open)
            })
            .unwrap_or((0, false));
        let chat_model_id =
            configured_model_id(self.config.openai_model.as_str()).map(ToOwned::to_owned);
        let discovered_model_ids = chat_model_id
            .clone()
            .into_iter()
            .chain(self.config.openai_embeddings_model.clone())
            .collect::<Vec<_>>();
        let service_tier_supported = self.openai_service_tier_supported();
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "openai-primary".to_owned(),
            credential_id: normalized_provider_credential_id(
                "openai-primary",
                self.config.auth_profile_id.as_deref(),
                self.config.credential_source,
            ),
            model_id: chat_model_id.clone(),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: true,
                audio_transcribe: true,
                embeddings: self.config.openai_embeddings_model.is_some(),
                reasoning: chat_model_id.as_deref().is_some_and(model_id_supports_reasoning_effort),
                reasoning_efforts: if chat_model_id
                    .as_deref()
                    .is_some_and(model_id_supports_reasoning_effort)
                {
                    vec![
                        ProviderReasoningEffort::None.as_str().to_owned(),
                        ProviderReasoningEffort::Minimal.as_str().to_owned(),
                        ProviderReasoningEffort::Low.as_str().to_owned(),
                        ProviderReasoningEffort::Medium.as_str().to_owned(),
                        ProviderReasoningEffort::High.as_str().to_owned(),
                        ProviderReasoningEffort::XHigh.as_str().to_owned(),
                    ]
                } else {
                    Vec::new()
                },
                service_tier: service_tier_supported,
                service_tiers: if service_tier_supported {
                    default_provider_service_tiers()
                } else {
                    Vec::new()
                },
                max_context_tokens: Some(128_000),
                cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
                recommended_use_cases: vec![
                    "general chat".to_owned(),
                    "JSON workflows".to_owned(),
                    "vision requests".to_owned(),
                ],
                known_limitations: vec![],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            },
            openai_base_url: Some(self.config.openai_base_url.clone()),
            anthropic_base_url: None,
            openai_model: chat_model_id.clone(),
            anthropic_model: None,
            openai_embeddings_model: self.config.openai_embeddings_model.clone(),
            openai_embeddings_dims: self.config.openai_embeddings_dims,
            auth_profile_id: self.config.auth_profile_id.clone(),
            auth_profile_provider_kind: self
                .config
                .auth_profile_provider_kind
                .map(|kind| kind.as_str().to_owned()),
            credential_source: self
                .config
                .credential_source
                .map(|source| source.as_str().to_owned()),
            api_key_configured: self.config.openai_api_key.is_some(),
            retry_policy: ProviderRetryPolicySnapshot {
                max_retries: self.config.max_retries,
                retry_backoff_ms: self.config.retry_backoff_ms,
            },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: self.config.circuit_breaker_failure_threshold,
                cooldown_ms: self.config.circuit_breaker_cooldown_ms,
                consecutive_failures,
                open,
            },
            runtime_metrics: self.runtime_metrics_snapshot(),
            response_cache: empty_response_cache_snapshot(response_cache_enabled_from_config(
                &self.config,
            )),
            health: if self.config.openai_api_key.is_some() || self.config.auth_profile_id.is_some()
            {
                empty_health_probe_snapshot("ok", "provider configured", "runtime")
            } else {
                empty_health_probe_snapshot("missing_auth", "provider has no credential", "runtime")
            },
            discovery: ProviderDiscoverySnapshot {
                status: if discovered_model_ids.is_empty() { "pending" } else { "static" }
                    .to_owned(),
                checked_at_unix_ms: None,
                expires_at_unix_ms: None,
                discovered_model_ids,
                source: "static".to_owned(),
                message: if chat_model_id.is_some() {
                    None
                } else {
                    Some("no provider-discovered models are configured yet".to_owned())
                },
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: chat_model_id,
                default_embeddings_model_id: self.config.openai_embeddings_model.clone(),
                default_audio_transcription_model_id: self
                    .transcription_model_name()
                    .map(ToOwned::to_owned),
                failover_enabled: true,
                response_cache_enabled: true,
                providers: Vec::new(),
                credentials: Vec::new(),
                models: Vec::new(),
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot.route_selection = route_selection_from_status_snapshot(&snapshot);
        snapshot
    }
}

/// HTTP backend for the Anthropic messages API and Anthropic-compatible
/// endpoints (MiniMax), with retries and a per-provider circuit breaker.
#[derive(Debug)]
struct AnthropicProvider {
    config: ModelProviderConfig,
    client: Client,
    circuit_state: Mutex<CircuitBreakerState>,
    runtime_metrics: Mutex<ProviderRuntimeMetrics>,
}

impl AnthropicProvider {
    fn new(config: &ModelProviderConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .context("failed to build anthropic provider HTTP client")?;
        Ok(Self {
            config: config.clone(),
            client,
            circuit_state: Mutex::new(CircuitBreakerState {
                consecutive_failures: 0,
                open_until: None,
            }),
            runtime_metrics: Mutex::new(ProviderRuntimeMetrics::default()),
        })
    }

    fn record_runtime_metrics(
        &self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
        failure: Option<ProviderFailureSnapshot>,
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms, failure);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }

    // Once the cooldown elapses the breaker closes fully and the failure
    // count resets; there is no half-open probe state.
    fn ensure_circuit_closed(&self) -> Result<(), ProviderError> {
        let now = Instant::now();
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        if let Some(open_until) = state.open_until {
            if now < open_until {
                let retry_after_ms = open_until.saturating_duration_since(now).as_millis() as u64;
                return Err(ProviderError::CircuitOpen { retry_after_ms });
            }
            state.open_until = None;
            state.consecutive_failures = 0;
        }
        Ok(())
    }

    fn record_success(&self) -> Result<(), ProviderError> {
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        state.consecutive_failures = 0;
        state.open_until = None;
        Ok(())
    }

    fn record_failure(&self) -> Result<(), ProviderError> {
        let mut state = self.circuit_state.lock().map_err(|_| ProviderError::StatePoisoned)?;
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.config.circuit_breaker_failure_threshold {
            state.open_until = Some(
                Instant::now() + Duration::from_millis(self.config.circuit_breaker_cooldown_ms),
            );
        }
        Ok(())
    }

    // Exponential backoff (base * 2^retry) with the exponent capped at 8 so
    // the multiplier stays bounded at 256x even for large retry budgets.
    fn backoff_for_retry(&self, retry_index: u32) -> Duration {
        let exponent = retry_index.min(8);
        let multiplier = 1_u64 << exponent;
        Duration::from_millis(self.config.retry_backoff_ms.saturating_mul(multiplier))
    }

    fn messages_endpoint(&self) -> String {
        format!(
            "{}{}",
            self.config.anthropic_base_url.trim_end_matches('/'),
            ANTHROPIC_MESSAGES_PATH
        )
    }

    async fn request_once(
        &self,
        api_key: &str,
        request: &ProviderRequest,
    ) -> Result<ProviderResponse, AttemptError> {
        let model_name = selected_chat_model_id(
            request.model_override.as_deref(),
            self.config.anthropic_model.as_str(),
            "anthropic-compatible",
            "anthropic_chat_model_missing",
        )?;
        if let Some(service_tier) = request.service_tier.or(self.config.service_tier) {
            if service_tier != ProviderServiceTier::Default {
                return Err(AttemptError::request_failed(
                    format!(
                        "provider 'anthropic' does not support service_tier={}",
                        service_tier.as_str()
                    ),
                    false,
                    user_action_provider_classification("service_tier_unsupported"),
                ));
            }
        }
        let adapter = AnthropicCompatibleChatAdapter;
        let body = adapter.request_payload(request, model_name);

        let request_builder = self
            .client
            .post(self.messages_endpoint())
            .header("anthropic-version", ANTHROPIC_API_VERSION);
        let request_builder = if anthropic_compatible_uses_bearer_auth(
            self.config.auth_profile_provider_kind,
            self.config.credential_source,
        ) {
            request_builder.bearer_auth(api_key)
        } else {
            request_builder.header("x-api-key", api_key)
        };
        let request_builder = if anthropic_compatible_uses_anthropic_oauth_headers(
            self.config.auth_profile_provider_kind,
            self.config.credential_source,
        ) {
            request_builder
                .header("anthropic-beta", ANTHROPIC_OAUTH_BETA_HEADER)
                .header("user-agent", ANTHROPIC_OAUTH_USER_AGENT)
        } else {
            request_builder
        };
        let response = request_builder.json(&body).send().await.map_err(|error| {
            AttemptError::request_failed(
                format!("anthropic request failed: {error}"),
                true,
                classify_reqwest_provider_failure("anthropic_chat_request", &error),
            )
        })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
            let retry_after_ms = retry_after_ms_from_response(&response);
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<anthropic error body unavailable>".to_owned());
            return Err(AttemptError::request_failed(
                format!(
                    "anthropic endpoint returned HTTP {status}: {}",
                    sanitize_remote_error(&body_text)
                ),
                retryable,
                classify_http_provider_failure_with_retry_after(
                    status,
                    retryable,
                    "anthropic_chat_http",
                    body_text.as_str(),
                    retry_after_ms,
                ),
            ));
        }

        let parsed = response.json::<AnthropicMessagesResponse>().await.map_err(|error| {
            AttemptError::retryable_invalid_response(
                format!("anthropic response JSON parsing failed: {error}"),
                "anthropic_chat_response_json",
            )
        })?;
        let provider_response_id = parsed.id.clone();
        let provider_model_id = parsed.model.clone();
        let provider_usage = parsed.usage.as_ref().map(|usage| {
            ProviderUsage::new(usage.input_tokens, usage.output_tokens, "provider")
                .with_cache_usage(
                    non_zero_cache_tokens(usage.cache_read_input_tokens),
                    non_zero_cache_tokens(usage.cache_creation_input_tokens),
                )
        });
        let finish_reason = ProviderFinishReason::from_anthropic(parsed.stop_reason.as_deref());
        let mut tool_events = Vec::new();
        let mut completion_fragments = Vec::new();
        for block in parsed.content {
            match block.kind.as_str() {
                "text" => {
                    if let Some(text) = block.text.and_then(trim_to_option) {
                        completion_fragments.push(text);
                    }
                }
                "tool_use" => {
                    let Some(tool_name) = block.name else {
                        continue;
                    };
                    let input_json = normalize_tool_input_value(
                        &block.input.unwrap_or(Value::Object(serde_json::Map::new())),
                    )
                    .map_err(|error| {
                        AttemptError::invalid_response(
                            format!("anthropic tool payload is invalid: {error}"),
                            "anthropic_chat_tool_payload",
                        )
                    })?;
                    tool_events.push(ProviderEvent::ToolProposal {
                        proposal_id: block.id.unwrap_or_else(|| Ulid::new().to_string()),
                        tool_name,
                        input_json,
                    });
                }
                _ => {}
            }
        }

        let mut completion_text = completion_fragments.join("\n");
        // Some models emit tool calls as inline markup in the text body
        // instead of the structured tool-call field. Recover those into real
        // proposals; malformed markup is retryable because a fresh sample
        // usually produces well-formed output.
        let coerced_raw_tool_markup = if tool_events.is_empty() {
            match coerce_raw_tool_call_markup(completion_text.as_str()).map_err(|error| {
                AttemptError::retryable_invalid_response(
                    format!("anthropic raw tool-call markup is invalid: {error}"),
                    "anthropic_raw_tool_call_markup",
                )
            })? {
                Some(extraction) => {
                    completion_text = extraction.cleaned_text;
                    tool_events.extend(extraction.tool_events);
                    true
                }
                None => false,
            }
        } else {
            false
        };
        // A blank completion with no tool calls would read as a dead turn
        // downstream; substitute a minimal acknowledgement instead.
        let full_text = if completion_text.trim().is_empty() && tool_events.is_empty() {
            "ack".to_owned()
        } else {
            completion_text
        };
        // Without provider-reported usage, fall back to local estimates and
        // label the source so token accounting can distinguish the two.
        let usage = provider_usage.unwrap_or_else(|| {
            ProviderUsage::new(
                estimate_token_count(request.input_text.as_str()),
                estimate_token_count(full_text.as_str()),
                "estimated",
            )
        });
        let output = provider_output_from_text_and_tools(
            full_text,
            tool_events,
            if coerced_raw_tool_markup { ProviderFinishReason::ToolCalls } else { finish_reason },
            usage,
            ProviderRawProviderRefs {
                provider_response_id,
                provider_model_id,
                system_fingerprint: None,
                provider_trace_ref: Some("anthropic_chat".to_owned()),
                stream_spill_ref: None,
            },
        );
        let events = provider_events_from_output(&output);

        Ok(ProviderResponse {
            prompt_tokens: output.usage.prompt_tokens,
            completion_tokens: output.usage.completion_tokens,
            output,
            events,
            retry_count: 0,
            provider_id: "anthropic-primary".to_owned(),
            model_id: model_name.to_owned(),
            served_from_cache: false,
            failover_count: 0,
            attempts: vec![ProviderAttemptSummary {
                provider_id: "anthropic-primary".to_owned(),
                model_id: model_name.to_owned(),
                outcome: "success".to_owned(),
                retryable: false,
                served_from_cache: false,
                reason_code: None,
                state: None,
            }],
        })
    }
}

impl ModelProvider for AnthropicProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            let started_at = Instant::now();
            let Some(api_key) = self.config.anthropic_api_key.as_ref() else {
                let error = ProviderError::MissingAnthropicApiKey;
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    0,
                    elapsed_millis_since(started_at),
                    Some(error.failure_snapshot()),
                );
                return Err(error);
            };
            if let Err(error) = self.ensure_circuit_closed() {
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    error.retry_count(),
                    elapsed_millis_since(started_at),
                    Some(error.failure_snapshot()),
                );
                return Err(error);
            }

            let mut retry_count = 0_u32;
            for attempt in 0..=self.config.max_retries {
                match self.request_once(api_key.as_str(), &request).await {
                    Ok(mut response) => {
                        self.record_success()?;
                        response.retry_count = retry_count;
                        self.record_runtime_metrics(
                            false,
                            response.prompt_tokens,
                            response.completion_tokens,
                            response.retry_count,
                            elapsed_millis_since(started_at),
                            None,
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        let can_retry = error.retryable && attempt < self.config.max_retries;
                        if can_retry {
                            tokio::time::sleep(self.backoff_for_retry(retry_count)).await;
                            retry_count = retry_count.saturating_add(1);
                            continue;
                        }

                        self.record_failure()?;
                        let provider_error = if error.invalid_response {
                            ProviderError::InvalidResponse {
                                message: error.message,
                                retry_count,
                                classification: error.classification,
                            }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                                classification: error.classification,
                            }
                        };
                        self.record_runtime_metrics(
                            true,
                            0,
                            0,
                            provider_error.retry_count(),
                            elapsed_millis_since(started_at),
                            Some(provider_error.failure_snapshot()),
                        );
                        return Err(provider_error);
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "anthropic execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
                classification: retry_provider_classification("anthropic_chat_retries_exhausted"),
            })
        })
    }

    fn transcribe_audio<'a>(
        &'a self,
        _request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>
    {
        Box::pin(async {
            Err(ProviderError::RequestFailed {
                message: "anthropic provider does not support audio transcription".to_owned(),
                retryable: false,
                retry_count: 0,
                classification: failover_provider_classification(
                    "anthropic_audio_transcription_unsupported",
                ),
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let (consecutive_failures, open) = self
            .circuit_state
            .lock()
            .map(|state| {
                let now = Instant::now();
                let open = state.open_until.is_some_and(|until| now < until);
                (state.consecutive_failures, open)
            })
            .unwrap_or((0, false));
        let chat_model_id =
            configured_model_id(self.config.anthropic_model.as_str()).map(ToOwned::to_owned);
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "anthropic-primary".to_owned(),
            credential_id: normalized_provider_credential_id(
                "anthropic-primary",
                self.config.auth_profile_id.as_deref(),
                self.config.credential_source,
            ),
            model_id: chat_model_id.clone(),
            capabilities: capability_defaults_for_kind(
                ModelProviderKind::Anthropic,
                ProviderModelRole::Chat,
            ),
            openai_base_url: None,
            anthropic_base_url: Some(self.config.anthropic_base_url.clone()),
            openai_model: None,
            anthropic_model: chat_model_id.clone(),
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: self.config.auth_profile_id.clone(),
            auth_profile_provider_kind: self
                .config
                .auth_profile_provider_kind
                .map(|kind| kind.as_str().to_owned()),
            credential_source: self
                .config
                .credential_source
                .map(|source| source.as_str().to_owned()),
            api_key_configured: self.config.anthropic_api_key.is_some(),
            retry_policy: ProviderRetryPolicySnapshot {
                max_retries: self.config.max_retries,
                retry_backoff_ms: self.config.retry_backoff_ms,
            },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: self.config.circuit_breaker_failure_threshold,
                cooldown_ms: self.config.circuit_breaker_cooldown_ms,
                consecutive_failures,
                open,
            },
            runtime_metrics: self.runtime_metrics_snapshot(),
            response_cache: empty_response_cache_snapshot(response_cache_enabled_from_config(
                &self.config,
            )),
            health: if self.config.anthropic_api_key.is_some()
                || self.config.auth_profile_id.is_some()
            {
                empty_health_probe_snapshot("ok", "provider configured", "runtime")
            } else {
                empty_health_probe_snapshot("missing_auth", "provider has no credential", "runtime")
            },
            discovery: ProviderDiscoverySnapshot {
                status: if chat_model_id.is_some() { "static" } else { "pending" }.to_owned(),
                checked_at_unix_ms: None,
                expires_at_unix_ms: None,
                discovered_model_ids: chat_model_id.clone().into_iter().collect(),
                source: "static".to_owned(),
                message: if chat_model_id.is_some() {
                    None
                } else {
                    Some("no provider-discovered models are configured yet".to_owned())
                },
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: chat_model_id,
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: true,
                response_cache_enabled: true,
                providers: Vec::new(),
                credentials: Vec::new(),
                models: Vec::new(),
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot.route_selection = route_selection_from_status_snapshot(&snapshot);
        snapshot
    }
}

/// Mutable accumulator behind each provider's runtime metrics mutex; all
/// counters saturate instead of wrapping.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ProviderRuntimeMetrics {
    request_count: u64,
    error_count: u64,
    total_retry_attempts: u64,
    total_prompt_tokens: u64,
    total_completion_tokens: u64,
    total_latency_ms: u64,
    last_latency_ms: u64,
    max_latency_ms: u64,
    last_used_at_unix_ms: Option<i64>,
    last_success_at_unix_ms: Option<i64>,
    last_error_at_unix_ms: Option<i64>,
    last_error: Option<ProviderFailureSnapshot>,
}

impl ProviderRuntimeMetrics {
    fn record(
        &mut self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
        failure: Option<ProviderFailureSnapshot>,
    ) {
        let observed_at_unix_ms = current_unix_ms().ok();
        self.request_count = self.request_count.saturating_add(1);
        self.last_used_at_unix_ms = observed_at_unix_ms;
        if error {
            self.error_count = self.error_count.saturating_add(1);
            self.last_error_at_unix_ms = observed_at_unix_ms;
            self.last_error = failure;
        } else {
            self.last_success_at_unix_ms = observed_at_unix_ms;
        }
        self.total_retry_attempts =
            self.total_retry_attempts.saturating_add(u64::from(retry_count));
        self.total_prompt_tokens = self.total_prompt_tokens.saturating_add(prompt_tokens);
        self.total_completion_tokens =
            self.total_completion_tokens.saturating_add(completion_tokens);
        self.total_latency_ms = self.total_latency_ms.saturating_add(latency_ms);
        self.last_latency_ms = latency_ms;
        self.max_latency_ms = self.max_latency_ms.max(latency_ms);
    }

    fn snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        let error_rate_bps = if self.request_count == 0 {
            0
        } else {
            ((u128::from(self.error_count) * 10_000_u128) / u128::from(self.request_count)) as u32
        };
        let avg_prompt_tokens_per_run =
            self.total_prompt_tokens.checked_div(self.request_count).unwrap_or(0);
        let avg_completion_tokens_per_run =
            self.total_completion_tokens.checked_div(self.request_count).unwrap_or(0);
        let avg_latency_ms = self.total_latency_ms.checked_div(self.request_count).unwrap_or(0);
        ProviderRuntimeMetricsSnapshot {
            request_count: self.request_count,
            error_count: self.error_count,
            error_rate_bps,
            total_retry_attempts: self.total_retry_attempts,
            total_prompt_tokens: self.total_prompt_tokens,
            total_completion_tokens: self.total_completion_tokens,
            avg_prompt_tokens_per_run,
            avg_completion_tokens_per_run,
            last_latency_ms: self.last_latency_ms,
            avg_latency_ms,
            max_latency_ms: self.max_latency_ms,
            last_used_at_unix_ms: self.last_used_at_unix_ms,
            last_success_at_unix_ms: self.last_success_at_unix_ms,
            last_error_at_unix_ms: self.last_error_at_unix_ms,
            last_error: self.last_error.clone(),
        }
    }
}

fn normalize_embeddings_inputs(request: &EmbeddingsRequest) -> Result<Vec<String>, ProviderError> {
    if request.inputs.is_empty() {
        return Err(ProviderError::InvalidEmbeddingsRequest {
            message: "input batch must include at least one item".to_owned(),
        });
    }
    if request.inputs.len() > MAX_EMBEDDINGS_BATCH_SIZE {
        return Err(ProviderError::InvalidEmbeddingsRequest {
            message: format!(
                "input batch size {} exceeds limit {MAX_EMBEDDINGS_BATCH_SIZE}",
                request.inputs.len()
            ),
        });
    }

    let mut normalized_inputs = Vec::with_capacity(request.inputs.len());
    let mut total_bytes = 0_usize;
    for (index, input) in request.inputs.iter().enumerate() {
        let normalized = input.trim();
        if normalized.is_empty() {
            return Err(ProviderError::InvalidEmbeddingsRequest {
                message: format!("input at index {index} must not be blank"),
            });
        }
        let input_bytes = normalized.len();
        if input_bytes > MAX_SINGLE_EMBEDDING_INPUT_BYTES {
            return Err(ProviderError::InvalidEmbeddingsRequest {
                message: format!(
                    "input at index {index} is {input_bytes} bytes and exceeds limit {MAX_SINGLE_EMBEDDING_INPUT_BYTES}"
                ),
            });
        }
        total_bytes = total_bytes.saturating_add(input_bytes);
        if total_bytes > MAX_EMBEDDINGS_INPUT_BYTES {
            return Err(ProviderError::InvalidEmbeddingsRequest {
                message: format!(
                    "input batch is {total_bytes} bytes and exceeds limit {MAX_EMBEDDINGS_INPUT_BYTES}"
                ),
            });
        }
        normalized_inputs.push(normalized.to_owned());
    }

    Ok(normalized_inputs)
}

// Deterministic feature-hashing embedding: each (token, position) pair is
// hashed into a signed bucket contribution, then the vector is L2-normalized.
// Not semantically meaningful; it only needs to be stable across runs.
fn hash_embed_text(text: &str, dims: usize) -> Vec<f32> {
    let mut vector = vec![0.0_f32; dims];
    if dims == 0 {
        return vector;
    }

    for (token_index, token) in text.split_whitespace().enumerate() {
        let normalized = token.to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        token_index.hash(&mut hasher);
        let digest = hasher.finish();
        let index = (digest as usize) % dims;
        let sign = if (digest >> 1) & 1 == 0 { 1.0_f32 } else { -1.0_f32 };
        let magnitude = 1.0 + f32::from((digest as u8) % 64) / 64.0;
        vector[index] += sign * magnitude;
    }
    normalize_vector(vector.as_mut_slice());
    vector
}

fn normalize_vector(vector: &mut [f32]) {
    let norm = vector.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>().sqrt();
    if norm <= f64::EPSILON {
        return;
    }
    for value in vector {
        *value = (f64::from(*value) / norm) as f32;
    }
}

// Metrics are advisory telemetry: recovering a poisoned guard is preferable
// to failing the request path over a bookkeeping mutex.
fn lock_runtime_metrics(
    metrics: &Mutex<ProviderRuntimeMetrics>,
) -> std::sync::MutexGuard<'_, ProviderRuntimeMetrics> {
    match metrics.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn elapsed_millis_since(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn current_unix_ms() -> Result<i64, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

fn provider_seconds_to_millis(seconds: f64) -> u64 {
    if !seconds.is_finite() || seconds <= 0.0 {
        return 0;
    }
    let milliseconds = seconds * 1_000.0;
    if milliseconds >= u64::MAX as f64 {
        u64::MAX
    } else {
        milliseconds as u64
    }
}

fn route_selection_from_status_snapshot(
    snapshot: &ProviderStatusSnapshot,
) -> ProviderRouteSelectionTrace {
    let health_state = provider_effective_health_state(
        &snapshot.health,
        &snapshot.runtime_metrics,
        &snapshot.circuit_breaker,
    )
    .to_owned();
    let model_id = snapshot.model_id.clone();
    let candidate = model_id.as_ref().map(|model_id| {
        let capability_state = if snapshot.api_key_configured || snapshot.kind == "deterministic" {
            "eligible"
        } else {
            "provider_unconfigured"
        };
        ProviderRouteCandidateTrace {
            provider_id: snapshot.provider_id.clone(),
            credential_id: snapshot.credential_id.clone(),
            model_id: model_id.clone(),
            role: "chat".to_owned(),
            capability_state: capability_state.to_owned(),
            reason_code: provider_route_reason(
                true,
                capability_state,
                health_state.as_str(),
                snapshot.registry.failover_enabled,
            ),
            health_state: health_state.clone(),
            selected: true,
        }
    });
    ProviderRouteSelectionTrace {
        default_model_id: model_id.clone(),
        failover_enabled: snapshot.registry.failover_enabled,
        generated_at_unix_ms: current_unix_ms().unwrap_or_default(),
        selected_provider_id: Some(snapshot.provider_id.clone()),
        selected_model_id: model_id,
        candidates: candidate.into_iter().collect(),
    }
}

fn trim_to_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

// OpenAI-compatible `message.content` arrives as a plain string, an array of
// multimodal parts, or an object depending on vendor; flatten all shapes to
// the concatenated text.
fn extract_completion_text(content: Option<Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(text) => text,
        Value::Array(parts) => {
            let mut segments = Vec::new();
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    segments.push(text.to_owned());
                }
            }
            segments.join(" ")
        }
        Value::Object(object) => {
            object.get("text").and_then(Value::as_str).map_or_else(String::new, ToOwned::to_owned)
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader, Read, Write},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener, TcpStream},
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    use super::{
        build_embeddings_provider, build_model_provider, capability_defaults_for_kind,
        classify_http_provider_failure, extract_completion_text, normalize_tool_arguments,
        provider_attempt_index, provider_seconds_to_millis, run_provider_failover_self_check,
        sanitize_remote_error, validate_openai_base_url_network_policy_with_resolver,
        validate_qa_mock_provider_attempt_bounds, AnthropicCompatibleChatAdapter,
        AnthropicProvider, EmbeddingsRequest, ModelProvider, ModelProviderAuthProviderKind,
        ModelProviderConfig, ModelProviderCredentialSource, ModelProviderKind,
        ModelProviderRegistryConfig, OpenAiCompatibleChatAdapter, OpenAiCompatibleProvider,
        ProviderCapabilitiesSnapshot, ProviderChatAdapter, ProviderError, ProviderEvent,
        ProviderFailureAction, ProviderFailureClass, ProviderFinishReason, ProviderImageInput,
        ProviderMessage, ProviderMessageContentPart, ProviderMessageRole, ProviderMessageToolCall,
        ProviderMetadataSource, ProviderModelEntryConfig, ProviderModelRole,
        ProviderOutputContentPart, ProviderRawProviderRefs, ProviderRegistryEntryConfig,
        ProviderRequest, ProviderRetryability, ProviderServiceTier, ProviderStreamAccumulator,
        ProviderStreamEvent, ProviderTurnOutput, ProviderUsage, RegistryBackedModelProvider,
        ANTHROPIC_OAUTH_BETA_HEADER, ANTHROPIC_OAUTH_USER_AGENT,
        FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID, FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID,
        FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID, OPENAI_RETRYABLE_STATUS_CODES,
    };
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use palyra_model_providers::{
        classify_transport_provider_failure, parse_qa_mock_provider_fixture_yaml,
    };

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("daemon crate should have workspace crates parent")
            .parent()
            .expect("workspace crates directory should have repository parent")
            .to_path_buf()
    }

    fn qa_mock_fixture_path() -> PathBuf {
        repo_root().join("qa").join("fixtures").join("provider_basic.yaml")
    }

    fn qa_mock_retry_fixture_path() -> PathBuf {
        repo_root().join("qa").join("fixtures").join("provider_retry_recovery.yaml")
    }

    #[test]
    fn provider_seconds_to_millis_preserves_subsecond_precision() {
        assert_eq!(provider_seconds_to_millis(1.75), 1_750);
        assert_eq!(provider_seconds_to_millis(0.125), 125);
        assert_eq!(provider_seconds_to_millis(-1.0), 0);
        assert_eq!(provider_seconds_to_millis(f64::INFINITY), 0);
        assert_eq!(provider_seconds_to_millis(u64::MAX as f64), u64::MAX);
    }

    #[test]
    fn transport_timeout_classification_is_actionable() {
        let timeout_failure = classify_transport_provider_failure("anthropic_chat_request", true);

        assert_eq!(timeout_failure.class, ProviderFailureClass::ProviderTimeout);
        assert_eq!(timeout_failure.recommended_action, ProviderFailureAction::Retry);
        assert_eq!(
            timeout_failure.provider_detail.as_deref(),
            Some("anthropic_chat_request:timeout")
        );

        let network_failure = classify_transport_provider_failure("anthropic_chat_request", false);
        assert_eq!(network_failure.class, ProviderFailureClass::NetworkUnavailable);
        assert_eq!(network_failure.recommended_action, ProviderFailureAction::Retry);
    }

    #[test]
    fn provider_failure_recovery_plan_uses_phase_two_taxonomy() {
        let rate_limit =
            classify_http_provider_failure(429, true, "openai_chat_http", "rate limit exceeded")
                .snapshot("redacted 429".to_owned());
        assert_eq!(rate_limit.class, "rate_limit");
        assert_eq!(rate_limit.recovery.category, "rate_limit");
        assert_eq!(rate_limit.recovery.action, "retry_after");

        let quota = classify_http_provider_failure(
            402,
            false,
            "openai_chat_http",
            "insufficient_quota: billing credits exhausted",
        )
        .snapshot("redacted quota".to_owned());
        assert_eq!(quota.class, "quota");
        assert_eq!(quota.recovery.category, "quota");
        assert_eq!(quota.recovery.action, "ask_user");

        let minimax_plan_limit = classify_http_provider_failure(
            429,
            false,
            "minimax_chat_http",
            "Token Plan usage limit reached",
        )
        .snapshot("redacted minimax quota".to_owned());
        assert_eq!(minimax_plan_limit.class, "quota");
        assert_eq!(minimax_plan_limit.recovery.category, "quota");
        assert_eq!(minimax_plan_limit.recovery.action, "ask_user");

        let context = classify_http_provider_failure(
            400,
            false,
            "openai_chat_http",
            "maximum context length exceeded",
        )
        .snapshot("redacted context".to_owned());
        assert_eq!(context.recovery.category, "context_overflow");
        assert_eq!(context.recovery.action, "compact_and_retry");

        let safety = classify_http_provider_failure(
            400,
            false,
            "openai_chat_http",
            "safety policy blocked this request",
        )
        .snapshot("redacted safety".to_owned());
        assert_eq!(safety.recovery.category, "safety_stop");
        assert_eq!(safety.recovery.action, "abort");

        assert!(
            OPENAI_RETRYABLE_STATUS_CODES.contains(&529),
            "provider overload HTTP 529 must be retried like other transient upstream failures"
        );
        let overload = classify_http_provider_failure(
            529,
            true,
            "anthropic_chat_http",
            r#"{"type":"error","error":{"type":"overloaded_error"}}"#,
        )
        .snapshot("redacted overload".to_owned());
        assert_eq!(overload.class, "transient_upstream");
        assert_eq!(overload.recovery.category, "transient");
        assert_eq!(overload.recovery.action, "retry_same");
    }

    #[test]
    fn circuit_open_failure_exposes_retry_after_recovery() {
        let snapshot = ProviderError::CircuitOpen { retry_after_ms: 1_250 }.failure_snapshot();
        assert_eq!(snapshot.recovery.category, "transient");
        assert_eq!(snapshot.recovery.action, "retry_after");
        assert_eq!(snapshot.recovery.retry_after_ms, Some(1_250));
    }

    #[test]
    fn provider_error_envelope_is_stable_and_redacted() {
        let error = ProviderError::RequestFailed {
            message: "Bearer secret-token rate limit exceeded".to_owned(),
            retryable: true,
            retry_count: 1,
            classification: classify_http_provider_failure(
                429,
                true,
                "openai_compatible_chat_http",
                "rate limit exceeded",
            ),
        };

        let envelope = error.envelope();

        assert_eq!(envelope.kind, super::ProviderErrorKind::RateLimit);
        assert_eq!(envelope.retryability, ProviderRetryability::RetryAfter);
        assert!(envelope.failover_eligible);
        assert!(
            !envelope.redacted_message.contains("secret-token"),
            "provider error envelope must not leak bearer token material"
        );
        assert_eq!(envelope.provider_trace_ref.as_deref(), Some("openai_compatible_chat_http"));
    }

    #[test]
    fn provider_request_adapters_serialize_message_contracts() {
        let request = ProviderRequest {
            input_text: "What changed?".to_owned(),
            user_visible_input_text: None,
            messages: vec![
                ProviderMessage {
                    role: ProviderMessageRole::System,
                    content: vec![ProviderMessageContentPart::text("You are concise.")],
                    name: None,
                    tool_call_id: None,
                    tool_calls: Vec::new(),
                },
                ProviderMessage::user_text("What changed?"),
            ],
            json_mode: true,
            vision_inputs: Vec::new(),
            model_override: None,
            tool_catalog_snapshot: Some(serde_json::json!({"tools":["palyra.echo"]})),
            instruction_hash: Some("instruction-sha256".to_owned()),
            context_trace_id: Some("ctx-01".to_owned()),
            budget_profile: Some("interactive-default".to_owned()),
            max_output_tokens: Some(6_144),
            reasoning_effort: None,
            service_tier: None,
            prompt_segments: Vec::new(),
            prompt_cache_policy: palyra_model_providers::PromptCachePolicy::default(),
            prompt_cache_report: None,
        };

        let openai_payload =
            OpenAiCompatibleChatAdapter.request_payload(&request, "gpt-contract-test");
        assert_eq!(openai_payload["model"], "gpt-contract-test");
        assert_eq!(openai_payload["messages"][0]["role"], "system");
        assert_eq!(openai_payload["messages"][1]["role"], "user");
        assert_eq!(openai_payload["response_format"]["type"], "json_object");
        assert_eq!(openai_payload["max_tokens"], 6_144);

        let anthropic_payload =
            AnthropicCompatibleChatAdapter.request_payload(&request, "claude-contract-test");
        assert_eq!(anthropic_payload["model"], "claude-contract-test");
        assert_eq!(anthropic_payload["max_tokens"], 6_144);
        assert_eq!(anthropic_payload["messages"][0]["role"], "user");
        assert!(
            anthropic_payload["system"].as_str().unwrap_or_default().contains("You are concise."),
            "system/developer messages should stay outside Anthropic user turns"
        );
    }

    #[test]
    fn provider_request_adapters_serialize_tool_refeed_messages() {
        let request = ProviderRequest {
            input_text: "Use a tool.".to_owned(),
            user_visible_input_text: None,
            messages: vec![
                ProviderMessage::user_text("Use a tool."),
                ProviderMessage {
                    role: ProviderMessageRole::Assistant,
                    content: Vec::new(),
                    name: None,
                    tool_call_id: None,
                    tool_calls: vec![
                        ProviderMessageToolCall {
                            proposal_id: "call_01".to_owned(),
                            tool_name: "palyra.echo".to_owned(),
                            input_json: serde_json::json!({"text":"hello"}),
                        },
                        ProviderMessageToolCall {
                            proposal_id: "call_02".to_owned(),
                            tool_name: "palyra.echo".to_owned(),
                            input_json: serde_json::json!({"text":"world"}),
                        },
                    ],
                },
                ProviderMessage::tool_result("call_01", r#"{"echo":"hello"}"#),
                ProviderMessage::tool_result("call_02", r#"{"echo":"world"}"#),
            ],
            json_mode: false,
            vision_inputs: Vec::new(),
            model_override: None,
            tool_catalog_snapshot: None,
            instruction_hash: None,
            context_trace_id: None,
            budget_profile: None,
            max_output_tokens: None,
            reasoning_effort: None,
            service_tier: None,
            prompt_segments: Vec::new(),
            prompt_cache_policy: palyra_model_providers::PromptCachePolicy::default(),
            prompt_cache_report: None,
        };

        let openai_payload =
            OpenAiCompatibleChatAdapter.request_payload(&request, "gpt-contract-test");
        assert_eq!(openai_payload["messages"][1]["role"], "assistant");
        assert_eq!(openai_payload["messages"][1]["content"], serde_json::Value::Null);
        assert_eq!(openai_payload["messages"][1]["tool_calls"][0]["id"], "call_01");
        assert_eq!(
            openai_payload["messages"][1]["tool_calls"][0]["function"]["name"],
            "palyra.echo"
        );
        assert_eq!(openai_payload["messages"][2]["role"], "tool");
        assert_eq!(openai_payload["messages"][2]["tool_call_id"], "call_01");
        assert_eq!(openai_payload["messages"][3]["role"], "tool");
        assert_eq!(openai_payload["messages"][3]["tool_call_id"], "call_02");

        let anthropic_payload =
            AnthropicCompatibleChatAdapter.request_payload(&request, "claude-contract-test");
        assert_eq!(anthropic_payload["messages"][1]["role"], "assistant");
        assert_eq!(anthropic_payload["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(anthropic_payload["messages"][1]["content"][0]["id"], "call_01");
        assert_eq!(anthropic_payload["messages"][2]["role"], "user");
        assert_eq!(anthropic_payload["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(anthropic_payload["messages"][2]["content"][0]["tool_use_id"], "call_01");
        assert_eq!(anthropic_payload["messages"][3]["role"], "assistant");
        assert_eq!(anthropic_payload["messages"][3]["content"][0]["type"], "tool_use");
        assert_eq!(anthropic_payload["messages"][3]["content"][0]["id"], "call_02");
        assert_eq!(anthropic_payload["messages"][4]["role"], "user");
        assert_eq!(anthropic_payload["messages"][4]["content"][0]["type"], "tool_result");
        assert_eq!(anthropic_payload["messages"][4]["content"][0]["tool_use_id"], "call_02");
        assert_eq!(anthropic_payload["messages"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn anthropic_adapter_converts_orphan_tool_results_to_text() {
        let request = ProviderRequest {
            input_text: "Continue from previous evidence.".to_owned(),
            user_visible_input_text: None,
            messages: vec![
                ProviderMessage::user_text("Continue from previous evidence."),
                ProviderMessage::tool_result("call_orphan_01", r#"{"status":"ok"}"#),
                ProviderMessage::user_text("Summarize the result."),
            ],
            json_mode: false,
            vision_inputs: Vec::new(),
            model_override: None,
            tool_catalog_snapshot: None,
            instruction_hash: None,
            context_trace_id: None,
            budget_profile: None,
            max_output_tokens: None,
            reasoning_effort: None,
            service_tier: None,
            prompt_segments: Vec::new(),
            prompt_cache_policy: palyra_model_providers::PromptCachePolicy::default(),
            prompt_cache_report: None,
        };

        let anthropic_payload =
            AnthropicCompatibleChatAdapter.request_payload(&request, "claude-contract-test");

        assert_eq!(anthropic_payload["messages"][1]["role"], "user");
        assert_eq!(anthropic_payload["messages"][1]["content"][0]["type"], "text");
        assert!(anthropic_payload["messages"][1]["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("Tool result for call_orphan_01"));
        assert_eq!(anthropic_payload["messages"][2]["role"], "user");
        assert_eq!(anthropic_payload["messages"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn anthropic_adapter_keeps_recovery_tool_result_adjacent_to_tool_use() {
        let request = ProviderRequest {
            input_text: "Use a tool then recover final answer.".to_owned(),
            user_visible_input_text: None,
            messages: vec![
                ProviderMessage::user_text("Use a tool."),
                ProviderMessage {
                    role: ProviderMessageRole::Assistant,
                    content: Vec::new(),
                    name: None,
                    tool_call_id: None,
                    tool_calls: vec![ProviderMessageToolCall {
                        proposal_id: "call_recovery_01".to_owned(),
                        tool_name: "palyra.echo".to_owned(),
                        input_json: serde_json::json!({"text":"hello"}),
                    }],
                },
                ProviderMessage::tool_result("call_recovery_01", r#"{"echo":"hello"}"#),
                ProviderMessage {
                    role: ProviderMessageRole::Assistant,
                    content: vec![ProviderMessageContentPart::text("ack")],
                    name: None,
                    tool_call_id: None,
                    tool_calls: Vec::new(),
                },
                ProviderMessage::user_text(
                    "The previous assistant turn did not provide a usable final answer.",
                ),
            ],
            json_mode: false,
            vision_inputs: Vec::new(),
            model_override: None,
            tool_catalog_snapshot: None,
            instruction_hash: None,
            context_trace_id: None,
            budget_profile: None,
            max_output_tokens: None,
            reasoning_effort: None,
            service_tier: None,
            prompt_segments: Vec::new(),
            prompt_cache_policy: palyra_model_providers::PromptCachePolicy::default(),
            prompt_cache_report: None,
        };

        let anthropic_payload =
            AnthropicCompatibleChatAdapter.request_payload(&request, "claude-contract-test");

        assert_eq!(anthropic_payload["messages"][1]["role"], "assistant");
        assert_eq!(anthropic_payload["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(anthropic_payload["messages"][2]["role"], "user");
        assert_eq!(anthropic_payload["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(
            anthropic_payload["messages"][2]["content"][0]["tool_use_id"],
            "call_recovery_01"
        );
        assert_eq!(anthropic_payload["messages"][3]["role"], "assistant");
        assert_eq!(anthropic_payload["messages"][3]["content"][0]["text"], "ack");
        assert_eq!(anthropic_payload["messages"][4]["role"], "user");
    }

    #[test]
    fn scripted_provider_stream_harness_accumulates_full_output_usage_and_tools() {
        let mut harness =
            ProviderStreamAccumulator::with_buffer_cap("fake-provider", "fake-model", 8);
        harness.apply(ProviderStreamEvent::Started {
            provider_id: "fake-provider".to_owned(),
            model_id: "fake-model".to_owned(),
        });
        harness.apply(ProviderStreamEvent::Delta { text: "alpha ".to_owned() });
        harness.apply(ProviderStreamEvent::Delta { text: "beta gamma".to_owned() });
        harness.apply(ProviderStreamEvent::ToolDelta {
            proposal_id: "tool-01".to_owned(),
            tool_name: "palyra.echo".to_owned(),
            input_json: serde_json::json!({"text":"hello"}),
        });
        harness.apply(ProviderStreamEvent::UsageDelta {
            prompt_tokens: 5,
            completion_tokens: 3,
            total_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
        });
        harness.apply(ProviderStreamEvent::Completed {
            finish_reason: super::ProviderFinishReason::ToolCalls,
            raw_provider_refs: ProviderRawProviderRefs {
                provider_response_id: Some("resp_01".to_owned()),
                provider_model_id: Some("fake-model".to_owned()),
                system_fingerprint: None,
                provider_trace_ref: Some("fake_trace".to_owned()),
                stream_spill_ref: None,
            },
        });

        let output = harness.finalize();

        assert_eq!(output.full_text, "alpha beta gamma");
        assert_eq!(output.usage.prompt_tokens, 5);
        assert_eq!(output.usage.completion_tokens, 3);
        assert_eq!(output.usage.total_tokens, 8);
        assert_eq!(output.raw_provider_refs.provider_response_id.as_deref(), Some("resp_01"));
        assert!(
            output.raw_provider_refs.stream_spill_ref.is_some(),
            "large streamed output should record the spill boundary"
        );
        assert!(output.content_parts.iter().any(|part| {
            matches!(
                part,
                ProviderOutputContentPart::ToolCall { tool_name, .. }
                    if tool_name == "palyra.echo"
            )
        }));
    }

    #[test]
    fn scripted_provider_stream_harness_applies_hard_output_cap() {
        let mut harness = ProviderStreamAccumulator::new("fake-provider", "fake-model");
        harness.apply(ProviderStreamEvent::Delta {
            text: "x".repeat(palyra_model_providers::MAX_PROVIDER_TURN_TEXT_BYTES + 1024),
        });
        harness.apply(ProviderStreamEvent::Completed {
            finish_reason: super::ProviderFinishReason::Stop,
            raw_provider_refs: ProviderRawProviderRefs::default(),
        });

        let output = harness.finalize();

        assert!(output.full_text.len() <= palyra_model_providers::MAX_PROVIDER_TURN_TEXT_BYTES);
        assert!(output.full_text.ends_with("[provider output truncated]"));
        assert!(output.redaction_state.output_redacted);
        assert_eq!(
            output.raw_provider_refs.stream_spill_ref.as_deref(),
            Some("provider-stream-inline-spill:fake-provider:fake-model")
        );
    }

    #[test]
    fn provider_turn_output_serializes_deterministically() {
        let output = ProviderTurnOutput::text(
            "complete answer".to_owned(),
            super::ProviderFinishReason::Stop,
            ProviderUsage::new(2, 2, "provider"),
            ProviderRawProviderRefs {
                provider_response_id: Some("resp_snapshot".to_owned()),
                provider_model_id: Some("model_snapshot".to_owned()),
                system_fingerprint: None,
                provider_trace_ref: Some("trace_snapshot".to_owned()),
                stream_spill_ref: None,
            },
        );

        let serialized = serde_json::to_string(&output).expect("output should serialize");

        assert_eq!(
            serialized,
            r#"{"full_text":"complete answer","content_parts":[{"kind":"text","text":"complete answer"}],"finish_reason":"stop","usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4,"source":"provider"},"raw_provider_refs":{"provider_response_id":"resp_snapshot","provider_model_id":"model_snapshot","provider_trace_ref":"trace_snapshot"},"redaction_state":{"output_redacted":false,"user_visible_projected":true,"diagnostics_redacted":true}}"#
        );
    }

    fn openai_test_config(base_url: String) -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: base_url,
            allow_private_base_url: true,
            openai_model: "gpt-4o-mini".to_owned(),
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            openai_api_key: Some("sk-test-secret".to_owned()),
            openai_api_key_vault_ref: None,
            request_timeout_ms: 5_000,
            max_retries: 2,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 2,
            circuit_breaker_cooldown_ms: 120_000,
            ..ModelProviderConfig::default()
        }
    }

    fn anthropic_test_config(base_url: String) -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_base_url: base_url,
            allow_private_base_url: true,
            anthropic_model: "claude-3-5-sonnet-latest".to_owned(),
            anthropic_api_key: Some("sk-anthropic-test".to_owned()),
            request_timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 2,
            circuit_breaker_cooldown_ms: 120_000,
            ..ModelProviderConfig::default()
        }
    }

    #[test]
    fn registry_provider_without_discovered_models_does_not_use_legacy_model_default() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: "http://127.0.0.1:1/v1".to_owned(),
            allow_private_base_url: true,
            registry: ModelProviderRegistryConfig {
                providers: vec![ProviderRegistryEntryConfig {
                    provider_id: "xai-primary".to_owned(),
                    display_name: Some("xAI".to_owned()),
                    kind: ModelProviderKind::OpenAiCompatible,
                    base_url: Some("http://127.0.0.1:1/v1".to_owned()),
                    allow_private_base_url: true,
                    enabled: true,
                    auth_profile_id: None,
                    auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Xai),
                    api_key: None,
                    api_key_secret_ref: None,
                    api_key_vault_ref: None,
                    credential_source: None,
                    request_timeout_ms: 5_000,
                    max_retries: 0,
                    retry_backoff_ms: 1,
                    circuit_breaker_failure_threshold: 1,
                    circuit_breaker_cooldown_ms: 60_000,
                }],
                models: Vec::new(),
                default_chat_model_id: None,
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: true,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        };

        let provider = RegistryBackedModelProvider::new(config)
            .expect("pending registry provider should be valid before discovery");
        let snapshot = provider.status_snapshot();

        assert_eq!(snapshot.model_id, None);
        assert_eq!(snapshot.openai_model, None);
        assert_eq!(snapshot.registry.default_chat_model_id, None);
        assert!(
            snapshot.registry.models.is_empty(),
            "pending provider registry must not synthesize legacy model defaults"
        );
        assert_eq!(snapshot.discovery.status, "pending");
    }

    #[test]
    fn registry_default_chat_model_outside_local_models_is_synthesized() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: "http://127.0.0.1:1/v1".to_owned(),
            allow_private_base_url: true,
            registry: ModelProviderRegistryConfig {
                providers: vec![ProviderRegistryEntryConfig {
                    provider_id: "openrouter-primary".to_owned(),
                    display_name: Some("OpenRouter".to_owned()),
                    kind: ModelProviderKind::OpenAiCompatible,
                    base_url: Some("http://127.0.0.1:1/v1".to_owned()),
                    allow_private_base_url: true,
                    enabled: true,
                    auth_profile_id: None,
                    auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Openrouter),
                    api_key: None,
                    api_key_secret_ref: None,
                    api_key_vault_ref: None,
                    credential_source: None,
                    request_timeout_ms: 5_000,
                    max_retries: 0,
                    retry_backoff_ms: 1,
                    circuit_breaker_failure_threshold: 1,
                    circuit_breaker_cooldown_ms: 60_000,
                }],
                models: vec![ProviderModelEntryConfig {
                    model_id: "google/gemini-3.1-flash-image".to_owned(),
                    provider_id: "openrouter-primary".to_owned(),
                    role: ProviderModelRole::Chat,
                    enabled: true,
                    metadata_source: ProviderMetadataSource::Static,
                    operator_override: false,
                    capabilities: ProviderCapabilitiesSnapshot {
                        tool_calls: false,
                        ..capability_defaults_for_kind(
                            ModelProviderKind::OpenAiCompatible,
                            ProviderModelRole::Chat,
                        )
                    },
                }],
                default_chat_model_id: Some("deepseek/deepseek-v4-flash".to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: true,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        };

        let provider = RegistryBackedModelProvider::new(config)
            .expect("provider-selected default models must not require local registry entries");
        let snapshot = provider.status_snapshot();
        let synthesized = snapshot
            .registry
            .models
            .iter()
            .find(|model| model.model_id == "deepseek/deepseek-v4-flash")
            .expect("custom default model should be synthesized into runtime metadata");

        assert_eq!(snapshot.model_id.as_deref(), Some("deepseek/deepseek-v4-flash"));
        assert_eq!(snapshot.openai_model.as_deref(), Some("deepseek/deepseek-v4-flash"));
        assert_eq!(
            snapshot.registry.default_chat_model_id.as_deref(),
            Some("deepseek/deepseek-v4-flash")
        );
        assert_eq!(synthesized.provider_id, "openrouter-primary");
        assert!(synthesized.capabilities.tool_calls);
        assert!(synthesized.capabilities.operator_override);
        assert_eq!(synthesized.capabilities.metadata_source, "operator_override");
    }

    #[test]
    fn legacy_provider_without_model_is_pending_before_discovery() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: "http://127.0.0.1:1/v1".to_owned(),
            allow_private_base_url: true,
            openai_api_key: Some("sk-test-secret".to_owned()),
            ..ModelProviderConfig::default()
        };

        let provider = RegistryBackedModelProvider::new(config)
            .expect("legacy provider without discovered models should remain valid");
        let snapshot = provider.status_snapshot();

        assert_eq!(snapshot.model_id, None);
        assert_eq!(snapshot.registry.default_chat_model_id, None);
        assert!(
            snapshot.registry.models.is_empty(),
            "legacy provider must not synthesize a provider model before discovery"
        );
        assert_eq!(snapshot.discovery.status, "pending");
    }

    #[tokio::test]
    async fn openai_provider_without_model_fails_before_http() {
        let mut config = openai_test_config("http://127.0.0.1:1/v1".to_owned());
        config.openai_model.clear();
        config.max_retries = 0;
        let provider = OpenAiCompatibleProvider::new(&config)
            .expect("provider construction should not require a discovered model");

        let error = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect_err("missing discovered model should fail locally");

        assert!(
            matches!(
                error,
                ProviderError::RequestFailed {
                    retryable: false,
                    ref message,
                    ..
                } if message.contains("no discovered chat model configured")
            ),
            "missing model error should be local and non-retryable: {error:?}"
        );
    }

    #[tokio::test]
    async fn anthropic_provider_without_model_fails_before_http() {
        let mut config = anthropic_test_config("http://127.0.0.1:1".to_owned());
        config.anthropic_model.clear();
        config.max_retries = 0;
        let provider = AnthropicProvider::new(&config)
            .expect("provider construction should not require a discovered model");

        let error = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect_err("missing discovered model should fail locally");

        assert!(
            matches!(
                error,
                ProviderError::RequestFailed {
                    retryable: false,
                    ref message,
                    ..
                } if message.contains("no discovered chat model configured")
            ),
            "missing model error should be local and non-retryable: {error:?}"
        );
    }

    fn multi_provider_test_config(
        openai_base_url: String,
        anthropic_base_url: String,
    ) -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: openai_base_url.clone(),
            anthropic_base_url: anthropic_base_url.clone(),
            allow_private_base_url: true,
            openai_model: "gpt-4o-mini".to_owned(),
            anthropic_model: "claude-3-5-sonnet-latest".to_owned(),
            openai_api_key: Some("sk-openai-test".to_owned()),
            anthropic_api_key: Some("sk-anthropic-test".to_owned()),
            registry: ModelProviderRegistryConfig {
                providers: vec![
                    ProviderRegistryEntryConfig {
                        provider_id: "openai-primary".to_owned(),
                        display_name: Some("OpenAI".to_owned()),
                        kind: ModelProviderKind::OpenAiCompatible,
                        base_url: Some(openai_base_url),
                        allow_private_base_url: true,
                        enabled: true,
                        auth_profile_id: None,
                        auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Openai),
                        api_key: Some("sk-openai-test".to_owned()),
                        api_key_secret_ref: None,
                        api_key_vault_ref: None,
                        credential_source: None,
                        request_timeout_ms: 5_000,
                        max_retries: 0,
                        retry_backoff_ms: 1,
                        circuit_breaker_failure_threshold: 1,
                        circuit_breaker_cooldown_ms: 60_000,
                    },
                    ProviderRegistryEntryConfig {
                        provider_id: "anthropic-primary".to_owned(),
                        display_name: Some("Anthropic".to_owned()),
                        kind: ModelProviderKind::Anthropic,
                        base_url: Some(anthropic_base_url),
                        allow_private_base_url: true,
                        enabled: true,
                        auth_profile_id: None,
                        auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Anthropic),
                        api_key: Some("sk-anthropic-test".to_owned()),
                        api_key_secret_ref: None,
                        api_key_vault_ref: None,
                        credential_source: None,
                        request_timeout_ms: 5_000,
                        max_retries: 0,
                        retry_backoff_ms: 1,
                        circuit_breaker_failure_threshold: 1,
                        circuit_breaker_cooldown_ms: 60_000,
                    },
                ],
                models: vec![
                    ProviderModelEntryConfig {
                        model_id: "gpt-4o-mini".to_owned(),
                        provider_id: "openai-primary".to_owned(),
                        role: ProviderModelRole::Chat,
                        enabled: true,
                        metadata_source: ProviderMetadataSource::Static,
                        operator_override: false,
                        capabilities: capability_defaults_for_kind(
                            ModelProviderKind::OpenAiCompatible,
                            ProviderModelRole::Chat,
                        ),
                    },
                    ProviderModelEntryConfig {
                        model_id: "claude-3-5-sonnet-latest".to_owned(),
                        provider_id: "anthropic-primary".to_owned(),
                        role: ProviderModelRole::Chat,
                        enabled: true,
                        metadata_source: ProviderMetadataSource::Static,
                        operator_override: false,
                        capabilities: capability_defaults_for_kind(
                            ModelProviderKind::Anthropic,
                            ProviderModelRole::Chat,
                        ),
                    },
                ],
                default_chat_model_id: Some("gpt-4o-mini".to_owned()),
                response_cache_enabled: true,
                response_cache_ttl_ms: 60_000,
                response_cache_max_entries: 32,
                ..ModelProviderRegistryConfig::default()
            },
            request_timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 1,
            circuit_breaker_cooldown_ms: 60_000,
            ..ModelProviderConfig::default()
        }
    }

    fn deterministic_fixture_failover_config(openai_base_url: String) -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: openai_base_url.clone(),
            allow_private_base_url: true,
            openai_model: "gpt-4o-mini".to_owned(),
            openai_api_key: Some("sk-openai-test".to_owned()),
            qa_mock_fixture_path: Some(qa_mock_retry_fixture_path()),
            qa_mock_fixture_enabled: true,
            registry: ModelProviderRegistryConfig {
                providers: vec![
                    ProviderRegistryEntryConfig {
                        provider_id: "openai-primary".to_owned(),
                        display_name: Some("OpenAI".to_owned()),
                        kind: ModelProviderKind::OpenAiCompatible,
                        base_url: Some(openai_base_url),
                        allow_private_base_url: true,
                        enabled: true,
                        auth_profile_id: None,
                        auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Openai),
                        api_key: Some("sk-openai-test".to_owned()),
                        api_key_secret_ref: None,
                        api_key_vault_ref: None,
                        credential_source: None,
                        request_timeout_ms: 5_000,
                        max_retries: 0,
                        retry_backoff_ms: 1,
                        circuit_breaker_failure_threshold: 1,
                        circuit_breaker_cooldown_ms: 60_000,
                    },
                    ProviderRegistryEntryConfig {
                        provider_id: "deterministic-fallback".to_owned(),
                        display_name: Some("Deterministic fixture".to_owned()),
                        kind: ModelProviderKind::Deterministic,
                        base_url: None,
                        allow_private_base_url: false,
                        enabled: true,
                        auth_profile_id: None,
                        auth_profile_provider_kind: None,
                        api_key: None,
                        api_key_secret_ref: None,
                        api_key_vault_ref: None,
                        credential_source: None,
                        request_timeout_ms: 5_000,
                        max_retries: 2,
                        retry_backoff_ms: 1,
                        circuit_breaker_failure_threshold: 1,
                        circuit_breaker_cooldown_ms: 60_000,
                    },
                ],
                models: vec![
                    ProviderModelEntryConfig {
                        model_id: "gpt-4o-mini".to_owned(),
                        provider_id: "openai-primary".to_owned(),
                        role: ProviderModelRole::Chat,
                        enabled: true,
                        metadata_source: ProviderMetadataSource::Static,
                        operator_override: false,
                        capabilities: capability_defaults_for_kind(
                            ModelProviderKind::OpenAiCompatible,
                            ProviderModelRole::Chat,
                        ),
                    },
                    ProviderModelEntryConfig {
                        model_id: "deterministic-fixture".to_owned(),
                        provider_id: "deterministic-fallback".to_owned(),
                        role: ProviderModelRole::Chat,
                        enabled: true,
                        metadata_source: ProviderMetadataSource::Static,
                        operator_override: false,
                        capabilities: capability_defaults_for_kind(
                            ModelProviderKind::Deterministic,
                            ProviderModelRole::Chat,
                        ),
                    },
                ],
                default_chat_model_id: Some("gpt-4o-mini".to_owned()),
                failover_enabled: true,
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            request_timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 1,
            circuit_breaker_cooldown_ms: 60_000,
            ..ModelProviderConfig::default()
        }
    }

    fn tool_catalog_response_cache_fixture(
        created_at_unix_ms: i64,
        snapshot_id: &str,
        catalog_hash: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "snapshot_id": snapshot_id,
            "catalog_hash": catalog_hash,
            "provider_dialect": "open_ai_compatible",
            "provider_kind": "openai_compatible",
            "provider_model_id": "gpt-4o-mini",
            "surface": "run_stream",
            "principal_hash": "principal_sha256",
            "channel_hash": null,
            "remaining_tool_budget": null,
            "created_at_unix_ms": created_at_unix_ms,
            "profile_expansion": {
                "profiles": [],
                "profile_expansions": [],
                "explicit_allowed_tools": ["palyra.echo"],
                "extra_tools": [],
                "disabled_tools": [],
                "effective_allowed_tools": ["palyra.echo"]
            },
            "exposure_mode": "direct",
            "compact_tool_threshold": 16,
            "direct_tool_count": 1,
            "exposed_tool_count": 1,
            "estimated_direct_tool_bytes": 128,
            "estimated_exposed_tool_bytes": 128,
            "estimated_saved_bytes": 0,
            "availability_probes": [],
            "index": {
                "schema_version": 1,
                "index_digest": "index_sha256",
                "entries": []
            },
            "indexed_tools": [{
                "name": "palyra.echo",
                "description": "Echo text for diagnostics.",
                "version": 1,
                "provenance": "builtin",
                "schema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"}
                    },
                    "required": ["text"]
                },
                "provider_schema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"}
                    },
                    "required": ["text"]
                },
                "internal_schema_hash": "internal_schema_sha256",
                "provider_schema_hash": "provider_schema_sha256",
                "provider_schema_transform": {
                    "schema_version": 1,
                    "dialect": "open_ai_compatible",
                    "input_schema_hash": "internal_schema_sha256",
                    "output_schema_hash": "provider_schema_sha256",
                    "steps": []
                },
                "description_hash": "description_sha256",
                "capabilities": ["diagnostics"],
                "approval_posture": "safe",
                "projection_policy": "inline_unless_large",
                "parallelism_policy": "read_only",
                "replay_safety_class": "read_only",
                "exposure_reason": "allowlisted_policy_visible"
            }],
            "tools": [{
                "name": "palyra.echo",
                "description": "Echo text for diagnostics.",
                "version": 1,
                "provenance": "builtin",
                "schema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"}
                    },
                    "required": ["text"]
                },
                "provider_schema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"}
                    },
                    "required": ["text"]
                },
                "internal_schema_hash": "internal_schema_sha256",
                "provider_schema_hash": "provider_schema_sha256",
                "provider_schema_transform": {
                    "schema_version": 1,
                    "dialect": "open_ai_compatible",
                    "input_schema_hash": "internal_schema_sha256",
                    "output_schema_hash": "provider_schema_sha256",
                    "steps": []
                },
                "description_hash": "description_sha256",
                "capabilities": ["diagnostics"],
                "approval_posture": "safe",
                "projection_policy": "inline_unless_large",
                "parallelism_policy": "read_only",
                "replay_safety_class": "read_only",
                "exposure_reason": "allowlisted_policy_visible"
            }],
            "filtered_tools": []
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_preserves_full_output_and_chunks_preview() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        let input_text = (0..64).map(|index| format!("token{index}")).collect::<Vec<_>>().join(" ");
        let request = ProviderRequest::from_input_text(input_text.clone(), false, Vec::new(), None);
        let response =
            provider.complete(request).await.expect("deterministic provider should succeed");
        let tokens = response
            .events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::ModelToken { token, .. } => Some(token),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(response.output.full_text, input_text);
        let reconstructed = tokens.iter().map(|token| token.as_str()).collect::<String>();
        assert_eq!(reconstructed, input_text, "preview chunks must reconstruct full output");
        assert!(
            tokens.len() > 1,
            "long deterministic output should be split into bounded preview chunks"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_echoes_user_visible_input_when_prompt_is_augmented() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        let mut request = ProviderRequest::from_input_text(
            "Runtime context: current_utc=2026-05-17T00:00:00Z\n\nalpha beta gamma".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.user_visible_input_text = Some("alpha beta gamma".to_owned());

        let response =
            provider.complete(request).await.expect("deterministic provider should succeed");

        assert_eq!(response.output.full_text, "alpha beta gamma");
        assert_eq!(response.completion_tokens, 3);
        assert!(
            response.prompt_tokens > response.completion_tokens,
            "augmented prompt accounting should still include model-visible context"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_replays_qa_mock_fixture_when_explicitly_enabled() {
        let provider = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_enabled: true,
            qa_mock_fixture_path: Some(qa_mock_fixture_path()),
            registry: ModelProviderRegistryConfig {
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        })
        .expect("QA mock fixture provider should build when enabled");

        let text_response = provider
            .complete(ProviderRequest::from_input_text(
                "Say a friendly deterministic answer".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("text fixture turn should succeed");
        assert!(text_response.output.full_text.contains("friendly deterministic answer"));
        assert_eq!(text_response.output.usage.source, "qa_mock_fixture");

        let tool_response = provider
            .complete(ProviderRequest::from_input_text(
                "Please propose the approval tool call".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("tool fixture turn should succeed");
        assert_eq!(tool_response.output.finish_reason, ProviderFinishReason::ToolCalls);
        assert!(matches!(
            tool_response.events.last(),
            Some(ProviderEvent::ToolProposal { proposal_id, tool_name, .. })
                if proposal_id == "qa-approval-read" && tool_name == "palyra.fs.read_file"
        ));

        let error = provider
            .complete(ProviderRequest::from_input_text(
                "Trigger malformed tool args".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect_err("malformed fixture turn should fail");
        assert!(matches!(error, ProviderError::InvalidResponse { .. }));
        assert_eq!(error.classification().class, ProviderFailureClass::MalformedResponse);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_retries_fixture_failures_and_preserves_attempt_evidence() {
        let provider = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_enabled: true,
            qa_mock_fixture_path: Some(qa_mock_retry_fixture_path()),
            request_timeout_ms: 5_000,
            max_retries: 2,
            retry_backoff_ms: 40,
            registry: ModelProviderRegistryConfig {
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        })
        .expect("QA retry fixture provider should build when enabled");

        let started_at = Instant::now();
        let text_response = provider
            .complete(ProviderRequest::from_input_text(
                "Recover a streamed answer".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("retryable malformed response should recover into text");

        assert_eq!(
            text_response.output.full_text,
            "Recovered after a retryable malformed response."
        );
        assert_eq!(text_response.retry_count, 1);
        assert_eq!(text_response.attempts.len(), 2);
        assert_eq!(text_response.attempts[0].outcome, "error");
        assert!(text_response.attempts[0].retryable);
        assert_eq!(
            text_response.attempts[0].reason_code.as_deref(),
            Some("qa_mock_malformed_output")
        );
        assert_eq!(text_response.attempts[1].outcome, "success");
        for (attempt_index, attempt) in text_response.attempts.iter().enumerate() {
            let state = attempt.state.as_ref().expect("fixture attempt should expose state");
            assert_eq!(state.attempt_index, provider_attempt_index(attempt_index));
            assert_eq!(state.provider_profile_id, "deterministic-primary");
            assert_eq!(state.model_id, "deterministic");
            assert!(!state.credential_id.is_empty());
        }
        let error_state = text_response.attempts[0]
            .state
            .as_ref()
            .expect("failed fixture attempt should expose state");
        assert_eq!(error_state.error_class.as_deref(), Some("malformed_response"));
        let success_state = text_response.attempts[1]
            .state
            .as_ref()
            .expect("successful fixture attempt should expose state");
        assert_eq!(success_state.prompt_tokens, text_response.prompt_tokens);
        assert_eq!(success_state.output_tokens, text_response.completion_tokens);
        assert_eq!(success_state.final_disposition, "success");
        assert!(text_response
            .events
            .iter()
            .any(|event| matches!(event, ProviderEvent::ModelToken { token, .. } if token.contains("Recovered"))));
        assert!(
            started_at.elapsed() >= Duration::from_millis(60),
            "configured attempt latencies and retry backoff should precede completion"
        );

        let tool_response = provider
            .complete(ProviderRequest::from_input_text(
                "Recover with a tool call".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("retryable malformed response should recover into a tool call");

        assert_eq!(tool_response.retry_count, 1);
        assert_eq!(tool_response.attempts.len(), 2);
        assert!(tool_response.attempts[0].retryable);
        assert_eq!(
            tool_response.attempts[0].reason_code.as_deref(),
            Some("qa_mock_malformed_output")
        );
        assert!(matches!(
            tool_response.events.last(),
            Some(ProviderEvent::ToolProposal { proposal_id, tool_name, .. })
                if proposal_id == "qa-recovered-read" && tool_name == "palyra.fs.read_file"
        ));

        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.runtime_metrics.request_count, 2);
        assert_eq!(snapshot.runtime_metrics.error_count, 0);
        assert_eq!(snapshot.runtime_metrics.total_retry_attempts, 2);
        assert!(snapshot.runtime_metrics.max_latency_ms >= 20);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_fixture_respects_zero_retry_budget() {
        let provider = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_enabled: true,
            qa_mock_fixture_path: Some(qa_mock_retry_fixture_path()),
            max_retries: 0,
            retry_backoff_ms: 1,
            registry: ModelProviderRegistryConfig {
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        })
        .expect("QA retry fixture provider should build when enabled");

        let error = provider
            .complete(ProviderRequest::from_input_text(
                "Recover a streamed answer".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect_err("zero retry budget must stop after the first fixture failure");

        assert_eq!(error.retry_count(), 0);
        assert_eq!(error.classification().class, ProviderFailureClass::MalformedResponse);
        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.runtime_metrics.request_count, 1);
        assert_eq!(snapshot.runtime_metrics.error_count, 1);
        assert_eq!(snapshot.runtime_metrics.total_retry_attempts, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_fixture_stops_when_retry_budget_is_exhausted() {
        let mut fixture_file =
            tempfile::NamedTempFile::new().expect("temporary QA fixture file should be created");
        fixture_file
            .write_all(
                br#"schema_version: 1
id: qa.mock.provider.exhausted-retries
turns:
  - id: exhaust
    match:
      prompt_contains: [exhaust fixture retries]
    attempts:
      - { kind: malformed_output, error_message: first failure }
      - { kind: stream_error, error_message: second failure }
      - { kind: text, text: unreachable success }
"#,
            )
            .expect("temporary QA fixture should be written");
        let provider = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_enabled: true,
            qa_mock_fixture_path: Some(fixture_file.path().to_path_buf()),
            max_retries: 1,
            retry_backoff_ms: 1,
            registry: ModelProviderRegistryConfig {
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        })
        .expect("bounded retry fixture should build");

        let error = provider
            .complete(ProviderRequest::from_input_text(
                "Exhaust fixture retries".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect_err("the third fixture attempt must remain outside a one-retry budget");

        assert_eq!(error.retry_count(), 1);
        assert_eq!(error.classification().provider_detail.as_deref(), Some("qa_mock_stream_error"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_fixture_applies_timeout_to_each_attempt() {
        let mut fixture_file =
            tempfile::NamedTempFile::new().expect("temporary QA fixture file should be created");
        fixture_file
            .write_all(
                br#"schema_version: 1
id: qa.mock.provider.attempt-timeout
turns:
  - id: recover
    match:
      prompt_contains: [recover after attempt timeout]
    attempts:
      - { kind: malformed_output, error_message: must time out, latency_ms: 100 }
      - { kind: text, text: recovered after timeout, latency_ms: 1 }
"#,
            )
            .expect("temporary QA fixture should be written");
        let provider = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_enabled: true,
            qa_mock_fixture_path: Some(fixture_file.path().to_path_buf()),
            request_timeout_ms: 10,
            max_retries: 1,
            retry_backoff_ms: 1,
            registry: ModelProviderRegistryConfig {
                response_cache_enabled: false,
                ..ModelProviderRegistryConfig::default()
            },
            ..ModelProviderConfig::default()
        })
        .expect("QA retry fixture provider should build when enabled");

        let response = provider
            .complete(ProviderRequest::from_input_text(
                "Recover after attempt timeout".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("second attempt should complete inside its request timeout");

        assert_eq!(response.retry_count, 1);
        assert_eq!(response.attempts.len(), 2);
        assert_eq!(
            response.attempts[0].reason_code.as_deref(),
            Some("qa_mock_fixture_attempt:timeout")
        );
        let timeout_state = response.attempts[0]
            .state
            .as_ref()
            .expect("timed-out fixture attempt should expose state");
        assert_eq!(timeout_state.error_class.as_deref(), Some("provider_timeout"));
        assert_eq!(response.attempts[1].outcome, "success");
    }

    #[test]
    fn deterministic_fixture_rejects_direct_one_attempt_sequence() {
        let fixture = parse_qa_mock_provider_fixture_yaml(
            fs::read_to_string(qa_mock_retry_fixture_path())
                .expect("retry fixture should be readable")
                .as_str(),
        )
        .expect("retry fixture should parse");
        let fixture_turn = fixture.turns.first().expect("retry fixture should contain a turn");
        let behavior = fixture_turn.behavior.clone();
        let direct_turn = palyra_model_providers::QaMockProviderTurn {
            id: "direct-one-attempt".to_owned(),
            matcher: fixture_turn.matcher.clone(),
            behavior: behavior.clone(),
            attempts: vec![behavior],
        };

        let error = validate_qa_mock_provider_attempt_bounds(&direct_turn)
            .expect_err("direct one-attempt sequence should fail defensively");

        assert_eq!(
            error.classification().provider_detail.as_deref(),
            Some("qa_mock_invalid_attempt_plan")
        );
        assert!(error.failure_snapshot().message.contains("shorter than the supported minimum"));
    }

    #[test]
    fn qa_mock_fixture_path_requires_explicit_preview_gate() {
        let result = build_model_provider(&ModelProviderConfig {
            qa_mock_fixture_path: Some(qa_mock_fixture_path()),
            qa_mock_fixture_enabled: false,
            ..ModelProviderConfig::default()
        });

        assert!(result.is_err(), "QA mock fixture must require explicit QA Lab preview mode");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_replays_scripted_tool_call_fixture() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        let mut request = ProviderRequest::from_input_text(
            format!(
                "Use the deterministic test response, create `{}` and verify the fixture.",
                super::DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH
            ),
            false,
            Vec::new(),
            None,
        );
        request.tool_catalog_snapshot = Some(serde_json::json!({
            "tools": [
                {"name": "palyra.fs.apply_patch"},
                {"name": "palyra.fs.read_file"}
            ]
        }));

        let write_response =
            provider.complete(request.clone()).await.expect("write turn should succeed");
        let write_proposal = write_response
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                    Some((proposal_id, tool_name, input_json))
                }
                ProviderEvent::ModelToken { .. } => None,
            })
            .expect("write turn should propose a tool");
        let write_input: serde_json::Value =
            serde_json::from_slice(write_proposal.2).expect("write input should be json");
        assert_eq!(write_proposal.0, super::DETERMINISTIC_TOOL_FIXTURE_WRITE_CALL_ID);
        assert_eq!(write_proposal.1, "palyra.fs.apply_patch");
        assert!(write_input["patch"]
            .as_str()
            .unwrap_or_default()
            .contains(super::DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH));
        assert!(write_input["patch"]
            .as_str()
            .unwrap_or_default()
            .contains(super::DETERMINISTIC_TOOL_FIXTURE_ID));

        request.messages.push(ProviderMessage::assistant_from_output(&write_response.output));
        request.messages.push(ProviderMessage::tool_result(
            super::DETERMINISTIC_TOOL_FIXTURE_WRITE_CALL_ID,
            r#"{"success":true,"files_touched":[{"path":"reports/deterministic-provider.md","operation":"create"}]}"#,
        ));

        let read_response =
            provider.complete(request.clone()).await.expect("read turn should succeed");
        let read_proposal = read_response
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                    Some((proposal_id, tool_name, input_json))
                }
                ProviderEvent::ModelToken { .. } => None,
            })
            .expect("read turn should propose a tool");
        let read_input: serde_json::Value =
            serde_json::from_slice(read_proposal.2).expect("read input should be json");
        assert_eq!(read_proposal.0, super::DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID);
        assert_eq!(read_proposal.1, "palyra.fs.read_file");
        assert_eq!(
            read_input["path"].as_str(),
            Some(super::DETERMINISTIC_TOOL_FIXTURE_REPORT_PATH)
        );

        request.messages.push(ProviderMessage::assistant_from_output(&read_response.output));
        request.messages.push(ProviderMessage::tool_result(
            super::DETERMINISTIC_TOOL_FIXTURE_READ_CALL_ID,
            super::DETERMINISTIC_TOOL_FIXTURE_REPORT,
        ));

        let final_response = provider.complete(request).await.expect("final turn should succeed");

        assert_eq!(final_response.output.finish_reason, ProviderFinishReason::Stop);
        assert!(final_response
            .events
            .iter()
            .all(|event| { !matches!(event, ProviderEvent::ToolProposal { .. }) }));
        assert!(final_response.output.full_text.contains(super::DETERMINISTIC_TOOL_FIXTURE_ID));
        assert!(final_response
            .output
            .raw_provider_refs
            .provider_trace_ref
            .as_deref()
            .is_some_and(|trace_ref| trace_ref.contains("tool-fixture-final")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_status_snapshot_reports_runtime_metrics() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        provider
            .complete(ProviderRequest::from_input_text(
                "measure deterministic metrics".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("deterministic provider should succeed");
        let failed = provider
            .complete(ProviderRequest::from_input_text(
                "vision request".to_owned(),
                false,
                vec![ProviderImageInput {
                    mime_type: "image/png".to_owned(),
                    bytes_base64: "iVBORw0KGgo=".to_owned(),
                    file_name: Some("vision.png".to_owned()),
                    width_px: Some(1),
                    height_px: Some(1),
                    artifact_id: None,
                }],
                None,
            ))
            .await;
        assert!(matches!(failed, Err(ProviderError::VisionUnsupported { .. })));

        let snapshot = provider.status_snapshot();
        assert!(snapshot.capabilities.tool_calls);
        assert!(snapshot
            .capabilities
            .recommended_use_cases
            .iter()
            .any(|use_case| use_case == "scripted tool-call regression"));
        assert_eq!(snapshot.runtime_metrics.request_count, 2);
        assert_eq!(snapshot.runtime_metrics.error_count, 1);
        assert_eq!(snapshot.runtime_metrics.error_rate_bps, 5_000);
        assert_eq!(snapshot.runtime_metrics.total_retry_attempts, 0);
        assert!(
            snapshot.runtime_metrics.total_prompt_tokens > 0,
            "successful deterministic calls should report prompt token usage"
        );
        assert!(
            snapshot.runtime_metrics.total_completion_tokens > 0,
            "successful deterministic calls should report completion token usage"
        );
        assert!(
            snapshot.runtime_metrics.max_latency_ms >= snapshot.runtime_metrics.last_latency_ms,
            "max latency should be at least as large as the latest observation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_provider_retries_on_retryable_error_then_succeeds() {
        let scripted = vec![
            (503_u16, r#"{"error":{"message":"temporary upstream error"}}"#.to_owned()),
            (200_u16, r#"{"choices":[{"message":{"content":"alpha beta gamma"}}]}"#.to_owned()),
        ];
        let (base_url, request_count, handle) = spawn_scripted_server(scripted);
        let config = openai_test_config(base_url);
        let provider = build_model_provider(&config).expect("openai provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect("provider should succeed after retry");
        assert_eq!(response.retry_count, 1, "one retry should be recorded");
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            2,
            "provider should issue two HTTP requests"
        );
        let model_tokens = response
            .events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::ModelToken { token, .. } => Some(token.as_str()),
                ProviderEvent::ToolProposal { .. } => None,
            })
            .collect::<String>();
        assert_eq!(
            model_tokens, "alpha beta gamma",
            "response preview chunks should reconstruct completion text"
        );
        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.runtime_metrics.request_count, 1);
        assert_eq!(snapshot.runtime_metrics.error_count, 0);
        assert_eq!(snapshot.runtime_metrics.error_rate_bps, 0);
        assert_eq!(snapshot.runtime_metrics.total_retry_attempts, 1);
        assert_eq!(
            snapshot.runtime_metrics.total_prompt_tokens, response.prompt_tokens,
            "status snapshot should accumulate prompt token usage per provider request"
        );
        assert_eq!(
            snapshot.runtime_metrics.total_completion_tokens, response.completion_tokens,
            "status snapshot should accumulate completion token usage per provider request"
        );
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chatgpt_oauth_token_uses_codex_responses_transport() {
        let response_body = [
            r#"data: {"type":"response.output_text.delta","delta":"PALYRA_ONBOARDING_OK"}"#,
            "",
            r#"data: {"type":"response.completed","response":{"id":"resp_test","model":"provider-selected-model","status":"completed","usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}}"#,
            "",
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(vec![(200_u16, response_body)]);
        let codex_base_url =
            base_url.strip_suffix("/v1").expect("test helper base URL should end in /v1");
        let mut config = openai_test_config(codex_base_url.to_owned());
        config.openai_model = "provider-selected-model".to_owned();
        config.openai_api_key = Some(fake_chatgpt_oauth_token("acct_test_123"));
        config.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
        let provider = build_model_provider(&config).expect("openai provider should build");
        let mut request =
            ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None);
        request.tool_catalog_snapshot =
            Some(tool_catalog_response_cache_fixture(1_781_883_663_913, "snapshot-01", "hash-01"));
        request.service_tier = Some(ProviderServiceTier::Priority);

        let response = provider
            .complete(request)
            .await
            .expect("chatgpt oauth provider should use responses transport");

        assert_eq!(response.output.full_text, "PALYRA_ONBOARDING_OK");
        assert_eq!(response.prompt_tokens, 3);
        assert_eq!(response.completion_tokens, 4);
        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        let requests = request_log.lock().expect("request log lock should not be poisoned");
        let captured = requests.first().expect("server should capture one request");
        assert_eq!(captured.path, "/responses");
        assert_eq!(
            header_value(captured, "originator").as_deref(),
            Some(super::OPENAI_CODEX_ORIGINATOR)
        );
        assert_eq!(
            header_value(captured, "user-agent").as_deref(),
            Some(super::OPENAI_CODEX_USER_AGENT)
        );
        assert_eq!(header_value(captured, "chatgpt-account-id").as_deref(), Some("acct_test_123"));
        let request_body: serde_json::Value =
            serde_json::from_str(captured.body.as_str()).expect("request body should be JSON");
        assert_eq!(request_body["model"], "provider-selected-model");
        assert_eq!(request_body["stream"], true);
        assert_eq!(request_body["store"], false);
        assert_eq!(request_body["instructions"], "You are a helpful assistant.");
        assert_eq!(request_body["input"][0]["role"], "user");
        assert_eq!(request_body["tool_choice"], "auto");
        assert_eq!(request_body["service_tier"], "priority");
        assert_eq!(request_body["tools"][0]["name"], "palyra_echo");
        drop(requests);
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chatgpt_oauth_responses_maps_sanitized_tool_names_to_palyra_tools() {
        let response_body = [
            r#"data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_01","name":"palyra_echo","arguments":"{\"text\":\"hello\"}"}}"#,
            "",
            r#"data: {"type":"response.completed","response":{"id":"resp_tool","model":"provider-selected-model","status":"completed","usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}"#,
            "",
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let (base_url, request_count, handle) =
            spawn_scripted_server(vec![(200_u16, response_body)]);
        let codex_base_url =
            base_url.strip_suffix("/v1").expect("test helper base URL should end in /v1");
        let mut config = openai_test_config(codex_base_url.to_owned());
        config.openai_api_key = Some(fake_chatgpt_oauth_token("acct_tool_123"));
        config.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
        let provider = build_model_provider(&config).expect("openai provider should build");
        let mut request =
            ProviderRequest::from_input_text("use a tool".to_owned(), false, Vec::new(), None);
        request.tool_catalog_snapshot =
            Some(tool_catalog_response_cache_fixture(1_781_883_663_913, "snapshot-02", "hash-02"));

        let response =
            provider.complete(request).await.expect("chatgpt oauth tool response should parse");

        let proposal = response
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                    Some((proposal_id, tool_name, input_json))
                }
                ProviderEvent::ModelToken { .. } => None,
            })
            .expect("responses transport should emit the tool proposal");
        assert_eq!(proposal.0, "call_01");
        assert_eq!(proposal.1, "palyra.echo");
        let input_json: serde_json::Value =
            serde_json::from_slice(proposal.2).expect("tool input should remain valid JSON");
        assert_eq!(input_json["text"], "hello");
        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chatgpt_oauth_responses_retries_unsupported_content_type_with_text_replay() {
        let response_body = [
            r#"data: {"type":"response.output_text.delta","delta":"RECOVERED_AFTER_COMPAT"}"#,
            "",
            r#"data: {"type":"response.completed","response":{"id":"resp_recovered","model":"provider-selected-model","status":"completed","usage":{"input_tokens":8,"output_tokens":3,"total_tokens":11}}}"#,
            "",
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(vec![
                (400_u16, r#"{"detail":"Unsupported content type"}"#.to_owned()),
                (200_u16, response_body),
            ]);
        let codex_base_url =
            base_url.strip_suffix("/v1").expect("test helper base URL should end in /v1");
        let mut config = openai_test_config(codex_base_url.to_owned());
        config.openai_model = "provider-selected-model".to_owned();
        config.openai_api_key = Some(fake_chatgpt_oauth_token("acct_tool_replay_123"));
        config.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
        config.max_retries = 0;
        let provider = build_model_provider(&config).expect("openai provider should build");

        let mut request = ProviderRequest::from_input_text(
            "use a tool then continue".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.tool_catalog_snapshot = Some(tool_catalog_response_cache_fixture(
            1_781_883_663_913,
            "snapshot-replay",
            "hash-replay",
        ));
        request.messages = vec![
            ProviderMessage::user_text("use a tool then continue"),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call_01".to_owned(),
                    tool_name: "palyra.echo".to_owned(),
                    input_json: serde_json::json!({"text": "hello"}),
                }],
            },
            ProviderMessage::tool_result("call_01", r#"{"echo":"hello"}"#),
        ];

        let response = provider
            .complete(request)
            .await
            .expect("unsupported content type retry should recover through text replay");

        assert_eq!(response.output.full_text, "RECOVERED_AFTER_COMPAT");
        assert_eq!(request_count.load(Ordering::Relaxed), 2);
        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests.len(), 2);
        let first_body: serde_json::Value =
            serde_json::from_str(requests[0].body.as_str()).expect("first body should be JSON");
        let second_body: serde_json::Value =
            serde_json::from_str(requests[1].body.as_str()).expect("second body should be JSON");
        let first_input = first_body["input"].as_array().expect("first input should be an array");
        let second_input =
            second_body["input"].as_array().expect("second input should be an array");
        assert!(first_input
            .iter()
            .any(|item| item["type"].as_str() == Some("function_call_output")));
        assert!(!second_input
            .iter()
            .any(|item| item["type"].as_str() == Some("function_call_output")));
        let replay_text = second_body["input"][0]["content"][0]["text"]
            .as_str()
            .expect("fallback request should carry replay text");
        assert!(replay_text.contains("[tool_result call_id=call_01]"));
        assert!(replay_text.contains(r#""echo":"hello""#));
        assert_eq!(second_body["tools"][0]["name"], "palyra_echo");
        drop(requests);
        handle.join().expect("scripted server thread should exit");
    }

    #[test]
    fn chatgpt_oauth_account_id_is_read_from_jwt_claims() {
        let token = fake_chatgpt_oauth_token("acct_unit_456");

        let account_id = super::openai_chatgpt_account_id_from_token(token.as_str());

        assert_eq!(account_id.as_deref(), Some("acct_unit_456"));
    }

    #[test]
    fn chatgpt_oauth_runtime_model_preserves_provider_selection() {
        assert_eq!(super::openai_codex_runtime_model_id("provider-model"), "provider-model");
        assert_eq!(super::openai_codex_runtime_model_id("openai/provider-model"), "provider-model");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_provider_fails_over_to_anthropic_when_primary_openai_fails() {
        let (openai_base_url, openai_request_count, openai_handle) = spawn_scripted_server(vec![(
            503_u16,
            r#"{"error":{"message":"temporary upstream error"}}"#.to_owned(),
        )]);
        let (anthropic_base_url, anthropic_request_count, anthropic_handle) =
            spawn_scripted_server(vec![(
                200_u16,
                r#"{"content":[{"type":"text","text":"fallback from anthropic"}],"stop_reason":"end_turn"}"#
                    .to_owned(),
            )]);
        let provider =
            build_model_provider(&multi_provider_test_config(openai_base_url, anthropic_base_url))
                .expect("registry-backed provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text(
                "fallback please".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("fallback provider should succeed");

        assert_eq!(response.provider_id, "anthropic-primary");
        assert_eq!(response.model_id, "claude-3-5-sonnet-latest");
        assert_eq!(response.failover_count, 1);
        assert_eq!(response.attempts.len(), 2);
        let failed_state =
            response.attempts[0].state.as_ref().expect("failed attempt should expose state");
        assert_eq!(failed_state.attempt_index, 0);
        assert_eq!(failed_state.provider_profile_id, "openai-primary");
        assert_eq!(failed_state.model_id, "gpt-4o-mini");
        assert_eq!(failed_state.final_disposition, "retryable_failure");
        assert_eq!(failed_state.error_class.as_deref(), Some("provider_unavailable"));
        let success_state =
            response.attempts[1].state.as_ref().expect("success attempt should expose state");
        assert_eq!(success_state.attempt_index, 1);
        assert_eq!(success_state.provider_profile_id, "anthropic-primary");
        assert_eq!(success_state.prompt_tokens, response.prompt_tokens);
        assert_eq!(success_state.output_tokens, response.completion_tokens);
        assert_eq!(success_state.cache_tokens, 0);
        assert_eq!(success_state.final_disposition, "failover_success");
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);
        assert_eq!(anthropic_request_count.load(Ordering::Relaxed), 1);

        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.registry.default_chat_model_id.as_deref(), Some("gpt-4o-mini"));
        assert_eq!(snapshot.registry.providers.len(), 2);

        openai_handle.join().expect("openai scripted server thread should exit");
        anthropic_handle.join().expect("anthropic scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_rebinds_and_offsets_all_fixture_retry_states_after_failover() {
        let (openai_base_url, openai_request_count, openai_handle) = spawn_scripted_server(vec![(
            503_u16,
            r#"{"error":{"message":"temporary upstream error"}}"#.to_owned(),
        )]);
        let provider =
            build_model_provider(&deterministic_fixture_failover_config(openai_base_url))
                .expect("fixture failover provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text(
                "Recover a streamed answer".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("deterministic fallback should recover after its fixture retry");

        assert_eq!(response.provider_id, "deterministic-fallback");
        assert_eq!(response.model_id, "deterministic-fixture");
        assert_eq!(response.failover_count, 1);
        assert_eq!(response.retry_count, 1);
        assert_eq!(response.attempts.len(), 3);
        assert_eq!(response.attempts[0].provider_id, "openai-primary");
        for (attempt_index, attempt) in response.attempts[1..].iter().enumerate() {
            let expected_index = provider_attempt_index(attempt_index.saturating_add(1));
            let state = attempt.state.as_ref().expect("fixture retry should expose state");
            assert_eq!(attempt.provider_id, "deterministic-fallback");
            assert_eq!(attempt.model_id, "deterministic-fixture");
            assert_eq!(state.attempt_index, expected_index);
            assert_eq!(state.provider_profile_id, "deterministic-fallback");
            assert_eq!(state.model_id, "deterministic-fixture");
            assert!(!state.credential_id.is_empty());
        }
        assert_eq!(response.attempts[1].outcome, "error");
        assert_eq!(response.attempts[2].outcome, "failover_success");
        assert_eq!(
            response.attempts[2].state.as_ref().map(|state| state.final_disposition.as_str()),
            Some("failover_success")
        );
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);

        openai_handle.join().expect("openai scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_provider_auth_expired_attempt_exposes_repair_hint() {
        let (openai_base_url, openai_request_count, openai_handle) = spawn_scripted_server(vec![(
            401_u16,
            r#"{"error":{"message":"access token expired"}}"#.to_owned(),
        )]);
        let (anthropic_base_url, anthropic_request_count, anthropic_handle) =
            spawn_scripted_server(vec![(
                200_u16,
                r#"{"content":[{"type":"text","text":"fallback after auth"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}"#
                    .to_owned(),
            )]);
        let provider =
            build_model_provider(&multi_provider_test_config(openai_base_url, anthropic_base_url))
                .expect("registry-backed provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text(
                "recover from auth".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("fallback provider should recover from expired auth");

        let failed_state =
            response.attempts[0].state.as_ref().expect("auth attempt should expose state");
        assert_eq!(failed_state.error_class.as_deref(), Some("auth_expired"));
        assert_eq!(failed_state.final_disposition, "credential_refresh_required");
        assert!(failed_state
            .repair_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("refresh the provider credential")));
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);
        assert_eq!(anthropic_request_count.load(Ordering::Relaxed), 1);

        openai_handle.join().expect("openai scripted server thread should exit");
        anthropic_handle.join().expect("anthropic scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_provider_rate_limit_attempt_respects_retry_after() {
        let (openai_base_url, openai_request_count, _openai_requests, openai_handle) =
            spawn_inspecting_scripted_server_with_headers(vec![(
                429_u16,
                vec![("Retry-After".to_owned(), "2".to_owned())],
                r#"{"error":{"message":"rate limit exceeded"}}"#.to_owned(),
            )]);
        let (anthropic_base_url, anthropic_request_count, anthropic_handle) =
            spawn_scripted_server(vec![(
                200_u16,
                r#"{"content":[{"type":"text","text":"fallback after rate limit"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":2}}"#
                    .to_owned(),
            )]);
        let provider =
            build_model_provider(&multi_provider_test_config(openai_base_url, anthropic_base_url))
                .expect("registry-backed provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text(
                "recover from rate limit".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("fallback provider should recover from rate limit");

        let failed_state =
            response.attempts[0].state.as_ref().expect("rate-limit attempt should expose state");
        assert_eq!(failed_state.error_class.as_deref(), Some("rate_limit"));
        assert_eq!(failed_state.retry_after_ms, Some(2_000));
        assert!(failed_state.cooldown_until_unix_ms.is_some());
        assert_eq!(failed_state.final_disposition, "retry_after_required");
        assert!(failed_state
            .repair_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("retry_after")));
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);
        assert_eq!(anthropic_request_count.load(Ordering::Relaxed), 1);

        openai_handle.join().expect("openai scripted server thread should exit");
        anthropic_handle.join().expect("anthropic scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_failover_self_check_uses_synthetic_missing_key_fallback() {
        let report = run_provider_failover_self_check()
            .await
            .expect("synthetic failover self-check should pass");

        assert_eq!(report.status, "passed");
        assert_eq!(report.mode, "in_memory_synthetic");
        assert_eq!(report.safety.label, "no_real_config_no_real_credentials_no_network");
        assert!(!report.safety.uses_real_config);
        assert!(!report.safety.uses_real_credentials);
        assert!(!report.safety.performs_network_io);
        assert_eq!(report.primary_provider_id, FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID);
        assert_eq!(report.fallback_provider_id, FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID);
        assert_eq!(report.resolved_provider_id, FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID);
        assert_eq!(report.resolved_model_id, FAILOVER_SELF_CHECK_FALLBACK_MODEL_ID);
        assert_eq!(report.failover_count, 1);
        assert_eq!(report.attempt_count, 2);
        assert_eq!(report.attempts[0].provider_id, FAILOVER_SELF_CHECK_PRIMARY_PROVIDER_ID);
        assert_eq!(report.attempts[0].outcome, "error");
        assert!(report.attempts[0].retryable);
        assert_eq!(report.attempts[0].reason_code.as_deref(), Some("missing_api_key"));
        assert_eq!(report.attempts[1].provider_id, FAILOVER_SELF_CHECK_FALLBACK_PROVIDER_ID);
        assert_eq!(report.attempts[1].outcome, "failover_success");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_provider_response_cache_ignores_tool_catalog_audit_metadata() {
        let (openai_base_url, openai_request_count, openai_handle) = spawn_scripted_server(vec![(
            200_u16,
            r#"{"choices":[{"message":{"content":"cached with tool catalog"}}]}"#.to_owned(),
        )]);
        let provider = build_model_provider(&multi_provider_test_config(
            openai_base_url,
            "http://127.0.0.1:9".to_owned(),
        ))
        .expect("registry-backed provider should build");

        let mut first_request = ProviderRequest::from_input_text(
            "cache tool catalog".to_owned(),
            false,
            Vec::new(),
            None,
        );
        first_request.tool_catalog_snapshot = Some(tool_catalog_response_cache_fixture(
            1_000,
            "toolcat_first_audit_id",
            "first_audit_hash",
        ));
        let first =
            provider.complete(first_request).await.expect("first upstream request should succeed");

        let mut second_request = ProviderRequest::from_input_text(
            "cache tool catalog".to_owned(),
            false,
            Vec::new(),
            None,
        );
        second_request.tool_catalog_snapshot = Some(tool_catalog_response_cache_fixture(
            2_000,
            "toolcat_second_audit_id",
            "second_audit_hash",
        ));
        let second = provider
            .complete(second_request)
            .await
            .expect("second request should be served from cache");

        assert!(!first.served_from_cache);
        assert!(second.served_from_cache);
        assert_eq!(
            openai_request_count.load(Ordering::Relaxed),
            1,
            "volatile tool catalog audit fields must not force a second upstream request"
        );

        openai_handle.join().expect("openai scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_provider_serves_safe_read_only_response_from_cache() {
        let (openai_base_url, openai_request_count, openai_handle) = spawn_scripted_server(vec![(
            200_u16,
            r#"{"choices":[{"message":{"content":"cached alpha beta"}}]}"#.to_owned(),
        )]);
        let provider = build_model_provider(&multi_provider_test_config(
            openai_base_url,
            "http://127.0.0.1:9".to_owned(),
        ))
        .expect("registry-backed provider should build");

        let first = provider
            .complete(ProviderRequest::from_input_text(
                "cache me".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("first upstream request should succeed");
        let second = provider
            .complete(ProviderRequest::from_input_text(
                "cache me".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await
            .expect("second request should be served from cache");

        assert!(!first.served_from_cache);
        assert!(second.served_from_cache);
        assert_eq!(second.attempts.len(), 1);
        let cache_state =
            second.attempts[0].state.as_ref().expect("cache hit should expose attempt state");
        assert_eq!(cache_state.final_disposition, "cache_hit");
        assert_eq!(
            cache_state.cache_tokens,
            second.prompt_tokens.saturating_add(second.completion_tokens)
        );
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);
        let snapshot = provider.status_snapshot();
        assert!(snapshot.response_cache.enabled);
        assert_eq!(snapshot.response_cache.entry_count, 1);
        assert_eq!(snapshot.response_cache.hit_count, 1);
        assert_eq!(snapshot.response_cache.miss_count, 1);

        openai_handle.join().expect("openai scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minimax_anthropic_compatible_provider_uses_bearer_auth() {
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(vec![(
                200_u16,
                r#"{"content":[{"type":"text","text":"hello from minimax"}],"stop_reason":"end_turn"}"#
                    .to_owned(),
            )]);
        let anthropic_base_url =
            base_url.strip_suffix("/v1").unwrap_or(base_url.as_str()).to_owned();
        let config = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_base_url,
            allow_private_base_url: true,
            anthropic_model: "MiniMax-M2.7".to_owned(),
            anthropic_api_key: Some("minimax-secret".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Minimax),
            request_timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 1,
            circuit_breaker_cooldown_ms: 60_000,
            ..ModelProviderConfig::default()
        };
        let provider = build_model_provider(&config).expect("minimax provider should build");

        provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect("minimax-compatible provider should succeed");

        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests[0].path, "/v1/messages");
        let authorization = requests[0]
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.as_str());
        assert_eq!(authorization, Some("Bearer minimax-secret"));
        assert!(
            !requests[0].headers.iter().any(|(name, _)| name == "x-api-key"),
            "MiniMax Anthropic-compatible transport must not use Anthropic x-api-key auth"
        );
        handle.join().expect("minimax scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_oauth_provider_uses_bearer_auth_and_oauth_betas() {
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(vec![(
                200_u16,
                r#"{"content":[{"type":"text","text":"hello from claude oauth"}],"stop_reason":"end_turn"}"#
                    .to_owned(),
            )]);
        let anthropic_base_url =
            base_url.strip_suffix("/v1").unwrap_or(base_url.as_str()).to_owned();
        let config = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_base_url,
            allow_private_base_url: true,
            anthropic_model: "claude-3-5-sonnet-latest".to_owned(),
            anthropic_api_key: Some("anthropic-oauth-access-token".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Anthropic),
            credential_source: Some(ModelProviderCredentialSource::AuthProfileOauthAccessToken),
            request_timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 1,
            circuit_breaker_cooldown_ms: 60_000,
            ..ModelProviderConfig::default()
        };
        let provider = build_model_provider(&config).expect("anthropic provider should build");

        provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect("anthropic OAuth provider should succeed");

        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests[0].path, "/v1/messages");
        let header = |name: &str| {
            requests[0]
                .headers
                .iter()
                .find(|(header_name, _)| header_name == name)
                .map(|(_, value)| value.as_str())
        };
        assert_eq!(header("authorization"), Some("Bearer anthropic-oauth-access-token"));
        assert_eq!(header("anthropic-beta"), Some(ANTHROPIC_OAUTH_BETA_HEADER));
        assert_eq!(header("user-agent"), Some(ANTHROPIC_OAUTH_USER_AGENT));
        assert!(
            !requests[0].headers.iter().any(|(name, _)| name == "x-api-key"),
            "Anthropic OAuth transport must not use x-api-key auth"
        );
        handle.join().expect("anthropic scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minimax_anthropic_compatible_provider_retries_malformed_json_response() {
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(vec![
                (200_u16, r#"{"content":["#.to_owned()),
                (
                    200_u16,
                    r#"{"content":[{"type":"text","text":"recovered from malformed JSON"}],"stop_reason":"end_turn"}"#
                        .to_owned(),
                ),
            ]);
        let anthropic_base_url =
            base_url.strip_suffix("/v1").unwrap_or(base_url.as_str()).to_owned();
        let config = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_base_url,
            allow_private_base_url: true,
            anthropic_model: "MiniMax-M3".to_owned(),
            anthropic_api_key: Some("minimax-secret".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Minimax),
            request_timeout_ms: 5_000,
            max_retries: 1,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 2,
            circuit_breaker_cooldown_ms: 60_000,
            ..ModelProviderConfig::default()
        };
        let provider = build_model_provider(&config).expect("minimax provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect("minimax-compatible provider should retry malformed JSON and recover");

        assert_eq!(response.retry_count, 1);
        assert_eq!(response.output.full_text, "recovered from malformed JSON");
        assert_eq!(request_count.load(Ordering::Relaxed), 2);
        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.path == "/v1/messages"));
        drop(requests);
        handle.join().expect("minimax scripted server thread should exit");
    }

    #[test]
    fn minimax_legacy_registry_does_not_advertise_vision() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_base_url: "https://api.minimax.io/anthropic".to_owned(),
            anthropic_model: "MiniMax-M2.7".to_owned(),
            anthropic_api_key: Some("minimax-secret".to_owned()),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Minimax),
            ..ModelProviderConfig::default()
        };

        let provider = build_model_provider(&config).expect("minimax provider should build");
        let snapshot = provider.status_snapshot();

        assert!(!snapshot.capabilities.vision);
        let model = snapshot
            .registry
            .models
            .iter()
            .find(|model| model.model_id == "MiniMax-M2.7")
            .expect("minimax registry model should be present");
        assert!(!model.capabilities.vision);
        assert!(model
            .capabilities
            .known_limitations
            .iter()
            .any(|limitation| limitation.contains("vision unsupported")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_embeddings_provider_sends_expected_request_payload() {
        let scripted = vec![(
            200_u16,
            r#"{"data":[{"index":0,"embedding":[0.1,0.2,0.3]},{"index":1,"embedding":[0.3,0.2,0.1]}],"model":"text-embedding-3-small"}"#
                .to_owned(),
        )];
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(scripted);
        let mut config = openai_test_config(base_url);
        config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        config.openai_embeddings_dims = Some(3);
        let provider =
            build_embeddings_provider(&config).expect("openai embeddings provider should build");

        let response = provider
            .embed(EmbeddingsRequest { inputs: vec!["alpha".to_owned(), "beta".to_owned()] })
            .await
            .expect("openai embeddings provider should succeed");
        assert_eq!(response.model_name, "text-embedding-3-small");
        assert_eq!(response.dimensions, 3);
        assert_eq!(response.vectors.len(), 2);
        assert_eq!(request_count.load(Ordering::Relaxed), 1);

        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests.len(), 1, "one HTTP call should be recorded");
        assert_eq!(requests[0].path, "/v1/embeddings");
        let body_json = serde_json::from_str::<serde_json::Value>(requests[0].body.as_str())
            .expect("embeddings request body should be valid JSON");
        assert_eq!(
            body_json["model"].as_str(),
            Some("text-embedding-3-small"),
            "request should include embeddings model id"
        );
        assert_eq!(
            body_json["dimensions"].as_u64(),
            Some(3),
            "request should pass configured embedding dimensions"
        );
        assert_eq!(
            body_json["input"].as_array().map(std::vec::Vec::len),
            Some(2),
            "request should forward both embedding inputs in one batch"
        );
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_embeddings_provider_applies_retry_backoff_before_retry() {
        let scripted = vec![
            (503_u16, r#"{"error":{"message":"temporary upstream error"}}"#.to_owned()),
            (
                200_u16,
                r#"{"data":[{"index":0,"embedding":[0.9,0.1]}],"model":"text-embedding-3-small"}"#
                    .to_owned(),
            ),
        ];
        let (base_url, request_count, request_log, handle) =
            spawn_inspecting_scripted_server(scripted);
        let mut config = openai_test_config(base_url);
        config.max_retries = 1;
        config.retry_backoff_ms = 80;
        config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        let provider =
            build_embeddings_provider(&config).expect("openai embeddings provider should build");

        let response = provider
            .embed(EmbeddingsRequest { inputs: vec!["retry me".to_owned()] })
            .await
            .expect("embeddings call should succeed after one retry");
        assert_eq!(response.retry_count, 1);
        assert_eq!(request_count.load(Ordering::Relaxed), 2);

        let requests = request_log.lock().expect("request log lock should not be poisoned");
        assert_eq!(requests.len(), 2, "retry flow should record both requests");
        let first = requests[0].received_at_ms;
        let second = requests[1].received_at_ms;
        assert!(
            second.saturating_sub(first) >= 60,
            "second request should be delayed by backoff (expected at least 60ms, got {}ms)",
            second.saturating_sub(first)
        );
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_embeddings_provider_classifies_retryable_and_permanent_errors() {
        let scripted_retryable =
            vec![(503_u16, r#"{"error":{"message":"temporary upstream error"}}"#.to_owned())];
        let (retryable_base_url, _, _, retryable_handle) =
            spawn_inspecting_scripted_server(scripted_retryable);
        let mut retryable_config = openai_test_config(retryable_base_url);
        retryable_config.max_retries = 0;
        retryable_config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        let retryable_provider = build_embeddings_provider(&retryable_config)
            .expect("retryable embeddings provider should build");

        let retryable_error = retryable_provider
            .embed(EmbeddingsRequest { inputs: vec!["transient".to_owned()] })
            .await
            .expect_err("503 response should fail");
        assert!(
            matches!(retryable_error, ProviderError::RequestFailed { retryable: true, .. }),
            "503 errors must be marked retryable"
        );
        retryable_handle.join().expect("scripted server thread should exit");

        let scripted_permanent =
            vec![(400_u16, r#"{"error":{"message":"invalid embeddings payload"}}"#.to_owned())];
        let (permanent_base_url, _, _, permanent_handle) =
            spawn_inspecting_scripted_server(scripted_permanent);
        let mut permanent_config = openai_test_config(permanent_base_url);
        permanent_config.max_retries = 0;
        permanent_config.openai_embeddings_model = Some("text-embedding-3-small".to_owned());
        let permanent_provider = build_embeddings_provider(&permanent_config)
            .expect("permanent embeddings provider should build");

        let permanent_error = permanent_provider
            .embed(EmbeddingsRequest { inputs: vec!["permanent".to_owned()] })
            .await
            .expect_err("400 response should fail");
        assert!(
            matches!(permanent_error, ProviderError::RequestFailed { retryable: false, .. }),
            "400 errors must be marked permanent"
        );
        permanent_handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_provider_opens_circuit_breaker_after_threshold_failures() {
        let scripted =
            vec![(503_u16, r#"{"error":{"message":"temporary upstream error"}}"#.to_owned())];
        let (base_url, request_count, handle) = spawn_scripted_server(scripted);
        let mut config = openai_test_config(base_url);
        config.max_retries = 0;
        config.circuit_breaker_failure_threshold = 1;
        let provider = build_model_provider(&config).expect("openai provider should build");

        let first = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await;
        assert!(matches!(first, Err(ProviderError::RequestFailed { .. })));
        let second = provider
            .complete(ProviderRequest::from_input_text(
                "hello again".to_owned(),
                false,
                Vec::new(),
                None,
            ))
            .await;
        assert!(
            matches!(second, Err(ProviderError::CircuitOpen { .. })),
            "second call should be rejected by circuit breaker"
        );
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            1,
            "circuit-open call must not hit upstream provider"
        );
        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.runtime_metrics.request_count, 2);
        assert_eq!(snapshot.runtime_metrics.error_count, 2);
        assert_eq!(snapshot.runtime_metrics.error_rate_bps, 10_000);
        handle.join().expect("scripted server thread should exit");
    }

    #[test]
    fn openai_provider_rejects_private_base_url_without_explicit_opt_in() {
        let mut config = openai_test_config("https://10.10.10.10/v1".to_owned());
        config.allow_private_base_url = false;
        let error = match build_model_provider(&config) {
            Ok(_) => panic!("private-network base URL must be rejected"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("allow_private_base_url"),
            "error should explain explicit opt-in path for local/private testing: {rendered}"
        );
    }

    #[test]
    fn openai_provider_rejects_hostname_resolving_to_private_ip_without_opt_in() {
        let error = validate_openai_base_url_network_policy_with_resolver(
            "https://api.example.invalid/v1",
            false,
            |_host, _port| Ok(vec![IpAddr::V4(Ipv4Addr::new(10, 10, 10, 10))]),
        )
        .expect_err("hostname resolving to private IP must be rejected");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("resolves to private/local address"),
            "error should describe resolved private-address guard failure: {rendered}"
        );
        assert!(
            rendered.contains("allow_private_base_url"),
            "error should explain explicit opt-in path for trusted environments: {rendered}"
        );
    }

    #[test]
    fn openai_provider_accepts_hostname_resolving_to_public_ip_without_opt_in() {
        validate_openai_base_url_network_policy_with_resolver(
            "https://api.example.invalid/v1",
            false,
            |_host, _port| Ok(vec![IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))]),
        )
        .expect("hostname resolving to public IP should pass private-network guard");
    }

    #[test]
    fn openai_provider_rejects_special_use_ipv4_ranges_without_opt_in() {
        for address in [
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)),
        ] {
            let error = validate_openai_base_url_network_policy_with_resolver(
                "https://api.example.invalid/v1",
                false,
                |_host, _port| Ok(vec![address]),
            )
            .expect_err("special-use IPv4 ranges must be rejected");
            let rendered = format!("{error:#}");
            assert!(
                rendered.contains("resolves to private/local address"),
                "error should describe private-network guard failure for {address}: {rendered}"
            );
        }
    }

    #[test]
    fn openai_provider_rejects_special_use_ipv6_ranges_without_opt_in() {
        for address in [
            IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)),
        ] {
            let error = validate_openai_base_url_network_policy_with_resolver(
                "https://api.example.invalid/v1",
                false,
                |_host, _port| Ok(vec![address]),
            )
            .expect_err("special-use IPv6 ranges must be rejected");
            let rendered = format!("{error:#}");
            assert!(
                rendered.contains("resolves to private/local address"),
                "error should describe private-network guard failure for {address}: {rendered}"
            );
        }
    }

    #[test]
    fn openai_provider_rejects_unresolvable_hostname_without_opt_in() {
        let error = validate_openai_base_url_network_policy_with_resolver(
            "https://api.example.invalid/v1",
            false,
            |_host, _port| Err(std::io::Error::other("dns resolution failed")),
        )
        .expect_err("unresolvable hostname should fail closed without explicit opt-in");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("could not be resolved to enforce private-network guard"),
            "error should explain fail-closed resolution guard: {rendered}"
        );
    }

    #[test]
    fn openai_provider_accepts_private_base_url_with_explicit_opt_in() {
        let mut config = openai_test_config("https://10.10.10.10/v1".to_owned());
        config.allow_private_base_url = true;
        build_model_provider(&config)
            .expect("private-network base URL should build with explicit opt-in");
    }

    #[test]
    fn openai_embeddings_provider_requires_model_configuration() {
        let config = openai_test_config("http://127.0.0.1:0/v1".to_owned());
        let error = match build_embeddings_provider(&config) {
            Ok(_) => panic!("embeddings provider should require explicit model configuration"),
            Err(error) => error,
        };
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("openai_embeddings_model"),
            "error should reference embeddings model configuration: {rendered}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn embeddings_provider_rejects_oversized_batch() {
        let provider = build_embeddings_provider(&ModelProviderConfig::default())
            .expect("deterministic embeddings provider should build from defaults");
        let inputs = (0..=super::MAX_EMBEDDINGS_BATCH_SIZE)
            .map(|index| format!("item-{index}"))
            .collect::<Vec<_>>();
        let error = provider
            .embed(EmbeddingsRequest { inputs })
            .await
            .expect_err("oversized batch should fail");
        assert!(
            matches!(error, ProviderError::InvalidEmbeddingsRequest { .. }),
            "oversized batch must return validation error"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_status_snapshot_redacts_api_key() {
        let config = openai_test_config("http://127.0.0.1:0/v1".to_owned());
        let provider = build_model_provider(&config).expect("openai provider should build");
        let snapshot_json = serde_json::to_string(&provider.status_snapshot())
            .expect("provider status snapshot should serialize");
        assert!(
            !snapshot_json.contains("sk-test-secret"),
            "status snapshot must never include raw provider API keys"
        );
        assert!(
            snapshot_json.contains("\"api_key_configured\":true"),
            "status snapshot should surface whether an API key is configured"
        );
    }

    #[test]
    fn normalize_tool_arguments_accepts_large_json_payload_within_limit() {
        let json_overhead = r#"{"text":""}"#.len();
        let payload = format!(
            r#"{{"text":"{}"}}"#,
            "a".repeat(super::MAX_TOOL_ARGUMENT_BYTES - json_overhead)
        );

        let normalized = normalize_tool_arguments(payload.as_str())
            .expect("payload within byte limit should be accepted");

        assert_eq!(normalized.len(), super::MAX_TOOL_ARGUMENT_BYTES);
    }

    #[test]
    fn normalize_tool_arguments_rejects_oversized_payload() {
        let oversized = "a".repeat(super::MAX_TOOL_ARGUMENT_BYTES + 1);
        let error =
            normalize_tool_arguments(oversized.as_str()).expect_err("oversized payload must fail");
        assert!(error.contains("tool arguments exceed"), "error should mention byte limit");
    }

    #[test]
    fn raw_minimax_tool_call_markup_is_coerced_to_tool_event() {
        let raw = r#"<minimax:tool_call>
<invoke name="palyra.fs.apply_patch">
{"patch":"*** Begin Patch\n*** Add File: app.js\n+console.log('ok');\n*** End Patch\n"}
</invoke>
</minimax:tool_call>"#;

        let extraction = super::coerce_raw_tool_call_markup(raw)
            .expect("raw MiniMax markup should parse")
            .expect("raw MiniMax markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.apply_patch");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert!(
                    input["patch"].as_str().is_some_and(|patch| patch.contains("*** Add File")),
                    "{input}"
                );
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_accepts_complete_invoke_without_outer_close() {
        let raw = r#"<tool_call>
<invoke name="palyra.fs.read_file">
{"path":"app.js"}
</invoke>"#;

        let extraction = super::coerce_raw_tool_call_markup(raw)
            .expect("complete invoke should be recoverable without outer close")
            .expect("raw markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.read_file");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert_eq!(input["path"], "app.js");
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_accepts_valid_json_when_invoke_close_is_missing() {
        let raw = r#"<tool_call><invoke name="palyra.fs.read_file">{"path":"app.js"}</tool_call>"#;

        let extraction = super::coerce_raw_tool_call_markup(raw)
            .expect("valid raw invocation should be recoverable without closing invoke")
            .expect("raw markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.read_file");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert_eq!(input["path"], "app.js");
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_is_removed_from_surrounding_text() {
        let raw = r#"I will inspect it.
<tool_call>
<invoke name='palyra.fs.read_file'>
{"path":"app.js"}
</invoke>
</tool_call>
Then I will continue."#;

        let extraction = super::coerce_raw_tool_call_markup(raw)
            .expect("raw generic markup should parse")
            .expect("raw generic markup should be detected");

        assert!(!extraction.cleaned_text.contains("<tool_call>"));
        assert!(extraction.cleaned_text.contains("I will inspect it."));
        assert!(extraction.cleaned_text.contains("Then I will continue."));
        assert_eq!(extraction.tool_events.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_provider_retries_malformed_raw_tool_call_markup_then_succeeds() {
        let malformed = serde_json::json!({
            "choices": [{
                "message": {
                    "content": "<tool_call><invoke>{\"path\":\"app.js\"}"
                }
            }]
        })
        .to_string();
        let success = serde_json::json!({
            "choices": [{
                "message": {
                    "content": "Recovered after retry."
                },
                "finish_reason": "stop"
            }]
        })
        .to_string();
        let (base_url, request_count, handle) =
            spawn_scripted_server(vec![(200_u16, malformed), (200_u16, success)]);
        let provider = build_model_provider(&openai_test_config(base_url))
            .expect("openai provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await
            .expect("provider should retry malformed raw markup and return the later response");

        assert_eq!(response.retry_count, 1);
        assert_eq!(response.output.full_text, "Recovered after retry.");
        assert_eq!(request_count.load(Ordering::Relaxed), 2);
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_provider_rejects_oversized_tool_arguments() {
        let oversized_arguments = serde_json::json!({
            "text": "a".repeat(super::MAX_TOOL_ARGUMENT_BYTES + 1)
        })
        .to_string();
        let body = serde_json::json!({
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "palyra.echo",
                                    "arguments": oversized_arguments
                                }
                            }
                        ]
                    }
                }
            ]
        })
        .to_string();
        let (base_url, request_count, handle) = spawn_scripted_server(vec![(200_u16, body)]);
        let provider = build_model_provider(&openai_test_config(base_url))
            .expect("openai provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await;

        match response {
            Err(ProviderError::InvalidResponse { message, .. }) => {
                assert!(
                    message.contains("tool arguments exceed"),
                    "invalid response should explain tool argument size limit"
                );
            }
            other => panic!("expected invalid-response error, got {other:?}"),
        }
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            1,
            "provider should issue one upstream request before rejecting response"
        );
        handle.join().expect("scripted server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_provider_rejects_oversized_tool_input() {
        let body = serde_json::json!({
            "id": "msg_oversized",
            "model": "claude-3-5-sonnet-latest",
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_oversized",
                    "name": "palyra.echo",
                    "input": {
                        "text": "a".repeat(super::MAX_TOOL_ARGUMENT_BYTES + 1)
                    }
                }
            ],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1
            }
        })
        .to_string();
        let (base_url, request_count, handle) = spawn_scripted_server(vec![(200_u16, body)]);
        let provider = build_model_provider(&anthropic_test_config(base_url))
            .expect("anthropic provider should build");

        let response = provider
            .complete(ProviderRequest::from_input_text("hello".to_owned(), false, Vec::new(), None))
            .await;

        match response {
            Err(ProviderError::InvalidResponse { message, .. }) => {
                assert!(
                    message.contains("tool arguments exceed"),
                    "invalid response should explain Anthropic tool input size limit"
                );
            }
            other => panic!("expected invalid-response error, got {other:?}"),
        }
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            1,
            "provider should issue one upstream request before rejecting response"
        );
        handle.join().expect("scripted server thread should exit");
    }

    #[test]
    fn extract_completion_text_supports_multimodal_array_shape() {
        let text = extract_completion_text(Some(serde_json::json!([
            {"type":"output_text","text":"alpha"},
            {"type":"output_text","text":"beta"}
        ])));
        assert_eq!(text, "alpha beta");
    }

    #[test]
    fn sanitize_remote_error_truncates_multibyte_text_without_panicking() {
        let input = "\u{1F642}".repeat(300);
        let sanitized = sanitize_remote_error(input.as_str());
        assert!(
            sanitized.ends_with('…'),
            "long multi-byte messages should be truncated with marker"
        );
        let truncated =
            sanitized.strip_suffix('…').expect("truncated message should include marker suffix");
        assert_eq!(
            truncated.chars().count(),
            240,
            "truncated body should keep first 240 Unicode scalar values"
        );
        assert_eq!(
            sanitized.chars().count(),
            241,
            "result should include 240 chars plus a truncation marker"
        );
    }

    #[test]
    fn sanitize_remote_error_redacts_common_secret_patterns() {
        let input = "Bearer topsecret123 sk-test-secret-token api_key=abc token=qwe secret=xyz";
        let sanitized = sanitize_remote_error(input);

        assert!(!sanitized.contains("topsecret123"), "bearer token value must be redacted");
        assert!(!sanitized.contains("sk-test-secret-token"), "sk-* token should be redacted");
        assert!(!sanitized.contains("api_key=abc"), "api_key value must be redacted");
        assert!(!sanitized.contains("token=qwe"), "token value must be redacted");
        assert!(!sanitized.contains("secret=xyz"), "secret value must be redacted");
        assert!(sanitized.contains("<redacted>"), "sanitized error should carry redaction markers");
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedHttpRequest {
        path: String,
        headers: Vec<(String, String)>,
        body: String,
        received_at_ms: u64,
    }

    type InspectingServer =
        (String, Arc<AtomicUsize>, Arc<Mutex<Vec<CapturedHttpRequest>>>, thread::JoinHandle<()>);
    type ScriptedResponseHeaders = Vec<(String, String)>;
    type ScriptedHttpResponse = (u16, ScriptedResponseHeaders, String);

    fn spawn_scripted_server(
        responses: Vec<(u16, String)>,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
        let (base_url, request_count, _request_log, handle) =
            spawn_inspecting_scripted_server(responses);
        (base_url, request_count, handle)
    }

    fn spawn_inspecting_scripted_server(responses: Vec<(u16, String)>) -> InspectingServer {
        let responses = responses
            .into_iter()
            .map(|(status_code, body)| (status_code, Vec::new(), body))
            .collect();
        spawn_inspecting_scripted_server_with_headers(responses)
    }

    fn spawn_inspecting_scripted_server_with_headers(
        responses: Vec<ScriptedHttpResponse>,
    ) -> InspectingServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        listener
            .set_nonblocking(false)
            .expect("listener should stay in blocking mode for deterministic tests");
        let address = listener.local_addr().expect("listener should have local address");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_for_thread = Arc::clone(&request_count);
        let request_log: Arc<Mutex<Vec<CapturedHttpRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let request_log_for_thread = Arc::clone(&request_log);
        let handle = thread::spawn(move || {
            let started_at = Instant::now();
            for (status_code, headers, body) in responses {
                let (mut stream, _) = listener.accept().expect("scripted server should accept");
                request_count_for_thread.fetch_add(1, Ordering::Relaxed);
                let mut captured = read_http_request(&mut stream);
                captured.received_at_ms = started_at.elapsed().as_millis() as u64;
                request_log_for_thread
                    .lock()
                    .expect("request log lock should not be poisoned")
                    .push(captured);
                let status_text = match status_code {
                    200 => "OK",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    502 => "Bad Gateway",
                    503 => "Service Unavailable",
                    504 => "Gateway Timeout",
                    _ => "Error",
                };
                let extra_headers = headers
                    .iter()
                    .map(|(name, value)| format!("{name}: {value}\r\n"))
                    .collect::<String>();
                let response = format!(
                    "HTTP/1.1 {status_code} {status_text}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("scripted server should write response");
                let _ = stream.flush();
            }
        });
        (format!("http://{}/v1", address), request_count, request_log, handle)
    }

    fn read_http_request(stream: &mut TcpStream) -> CapturedHttpRequest {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout should be set for deterministic tests");
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        let request_line_bytes = reader
            .read_line(&mut request_line)
            .expect("scripted server should read HTTP request line");
        assert!(request_line_bytes > 0, "scripted openai request line must be present");
        let path = request_line.split_ascii_whitespace().nth(1).unwrap_or_default().to_owned();

        let mut content_length = 0_usize;
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            let bytes_read =
                reader.read_line(&mut line).expect("scripted server should read request line");
            if bytes_read == 0 || line == "\r\n" {
                break;
            }
            let line_trimmed = line.trim_end_matches(['\r', '\n']);
            if let Some((name, value)) = line_trimmed.split_once(':') {
                headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
                if name.trim().eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse::<usize>().unwrap_or(0);
                }
            }
        }

        let mut body_text = String::new();
        if content_length > 0 {
            let mut body = vec![0_u8; content_length];
            reader.read_exact(&mut body).expect("scripted server should read full request body");
            assert!(!body.is_empty(), "scripted openai requests must carry a non-empty JSON body");
            body_text = String::from_utf8_lossy(body.as_slice()).into_owned();
        }

        CapturedHttpRequest { path, headers, body: body_text, received_at_ms: 0 }
    }

    fn fake_chatgpt_oauth_token(account_id: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                super::OPENAI_CHATGPT_AUTH_CLAIM_NAMESPACE: {
                    super::OPENAI_CHATGPT_ACCOUNT_ID_CLAIM: account_id,
                }
            })
            .to_string(),
        );
        format!("{header}.{payload}.signature")
    }

    fn header_value(request: &CapturedHttpRequest, name: &str) -> Option<String> {
        request
            .headers
            .iter()
            .find(|(header_name, _)| header_name == name)
            .map(|(_, value)| value.clone())
    }
}
