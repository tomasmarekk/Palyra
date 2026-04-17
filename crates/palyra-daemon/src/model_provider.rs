use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    future::Future,
    hash::{Hash, Hasher},
    net::{IpAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_common::secret_refs::SecretRef;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ulid::Ulid;

use crate::orchestrator::{estimate_token_count, split_model_tokens, MAX_MODEL_TOKENS_PER_EVENT};

const OPENAI_CHAT_COMPLETIONS_PATH: &str = "/chat/completions";
const OPENAI_EMBEDDINGS_PATH: &str = "/embeddings";
const OPENAI_AUDIO_TRANSCRIPTIONS_PATH: &str = "/audio/transcriptions";
const ANTHROPIC_MESSAGES_PATH: &str = "/v1/messages";
const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const OPENAI_RETRYABLE_STATUS_CODES: &[u16] = &[429, 500, 502, 503, 504];
// Keep provider envelope above default wasm module quota (256KiB) including base64 and JSON overhead.
const MAX_TOOL_ARGUMENT_BYTES: usize = 512 * 1024;
const MAX_EMBEDDINGS_BATCH_SIZE: usize = 64;
const MAX_EMBEDDINGS_INPUT_BYTES: usize = 256 * 1024;
const MAX_SINGLE_EMBEDDING_INPUT_BYTES: usize = 64 * 1024;
const DEFAULT_DETERMINISTIC_EMBEDDINGS_DIMS: usize = 64;
const DEFAULT_OPENAI_TRANSCRIPTION_MODEL: &str = "gpt-4o-mini-transcribe";
const DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS: u64 = 30_000;
const DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES: usize = 128;
const DEFAULT_PROVIDER_DISCOVERY_TTL_MS: u64 = 5 * 60 * 1_000;
const DEFAULT_PROVIDER_HEALTH_TTL_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderKind {
    Deterministic,
    OpenAiCompatible,
    Anthropic,
}

impl ModelProviderKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::OpenAiCompatible => "openai_compatible",
            Self::Anthropic => "anthropic",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "deterministic" => Ok(Self::Deterministic),
            "openai_compatible" | "openai-compatible" | "openai" => Ok(Self::OpenAiCompatible),
            "anthropic" => Ok(Self::Anthropic),
            _ => anyhow::bail!("unsupported model provider kind: {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderModelRole {
    Chat,
    Embeddings,
    AudioTranscription,
}

impl ProviderModelRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Embeddings => "embeddings",
            Self::AudioTranscription => "audio_transcription",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMetadataSource {
    LegacyMigration,
    Static,
    Discovery,
    OperatorOverride,
}

impl ProviderMetadataSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyMigration => "legacy_migration",
            Self::Static => "static",
            Self::Discovery => "discovery",
            Self::OperatorOverride => "operator_override",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostTier {
    Low,
    Standard,
    Premium,
}

impl ProviderCostTier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Standard => "standard",
            Self::Premium => "premium",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLatencyTier {
    Low,
    Standard,
    High,
}

impl ProviderLatencyTier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Standard => "standard",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRegistryEntryConfig {
    pub provider_id: String,
    pub display_name: Option<String>,
    pub kind: ModelProviderKind,
    pub base_url: Option<String>,
    pub allow_private_base_url: bool,
    pub enabled: bool,
    pub auth_profile_id: Option<String>,
    pub auth_profile_provider_kind: Option<ModelProviderAuthProviderKind>,
    pub api_key: Option<String>,
    pub api_key_secret_ref: Option<SecretRef>,
    pub api_key_vault_ref: Option<String>,
    pub credential_source: Option<ModelProviderCredentialSource>,
    pub request_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub circuit_breaker_failure_threshold: u32,
    pub circuit_breaker_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderModelEntryConfig {
    pub model_id: String,
    pub provider_id: String,
    pub role: ProviderModelRole,
    pub enabled: bool,
    pub metadata_source: ProviderMetadataSource,
    pub operator_override: bool,
    pub capabilities: ProviderCapabilitiesSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelProviderRegistryConfig {
    pub providers: Vec<ProviderRegistryEntryConfig>,
    pub models: Vec<ProviderModelEntryConfig>,
    pub default_chat_model_id: Option<String>,
    pub default_embeddings_model_id: Option<String>,
    pub default_audio_transcription_model_id: Option<String>,
    pub failover_enabled: bool,
    pub response_cache_enabled: bool,
    pub response_cache_ttl_ms: u64,
    pub response_cache_max_entries: usize,
    pub discovery_ttl_ms: u64,
    pub health_ttl_ms: u64,
}

impl Default for ModelProviderRegistryConfig {
    fn default() -> Self {
        Self {
            providers: Vec::new(),
            models: Vec::new(),
            default_chat_model_id: None,
            default_embeddings_model_id: None,
            default_audio_transcription_model_id: None,
            failover_enabled: true,
            response_cache_enabled: true,
            response_cache_ttl_ms: DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS,
            response_cache_max_entries: DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES,
            discovery_ttl_ms: DEFAULT_PROVIDER_DISCOVERY_TTL_MS,
            health_ttl_ms: DEFAULT_PROVIDER_HEALTH_TTL_MS,
        }
    }
}

fn provider_request_has_vision(request: &ProviderRequest) -> bool {
    !request.vision_inputs.is_empty()
}

fn build_openai_chat_content(request: &ProviderRequest) -> Value {
    if request.vision_inputs.is_empty() {
        return Value::String(request.input_text.clone());
    }

    let mut parts = Vec::with_capacity(request.vision_inputs.len().saturating_add(1));
    parts.push(json!({
        "type": "text",
        "text": request.input_text,
    }));
    for image in &request.vision_inputs {
        parts.push(json!({
            "type": "image_url",
            "image_url": {
                "url": format!("data:{};base64,{}", image.mime_type, image.bytes_base64),
                "detail": "low",
            }
        }));
    }
    Value::Array(parts)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderAuthProviderKind {
    Openai,
    Anthropic,
}

impl ModelProviderAuthProviderKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai_compatible" | "openai-compatible" => Ok(Self::Openai),
            "anthropic" => Ok(Self::Anthropic),
            _ => anyhow::bail!("unsupported model provider auth provider kind: {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderCredentialSource {
    InlineConfig,
    SecretRef,
    VaultRef,
    AuthProfileApiKey,
    AuthProfileOauthAccessToken,
}

impl ModelProviderCredentialSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineConfig => "inline_config",
            Self::SecretRef => "secret_ref",
            Self::VaultRef => "vault_ref",
            Self::AuthProfileApiKey => "auth_profile_api_key",
            Self::AuthProfileOauthAccessToken => "auth_profile_oauth_access_token",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProviderConfig {
    pub kind: ModelProviderKind,
    pub openai_base_url: String,
    pub anthropic_base_url: String,
    pub allow_private_base_url: bool,
    pub openai_model: String,
    pub anthropic_model: String,
    pub openai_embeddings_model: Option<String>,
    pub openai_embeddings_dims: Option<u32>,
    pub openai_api_key: Option<String>,
    pub openai_api_key_secret_ref: Option<SecretRef>,
    pub openai_api_key_vault_ref: Option<String>,
    pub anthropic_api_key: Option<String>,
    pub anthropic_api_key_secret_ref: Option<SecretRef>,
    pub anthropic_api_key_vault_ref: Option<String>,
    pub auth_profile_id: Option<String>,
    pub auth_profile_provider_kind: Option<ModelProviderAuthProviderKind>,
    pub credential_source: Option<ModelProviderCredentialSource>,
    pub request_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub circuit_breaker_failure_threshold: u32,
    pub circuit_breaker_cooldown_ms: u64,
    pub registry: ModelProviderRegistryConfig,
}

impl Default for ModelProviderConfig {
    fn default() -> Self {
        Self {
            kind: ModelProviderKind::Deterministic,
            openai_base_url: "https://api.openai.com/v1".to_owned(),
            anthropic_base_url: "https://api.anthropic.com".to_owned(),
            allow_private_base_url: false,
            openai_model: "gpt-4o-mini".to_owned(),
            anthropic_model: "claude-3-5-sonnet-latest".to_owned(),
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            openai_api_key: None,
            openai_api_key_secret_ref: None,
            openai_api_key_vault_ref: None,
            anthropic_api_key: None,
            anthropic_api_key_secret_ref: None,
            anthropic_api_key_vault_ref: None,
            auth_profile_id: None,
            auth_profile_provider_kind: None,
            credential_source: None,
            request_timeout_ms: 15_000,
            max_retries: 2,
            retry_backoff_ms: 150,
            circuit_breaker_failure_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
            registry: ModelProviderRegistryConfig::default(),
        }
    }
}

impl ModelProviderConfig {
    pub fn normalized_registry(&self) -> Result<ModelProviderRegistryConfig> {
        let mut registry = self.registry.clone();
        if registry.providers.is_empty() && registry.models.is_empty() {
            registry = legacy_registry_from_config(self);
        }
        normalize_provider_registry(&mut registry)?;
        Ok(registry)
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn default_chat_model_id(&self) -> Option<String> {
        self.normalized_registry().ok().and_then(|registry| registry.default_chat_model_id).or_else(
            || match self.kind {
                ModelProviderKind::Deterministic => Some("deterministic".to_owned()),
                ModelProviderKind::OpenAiCompatible => Some(self.openai_model.clone()),
                ModelProviderKind::Anthropic => Some(self.anthropic_model.clone()),
            },
        )
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn default_embeddings_model_id(&self) -> Option<String> {
        self.normalized_registry()
            .ok()
            .and_then(|registry| registry.default_embeddings_model_id)
            .or_else(|| self.openai_embeddings_model.clone())
    }
}

fn legacy_registry_from_config(config: &ModelProviderConfig) -> ModelProviderRegistryConfig {
    let provider_id = match config.kind {
        ModelProviderKind::Deterministic => "deterministic-primary".to_owned(),
        ModelProviderKind::OpenAiCompatible => "openai-primary".to_owned(),
        ModelProviderKind::Anthropic => "anthropic-primary".to_owned(),
    };
    let (
        display_name,
        base_url,
        api_key,
        api_key_secret_ref,
        api_key_vault_ref,
        model_id,
        auth_kind,
        capabilities,
    ) = match config.kind {
        ModelProviderKind::Deterministic => (
            Some("Deterministic".to_owned()),
            None,
            None,
            None,
            None,
            "deterministic".to_owned(),
            None,
            capability_defaults_for_kind(config.kind, ProviderModelRole::Chat),
        ),
        ModelProviderKind::OpenAiCompatible => (
            Some("OpenAI-compatible".to_owned()),
            Some(config.openai_base_url.clone()),
            config.openai_api_key.clone(),
            config.openai_api_key_secret_ref.clone(),
            config.openai_api_key_vault_ref.clone(),
            config.openai_model.clone(),
            Some(ModelProviderAuthProviderKind::Openai),
            capability_defaults_for_kind(config.kind, ProviderModelRole::Chat),
        ),
        ModelProviderKind::Anthropic => (
            Some("Anthropic".to_owned()),
            Some(config.anthropic_base_url.clone()),
            config.anthropic_api_key.clone(),
            config.anthropic_api_key_secret_ref.clone(),
            config.anthropic_api_key_vault_ref.clone(),
            config.anthropic_model.clone(),
            Some(ModelProviderAuthProviderKind::Anthropic),
            capability_defaults_for_kind(config.kind, ProviderModelRole::Chat),
        ),
    };
    let mut registry = ModelProviderRegistryConfig {
        providers: vec![ProviderRegistryEntryConfig {
            provider_id: provider_id.clone(),
            display_name,
            kind: config.kind,
            base_url,
            allow_private_base_url: config.allow_private_base_url,
            enabled: true,
            auth_profile_id: config.auth_profile_id.clone(),
            auth_profile_provider_kind: config.auth_profile_provider_kind.or(auth_kind),
            api_key,
            api_key_secret_ref,
            api_key_vault_ref,
            credential_source: config.credential_source,
            request_timeout_ms: config.request_timeout_ms,
            max_retries: config.max_retries,
            retry_backoff_ms: config.retry_backoff_ms,
            circuit_breaker_failure_threshold: config.circuit_breaker_failure_threshold,
            circuit_breaker_cooldown_ms: config.circuit_breaker_cooldown_ms,
        }],
        models: vec![ProviderModelEntryConfig {
            model_id: model_id.clone(),
            provider_id: provider_id.clone(),
            role: ProviderModelRole::Chat,
            enabled: true,
            metadata_source: ProviderMetadataSource::LegacyMigration,
            operator_override: false,
            capabilities,
        }],
        default_chat_model_id: Some(model_id),
        default_embeddings_model_id: None,
        default_audio_transcription_model_id: None,
        failover_enabled: true,
        response_cache_enabled: true,
        response_cache_ttl_ms: DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS,
        response_cache_max_entries: DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES,
        discovery_ttl_ms: DEFAULT_PROVIDER_DISCOVERY_TTL_MS,
        health_ttl_ms: DEFAULT_PROVIDER_HEALTH_TTL_MS,
    };
    if let Some(model_id) = config.openai_embeddings_model.clone() {
        registry.default_embeddings_model_id = Some(model_id.clone());
        registry.models.push(ProviderModelEntryConfig {
            model_id,
            provider_id,
            role: ProviderModelRole::Embeddings,
            enabled: true,
            metadata_source: ProviderMetadataSource::LegacyMigration,
            operator_override: false,
            capabilities: capability_defaults_for_kind(
                ModelProviderKind::OpenAiCompatible,
                ProviderModelRole::Embeddings,
            ),
        });
    }
    registry
}

fn normalize_provider_registry(registry: &mut ModelProviderRegistryConfig) -> Result<()> {
    if registry.response_cache_ttl_ms == 0 {
        anyhow::bail!("model provider response cache TTL must be greater than 0ms");
    }
    if registry.response_cache_max_entries == 0 {
        anyhow::bail!("model provider response cache max entries must be greater than 0");
    }
    if registry.discovery_ttl_ms == 0 {
        anyhow::bail!("model provider discovery TTL must be greater than 0ms");
    }
    if registry.health_ttl_ms == 0 {
        anyhow::bail!("model provider health TTL must be greater than 0ms");
    }
    if registry.providers.is_empty() {
        anyhow::bail!("model provider registry must define at least one provider");
    }
    if registry.models.is_empty() {
        anyhow::bail!("model provider registry must define at least one model");
    }

    let mut providers = HashMap::<String, ProviderRegistryEntryConfig>::new();
    for provider in &mut registry.providers {
        provider.provider_id = normalize_registry_identifier(
            provider.provider_id.as_str(),
            "model_provider.registry.providers[].provider_id",
        )?;
        if let Some(base_url) = provider.base_url.clone() {
            validate_provider_base_url(
                provider.kind,
                base_url.as_str(),
                provider.allow_private_base_url,
            )?;
        }
        if providers.insert(provider.provider_id.clone(), provider.clone()).is_some() {
            anyhow::bail!(
                "duplicate provider id '{}' in model provider registry",
                provider.provider_id
            );
        }
    }

    let mut model_ids = HashMap::<String, ProviderModelEntryConfig>::new();
    for model in &mut registry.models {
        model.model_id = model.model_id.trim().to_owned();
        if model.model_id.is_empty() {
            anyhow::bail!("model_provider.registry.models[].model_id cannot be empty");
        }
        model.provider_id = normalize_registry_identifier(
            model.provider_id.as_str(),
            "model_provider.registry.models[].provider_id",
        )?;
        if !providers.contains_key(model.provider_id.as_str()) {
            anyhow::bail!(
                "model '{}' references unknown provider '{}'",
                model.model_id,
                model.provider_id
            );
        }
        if model_ids.insert(model.model_id.clone(), model.clone()).is_some() {
            anyhow::bail!("duplicate model id '{}' in model provider registry", model.model_id);
        }
    }

    if let Some(model_id) = registry.default_chat_model_id.as_ref() {
        validate_default_model_role(model_id, ProviderModelRole::Chat, &model_ids)?;
    } else {
        registry.default_chat_model_id = registry
            .models
            .iter()
            .find(|model| model.enabled && model.role == ProviderModelRole::Chat)
            .map(|model| model.model_id.clone());
    }
    if let Some(model_id) = registry.default_embeddings_model_id.as_ref() {
        validate_default_model_role(model_id, ProviderModelRole::Embeddings, &model_ids)?;
    }
    if let Some(model_id) = registry.default_audio_transcription_model_id.as_ref() {
        validate_default_model_role(model_id, ProviderModelRole::AudioTranscription, &model_ids)?;
    }
    Ok(())
}

fn validate_default_model_role(
    model_id: &str,
    expected_role: ProviderModelRole,
    models: &HashMap<String, ProviderModelEntryConfig>,
) -> Result<()> {
    let model = models.get(model_id).ok_or_else(|| {
        anyhow::anyhow!("default model '{}' was not found in provider registry", model_id)
    })?;
    if model.role != expected_role {
        anyhow::bail!("default model '{}' must have role '{}'", model_id, expected_role.as_str());
    }
    Ok(())
}

fn normalize_registry_identifier(raw: &str, field: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        anyhow::bail!("{field} contains invalid identifier '{raw}'");
    }
    Ok(normalized)
}

fn validate_provider_base_url(
    kind: ModelProviderKind,
    base_url: &str,
    allow_private_base_url: bool,
) -> Result<()> {
    match kind {
        ModelProviderKind::Deterministic => Ok(()),
        ModelProviderKind::OpenAiCompatible | ModelProviderKind::Anthropic => {
            validate_openai_base_url_network_policy(base_url, allow_private_base_url)
        }
    }
}

fn capability_defaults_for_kind(
    kind: ModelProviderKind,
    role: ProviderModelRole,
) -> ProviderCapabilitiesSnapshot {
    match (kind, role) {
        (ModelProviderKind::Deterministic, ProviderModelRole::Chat) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: false,
                json_mode: true,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                max_context_tokens: Some(8_192),
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec![
                    "offline testing".to_owned(),
                    "deterministic smoke flows".to_owned(),
                ],
                known_limitations: vec!["no real provider auth".to_owned(), "no vision".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::OpenAiCompatible, ProviderModelRole::Chat) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: true,
                audio_transcribe: true,
                embeddings: false,
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
            }
        }
        (ModelProviderKind::Anthropic, ProviderModelRole::Chat) => ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: true,
            vision: true,
            audio_transcribe: false,
            embeddings: false,
            max_context_tokens: Some(200_000),
            cost_tier: ProviderCostTier::Premium.as_str().to_owned(),
            latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
            recommended_use_cases: vec![
                "long-context reasoning".to_owned(),
                "tool-heavy chat".to_owned(),
            ],
            known_limitations: vec!["audio transcription not supported".to_owned()],
            operator_override: false,
            metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
        },
        (_, ProviderModelRole::Embeddings) => ProviderCapabilitiesSnapshot {
            streaming_tokens: false,
            tool_calls: false,
            json_mode: false,
            vision: false,
            audio_transcribe: false,
            embeddings: true,
            max_context_tokens: Some(8_192),
            cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
            latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
            recommended_use_cases: vec!["memory indexing".to_owned()],
            known_limitations: vec!["text embeddings only".to_owned()],
            operator_override: false,
            metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
        },
        (_, ProviderModelRole::AudioTranscription) => ProviderCapabilitiesSnapshot {
            streaming_tokens: false,
            tool_calls: false,
            json_mode: false,
            vision: false,
            audio_transcribe: true,
            embeddings: false,
            max_context_tokens: None,
            cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
            latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
            recommended_use_cases: vec!["audio ingestion".to_owned()],
            known_limitations: vec![],
            operator_override: false,
            metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
        },
    }
}

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
    }
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
            models: Vec::new(),
        };
    };

    let providers = registry
        .providers
        .iter()
        .map(|provider| ProviderRegistryProviderSnapshot {
            provider_id: provider.provider_id.clone(),
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
                }
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
        models,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderImageInput {
    pub mime_type: String,
    pub bytes_base64: String,
    pub file_name: Option<String>,
    pub width_px: Option<u32>,
    pub height_px: Option<u32>,
    pub artifact_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequest {
    pub input_text: String,
    pub json_mode: bool,
    pub vision_inputs: Vec<ProviderImageInput>,
    pub model_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEvent {
    ModelToken { token: String, is_final: bool },
    ToolProposal { proposal_id: String, tool_name: String, input_json: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponse {
    pub events: Vec<ProviderEvent>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub retry_count: u32,
    pub provider_id: String,
    pub model_id: String,
    pub served_from_cache: bool,
    pub failover_count: u32,
    pub attempts: Vec<ProviderAttemptSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAttemptSummary {
    pub provider_id: String,
    pub model_id: String,
    pub outcome: String,
    pub retryable: bool,
    pub served_from_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsRequest {
    pub inputs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingsResponse {
    pub model_name: String,
    pub dimensions: usize,
    pub vectors: Vec<Vec<f32>>,
    pub retry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioTranscriptionRequest {
    pub file_name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub prompt: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AudioTranscriptionSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AudioTranscriptionResponse {
    pub text: String,
    pub language: Option<String>,
    pub duration_ms: Option<u64>,
    pub model_name: String,
    pub retry_count: u32,
    pub segments: Vec<AudioTranscriptionSegment>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("model provider circuit breaker is open; retry after {retry_after_ms}ms")]
    CircuitOpen { retry_after_ms: u64 },
    #[error(
        "openai-compatible provider requires PALYRA_MODEL_PROVIDER_OPENAI_API_KEY, PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID, model_provider.openai_api_key_secret_ref, or model_provider.openai_api_key_vault_ref"
    )]
    MissingApiKey,
    #[error(
        "anthropic provider requires PALYRA_MODEL_PROVIDER_ANTHROPIC_API_KEY, PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID, model_provider.anthropic_api_key_secret_ref, or model_provider.anthropic_api_key_vault_ref"
    )]
    MissingAnthropicApiKey,
    #[error(
        "openai-compatible embeddings provider requires model_provider.openai_embeddings_model or PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL"
    )]
    MissingEmbeddingsModel,
    #[error("provider '{provider}' does not support vision inputs")]
    VisionUnsupported { provider: String },
    #[error("embeddings request is invalid: {message}")]
    InvalidEmbeddingsRequest { message: String },
    #[error(
        "provider request failed after {retry_count} retries (retryable={retryable}): {message}"
    )]
    RequestFailed { message: String, retryable: bool, retry_count: u32 },
    #[error("provider response was invalid after {retry_count} retries: {message}")]
    InvalidResponse { message: String, retry_count: u32 },
    #[error("provider state lock was poisoned")]
    StatePoisoned,
}

impl ProviderError {
    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        match self {
            Self::RequestFailed { retry_count, .. } => *retry_count,
            Self::InvalidResponse { retry_count, .. } => *retry_count,
            _ => 0,
        }
    }

    #[must_use]
    pub const fn is_circuit_open(&self) -> bool {
        matches!(self, Self::CircuitOpen { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCapabilitiesSnapshot {
    pub streaming_tokens: bool,
    pub tool_calls: bool,
    pub json_mode: bool,
    pub vision: bool,
    pub audio_transcribe: bool,
    pub embeddings: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_context_tokens: Option<u32>,
    pub cost_tier: String,
    pub latency_tier: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommended_use_cases: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub known_limitations: Vec<String>,
    pub operator_override: bool,
    pub metadata_source: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRetryPolicySnapshot {
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderCircuitBreakerSnapshot {
    pub failure_threshold: u32,
    pub cooldown_ms: u64,
    pub consecutive_failures: u32,
    pub open: bool,
}

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
}

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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistryProviderSnapshot {
    pub provider_id: String,
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderRegistryModelSnapshot {
    pub model_id: String,
    pub provider_id: String,
    pub role: String,
    pub enabled: bool,
    pub capabilities: ProviderCapabilitiesSnapshot,
}

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
    pub models: Vec<ProviderRegistryModelSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderStatusSnapshot {
    pub kind: String,
    pub provider_id: String,
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
    pub health: ProviderHealthProbeSnapshot,
    pub discovery: ProviderDiscoverySnapshot,
    pub registry: ProviderRegistrySnapshot,
}

pub trait ModelProvider: Send + Sync {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>;
    fn transcribe_audio<'a>(
        &'a self,
        request: AudioTranscriptionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AudioTranscriptionResponse, ProviderError>> + Send + 'a>>;
    fn status_snapshot(&self) -> ProviderStatusSnapshot;
}

pub trait EmbeddingsProvider: Send + Sync {
    fn embed<'a>(
        &'a self,
        request: EmbeddingsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<EmbeddingsResponse, ProviderError>> + Send + 'a>>;
}

pub fn build_model_provider(config: &ModelProviderConfig) -> Result<Arc<dyn ModelProvider>> {
    validate_model_provider_config(config)?;
    Ok(Arc::new(RegistryBackedModelProvider::new(config.clone())?))
}

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

fn validate_model_provider_config(config: &ModelProviderConfig) -> Result<()> {
    if config.request_timeout_ms == 0 {
        anyhow::bail!("model provider request timeout must be greater than 0ms");
    }
    if config.retry_backoff_ms == 0 {
        anyhow::bail!("model provider retry backoff must be greater than 0ms");
    }
    if config.circuit_breaker_failure_threshold == 0 {
        anyhow::bail!("model provider circuit breaker failure threshold must be greater than 0");
    }
    if config.circuit_breaker_cooldown_ms == 0 {
        anyhow::bail!("model provider circuit breaker cooldown must be greater than 0ms");
    }
    match config.kind {
        ModelProviderKind::OpenAiCompatible => {
            validate_openai_base_url_network_policy(
                config.openai_base_url.as_str(),
                config.allow_private_base_url,
            )?;
        }
        ModelProviderKind::Anthropic => {
            validate_openai_base_url_network_policy(
                config.anthropic_base_url.as_str(),
                config.allow_private_base_url,
            )?;
        }
        ModelProviderKind::Deterministic => {}
    }
    let _ = config.normalized_registry()?;
    Ok(())
}

pub fn validate_openai_base_url_network_policy(
    base_url: &str,
    allow_private_base_url: bool,
) -> Result<()> {
    validate_openai_base_url_network_policy_with_resolver(
        base_url,
        allow_private_base_url,
        resolve_hostname_ip_addrs,
    )
}

fn validate_openai_base_url_network_policy_with_resolver<F>(
    base_url: &str,
    allow_private_base_url: bool,
    resolver: F,
) -> Result<()>
where
    F: Fn(&str, u16) -> std::io::Result<Vec<IpAddr>>,
{
    let parsed = reqwest::Url::parse(base_url)
        .context("model_provider.openai_base_url must be a valid absolute URL")?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("model_provider.openai_base_url must include a host"))?;

    if allow_private_base_url {
        return Ok(());
    }

    if is_localhost_hostname(host) {
        anyhow::bail!(
            "model_provider.openai_base_url host '{}' targets localhost/private network; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host
        );
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        if palyra_common::netguard::is_private_or_local_ip(address) {
            anyhow::bail!(
                "model_provider.openai_base_url host '{}' targets localhost/private network; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
                host
            );
        }
        return Ok(());
    }

    let port = parsed.port_or_known_default().ok_or_else(|| {
        anyhow::anyhow!(
            "model_provider.openai_base_url must include an explicit port for unknown URL schemes"
        )
    })?;
    let resolved_addresses = resolver(host, port).map_err(|error| {
        anyhow::anyhow!(
            "model_provider.openai_base_url host '{}' could not be resolved to enforce private-network guard: {}; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host,
            error
        )
    })?;
    if resolved_addresses.is_empty() {
        anyhow::bail!(
            "model_provider.openai_base_url host '{}' resolved with no addresses; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host
        );
    }
    if let Some(address) = resolved_addresses
        .into_iter()
        .find(|address| palyra_common::netguard::is_private_or_local_ip(*address))
    {
        anyhow::bail!(
            "model_provider.openai_base_url host '{}' resolves to private/local address '{}'; set model_provider.allow_private_base_url=true or PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL=true to override for trusted local testing",
            host,
            address
        );
    }
    Ok(())
}

fn resolve_hostname_ip_addrs(host: &str, port: u16) -> std::io::Result<Vec<IpAddr>> {
    (host, port)
        .to_socket_addrs()
        .map(|socket_addrs| socket_addrs.map(|socket_addr| socket_addr.ip()).collect())
}

fn is_localhost_hostname(host: &str) -> bool {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    normalized == "localhost" || normalized.ends_with(".localhost")
}

struct RegistryProviderRuntime {
    entry: ProviderRegistryEntryConfig,
    provider: Arc<dyn ModelProvider>,
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
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
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
            })?;
        let primary = compatible
            .iter()
            .find(|model| model.model_id == requested_model_id)
            .copied()
            .ok_or_else(|| ProviderError::RequestFailed {
                message: format!("requested chat model '{requested_model_id}' is unavailable"),
                retryable: false,
                retry_count: 0,
            })?;

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
        if self.registry.failover_enabled && request.model_override.is_none() {
            ordered.extend(
                fallbacks.into_iter().filter(|model| model.provider_id != primary.provider_id),
            );
        }
        Ok(ordered)
    }

    fn response_cache_key(
        &self,
        request: &ProviderRequest,
        model: &ProviderModelEntryConfig,
    ) -> String {
        let mut hasher = DefaultHasher::new();
        model.provider_id.hash(&mut hasher);
        model.model_id.hash(&mut hasher);
        request.input_text.hash(&mut hasher);
        request.json_mode.hash(&mut hasher);
        for image in &request.vision_inputs {
            image.mime_type.hash(&mut hasher);
            image.bytes_base64.hash(&mut hasher);
            image.file_name.hash(&mut hasher);
            image.width_px.hash(&mut hasher);
            image.height_px.hash(&mut hasher);
            image.artifact_id.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    fn lookup_cached_response(
        &self,
        cache_key: &str,
        model: &ProviderModelEntryConfig,
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
        let mut response = entry.response;
        response.provider_id = model.provider_id.clone();
        response.model_id = model.model_id.clone();
        response.retry_count = 0;
        response.served_from_cache = true;
        response.failover_count = 0;
        response.attempts = vec![ProviderAttemptSummary {
            provider_id: model.provider_id.clone(),
            model_id: model.model_id.clone(),
            outcome: "cache_hit".to_owned(),
            retryable: false,
            served_from_cache: true,
        }];
        Some(response)
    }

    fn insert_cached_response(&self, cache_key: String, response: &ProviderResponse) {
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
                }
                if snapshot.discovery.discovered_model_ids.is_empty() {
                    snapshot.discovery.status = "ok".to_owned();
                    snapshot.discovery.discovered_model_ids = self
                        .registry
                        .models
                        .iter()
                        .filter(|model| model.provider_id == *provider_id)
                        .map(|model| model.model_id.clone())
                        .collect();
                    snapshot.discovery.source = "registry".to_owned();
                    snapshot.discovery.message =
                        Some("serving configured registry models".to_owned());
                }
                (provider_id.clone(), snapshot)
            })
            .collect()
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
                    );
                    return Err(error);
                }
            };
            let mut attempts = Vec::new();
            let mut failover_count = 0_u32;
            let mut last_error = None;

            for (index, model) in candidates.iter().enumerate() {
                let runtime = self
                    .providers
                    .get(model.provider_id.as_str())
                    .ok_or(ProviderError::StatePoisoned)?;
                let cache_key = self.response_cache_key(&request, model);
                if let Some(mut cached) = self.lookup_cached_response(cache_key.as_str(), model) {
                    cached.failover_count = failover_count;
                    cached.attempts =
                        attempts.into_iter().chain(cached.attempts.into_iter()).collect();
                    self.record_runtime_metrics(
                        false,
                        cached.prompt_tokens,
                        cached.completion_tokens,
                        cached.retry_count,
                        elapsed_millis_since(started_at),
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
                        attempts.push(ProviderAttemptSummary {
                            provider_id: model.provider_id.clone(),
                            model_id: model.model_id.clone(),
                            outcome: if index == 0 {
                                "success".to_owned()
                            } else {
                                "failover_success".to_owned()
                            },
                            retryable: false,
                            served_from_cache: false,
                        });
                        response.attempts = attempts;
                        self.insert_cached_response(cache_key, &response);
                        self.record_runtime_metrics(
                            false,
                            response.prompt_tokens,
                            response.completion_tokens,
                            response.retry_count,
                            elapsed_millis_since(started_at),
                        );
                        return Ok(response);
                    }
                    Err(error) => {
                        let retryable = matches!(
                            error,
                            ProviderError::CircuitOpen { .. }
                                | ProviderError::RequestFailed { retryable: true, .. }
                                | ProviderError::MissingApiKey
                                | ProviderError::MissingAnthropicApiKey
                        );
                        attempts.push(ProviderAttemptSummary {
                            provider_id: model.provider_id.clone(),
                            model_id: model.model_id.clone(),
                            outcome: "error".to_owned(),
                            retryable,
                            served_from_cache: false,
                        });
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

        ProviderStatusSnapshot {
            kind: default_provider_entry
                .map(|provider| provider.kind.as_str().to_owned())
                .unwrap_or_else(|| self.config.kind.as_str().to_owned()),
            provider_id: default_provider_id.clone(),
            model_id: default_model_id.clone(),
            capabilities: default_model.map(|model| model.capabilities.clone()).unwrap_or_else(
                || capability_defaults_for_kind(self.config.kind, ProviderModelRole::Chat),
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
                models,
            },
        }
    }
}

fn default_model_for_provider(
    registry: &ModelProviderRegistryConfig,
    provider_id: &str,
) -> Option<String> {
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
        request_timeout_ms: entry.request_timeout_ms,
        max_retries: entry.max_retries,
        retry_backoff_ms: entry.retry_backoff_ms,
        circuit_breaker_failure_threshold: entry.circuit_breaker_failure_threshold,
        circuit_breaker_cooldown_ms: entry.circuit_breaker_cooldown_ms,
        registry: ModelProviderRegistryConfig::default(),
    };
    match entry.kind {
        ModelProviderKind::Deterministic => {
            Ok(Arc::new(DeterministicProvider::new(config)) as Arc<dyn ModelProvider>)
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

#[derive(Debug)]
struct DeterministicProvider {
    config: ModelProviderConfig,
    runtime_metrics: Mutex<ProviderRuntimeMetrics>,
}

impl DeterministicProvider {
    fn new(config: ModelProviderConfig) -> Self {
        Self { config, runtime_metrics: Mutex::new(ProviderRuntimeMetrics::default()) }
    }

    fn record_runtime_metrics(
        &self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
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
                self.record_runtime_metrics(true, 0, 0, 0, elapsed_millis_since(started_at));
                return Err(ProviderError::VisionUnsupported {
                    provider: "deterministic".to_owned(),
                });
            }

            let completion_source = if request.json_mode {
                r#"{"ack":"ok"}"#.to_owned()
            } else {
                request.input_text.clone()
            };

            let mut tokens =
                split_model_tokens(completion_source.as_str(), MAX_MODEL_TOKENS_PER_EVENT);
            if tokens.is_empty() {
                tokens.push("ack".to_owned());
            }
            let token_count = tokens.len();
            let events = tokens
                .into_iter()
                .enumerate()
                .map(|(index, token)| ProviderEvent::ModelToken {
                    token,
                    is_final: index + 1 == token_count,
                })
                .collect::<Vec<_>>();
            let prompt_tokens = estimate_token_count(request.input_text.as_str());
            let completion_tokens = token_count as u64;
            let actual_model_id =
                request.model_override.clone().unwrap_or_else(|| "deterministic".to_owned());
            self.record_runtime_metrics(
                false,
                prompt_tokens,
                completion_tokens,
                0,
                elapsed_millis_since(started_at),
            );
            Ok(ProviderResponse {
                events,
                prompt_tokens,
                completion_tokens,
                retry_count: 0,
                provider_id: "deterministic-primary".to_owned(),
                model_id: actual_model_id.clone(),
                served_from_cache: false,
                failover_count: 0,
                attempts: vec![ProviderAttemptSummary {
                    provider_id: "deterministic-primary".to_owned(),
                    model_id: actual_model_id,
                    outcome: "success".to_owned(),
                    retryable: false,
                    served_from_cache: false,
                }],
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
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "deterministic-primary".to_owned(),
            model_id: Some("deterministic".to_owned()),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: false,
                json_mode: true,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                max_context_tokens: Some(8_192),
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec![
                    "offline testing".to_owned(),
                    "deterministic smoke flows".to_owned(),
                ],
                known_limitations: vec!["vision unsupported".to_owned()],
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
                models: Vec::new(),
            },
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot
    }
}

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

#[derive(Debug)]
struct AttemptError {
    message: String,
    retryable: bool,
    invalid_response: bool,
}

impl AttemptError {
    fn request_failed(message: String, retryable: bool) -> Self {
        Self { message, retryable, invalid_response: false }
    }

    fn invalid_response(message: String) -> Self {
        Self { message, retryable: false, invalid_response: true }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiChatCompletionResponse {
    choices: Vec<OpenAiChatChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatChoice {
    message: OpenAiChatMessage,
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
    function: Option<OpenAiToolFunction>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolFunction {
    name: String,
    #[serde(default)]
    arguments: String,
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
    content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
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
        let model_name = self
            .config
            .openai_embeddings_model
            .as_ref()
            .ok_or(ProviderError::MissingEmbeddingsModel)
            .map_err(|error| AttemptError::request_failed(error.to_string(), false))?;
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
                )
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
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
            ));
        }

        let parsed = response.json::<OpenAiEmbeddingsResponse>().await.map_err(|error| {
            AttemptError::invalid_response(format!(
                "openai-compatible embeddings response JSON parsing failed: {error}"
            ))
        })?;
        if parsed.data.is_empty() {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response did not include vectors".to_owned(),
            ));
        }

        let mut ordered_vectors: Vec<Option<Vec<f32>>> = vec![None; inputs.len()];
        for (position, item) in parsed.data.into_iter().enumerate() {
            let index = item.index.unwrap_or(position);
            if index >= ordered_vectors.len() {
                return Err(AttemptError::invalid_response(format!(
                    "openai-compatible embeddings response contained out-of-range vector index {index}"
                )));
            }
            if ordered_vectors[index].is_some() {
                return Err(AttemptError::invalid_response(format!(
                    "openai-compatible embeddings response duplicated vector index {index}"
                )));
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
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dimensions = vectors.first().map_or(0, |vector| vector.len());
        if dimensions == 0 {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response vectors must be non-empty".to_owned(),
            ));
        }
        if vectors.iter().any(|vector| vector.len() != dimensions) {
            return Err(AttemptError::invalid_response(
                "openai-compatible embeddings response returned inconsistent vector dimensions"
                    .to_owned(),
            ));
        }
        if let Some(expected_dimensions) = self.config.openai_embeddings_dims {
            if dimensions != expected_dimensions as usize {
                return Err(AttemptError::invalid_response(format!(
                    "openai-compatible embeddings response returned dims {dimensions}, expected {}",
                    expected_dimensions
                )));
            }
        }
        if vectors.len() != inputs.len() {
            return Err(AttemptError::invalid_response(format!(
                "openai-compatible embeddings response returned {} vectors for {} inputs",
                vectors.len(),
                inputs.len()
            )));
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
                            ProviderError::InvalidResponse { message: error.message, retry_count }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                            }
                        });
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "openai-compatible embeddings execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
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
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }

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

    fn audio_transcriptions_endpoint(&self) -> String {
        format!(
            "{}{}",
            self.config.openai_base_url.trim_end_matches('/'),
            OPENAI_AUDIO_TRANSCRIPTIONS_PATH
        )
    }

    fn transcription_model_name(&self) -> &str {
        if self.config.openai_model.contains("transcribe") {
            self.config.openai_model.as_str()
        } else {
            DEFAULT_OPENAI_TRANSCRIPTION_MODEL
        }
    }

    async fn request_once(
        &self,
        api_key: &str,
        request: &ProviderRequest,
    ) -> Result<ProviderResponse, AttemptError> {
        let mut body = json!({
            "model": request
                .model_override
                .clone()
                .unwrap_or_else(|| self.config.openai_model.clone()),
            "messages": [{"role":"user","content": build_openai_chat_content(request)}],
            "stream": false,
        });
        if request.json_mode {
            body["response_format"] = json!({"type":"json_object"});
        }

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
                )
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
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
            ));
        }

        let parsed = response.json::<OpenAiChatCompletionResponse>().await.map_err(|error| {
            AttemptError::invalid_response(format!(
                "openai-compatible response JSON parsing failed: {error}"
            ))
        })?;
        let choice = parsed.choices.into_iter().next().ok_or_else(|| {
            AttemptError::invalid_response(
                "openai-compatible response did not include choices".to_owned(),
            )
        })?;

        let mut events = Vec::new();
        for tool_call in choice.message.tool_calls {
            let Some(function) = tool_call.function else {
                continue;
            };
            if function.name.trim().is_empty() {
                continue;
            }
            let input_json =
                normalize_tool_arguments(function.arguments.as_str()).map_err(|error| {
                    AttemptError::invalid_response(format!(
                        "openai-compatible tool arguments are invalid: {error}"
                    ))
                })?;
            events.push(ProviderEvent::ToolProposal {
                proposal_id: Ulid::new().to_string(),
                tool_name: function.name,
                input_json,
            });
        }

        let completion_text = extract_completion_text(choice.message.content);
        let mut completion_tokens = 0_u64;
        let mut tokens = split_model_tokens(completion_text.as_str(), MAX_MODEL_TOKENS_PER_EVENT);
        if tokens.is_empty() && events.is_empty() {
            tokens.push("ack".to_owned());
        }
        let token_count = tokens.len();
        completion_tokens += token_count as u64;
        for (index, token) in tokens.into_iter().enumerate() {
            events.push(ProviderEvent::ModelToken { token, is_final: index + 1 == token_count });
        }
        let actual_model_id =
            request.model_override.clone().unwrap_or_else(|| self.config.openai_model.clone());

        Ok(ProviderResponse {
            events,
            prompt_tokens: estimate_token_count(request.input_text.as_str()),
            completion_tokens,
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
            }],
        })
    }

    async fn transcribe_audio_once(
        &self,
        api_key: &str,
        request: &AudioTranscriptionRequest,
    ) -> Result<AudioTranscriptionResponse, AttemptError> {
        let file_part = reqwest::multipart::Part::bytes(request.bytes.clone())
            .file_name(request.file_name.clone())
            .mime_str(request.content_type.as_str())
            .map_err(|error| {
                AttemptError::request_failed(
                    format!("invalid audio transcription content type: {error}"),
                    false,
                )
            })?;
        let mut form = reqwest::multipart::Form::new()
            .text("model", self.transcription_model_name().to_owned())
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
                )
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
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
            ));
        }

        let parsed =
            response.json::<OpenAiAudioTranscriptionResponse>().await.map_err(|error| {
                AttemptError::invalid_response(format!(
                    "openai-compatible audio transcription response JSON parsing failed: {error}"
                ))
            })?;
        let segments = parsed
            .segments
            .into_iter()
            .filter(|segment| !segment.text.trim().is_empty())
            .map(|segment| AudioTranscriptionSegment {
                start_ms: segment.start.unwrap_or_default().max(0.0) as u64 * 1_000,
                end_ms: segment.end.unwrap_or_default().max(0.0) as u64 * 1_000,
                text: segment.text,
                confidence: segment.avg_logprob.map(|value| value.exp()),
            })
            .collect::<Vec<_>>();
        Ok(AudioTranscriptionResponse {
            text: parsed.text,
            language: parsed.language,
            duration_ms: parsed.duration.map(|value| value.max(0.0) as u64 * 1_000),
            model_name: self.transcription_model_name().to_owned(),
            retry_count: 0,
            segments,
        })
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
                self.record_runtime_metrics(true, 0, 0, 0, elapsed_millis_since(started_at));
                return Err(ProviderError::MissingApiKey);
            };
            if let Err(error) = self.ensure_circuit_closed() {
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    error.retry_count(),
                    elapsed_millis_since(started_at),
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
                            ProviderError::InvalidResponse { message: error.message, retry_count }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                            }
                        };
                        self.record_runtime_metrics(
                            true,
                            0,
                            0,
                            provider_error.retry_count(),
                            elapsed_millis_since(started_at),
                        );
                        return Err(provider_error);
                    }
                }
            }

            let exhausted_error = ProviderError::RequestFailed {
                message: "openai-compatible execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
            };
            self.record_runtime_metrics(
                true,
                0,
                0,
                exhausted_error.retry_count(),
                elapsed_millis_since(started_at),
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
                            ProviderError::InvalidResponse { message: error.message, retry_count }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                            }
                        });
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "openai-compatible audio transcription exhausted retries".to_owned(),
                retryable: true,
                retry_count,
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let (consecutive_failures, open) = self
            .circuit_state
            .lock()
            .map(|state| {
                let now = Instant::now();
                let open = state.open_until.map(|until| now < until).unwrap_or(false);
                (state.consecutive_failures, open)
            })
            .unwrap_or((0, false));
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "openai-primary".to_owned(),
            model_id: Some(self.config.openai_model.clone()),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: true,
                audio_transcribe: true,
                embeddings: self.config.openai_embeddings_model.is_some(),
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
            openai_model: Some(self.config.openai_model.clone()),
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
            health: if self.config.openai_api_key.is_some() || self.config.auth_profile_id.is_some()
            {
                empty_health_probe_snapshot("ok", "provider configured", "runtime")
            } else {
                empty_health_probe_snapshot("missing_auth", "provider has no credential", "runtime")
            },
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: None,
                expires_at_unix_ms: None,
                discovered_model_ids: std::iter::once(self.config.openai_model.clone())
                    .chain(self.config.openai_embeddings_model.clone())
                    .collect(),
                source: "static".to_owned(),
                message: None,
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some(self.config.openai_model.clone()),
                default_embeddings_model_id: self.config.openai_embeddings_model.clone(),
                default_audio_transcription_model_id: Some(
                    self.transcription_model_name().to_owned(),
                ),
                failover_enabled: true,
                response_cache_enabled: true,
                providers: Vec::new(),
                models: Vec::new(),
            },
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot
    }
}

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
    ) {
        let mut metrics = lock_runtime_metrics(&self.runtime_metrics);
        metrics.record(error, prompt_tokens, completion_tokens, retry_count, latency_ms);
    }

    fn runtime_metrics_snapshot(&self) -> ProviderRuntimeMetricsSnapshot {
        lock_runtime_metrics(&self.runtime_metrics).snapshot()
    }

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
        let model_name =
            request.model_override.clone().unwrap_or_else(|| self.config.anthropic_model.clone());
        let mut content = vec![json!({
            "type": "text",
            "text": request.input_text,
        })];
        for image in &request.vision_inputs {
            content.push(json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": image.mime_type,
                    "data": image.bytes_base64,
                }
            }));
        }
        let mut body = json!({
            "model": model_name,
            "max_tokens": 2048,
            "messages": [{
                "role": "user",
                "content": content,
            }],
        });
        if request.json_mode {
            body["system"] = json!("Return valid JSON only.");
        }

        let response = self
            .client
            .post(self.messages_endpoint())
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_API_VERSION)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                AttemptError::request_failed(format!("anthropic request failed: {error}"), true)
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let retryable = OPENAI_RETRYABLE_STATUS_CODES.contains(&status);
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
            ));
        }

        let parsed = response.json::<AnthropicMessagesResponse>().await.map_err(|error| {
            AttemptError::invalid_response(format!(
                "anthropic response JSON parsing failed: {error}"
            ))
        })?;
        let mut events = Vec::new();
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
                    let input_json = serde_json::to_vec(
                        &block.input.unwrap_or(Value::Object(serde_json::Map::new())),
                    )
                    .map_err(|error| {
                        AttemptError::invalid_response(format!(
                            "anthropic tool payload serialization failed: {error}"
                        ))
                    })?;
                    events.push(ProviderEvent::ToolProposal {
                        proposal_id: block.id.unwrap_or_else(|| Ulid::new().to_string()),
                        tool_name,
                        input_json,
                    });
                }
                _ => {}
            }
        }

        let completion_text = completion_fragments.join("\n");
        let mut completion_tokens = 0_u64;
        let mut tokens = split_model_tokens(completion_text.as_str(), MAX_MODEL_TOKENS_PER_EVENT);
        if tokens.is_empty() && events.is_empty() {
            tokens.push(parsed.stop_reason.unwrap_or_else(|| "ack".to_owned()));
        }
        let token_count = tokens.len();
        completion_tokens += token_count as u64;
        for (index, token) in tokens.into_iter().enumerate() {
            events.push(ProviderEvent::ModelToken { token, is_final: index + 1 == token_count });
        }
        let actual_model_id =
            request.model_override.clone().unwrap_or_else(|| self.config.anthropic_model.clone());

        Ok(ProviderResponse {
            events,
            prompt_tokens: estimate_token_count(request.input_text.as_str()),
            completion_tokens,
            retry_count: 0,
            provider_id: "anthropic-primary".to_owned(),
            model_id: actual_model_id.clone(),
            served_from_cache: false,
            failover_count: 0,
            attempts: vec![ProviderAttemptSummary {
                provider_id: "anthropic-primary".to_owned(),
                model_id: actual_model_id,
                outcome: "success".to_owned(),
                retryable: false,
                served_from_cache: false,
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
                self.record_runtime_metrics(true, 0, 0, 0, elapsed_millis_since(started_at));
                return Err(ProviderError::MissingAnthropicApiKey);
            };
            if let Err(error) = self.ensure_circuit_closed() {
                self.record_runtime_metrics(
                    true,
                    0,
                    0,
                    error.retry_count(),
                    elapsed_millis_since(started_at),
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
                            ProviderError::InvalidResponse { message: error.message, retry_count }
                        } else {
                            ProviderError::RequestFailed {
                                message: error.message,
                                retryable: error.retryable,
                                retry_count,
                            }
                        };
                        self.record_runtime_metrics(
                            true,
                            0,
                            0,
                            provider_error.retry_count(),
                            elapsed_millis_since(started_at),
                        );
                        return Err(provider_error);
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "anthropic execution exhausted retries".to_owned(),
                retryable: true,
                retry_count,
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
            })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        let (consecutive_failures, open) = self
            .circuit_state
            .lock()
            .map(|state| {
                let now = Instant::now();
                let open = state.open_until.map(|until| now < until).unwrap_or(false);
                (state.consecutive_failures, open)
            })
            .unwrap_or((0, false));
        let mut snapshot = ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            provider_id: "anthropic-primary".to_owned(),
            model_id: Some(self.config.anthropic_model.clone()),
            capabilities: capability_defaults_for_kind(
                ModelProviderKind::Anthropic,
                ProviderModelRole::Chat,
            ),
            openai_base_url: None,
            anthropic_base_url: Some(self.config.anthropic_base_url.clone()),
            openai_model: None,
            anthropic_model: Some(self.config.anthropic_model.clone()),
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
            health: if self.config.anthropic_api_key.is_some()
                || self.config.auth_profile_id.is_some()
            {
                empty_health_probe_snapshot("ok", "provider configured", "runtime")
            } else {
                empty_health_probe_snapshot("missing_auth", "provider has no credential", "runtime")
            },
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: None,
                expires_at_unix_ms: None,
                discovered_model_ids: vec![self.config.anthropic_model.clone()],
                source: "static".to_owned(),
                message: None,
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some(self.config.anthropic_model.clone()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled: true,
                response_cache_enabled: true,
                providers: Vec::new(),
                models: Vec::new(),
            },
        };
        snapshot.registry = registry_snapshot_from_config(&self.config, &snapshot);
        snapshot
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ProviderRuntimeMetrics {
    request_count: u64,
    error_count: u64,
    total_retry_attempts: u64,
    total_prompt_tokens: u64,
    total_completion_tokens: u64,
    total_latency_ms: u64,
    last_latency_ms: u64,
    max_latency_ms: u64,
}

impl ProviderRuntimeMetrics {
    fn record(
        &mut self,
        error: bool,
        prompt_tokens: u64,
        completion_tokens: u64,
        retry_count: u32,
        latency_ms: u64,
    ) {
        self.request_count = self.request_count.saturating_add(1);
        if error {
            self.error_count = self.error_count.saturating_add(1);
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
            if self.request_count == 0 { 0 } else { self.total_prompt_tokens / self.request_count };
        let avg_completion_tokens_per_run = if self.request_count == 0 {
            0
        } else {
            self.total_completion_tokens / self.request_count
        };
        let avg_latency_ms =
            if self.request_count == 0 { 0 } else { self.total_latency_ms / self.request_count };
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

pub(crate) fn sanitize_remote_error(body: &str) -> String {
    let collapsed = body.replace(['\r', '\n', '\t'], " ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return "<empty>".to_owned();
    }
    let redacted = redact_remote_error_secrets(trimmed);
    const MAX_CHARS: usize = 240;
    if redacted.chars().count() <= MAX_CHARS {
        redacted
    } else {
        let truncated: String = redacted.chars().take(MAX_CHARS).collect();
        format!("{truncated}…")
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

fn redact_remote_error_secrets(raw: &str) -> String {
    const REDACTED: &[u8] = b"<redacted>";
    const KV_PATTERNS: [&[u8]; 3] = [b"api_key=", b"token=", b"secret="];

    let source = raw.as_bytes();
    let mut output = Vec::with_capacity(source.len());
    let mut index = 0;

    while index < source.len() {
        if starts_with_ascii_case_insensitive(source, index, b"bearer ") {
            output.extend_from_slice(b"Bearer ");
            output.extend_from_slice(REDACTED);
            index += b"bearer ".len();
            while index < source.len() && is_bearer_token_byte(source[index]) {
                index += 1;
            }
            continue;
        }

        if starts_with_ascii_case_insensitive(source, index, b"sk-") {
            let mut end = index + b"sk-".len();
            while end < source.len() && is_sk_token_byte(source[end]) {
                end += 1;
            }
            if end.saturating_sub(index + b"sk-".len()) >= 8 {
                output.extend_from_slice(REDACTED);
                index = end;
                continue;
            }
        }

        let mut matched_kv = false;
        for pattern in KV_PATTERNS {
            if starts_with_ascii_case_insensitive(source, index, pattern) {
                output.extend_from_slice(&source[index..index + pattern.len()]);
                index += pattern.len();
                let value_start = index;
                while index < source.len() && !is_secret_value_delimiter(source[index]) {
                    index += 1;
                }
                if index > value_start {
                    output.extend_from_slice(REDACTED);
                }
                matched_kv = true;
                break;
            }
        }
        if matched_kv {
            continue;
        }

        output.push(source[index]);
        index += 1;
    }

    String::from_utf8_lossy(output.as_slice()).into_owned()
}

fn starts_with_ascii_case_insensitive(source: &[u8], offset: usize, pattern: &[u8]) -> bool {
    if source.len().saturating_sub(offset) < pattern.len() {
        return false;
    }
    source[offset..offset + pattern.len()]
        .iter()
        .zip(pattern.iter())
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn is_bearer_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'+' | b'/' | b'=')
}

fn is_sk_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

fn is_secret_value_delimiter(byte: u8) -> bool {
    byte.is_ascii_whitespace()
        || matches!(byte, b'&' | b',' | b';' | b'"' | b'\'' | b')' | b']' | b'}')
}

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

fn normalize_tool_arguments(raw: &str) -> Result<Vec<u8>, String> {
    if raw.trim().is_empty() {
        return Ok(b"{}".to_vec());
    }
    if raw.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(format!(
            "tool arguments exceed {MAX_TOOL_ARGUMENT_BYTES} bytes before normalization"
        ));
    }
    if serde_json::from_str::<Value>(raw).is_ok() {
        return Ok(raw.as_bytes().to_vec());
    }
    let normalized = json!({ "raw": raw }).to_string().into_bytes();
    if normalized.len() > MAX_TOOL_ARGUMENT_BYTES {
        return Err(format!(
            "tool arguments exceed {MAX_TOOL_ARGUMENT_BYTES} bytes after normalization"
        ));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    use super::{
        build_embeddings_provider, build_model_provider, capability_defaults_for_kind,
        extract_completion_text, normalize_tool_arguments, sanitize_remote_error,
        validate_openai_base_url_network_policy_with_resolver, EmbeddingsRequest,
        ModelProviderAuthProviderKind, ModelProviderConfig, ModelProviderKind,
        ModelProviderRegistryConfig, ProviderError, ProviderEvent, ProviderImageInput,
        ProviderMetadataSource, ProviderModelEntryConfig, ProviderModelRole,
        ProviderRegistryEntryConfig, ProviderRequest,
    };

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

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_streams_bounded_tokens() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        let request = ProviderRequest {
            input_text: (0..64).map(|index| format!("token{index}")).collect::<Vec<_>>().join(" "),
            json_mode: false,
            vision_inputs: Vec::new(),
            model_override: None,
        };
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
        assert_eq!(tokens.len(), 16, "deterministic provider must enforce token bound");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_status_snapshot_reports_runtime_metrics() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        provider
            .complete(ProviderRequest {
                input_text: "measure deterministic metrics".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
            .await
            .expect("deterministic provider should succeed");
        let failed = provider
            .complete(ProviderRequest {
                input_text: "vision request".to_owned(),
                json_mode: false,
                vision_inputs: vec![ProviderImageInput {
                    mime_type: "image/png".to_owned(),
                    bytes_base64: "iVBORw0KGgo=".to_owned(),
                    file_name: Some("vision.png".to_owned()),
                    width_px: Some(1),
                    height_px: Some(1),
                    artifact_id: None,
                }],
                model_override: None,
            })
            .await;
        assert!(matches!(failed, Err(ProviderError::VisionUnsupported { .. })));

        let snapshot = provider.status_snapshot();
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
            .complete(ProviderRequest {
                input_text: "hello".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
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
            .filter(|event| matches!(event, ProviderEvent::ModelToken { .. }))
            .count();
        assert_eq!(model_tokens, 3, "response should map completion text into model tokens");
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
            .complete(ProviderRequest {
                input_text: "fallback please".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
            .await
            .expect("fallback provider should succeed");

        assert_eq!(response.provider_id, "anthropic-primary");
        assert_eq!(response.model_id, "claude-3-5-sonnet-latest");
        assert_eq!(response.failover_count, 1);
        assert_eq!(response.attempts.len(), 2);
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);
        assert_eq!(anthropic_request_count.load(Ordering::Relaxed), 1);

        let snapshot = provider.status_snapshot();
        assert_eq!(snapshot.registry.default_chat_model_id.as_deref(), Some("gpt-4o-mini"));
        assert_eq!(snapshot.registry.providers.len(), 2);

        openai_handle.join().expect("openai scripted server thread should exit");
        anthropic_handle.join().expect("anthropic scripted server thread should exit");
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
            .complete(ProviderRequest {
                input_text: "cache me".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
            .await
            .expect("first upstream request should succeed");
        let second = provider
            .complete(ProviderRequest {
                input_text: "cache me".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
            .await
            .expect("second request should be served from cache");

        assert!(!first.served_from_cache);
        assert!(second.served_from_cache);
        assert_eq!(second.attempts.len(), 1);
        assert_eq!(openai_request_count.load(Ordering::Relaxed), 1);

        openai_handle.join().expect("openai scripted server thread should exit");
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
            .complete(ProviderRequest {
                input_text: "hello".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
            .await;
        assert!(matches!(first, Err(ProviderError::RequestFailed { .. })));
        let second = provider
            .complete(ProviderRequest {
                input_text: "hello again".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
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
            .complete(ProviderRequest {
                input_text: "hello".to_owned(),
                json_mode: false,
                vision_inputs: Vec::new(),
                model_override: None,
            })
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
        let input = "é".repeat(300);
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
        body: String,
        received_at_ms: u64,
    }

    type InspectingServer =
        (String, Arc<AtomicUsize>, Arc<Mutex<Vec<CapturedHttpRequest>>>, thread::JoinHandle<()>);

    fn spawn_scripted_server(
        responses: Vec<(u16, String)>,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
        let (base_url, request_count, _request_log, handle) =
            spawn_inspecting_scripted_server(responses);
        (base_url, request_count, handle)
    }

    fn spawn_inspecting_scripted_server(responses: Vec<(u16, String)>) -> InspectingServer {
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
            for (status_code, body) in responses {
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
                let response = format!(
                    "HTTP/1.1 {status_code} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
        loop {
            let mut line = String::new();
            let bytes_read =
                reader.read_line(&mut line).expect("scripted server should read request line");
            if bytes_read == 0 || line == "\r\n" {
                break;
            }
            let line_trimmed = line.trim_end_matches(['\r', '\n']);
            if let Some((name, value)) = line_trimmed.split_once(':') {
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

        CapturedHttpRequest { path, body: body_text, received_at_ms: 0 }
    }
}
