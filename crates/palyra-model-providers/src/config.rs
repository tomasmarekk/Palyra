//! Provider registry configuration, normalization, and network guardrails.

use std::{
    collections::HashMap,
    net::{IpAddr, ToSocketAddrs},
};

use anyhow::{Context, Result};
use palyra_common::secret_refs::SecretRef;
use serde::{Deserialize, Serialize};

use crate::contract::{
    model_id_supports_reasoning_effort, ProviderReasoningEffort, ProviderServiceTier,
};

pub use crate::providers::{capability_defaults_for_kind, capability_defaults_for_provider};

/// Default TTL for provider response cache entries, in milliseconds.
pub const DEFAULT_PROVIDER_RESPONSE_CACHE_TTL_MS: u64 = 30_000;
/// Default maximum number of response cache entries per registry-backed provider.
pub const DEFAULT_PROVIDER_RESPONSE_CACHE_MAX_ENTRIES: usize = 128;
/// Default model discovery cache TTL, in milliseconds.
pub const DEFAULT_PROVIDER_DISCOVERY_TTL_MS: u64 = 5 * 60 * 1_000;
/// Default health snapshot TTL, in milliseconds.
pub const DEFAULT_PROVIDER_HEALTH_TTL_MS: u64 = 60_000;
/// Default HTTP model provider request timeout, in milliseconds.
pub const DEFAULT_MODEL_PROVIDER_REQUEST_TIMEOUT_MS: u64 = 180_000;
/// Capability envelope advertised for a model: supported features, context
/// budget, and cost/latency tiers used for candidate ranking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCapabilitiesSnapshot {
    pub streaming_tokens: bool,
    pub tool_calls: bool,
    pub json_mode: bool,
    pub vision: bool,
    pub audio_transcribe: bool,
    pub embeddings: bool,
    pub reasoning: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasoning_efforts: Vec<String>,
    pub service_tier: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub service_tiers: Vec<String>,
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

/// Transport/protocol family a provider speaks.
///
/// `Anthropic` also covers Anthropic-compatible endpoints such as MiniMax;
/// the auth header style is selected separately via
/// [`ModelProviderAuthProviderKind`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderKind {
    Deterministic,
    OpenAiCompatible,
    Anthropic,
}

impl ModelProviderKind {
    /// Returns the canonical snake_case identifier used in config and snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::OpenAiCompatible => "openai_compatible",
            Self::Anthropic => "anthropic",
        }
    }

    /// Parses a provider kind from a config string, accepting common aliases.
    ///
    /// # Errors
    /// Returns an error when `value` does not name a supported provider kind.
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "deterministic" => Ok(Self::Deterministic),
            "openai_compatible" | "openai-compatible" | "openai" => Ok(Self::OpenAiCompatible),
            "anthropic" => Ok(Self::Anthropic),
            _ => anyhow::bail!("unsupported model provider kind: {value}"),
        }
    }
}

/// Functional role a registry model entry fulfills.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderModelRole {
    Chat,
    Embeddings,
    AudioTranscription,
}

impl ProviderModelRole {
    /// Returns the canonical snake_case identifier used in config and snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Embeddings => "embeddings",
            Self::AudioTranscription => "audio_transcription",
        }
    }
}

/// Origin of a model capability record, for audit and override precedence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMetadataSource {
    LegacyMigration,
    Static,
    Discovery,
    OperatorOverride,
}

impl ProviderMetadataSource {
    /// Returns the canonical snake_case identifier used in config and snapshots.
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

/// Coarse cost bucket used to rank failover candidates (cheapest first).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCostTier {
    Low,
    Standard,
    Premium,
}

impl ProviderCostTier {
    /// Returns the canonical snake_case identifier used in capability snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Standard => "standard",
            Self::Premium => "premium",
        }
    }
}

/// Coarse latency bucket used as a failover tiebreaker after cost.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLatencyTier {
    Low,
    Standard,
    High,
}

impl ProviderLatencyTier {
    /// Returns the canonical snake_case identifier used in capability snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Standard => "standard",
            Self::High => "high",
        }
    }
}

/// Configuration for one provider endpoint in the registry: transport kind,
/// base URL policy, credential references, and retry/circuit-breaker tuning.
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

/// Configuration for one model exposed by a registry provider, including the
/// capability envelope used for request/candidate matching.
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

/// Full provider registry: provider endpoints, model entries, role defaults,
/// and failover/cache/discovery/health tuning.
///
/// An empty registry is back-filled from the legacy single-provider fields of
/// [`ModelProviderConfig`] by [`ModelProviderConfig::normalized_registry`].
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

/// Vendor behind an auth profile; selects credential semantics such as the
/// auth header style on Anthropic-compatible transports.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderAuthProviderKind {
    Openai,
    Anthropic,
    Minimax,
    Xai,
    GoogleGemini,
    GoogleGeminiCli,
    Openrouter,
}

impl ModelProviderAuthProviderKind {
    /// Returns the canonical snake_case identifier used in config and snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
            Self::Minimax => "minimax",
            Self::Xai => "xai",
            Self::GoogleGemini => "google_gemini",
            Self::GoogleGeminiCli => "google_gemini_cli",
            Self::Openrouter => "openrouter",
        }
    }

    /// Parses an auth provider kind from a config string, accepting aliases.
    ///
    /// # Errors
    /// Returns an error when `value` does not name a supported auth provider.
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai_compatible" | "openai-compatible" => Ok(Self::Openai),
            "anthropic" => Ok(Self::Anthropic),
            "minimax" | "minimax-portal" => Ok(Self::Minimax),
            "xai" | "x-ai" | "grok" => Ok(Self::Xai),
            "google_gemini" | "google-gemini" | "gemini" => Ok(Self::GoogleGemini),
            "google_gemini_cli" | "google-gemini-cli" | "gemini_cli" | "gemini-cli" => {
                Ok(Self::GoogleGeminiCli)
            }
            "openrouter" | "open-router" => Ok(Self::Openrouter),
            _ => anyhow::bail!("unsupported model provider auth provider kind: {value}"),
        }
    }
}

/// Where a provider credential was sourced from, for audit display; the raw
/// secret itself is never exposed in snapshots.
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
    /// Returns the canonical snake_case identifier used in config and snapshots.
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

/// Top-level model provider configuration.
///
/// The flat `openai_*`/`anthropic_*` fields are the legacy single-provider
/// surface; `registry` is the multi-provider surface. When the registry is
/// empty it is synthesized from the legacy fields so both shapes behave
/// identically downstream.
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
    pub reasoning_effort: Option<ProviderReasoningEffort>,
    pub service_tier: Option<ProviderServiceTier>,
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
            openai_model: String::new(),
            anthropic_model: String::new(),
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
            reasoning_effort: None,
            service_tier: None,
            request_timeout_ms: DEFAULT_MODEL_PROVIDER_REQUEST_TIMEOUT_MS,
            max_retries: 2,
            retry_backoff_ms: 150,
            circuit_breaker_failure_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
            registry: ModelProviderRegistryConfig::default(),
        }
    }
}

fn default_reasoning_efforts() -> Vec<String> {
    [
        ProviderReasoningEffort::None,
        ProviderReasoningEffort::Minimal,
        ProviderReasoningEffort::Low,
        ProviderReasoningEffort::Medium,
        ProviderReasoningEffort::High,
        ProviderReasoningEffort::XHigh,
    ]
    .into_iter()
    .map(ProviderReasoningEffort::as_str)
    .map(ToOwned::to_owned)
    .collect()
}

impl ModelProviderConfig {
    /// Returns the validated provider registry, synthesizing one from the
    /// legacy single-provider fields when no registry is configured.
    ///
    /// # Errors
    /// Returns an error when the registry is structurally invalid: zero TTLs
    /// or cache sizes, missing providers, duplicate or malformed identifiers,
    /// unknown provider references, disallowed private base URLs, or default
    /// models with a mismatched role.
    pub fn normalized_registry(&self) -> Result<ModelProviderRegistryConfig> {
        let mut registry = self.registry.clone();
        if registry.providers.is_empty() && registry.models.is_empty() {
            registry = legacy_registry_from_config(self);
        }
        normalize_provider_registry(&mut registry)?;
        Ok(registry)
    }

    /// Returns the effective default chat model id, falling back to the
    /// legacy per-kind model fields when the registry defines none.
    #[must_use]
    #[allow(dead_code)]
    pub fn default_chat_model_id(&self) -> Option<String> {
        self.normalized_registry().ok().and_then(|registry| registry.default_chat_model_id).or_else(
            || match self.kind {
                ModelProviderKind::Deterministic => Some("deterministic".to_owned()),
                ModelProviderKind::OpenAiCompatible => {
                    configured_model_id(self.openai_model.as_str()).map(ToOwned::to_owned)
                }
                ModelProviderKind::Anthropic => {
                    configured_model_id(self.anthropic_model.as_str()).map(ToOwned::to_owned)
                }
            },
        )
    }

    /// Returns the effective default embeddings model id, falling back to the
    /// legacy `openai_embeddings_model` field when the registry defines none.
    #[must_use]
    #[allow(dead_code)]
    pub fn default_embeddings_model_id(&self) -> Option<String> {
        self.normalized_registry()
            .ok()
            .and_then(|registry| registry.default_embeddings_model_id)
            .or_else(|| self.openai_embeddings_model.clone())
    }
}

/// Returns a configured model id after trimming empty legacy/default fields.
#[must_use]
pub fn configured_model_id(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

// Synthesizes a one-provider registry from the legacy flat config fields so
// pre-registry deployments keep working unchanged.
fn legacy_registry_from_config(config: &ModelProviderConfig) -> ModelProviderRegistryConfig {
    let provider_id =
        crate::providers::legacy_provider_id(config.kind, config.auth_profile_provider_kind)
            .to_owned();
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
            Some(
                crate::providers::legacy_display_name(
                    config.kind,
                    config.auth_profile_provider_kind,
                )
                .to_owned(),
            ),
            None,
            None,
            None,
            None,
            "deterministic".to_owned(),
            crate::providers::default_auth_provider_kind(
                config.kind,
                config.auth_profile_provider_kind,
            ),
            capability_defaults_for_kind(config.kind, ProviderModelRole::Chat),
        ),
        ModelProviderKind::OpenAiCompatible => (
            Some(
                crate::providers::legacy_display_name(
                    config.kind,
                    config.auth_profile_provider_kind,
                )
                .to_owned(),
            ),
            Some(config.openai_base_url.clone()),
            config.openai_api_key.clone(),
            config.openai_api_key_secret_ref.clone(),
            config.openai_api_key_vault_ref.clone(),
            config.openai_model.clone(),
            crate::providers::default_auth_provider_kind(
                config.kind,
                config.auth_profile_provider_kind,
            ),
            capability_defaults_for_kind(config.kind, ProviderModelRole::Chat),
        ),
        ModelProviderKind::Anthropic => (
            Some(
                crate::providers::legacy_display_name(
                    config.kind,
                    config.auth_profile_provider_kind,
                )
                .to_owned(),
            ),
            Some(config.anthropic_base_url.clone()),
            config.anthropic_api_key.clone(),
            config.anthropic_api_key_secret_ref.clone(),
            config.anthropic_api_key_vault_ref.clone(),
            config.anthropic_model.clone(),
            crate::providers::default_auth_provider_kind(
                config.kind,
                config.auth_profile_provider_kind,
            ),
            capability_defaults_for_provider(
                config.kind,
                ProviderModelRole::Chat,
                config.auth_profile_provider_kind,
            ),
        ),
    };
    let configured_chat_model_id = configured_model_id(model_id.as_str()).map(ToOwned::to_owned);
    let mut models = Vec::new();
    if let Some(chat_model_id) = configured_chat_model_id.clone() {
        let mut capabilities = capabilities;
        if model_id_supports_reasoning_effort(chat_model_id.as_str()) {
            capabilities.reasoning = true;
            capabilities.reasoning_efforts = default_reasoning_efforts();
        }
        models.push(ProviderModelEntryConfig {
            model_id: chat_model_id,
            provider_id: provider_id.clone(),
            role: ProviderModelRole::Chat,
            enabled: true,
            metadata_source: ProviderMetadataSource::LegacyMigration,
            operator_override: false,
            capabilities,
        });
    }
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
        models,
        default_chat_model_id: configured_chat_model_id,
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

    if let Some(model_id) = registry.default_chat_model_id.clone() {
        synthesize_default_model_if_missing(
            registry,
            &providers,
            &mut model_ids,
            model_id.as_str(),
            ProviderModelRole::Chat,
        )?;
        validate_default_model_role(model_id.as_str(), ProviderModelRole::Chat, &model_ids)?;
    } else {
        registry.default_chat_model_id = registry
            .models
            .iter()
            .find(|model| model.enabled && model.role == ProviderModelRole::Chat)
            .map(|model| model.model_id.clone());
    }
    if let Some(model_id) = registry.default_embeddings_model_id.clone() {
        synthesize_default_model_if_missing(
            registry,
            &providers,
            &mut model_ids,
            model_id.as_str(),
            ProviderModelRole::Embeddings,
        )?;
        validate_default_model_role(model_id.as_str(), ProviderModelRole::Embeddings, &model_ids)?;
    }
    if let Some(model_id) = registry.default_audio_transcription_model_id.clone() {
        synthesize_default_model_if_missing(
            registry,
            &providers,
            &mut model_ids,
            model_id.as_str(),
            ProviderModelRole::AudioTranscription,
        )?;
        validate_default_model_role(
            model_id.as_str(),
            ProviderModelRole::AudioTranscription,
            &model_ids,
        )?;
    }
    Ok(())
}

fn synthesize_default_model_if_missing(
    registry: &mut ModelProviderRegistryConfig,
    providers: &HashMap<String, ProviderRegistryEntryConfig>,
    models: &mut HashMap<String, ProviderModelEntryConfig>,
    model_id: &str,
    role: ProviderModelRole,
) -> Result<()> {
    if models.contains_key(model_id) {
        return Ok(());
    }
    let provider =
        provider_for_unregistered_default_model(registry, providers, role).ok_or_else(|| {
            anyhow::anyhow!("default model '{}' has no enabled provider to route through", model_id)
        })?;
    let model = synthetic_default_model_entry(model_id, &provider, role);
    models.insert(model.model_id.clone(), model.clone());
    registry.models.push(model);
    Ok(())
}

fn provider_for_unregistered_default_model(
    registry: &ModelProviderRegistryConfig,
    providers: &HashMap<String, ProviderRegistryEntryConfig>,
    role: ProviderModelRole,
) -> Option<ProviderRegistryEntryConfig> {
    let enabled_providers =
        registry.providers.iter().filter(|provider| provider.enabled).cloned().collect::<Vec<_>>();
    if enabled_providers.len() == 1 {
        return enabled_providers.into_iter().next();
    }
    registry
        .models
        .iter()
        .find(|model| model.enabled && model.role == role)
        .and_then(|model| providers.get(model.provider_id.as_str()))
        .filter(|provider| provider.enabled)
        .cloned()
        .or_else(|| enabled_providers.first().cloned())
}

fn synthetic_default_model_entry(
    model_id: &str,
    provider: &ProviderRegistryEntryConfig,
    role: ProviderModelRole,
) -> ProviderModelEntryConfig {
    let mut capabilities =
        capability_defaults_for_provider(provider.kind, role, provider.auth_profile_provider_kind);
    capabilities.operator_override = true;
    capabilities.metadata_source = ProviderMetadataSource::OperatorOverride.as_str().to_owned();
    ProviderModelEntryConfig {
        model_id: model_id.to_owned(),
        provider_id: provider.provider_id.clone(),
        role,
        enabled: true,
        metadata_source: ProviderMetadataSource::OperatorOverride,
        operator_override: true,
        capabilities,
    }
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

/// Validates model-provider runtime configuration before provider construction.
///
/// This covers timeout/retry/circuit-breaker bounds, base URL network policy,
/// and registry normalization. It intentionally validates the legacy flat
/// fields and the synthesized/explicit registry so callers can keep accepting
/// both public TOML shapes unchanged.
///
/// # Errors
/// Returns an error when numeric policy values are invalid, a provider base URL
/// violates the private-network guard, or the provider registry is malformed.
pub fn validate_model_provider_config(config: &ModelProviderConfig) -> Result<()> {
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

/// Enforces the SSRF guard on a provider base URL: localhost, private, and
/// special-use addresses are rejected unless `allow_private_base_url` is set.
///
/// Hostnames are resolved eagerly and the guard fails closed when resolution
/// fails or yields any private/local address.
///
/// # Errors
/// Returns an error when the URL is malformed, lacks a host or resolvable
/// port, cannot be resolved, or targets a private/local network without the
/// explicit opt-in.
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

#[doc(hidden)]
pub fn validate_openai_base_url_network_policy_with_resolver<F>(
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::{
        validate_openai_base_url_network_policy_with_resolver, ModelProviderAuthProviderKind,
        ModelProviderConfig, ModelProviderKind,
    };

    #[test]
    fn auth_provider_kind_parse_preserves_aliases() {
        assert_eq!(
            ModelProviderAuthProviderKind::parse("openai-compatible").unwrap(),
            ModelProviderAuthProviderKind::Openai
        );
        assert_eq!(
            ModelProviderAuthProviderKind::parse("minimax-portal").unwrap(),
            ModelProviderAuthProviderKind::Minimax
        );
        assert_eq!(
            ModelProviderAuthProviderKind::parse("gemini-cli").unwrap(),
            ModelProviderAuthProviderKind::GoogleGeminiCli
        );
        assert!(ModelProviderAuthProviderKind::parse("unsupported").is_err());
    }

    #[test]
    fn minimax_legacy_registry_keeps_vendor_identity_and_disables_vision() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::Anthropic,
            anthropic_model: "MiniMax-M2".to_owned(),
            auth_profile_provider_kind: Some(ModelProviderAuthProviderKind::Minimax),
            ..ModelProviderConfig::default()
        };

        let registry = config.normalized_registry().unwrap();
        assert_eq!(registry.providers[0].provider_id, "minimax-primary");
        assert_eq!(
            registry.providers[0].auth_profile_provider_kind,
            Some(ModelProviderAuthProviderKind::Minimax)
        );
        assert!(!registry.models[0].capabilities.vision);
    }

    #[test]
    fn base_url_guard_fails_closed_for_private_dns_results() {
        let error = validate_openai_base_url_network_policy_with_resolver(
            "https://api.example.invalid/v1",
            false,
            |_host, _port| Ok(vec![IpAddr::V4(Ipv4Addr::new(10, 10, 10, 10))]),
        )
        .expect_err("private DNS results must be rejected without opt-in");
        let rendered = format!("{error:#}");

        assert!(rendered.contains("resolves to private/local address"));
        assert!(rendered.contains("allow_private_base_url"));
    }
}
