use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ulid::Ulid;

use crate::orchestrator::{estimate_token_count, split_model_tokens, MAX_MODEL_TOKENS_PER_EVENT};

const OPENAI_CHAT_COMPLETIONS_PATH: &str = "/chat/completions";
const OPENAI_RETRYABLE_STATUS_CODES: &[u16] = &[429, 500, 502, 503, 504];
// Keep provider envelope above default wasm module quota (256KiB) including base64 and JSON overhead.
const MAX_TOOL_ARGUMENT_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelProviderKind {
    Deterministic,
    OpenAiCompatible,
}

impl ModelProviderKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::OpenAiCompatible => "openai_compatible",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "deterministic" => Ok(Self::Deterministic),
            "openai_compatible" | "openai-compatible" | "openai" => Ok(Self::OpenAiCompatible),
            _ => anyhow::bail!("unsupported model provider kind: {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelProviderAuthProviderKind {
    Openai,
}

impl ModelProviderAuthProviderKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai_compatible" | "openai-compatible" => Ok(Self::Openai),
            _ => anyhow::bail!("unsupported model provider auth provider kind: {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelProviderCredentialSource {
    InlineConfig,
    VaultRef,
    AuthProfileApiKey,
    AuthProfileOauthAccessToken,
}

impl ModelProviderCredentialSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineConfig => "inline_config",
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
    pub allow_private_base_url: bool,
    pub openai_model: String,
    pub openai_embeddings_model: Option<String>,
    pub openai_embeddings_dims: Option<u32>,
    pub openai_api_key: Option<String>,
    pub openai_api_key_vault_ref: Option<String>,
    pub auth_profile_id: Option<String>,
    pub auth_profile_provider_kind: Option<ModelProviderAuthProviderKind>,
    pub credential_source: Option<ModelProviderCredentialSource>,
    pub request_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub circuit_breaker_failure_threshold: u32,
    pub circuit_breaker_cooldown_ms: u64,
}

impl Default for ModelProviderConfig {
    fn default() -> Self {
        Self {
            kind: ModelProviderKind::Deterministic,
            openai_base_url: "https://api.openai.com/v1".to_owned(),
            allow_private_base_url: false,
            openai_model: "gpt-4o-mini".to_owned(),
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            openai_api_key: None,
            openai_api_key_vault_ref: None,
            auth_profile_id: None,
            auth_profile_provider_kind: None,
            credential_source: None,
            request_timeout_ms: 15_000,
            max_retries: 2,
            retry_backoff_ms: 150,
            circuit_breaker_failure_threshold: 3,
            circuit_breaker_cooldown_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequest {
    pub input_text: String,
    pub json_mode: bool,
    pub vision_requested: bool,
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
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("model provider circuit breaker is open; retry after {retry_after_ms}ms")]
    CircuitOpen { retry_after_ms: u64 },
    #[error(
        "openai-compatible provider requires PALYRA_MODEL_PROVIDER_OPENAI_API_KEY, PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID, or model_provider.openai_api_key_vault_ref"
    )]
    MissingApiKey,
    #[error("provider '{provider}' does not support vision inputs")]
    VisionUnsupported { provider: String },
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProviderCapabilitiesSnapshot {
    pub streaming_tokens: bool,
    pub tool_calls: bool,
    pub json_mode: bool,
    pub vision: bool,
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
pub struct ProviderStatusSnapshot {
    pub kind: String,
    pub capabilities: ProviderCapabilitiesSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_model: Option<String>,
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
}

pub trait ModelProvider: Send + Sync {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>>;
    fn status_snapshot(&self) -> ProviderStatusSnapshot;
}

pub fn build_model_provider(config: &ModelProviderConfig) -> Result<Arc<dyn ModelProvider>> {
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
    if config.kind == ModelProviderKind::OpenAiCompatible {
        validate_openai_base_url_network_policy(
            config.openai_base_url.as_str(),
            config.allow_private_base_url,
        )?;
    }

    match config.kind {
        ModelProviderKind::Deterministic => {
            Ok(Arc::new(DeterministicProvider::new(config.clone())))
        }
        ModelProviderKind::OpenAiCompatible => Ok(Arc::new(OpenAiCompatibleProvider::new(config)?)),
    }
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
        if is_private_or_local_ip(address) {
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
    if let Some(address) =
        resolved_addresses.into_iter().find(|address| is_private_or_local_ip(*address))
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

fn is_private_or_local_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ipv4) => is_private_or_local_ipv4(ipv4),
        IpAddr::V6(ipv6) => is_private_or_local_ipv6(ipv6),
    }
}

fn is_private_or_local_ipv4(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
}

fn is_private_or_local_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped_ipv4) = address.to_ipv4_mapped() {
        return is_private_or_local_ipv4(mapped_ipv4);
    }
    address.is_loopback()
        || address.is_unicast_link_local()
        || address.is_unique_local()
        || address.is_unspecified()
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
            if request.vision_requested {
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
            self.record_runtime_metrics(
                false,
                prompt_tokens,
                completion_tokens,
                0,
                elapsed_millis_since(started_at),
            );
            Ok(ProviderResponse { events, prompt_tokens, completion_tokens, retry_count: 0 })
        })
    }

    fn status_snapshot(&self) -> ProviderStatusSnapshot {
        ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: false,
                json_mode: true,
                vision: false,
            },
            openai_base_url: None,
            openai_model: None,
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
        }
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

    async fn request_once(
        &self,
        api_key: &str,
        request: &ProviderRequest,
    ) -> Result<ProviderResponse, AttemptError> {
        let mut body = json!({
            "model": self.config.openai_model,
            "messages": [{"role":"user","content": request.input_text}],
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

        Ok(ProviderResponse {
            events,
            prompt_tokens: estimate_token_count(request.input_text.as_str()),
            completion_tokens,
            retry_count: 0,
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
            if request.vision_requested {
                self.record_runtime_metrics(true, 0, 0, 0, elapsed_millis_since(started_at));
                return Err(ProviderError::VisionUnsupported {
                    provider: ModelProviderKind::OpenAiCompatible.as_str().to_owned(),
                });
            }
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
        ProviderStatusSnapshot {
            kind: self.config.kind.as_str().to_owned(),
            capabilities: ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: false,
            },
            openai_base_url: Some(self.config.openai_base_url.clone()),
            openai_model: Some(self.config.openai_model.clone()),
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
        }
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
        net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use super::{
        build_model_provider, extract_completion_text, normalize_tool_arguments,
        sanitize_remote_error, validate_openai_base_url_network_policy_with_resolver,
        ModelProviderConfig, ModelProviderKind, ProviderError, ProviderEvent, ProviderRequest,
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
            auth_profile_id: None,
            auth_profile_provider_kind: None,
            credential_source: None,
            request_timeout_ms: 5_000,
            max_retries: 2,
            retry_backoff_ms: 1,
            circuit_breaker_failure_threshold: 2,
            circuit_breaker_cooldown_ms: 120_000,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deterministic_provider_streams_bounded_tokens() {
        let provider = build_model_provider(&ModelProviderConfig::default())
            .expect("provider should build from defaults");
        let request = ProviderRequest {
            input_text: (0..64).map(|index| format!("token{index}")).collect::<Vec<_>>().join(" "),
            json_mode: false,
            vision_requested: false,
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
                vision_requested: false,
            })
            .await
            .expect("deterministic provider should succeed");
        let failed = provider
            .complete(ProviderRequest {
                input_text: "vision request".to_owned(),
                json_mode: false,
                vision_requested: true,
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
                vision_requested: false,
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
                vision_requested: false,
            })
            .await;
        assert!(matches!(first, Err(ProviderError::RequestFailed { .. })));
        let second = provider
            .complete(ProviderRequest {
                input_text: "hello again".to_owned(),
                json_mode: false,
                vision_requested: false,
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
            |_host, _port| Ok(vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10))]),
        )
        .expect("hostname resolving to public IP should pass private-network guard");
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
                vision_requested: false,
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

    fn spawn_scripted_server(
        responses: Vec<(u16, String)>,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        listener
            .set_nonblocking(false)
            .expect("listener should stay in blocking mode for deterministic tests");
        let address = listener.local_addr().expect("listener should have local address");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_for_thread = Arc::clone(&request_count);
        let handle = thread::spawn(move || {
            for (status_code, body) in responses {
                let (mut stream, _) = listener.accept().expect("scripted server should accept");
                request_count_for_thread.fetch_add(1, Ordering::Relaxed);
                read_http_request(&mut stream);
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
        (format!("http://{}/v1", address), request_count, handle)
    }

    fn read_http_request(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout should be set for deterministic tests");
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        let mut content_length = 0_usize;
        loop {
            let mut line = String::new();
            let bytes_read =
                reader.read_line(&mut line).expect("scripted server should read request line");
            if bytes_read == 0 || line == "\r\n" {
                break;
            }
            let line_trimmed = line.trim_end_matches(['\r', '\n']);
            headers.push_str(line_trimmed);
            headers.push('\n');
            if let Some(value) = line_trimmed.strip_prefix("Content-Length:") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            }
        }

        if content_length > 0 {
            let mut body = vec![0_u8; content_length];
            reader.read_exact(&mut body).expect("scripted server should read full request body");
            assert!(!body.is_empty(), "scripted openai requests must carry a non-empty JSON body");
        }
    }
}
