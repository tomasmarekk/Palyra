use std::{
    future::Future,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProviderConfig {
    pub kind: ModelProviderKind,
    pub openai_base_url: String,
    pub openai_model: String,
    pub openai_api_key: Option<String>,
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
            openai_model: "gpt-4o-mini".to_owned(),
            openai_api_key: None,
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
    #[error("openai-compatible provider requires PALYRA_MODEL_PROVIDER_OPENAI_API_KEY")]
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
pub struct ProviderStatusSnapshot {
    pub kind: String,
    pub capabilities: ProviderCapabilitiesSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai_model: Option<String>,
    pub api_key_configured: bool,
    pub retry_policy: ProviderRetryPolicySnapshot,
    pub circuit_breaker: ProviderCircuitBreakerSnapshot,
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

    match config.kind {
        ModelProviderKind::Deterministic => {
            Ok(Arc::new(DeterministicProvider::new(config.clone())))
        }
        ModelProviderKind::OpenAiCompatible => Ok(Arc::new(OpenAiCompatibleProvider::new(config)?)),
    }
}

#[derive(Debug)]
struct DeterministicProvider {
    config: ModelProviderConfig,
}

impl DeterministicProvider {
    fn new(config: ModelProviderConfig) -> Self {
        Self { config }
    }
}

impl ModelProvider for DeterministicProvider {
    fn complete<'a>(
        &'a self,
        request: ProviderRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderResponse, ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            if request.vision_requested {
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
            Ok(ProviderResponse {
                events,
                prompt_tokens: estimate_token_count(request.input_text.as_str()),
                completion_tokens: token_count as u64,
                retry_count: 0,
            })
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
        }
    }
}

#[derive(Debug)]
struct OpenAiCompatibleProvider {
    config: ModelProviderConfig,
    client: Client,
    circuit_state: Mutex<CircuitBreakerState>,
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
        })
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
            if request.vision_requested {
                return Err(ProviderError::VisionUnsupported {
                    provider: ModelProviderKind::OpenAiCompatible.as_str().to_owned(),
                });
            }
            let api_key =
                self.config.openai_api_key.as_ref().ok_or(ProviderError::MissingApiKey)?;
            self.ensure_circuit_closed()?;

            let mut retry_count = 0_u32;
            for attempt in 0..=self.config.max_retries {
                match self.request_once(api_key.as_str(), &request).await {
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
                        if error.invalid_response {
                            return Err(ProviderError::InvalidResponse {
                                message: error.message,
                                retry_count,
                            });
                        }
                        return Err(ProviderError::RequestFailed {
                            message: error.message,
                            retryable: error.retryable,
                            retry_count,
                        });
                    }
                }
            }

            Err(ProviderError::RequestFailed {
                message: "openai-compatible execution exhausted retries".to_owned(),
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
        }
    }
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
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use super::{
        build_model_provider, extract_completion_text, normalize_tool_arguments,
        sanitize_remote_error, ModelProviderConfig, ModelProviderKind, ProviderError,
        ProviderEvent, ProviderRequest,
    };

    fn openai_test_config(base_url: String) -> ModelProviderConfig {
        ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_base_url: base_url,
            openai_model: "gpt-4o-mini".to_owned(),
            openai_api_key: Some("sk-test-secret".to_owned()),
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
        handle.join().expect("scripted server thread should exit");
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
