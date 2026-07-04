//! Provider-neutral data contract: request/message/output types shared by
//! every model backend, plus the hard output-size bounds applied before
//! persistence.
//!
//! Streaming semantics live here as the projection step: a finished
//! [`ProviderTurnOutput`] is re-chunked into bounded
//! [`ProviderEvent::ModelToken`] preview events by
//! [`provider_events_from_output`], and oversized text is truncated with an
//! explicit marker plus a `stream_spill_ref` so the truncation is auditable.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default number of model-output words per streamed preview event.
///
/// This preserves the daemon's historical `MAX_MODEL_TOKENS_PER_EVENT` value
/// while keeping the provider contract independent from daemon orchestration.
pub const DEFAULT_PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE: usize = 16;

/// Hard inline text bound: keeps a serialized turn output comfortably under
/// the default journal payload limit (256KiB) even with JSON overhead and
/// tool-call parts.
pub const MAX_PROVIDER_TURN_TEXT_BYTES: usize = 64 * 1024;
const PROVIDER_OUTPUT_TRUNCATED_MARKER: &str = "\n\n[provider output truncated]";

/// Base64-encoded image attached to a request for vision-capable models.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderImageInput {
    pub mime_type: String,
    pub bytes_base64: String,
    pub file_name: Option<String>,
    pub width_px: Option<u32>,
    pub height_px: Option<u32>,
    pub artifact_id: Option<String>,
}

/// Provider-neutral conversation role; adapters map it to each dialect.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMessageRole {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}

impl ProviderMessageRole {
    /// Returns the role string used by OpenAI-compatible chat payloads.
    #[must_use]
    pub const fn as_openai_role(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Developer => "developer",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }

    /// Returns the role string used by Anthropic message payloads.
    ///
    /// Anthropic only accepts user/assistant turns: system and developer
    /// content moves to the top-level system block, and tool results travel
    /// inside user turns, so everything except assistant maps to "user".
    #[must_use]
    pub const fn as_anthropic_role(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::System | Self::Developer | Self::User | Self::Tool => "user",
        }
    }
}

/// One content block inside a message: text or an inline image.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderMessageContentPart {
    Text { text: String },
    Image { image: ProviderImageInput },
}

impl ProviderMessageContentPart {
    /// Creates a text content part.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
}

/// One provider-neutral conversation turn. `tool_call_id` is set on tool
/// result messages; `tool_calls` is set on assistant turns that proposed
/// tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMessage {
    pub role: ProviderMessageRole,
    pub content: Vec<ProviderMessageContentPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ProviderMessageToolCall>,
}

/// Tool invocation recorded on an assistant turn, re-fed to providers when
/// continuing a tool exchange.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMessageToolCall {
    pub proposal_id: String,
    pub tool_name: String,
    pub input_json: Value,
}

/// Provider-neutral reasoning effort requested for a model turn.
///
/// Providers map this normalized value to their own wire shape, and must omit
/// it when the selected provider/model does not support configurable reasoning.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReasoningEffort {
    #[serde(rename = "none")]
    None,
    Minimal,
    Low,
    Medium,
    High,
    #[serde(rename = "xhigh")]
    XHigh,
}

impl ProviderReasoningEffort {
    /// Parses CLI/config spelling into the canonical effort enum.
    ///
    /// # Errors
    /// Returns an error when `value` is empty or not one of the supported
    /// normalized effort levels.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "none" | "off" | "disabled" | "false" => Ok(Self::None),
            "minimal" | "min" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" | "med" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" | "extra" | "extrahigh" => Ok(Self::XHigh),
            _ => Err(format!(
                "unsupported reasoning effort '{value}'; expected one of none, minimal, low, medium, high, xhigh"
            )),
        }
    }

    /// Returns the canonical config/JSON spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
        }
    }
}

/// Provider-neutral processing tier requested for a model turn.
///
/// Providers map this normalized value to their own wire shape, and must omit
/// it when the selected provider/model does not support service-tier control.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderServiceTier {
    Auto,
    Default,
    Priority,
    Flex,
}

impl ProviderServiceTier {
    /// Parses CLI/config spelling into the canonical service-tier enum.
    ///
    /// # Errors
    /// Returns an error when `value` is empty or not one of the supported
    /// normalized service tiers.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().replace(['-', '_'], "").as_str() {
            "auto" => Ok(Self::Auto),
            "default" | "standard" | "normal" | "off" | "false" | "nofast" => Ok(Self::Default),
            "priority" | "fast" | "on" | "true" => Ok(Self::Priority),
            "flex" | "lowcost" | "cheap" => Ok(Self::Flex),
            _ => Err(format!(
                "unsupported service tier '{value}'; expected one of auto, default, priority, flex"
            )),
        }
    }

    /// Returns the canonical config/JSON spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Default => "default",
            Self::Priority => "priority",
            Self::Flex => "flex",
        }
    }
}

/// Returns true when a provider model id belongs to a known reasoning-capable
/// model family that accepts a normalized reasoning effort.
#[must_use]
pub fn model_id_supports_reasoning_effort(model_id: &str) -> bool {
    let normalized = model_id.trim().to_ascii_lowercase();
    let model = normalized.rsplit('/').next().unwrap_or(normalized.as_str());
    matches!(model, "o1" | "o1-mini" | "o1-pro" | "o3" | "o3-mini" | "o3-pro" | "o4-mini")
        || model.starts_with("o1-")
        || model.starts_with("o3-")
        || model.starts_with("o4-")
        || model.starts_with("gpt-5")
}

impl ProviderMessage {
    /// Creates a plain user text message.
    #[must_use]
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: ProviderMessageRole::User,
            content: vec![ProviderMessageContentPart::text(text)],
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
        }
    }

    /// Rebuilds the assistant turn corresponding to a completed output so the
    /// conversation can be re-fed to a provider (e.g. after tool execution).
    #[must_use]
    pub fn assistant_from_output(output: &ProviderTurnOutput) -> Self {
        let mut content = Vec::new();
        let mut tool_calls = Vec::new();
        for part in &output.content_parts {
            match part {
                ProviderOutputContentPart::Text { text } => {
                    if !text.is_empty() {
                        content.push(ProviderMessageContentPart::text(text.clone()));
                    }
                }
                ProviderOutputContentPart::ToolCall { proposal_id, tool_name, input_json } => {
                    tool_calls.push(ProviderMessageToolCall {
                        proposal_id: proposal_id.clone(),
                        tool_name: tool_name.clone(),
                        input_json: input_json.clone(),
                    });
                }
            }
        }
        if content.is_empty() && tool_calls.is_empty() && !output.full_text.is_empty() {
            content.push(ProviderMessageContentPart::text(output.full_text.clone()));
        }
        Self {
            role: ProviderMessageRole::Assistant,
            content,
            name: None,
            tool_call_id: None,
            tool_calls,
        }
    }

    /// Creates a tool result message answering the given tool call id.
    #[must_use]
    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: ProviderMessageRole::Tool,
            content: vec![ProviderMessageContentPart::text(content.into())],
            name: None,
            tool_call_id: Some(tool_call_id.into()),
            tool_calls: Vec::new(),
        }
    }

    /// Returns all text parts joined with newlines, ignoring image parts.
    #[must_use]
    pub fn text_content(&self) -> String {
        self.content
            .iter()
            .filter_map(|part| match part {
                ProviderMessageContentPart::Text { text } => Some(text.as_str()),
                ProviderMessageContentPart::Image { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// One completion request as every backend sees it.
///
/// `input_text` is the full model-visible prompt (used for token estimation
/// and cache keying); `user_visible_input_text` is the un-augmented user text
/// and is deliberately excluded from serialization so prompt augmentation
/// context never leaks into persisted payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRequest {
    pub input_text: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub user_visible_input_text: Option<String>,
    pub messages: Vec<ProviderMessage>,
    pub json_mode: bool,
    pub vision_inputs: Vec<ProviderImageInput>,
    pub model_override: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_catalog_snapshot: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ProviderReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<ProviderServiceTier>,
    pub prompt_segments: Vec<ProviderPromptSegment>,
    pub prompt_cache_policy: PromptCachePolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_report: Option<PromptCacheReport>,
}

/// Stable prompt segment classification for provider cache hints and request explainability.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPromptSegmentKind {
    System,
    Tool,
    Policy,
    Project,
    Memory,
    Session,
    Tail,
    CurrentTurn,
}

/// Provider-neutral cache hint for one prompt segment.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPromptCacheHint {
    LongLived,
    ShortLived,
    Volatile,
    Sensitive,
    Disabled,
}

/// Hash-only prompt segment metadata; raw prompt text stays in `input_text` and `messages`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderPromptSegment {
    pub kind: ProviderPromptSegmentKind,
    pub content_hash: String,
    pub byte_len: usize,
    pub trust_label: String,
    pub cache_hint: ProviderPromptCacheHint,
    pub invalidation_reason: Option<String>,
}

/// Strategy for choosing provider cache breakpoints.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheStrategy {
    ProviderDefault,
    StablePrefix,
    SystemAndTool,
    Disabled,
}

/// Provider-neutral prompt cache policy carried alongside a request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptCachePolicy {
    pub enabled: bool,
    pub ttl_ms: u64,
    pub strategy: PromptCacheStrategy,
    pub max_breakpoints: usize,
    pub provider_compatibility: String,
}

impl Default for PromptCachePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_ms: 300_000,
            strategy: PromptCacheStrategy::ProviderDefault,
            max_breakpoints: 4,
            provider_compatibility: "metadata_only".to_owned(),
        }
    }
}

/// Cache accounting emitted without raw prompt text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptCacheReport {
    pub eligible_bytes: usize,
    pub invalidated_bytes: usize,
    pub invalidation_reasons: Vec<String>,
    pub provider_request_hash: String,
}

impl ProviderRequest {
    /// Creates a single-turn request whose message history is just the input
    /// text as one user message.
    #[must_use]
    pub fn from_input_text(
        input_text: String,
        json_mode: bool,
        vision_inputs: Vec<ProviderImageInput>,
        model_override: Option<String>,
    ) -> Self {
        Self {
            messages: vec![ProviderMessage::user_text(input_text.clone())],
            input_text,
            user_visible_input_text: None,
            json_mode,
            vision_inputs,
            model_override,
            tool_catalog_snapshot: None,
            instruction_hash: None,
            context_trace_id: None,
            budget_profile: None,
            max_output_tokens: None,
            reasoning_effort: None,
            service_tier: None,
            prompt_segments: Vec::new(),
            prompt_cache_policy: PromptCachePolicy::default(),
            prompt_cache_report: None,
        }
    }

    /// Returns the message history to send, synthesizing a single user turn
    /// from `input_text` when no explicit messages were provided.
    #[must_use]
    pub fn effective_messages(&self) -> Vec<ProviderMessage> {
        if self.messages.is_empty() {
            vec![ProviderMessage::user_text(self.input_text.clone())]
        } else {
            self.messages.clone()
        }
    }
}

/// Uniform event consumed by the orchestrator: bounded text preview chunks
/// (`is_final` marks the last chunk of a completed turn) or a tool proposal
/// whose `input_json` is guaranteed-valid JSON bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEvent {
    ModelToken { token: String, is_final: bool },
    ToolProposal { proposal_id: String, tool_name: String, input_json: Vec<u8> },
}

/// Provider-neutral reason a turn ended; unrecognized vendor values map to
/// `Unknown` rather than failing the turn.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Cancelled,
    Error,
    Unknown,
}

impl ProviderFinishReason {
    /// Maps an OpenAI-compatible `finish_reason` string to the neutral enum.
    #[must_use]
    pub fn from_openai(value: Option<&str>) -> Self {
        match value.unwrap_or_default().trim().to_ascii_lowercase().as_str() {
            "stop" => Self::Stop,
            "length" => Self::Length,
            "tool_calls" | "function_call" => Self::ToolCalls,
            "content_filter" => Self::ContentFilter,
            _ => Self::Unknown,
        }
    }

    /// Maps an Anthropic `stop_reason` string to the neutral enum.
    #[must_use]
    pub fn from_anthropic(value: Option<&str>) -> Self {
        match value.unwrap_or_default().trim().to_ascii_lowercase().as_str() {
            "end_turn" | "stop_sequence" => Self::Stop,
            "max_tokens" => Self::Length,
            "tool_use" => Self::ToolCalls,
            _ => Self::Unknown,
        }
    }
}

/// Token accounting for one turn. `source` distinguishes provider-reported
/// counts ("provider") from local estimates ("estimated") so downstream
/// budget logic can weigh them differently.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub source: String,
}

impl ProviderUsage {
    /// Creates usage with `total_tokens` derived as the saturating sum.
    #[must_use]
    pub fn new(prompt_tokens: u64, completion_tokens: u64, source: impl Into<String>) -> Self {
        Self {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens.saturating_add(completion_tokens),
            source: source.into(),
        }
    }
}

/// Batch of texts to embed; validated against batch and byte limits before
/// any provider call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsRequest {
    pub inputs: Vec<String>,
}

/// Embedding vectors in the same order as the request inputs; all vectors
/// share `dimensions`.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingsResponse {
    pub model_name: String,
    pub dimensions: usize,
    pub vectors: Vec<Vec<f32>>,
    pub retry_count: u32,
}

/// Audio payload plus optional transcription hints (prompt, language).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioTranscriptionRequest {
    pub file_name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub prompt: Option<String>,
    pub language: Option<String>,
}

/// One timed transcript segment; `confidence` is provider-derived when
/// available.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AudioTranscriptionSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub confidence: Option<f64>,
}

/// Full transcription result: flattened text plus per-segment timing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AudioTranscriptionResponse {
    pub text: String,
    pub language: Option<String>,
    pub duration_ms: Option<u64>,
    pub model_name: String,
    pub retry_count: u32,
    pub segments: Vec<AudioTranscriptionSegment>,
}

/// One ordered block of a turn output: text or a proposed tool call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderOutputContentPart {
    Text { text: String },
    ToolCall { proposal_id: String, tool_name: String, input_json: Value },
}

/// Opaque upstream correlation ids kept for tracing and replay;
/// `stream_spill_ref` marks where truncated/spilled output can be located.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProviderRawProviderRefs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_trace_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_spill_ref: Option<String>,
}

/// Flags recording which safety projections were applied to a turn output;
/// `output_redacted` is set whenever text was truncated to the size bound.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRedactionState {
    pub output_redacted: bool,
    pub user_visible_projected: bool,
    pub diagnostics_redacted: bool,
}

impl Default for ProviderRedactionState {
    fn default() -> Self {
        Self { output_redacted: false, user_visible_projected: true, diagnostics_redacted: true }
    }
}

/// Canonical, size-bounded result of one completed model turn.
///
/// Invariant: `full_text` never exceeds the inline text bound; oversized
/// output is truncated with a visible marker, `redaction_state` is flagged,
/// and a `stream_spill_ref` is recorded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderTurnOutput {
    pub full_text: String,
    pub content_parts: Vec<ProviderOutputContentPart>,
    pub finish_reason: ProviderFinishReason,
    pub usage: ProviderUsage,
    pub raw_provider_refs: ProviderRawProviderRefs,
    pub redaction_state: ProviderRedactionState,
}

impl ProviderTurnOutput {
    /// Builds a text-only output, applying the inline size bound and
    /// recording truncation in the redaction state when it occurs.
    #[must_use]
    pub fn text(
        full_text: String,
        finish_reason: ProviderFinishReason,
        usage: ProviderUsage,
        raw_provider_refs: ProviderRawProviderRefs,
    ) -> Self {
        let (full_text, output_redacted) =
            project_provider_output_text(full_text, MAX_PROVIDER_TURN_TEXT_BYTES);
        let mut raw_provider_refs = raw_provider_refs;
        if output_redacted && raw_provider_refs.stream_spill_ref.is_none() {
            raw_provider_refs.stream_spill_ref = Some(provider_output_truncation_ref());
        }
        let content_parts = if full_text.is_empty() {
            Vec::new()
        } else {
            vec![ProviderOutputContentPart::Text { text: full_text.clone() }]
        };
        Self {
            full_text,
            content_parts,
            finish_reason,
            usage,
            raw_provider_refs,
            redaction_state: ProviderRedactionState {
                output_redacted,
                ..ProviderRedactionState::default()
            },
        }
    }
}

/// Returns a copy of `output` with `full_text` and every text content part
/// re-bounded for persistence, flagging truncation when any part was cut.
///
/// Needed because outputs assembled part-by-part (e.g. from accumulated tool
/// exchanges) can bypass the bound enforced by [`ProviderTurnOutput::text`].
#[must_use]
pub fn bounded_provider_turn_output_for_persistence(
    output: &ProviderTurnOutput,
) -> ProviderTurnOutput {
    let mut bounded = output.clone();
    let (full_text, full_text_redacted) =
        project_provider_output_text(bounded.full_text, MAX_PROVIDER_TURN_TEXT_BYTES);
    bounded.full_text = full_text;
    let mut output_redacted = full_text_redacted;
    for part in &mut bounded.content_parts {
        if let ProviderOutputContentPart::Text { text } = part {
            let (bounded_text, text_redacted) =
                project_provider_output_text(std::mem::take(text), MAX_PROVIDER_TURN_TEXT_BYTES);
            *text = bounded_text;
            output_redacted |= text_redacted;
        }
    }
    if output_redacted {
        bounded.redaction_state.output_redacted = true;
        if bounded.raw_provider_refs.stream_spill_ref.is_none() {
            bounded.raw_provider_refs.stream_spill_ref = Some(provider_output_truncation_ref());
        }
    }
    bounded
}

/// Appends `incoming` to `target` while keeping `target` within `max_bytes`
/// (plus room for the truncation marker); returns true once truncation has
/// occurred. Idempotent after truncation: a target already ending with the
/// marker rejects further input so the marker stays terminal across repeated
/// stream deltas.
#[doc(hidden)]
pub fn append_provider_text_with_hard_limit(
    target: &mut String,
    incoming: &str,
    max_bytes: usize,
) -> bool {
    if incoming.is_empty() {
        return false;
    }
    let limit = provider_output_text_limit(max_bytes);
    if target.ends_with(PROVIDER_OUTPUT_TRUNCATED_MARKER) {
        return true;
    }
    if target.len().saturating_add(incoming.len()) <= limit {
        target.push_str(incoming);
        return false;
    }

    // Cut at UTF-8 boundaries only: the budget is in bytes but the text must
    // remain valid for display and serialization.
    let prefix_budget = limit.saturating_sub(PROVIDER_OUTPUT_TRUNCATED_MARKER.len());
    if target.len() > prefix_budget {
        truncate_string_to_utf8_boundary(target, prefix_budget);
    } else if target.len() < prefix_budget {
        let remaining = prefix_budget.saturating_sub(target.len());
        target.push_str(utf8_prefix(incoming, remaining));
    }
    target.push_str(PROVIDER_OUTPUT_TRUNCATED_MARKER);
    true
}

fn project_provider_output_text(full_text: String, max_bytes: usize) -> (String, bool) {
    let limit = provider_output_text_limit(max_bytes);
    if full_text.len() <= limit {
        return (full_text, false);
    }
    let mut bounded = String::with_capacity(limit);
    append_provider_text_with_hard_limit(&mut bounded, full_text.as_str(), limit);
    (bounded, true)
}

fn provider_output_text_limit(max_bytes: usize) -> usize {
    max_bytes.max(PROVIDER_OUTPUT_TRUNCATED_MARKER.len())
}

fn provider_output_truncation_ref() -> String {
    "provider-output-inline-truncated".to_owned()
}

fn truncate_string_to_utf8_boundary(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

/// Detailed state for one zero-based provider/model attempt in a completion.
///
/// Values are safe for run snapshots and health surfaces: credential ids are
/// stable references, never raw secret material, and provider messages stay in
/// coarse error classes or repair hints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAttemptState {
    pub attempt_index: u32,
    pub provider_profile_id: String,
    pub credential_id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_until_unix_ms: Option<i64>,
    pub prompt_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_microusd: Option<u64>,
    pub final_disposition: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair_hint: Option<String>,
}

/// Audit record of one provider/model attempt within a single completion,
/// covering cache hits, failover hops, and terminal errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAttemptSummary {
    pub provider_id: String,
    pub model_id: String,
    pub outcome: String,
    pub retryable: bool,
    pub served_from_cache: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ProviderAttemptState>,
}

/// Full result of one provider completion call: the bounded turn output, its
/// projected events, token totals, and routing metadata (cache/retry/failover
/// attempt history).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponse {
    pub output: ProviderTurnOutput,
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

/// Returns true when the request carries image input anywhere (top-level
/// vision inputs or image parts inside messages), gating vision-capability
/// checks.
#[must_use]
pub fn provider_request_has_vision(request: &ProviderRequest) -> bool {
    !request.vision_inputs.is_empty()
        || request.effective_messages().iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ProviderMessageContentPart::Image { .. }))
        })
}

// Splits text into chunks of at most `max_words_per_chunk` words while
// preserving every byte of the original (including inter-word whitespace),
// so concatenating the chunks reconstructs the input exactly. That
// reconstruction property is what lets preview events double as the full
// output stream.
fn split_provider_stream_text(input: &str, max_words_per_chunk: usize) -> Vec<String> {
    if max_words_per_chunk == 0 || input.trim().is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_words = 0_usize;
    let mut pending_whitespace = String::new();
    let mut current_word = String::new();

    for ch in input.chars() {
        if ch.is_whitespace() {
            if current_word.is_empty() {
                pending_whitespace.push(ch);
            } else {
                if current_words == max_words_per_chunk {
                    chunks.push(std::mem::take(&mut current));
                    current_words = 0;
                }
                current.push_str(pending_whitespace.as_str());
                pending_whitespace.clear();
                current.push_str(current_word.as_str());
                current_word.clear();
                current_words = current_words.saturating_add(1);
                pending_whitespace.push(ch);
            }
            continue;
        }
        current_word.push(ch);
    }

    if !current_word.is_empty() {
        if current_words == max_words_per_chunk {
            chunks.push(std::mem::take(&mut current));
            current_words = 0;
        }
        current.push_str(pending_whitespace.as_str());
        current.push_str(current_word.as_str());
        current_words = current_words.saturating_add(1);
        pending_whitespace.clear();
    } else if !pending_whitespace.is_empty() && !current.is_empty() {
        current.push_str(pending_whitespace.as_str());
    }

    if current_words > 0 || !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Projects a finished turn output into the uniform event stream: text parts
/// become bounded `ModelToken` chunks, tool calls become `ToolProposal`s.
///
/// The last text token is marked final only when the turn proposes no tools;
/// a tool-calling turn continues after execution, so its text must not signal
/// completion to streaming consumers.
#[must_use]
pub fn provider_events_from_output(output: &ProviderTurnOutput) -> Vec<ProviderEvent> {
    let mut events = Vec::new();
    let should_mark_final_model_token =
        !matches!(output.finish_reason, ProviderFinishReason::ToolCalls)
            && !output
                .content_parts
                .iter()
                .any(|part| matches!(part, ProviderOutputContentPart::ToolCall { .. }));
    let mut last_model_token_index = None;
    for part in &output.content_parts {
        match part {
            ProviderOutputContentPart::Text { text } => {
                let chunks = split_provider_stream_text(
                    text.as_str(),
                    DEFAULT_PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE,
                );
                for token in chunks {
                    last_model_token_index = Some(events.len());
                    events.push(ProviderEvent::ModelToken { token, is_final: false });
                }
            }
            ProviderOutputContentPart::ToolCall { proposal_id, tool_name, input_json } => {
                events.push(ProviderEvent::ToolProposal {
                    proposal_id: proposal_id.clone(),
                    tool_name: tool_name.clone(),
                    input_json: serde_json::to_vec(input_json).unwrap_or_else(|_| b"{}".to_vec()),
                });
            }
        }
    }
    if should_mark_final_model_token {
        if let Some(index) = last_model_token_index {
            if let Some(ProviderEvent::ModelToken { is_final, .. }) = events.get_mut(index) {
                *is_final = true;
            }
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        bounded_provider_turn_output_for_persistence, provider_events_from_output, ProviderEvent,
        ProviderFinishReason, ProviderOutputContentPart, ProviderRawProviderRefs,
        ProviderRedactionState, ProviderTurnOutput, ProviderUsage, MAX_PROVIDER_TURN_TEXT_BYTES,
    };

    fn provider_output(
        content_parts: Vec<ProviderOutputContentPart>,
        finish_reason: ProviderFinishReason,
    ) -> ProviderTurnOutput {
        let full_text = content_parts
            .iter()
            .filter_map(|part| match part {
                ProviderOutputContentPart::Text { text } => Some(text.as_str()),
                ProviderOutputContentPart::ToolCall { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("");
        ProviderTurnOutput {
            full_text,
            content_parts,
            finish_reason,
            usage: ProviderUsage::new(1, 1, "test"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: ProviderRedactionState::default(),
        }
    }

    #[test]
    fn provider_turn_output_truncates_large_text_before_persistence() {
        let output = ProviderTurnOutput::text(
            "a".repeat(MAX_PROVIDER_TURN_TEXT_BYTES + 1024),
            ProviderFinishReason::Stop,
            ProviderUsage::new(1, 1, "test"),
            ProviderRawProviderRefs::default(),
        );

        assert!(output.full_text.len() <= MAX_PROVIDER_TURN_TEXT_BYTES);
        assert!(output.full_text.ends_with("[provider output truncated]"));
        assert!(output.redaction_state.output_redacted);
        assert_eq!(
            output.raw_provider_refs.stream_spill_ref.as_deref(),
            Some("provider-output-inline-truncated")
        );
        assert!(
            matches!(
                output.content_parts.first(),
                Some(ProviderOutputContentPart::Text { text }) if text == &output.full_text
            ),
            "{:?}",
            output.content_parts
        );
        let serialized = serde_json::to_vec(&output).expect("bounded output should serialize");
        assert!(
            serialized.len() < 256 * 1024,
            "bounded provider turn output should fit the default journal payload limit"
        );
    }

    #[test]
    fn bounded_provider_turn_output_for_persistence_bounds_manual_output() {
        let output = provider_output(
            vec![ProviderOutputContentPart::Text {
                text: "b".repeat(MAX_PROVIDER_TURN_TEXT_BYTES + 1024),
            }],
            ProviderFinishReason::Stop,
        );

        let bounded = bounded_provider_turn_output_for_persistence(&output);

        assert!(bounded.full_text.len() <= MAX_PROVIDER_TURN_TEXT_BYTES);
        assert!(bounded.redaction_state.output_redacted);
        assert_eq!(
            bounded.raw_provider_refs.stream_spill_ref.as_deref(),
            Some("provider-output-inline-truncated")
        );
        assert!(
            matches!(
                bounded.content_parts.first(),
                Some(ProviderOutputContentPart::Text { text })
                    if text.len() <= MAX_PROVIDER_TURN_TEXT_BYTES
                        && text.ends_with("[provider output truncated]")
            ),
            "{:?}",
            bounded.content_parts
        );
    }

    #[test]
    fn provider_events_from_output_defers_final_when_tool_call_follows_text() {
        let output = provider_output(
            vec![
                ProviderOutputContentPart::Text {
                    text: "I will inspect the workspace.".to_owned(),
                },
                ProviderOutputContentPart::ToolCall {
                    proposal_id: "proposal-1".to_owned(),
                    tool_name: "palyra.process.run".to_owned(),
                    input_json: json!({"command": "ls", "args": []}),
                },
            ],
            ProviderFinishReason::ToolCalls,
        );

        let events = provider_events_from_output(&output);

        assert!(
            matches!(events.first(), Some(ProviderEvent::ModelToken { is_final: false, .. })),
            "{events:?}"
        );
        assert!(
            matches!(events.last(), Some(ProviderEvent::ToolProposal { proposal_id, .. }) if proposal_id == "proposal-1"),
            "{events:?}"
        );
    }

    #[test]
    fn provider_events_from_output_marks_only_last_text_token_final_without_tool_calls() {
        let output = provider_output(
            vec![
                ProviderOutputContentPart::Text { text: "First part.".to_owned() },
                ProviderOutputContentPart::Text { text: "Final answer.".to_owned() },
            ],
            ProviderFinishReason::Stop,
        );

        let events = provider_events_from_output(&output);
        let final_flags = events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::ModelToken { is_final, .. } => Some(*is_final),
                ProviderEvent::ToolProposal { .. } => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(final_flags, vec![false, true]);
    }
}
