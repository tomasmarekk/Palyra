use serde::{Deserialize, Serialize};
use serde_json::Value;

const PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE: usize =
    crate::orchestrator::MAX_MODEL_TOKENS_PER_EVENT;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderImageInput {
    pub mime_type: String,
    pub bytes_base64: String,
    pub file_name: Option<String>,
    pub width_px: Option<u32>,
    pub height_px: Option<u32>,
    pub artifact_id: Option<String>,
}

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

    #[must_use]
    pub const fn as_anthropic_role(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::System | Self::Developer | Self::User | Self::Tool => "user",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderMessageContentPart {
    Text { text: String },
    Image { image: ProviderImageInput },
}

impl ProviderMessageContentPart {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMessage {
    pub role: ProviderMessageRole,
    pub content: Vec<ProviderMessageContentPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl ProviderMessage {
    #[must_use]
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: ProviderMessageRole::User,
            content: vec![ProviderMessageContentPart::text(text)],
            name: None,
            tool_call_id: None,
        }
    }

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRequest {
    pub input_text: String,
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
}

impl ProviderRequest {
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
            json_mode,
            vision_inputs,
            model_override,
            tool_catalog_snapshot: None,
            instruction_hash: None,
            context_trace_id: None,
            budget_profile: None,
        }
    }

    #[must_use]
    pub fn effective_messages(&self) -> Vec<ProviderMessage> {
        if self.messages.is_empty() {
            vec![ProviderMessage::user_text(self.input_text.clone())]
        } else {
            self.messages.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEvent {
    ModelToken { token: String, is_final: bool },
    ToolProposal { proposal_id: String, tool_name: String, input_json: Vec<u8> },
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub source: String,
}

impl ProviderUsage {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderOutputContentPart {
    Text { text: String },
    ToolCall { proposal_id: String, tool_name: String, input_json: Value },
}

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
    #[must_use]
    pub fn text(
        full_text: String,
        finish_reason: ProviderFinishReason,
        usage: ProviderUsage,
        raw_provider_refs: ProviderRawProviderRefs,
    ) -> Self {
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
            redaction_state: ProviderRedactionState::default(),
        }
    }
}

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
    pub attempts: Vec<super::ProviderAttemptSummary>,
}

pub(super) fn provider_request_has_vision(request: &ProviderRequest) -> bool {
    !request.vision_inputs.is_empty()
        || request.effective_messages().iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ProviderMessageContentPart::Image { .. }))
        })
}

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

pub(super) fn provider_events_from_output(output: &ProviderTurnOutput) -> Vec<ProviderEvent> {
    let mut events = Vec::new();
    for part in &output.content_parts {
        match part {
            ProviderOutputContentPart::Text { text } => {
                let chunks = split_provider_stream_text(
                    text.as_str(),
                    PROVIDER_STREAM_EVENT_TOKEN_CHUNK_SIZE,
                );
                let chunk_count = chunks.len();
                events.extend(chunks.into_iter().enumerate().map(|(index, token)| {
                    ProviderEvent::ModelToken { token, is_final: index + 1 == chunk_count }
                }));
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
    events
}
