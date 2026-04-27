use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    ProviderErrorEnvelope, ProviderEvent, ProviderFinishReason, ProviderOutputContentPart,
    ProviderRawProviderRefs, ProviderTurnOutput, ProviderUsage,
};

const DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderStreamEvent {
    Started { provider_id: String, model_id: String },
    Delta { text: String },
    ToolDelta { proposal_id: String, tool_name: String, input_json: Value },
    UsageDelta { prompt_tokens: u64, completion_tokens: u64, total_tokens: Option<u64> },
    Completed { finish_reason: ProviderFinishReason, raw_provider_refs: ProviderRawProviderRefs },
    Failed { error: ProviderErrorEnvelope },
    Cancelled { reason: String },
}

#[derive(Debug, Clone)]
pub struct ProviderStreamAccumulator {
    provider_id: String,
    model_id: String,
    full_text: String,
    tool_calls: Vec<ProviderOutputContentPart>,
    usage: ProviderUsage,
    finish_reason: ProviderFinishReason,
    raw_provider_refs: ProviderRawProviderRefs,
    finalized: bool,
    failed: Option<ProviderErrorEnvelope>,
    cancelled_reason: Option<String>,
    buffer_cap_bytes: usize,
    spill_ref: Option<String>,
}

impl ProviderStreamAccumulator {
    #[must_use]
    pub fn new(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self::with_buffer_cap(provider_id, model_id, DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES)
    }

    #[must_use]
    pub fn with_buffer_cap(
        provider_id: impl Into<String>,
        model_id: impl Into<String>,
        buffer_cap_bytes: usize,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            model_id: model_id.into(),
            full_text: String::new(),
            tool_calls: Vec::new(),
            usage: ProviderUsage::new(0, 0, "stream_accumulator"),
            finish_reason: ProviderFinishReason::Unknown,
            raw_provider_refs: ProviderRawProviderRefs::default(),
            finalized: false,
            failed: None,
            cancelled_reason: None,
            buffer_cap_bytes: buffer_cap_bytes.max(1),
            spill_ref: None,
        }
    }

    pub fn apply(&mut self, event: ProviderStreamEvent) {
        if self.finalized {
            return;
        }
        match event {
            ProviderStreamEvent::Started { provider_id, model_id } => {
                self.provider_id = provider_id;
                self.model_id = model_id;
            }
            ProviderStreamEvent::Delta { text } => {
                self.full_text.push_str(text.as_str());
                if self.full_text.len() > self.buffer_cap_bytes && self.spill_ref.is_none() {
                    self.spill_ref = Some(format!(
                        "provider-stream-inline-spill:{}:{}",
                        self.provider_id, self.model_id
                    ));
                }
            }
            ProviderStreamEvent::ToolDelta { proposal_id, tool_name, input_json } => {
                self.tool_calls.push(ProviderOutputContentPart::ToolCall {
                    proposal_id,
                    tool_name,
                    input_json,
                });
            }
            ProviderStreamEvent::UsageDelta { prompt_tokens, completion_tokens, total_tokens } => {
                self.usage.prompt_tokens = self.usage.prompt_tokens.saturating_add(prompt_tokens);
                self.usage.completion_tokens =
                    self.usage.completion_tokens.saturating_add(completion_tokens);
                self.usage.total_tokens = total_tokens.unwrap_or_else(|| {
                    self.usage.prompt_tokens.saturating_add(self.usage.completion_tokens)
                });
            }
            ProviderStreamEvent::Completed { finish_reason, raw_provider_refs } => {
                self.finish_reason = finish_reason;
                self.raw_provider_refs = raw_provider_refs;
                self.finalized = true;
            }
            ProviderStreamEvent::Failed { error } => {
                self.failed = Some(error);
                self.finish_reason = ProviderFinishReason::Error;
                self.finalized = true;
            }
            ProviderStreamEvent::Cancelled { reason } => {
                self.cancelled_reason = Some(reason);
                self.finish_reason = ProviderFinishReason::Cancelled;
                self.finalized = true;
            }
        }
    }

    #[must_use]
    pub fn finalize(mut self) -> ProviderTurnOutput {
        if let Some(spill_ref) = self.spill_ref.take() {
            self.raw_provider_refs.stream_spill_ref = Some(spill_ref);
        }
        let mut output = ProviderTurnOutput::text(
            self.full_text,
            self.finish_reason,
            self.usage,
            self.raw_provider_refs,
        );
        output.content_parts.extend(self.tool_calls);
        output
    }
}

pub(super) fn provider_output_from_text_and_tools(
    full_text: String,
    tool_calls: Vec<ProviderEvent>,
    finish_reason: ProviderFinishReason,
    usage: ProviderUsage,
    raw_provider_refs: ProviderRawProviderRefs,
) -> ProviderTurnOutput {
    let provider_id =
        raw_provider_refs.provider_trace_ref.clone().unwrap_or_else(|| "provider".to_owned());
    let model_id =
        raw_provider_refs.provider_model_id.clone().unwrap_or_else(|| "model".to_owned());
    let usage_source = usage.source.clone();
    let mut accumulator = ProviderStreamAccumulator::new(provider_id, model_id);
    if !full_text.is_empty() {
        accumulator.apply(ProviderStreamEvent::Delta { text: full_text });
    }
    for event in tool_calls {
        if let ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } = event {
            accumulator.apply(ProviderStreamEvent::ToolDelta {
                proposal_id,
                tool_name,
                input_json: tool_input_json_value(input_json.as_slice()),
            });
        }
    }
    accumulator.apply(ProviderStreamEvent::UsageDelta {
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: Some(usage.total_tokens),
    });
    accumulator.apply(ProviderStreamEvent::Completed { finish_reason, raw_provider_refs });
    let mut output = accumulator.finalize();
    output.usage.source = usage_source;
    if output.full_text.is_empty()
        && output
            .content_parts
            .iter()
            .all(|part| !matches!(part, ProviderOutputContentPart::Text { .. }))
    {
        output.content_parts.insert(0, ProviderOutputContentPart::Text { text: String::new() });
    }
    output
}

fn tool_input_json_value(input_json: &[u8]) -> Value {
    serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }))
}
