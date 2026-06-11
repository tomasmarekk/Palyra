//! Provider stream event model and accumulation.
//!
//! [`ProviderStreamEvent`] is the canonical incremental event vocabulary
//! (started/delta/tool/usage/completed/failed/cancelled);
//! [`ProviderStreamAccumulator`] folds those events into one size-bounded
//! [`ProviderTurnOutput`], recording a spill reference when text exceeds the
//! inline buffer cap. Non-streaming HTTP responses are funneled through the
//! same accumulator (see [`provider_output_from_text_and_tools`]) so both
//! paths share identical truncation and tool-call semantics.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    contract::{append_provider_text_with_hard_limit, MAX_PROVIDER_TURN_TEXT_BYTES},
    ProviderErrorEnvelope, ProviderEvent, ProviderFinishReason, ProviderOutputContentPart,
    ProviderRawProviderRefs, ProviderTurnOutput, ProviderUsage,
};

const DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES: usize = 256 * 1024;

/// Incremental event emitted while a provider turn is in flight.
///
/// `Completed`, `Failed`, and `Cancelled` are terminal: the accumulator
/// ignores every event that arrives after one of them.
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

/// Folds [`ProviderStreamEvent`]s into one bounded [`ProviderTurnOutput`].
///
/// Text deltas accumulate under the hard inline limit; once either the limit
/// or the configured buffer cap is crossed, a spill reference naming the
/// provider/model is recorded so the truncation point stays auditable.
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
    output_truncated: bool,
}

impl ProviderStreamAccumulator {
    /// Creates an accumulator with the default inline buffer cap.
    #[must_use]
    pub fn new(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self::with_buffer_cap(provider_id, model_id, DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES)
    }

    /// Creates an accumulator with an explicit buffer cap in bytes (floored
    /// to 1); crossing the cap records the spill reference.
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
            output_truncated: false,
        }
    }

    /// Applies one stream event; events arriving after a terminal event
    /// (completed/failed/cancelled) are silently ignored.
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
                let output_truncated = append_provider_text_with_hard_limit(
                    &mut self.full_text,
                    text.as_str(),
                    MAX_PROVIDER_TURN_TEXT_BYTES,
                );
                self.output_truncated |= output_truncated;
                if (output_truncated || self.full_text.len() > self.buffer_cap_bytes)
                    && self.spill_ref.is_none()
                {
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

    /// Consumes the accumulator and produces the bounded turn output,
    /// carrying forward any spill reference and truncation flag.
    #[must_use]
    pub fn finalize(mut self) -> ProviderTurnOutput {
        // The accumulator's spill ref wins over whatever Completed carried:
        // it reflects what actually overflowed during this stream.
        if let Some(spill_ref) = self.spill_ref.take() {
            self.raw_provider_refs.stream_spill_ref = Some(spill_ref);
        }
        let mut output = ProviderTurnOutput::text(
            self.full_text,
            self.finish_reason,
            self.usage,
            self.raw_provider_refs,
        );
        output.redaction_state.output_redacted |= self.output_truncated;
        output.content_parts.extend(self.tool_calls);
        output
    }
}

/// Builds a turn output from a non-streaming response by replaying it
/// through [`ProviderStreamAccumulator`], so HTTP and streamed paths share
/// the same truncation, spill, and tool-call semantics.
pub(super) fn provider_output_from_text_and_tools(
    full_text: String,
    tool_calls: Vec<ProviderEvent>,
    finish_reason: ProviderFinishReason,
    usage: ProviderUsage,
    raw_provider_refs: ProviderRawProviderRefs,
) -> ProviderTurnOutput {
    // No registry provider id is available at this layer; the trace ref and
    // provider model id stand in for spill-reference labeling only.
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
    // Tool-only turns still get an empty leading text part so consumers can
    // rely on a text block always being present in content_parts.
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

// Tool inputs are stored as raw bytes upstream; non-JSON bytes are wrapped as
// {"raw": ...} instead of dropped so the proposal survives intact.
fn tool_input_json_value(input_json: &[u8]) -> Value {
    serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }))
}
