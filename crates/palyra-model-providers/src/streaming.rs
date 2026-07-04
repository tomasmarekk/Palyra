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
use sha2::{Digest, Sha256};

use crate::errors::provider_failure_classification;
use crate::{
    append_provider_text_with_hard_limit, ProviderError, ProviderErrorEnvelope, ProviderEvent,
    ProviderFailureAction, ProviderFailureClass, ProviderFinishReason, ProviderOutputContentPart,
    ProviderRawProviderRefs, ProviderRecoveryDecision, ProviderTurnOutput, ProviderUsage,
    MAX_PROVIDER_TURN_TEXT_BYTES,
};

const DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES: usize = 256 * 1024;
const PROVIDER_SSE_NORMALIZER_SCHEMA_VERSION: u16 = 1;
const DEFAULT_PROVIDER_STREAM_IDLE_TIMEOUT_MS: u64 = 30_000;
/// Audit event emitted for provider SSE stream normalization decisions.
pub const PROVIDER_SSE_NORMALIZER_AUDIT_EVENT: &str = "provider.stream.sse.normalized";

/// Incremental event emitted while a provider turn is in flight.
///
/// `Completed`, `Failed`, and `Cancelled` are terminal: the accumulator
/// ignores every event that arrives after one of them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
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

/// Severity of one provider SSE normalizer audit event.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSseAuditSeverity {
    Info,
    Recovered,
    Failed,
}

/// Hash-only audit metadata for one provider SSE parser decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderSseAuditEvent {
    pub schema_version: u16,
    pub event_type: String,
    pub reason_code: String,
    pub severity: ProviderSseAuditSeverity,
    pub frame_index: Option<usize>,
    pub byte_len: usize,
    pub payload_sha256: Option<String>,
}

/// Result of normalizing provider SSE frames into canonical stream events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderSseNormalizationReport {
    pub schema_version: u16,
    pub events: Vec<ProviderStreamEvent>,
    pub audit_events: Vec<ProviderSseAuditEvent>,
    pub recovery_decision: Option<ProviderRecoveryDecision>,
    pub terminal: bool,
}

#[derive(Debug, Clone)]
struct ParsedSseFrame {
    event_name: Option<String>,
    data: String,
    byte_len: usize,
    payload_sha256: Option<String>,
}

/// Normalizes a provider SSE response body using the default idle timeout.
#[must_use]
pub fn normalize_provider_sse_stream(
    input: &str,
    provider_id: &str,
    model_id: &str,
) -> ProviderSseNormalizationReport {
    normalize_provider_sse_stream_with_idle_timeout(
        input,
        provider_id,
        model_id,
        None,
        DEFAULT_PROVIDER_STREAM_IDLE_TIMEOUT_MS,
    )
}

/// Normalizes provider SSE frames and fails closed when an observed idle gap
/// exceeds `idle_timeout_ms`.
#[must_use]
pub fn normalize_provider_sse_stream_with_idle_timeout(
    input: &str,
    provider_id: &str,
    model_id: &str,
    observed_idle_ms: Option<u64>,
    idle_timeout_ms: u64,
) -> ProviderSseNormalizationReport {
    let mut report = ProviderSseNormalizationReport {
        schema_version: PROVIDER_SSE_NORMALIZER_SCHEMA_VERSION,
        events: vec![ProviderStreamEvent::Started {
            provider_id: provider_id.to_owned(),
            model_id: model_id.to_owned(),
        }],
        audit_events: Vec::new(),
        recovery_decision: None,
        terminal: false,
    };
    let mut last_delta_hash: Option<String> = None;
    let mut usage_seen = false;

    for (frame_index, raw_frame) in input.split("\n\n").enumerate() {
        if raw_frame.trim().is_empty() {
            continue;
        }
        let frame = match parse_sse_frame(raw_frame, frame_index) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                report.audit_events.push(sse_audit_event(
                    "provider.stream.comment",
                    ProviderSseAuditSeverity::Info,
                    Some(frame_index),
                    raw_frame.len(),
                    None,
                ));
                continue;
            }
            Err(reason_code) => {
                fail_sse_report(
                    &mut report,
                    provider_id,
                    model_id,
                    reason_code.as_str(),
                    ProviderFailureClass::MalformedStream,
                    ProviderFailureAction::Retry,
                    Some(frame_index),
                    raw_frame.len(),
                    Some(stable_hash_text(raw_frame)),
                );
                return report;
            }
        };

        if report.terminal {
            if frame_has_usage_json(frame.data.as_str()) {
                report.audit_events.push(sse_audit_event(
                    "provider.stream.late_usage",
                    ProviderSseAuditSeverity::Recovered,
                    Some(frame_index),
                    frame.byte_len,
                    frame.payload_sha256,
                ));
            }
            continue;
        }

        if frame.data.trim() == "[DONE]" {
            push_sse_completed(provider_id, model_id, ProviderFinishReason::Unknown, &mut report);
            continue;
        }

        let value = match serde_json::from_str::<Value>(frame.data.as_str()) {
            Ok(value) => value,
            Err(_) => {
                fail_sse_report(
                    &mut report,
                    provider_id,
                    model_id,
                    "provider.stream.malformed_chunk",
                    ProviderFailureClass::MalformedStream,
                    ProviderFailureAction::Retry,
                    Some(frame_index),
                    frame.byte_len,
                    frame.payload_sha256,
                );
                return report;
            }
        };

        if let Some((prompt_tokens, completion_tokens, total_tokens)) = usage_delta(&value) {
            usage_seen = true;
            report.events.push(ProviderStreamEvent::UsageDelta {
                prompt_tokens,
                completion_tokens,
                total_tokens,
            });
        }

        if let Some(delta) = text_delta(&value) {
            let delta_hash = stable_hash_text(delta.as_str());
            if last_delta_hash.as_deref() == Some(delta_hash.as_str()) {
                report.audit_events.push(sse_audit_event(
                    "provider.stream.duplicate_delta",
                    ProviderSseAuditSeverity::Recovered,
                    Some(frame_index),
                    frame.byte_len,
                    Some(delta_hash),
                ));
            } else {
                last_delta_hash = Some(delta_hash);
                report.events.push(ProviderStreamEvent::Delta { text: delta });
            }
        }

        if let Some(finish_reason) = finish_reason(&value, frame.event_name.as_deref()) {
            if !usage_seen && frame_has_usage_json(frame.data.as_str()) {
                report.audit_events.push(sse_audit_event(
                    "provider.stream.usage_on_final_frame",
                    ProviderSseAuditSeverity::Info,
                    Some(frame_index),
                    frame.byte_len,
                    frame.payload_sha256.clone(),
                ));
            }
            push_sse_completed(provider_id, model_id, finish_reason, &mut report);
        }
    }

    if !report.terminal {
        let idle_exceeded = observed_idle_ms.is_some_and(|idle| idle > idle_timeout_ms);
        let (reason_code, class) = if idle_exceeded {
            ("provider.stream.idle_timeout", ProviderFailureClass::ProviderTimeout)
        } else {
            ("provider.stream.missing_final_event", ProviderFailureClass::MalformedStream)
        };
        fail_sse_report(
            &mut report,
            provider_id,
            model_id,
            reason_code,
            class,
            ProviderFailureAction::Retry,
            None,
            input.len(),
            Some(stable_hash_text(input)),
        );
    }

    report
}

fn parse_sse_frame(raw_frame: &str, frame_index: usize) -> Result<Option<ParsedSseFrame>, String> {
    let mut event_name = None;
    let mut data_lines = Vec::new();
    for line in raw_frame.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            return Err("provider.stream.malformed_chunk".to_owned());
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => event_name = Some(value.to_owned()),
            "data" => data_lines.push(value.to_owned()),
            "id" | "retry" => {}
            _ => {
                return Err(format!("provider.stream.unsupported_sse_field.{frame_index}"));
            }
        }
    }
    if data_lines.is_empty() {
        return Ok(None);
    }
    let data = data_lines.join("\n");
    Ok(Some(ParsedSseFrame {
        event_name,
        byte_len: raw_frame.len(),
        payload_sha256: Some(stable_hash_text(data.as_str())),
        data,
    }))
}

fn text_delta(value: &Value) -> Option<String> {
    value
        .pointer("/choices/0/delta/content")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            value
                .get("delta")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            value
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn usage_delta(value: &Value) -> Option<(u64, u64, Option<u64>)> {
    let usage = value.get("usage").or_else(|| value.pointer("/response/usage"))?;
    let prompt_tokens = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion_tokens = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = usage.get("total_tokens").and_then(Value::as_u64);
    Some((prompt_tokens, completion_tokens, total_tokens))
}

fn finish_reason(value: &Value, event_name: Option<&str>) -> Option<ProviderFinishReason> {
    if event_name.is_some_and(|event| event.ends_with(".completed") || event == "done") {
        return Some(ProviderFinishReason::Stop);
    }
    if value.get("type").and_then(Value::as_str).is_some_and(|kind| {
        kind.ends_with(".completed") || kind == "response.completed" || kind == "message_stop"
    }) {
        return Some(ProviderFinishReason::Stop);
    }
    value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .map(|reason| ProviderFinishReason::from_openai(Some(reason)))
}

fn frame_has_usage_json(data: &str) -> bool {
    serde_json::from_str::<Value>(data).ok().and_then(|value| usage_delta(&value)).is_some()
}

fn push_sse_completed(
    provider_id: &str,
    model_id: &str,
    finish_reason: ProviderFinishReason,
    report: &mut ProviderSseNormalizationReport,
) {
    let raw_provider_refs = ProviderRawProviderRefs {
        provider_model_id: Some(model_id.to_owned()),
        provider_trace_ref: Some(provider_id.to_owned()),
        ..ProviderRawProviderRefs::default()
    };
    report.events.push(ProviderStreamEvent::Completed { finish_reason, raw_provider_refs });
    report.terminal = true;
}

#[allow(clippy::too_many_arguments)]
fn fail_sse_report(
    report: &mut ProviderSseNormalizationReport,
    provider_id: &str,
    model_id: &str,
    reason_code: &str,
    class: ProviderFailureClass,
    action: ProviderFailureAction,
    frame_index: Option<usize>,
    byte_len: usize,
    payload_sha256: Option<String>,
) {
    let classification = provider_failure_classification(
        class,
        action,
        None,
        format!("{provider_id}:{model_id}:{reason_code}"),
    );
    let error = ProviderError::RequestFailed {
        message: reason_code.to_owned(),
        retryable: action == ProviderFailureAction::Retry,
        retry_count: 0,
        classification,
    };
    let envelope = ProviderErrorEnvelope::from_error(&error);
    report.recovery_decision = Some(envelope.recovery_decision.clone());
    report.events.push(ProviderStreamEvent::Failed { error: envelope });
    report.audit_events.push(sse_audit_event(
        reason_code,
        ProviderSseAuditSeverity::Failed,
        frame_index,
        byte_len,
        payload_sha256,
    ));
    report.terminal = true;
}

fn sse_audit_event(
    reason_code: &str,
    severity: ProviderSseAuditSeverity,
    frame_index: Option<usize>,
    byte_len: usize,
    payload_sha256: Option<String>,
) -> ProviderSseAuditEvent {
    ProviderSseAuditEvent {
        schema_version: PROVIDER_SSE_NORMALIZER_SCHEMA_VERSION,
        event_type: PROVIDER_SSE_NORMALIZER_AUDIT_EVENT.to_owned(),
        reason_code: reason_code.to_owned(),
        severity,
        frame_index,
        byte_len,
        payload_sha256,
    }
}

fn stable_hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
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
#[must_use]
pub fn provider_output_from_text_and_tools(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderRecoveryDecisionKind;

    #[test]
    fn sse_normalizer_marks_malformed_chunk_recoverable() {
        let report = normalize_provider_sse_stream(
            r#"data: {"id":"resp_01","choices":[{"delta":{"content":"hello"}"#,
            "openai-compatible",
            "gpt-test",
        );

        assert!(report.terminal);
        assert!(matches!(report.events.last(), Some(ProviderStreamEvent::Failed { .. })));
        assert_eq!(
            report.recovery_decision.as_ref().map(|decision| decision.decision),
            Some(ProviderRecoveryDecisionKind::RetrySameProvider)
        );
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.malformed_chunk"));
    }

    #[test]
    fn sse_normalizer_recovers_duplicate_delta_and_late_usage() {
        let input = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":9,\"total_tokens\":18}}\n\n",
        );

        let report = normalize_provider_sse_stream(input, "openai-compatible", "gpt-test");
        let deltas = report
            .events
            .iter()
            .filter(|event| matches!(event, ProviderStreamEvent::Delta { .. }))
            .count();

        assert_eq!(deltas, 1);
        assert!(matches!(report.events.last(), Some(ProviderStreamEvent::Completed { .. })));
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.duplicate_delta"));
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.late_usage"));
    }

    #[test]
    fn sse_normalizer_fails_closed_on_missing_final_event() {
        let report = normalize_provider_sse_stream(
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
            "openai-compatible",
            "gpt-test",
        );

        assert!(matches!(report.events.last(), Some(ProviderStreamEvent::Failed { .. })));
        assert_eq!(
            report.recovery_decision.as_ref().map(|decision| decision.decision),
            Some(ProviderRecoveryDecisionKind::RetrySameProvider)
        );
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.missing_final_event"));
    }

    #[test]
    fn sse_normalizer_fails_idle_timeout_without_hanging() {
        let report = normalize_provider_sse_stream_with_idle_timeout(
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
            "openai-compatible",
            "gpt-test",
            Some(31_000),
            30_000,
        );

        assert!(matches!(report.events.last(), Some(ProviderStreamEvent::Failed { .. })));
        assert_eq!(
            report.recovery_decision.as_ref().map(|decision| decision.decision),
            Some(ProviderRecoveryDecisionKind::RetrySameProvider)
        );
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.idle_timeout"));
    }
}
