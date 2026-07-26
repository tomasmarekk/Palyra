//! Provider stream event model and accumulation.
//!
//! [`ProviderStreamEvent`] is the legacy incremental event vocabulary
//! (started/delta/tool/usage/completed/failed/cancelled). The finer-grained
//! [`ProviderCanonicalEvent`] stream keeps provider chunk shapes out of tool
//! assembly and recovery logic.
//! [`ProviderStreamAccumulator`] folds those events into one size-bounded
//! [`ProviderTurnOutput`], recording a spill reference when text exceeds the
//! inline buffer cap. Non-streaming HTTP responses are funneled through the
//! same accumulator (see [`provider_output_from_text_and_tools`]) so both
//! paths share identical truncation and tool-call semantics.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::errors::provider_failure_classification;
use crate::{
    append_provider_text_with_hard_limit, ProviderError, ProviderErrorEnvelope, ProviderEvent,
    ProviderFailureAction, ProviderFailureClass, ProviderFinishReason, ProviderOutputContentPart,
    ProviderRawProviderRefs, ProviderRecoveryDecision, ProviderRecoveryDecisionKind,
    ProviderTurnOutput, ProviderUsage, MAX_PROVIDER_TURN_TEXT_BYTES,
};

const DEFAULT_PROVIDER_STREAM_BUFFER_CAP_BYTES: usize = 256 * 1024;
const PROVIDER_SSE_NORMALIZER_SCHEMA_VERSION: u16 = 1;
const PROVIDER_CANONICAL_STREAM_SCHEMA_VERSION: u16 = 1;
const NORMALIZED_PROVIDER_EVENT_SCHEMA_VERSION: u16 = 2;
const PROVIDER_TERMINAL_VALIDATION_SCHEMA_VERSION: u16 = 1;
const DEFAULT_PROVIDER_STREAM_IDLE_TIMEOUT_MS: u64 = 30_000;
/// Audit event emitted for provider SSE stream normalization decisions.
pub const PROVIDER_SSE_NORMALIZER_AUDIT_EVENT: &str = "provider.stream.sse.normalized";
/// Audit event emitted for canonical stream sequence validation.
pub const PROVIDER_CANONICAL_STREAM_AUDIT_EVENT: &str = "provider.stream.canonical";
/// Audit event emitted after the normalized terminal boundary is validated.
pub const PROVIDER_TERMINAL_VALIDATION_AUDIT_EVENT: &str = "provider.stream.terminal_validation";

/// Incremental event emitted while a provider turn is in flight.
///
/// `Completed`, `Failed`, and `Cancelled` are terminal: the accumulator
/// ignores every event that arrives after one of them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderStreamEvent {
    Started {
        provider_id: String,
        model_id: String,
    },
    Delta {
        text: String,
    },
    ToolDelta {
        proposal_id: String,
        tool_name: String,
        input_json: Value,
    },
    UsageDelta {
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_read_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_write_tokens: Option<u64>,
    },
    Completed {
        finish_reason: ProviderFinishReason,
        raw_provider_refs: ProviderRawProviderRefs,
    },
    Failed {
        error: ProviderErrorEnvelope,
    },
    Cancelled {
        reason: String,
    },
}

/// Provider-neutral incremental stream event consumed by tool-call assembly.
///
/// The enum is intentionally more granular than [`ProviderStreamEvent`]:
/// provider adapters can emit fragmented tool names and arguments without
/// leaking the provider's raw SSE frame shape into later recovery logic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderCanonicalEvent {
    MessageStart {
        provider_id: String,
        model_id: String,
        provider_call_id: String,
    },
    ContentDelta {
        text: String,
    },
    /// Internal reasoning is retained as hash-only metadata, never raw text.
    ReasoningDelta {
        byte_len: usize,
        payload_sha256: String,
    },
    ToolCallStart {
        index: u32,
        provider_call_id: Option<String>,
    },
    ToolCallNameDelta {
        index: u32,
        name_delta: String,
    },
    ToolCallArgumentsDelta {
        index: u32,
        arguments_delta: String,
    },
    ToolCallEnd {
        index: u32,
    },
    UsageUpdate {
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_read_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_write_tokens: Option<u64>,
    },
    FinishReason {
        finish_reason: ProviderFinishReason,
    },
    ProviderWarning {
        reason_code: String,
        message: String,
    },
    StreamError {
        reason_code: String,
        recoverable: bool,
    },
}

/// Provider-neutral event vocabulary accepted by RuntimeKernelV2.
///
/// Provider sequence identifiers are consumed inside adapters for
/// deduplication. The sequence here is host-issued and monotonic, so runtime
/// consumers never need to interpret provider-specific frame identifiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizedProviderEventV2 {
    TextDelta {
        sequence: u64,
        text: String,
    },
    ReasoningDelta {
        sequence: u64,
        byte_len: usize,
        payload_sha256: String,
    },
    ToolCallDelta {
        sequence: u64,
        index: u32,
        delta_kind: NormalizedProviderToolDeltaKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delta: Option<String>,
    },
    ToolCallComplete {
        sequence: u64,
        index: u32,
    },
    Usage {
        sequence: u64,
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_read_tokens: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_write_tokens: Option<u64>,
        late: bool,
    },
    ProviderWarning {
        sequence: u64,
        reason_code: String,
        message: String,
    },
    Terminal {
        sequence: u64,
        status: NormalizedProviderTerminalStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<ProviderFinishReason>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason_code: Option<String>,
    },
}

impl NormalizedProviderEventV2 {
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        match self {
            Self::TextDelta { sequence, .. }
            | Self::ReasoningDelta { sequence, .. }
            | Self::ToolCallDelta { sequence, .. }
            | Self::ToolCallComplete { sequence, .. }
            | Self::Usage { sequence, .. }
            | Self::ProviderWarning { sequence, .. }
            | Self::Terminal { sequence, .. } => *sequence,
        }
    }
}

/// Fragment type carried by a normalized tool-call delta.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedProviderToolDeltaKind {
    Start,
    Name,
    Arguments,
}

/// Terminal meaning after provider-specific framing has been removed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedProviderTerminalStatus {
    Complete,
    RecoverableError,
    TerminalError,
    Cancelled,
}

/// High-level terminal decision made before events enter the runtime kernel.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTerminalDisposition {
    Complete,
    Recoverable,
    TerminallyInvalid,
}

/// Placement of provider usage relative to the terminal boundary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUsageTiming {
    BeforeTerminal,
    Missing,
    LateOnly,
    BeforeAndLate,
}

/// Redacted terminal validation result consumed by orchestration and traces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderTerminalValidationOutcome {
    pub schema_version: u16,
    pub event_type: String,
    pub disposition: ProviderTerminalDisposition,
    pub reason_code: String,
    pub terminal_count: usize,
    pub text_delta_count: usize,
    pub reasoning_delta_count: usize,
    pub tool_call_count: usize,
    pub repaired_tool_call_count: usize,
    pub invalid_tool_call_count: usize,
    pub usage_timing: ProviderUsageTiming,
    pub diagnostic_reason_codes: Vec<String>,
}

/// Complete normalized provider boundary output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NormalizedProviderStreamV2 {
    pub schema_version: u16,
    pub events: Vec<NormalizedProviderEventV2>,
    pub terminal_validation: ProviderTerminalValidationOutcome,
}

/// Explicit opt-in for a hash-only provider debug artifact.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProviderRawDebugArtifactPolicy {
    pub explicitly_enabled: bool,
    pub max_input_bytes: usize,
}

/// Redacted provider debug artifact metadata; raw bytes are never retained.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderRawDebugArtifact {
    pub schema_version: u16,
    pub redaction_level: String,
    pub observed_bytes: usize,
    pub hashed_bytes: usize,
    pub payload_sha256: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderUsageDelta {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
}

/// Hash-only validation diagnostic for a canonical stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCanonicalStreamDiagnostic {
    pub schema_version: u16,
    pub event_type: String,
    pub reason_code: String,
    pub severity: ProviderSseAuditSeverity,
    pub event_index: usize,
    pub provider_call_id: Option<String>,
}

/// Validation report for canonical event ordering and terminal closure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCanonicalStreamReport {
    pub schema_version: u16,
    pub provider_call_id: Option<String>,
    pub valid: bool,
    pub terminal: bool,
    pub tool_call_count: usize,
    pub diagnostics: Vec<ProviderCanonicalStreamDiagnostic>,
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
    pub canonical_events: Vec<ProviderCanonicalEvent>,
    pub normalized_stream_v2: NormalizedProviderStreamV2,
    pub audit_events: Vec<ProviderSseAuditEvent>,
    pub recovery_decision: Option<ProviderRecoveryDecision>,
    pub terminal: bool,
}

#[derive(Debug, Clone)]
struct ParsedSseFrame {
    event_name: Option<String>,
    event_id: Option<String>,
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
        canonical_events: vec![ProviderCanonicalEvent::MessageStart {
            provider_id: provider_id.to_owned(),
            model_id: model_id.to_owned(),
            provider_call_id: provider_call_id(provider_id, model_id),
        }],
        normalized_stream_v2: normalized_provider_stream_from_canonical_events_v2(&[]),
        audit_events: Vec::new(),
        recovery_decision: None,
        terminal: false,
    };
    let mut provider_event_ids = BTreeMap::<String, String>::new();
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
                refresh_sse_normalized_stream(&mut report);
                return report;
            }
        };

        if let (Some(event_id), Some(payload_sha256)) =
            (frame.event_id.as_ref(), frame.payload_sha256.as_ref())
        {
            if let Some(previous_sha256) = provider_event_ids.get(event_id) {
                if previous_sha256 == payload_sha256 {
                    report.audit_events.push(sse_audit_event(
                        "provider.stream.duplicate_sequence",
                        ProviderSseAuditSeverity::Recovered,
                        Some(frame_index),
                        frame.byte_len,
                        Some(payload_sha256.clone()),
                    ));
                    continue;
                }
                fail_sse_report(
                    &mut report,
                    provider_id,
                    model_id,
                    "provider.stream.sequence_payload_conflict",
                    ProviderFailureClass::MalformedStream,
                    ProviderFailureAction::FailClosedNoRetry,
                    Some(frame_index),
                    frame.byte_len,
                    Some(payload_sha256.clone()),
                );
                refresh_sse_normalized_stream(&mut report);
                return report;
            }
            provider_event_ids.insert(event_id.clone(), payload_sha256.clone());
        }

        if report.terminal {
            if let Some(usage) = serde_json::from_str::<Value>(frame.data.as_str())
                .ok()
                .and_then(|value| usage_delta(&value))
            {
                report.audit_events.push(sse_audit_event(
                    "provider.stream.late_usage",
                    ProviderSseAuditSeverity::Recovered,
                    Some(frame_index),
                    frame.byte_len,
                    frame.payload_sha256,
                ));
                report.canonical_events.push(ProviderCanonicalEvent::UsageUpdate {
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                    cache_read_tokens: usage.cache_read_tokens,
                    cache_write_tokens: usage.cache_write_tokens,
                });
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
                refresh_sse_normalized_stream(&mut report);
                return report;
            }
        };

        if let Some(usage) = usage_delta(&value) {
            usage_seen = true;
            report.events.push(ProviderStreamEvent::UsageDelta {
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
            });
            report.canonical_events.push(ProviderCanonicalEvent::UsageUpdate {
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
                cache_read_tokens: usage.cache_read_tokens,
                cache_write_tokens: usage.cache_write_tokens,
            });
        }

        if let Some(delta) = text_delta(&value) {
            report.events.push(ProviderStreamEvent::Delta { text: delta.clone() });
            report.canonical_events.push(ProviderCanonicalEvent::ContentDelta { text: delta });
        }

        if let Some(reasoning) = reasoning_delta(&value) {
            report.canonical_events.push(ProviderCanonicalEvent::ReasoningDelta {
                byte_len: reasoning.len(),
                payload_sha256: stable_hash_text(reasoning.as_str()),
            });
        }

        report
            .canonical_events
            .extend(tool_call_canonical_events(&value, frame.event_name.as_deref()));

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
            close_open_tool_calls_before_terminal(&mut report.canonical_events);
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

    refresh_sse_normalized_stream(&mut report);
    report
}

fn parse_sse_frame(raw_frame: &str, frame_index: usize) -> Result<Option<ParsedSseFrame>, String> {
    let mut event_name = None;
    let mut event_id = None;
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
            "id" => event_id = (!value.is_empty()).then(|| value.to_owned()),
            "retry" => {}
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
        event_id,
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

fn reasoning_delta(value: &Value) -> Option<String> {
    value
        .pointer("/choices/0/delta/reasoning_content")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/choices/0/delta/reasoning").and_then(Value::as_str))
        .or_else(|| value.pointer("/delta/thinking").and_then(Value::as_str))
        .or_else(|| value.get("reasoning_delta").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn tool_call_canonical_events(
    value: &Value,
    event_name: Option<&str>,
) -> Vec<ProviderCanonicalEvent> {
    let mut events = Vec::new();
    if let Some(tool_calls) = value.pointer("/choices/0/delta/tool_calls").and_then(Value::as_array)
    {
        for (fallback_index, tool_call) in tool_calls.iter().enumerate() {
            let index = tool_call
                .get("index")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or_else(|| u32::try_from(fallback_index).unwrap_or(u32::MAX));
            if let Some(id) = tool_call.get("id").and_then(Value::as_str) {
                events.push(ProviderCanonicalEvent::ToolCallStart {
                    index,
                    provider_call_id: Some(id.to_owned()),
                });
            }
            if let Some(name_delta) = tool_call.pointer("/function/name").and_then(Value::as_str) {
                if !name_delta.is_empty() {
                    events.push(ProviderCanonicalEvent::ToolCallNameDelta {
                        index,
                        name_delta: name_delta.to_owned(),
                    });
                }
            }
            if let Some(arguments_delta) =
                tool_call.pointer("/function/arguments").and_then(Value::as_str)
            {
                if !arguments_delta.is_empty() {
                    events.push(ProviderCanonicalEvent::ToolCallArgumentsDelta {
                        index,
                        arguments_delta: arguments_delta.to_owned(),
                    });
                }
            }
        }
    }

    let event_type = value.get("type").and_then(Value::as_str).or(event_name);
    if matches!(event_type, Some("response.output_item.added" | "content_block_start")) {
        if let Some((index, id, name)) = response_tool_start(value) {
            events.push(ProviderCanonicalEvent::ToolCallStart { index, provider_call_id: id });
            if let Some(name) = name {
                events.push(ProviderCanonicalEvent::ToolCallNameDelta { index, name_delta: name });
            }
        }
    }
    if matches!(event_type, Some("response.function_call_arguments.delta" | "content_block_delta"))
    {
        if let Some((index, delta)) = response_tool_arguments_delta(value) {
            events.push(ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index,
                arguments_delta: delta,
            });
        }
    }
    if matches!(
        event_type,
        Some(
            "response.function_call_arguments.done"
                | "response.output_item.done"
                | "content_block_stop"
        )
    ) {
        let index = response_tool_index(value).unwrap_or(0);
        events.push(ProviderCanonicalEvent::ToolCallEnd { index });
    }
    events
}

fn response_tool_start(value: &Value) -> Option<(u32, Option<String>, Option<String>)> {
    let item = value.get("item").or_else(|| value.get("content_block")).unwrap_or(value);
    let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
    if !matches!(kind, "function_call" | "tool_use") {
        return None;
    }
    let index = response_tool_index(value).unwrap_or(0);
    let id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned);
    let name = item
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned);
    Some((index, id, name))
}

fn response_tool_arguments_delta(value: &Value) -> Option<(u32, String)> {
    let delta = value
        .get("delta")
        .and_then(Value::as_str)
        .or_else(|| value.get("partial_json").and_then(Value::as_str))
        .or_else(|| value.pointer("/delta/partial_json").and_then(Value::as_str))
        .filter(|delta| !delta.is_empty())?;
    Some((response_tool_index(value).unwrap_or(0), delta.to_owned()))
}

fn response_tool_index(value: &Value) -> Option<u32> {
    value
        .get("output_index")
        .or_else(|| value.get("item_index"))
        .or_else(|| value.get("index"))
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn close_open_tool_calls_before_terminal(events: &mut Vec<ProviderCanonicalEvent>) {
    let mut open_tools = BTreeSet::<u32>::new();
    for event in events.iter() {
        match event {
            ProviderCanonicalEvent::ToolCallStart { index, .. } => {
                open_tools.insert(*index);
            }
            ProviderCanonicalEvent::ToolCallEnd { index } => {
                open_tools.remove(index);
            }
            ProviderCanonicalEvent::MessageStart { .. }
            | ProviderCanonicalEvent::ContentDelta { .. }
            | ProviderCanonicalEvent::ReasoningDelta { .. }
            | ProviderCanonicalEvent::ToolCallNameDelta { .. }
            | ProviderCanonicalEvent::ToolCallArgumentsDelta { .. }
            | ProviderCanonicalEvent::UsageUpdate { .. }
            | ProviderCanonicalEvent::FinishReason { .. }
            | ProviderCanonicalEvent::ProviderWarning { .. }
            | ProviderCanonicalEvent::StreamError { .. } => {}
        }
    }
    events
        .extend(open_tools.into_iter().map(|index| ProviderCanonicalEvent::ToolCallEnd { index }));
}

fn usage_delta(value: &Value) -> Option<ProviderUsageDelta> {
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
    let cache_read_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
        .or_else(|| usage.get("cache_read_input_tokens"))
        .and_then(Value::as_u64);
    let cache_write_tokens = usage.get("cache_creation_input_tokens").and_then(Value::as_u64);
    Some(ProviderUsageDelta {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        cache_read_tokens,
        cache_write_tokens,
    })
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
    report.canonical_events.push(ProviderCanonicalEvent::FinishReason { finish_reason });
    report.terminal = true;
}

/// Projects legacy provider stream events into canonical events.
#[must_use]
pub fn canonical_events_from_provider_stream_events(
    events: &[ProviderStreamEvent],
    provider_id: &str,
    model_id: &str,
) -> Vec<ProviderCanonicalEvent> {
    let mut canonical_events = vec![ProviderCanonicalEvent::MessageStart {
        provider_id: provider_id.to_owned(),
        model_id: model_id.to_owned(),
        provider_call_id: provider_call_id(provider_id, model_id),
    }];
    for event in events {
        match event {
            ProviderStreamEvent::Started { provider_id, model_id } => {
                canonical_events.push(ProviderCanonicalEvent::MessageStart {
                    provider_id: provider_id.clone(),
                    model_id: model_id.clone(),
                    provider_call_id: provider_call_id(provider_id, model_id),
                });
            }
            ProviderStreamEvent::Delta { text } => {
                canonical_events.push(ProviderCanonicalEvent::ContentDelta { text: text.clone() });
            }
            ProviderStreamEvent::ToolDelta { proposal_id, tool_name, input_json } => {
                let index = u32::try_from(canonical_events.len()).unwrap_or(u32::MAX);
                canonical_events.push(ProviderCanonicalEvent::ToolCallStart {
                    index,
                    provider_call_id: Some(proposal_id.clone()),
                });
                canonical_events.push(ProviderCanonicalEvent::ToolCallNameDelta {
                    index,
                    name_delta: tool_name.clone(),
                });
                canonical_events.push(ProviderCanonicalEvent::ToolCallArgumentsDelta {
                    index,
                    arguments_delta: serde_json::to_string(input_json)
                        .unwrap_or_else(|_| "{}".to_owned()),
                });
                canonical_events.push(ProviderCanonicalEvent::ToolCallEnd { index });
            }
            ProviderStreamEvent::UsageDelta {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => canonical_events.push(ProviderCanonicalEvent::UsageUpdate {
                prompt_tokens: *prompt_tokens,
                completion_tokens: *completion_tokens,
                total_tokens: *total_tokens,
                cache_read_tokens: *cache_read_tokens,
                cache_write_tokens: *cache_write_tokens,
            }),
            ProviderStreamEvent::Completed { finish_reason, .. } => {
                canonical_events
                    .push(ProviderCanonicalEvent::FinishReason { finish_reason: *finish_reason });
            }
            ProviderStreamEvent::Failed { error } => {
                canonical_events.push(ProviderCanonicalEvent::StreamError {
                    reason_code: error.recovery_decision.reason_code.clone(),
                    recoverable: provider_recovery_decision_is_recoverable(
                        error.recovery_decision.decision,
                    ),
                });
            }
            ProviderStreamEvent::Cancelled { reason } => {
                canonical_events.push(ProviderCanonicalEvent::StreamError {
                    reason_code: format!("provider.stream.cancelled.{}", stable_hash_text(reason)),
                    recoverable: false,
                });
            }
        }
    }
    canonical_events
}

/// Validates canonical stream ordering before tool-call assembly.
#[must_use]
pub fn validate_canonical_provider_stream(
    events: &[ProviderCanonicalEvent],
) -> ProviderCanonicalStreamReport {
    let mut open_tools = BTreeSet::<u32>::new();
    let mut seen_tools = BTreeSet::<u32>::new();
    let mut diagnostics = Vec::new();
    let mut provider_call_ids = BTreeMap::<String, usize>::new();
    let mut terminal = false;

    for (event_index, event) in events.iter().enumerate() {
        match event {
            ProviderCanonicalEvent::MessageStart { provider_call_id, .. } => {
                *provider_call_ids.entry(provider_call_id.clone()).or_default() += 1;
            }
            ProviderCanonicalEvent::ToolCallStart { index, provider_call_id } => {
                if !seen_tools.insert(*index) {
                    diagnostics.push(canonical_stream_diagnostic(
                        "provider.stream.tool_call.duplicated_start",
                        ProviderSseAuditSeverity::Failed,
                        event_index,
                        provider_call_id.clone(),
                    ));
                }
                open_tools.insert(*index);
            }
            ProviderCanonicalEvent::ToolCallNameDelta { index, .. }
            | ProviderCanonicalEvent::ToolCallArgumentsDelta { index, .. } => {
                if !open_tools.contains(index) {
                    diagnostics.push(canonical_stream_diagnostic(
                        "provider.stream.tool_call.delta_without_start",
                        ProviderSseAuditSeverity::Failed,
                        event_index,
                        None,
                    ));
                }
            }
            ProviderCanonicalEvent::ToolCallEnd { index } => {
                if !open_tools.remove(index) {
                    diagnostics.push(canonical_stream_diagnostic(
                        "provider.stream.tool_call.end_without_start",
                        ProviderSseAuditSeverity::Failed,
                        event_index,
                        None,
                    ));
                }
            }
            ProviderCanonicalEvent::FinishReason { .. }
            | ProviderCanonicalEvent::StreamError { .. } => {
                terminal = true;
                if !open_tools.is_empty() {
                    diagnostics.push(canonical_stream_diagnostic(
                        "provider.stream.tool_call.incomplete_at_terminal",
                        ProviderSseAuditSeverity::Failed,
                        event_index,
                        None,
                    ));
                }
            }
            ProviderCanonicalEvent::ContentDelta { .. }
            | ProviderCanonicalEvent::ReasoningDelta { .. }
            | ProviderCanonicalEvent::UsageUpdate { .. }
            | ProviderCanonicalEvent::ProviderWarning { .. } => {}
        }
    }

    if !terminal {
        diagnostics.push(canonical_stream_diagnostic(
            "provider.stream.missing_terminal_event",
            ProviderSseAuditSeverity::Failed,
            events.len(),
            None,
        ));
    }

    let provider_call_id = provider_call_ids.keys().next().cloned();
    ProviderCanonicalStreamReport {
        schema_version: PROVIDER_CANONICAL_STREAM_SCHEMA_VERSION,
        provider_call_id,
        valid: diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != ProviderSseAuditSeverity::Failed),
        terminal,
        tool_call_count: seen_tools.len(),
        diagnostics,
    }
}

/// Converts adapter canonical events into the only event model accepted by
/// RuntimeKernelV2.
#[must_use]
pub fn normalized_provider_events_from_canonical_v2(
    events: &[ProviderCanonicalEvent],
) -> Vec<NormalizedProviderEventV2> {
    let mut normalized = Vec::with_capacity(events.len());
    let mut terminal_seen = false;
    for event in events {
        let sequence = u64::try_from(normalized.len()).unwrap_or(u64::MAX);
        let event = match event {
            ProviderCanonicalEvent::MessageStart { .. } => continue,
            ProviderCanonicalEvent::ContentDelta { text } => {
                NormalizedProviderEventV2::TextDelta { sequence, text: text.clone() }
            }
            ProviderCanonicalEvent::ReasoningDelta { byte_len, payload_sha256 } => {
                NormalizedProviderEventV2::ReasoningDelta {
                    sequence,
                    byte_len: *byte_len,
                    payload_sha256: payload_sha256.clone(),
                }
            }
            ProviderCanonicalEvent::ToolCallStart { index, provider_call_id } => {
                NormalizedProviderEventV2::ToolCallDelta {
                    sequence,
                    index: *index,
                    delta_kind: NormalizedProviderToolDeltaKind::Start,
                    provider_call_id: provider_call_id.clone(),
                    delta: None,
                }
            }
            ProviderCanonicalEvent::ToolCallNameDelta { index, name_delta } => {
                NormalizedProviderEventV2::ToolCallDelta {
                    sequence,
                    index: *index,
                    delta_kind: NormalizedProviderToolDeltaKind::Name,
                    provider_call_id: None,
                    delta: Some(name_delta.clone()),
                }
            }
            ProviderCanonicalEvent::ToolCallArgumentsDelta { index, arguments_delta } => {
                NormalizedProviderEventV2::ToolCallDelta {
                    sequence,
                    index: *index,
                    delta_kind: NormalizedProviderToolDeltaKind::Arguments,
                    provider_call_id: None,
                    delta: Some(arguments_delta.clone()),
                }
            }
            ProviderCanonicalEvent::ToolCallEnd { index } => {
                NormalizedProviderEventV2::ToolCallComplete { sequence, index: *index }
            }
            ProviderCanonicalEvent::UsageUpdate {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => NormalizedProviderEventV2::Usage {
                sequence,
                prompt_tokens: *prompt_tokens,
                completion_tokens: *completion_tokens,
                total_tokens: *total_tokens,
                cache_read_tokens: *cache_read_tokens,
                cache_write_tokens: *cache_write_tokens,
                late: terminal_seen,
            },
            ProviderCanonicalEvent::FinishReason { finish_reason } => {
                terminal_seen = true;
                let status = match finish_reason {
                    ProviderFinishReason::Cancelled => NormalizedProviderTerminalStatus::Cancelled,
                    ProviderFinishReason::Error => NormalizedProviderTerminalStatus::TerminalError,
                    ProviderFinishReason::Stop
                    | ProviderFinishReason::Length
                    | ProviderFinishReason::ToolCalls
                    | ProviderFinishReason::ContentFilter
                    | ProviderFinishReason::Unknown => NormalizedProviderTerminalStatus::Complete,
                };
                NormalizedProviderEventV2::Terminal {
                    sequence,
                    status,
                    finish_reason: Some(*finish_reason),
                    reason_code: None,
                }
            }
            ProviderCanonicalEvent::ProviderWarning { reason_code, message } => {
                NormalizedProviderEventV2::ProviderWarning {
                    sequence,
                    reason_code: reason_code.clone(),
                    message: message.clone(),
                }
            }
            ProviderCanonicalEvent::StreamError { reason_code, recoverable } => {
                terminal_seen = true;
                NormalizedProviderEventV2::Terminal {
                    sequence,
                    status: if *recoverable {
                        NormalizedProviderTerminalStatus::RecoverableError
                    } else {
                        NormalizedProviderTerminalStatus::TerminalError
                    },
                    finish_reason: None,
                    reason_code: Some(reason_code.clone()),
                }
            }
        };
        normalized.push(event);
    }
    normalized
}

/// Reconstructs the internal canonical fragments needed by the tool-call
/// assembler. Provider adapters remain the only code that interprets raw
/// provider frames.
#[must_use]
pub fn canonical_events_from_normalized_provider_events_v2(
    events: &[NormalizedProviderEventV2],
) -> Vec<ProviderCanonicalEvent> {
    let mut canonical = Vec::with_capacity(events.len());
    for event in events {
        match event {
            NormalizedProviderEventV2::TextDelta { text, .. } => {
                canonical.push(ProviderCanonicalEvent::ContentDelta { text: text.clone() });
            }
            NormalizedProviderEventV2::ReasoningDelta { byte_len, payload_sha256, .. } => canonical
                .push(ProviderCanonicalEvent::ReasoningDelta {
                    byte_len: *byte_len,
                    payload_sha256: payload_sha256.clone(),
                }),
            NormalizedProviderEventV2::ToolCallDelta {
                index,
                delta_kind,
                provider_call_id,
                delta,
                ..
            } => match delta_kind {
                NormalizedProviderToolDeltaKind::Start => {
                    canonical.push(ProviderCanonicalEvent::ToolCallStart {
                        index: *index,
                        provider_call_id: provider_call_id.clone(),
                    });
                }
                NormalizedProviderToolDeltaKind::Name => {
                    canonical.push(ProviderCanonicalEvent::ToolCallNameDelta {
                        index: *index,
                        name_delta: delta.clone().unwrap_or_default(),
                    });
                }
                NormalizedProviderToolDeltaKind::Arguments => {
                    canonical.push(ProviderCanonicalEvent::ToolCallArgumentsDelta {
                        index: *index,
                        arguments_delta: delta.clone().unwrap_or_default(),
                    });
                }
            },
            NormalizedProviderEventV2::ToolCallComplete { index, .. } => {
                canonical.push(ProviderCanonicalEvent::ToolCallEnd { index: *index });
            }
            NormalizedProviderEventV2::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_read_tokens,
                cache_write_tokens,
                ..
            } => canonical.push(ProviderCanonicalEvent::UsageUpdate {
                prompt_tokens: *prompt_tokens,
                completion_tokens: *completion_tokens,
                total_tokens: *total_tokens,
                cache_read_tokens: *cache_read_tokens,
                cache_write_tokens: *cache_write_tokens,
            }),
            NormalizedProviderEventV2::ProviderWarning { reason_code, message, .. } => {
                canonical.push(ProviderCanonicalEvent::ProviderWarning {
                    reason_code: reason_code.clone(),
                    message: message.clone(),
                });
            }
            NormalizedProviderEventV2::Terminal { status, finish_reason, reason_code, .. } => {
                match status {
                    NormalizedProviderTerminalStatus::Complete => {
                        canonical.push(ProviderCanonicalEvent::FinishReason {
                            finish_reason: finish_reason.unwrap_or(ProviderFinishReason::Unknown),
                        });
                    }
                    NormalizedProviderTerminalStatus::RecoverableError
                    | NormalizedProviderTerminalStatus::TerminalError
                    | NormalizedProviderTerminalStatus::Cancelled => {
                        canonical.push(ProviderCanonicalEvent::StreamError {
                            reason_code: reason_code
                                .clone()
                                .unwrap_or_else(|| "provider.stream.terminal_error".to_owned()),
                            recoverable: *status
                                == NormalizedProviderTerminalStatus::RecoverableError,
                        });
                    }
                }
            }
        }
    }
    canonical
}

/// Creates a normalized stream from a completed adapter output.
#[must_use]
pub fn normalized_provider_stream_from_output_v2(
    output: &ProviderTurnOutput,
) -> NormalizedProviderStreamV2 {
    let mut canonical = Vec::new();
    let mut next_tool_index = 0_u32;
    for part in &output.content_parts {
        match part {
            ProviderOutputContentPart::Text { text } if !text.is_empty() => {
                canonical.push(ProviderCanonicalEvent::ContentDelta { text: text.clone() });
            }
            ProviderOutputContentPart::Text { .. } => {}
            ProviderOutputContentPart::ToolCall { proposal_id, tool_name, input_json } => {
                let index = next_tool_index;
                next_tool_index = next_tool_index.saturating_add(1);
                canonical.push(ProviderCanonicalEvent::ToolCallStart {
                    index,
                    provider_call_id: Some(proposal_id.clone()),
                });
                canonical.push(ProviderCanonicalEvent::ToolCallNameDelta {
                    index,
                    name_delta: tool_name.clone(),
                });
                canonical.push(ProviderCanonicalEvent::ToolCallArgumentsDelta {
                    index,
                    arguments_delta: serde_json::to_string(input_json)
                        .unwrap_or_else(|_| "{}".to_owned()),
                });
                canonical.push(ProviderCanonicalEvent::ToolCallEnd { index });
            }
        }
    }
    canonical.push(ProviderCanonicalEvent::UsageUpdate {
        prompt_tokens: output.usage.prompt_tokens,
        completion_tokens: output.usage.completion_tokens,
        total_tokens: Some(output.usage.total_tokens),
        cache_read_tokens: output.usage.cache_read_tokens,
        cache_write_tokens: output.usage.cache_write_tokens,
    });
    canonical.push(ProviderCanonicalEvent::FinishReason { finish_reason: output.finish_reason });
    normalized_provider_stream_from_canonical_events_v2(canonical.as_slice())
}

/// Normalizes and validates one canonical adapter stream.
#[must_use]
pub fn normalized_provider_stream_from_canonical_events_v2(
    events: &[ProviderCanonicalEvent],
) -> NormalizedProviderStreamV2 {
    let events = normalized_provider_events_from_canonical_v2(events);
    let terminal_validation = validate_normalized_provider_terminal_v2(events.as_slice());
    NormalizedProviderStreamV2 {
        schema_version: NORMALIZED_PROVIDER_EVENT_SCHEMA_VERSION,
        events,
        terminal_validation,
    }
}

/// Validates terminal closure and classifies recoverable versus ambiguous
/// normalized streams before orchestration can act on them.
#[must_use]
pub fn validate_normalized_provider_terminal_v2(
    events: &[NormalizedProviderEventV2],
) -> ProviderTerminalValidationOutcome {
    let mut terminal_count = 0_usize;
    let mut terminal_status = None;
    let mut terminal_reason_code = None;
    let mut terminal_seen = false;
    let mut before_terminal_usage = false;
    let mut late_usage = false;
    let mut text_delta_count = 0_usize;
    let mut reasoning_delta_count = 0_usize;
    let mut open_tools = BTreeSet::<u32>::new();
    let mut diagnostics = Vec::<String>::new();
    let mut previous_sequence = None;

    for event in events {
        let sequence = event.sequence();
        if previous_sequence.is_some_and(|previous| sequence <= previous) {
            push_unique_reason(&mut diagnostics, "provider.stream.non_monotonic_host_sequence");
        }
        previous_sequence = Some(sequence);

        match event {
            NormalizedProviderEventV2::TextDelta { .. } => {
                text_delta_count = text_delta_count.saturating_add(1);
                if terminal_seen {
                    push_unique_reason(&mut diagnostics, "provider.stream.event_after_terminal");
                }
            }
            NormalizedProviderEventV2::ReasoningDelta { .. } => {
                reasoning_delta_count = reasoning_delta_count.saturating_add(1);
                if terminal_seen {
                    push_unique_reason(&mut diagnostics, "provider.stream.event_after_terminal");
                }
            }
            NormalizedProviderEventV2::ToolCallDelta { index, delta_kind, .. } => {
                if terminal_seen {
                    push_unique_reason(&mut diagnostics, "provider.stream.event_after_terminal");
                }
                match delta_kind {
                    NormalizedProviderToolDeltaKind::Start => {
                        if !open_tools.insert(*index) {
                            push_unique_reason(
                                &mut diagnostics,
                                "provider.stream.tool_call.duplicated_start",
                            );
                        }
                    }
                    NormalizedProviderToolDeltaKind::Name
                    | NormalizedProviderToolDeltaKind::Arguments => {
                        if !open_tools.contains(index) {
                            push_unique_reason(
                                &mut diagnostics,
                                "provider.stream.tool_call.delta_without_start",
                            );
                        }
                    }
                }
            }
            NormalizedProviderEventV2::ToolCallComplete { index, .. } => {
                if terminal_seen {
                    push_unique_reason(&mut diagnostics, "provider.stream.event_after_terminal");
                }
                if !open_tools.remove(index) {
                    push_unique_reason(
                        &mut diagnostics,
                        "provider.stream.tool_call.complete_without_start",
                    );
                }
            }
            NormalizedProviderEventV2::Usage { late, .. } => {
                if terminal_seen {
                    late_usage = true;
                    if !late {
                        push_unique_reason(
                            &mut diagnostics,
                            "provider.stream.late_usage_marker_missing",
                        );
                    }
                } else {
                    before_terminal_usage = true;
                    if *late {
                        push_unique_reason(
                            &mut diagnostics,
                            "provider.stream.early_usage_marked_late",
                        );
                    }
                }
            }
            NormalizedProviderEventV2::ProviderWarning { .. } => {
                if terminal_seen {
                    push_unique_reason(&mut diagnostics, "provider.stream.event_after_terminal");
                }
            }
            NormalizedProviderEventV2::Terminal { status, reason_code, .. } => {
                terminal_count = terminal_count.saturating_add(1);
                terminal_status.get_or_insert(*status);
                if terminal_reason_code.is_none() {
                    terminal_reason_code = reason_code.clone();
                }
                if terminal_seen {
                    push_unique_reason(
                        &mut diagnostics,
                        "provider.stream.multiple_terminal_events",
                    );
                }
                terminal_seen = true;
                if !open_tools.is_empty() {
                    push_unique_reason(
                        &mut diagnostics,
                        "provider.stream.tool_call.incomplete_at_terminal",
                    );
                }
            }
        }
    }

    if terminal_count == 0 {
        push_unique_reason(&mut diagnostics, "provider.stream.missing_terminal_event");
    }

    let canonical = canonical_events_from_normalized_provider_events_v2(events);
    let mut observed_tool_names = BTreeMap::<u32, String>::new();
    for event in events {
        if let NormalizedProviderEventV2::ToolCallDelta {
            index,
            delta_kind: NormalizedProviderToolDeltaKind::Name,
            delta: Some(delta),
            ..
        } = event
        {
            observed_tool_names.entry(*index).or_default().push_str(delta);
        }
    }
    let assembly_policy =
        crate::tool_call_assembler::ToolCallAssemblyPolicy::new(observed_tool_names.values());
    let assembly_report =
        crate::tool_call_assembler::assemble_canonical_tool_calls(&canonical, &assembly_policy);
    let repaired_tool_call_count = assembly_report
        .tool_calls
        .iter()
        .filter(|tool_call| {
            tool_call.status == crate::tool_call_assembler::AssembledToolCallStatus::ExecutionReady
                && !tool_call.repair_steps.is_empty()
        })
        .count();
    let invalid_tool_call_count = assembly_report
        .tool_calls
        .iter()
        .filter(|tool_call| {
            tool_call.status != crate::tool_call_assembler::AssembledToolCallStatus::ExecutionReady
        })
        .count();
    let needs_self_correction = assembly_report.tool_calls.iter().any(|tool_call| {
        tool_call.status
            == crate::tool_call_assembler::AssembledToolCallStatus::NeedsModelSelfCorrection
    });
    let ambiguous_tool_call = assembly_report.tool_calls.iter().any(|tool_call| {
        tool_call.status == crate::tool_call_assembler::AssembledToolCallStatus::FailClosed
    });
    if needs_self_correction {
        push_unique_reason(&mut diagnostics, "provider.stream.tool_call.repairable");
    }
    if ambiguous_tool_call && open_tools.is_empty() {
        push_unique_reason(&mut diagnostics, "provider.stream.tool_call.ambiguous");
    }

    let usage_timing = match (before_terminal_usage, late_usage) {
        (true, false) => ProviderUsageTiming::BeforeTerminal,
        (false, false) => ProviderUsageTiming::Missing,
        (false, true) => ProviderUsageTiming::LateOnly,
        (true, true) => ProviderUsageTiming::BeforeAndLate,
    };
    let structural_invalid = terminal_count > 1
        || diagnostics.iter().any(|reason| {
            matches!(
                reason.as_str(),
                "provider.stream.non_monotonic_host_sequence"
                    | "provider.stream.event_after_terminal"
                    | "provider.stream.tool_call.duplicated_start"
                    | "provider.stream.tool_call.delta_without_start"
                    | "provider.stream.tool_call.complete_without_start"
                    | "provider.stream.multiple_terminal_events"
                    | "provider.stream.late_usage_marker_missing"
                    | "provider.stream.early_usage_marked_late"
                    | "provider.stream.tool_call.ambiguous"
            )
        });
    let terminal_status =
        terminal_status.unwrap_or(NormalizedProviderTerminalStatus::RecoverableError);
    let (disposition, reason_code) = if structural_invalid {
        (
            ProviderTerminalDisposition::TerminallyInvalid,
            diagnostics
                .first()
                .cloned()
                .unwrap_or_else(|| "provider.stream.terminally_invalid".to_owned()),
        )
    } else if terminal_count == 0 {
        (
            ProviderTerminalDisposition::Recoverable,
            "provider.stream.missing_terminal_event".to_owned(),
        )
    } else if !open_tools.is_empty() {
        (
            ProviderTerminalDisposition::Recoverable,
            "provider.stream.tool_call.incomplete_at_terminal".to_owned(),
        )
    } else if needs_self_correction {
        (
            ProviderTerminalDisposition::Recoverable,
            "provider.stream.tool_call.repairable".to_owned(),
        )
    } else {
        match terminal_status {
            NormalizedProviderTerminalStatus::RecoverableError => (
                ProviderTerminalDisposition::Recoverable,
                terminal_reason_code
                    .unwrap_or_else(|| "provider.stream.recoverable_terminal".to_owned()),
            ),
            NormalizedProviderTerminalStatus::TerminalError => (
                ProviderTerminalDisposition::TerminallyInvalid,
                terminal_reason_code.unwrap_or_else(|| "provider.stream.terminal_error".to_owned()),
            ),
            NormalizedProviderTerminalStatus::Cancelled => {
                (ProviderTerminalDisposition::Complete, "provider.stream.cancelled".to_owned())
            }
            NormalizedProviderTerminalStatus::Complete
                if text_delta_count == 0
                    && assembly_report.tool_calls.is_empty()
                    && reasoning_delta_count > 0 =>
            {
                (
                    ProviderTerminalDisposition::Recoverable,
                    "provider.stream.reasoning_only_terminal".to_owned(),
                )
            }
            NormalizedProviderTerminalStatus::Complete
                if text_delta_count == 0 && assembly_report.tool_calls.is_empty() =>
            {
                (
                    ProviderTerminalDisposition::Recoverable,
                    "provider.stream.empty_terminal".to_owned(),
                )
            }
            NormalizedProviderTerminalStatus::Complete => {
                let reason_code = match usage_timing {
                    ProviderUsageTiming::Missing => "provider.stream.complete_usage_missing",
                    ProviderUsageTiming::LateOnly | ProviderUsageTiming::BeforeAndLate => {
                        "provider.stream.complete_late_usage_ignored"
                    }
                    ProviderUsageTiming::BeforeTerminal => "provider.stream.complete",
                };
                (ProviderTerminalDisposition::Complete, reason_code.to_owned())
            }
        }
    };

    ProviderTerminalValidationOutcome {
        schema_version: PROVIDER_TERMINAL_VALIDATION_SCHEMA_VERSION,
        event_type: PROVIDER_TERMINAL_VALIDATION_AUDIT_EVENT.to_owned(),
        disposition,
        reason_code,
        terminal_count,
        text_delta_count,
        reasoning_delta_count,
        tool_call_count: assembly_report.tool_calls.len(),
        repaired_tool_call_count,
        invalid_tool_call_count,
        usage_timing,
        diagnostic_reason_codes: diagnostics,
    }
}

/// Produces a hash-only artifact descriptor when debug capture was explicitly
/// enabled. Raw provider bytes are neither returned nor retained.
#[must_use]
pub fn redacted_provider_raw_debug_artifact(
    raw_payload: &[u8],
    policy: ProviderRawDebugArtifactPolicy,
) -> Option<ProviderRawDebugArtifact> {
    if !policy.explicitly_enabled {
        return None;
    }
    let hashed_bytes = raw_payload.len().min(policy.max_input_bytes.max(1));
    let payload_sha256 = stable_hash_bytes(&raw_payload[..hashed_bytes]);
    Some(ProviderRawDebugArtifact {
        schema_version: 1,
        redaction_level: "hash_only".to_owned(),
        observed_bytes: raw_payload.len(),
        hashed_bytes,
        payload_sha256,
        truncated: hashed_bytes < raw_payload.len(),
    })
}

fn refresh_sse_normalized_stream(report: &mut ProviderSseNormalizationReport) {
    report.normalized_stream_v2 =
        normalized_provider_stream_from_canonical_events_v2(&report.canonical_events);
}

fn push_unique_reason(reasons: &mut Vec<String>, reason_code: &str) {
    if !reasons.iter().any(|reason| reason == reason_code) {
        reasons.push(reason_code.to_owned());
    }
}

fn canonical_stream_diagnostic(
    reason_code: &str,
    severity: ProviderSseAuditSeverity,
    event_index: usize,
    provider_call_id: Option<String>,
) -> ProviderCanonicalStreamDiagnostic {
    ProviderCanonicalStreamDiagnostic {
        schema_version: PROVIDER_CANONICAL_STREAM_SCHEMA_VERSION,
        event_type: PROVIDER_CANONICAL_STREAM_AUDIT_EVENT.to_owned(),
        reason_code: reason_code.to_owned(),
        severity,
        event_index,
        provider_call_id,
    }
}

fn provider_call_id(provider_id: &str, model_id: &str) -> String {
    format!("{}:{}", provider_id.trim(), model_id.trim())
}

const fn provider_recovery_decision_is_recoverable(decision: ProviderRecoveryDecisionKind) -> bool {
    matches!(
        decision,
        ProviderRecoveryDecisionKind::RetrySameProvider
            | ProviderRecoveryDecisionKind::RetryAfter
            | ProviderRecoveryDecisionKind::RetryTransformed
            | ProviderRecoveryDecisionKind::RefreshCredential
            | ProviderRecoveryDecisionKind::FailoverProvider
            | ProviderRecoveryDecisionKind::CompactAndRetry
    )
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
    report.canonical_events.push(ProviderCanonicalEvent::StreamError {
        reason_code: reason_code.to_owned(),
        recoverable: action != ProviderFailureAction::FailClosedNoRetry,
    });
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
    stable_hash_bytes(input.as_bytes())
}

fn stable_hash_bytes(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
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
            ProviderStreamEvent::UsageDelta {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_read_tokens,
                cache_write_tokens,
            } => {
                self.usage.prompt_tokens = self.usage.prompt_tokens.saturating_add(prompt_tokens);
                self.usage.completion_tokens =
                    self.usage.completion_tokens.saturating_add(completion_tokens);
                self.usage.total_tokens = total_tokens.unwrap_or_else(|| {
                    self.usage.prompt_tokens.saturating_add(self.usage.completion_tokens)
                });
                self.usage.cache_read_tokens =
                    merge_optional_usage_counter(self.usage.cache_read_tokens, cache_read_tokens);
                self.usage.cache_write_tokens =
                    merge_optional_usage_counter(self.usage.cache_write_tokens, cache_write_tokens);
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

fn merge_optional_usage_counter(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
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
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
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
        assert_eq!(
            report.normalized_stream_v2.terminal_validation.disposition,
            ProviderTerminalDisposition::Recoverable
        );
        assert_eq!(
            report.normalized_stream_v2.terminal_validation.reason_code,
            "provider.stream.malformed_chunk"
        );
    }

    #[test]
    fn sse_normalizer_deduplicates_provider_sequence_and_ignores_late_usage() {
        let input = concat!(
            "id: text-1\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "id: text-1\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "id: usage-1\n",
            "data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
            "id: terminal-1\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
            "id: usage-2\n",
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
            .any(|event| event.reason_code == "provider.stream.duplicate_sequence"));
        assert!(report
            .audit_events
            .iter()
            .any(|event| event.reason_code == "provider.stream.late_usage"));
        assert_eq!(
            report.normalized_stream_v2.terminal_validation.usage_timing,
            ProviderUsageTiming::BeforeAndLate
        );
        assert_eq!(
            report.normalized_stream_v2.terminal_validation.disposition,
            ProviderTerminalDisposition::Complete
        );
    }

    #[test]
    fn sse_normalizer_preserves_legitimate_repeated_text_without_sequence() {
        let input = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ha\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"ha\"}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
        );

        let report = normalize_provider_sse_stream(input, "openai-compatible", "gpt-test");
        let text = report
            .events
            .iter()
            .filter_map(|event| match event {
                ProviderStreamEvent::Delta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();

        assert_eq!(text, "haha");
    }

    #[test]
    fn sse_normalizer_captures_provider_cache_usage() {
        let input = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            "data: {\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":2,\"total_tokens\":10,",
            "\"prompt_tokens_details\":{\"cached_tokens\":6},",
            "\"cache_creation_input_tokens\":4}}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n",
        );

        let report = normalize_provider_sse_stream(input, "openai-compatible", "gpt-test");
        let usage_event = report
            .events
            .iter()
            .find_map(|event| match event {
                ProviderStreamEvent::UsageDelta {
                    cache_read_tokens, cache_write_tokens, ..
                } => Some((*cache_read_tokens, *cache_write_tokens)),
                _ => None,
            })
            .expect("usage event should be present");
        let output = provider_output_from_text_and_tools(
            "hello".to_owned(),
            Vec::new(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(8, 2, "provider").with_cache_usage(Some(6), Some(4)),
            ProviderRawProviderRefs::default(),
        );

        assert_eq!(usage_event, (Some(6), Some(4)));
        assert_eq!(output.usage.cache_read_tokens, Some(6));
        assert_eq!(output.usage.cache_write_tokens, Some(4));
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
        assert_eq!(
            report.normalized_stream_v2.terminal_validation.reason_code,
            "provider.stream.idle_timeout"
        );
    }

    #[test]
    fn sse_normalizer_emits_canonical_fragmented_tool_arguments() {
        let input = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"palyra.fs.read\",\"arguments\":\"{\\\"pa\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"th\\\":\\\"Cargo.toml\\\"}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"finish_reason\":\"tool_calls\"}]}\n\n",
        );

        let report = normalize_provider_sse_stream(input, "openai-compatible", "gpt-test");

        assert!(report.canonical_events.iter().any(|event| {
            matches!(
                event,
                ProviderCanonicalEvent::ToolCallNameDelta { index: 0, name_delta }
                    if name_delta == "palyra.fs.read"
            )
        }));
        assert_eq!(
            report
                .canonical_events
                .iter()
                .filter(|event| matches!(
                    event,
                    ProviderCanonicalEvent::ToolCallArgumentsDelta { .. }
                ))
                .count(),
            2
        );
        assert!(matches!(
            report.canonical_events.last(),
            Some(ProviderCanonicalEvent::FinishReason {
                finish_reason: ProviderFinishReason::ToolCalls
            })
        ));
    }

    #[test]
    fn canonical_stream_validation_blocks_incomplete_tool_call() {
        let events = vec![
            ProviderCanonicalEvent::MessageStart {
                provider_id: "openai-compatible".to_owned(),
                model_id: "gpt-test".to_owned(),
                provider_call_id: "call".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallStart {
                index: 0,
                provider_call_id: Some("call_1".to_owned()),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml"}"#.to_owned(),
            },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::ToolCalls },
        ];

        let report = validate_canonical_provider_stream(events.as_slice());

        assert!(!report.valid);
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason_code == "provider.stream.tool_call.incomplete_at_terminal"
        }));
    }

    #[test]
    fn normalized_terminal_repairs_unambiguous_partial_tool_json() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart {
                index: 0,
                provider_call_id: Some("call_1".to_owned()),
            },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.read".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml",}"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::ToolCalls },
        ];

        let stream = normalized_provider_stream_from_canonical_events_v2(&events);

        assert_eq!(stream.terminal_validation.disposition, ProviderTerminalDisposition::Complete);
        assert_eq!(stream.terminal_validation.repaired_tool_call_count, 1);
        assert_eq!(stream.terminal_validation.invalid_tool_call_count, 0);
    }

    #[test]
    fn normalized_terminal_rejects_ambiguous_tool_json() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart {
                index: 0,
                provider_call_id: Some("call_1".to_owned()),
            },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.read".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml""#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::ToolCalls },
        ];

        let stream = normalized_provider_stream_from_canonical_events_v2(&events);

        assert_eq!(
            stream.terminal_validation.disposition,
            ProviderTerminalDisposition::TerminallyInvalid
        );
        assert_eq!(stream.terminal_validation.reason_code, "provider.stream.tool_call.ambiguous");
    }

    #[test]
    fn normalized_terminal_classifies_reasoning_only_as_recoverable() {
        let events = vec![
            ProviderCanonicalEvent::ReasoningDelta {
                byte_len: 7,
                payload_sha256: stable_hash_text("private"),
            },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::Stop },
        ];

        let stream = normalized_provider_stream_from_canonical_events_v2(&events);

        assert_eq!(
            stream.terminal_validation.disposition,
            ProviderTerminalDisposition::Recoverable
        );
        assert_eq!(
            stream.terminal_validation.reason_code,
            "provider.stream.reasoning_only_terminal"
        );
    }

    #[test]
    fn normalized_terminal_accepts_missing_usage_and_rejects_multiple_terminals() {
        let missing_usage = vec![
            ProviderCanonicalEvent::ContentDelta { text: "done".to_owned() },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::Stop },
        ];
        let missing_usage_stream =
            normalized_provider_stream_from_canonical_events_v2(&missing_usage);
        assert_eq!(
            missing_usage_stream.terminal_validation.disposition,
            ProviderTerminalDisposition::Complete
        );
        assert_eq!(
            missing_usage_stream.terminal_validation.usage_timing,
            ProviderUsageTiming::Missing
        );

        let multiple_terminals = vec![
            ProviderCanonicalEvent::ContentDelta { text: "done".to_owned() },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::Stop },
            ProviderCanonicalEvent::FinishReason { finish_reason: ProviderFinishReason::Stop },
        ];
        let invalid_stream =
            normalized_provider_stream_from_canonical_events_v2(&multiple_terminals);
        assert_eq!(
            invalid_stream.terminal_validation.disposition,
            ProviderTerminalDisposition::TerminallyInvalid
        );
        assert_eq!(invalid_stream.terminal_validation.terminal_count, 2);
    }

    #[test]
    fn raw_provider_debug_artifact_requires_explicit_opt_in_and_keeps_only_a_hash() {
        let raw = br#"{"secret":"provider-payload"}"#;

        assert!(redacted_provider_raw_debug_artifact(
            raw,
            ProviderRawDebugArtifactPolicy::default()
        )
        .is_none());

        let artifact = redacted_provider_raw_debug_artifact(
            raw,
            ProviderRawDebugArtifactPolicy { explicitly_enabled: true, max_input_bytes: 8 },
        )
        .expect("explicit debug policy should produce hash-only metadata");
        assert_eq!(artifact.redaction_level, "hash_only");
        assert_eq!(artifact.hashed_bytes, 8);
        assert!(artifact.truncated);
        assert!(!serde_json::to_string(&artifact)
            .expect("artifact should serialize")
            .contains("provider-payload"));
    }

    #[test]
    fn provider_stream_fault_matrix_covers_required_anomalies() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../fixtures/golden/provider_stream_fault_matrix_v2.json"
        ))
        .expect("provider stream fault matrix should parse");
        let case_ids = fixture
            .get("cases")
            .and_then(Value::as_array)
            .expect("fault matrix cases should be an array")
            .iter()
            .filter_map(|case| case.get("id").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();

        for required in [
            "malformed_sse_chunk",
            "duplicate_text_delta",
            "partial_tool_json",
            "reasoning_only_terminal",
            "missing_usage",
            "late_usage",
            "midstream_idle_timeout",
        ] {
            assert!(case_ids.contains(required), "missing fault case: {required}");
        }
    }
}
