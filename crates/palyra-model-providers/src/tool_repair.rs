//! Conservative tool-call repair contracts for provider outputs.
//!
//! The parser intentionally produces hash-only audit metadata by default. The
//! runtime-only normalized argument bytes stay off serialized decisions so
//! later daemon integration can audit malformed provider output without
//! persisting raw model-provided arguments.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    ProviderFinishReason, ProviderOutputContentPart, ProviderRawProviderRefs, ProviderStreamEvent,
    ProviderTurnOutput,
};

/// Schema version for tool repair parser and audit payloads.
pub const TOOL_REPAIR_SCHEMA_VERSION: u16 = 1;
/// Initial hard cap for repaired argument payloads.
pub const DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES: usize = 256 * 1024;

/// Tape/audit event emitted after provider output normalization.
pub const PROVIDER_STREAM_NORMALIZED_EVENT: &str = "provider.stream.normalized";
/// Tape/audit event emitted when a closed output can be inspected for repair.
pub const PROVIDER_STREAM_REPAIR_BOUNDARY_CLOSED_EVENT: &str =
    "provider.stream.repair_boundary_closed";
/// Tape/audit event emitted when a tool-repair-shaped candidate is detected.
pub const TOOL_REPAIR_CANDIDATE_DETECTED_EVENT: &str = "tool.repair.candidate_detected";
/// Tape/audit event emitted when a candidate passes strict parser validation.
pub const TOOL_REPAIR_ACCEPTED_EVENT: &str = "tool.repair.accepted";
/// Tape/audit event emitted when a candidate is rejected.
pub const TOOL_REPAIR_REJECTED_EVENT: &str = "tool.repair.rejected";

/// Allowed surface grammar that produced a candidate.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRepairCandidateFormat {
    JsonObject,
    XmlToolCall,
    HarmonyToolCall,
}

impl ToolRepairCandidateFormat {
    const fn as_str(self) -> &'static str {
        match self {
            Self::JsonObject => "json_object",
            Self::XmlToolCall => "xml_tool_call",
            Self::HarmonyToolCall => "harmony_tool_call",
        }
    }
}

/// Result class for one repair parser decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRepairDecisionStatus {
    Accepted,
    Rejected,
    NotCandidate,
}

impl ToolRepairDecisionStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::NotCandidate => "not_candidate",
        }
    }
}

/// Whether the provider stream segment is complete enough for repair parsing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRepairBoundaryState {
    Open,
    Closed,
}

impl ToolRepairBoundaryState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }
}

/// Hash-only summary of one provider stream segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderStreamSegment {
    pub schema_version: u16,
    pub kind: String,
    pub byte_len: usize,
    pub payload_sha256: Option<String>,
    pub reason_code: String,
}

/// Provider-neutral event projection used before tool repair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderNeutralStreamEvent {
    AssistantText { byte_len: usize, payload_sha256: String },
    ToolProposal { proposal_id: String, tool_name: String, arguments_json_sha256: String },
    ProviderError { reason_code: String },
    Finish { reason: ProviderFinishReason },
}

/// Closed/open boundary metadata for repair parsing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolRepairBoundary {
    pub schema_version: u16,
    pub state: ToolRepairBoundaryState,
    pub reason_code: String,
    pub assistant_text_bytes: usize,
    pub assistant_text_sha256: Option<String>,
    pub structured_tool_proposal_count: usize,
    pub finish_reason: ProviderFinishReason,
}

/// Normalized assistant output without raw provider text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NormalizedAssistantOutput {
    pub schema_version: u16,
    pub events: Vec<ProviderNeutralStreamEvent>,
    pub segments: Vec<ProviderStreamSegment>,
    pub repair_boundary: ToolRepairBoundary,
    pub mixed_assistant_text_and_tool_call: bool,
}

/// Hash-only metadata for a repaired tool proposal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolProposalCandidate {
    pub schema_version: u16,
    pub proposal_id: String,
    pub tool_name: String,
    pub format: ToolRepairCandidateFormat,
    pub source_payload_sha256: String,
    pub source_payload_bytes: usize,
    pub arguments_json_sha256: String,
    pub arguments_json_bytes: usize,
}

/// Candidate envelope produced by the repair parser.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolRepairCandidate {
    pub schema_version: u16,
    pub proposal: ToolProposalCandidate,
    pub reason_code: String,
}

/// Strict repair parser decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolRepairDecision {
    pub schema_version: u16,
    pub status: ToolRepairDecisionStatus,
    pub reason_code: String,
    pub message: String,
    pub candidate: Option<ToolRepairCandidate>,
    pub source_payload_sha256: String,
    pub source_payload_bytes: usize,
    /// Runtime-only canonical JSON arguments. This field is deliberately not
    /// serialized into audit payloads because arguments can contain secrets.
    #[serde(skip)]
    pub normalized_input_json: Option<Vec<u8>>,
}

/// Hash-only event ready to be mirrored into a tape row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolRepairAuditEvent {
    pub event_type: String,
    pub payload_json: Value,
}

#[derive(Debug, Clone)]
struct ParsedToolRepairCandidate {
    format: ToolRepairCandidateFormat,
    tool_name: String,
    arguments: Value,
}

/// Incrementally normalizes provider stream events for repair parsing.
#[derive(Debug, Clone)]
pub struct ToolRepairStreamNormalizer {
    assistant_text: String,
    events: Vec<ProviderNeutralStreamEvent>,
    segments: Vec<ProviderStreamSegment>,
    tool_proposal_count: usize,
    finish_reason: ProviderFinishReason,
    closed: bool,
}

impl Default for ToolRepairStreamNormalizer {
    fn default() -> Self {
        Self {
            assistant_text: String::new(),
            events: Vec::new(),
            segments: Vec::new(),
            tool_proposal_count: 0,
            finish_reason: ProviderFinishReason::Unknown,
            closed: false,
        }
    }
}

impl ToolRepairStreamNormalizer {
    /// Creates an empty stream normalizer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one provider event. Events after a terminal event are ignored.
    pub fn apply(&mut self, event: ProviderStreamEvent) {
        if self.closed {
            return;
        }
        match event {
            ProviderStreamEvent::Started { .. } => {}
            ProviderStreamEvent::Delta { text } => {
                self.record_text_segment(text);
            }
            ProviderStreamEvent::ToolDelta { proposal_id, tool_name, input_json } => {
                self.tool_proposal_count = self.tool_proposal_count.saturating_add(1);
                self.events.push(ProviderNeutralStreamEvent::ToolProposal {
                    proposal_id,
                    tool_name,
                    arguments_json_sha256: stable_hash_value(&input_json),
                });
            }
            ProviderStreamEvent::UsageDelta { .. } => {}
            ProviderStreamEvent::Completed { finish_reason, raw_provider_refs: _ } => {
                self.close(finish_reason);
            }
            ProviderStreamEvent::Failed { error } => {
                self.events.push(ProviderNeutralStreamEvent::ProviderError {
                    reason_code: provider_error_kind_reason(error.kind).to_owned(),
                });
                self.close(ProviderFinishReason::Error);
            }
            ProviderStreamEvent::Cancelled { reason: _ } => {
                self.close(ProviderFinishReason::Cancelled);
            }
        }
    }

    /// Returns the current repair boundary without consuming the normalizer.
    #[must_use]
    pub fn boundary(&self) -> ToolRepairBoundary {
        boundary_from_parts(
            if self.closed {
                ToolRepairBoundaryState::Closed
            } else {
                ToolRepairBoundaryState::Open
            },
            self.assistant_text.as_str(),
            self.tool_proposal_count,
            self.finish_reason,
        )
    }

    /// Returns raw text only after the stream is terminal.
    #[must_use]
    pub fn closed_text_for_repair(&self) -> Option<&str> {
        self.closed.then_some(self.assistant_text.as_str())
    }

    /// Builds the hash-only normalized output projection.
    #[must_use]
    pub fn normalized_output(&self) -> NormalizedAssistantOutput {
        NormalizedAssistantOutput {
            schema_version: TOOL_REPAIR_SCHEMA_VERSION,
            events: self.events.clone(),
            segments: self.segments.clone(),
            repair_boundary: self.boundary(),
            mixed_assistant_text_and_tool_call: !self.assistant_text.trim().is_empty()
                && self.tool_proposal_count > 0,
        }
    }

    fn record_text_segment(&mut self, text: String) {
        let payload_sha256 = sha256_hex(text.as_bytes());
        self.segments.push(ProviderStreamSegment {
            schema_version: TOOL_REPAIR_SCHEMA_VERSION,
            kind: "assistant_text".to_owned(),
            byte_len: text.len(),
            payload_sha256: Some(payload_sha256.clone()),
            reason_code: "provider.stream.assistant_text_delta".to_owned(),
        });
        self.events.push(ProviderNeutralStreamEvent::AssistantText {
            byte_len: text.len(),
            payload_sha256,
        });
        self.assistant_text.push_str(text.as_str());
    }

    fn close(&mut self, finish_reason: ProviderFinishReason) {
        self.finish_reason = finish_reason;
        self.closed = true;
        self.events.push(ProviderNeutralStreamEvent::Finish { reason: finish_reason });
    }
}

/// Builds a hash-only normalized projection from a completed provider output.
#[must_use]
pub fn normalize_assistant_output_for_tool_repair(
    output: &ProviderTurnOutput,
) -> NormalizedAssistantOutput {
    let mut normalizer = ToolRepairStreamNormalizer::new();
    for part in &output.content_parts {
        match part {
            ProviderOutputContentPart::Text { text } => {
                if !text.is_empty() {
                    normalizer.record_text_segment(text.clone());
                }
            }
            ProviderOutputContentPart::ToolCall { proposal_id, tool_name, input_json } => {
                normalizer.tool_proposal_count = normalizer.tool_proposal_count.saturating_add(1);
                normalizer.events.push(ProviderNeutralStreamEvent::ToolProposal {
                    proposal_id: proposal_id.clone(),
                    tool_name: tool_name.clone(),
                    arguments_json_sha256: stable_hash_value(input_json),
                });
            }
        }
    }
    if normalizer.assistant_text.is_empty() && !output.full_text.is_empty() {
        normalizer.record_text_segment(output.full_text.clone());
    }
    normalizer.close(output.finish_reason);
    NormalizedAssistantOutput {
        repair_boundary: boundary_from_parts(
            ToolRepairBoundaryState::Closed,
            output.full_text.as_str(),
            normalizer.tool_proposal_count,
            output.finish_reason,
        ),
        ..normalizer.normalized_output()
    }
}

/// Parses one closed provider text segment as a strictly shaped tool call.
///
/// # Errors
/// This function does not return `Result`; parser failures are represented as
/// [`ToolRepairDecisionStatus::Rejected`] with a stable reason code so they
/// can be mirrored into audit logs and replay fixtures.
#[must_use]
pub fn decide_tool_repair_candidate<I, S>(
    source_text: &str,
    visible_tool_names: I,
    max_payload_bytes: usize,
) -> ToolRepairDecision
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let max_payload_bytes = max_payload_bytes.max(1);
    let source_payload_sha256 = sha256_hex(source_text.as_bytes());
    let source_payload_bytes = source_text.len();
    if source_payload_bytes > max_payload_bytes {
        return rejected_decision(
            "tool_repair.rejected.payload_too_large",
            "tool repair payload exceeds the configured byte limit",
            source_payload_sha256,
            source_payload_bytes,
            None,
        );
    }

    let visible_tools = visible_tool_names
        .into_iter()
        .map(|name| name.as_ref().to_owned())
        .collect::<BTreeSet<_>>();
    let parsed = match parse_candidate_shape(source_text) {
        CandidateParseOutcome::Candidate(candidate) => candidate,
        CandidateParseOutcome::Rejected { reason_code, message } => {
            return rejected_decision(
                reason_code,
                message,
                source_payload_sha256,
                source_payload_bytes,
                None,
            );
        }
        CandidateParseOutcome::NotCandidate => {
            return ToolRepairDecision {
                schema_version: TOOL_REPAIR_SCHEMA_VERSION,
                status: ToolRepairDecisionStatus::NotCandidate,
                reason_code: "tool_repair.not_candidate".to_owned(),
                message: "provider text does not match an allowed repair grammar".to_owned(),
                candidate: None,
                source_payload_sha256,
                source_payload_bytes,
                normalized_input_json: None,
            };
        }
    };

    if !visible_tools.contains(parsed.tool_name.as_str()) {
        return rejected_decision(
            "tool_repair.rejected.tool_not_visible",
            "tool name is not visible in the current catalog snapshot",
            source_payload_sha256,
            source_payload_bytes,
            None,
        );
    }
    if !parsed.arguments.is_object() {
        return rejected_decision(
            "tool_repair.rejected.arguments_not_object",
            "tool arguments must be a JSON object",
            source_payload_sha256,
            source_payload_bytes,
            None,
        );
    }

    let normalized_input_json = canonical_json_bytes(&parsed.arguments);
    if normalized_input_json.len() > max_payload_bytes {
        return rejected_decision(
            "tool_repair.rejected.payload_too_large",
            "normalized tool arguments exceed the configured byte limit",
            source_payload_sha256,
            source_payload_bytes,
            None,
        );
    }
    let arguments_json_sha256 = sha256_hex(normalized_input_json.as_slice());
    let proposal_id = stable_repair_proposal_id(source_payload_sha256.as_str());
    let proposal = ToolProposalCandidate {
        schema_version: TOOL_REPAIR_SCHEMA_VERSION,
        proposal_id,
        tool_name: parsed.tool_name,
        format: parsed.format,
        source_payload_sha256: source_payload_sha256.clone(),
        source_payload_bytes,
        arguments_json_sha256,
        arguments_json_bytes: normalized_input_json.len(),
    };
    ToolRepairDecision {
        schema_version: TOOL_REPAIR_SCHEMA_VERSION,
        status: ToolRepairDecisionStatus::Accepted,
        reason_code: "tool_repair.accepted".to_owned(),
        message: "tool repair candidate passed strict grammar and catalog validation".to_owned(),
        candidate: Some(ToolRepairCandidate {
            schema_version: TOOL_REPAIR_SCHEMA_VERSION,
            proposal,
            reason_code: "tool_repair.candidate_detected".to_owned(),
        }),
        source_payload_sha256,
        source_payload_bytes,
        normalized_input_json: Some(normalized_input_json),
    }
}

/// Converts normalized output plus parser decision into hash-only audit rows.
#[must_use]
pub fn tool_repair_audit_events_for_decision(
    normalized_output: &NormalizedAssistantOutput,
    decision: &ToolRepairDecision,
) -> Vec<ToolRepairAuditEvent> {
    let mut events = vec![
        ToolRepairAuditEvent {
            event_type: PROVIDER_STREAM_NORMALIZED_EVENT.to_owned(),
            payload_json: json!({
                "schema_version": TOOL_REPAIR_SCHEMA_VERSION,
                "event": PROVIDER_STREAM_NORMALIZED_EVENT,
                "redaction_level": "hash_only",
                "repair_boundary_state": normalized_output.repair_boundary.state.as_str(),
                "repair_boundary_reason_code": normalized_output.repair_boundary.reason_code,
                "assistant_text_bytes": normalized_output.repair_boundary.assistant_text_bytes,
                "assistant_text_sha256": normalized_output.repair_boundary.assistant_text_sha256,
                "structured_tool_proposal_count": normalized_output.repair_boundary.structured_tool_proposal_count,
                "mixed_assistant_text_and_tool_call": normalized_output.mixed_assistant_text_and_tool_call,
            }),
        },
        ToolRepairAuditEvent {
            event_type: PROVIDER_STREAM_REPAIR_BOUNDARY_CLOSED_EVENT.to_owned(),
            payload_json: json!({
                "schema_version": TOOL_REPAIR_SCHEMA_VERSION,
                "event": PROVIDER_STREAM_REPAIR_BOUNDARY_CLOSED_EVENT,
                "redaction_level": "hash_only",
                "state": normalized_output.repair_boundary.state.as_str(),
                "reason_code": normalized_output.repair_boundary.reason_code,
                "finish_reason": normalized_output.repair_boundary.finish_reason,
            }),
        },
    ];

    if let Some(candidate) = &decision.candidate {
        let proposal = &candidate.proposal;
        events.push(ToolRepairAuditEvent {
            event_type: TOOL_REPAIR_CANDIDATE_DETECTED_EVENT.to_owned(),
            payload_json: json!({
                "schema_version": TOOL_REPAIR_SCHEMA_VERSION,
                "event": TOOL_REPAIR_CANDIDATE_DETECTED_EVENT,
                "redaction_level": "hash_only",
                "reason_code": candidate.reason_code,
                "proposal_id": proposal.proposal_id,
                "tool_name": proposal.tool_name,
                "format": proposal.format.as_str(),
                "source_payload_sha256": proposal.source_payload_sha256,
                "source_payload_bytes": proposal.source_payload_bytes,
                "arguments_json_sha256": proposal.arguments_json_sha256,
                "arguments_json_bytes": proposal.arguments_json_bytes,
            }),
        });
    }

    let event_type = match decision.status {
        ToolRepairDecisionStatus::Accepted => TOOL_REPAIR_ACCEPTED_EVENT,
        ToolRepairDecisionStatus::Rejected | ToolRepairDecisionStatus::NotCandidate => {
            TOOL_REPAIR_REJECTED_EVENT
        }
    };
    events.push(ToolRepairAuditEvent {
        event_type: event_type.to_owned(),
        payload_json: json!({
            "schema_version": TOOL_REPAIR_SCHEMA_VERSION,
            "event": event_type,
            "redaction_level": "hash_only",
            "status": decision.status.as_str(),
            "reason_code": decision.reason_code,
            "message": decision.message,
            "source_payload_sha256": decision.source_payload_sha256,
            "source_payload_bytes": decision.source_payload_bytes,
        }),
    });
    events
}

fn boundary_from_parts(
    state: ToolRepairBoundaryState,
    assistant_text: &str,
    structured_tool_proposal_count: usize,
    finish_reason: ProviderFinishReason,
) -> ToolRepairBoundary {
    let assistant_text_sha256 =
        (!assistant_text.is_empty()).then(|| sha256_hex(assistant_text.as_bytes()));
    let reason_code = match state {
        ToolRepairBoundaryState::Open => "provider.stream.repair_boundary.open",
        ToolRepairBoundaryState::Closed => "provider.stream.repair_boundary.closed",
    };
    ToolRepairBoundary {
        schema_version: TOOL_REPAIR_SCHEMA_VERSION,
        state,
        reason_code: reason_code.to_owned(),
        assistant_text_bytes: assistant_text.len(),
        assistant_text_sha256,
        structured_tool_proposal_count,
        finish_reason,
    }
}

enum CandidateParseOutcome {
    Candidate(ParsedToolRepairCandidate),
    Rejected { reason_code: &'static str, message: &'static str },
    NotCandidate,
}

fn parse_candidate_shape(input: &str) -> CandidateParseOutcome {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return CandidateParseOutcome::NotCandidate;
    }
    if trimmed.starts_with('{') {
        return parse_json_tool_call_object(trimmed);
    }
    if trimmed.contains("<|tool_call|>") || trimmed.contains("<|/tool_call|>") {
        return parse_harmony_tool_call(trimmed);
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("<tool_call")
        || lower.contains("<invoke")
        || lower.contains("<minimax:tool_call")
    {
        return parse_xml_tool_call(trimmed);
    }
    CandidateParseOutcome::NotCandidate
}

fn parse_json_tool_call_object(input: &str) -> CandidateParseOutcome {
    let value = match serde_json::from_str::<Value>(input) {
        Ok(value) => value,
        Err(_) => {
            return CandidateParseOutcome::Rejected {
                reason_code: "tool_repair.rejected.invalid_candidate_json",
                message: "candidate JSON is malformed",
            };
        }
    };
    parse_json_candidate_value(value, ToolRepairCandidateFormat::JsonObject)
}

fn parse_harmony_tool_call(input: &str) -> CandidateParseOutcome {
    let Some(body) = input.strip_prefix("<|tool_call|>") else {
        return mixed_content_rejection();
    };
    let Some(body) = body.strip_suffix("<|/tool_call|>") else {
        return mixed_content_rejection();
    };
    let value = match serde_json::from_str::<Value>(body.trim()) {
        Ok(value) => value,
        Err(_) => {
            return CandidateParseOutcome::Rejected {
                reason_code: "tool_repair.rejected.invalid_candidate_json",
                message: "harmony tool-call JSON is malformed",
            };
        }
    };
    parse_json_candidate_value(value, ToolRepairCandidateFormat::HarmonyToolCall)
}

fn parse_xml_tool_call(input: &str) -> CandidateParseOutcome {
    let lower = input.to_ascii_lowercase();
    if lower.starts_with("<minimax:tool_call") {
        return parse_minimax_wrapper(input);
    }
    parse_exact_xml_invocation(input, "tool_call")
        .or_else(|| parse_exact_xml_invocation(input, "invoke"))
        .map_or_else(mixed_content_rejection, CandidateParseOutcome::Candidate)
}

fn parse_minimax_wrapper(input: &str) -> CandidateParseOutcome {
    let tag_end = match input.find('>') {
        Some(tag_end) => tag_end,
        None => return malformed_xml_rejection(),
    };
    let lower = input.to_ascii_lowercase();
    let close_tag = "</minimax:tool_call>";
    if !lower.ends_with(close_tag) {
        return mixed_content_rejection();
    }
    let body_end = input.len().saturating_sub(close_tag.len());
    let body = input[tag_end + 1..body_end].trim();
    parse_exact_xml_invocation(body, "invoke")
        .map_or_else(malformed_xml_rejection, CandidateParseOutcome::Candidate)
}

fn parse_exact_xml_invocation(input: &str, tag: &str) -> Option<ParsedToolRepairCandidate> {
    let lower = input.to_ascii_lowercase();
    let opening_prefix = format!("<{tag}");
    if !lower.starts_with(opening_prefix.as_str()) {
        return None;
    }
    let tag_end = input.find('>')?;
    let opening_tag = &input[..=tag_end];
    let close_tag = format!("</{tag}>");
    if !lower.ends_with(close_tag.as_str()) {
        return None;
    }
    let tool_name = extract_name_attribute(opening_tag)?;
    let body_end = input.len().checked_sub(close_tag.len())?;
    let arguments = parse_json_object_value(input[tag_end + 1..body_end].trim())?;
    Some(ParsedToolRepairCandidate {
        format: ToolRepairCandidateFormat::XmlToolCall,
        tool_name,
        arguments,
    })
}

fn parse_json_candidate_value(
    value: Value,
    format: ToolRepairCandidateFormat,
) -> CandidateParseOutcome {
    let Some(object) = value.as_object() else {
        return CandidateParseOutcome::NotCandidate;
    };
    let Some(tool_name) = object
        .get("tool_name")
        .or_else(|| object.get("name"))
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
    else {
        return CandidateParseOutcome::NotCandidate;
    };
    let Some(arguments) = object.get("arguments").cloned() else {
        return CandidateParseOutcome::Rejected {
            reason_code: "tool_repair.rejected.arguments_missing",
            message: "candidate is missing arguments",
        };
    };
    CandidateParseOutcome::Candidate(ParsedToolRepairCandidate {
        format,
        tool_name: tool_name.trim().to_owned(),
        arguments,
    })
}

fn parse_json_object_value(input: &str) -> Option<Value> {
    let value = serde_json::from_str::<Value>(input).ok()?;
    value.is_object().then_some(value)
}

fn extract_name_attribute(opening_tag: &str) -> Option<String> {
    let lower = opening_tag.to_ascii_lowercase();
    let mut cursor = lower.find("name")? + "name".len();
    let bytes = opening_tag.as_bytes();
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor = cursor.saturating_add(1);
    }
    if bytes.get(cursor).copied() != Some(b'=') {
        return None;
    }
    cursor = cursor.saturating_add(1);
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor = cursor.saturating_add(1);
    }
    let quote = bytes.get(cursor).copied()?;
    if !matches!(quote, b'"' | b'\'') {
        return None;
    }
    cursor = cursor.saturating_add(1);
    let value_start = cursor;
    while let Some(byte) = bytes.get(cursor).copied() {
        if byte == quote {
            let value = opening_tag[value_start..cursor].trim();
            return (!value.is_empty()).then(|| value.to_owned());
        }
        cursor = cursor.saturating_add(1);
    }
    None
}

fn rejected_decision(
    reason_code: impl Into<String>,
    message: impl Into<String>,
    source_payload_sha256: String,
    source_payload_bytes: usize,
    candidate: Option<ToolRepairCandidate>,
) -> ToolRepairDecision {
    ToolRepairDecision {
        schema_version: TOOL_REPAIR_SCHEMA_VERSION,
        status: ToolRepairDecisionStatus::Rejected,
        reason_code: reason_code.into(),
        message: message.into(),
        candidate,
        source_payload_sha256,
        source_payload_bytes,
        normalized_input_json: None,
    }
}

fn mixed_content_rejection() -> CandidateParseOutcome {
    CandidateParseOutcome::Rejected {
        reason_code: "tool_repair.rejected.mixed_content",
        message: "tool repair candidate must be the whole assistant output",
    }
}

fn malformed_xml_rejection() -> CandidateParseOutcome {
    CandidateParseOutcome::Rejected {
        reason_code: "tool_repair.rejected.malformed_xml",
        message: "XML-style tool repair candidate is malformed",
    }
}

fn stable_repair_proposal_id(source_payload_sha256: &str) -> String {
    let prefix_len = source_payload_sha256.len().min(16);
    format!("tool_repair_{}", &source_payload_sha256[..prefix_len])
}

fn stable_hash_value(value: &Value) -> String {
    sha256_hex(canonical_json_bytes(value).as_slice())
}

fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut sorted = value.clone();
    sort_json_value(&mut sorted);
    serde_json::to_vec(&sorted).unwrap_or_else(|_| b"null".to_vec())
}

fn sort_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let mut sorted = std::collections::BTreeMap::new();
            for (key, mut value) in std::mem::take(map) {
                sort_json_value(&mut value);
                sorted.insert(key, value);
            }
            *map = sorted.into_iter().collect();
        }
        Value::Array(values) => {
            for value in values {
                sort_json_value(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn provider_error_kind_reason(kind: crate::ProviderErrorKind) -> &'static str {
    match kind {
        crate::ProviderErrorKind::Auth => "auth",
        crate::ProviderErrorKind::Quota => "quota",
        crate::ProviderErrorKind::RateLimit => "rate_limit",
        crate::ProviderErrorKind::TransientNetwork => "transient_network",
        crate::ProviderErrorKind::MalformedResponse => "malformed_response",
        crate::ProviderErrorKind::ProviderPolicy => "provider_policy",
        crate::ProviderErrorKind::Timeout => "timeout",
        crate::ProviderErrorKind::UnsupportedFeature => "unsupported_feature",
        crate::ProviderErrorKind::CircuitOpen => "circuit_open",
        crate::ProviderErrorKind::MissingConfiguration => "missing_configuration",
        crate::ProviderErrorKind::Internal => "internal",
    }
}

#[allow(dead_code)]
fn _raw_provider_refs_are_not_part_of_repair_payload(_: &ProviderRawProviderRefs) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderRawProviderRefs, ProviderUsage};

    fn visible_tools() -> [&'static str; 2] {
        ["palyra.fs.read", "palyra.process.run"]
    }

    #[test]
    fn json_candidate_is_accepted_when_tool_is_visible() {
        let decision = decide_tool_repair_candidate(
            r#"{"tool_name":"palyra.fs.read","arguments":{"path":"src/lib.rs"}}"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Accepted);
        assert_eq!(decision.reason_code, "tool_repair.accepted");
        assert!(decision.normalized_input_json.is_some());
        let candidate = decision.candidate.expect("candidate should be available");
        assert_eq!(candidate.proposal.tool_name, "palyra.fs.read");
        assert_eq!(candidate.proposal.format, ToolRepairCandidateFormat::JsonObject);
    }

    #[test]
    fn xml_candidate_is_accepted_when_it_is_the_whole_output() {
        let decision = decide_tool_repair_candidate(
            r#"<tool_call name="palyra.process.run">{"cmd":"cargo test"}</tool_call>"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Accepted);
        assert_eq!(
            decision.candidate.expect("candidate").proposal.format,
            ToolRepairCandidateFormat::XmlToolCall
        );
    }

    #[test]
    fn minimax_invoke_wrapper_is_accepted_strictly() {
        let decision = decide_tool_repair_candidate(
            r#"<minimax:tool_call><invoke name="palyra.fs.read">{"path":"Cargo.toml"}</invoke></minimax:tool_call>"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Accepted);
        assert_eq!(decision.candidate.expect("candidate").proposal.tool_name, "palyra.fs.read");
    }

    #[test]
    fn harmony_candidate_is_accepted_when_wrapped_exactly() {
        let decision = decide_tool_repair_candidate(
            r#"<|tool_call|>{"name":"palyra.fs.read","arguments":{"path":"Cargo.toml"}}<|/tool_call|>"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Accepted);
        assert_eq!(
            decision.candidate.expect("candidate").proposal.format,
            ToolRepairCandidateFormat::HarmonyToolCall
        );
    }

    #[test]
    fn mixed_text_and_tool_call_is_rejected() {
        let decision = decide_tool_repair_candidate(
            r#"I will inspect it. <tool_call name="palyra.fs.read">{"path":"Cargo.toml"}</tool_call>"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Rejected);
        assert_eq!(decision.reason_code, "tool_repair.rejected.mixed_content");
        assert!(decision.normalized_input_json.is_none());
    }

    #[test]
    fn unknown_tool_is_rejected() {
        let decision = decide_tool_repair_candidate(
            r#"{"tool_name":"palyra.unknown","arguments":{}}"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Rejected);
        assert_eq!(decision.reason_code, "tool_repair.rejected.tool_not_visible");
    }

    #[test]
    fn arguments_must_be_json_object() {
        let decision = decide_tool_repair_candidate(
            r#"{"tool_name":"palyra.fs.read","arguments":"Cargo.toml"}"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        assert_eq!(decision.status, ToolRepairDecisionStatus::Rejected);
        assert_eq!(decision.reason_code, "tool_repair.rejected.arguments_not_object");
    }

    #[test]
    fn payload_limit_is_enforced() {
        let oversized = "a".repeat(32);

        let decision = decide_tool_repair_candidate(oversized.as_str(), visible_tools(), 16);

        assert_eq!(decision.status, ToolRepairDecisionStatus::Rejected);
        assert_eq!(decision.reason_code, "tool_repair.rejected.payload_too_large");
    }

    #[test]
    fn serialized_decision_excludes_runtime_arguments() {
        let decision = decide_tool_repair_candidate(
            r#"{"tool_name":"palyra.fs.read","arguments":{"path":"src/lib.rs"}}"#,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        let encoded = serde_json::to_value(&decision).expect("decision should serialize");

        assert!(encoded.get("normalized_input_json").is_none());
        assert_eq!(encoded["status"], "accepted");
    }

    #[test]
    fn stream_normalizer_waits_for_closed_boundary() {
        let mut normalizer = ToolRepairStreamNormalizer::new();
        normalizer.apply(ProviderStreamEvent::Delta {
            text: r#"{"tool_name":"palyra.fs.read","#.to_owned(),
        });

        assert_eq!(normalizer.boundary().state, ToolRepairBoundaryState::Open);
        assert!(normalizer.closed_text_for_repair().is_none());

        normalizer.apply(ProviderStreamEvent::Delta {
            text: r#""arguments":{"path":"Cargo.toml"}}"#.to_owned(),
        });
        normalizer.apply(ProviderStreamEvent::Completed {
            finish_reason: ProviderFinishReason::ToolCalls,
            raw_provider_refs: ProviderRawProviderRefs::default(),
        });

        assert_eq!(normalizer.boundary().state, ToolRepairBoundaryState::Closed);
        let closed = normalizer.closed_text_for_repair().expect("closed text should be available");
        let decision = decide_tool_repair_candidate(
            closed,
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );
        assert_eq!(decision.status, ToolRepairDecisionStatus::Accepted);
    }

    #[test]
    fn completed_output_projection_is_hash_only() {
        let output = ProviderTurnOutput::text(
            "plain response".to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(1, 1, "test"),
            ProviderRawProviderRefs::default(),
        );

        let normalized = normalize_assistant_output_for_tool_repair(&output);
        let encoded = serde_json::to_string(&normalized).expect("projection should serialize");

        assert_eq!(normalized.repair_boundary.state, ToolRepairBoundaryState::Closed);
        assert!(encoded.contains("payload_sha256"));
        assert!(!encoded.contains("plain response"));
    }

    #[test]
    fn audit_events_record_candidate_and_decision_without_arguments() {
        let output = ProviderTurnOutput::text(
            r#"{"tool_name":"palyra.fs.read","arguments":{"path":"secret.txt"}}"#.to_owned(),
            ProviderFinishReason::ToolCalls,
            ProviderUsage::new(1, 1, "test"),
            ProviderRawProviderRefs::default(),
        );
        let normalized = normalize_assistant_output_for_tool_repair(&output);
        let decision = decide_tool_repair_candidate(
            output.full_text.as_str(),
            visible_tools(),
            DEFAULT_TOOL_REPAIR_ARGUMENT_LIMIT_BYTES,
        );

        let events = tool_repair_audit_events_for_decision(&normalized, &decision);
        let encoded = serde_json::to_string(&events).expect("events should serialize");

        assert_eq!(events[0].event_type, PROVIDER_STREAM_NORMALIZED_EVENT);
        assert!(events.iter().any(|event| event.event_type == TOOL_REPAIR_ACCEPTED_EVENT));
        assert!(encoded.contains("tool.repair.candidate_detected"));
        assert!(!encoded.contains("secret.txt"));
    }
}
