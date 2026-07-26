//! Deterministic provider transcript projection and tool-pair repair.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ProviderMessage, ProviderMessageRole};

/// Provider-message limit enforced before a transcript reaches a wire adapter.
pub const MAX_PROVIDER_TRANSCRIPT_MESSAGES: usize = 512;
/// Maximum text retained in one replayed tool result.
pub const MAX_PROVIDER_TRANSCRIPT_TOOL_RESULT_CHARS: usize = 16 * 1024;
const MAX_PROVIDER_TOOL_CALL_ID_CHARS: usize = 64;
const SYNTHETIC_TOOL_FAILURE: &str =
    r#"{"error":"tool result unavailable during transcript repair","success":false}"#;
const ORPHAN_TOOL_RESULT_MARKER: &str =
    "[provider transcript repair: unpaired tool result omitted]";

/// Closed provider dialects whose role and tool-pair rules affect projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTranscriptDialect {
    ProviderNeutral,
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

impl ProviderTranscriptDialect {
    /// Resolves the active adapter family from a configured provider kind.
    #[must_use]
    pub fn from_provider_kind(provider_kind: &str) -> Self {
        let normalized = provider_kind.trim().to_ascii_lowercase();
        if normalized.contains("anthropic") || normalized.contains("minimax") {
            Self::AnthropicMessages
        } else if normalized.contains("responses") || normalized.contains("chatgpt") {
            Self::OpenAiResponses
        } else if normalized.contains("openai") || normalized.contains("xai") {
            Self::OpenAiChatCompletions
        } else {
            Self::ProviderNeutral
        }
    }

    const fn requires_complete_tool_pairs(self) -> bool {
        !matches!(self, Self::ProviderNeutral)
    }

    const fn normalized_tool_id_prefix(self) -> &'static str {
        match self {
            Self::AnthropicMessages => "toolu",
            Self::ProviderNeutral | Self::OpenAiChatCompletions | Self::OpenAiResponses => "call",
        }
    }
}

/// One provider-neutral message with references to its authoritative source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTranscriptSourceMessage {
    pub message: ProviderMessage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_tape_refs: Vec<String>,
}

/// Request for a deterministic provider transcript projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTranscriptProjectionRequest {
    pub dialect: ProviderTranscriptDialect,
    pub model_id: String,
    pub projection_epoch: u64,
    pub messages: Vec<ProviderTranscriptSourceMessage>,
}

/// One bounded repair applied only to the provider projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptRepairRecord {
    pub reason_code: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_tape_refs: Vec<String>,
}

/// Replay-visible report for one immutable transcript projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptRepairReport {
    pub schema_version: u16,
    pub reason_code: String,
    pub repairs: Vec<TranscriptRepairRecord>,
}

/// Provider-ready transcript plus deterministic identity and repair evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTranscriptProjectionV1 {
    pub schema_version: u16,
    pub projection_id: String,
    pub projection_sha256: String,
    pub projection_epoch: u64,
    pub dialect: ProviderTranscriptDialect,
    pub model_id_sha256: String,
    pub messages: Vec<ProviderMessage>,
    pub repair_report: TranscriptRepairReport,
}

/// Provenance required before opaque provider reasoning state may be replayed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReasoningReplayRequest {
    pub opt_in: bool,
    pub source_provider_id: String,
    pub target_provider_id: String,
    pub source_model_id: String,
    pub target_model_id: String,
    pub source_auth_profile_id_sha256: String,
    pub target_auth_profile_id_sha256: String,
    pub retention_policy_allows_replay: bool,
}

/// Fail-closed rejection for unsafe opaque reasoning replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProviderReasoningReplayError {
    #[error("provider reasoning replay requires explicit opt-in")]
    OptInRequired,
    #[error("provider reasoning replay requires the same provider")]
    ProviderMismatch,
    #[error("provider reasoning replay requires the same model")]
    ModelMismatch,
    #[error("provider reasoning replay requires the same auth provenance")]
    AuthProvenanceMismatch,
    #[error("provider reasoning replay is denied by retention policy")]
    RetentionPolicyDenied,
}

/// Projection construction failure.
#[derive(Debug, Error)]
pub enum ProviderTranscriptProjectionError {
    #[error("provider transcript projection could not be serialized: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
struct ToolCallProjection {
    message_index: usize,
    normalized_id: String,
    raw_id: String,
    source_tape_refs: Vec<String>,
}

#[derive(Debug)]
struct RepairOutcome {
    messages: Vec<ProviderMessage>,
    report: TranscriptRepairReport,
}

/// Authorizes opaque reasoning replay only when every privacy invariant matches.
///
/// # Errors
/// Returns a closed rejection when replay is not explicitly enabled, changes
/// provider/model/auth provenance, or conflicts with retention policy.
pub fn authorize_provider_reasoning_replay(
    request: &ProviderReasoningReplayRequest,
) -> Result<(), ProviderReasoningReplayError> {
    if !request.opt_in {
        return Err(ProviderReasoningReplayError::OptInRequired);
    }
    if request.source_provider_id != request.target_provider_id {
        return Err(ProviderReasoningReplayError::ProviderMismatch);
    }
    if request.source_model_id != request.target_model_id {
        return Err(ProviderReasoningReplayError::ModelMismatch);
    }
    if request.source_auth_profile_id_sha256 != request.target_auth_profile_id_sha256 {
        return Err(ProviderReasoningReplayError::AuthProvenanceMismatch);
    }
    if !request.retention_policy_allows_replay {
        return Err(ProviderReasoningReplayError::RetentionPolicyDenied);
    }
    Ok(())
}

/// Builds the deterministic, provider-specific projection without mutating its
/// authoritative source messages.
///
/// # Errors
/// Returns [`ProviderTranscriptProjectionError::Serialize`] only when the
/// bounded projection cannot be serialized for hashing.
pub fn project_provider_transcript(
    request: ProviderTranscriptProjectionRequest,
) -> Result<ProviderTranscriptProjectionV1, ProviderTranscriptProjectionError> {
    let dialect = request.dialect;
    let projection_epoch = request.projection_epoch;
    let model_id_sha256 = sha256_hex(request.model_id.as_bytes());
    let outcome = repair_provider_messages(request.messages, dialect);
    let hash_input = serde_json::to_vec(&(
        1_u16,
        projection_epoch,
        dialect,
        model_id_sha256.as_str(),
        &outcome.messages,
        &outcome.report,
    ))?;
    let projection_sha256 = sha256_hex(hash_input.as_slice());
    let projection_id = format!("provider_projection_v1:{}", &projection_sha256[..26]);
    Ok(ProviderTranscriptProjectionV1 {
        schema_version: 1,
        projection_id,
        projection_sha256,
        projection_epoch,
        dialect,
        model_id_sha256,
        messages: outcome.messages,
        repair_report: outcome.report,
    })
}

/// Applies the same projector used by durable replay directly before a wire
/// adapter serializes a live request.
#[must_use]
pub fn project_provider_request_messages(
    messages: Vec<ProviderMessage>,
    dialect: ProviderTranscriptDialect,
) -> Vec<ProviderMessage> {
    let sources = messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| ProviderTranscriptSourceMessage {
            message,
            source_tape_refs: vec![format!("request_message:{index}")],
        })
        .collect();
    repair_provider_messages(sources, dialect).messages
}

fn repair_provider_messages(
    mut source_messages: Vec<ProviderTranscriptSourceMessage>,
    dialect: ProviderTranscriptDialect,
) -> RepairOutcome {
    let mut repairs = Vec::new();
    if source_messages.len() > MAX_PROVIDER_TRANSCRIPT_MESSAGES {
        let dropped = source_messages.len() - MAX_PROVIDER_TRANSCRIPT_MESSAGES;
        let dropped_refs = source_messages
            .iter()
            .take(dropped)
            .flat_map(|source| source.source_tape_refs.iter().cloned())
            .collect();
        source_messages.drain(0..dropped);
        repairs.push(repair_record("provider.transcript.message_limit_applied", dropped_refs));
    }

    let tool_calls = normalize_tool_call_ids(&mut source_messages, dialect, &mut repairs);
    let result_indexes_by_raw_id = tool_result_indexes(&source_messages);
    let (selected_results, duplicate_results) = pair_tool_results(
        &tool_calls,
        &result_indexes_by_raw_id,
        source_messages.as_slice(),
        &mut repairs,
    );
    let call_message_indexes =
        tool_calls.iter().map(|call| call.message_index).collect::<BTreeSet<_>>();
    let matched_result_indexes = selected_results.values().copied().collect::<BTreeSet<_>>();
    let mut projected = Vec::new();

    for (message_index, source) in source_messages.iter().enumerate() {
        if call_message_indexes.contains(&message_index) {
            projected.push(source.message.clone());
            for (call_index, call) in tool_calls
                .iter()
                .enumerate()
                .filter(|(_, call)| call.message_index == message_index)
            {
                if let Some(result_index) = selected_results.get(&call_index).copied() {
                    let result_source = &source_messages[result_index];
                    if tool_result_is_late(message_index, result_index, source_messages.as_slice())
                    {
                        repairs.push(repair_record(
                            "provider.transcript.late_result_moved",
                            combined_refs(call, result_source),
                        ));
                    }
                    projected.push(project_tool_result(
                        result_source,
                        call.normalized_id.as_str(),
                        &mut repairs,
                    ));
                } else if dialect.requires_complete_tool_pairs() {
                    repairs.push(repair_record(
                        "provider.transcript.missing_result_synthesized",
                        call.source_tape_refs.clone(),
                    ));
                    projected.push(ProviderMessage::tool_result(
                        call.normalized_id.clone(),
                        SYNTHETIC_TOOL_FAILURE,
                    ));
                }
            }
            continue;
        }
        if source.message.role == ProviderMessageRole::Tool {
            if matched_result_indexes.contains(&message_index)
                || duplicate_results.contains(&message_index)
            {
                continue;
            }
            repairs.push(repair_record(
                "provider.transcript.orphan_result_isolated",
                source.source_tape_refs.clone(),
            ));
            projected.push(ProviderMessage::user_text(ORPHAN_TOOL_RESULT_MARKER));
            continue;
        }
        projected.push(source.message.clone());
    }

    RepairOutcome {
        messages: projected,
        report: TranscriptRepairReport {
            schema_version: 1,
            reason_code: if repairs.is_empty() {
                "provider.transcript.valid".to_owned()
            } else {
                "provider.transcript.repaired".to_owned()
            },
            repairs,
        },
    }
}

fn normalize_tool_call_ids(
    messages: &mut [ProviderTranscriptSourceMessage],
    dialect: ProviderTranscriptDialect,
    repairs: &mut Vec<TranscriptRepairRecord>,
) -> Vec<ToolCallProjection> {
    let mut calls = Vec::new();
    let mut used_ids = BTreeSet::new();
    for (message_index, source) in messages.iter_mut().enumerate() {
        if source.message.role != ProviderMessageRole::Assistant {
            continue;
        }
        for tool_call in &mut source.message.tool_calls {
            let raw_id = tool_call.proposal_id.clone();
            let ordinal = calls.len();
            let valid_unique =
                valid_tool_call_id(raw_id.as_str()) && used_ids.insert(raw_id.clone());
            let normalized_id = if valid_unique {
                raw_id.clone()
            } else {
                let generated = normalized_tool_call_id(dialect, raw_id.as_str(), ordinal);
                used_ids.insert(generated.clone());
                repairs.push(repair_record(
                    "provider.transcript.tool_id_normalized",
                    source.source_tape_refs.clone(),
                ));
                generated
            };
            tool_call.proposal_id.clone_from(&normalized_id);
            calls.push(ToolCallProjection {
                message_index,
                normalized_id,
                raw_id,
                source_tape_refs: source.source_tape_refs.clone(),
            });
        }
    }
    calls
}

fn tool_result_indexes(
    messages: &[ProviderTranscriptSourceMessage],
) -> BTreeMap<String, Vec<usize>> {
    let mut indexes = BTreeMap::<String, Vec<usize>>::new();
    for (index, source) in messages.iter().enumerate() {
        if source.message.role == ProviderMessageRole::Tool {
            indexes
                .entry(source.message.tool_call_id.clone().unwrap_or_default())
                .or_default()
                .push(index);
        }
    }
    indexes
}

fn pair_tool_results(
    calls: &[ToolCallProjection],
    result_indexes_by_raw_id: &BTreeMap<String, Vec<usize>>,
    messages: &[ProviderTranscriptSourceMessage],
    repairs: &mut Vec<TranscriptRepairRecord>,
) -> (BTreeMap<usize, usize>, BTreeSet<usize>) {
    let mut selected = BTreeMap::new();
    let mut consumed = BTreeSet::new();
    for (call_index, call) in calls.iter().enumerate() {
        let Some(result_indexes) = result_indexes_by_raw_id.get(call.raw_id.as_str()) else {
            continue;
        };
        if let Some(result_index) = result_indexes
            .iter()
            .copied()
            .find(|index| *index > call.message_index && !consumed.contains(index))
        {
            selected.insert(call_index, result_index);
            consumed.insert(result_index);
        }
    }

    let mut duplicates = BTreeSet::new();
    for call in calls {
        let Some(result_indexes) = result_indexes_by_raw_id.get(call.raw_id.as_str()) else {
            continue;
        };
        for result_index in result_indexes {
            if *result_index > call.message_index && !consumed.contains(result_index) {
                duplicates.insert(*result_index);
            }
        }
    }
    for result_index in &duplicates {
        repairs.push(repair_record(
            "provider.transcript.duplicate_result_dropped",
            messages[*result_index].source_tape_refs.clone(),
        ));
    }
    (selected, duplicates)
}

fn project_tool_result(
    source: &ProviderTranscriptSourceMessage,
    normalized_id: &str,
    repairs: &mut Vec<TranscriptRepairRecord>,
) -> ProviderMessage {
    let raw_text = source.message.text_content();
    let (text, truncated) =
        truncate_chars(raw_text.as_str(), MAX_PROVIDER_TRANSCRIPT_TOOL_RESULT_CHARS);
    if truncated {
        repairs.push(repair_record(
            "provider.transcript.tool_result_truncated",
            source.source_tape_refs.clone(),
        ));
    }
    ProviderMessage::tool_result(normalized_id.to_owned(), text)
}

fn tool_result_is_late(
    call_index: usize,
    result_index: usize,
    messages: &[ProviderTranscriptSourceMessage],
) -> bool {
    messages[call_index + 1..result_index]
        .iter()
        .any(|source| source.message.role != ProviderMessageRole::Tool)
}

fn combined_refs(
    call: &ToolCallProjection,
    result: &ProviderTranscriptSourceMessage,
) -> Vec<String> {
    let mut refs = call.source_tape_refs.clone();
    refs.extend(result.source_tape_refs.iter().cloned());
    refs.sort();
    refs.dedup();
    refs
}

fn repair_record(reason_code: &str, mut source_tape_refs: Vec<String>) -> TranscriptRepairRecord {
    source_tape_refs.sort();
    source_tape_refs.dedup();
    TranscriptRepairRecord { reason_code: reason_code.to_owned(), source_tape_refs }
}

fn valid_tool_call_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw.chars().count() <= MAX_PROVIDER_TOOL_CALL_ID_CHARS
        && raw.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
}

fn normalized_tool_call_id(
    dialect: ProviderTranscriptDialect,
    raw: &str,
    ordinal: usize,
) -> String {
    let digest = sha256_hex(format!("{raw}\0{ordinal}").as_bytes());
    format!("{}_{}", dialect.normalized_tool_id_prefix(), &digest[..24])
}

fn truncate_chars(raw: &str, max_chars: usize) -> (String, bool) {
    if raw.chars().count() <= max_chars {
        return (raw.to_owned(), false);
    }
    let mut truncated = raw.chars().take(max_chars.saturating_sub(1)).collect::<String>();
    truncated.push('…');
    (truncated, true)
}

fn sha256_hex(input: &[u8]) -> String {
    hex::encode(Sha256::digest(input))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{ProviderMessageContentPart, ProviderMessageToolCall};

    use super::*;

    fn source(message: ProviderMessage, reference: &str) -> ProviderTranscriptSourceMessage {
        ProviderTranscriptSourceMessage { message, source_tape_refs: vec![reference.to_owned()] }
    }

    fn tool_call(id: &str) -> ProviderMessage {
        ProviderMessage {
            role: ProviderMessageRole::Assistant,
            content: Vec::new(),
            name: None,
            tool_call_id: None,
            tool_calls: vec![ProviderMessageToolCall {
                proposal_id: id.to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_json: json!({"text": "hello"}),
            }],
        }
    }

    fn project(messages: Vec<ProviderTranscriptSourceMessage>) -> ProviderTranscriptProjectionV1 {
        project_provider_transcript(ProviderTranscriptProjectionRequest {
            dialect: ProviderTranscriptDialect::AnthropicMessages,
            model_id: "claude-test".to_owned(),
            projection_epoch: 7,
            messages,
        })
        .expect("projection should serialize")
    }

    #[test]
    fn repairs_missing_duplicate_orphan_and_late_results_without_mutating_source() {
        let messages = vec![
            source(tool_call("missing"), "tape_seq:1"),
            source(tool_call("late"), "tape_seq:2"),
            source(ProviderMessage::user_text("intervening"), "tape_seq:3"),
            source(ProviderMessage::tool_result("late", "late result"), "tape_seq:4"),
            source(tool_call("duplicate"), "tape_seq:5"),
            source(ProviderMessage::tool_result("duplicate", "first"), "tape_seq:6"),
            source(ProviderMessage::tool_result("duplicate", "second"), "tape_seq:7"),
            source(ProviderMessage::tool_result("orphan", "orphan"), "tape_seq:8"),
        ];
        let original = messages.clone();

        let projection = project(messages);

        assert_eq!(original[0].message.tool_calls[0].proposal_id, "missing");
        let reason_codes = projection
            .repair_report
            .repairs
            .iter()
            .map(|repair| repair.reason_code.as_str())
            .collect::<BTreeSet<_>>();
        assert!(reason_codes.contains("provider.transcript.missing_result_synthesized"));
        assert!(reason_codes.contains("provider.transcript.late_result_moved"));
        assert!(reason_codes.contains("provider.transcript.duplicate_result_dropped"));
        assert!(reason_codes.contains("provider.transcript.orphan_result_isolated"));
        let missing_index = projection.messages.iter().position(|message| {
            message.role == ProviderMessageRole::Assistant
                && message.tool_calls.first().is_some_and(|call| call.proposal_id == "missing")
        });
        let missing_index = missing_index.expect("missing call should remain");
        assert_eq!(projection.messages[missing_index + 1].text_content(), SYNTHETIC_TOOL_FAILURE);
        assert!(!projection.messages[missing_index + 1].text_content().contains("success\":true"));
    }

    #[test]
    fn malformed_tool_ids_are_normalized_and_results_follow_the_new_id() {
        let projection = project(vec![
            source(tool_call("bad id/with spaces"), "tape_seq:1"),
            source(ProviderMessage::tool_result("bad id/with spaces", "ok"), "tape_seq:2"),
        ]);

        let normalized = &projection.messages[0].tool_calls[0].proposal_id;
        assert!(normalized.starts_with("toolu_"));
        assert!(valid_tool_call_id(normalized));
        assert_eq!(projection.messages[1].tool_call_id.as_deref(), Some(normalized.as_str()));
    }

    #[test]
    fn legacy_text_only_history_is_stable_and_repair_free() {
        let projection = project(vec![
            source(ProviderMessage::user_text("hello"), "tape_seq:1"),
            source(
                ProviderMessage {
                    role: ProviderMessageRole::Assistant,
                    content: vec![ProviderMessageContentPart::text("hi")],
                    name: None,
                    tool_call_id: None,
                    tool_calls: Vec::new(),
                },
                "tape_seq:2",
            ),
        ]);

        assert_eq!(projection.messages.len(), 2);
        assert_eq!(projection.repair_report.reason_code, "provider.transcript.valid");
        assert!(projection.repair_report.repairs.is_empty());
    }

    #[test]
    fn projection_identity_changes_with_epoch_and_model() {
        let source_messages = vec![source(ProviderMessage::user_text("hello"), "tape_seq:1")];
        let first = project_provider_transcript(ProviderTranscriptProjectionRequest {
            dialect: ProviderTranscriptDialect::OpenAiChatCompletions,
            model_id: "gpt-a".to_owned(),
            projection_epoch: 1,
            messages: source_messages.clone(),
        })
        .expect("projection should serialize");
        let new_epoch = project_provider_transcript(ProviderTranscriptProjectionRequest {
            dialect: ProviderTranscriptDialect::OpenAiChatCompletions,
            model_id: "gpt-a".to_owned(),
            projection_epoch: 2,
            messages: source_messages.clone(),
        })
        .expect("projection should serialize");
        let new_model = project_provider_transcript(ProviderTranscriptProjectionRequest {
            dialect: ProviderTranscriptDialect::OpenAiChatCompletions,
            model_id: "gpt-b".to_owned(),
            projection_epoch: 1,
            messages: source_messages,
        })
        .expect("projection should serialize");

        assert_ne!(first.projection_id, new_epoch.projection_id);
        assert_ne!(first.projection_id, new_model.projection_id);
    }

    #[test]
    fn reasoning_replay_rejects_different_auth_provenance() {
        let request = ProviderReasoningReplayRequest {
            opt_in: true,
            source_provider_id: "openai".to_owned(),
            target_provider_id: "openai".to_owned(),
            source_model_id: "gpt-5".to_owned(),
            target_model_id: "gpt-5".to_owned(),
            source_auth_profile_id_sha256: "a".repeat(64),
            target_auth_profile_id_sha256: "b".repeat(64),
            retention_policy_allows_replay: true,
        };

        assert_eq!(
            authorize_provider_reasoning_replay(&request),
            Err(ProviderReasoningReplayError::AuthProvenanceMismatch)
        );
    }
}
