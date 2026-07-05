//! Tool-call assembly from canonical provider stream events.
//!
//! This module is deliberately provider-neutral and side-effect free: it
//! reconstructs fragmented tool-call names and argument bytes, classifies
//! malformed sequences, and returns hash-only repair metadata. Runtime code
//! still owns catalog snapshots, schema validation, approvals, and execution.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::streaming::ProviderCanonicalEvent;

/// Schema version stamped on assembler reports.
pub const TOOL_CALL_ASSEMBLER_SCHEMA_VERSION: u16 = 1;
/// Event name used by daemon tape/journal projections.
pub const TOOL_CALL_ASSEMBLER_AUDIT_EVENT: &str = "tool_call.assembler.report";
/// Default argument byte cap before a repair candidate is rejected.
pub const DEFAULT_ASSEMBLED_TOOL_ARGUMENT_LIMIT_BYTES: usize = 256 * 1024;

/// Final decision for one assembled tool call.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssembledToolCallStatus {
    ExecutionReady,
    NeedsModelSelfCorrection,
    FailClosed,
}

impl AssembledToolCallStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionReady => "execution_ready",
            Self::NeedsModelSelfCorrection => "needs_model_self_correction",
            Self::FailClosed => "fail_closed",
        }
    }
}

/// Repair class applied to a tool name or argument payload.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallRepairClass {
    EmptyArgumentsObject,
    TrailingComma,
    FencedPayload,
    StringifiedJson,
    LegacyFunctionCall,
    AliasPromoted,
}

impl ToolCallRepairClass {
    #[must_use]
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::EmptyArgumentsObject => "tool_call.repair.empty_arguments_object",
            Self::TrailingComma => "tool_call.repair.trailing_comma",
            Self::FencedPayload => "tool_call.repair.fenced_payload",
            Self::StringifiedJson => "tool_call.repair.stringified_json",
            Self::LegacyFunctionCall => "tool_call.repair.legacy_function_call",
            Self::AliasPromoted => "tool_call.repair.alias_promoted",
        }
    }
}

/// Hash-only metadata for one repair step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallRepairStep {
    pub class: ToolCallRepairClass,
    pub reason_code: String,
    pub original_sha256: String,
    pub repaired_sha256: String,
}

/// One assembled tool call or rejected candidate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssembledToolCall {
    pub schema_version: u16,
    pub status: AssembledToolCallStatus,
    pub reason_code: String,
    pub call_index: u32,
    pub provider_call_id: Option<String>,
    pub proposal_id: String,
    pub tool_name: String,
    pub input_json: Option<Value>,
    pub original_arguments_sha256: String,
    pub repaired_arguments_sha256: Option<String>,
    pub repair_steps: Vec<ToolCallRepairStep>,
}

/// Sequence-level diagnostic for the assembler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallAssemblyDiagnostic {
    pub event_type: String,
    pub reason_code: String,
    pub call_index: Option<u32>,
    pub status: AssembledToolCallStatus,
}

/// Complete report for one canonical provider stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallAssemblyReport {
    pub schema_version: u16,
    pub event_type: String,
    pub redaction_level: String,
    pub tool_calls: Vec<AssembledToolCall>,
    pub diagnostics: Vec<ToolCallAssemblyDiagnostic>,
    pub fail_closed: bool,
}

/// Catalog context needed for conservative name and empty-argument repair.
#[derive(Debug, Clone)]
pub struct ToolCallAssemblyPolicy {
    visible_tool_names: BTreeSet<String>,
    empty_object_allowed_tools: BTreeSet<String>,
    max_arguments_bytes: usize,
}

impl ToolCallAssemblyPolicy {
    /// Builds a policy from the current model-visible catalog snapshot.
    #[must_use]
    pub fn new<I, S>(visible_tool_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let visible_tool_names = visible_tool_names
            .into_iter()
            .map(|name| name.as_ref().trim().to_owned())
            .filter(|name| !name.is_empty())
            .collect::<BTreeSet<_>>();
        Self {
            empty_object_allowed_tools: visible_tool_names.clone(),
            visible_tool_names,
            max_arguments_bytes: DEFAULT_ASSEMBLED_TOOL_ARGUMENT_LIMIT_BYTES,
        }
    }

    /// Overrides the argument byte cap.
    #[must_use]
    pub fn with_max_arguments_bytes(mut self, max_arguments_bytes: usize) -> Self {
        self.max_arguments_bytes = max_arguments_bytes.max(1);
        self
    }

    /// Restricts empty-string-to-object repair to tools known to accept `{}`.
    #[must_use]
    pub fn with_empty_object_allowed_tools<I, S>(mut self, tool_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.empty_object_allowed_tools = tool_names
            .into_iter()
            .map(|name| name.as_ref().trim().to_owned())
            .filter(|name| !name.is_empty())
            .collect();
        self
    }

    fn visible_tool_names(&self) -> &BTreeSet<String> {
        &self.visible_tool_names
    }
}

#[derive(Debug, Clone, Default)]
struct PendingToolCall {
    provider_call_id: Option<String>,
    name: String,
    arguments: String,
    started: bool,
    ended: bool,
}

#[derive(Debug, Clone)]
struct RepairOutcome {
    value: Option<Value>,
    reason_code: String,
    status: AssembledToolCallStatus,
    repaired_sha256: Option<String>,
    steps: Vec<ToolCallRepairStep>,
}

/// Assembles tool calls from canonical stream events without executing them.
#[must_use]
pub fn assemble_canonical_tool_calls(
    events: &[ProviderCanonicalEvent],
    policy: &ToolCallAssemblyPolicy,
) -> ToolCallAssemblyReport {
    let mut pending = BTreeMap::<u32, PendingToolCall>::new();
    let mut diagnostics = Vec::new();

    for event in events {
        match event {
            ProviderCanonicalEvent::ToolCallStart { index, provider_call_id } => {
                let call = pending.entry(*index).or_default();
                if call.started {
                    diagnostics.push(diagnostic(
                        "tool_call.assembler.duplicated_start",
                        Some(*index),
                        AssembledToolCallStatus::FailClosed,
                    ));
                }
                call.provider_call_id = provider_call_id.clone();
                call.started = true;
            }
            ProviderCanonicalEvent::ToolCallNameDelta { index, name_delta } => {
                let call = pending.entry(*index).or_default();
                if !call.started {
                    diagnostics.push(diagnostic(
                        "tool_call.assembler.name_delta_without_start",
                        Some(*index),
                        AssembledToolCallStatus::FailClosed,
                    ));
                }
                call.name.push_str(name_delta);
            }
            ProviderCanonicalEvent::ToolCallArgumentsDelta { index, arguments_delta } => {
                let call = pending.entry(*index).or_default();
                if !call.started {
                    diagnostics.push(diagnostic(
                        "tool_call.assembler.arguments_delta_without_start",
                        Some(*index),
                        AssembledToolCallStatus::FailClosed,
                    ));
                }
                call.arguments.push_str(arguments_delta);
            }
            ProviderCanonicalEvent::ToolCallEnd { index } => {
                let call = pending.entry(*index).or_default();
                if !call.started {
                    diagnostics.push(diagnostic(
                        "tool_call.assembler.end_without_start",
                        Some(*index),
                        AssembledToolCallStatus::FailClosed,
                    ));
                }
                if call.ended {
                    diagnostics.push(diagnostic(
                        "tool_call.assembler.duplicated_end",
                        Some(*index),
                        AssembledToolCallStatus::FailClosed,
                    ));
                }
                call.ended = true;
            }
            ProviderCanonicalEvent::FinishReason { .. }
            | ProviderCanonicalEvent::StreamError { .. } => {
                for (index, call) in &pending {
                    if call.started && !call.ended {
                        diagnostics.push(diagnostic(
                            "tool_call.assembler.incomplete_at_terminal",
                            Some(*index),
                            AssembledToolCallStatus::FailClosed,
                        ));
                    }
                }
            }
            ProviderCanonicalEvent::MessageStart { .. }
            | ProviderCanonicalEvent::ContentDelta { .. }
            | ProviderCanonicalEvent::ReasoningDelta { .. }
            | ProviderCanonicalEvent::UsageUpdate { .. }
            | ProviderCanonicalEvent::ProviderWarning { .. } => {}
        }
    }

    let tool_calls = pending
        .into_iter()
        .map(|(index, call)| assemble_one_tool_call(index, call, policy))
        .collect::<Vec<_>>();
    let fail_closed =
        diagnostics.iter().any(|entry| entry.status == AssembledToolCallStatus::FailClosed)
            || tool_calls
                .iter()
                .any(|tool_call| tool_call.status == AssembledToolCallStatus::FailClosed);

    ToolCallAssemblyReport {
        schema_version: TOOL_CALL_ASSEMBLER_SCHEMA_VERSION,
        event_type: TOOL_CALL_ASSEMBLER_AUDIT_EVENT.to_owned(),
        redaction_level: "hash_only".to_owned(),
        tool_calls,
        diagnostics,
        fail_closed,
    }
}

fn assemble_one_tool_call(
    index: u32,
    call: PendingToolCall,
    policy: &ToolCallAssemblyPolicy,
) -> AssembledToolCall {
    let original_arguments_sha256 = sha256_hex(call.arguments.as_bytes());
    let (tool_name, mut name_steps, name_status, name_reason_code) =
        repair_tool_name(call.name.trim(), policy.visible_tool_names());
    let argument_repair = repair_tool_arguments(
        call.arguments.as_str(),
        tool_name.as_str(),
        policy,
        original_arguments_sha256.as_str(),
    );
    name_steps.extend(argument_repair.steps);
    let status = if !call.started
        || !call.ended
        || name_status == AssembledToolCallStatus::FailClosed
        || argument_repair.status == AssembledToolCallStatus::FailClosed
    {
        AssembledToolCallStatus::FailClosed
    } else if name_status == AssembledToolCallStatus::NeedsModelSelfCorrection
        || argument_repair.status == AssembledToolCallStatus::NeedsModelSelfCorrection
    {
        AssembledToolCallStatus::NeedsModelSelfCorrection
    } else {
        AssembledToolCallStatus::ExecutionReady
    };
    let reason_code = if !call.started {
        "tool_call.assembler.missing_start"
    } else if !call.ended {
        "tool_call.assembler.incomplete"
    } else if status == AssembledToolCallStatus::ExecutionReady {
        "tool_call.assembler.execution_ready"
    } else if name_status != AssembledToolCallStatus::ExecutionReady {
        name_reason_code.as_str()
    } else {
        argument_repair.reason_code.as_str()
    };
    let proposal_id = call.provider_call_id.clone().unwrap_or_else(|| {
        stable_proposal_id(index, tool_name.as_str(), original_arguments_sha256.as_str())
    });

    AssembledToolCall {
        schema_version: TOOL_CALL_ASSEMBLER_SCHEMA_VERSION,
        status,
        reason_code: reason_code.to_owned(),
        call_index: index,
        provider_call_id: call.provider_call_id,
        proposal_id,
        tool_name,
        input_json: (status == AssembledToolCallStatus::ExecutionReady)
            .then_some(argument_repair.value)
            .flatten(),
        original_arguments_sha256,
        repaired_arguments_sha256: argument_repair.repaired_sha256,
        repair_steps: name_steps,
    }
}

fn repair_tool_name(
    raw_name: &str,
    visible_tool_names: &BTreeSet<String>,
) -> (String, Vec<ToolCallRepairStep>, AssembledToolCallStatus, String) {
    if visible_tool_names.contains(raw_name) {
        return (
            raw_name.to_owned(),
            Vec::new(),
            AssembledToolCallStatus::ExecutionReady,
            "tool_call.name.exact".to_owned(),
        );
    }
    let normalized_raw = normalize_tool_name(raw_name);
    let matches = visible_tool_names
        .iter()
        .filter(|name| normalize_tool_name(name.as_str()) == normalized_raw)
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        let repaired = matches[0].clone();
        return (
            repaired.clone(),
            vec![repair_step(
                ToolCallRepairClass::AliasPromoted,
                raw_name.as_bytes(),
                repaired.as_bytes(),
            )],
            AssembledToolCallStatus::ExecutionReady,
            ToolCallRepairClass::AliasPromoted.reason_code().to_owned(),
        );
    }
    let fuzzy = unique_fuzzy_tool_match(raw_name, visible_tool_names);
    if let Some(repaired) = fuzzy {
        return (
            repaired.clone(),
            vec![repair_step(
                ToolCallRepairClass::AliasPromoted,
                raw_name.as_bytes(),
                repaired.as_bytes(),
            )],
            AssembledToolCallStatus::ExecutionReady,
            ToolCallRepairClass::AliasPromoted.reason_code().to_owned(),
        );
    }
    (
        raw_name.to_owned(),
        Vec::new(),
        AssembledToolCallStatus::NeedsModelSelfCorrection,
        "tool_call.name.unknown_or_ambiguous".to_owned(),
    )
}

fn repair_tool_arguments(
    raw_arguments: &str,
    tool_name: &str,
    policy: &ToolCallAssemblyPolicy,
    original_sha256: &str,
) -> RepairOutcome {
    let trimmed = raw_arguments.trim();
    if trimmed.len() > policy.max_arguments_bytes {
        return no_value(
            "tool_call.arguments.payload_too_large",
            AssembledToolCallStatus::FailClosed,
        );
    }
    if trimmed.is_empty() {
        if policy.empty_object_allowed_tools.contains(tool_name) {
            let repaired = b"{}";
            return RepairOutcome {
                value: Some(json!({})),
                reason_code: ToolCallRepairClass::EmptyArgumentsObject.reason_code().to_owned(),
                status: AssembledToolCallStatus::ExecutionReady,
                repaired_sha256: Some(sha256_hex(repaired)),
                steps: vec![repair_step(
                    ToolCallRepairClass::EmptyArgumentsObject,
                    raw_arguments.as_bytes(),
                    repaired,
                )],
            };
        }
        return no_value(
            "tool_call.arguments.empty_not_allowed",
            AssembledToolCallStatus::NeedsModelSelfCorrection,
        );
    }
    if looks_truncated_json(trimmed) {
        return no_value(
            "tool_call.arguments.truncated_fail_closed",
            AssembledToolCallStatus::FailClosed,
        );
    }

    let mut candidate = trimmed.to_owned();
    let mut steps = Vec::new();
    if let Some(unfenced) = strip_json_fence(candidate.as_str()) {
        steps.push(repair_step(
            ToolCallRepairClass::FencedPayload,
            candidate.as_bytes(),
            unfenced.as_bytes(),
        ));
        candidate = unfenced;
    }
    if let Some(inner) = strip_legacy_function_call(candidate.as_str()) {
        steps.push(repair_step(
            ToolCallRepairClass::LegacyFunctionCall,
            candidate.as_bytes(),
            inner.as_bytes(),
        ));
        candidate = inner;
    }

    match parse_json_object(candidate.as_str()) {
        Some(value) => return repaired_value(value, original_sha256, steps),
        None => {
            if let Some(without_trailing_comma) = remove_trailing_json_commas(candidate.as_str()) {
                steps.push(repair_step(
                    ToolCallRepairClass::TrailingComma,
                    candidate.as_bytes(),
                    without_trailing_comma.as_bytes(),
                ));
                candidate = without_trailing_comma;
                if let Some(value) = parse_json_object(candidate.as_str()) {
                    return repaired_value(value, original_sha256, steps);
                }
            }
        }
    }

    if let Ok(Value::String(stringified)) = serde_json::from_str::<Value>(candidate.as_str()) {
        if let Some(value) = parse_json_object(stringified.as_str()) {
            steps.push(repair_step(
                ToolCallRepairClass::StringifiedJson,
                candidate.as_bytes(),
                stringified.as_bytes(),
            ));
            return repaired_value(value, original_sha256, steps);
        }
    }

    no_value(
        "tool_call.arguments.invalid_json_retry",
        AssembledToolCallStatus::NeedsModelSelfCorrection,
    )
}

fn repaired_value(
    value: Value,
    original_sha256: &str,
    steps: Vec<ToolCallRepairStep>,
) -> RepairOutcome {
    let repaired_bytes = canonical_json_bytes(&value);
    let repaired_sha256 = sha256_hex(repaired_bytes.as_slice());
    RepairOutcome {
        value: Some(value),
        reason_code: if steps.is_empty() {
            "tool_call.arguments.valid_json".to_owned()
        } else {
            "tool_call.arguments.repaired".to_owned()
        },
        status: AssembledToolCallStatus::ExecutionReady,
        repaired_sha256: (repaired_sha256 != original_sha256).then_some(repaired_sha256),
        steps,
    }
}

fn no_value(reason_code: &str, status: AssembledToolCallStatus) -> RepairOutcome {
    RepairOutcome {
        value: None,
        reason_code: reason_code.to_owned(),
        status,
        repaired_sha256: None,
        steps: Vec::new(),
    }
}

fn diagnostic(
    reason_code: &str,
    call_index: Option<u32>,
    status: AssembledToolCallStatus,
) -> ToolCallAssemblyDiagnostic {
    ToolCallAssemblyDiagnostic {
        event_type: TOOL_CALL_ASSEMBLER_AUDIT_EVENT.to_owned(),
        reason_code: reason_code.to_owned(),
        call_index,
        status,
    }
}

fn normalize_tool_name(name: &str) -> String {
    name.chars().filter(|ch| ch.is_ascii_alphanumeric()).flat_map(char::to_lowercase).collect()
}

fn unique_fuzzy_tool_match(
    raw_name: &str,
    visible_tool_names: &BTreeSet<String>,
) -> Option<String> {
    let normalized_raw = normalize_tool_name(raw_name);
    let mut matches = visible_tool_names
        .iter()
        .filter_map(|candidate| {
            let distance = bounded_edit_distance(
                normalized_raw.as_str(),
                normalize_tool_name(candidate).as_str(),
                2,
            )?;
            (distance <= 2).then_some((distance, candidate.clone()))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    match matches.as_slice() {
        [(_, tool_name)] => Some(tool_name.clone()),
        [(distance, first), (second_distance, _), ..] if distance < second_distance => {
            Some(first.clone())
        }
        _ => None,
    }
}

fn bounded_edit_distance(left: &str, right: &str, max_distance: usize) -> Option<usize> {
    if left.len().abs_diff(right.len()) > max_distance {
        return None;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        let mut row_min = current[0];
        for (right_index, right_char) in right.chars().enumerate() {
            let replace_cost = usize::from(left_char != right_char);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + replace_cost);
            row_min = row_min.min(current[right_index + 1]);
        }
        if row_min > max_distance {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()].le(&max_distance).then_some(previous[right.len()])
}

fn looks_truncated_json(input: &str) -> bool {
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    for byte in input.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth = depth.saturating_add(1),
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    in_string || depth > 0
}

fn strip_json_fence(input: &str) -> Option<String> {
    let trimmed = input.trim();
    let body = trimmed.strip_prefix("```")?;
    let body = body.strip_suffix("```")?.trim();
    let body = body.strip_prefix("json").unwrap_or(body).trim();
    (!body.is_empty()).then(|| body.to_owned())
}

fn strip_legacy_function_call(input: &str) -> Option<String> {
    let trimmed = input.trim();
    let open = trimmed.find('(')?;
    let close = trimmed.rfind(')')?;
    if close <= open || !trimmed[..open].chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }
    let body = trimmed[open + 1..close].trim();
    body.starts_with('{').then(|| body.to_owned())
}

fn remove_trailing_json_commas(input: &str) -> Option<String> {
    let repaired = input.replace(",}", "}").replace(",]", "]");
    (repaired != input).then_some(repaired)
}

fn parse_json_object(input: &str) -> Option<Value> {
    let value = serde_json::from_str::<Value>(input).ok()?;
    value.is_object().then_some(value)
}

fn repair_step(class: ToolCallRepairClass, original: &[u8], repaired: &[u8]) -> ToolCallRepairStep {
    ToolCallRepairStep {
        class,
        reason_code: class.reason_code().to_owned(),
        original_sha256: sha256_hex(original),
        repaired_sha256: sha256_hex(repaired),
    }
}

fn stable_proposal_id(index: u32, tool_name: &str, arguments_sha256: &str) -> String {
    let seed = format!("{index}:{tool_name}:{arguments_sha256}");
    let digest = sha256_hex(seed.as_bytes());
    format!("tool_call_{}", &digest[..16])
}

fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut sorted = value.clone();
    sort_json_value(&mut sorted);
    serde_json::to_vec(&sorted).unwrap_or_else(|_| b"null".to_vec())
}

fn sort_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ToolCallAssemblyPolicy {
        ToolCallAssemblyPolicy::new(["palyra.fs.read", "palyra.process.run"])
            .with_empty_object_allowed_tools(["palyra.fs.read"])
    }

    #[test]
    fn assembles_fragmented_arguments_deterministically() {
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
                arguments_delta: r#"{"pa"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"th":"Cargo.toml"}"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy());

        assert!(!report.fail_closed);
        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::ExecutionReady);
        assert_eq!(report.tool_calls[0].input_json, Some(json!({"path": "Cargo.toml"})));
        assert_eq!(report.tool_calls[0].proposal_id, "call_1");
    }

    #[test]
    fn trailing_comma_arguments_are_repaired() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart { index: 0, provider_call_id: None },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.read".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml",}"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy());

        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::ExecutionReady);
        assert!(report.tool_calls[0]
            .repair_steps
            .iter()
            .any(|step| { step.class == ToolCallRepairClass::TrailingComma }));
    }

    #[test]
    fn truncated_json_fails_closed() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart { index: 0, provider_call_id: None },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.read".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy());

        assert!(report.fail_closed);
        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::FailClosed);
        assert!(report.tool_calls[0].input_json.is_none());
    }

    #[test]
    fn fuzzy_name_repair_requires_unique_match() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart { index: 0, provider_call_id: None },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.red".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml"}"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy());

        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::ExecutionReady);
        assert_eq!(report.tool_calls[0].tool_name, "palyra.fs.read");
    }

    #[test]
    fn ambiguous_name_is_not_repaired() {
        let policy = ToolCallAssemblyPolicy::new(["palyra.fs.read", "palyra.fs.reap"]);
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart { index: 0, provider_call_id: None },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.rea".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallArgumentsDelta {
                index: 0,
                arguments_delta: r#"{"path":"Cargo.toml"}"#.to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy);

        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::NeedsModelSelfCorrection);
        assert!(report.tool_calls[0].input_json.is_none());
    }

    #[test]
    fn empty_arguments_can_be_converted_to_empty_object() {
        let events = vec![
            ProviderCanonicalEvent::ToolCallStart { index: 0, provider_call_id: None },
            ProviderCanonicalEvent::ToolCallNameDelta {
                index: 0,
                name_delta: "palyra.fs.read".to_owned(),
            },
            ProviderCanonicalEvent::ToolCallEnd { index: 0 },
        ];

        let report = assemble_canonical_tool_calls(events.as_slice(), &policy());

        assert_eq!(report.tool_calls[0].status, AssembledToolCallStatus::ExecutionReady);
        assert_eq!(report.tool_calls[0].input_json, Some(json!({})));
    }
}
