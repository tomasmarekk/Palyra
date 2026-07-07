//! QA Lab mock-provider fixtures and deterministic projection.
//!
//! Fixtures describe provider turns as YAML, then compile into provider-neutral
//! stream events and bounded turn outputs consumed by the daemon runtime.

use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    errors::provider_failure_classification, invalid_response_classification,
    provider_events_from_output, retry_provider_classification, ProviderError, ProviderEvent,
    ProviderFailureAction, ProviderFailureClass, ProviderFinishReason, ProviderMessageRole,
    ProviderRawProviderRefs, ProviderRequest, ProviderStreamAccumulator, ProviderStreamEvent,
    ProviderTurnOutput,
};

/// Current QA mock-provider fixture schema version.
pub const QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION: u32 = 1;

/// Stable format label embedded in fixture schema snapshots.
pub const QA_MOCK_PROVIDER_FIXTURE_FORMAT: &str = "palyra-qa-mock-provider-fixture";

const BEHAVIOR_KIND_VALUES: &[&str] = &[
    "text",
    "tool_calls",
    "empty",
    "context_overflow",
    "malformed_output",
    "malformed_tool_args",
    "stream_error",
    "approval_required",
];
const FINISH_REASON_VALUES: &[&str] =
    &["stop", "length", "tool_calls", "content_filter", "cancelled", "error", "unknown"];

/// Parsed and validated QA mock-provider fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderFixture {
    /// Version of the mock-provider fixture schema.
    pub schema_version: u32,
    /// Stable fixture identifier.
    pub id: String,
    /// Ordered turns; the first matching turn is selected for a request.
    pub turns: Vec<QaMockProviderTurn>,
}

/// One deterministic provider turn from a QA fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderTurn {
    /// Stable turn identifier scoped to one fixture.
    pub id: String,
    /// Request selector used to choose this turn.
    pub matcher: QaMockProviderTurnMatcher,
    /// Provider behavior emitted by this turn.
    pub behavior: QaMockProviderBehavior,
}

impl QaMockProviderTurn {
    /// Returns true when this turn applies to `request`.
    #[must_use]
    pub fn matches_request(&self, request: &ProviderRequest) -> bool {
        self.matcher.matches_request(request)
    }
}

/// Request selector for a fixture turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderTurnMatcher {
    /// Required case-insensitive substrings in the visible request text.
    pub prompt_contains: Vec<String>,
    /// Optional tool-call id that must already have a tool result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_result_call_id: Option<String>,
}

impl QaMockProviderTurnMatcher {
    fn matches_request(&self, request: &ProviderRequest) -> bool {
        let text = searchable_request_text(request);
        let prompt_matches = self
            .prompt_contains
            .iter()
            .all(|needle| text.contains(needle.trim().to_ascii_lowercase().as_str()));
        let tool_result_matches = self
            .tool_result_call_id
            .as_deref()
            .is_none_or(|tool_call_id| request_has_tool_result(request, tool_call_id));
        prompt_matches && tool_result_matches
    }
}

/// Fixture behavior kind for one mock-provider turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaMockProviderBehaviorKind {
    Text,
    ToolCalls,
    Empty,
    ContextOverflow,
    MalformedOutput,
    MalformedToolArgs,
    StreamError,
    ApprovalRequired,
}

impl QaMockProviderBehaviorKind {
    /// Returns the fixture string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::ToolCalls => "tool_calls",
            Self::Empty => "empty",
            Self::ContextOverflow => "context_overflow",
            Self::MalformedOutput => "malformed_output",
            Self::MalformedToolArgs => "malformed_tool_args",
            Self::StreamError => "stream_error",
            Self::ApprovalRequired => "approval_required",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "tool_calls" => Some(Self::ToolCalls),
            "empty" => Some(Self::Empty),
            "context_overflow" => Some(Self::ContextOverflow),
            "malformed_output" => Some(Self::MalformedOutput),
            "malformed_tool_args" => Some(Self::MalformedToolArgs),
            "stream_error" => Some(Self::StreamError),
            "approval_required" => Some(Self::ApprovalRequired),
            _ => None,
        }
    }
}

/// Provider behavior emitted by a mock turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderBehavior {
    /// Behavior class.
    pub kind: QaMockProviderBehaviorKind,
    /// Optional complete text response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Optional streamed text deltas. When present, these drive stream output.
    pub deltas: Vec<String>,
    /// Tool proposals emitted by the turn.
    pub tool_calls: Vec<QaMockProviderToolCall>,
    /// Optional explicit finish reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<ProviderFinishReason>,
    /// Optional error text for negative provider behaviors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// Optional prompt-token fixture override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u64>,
    /// Optional completion-token fixture override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u64>,
}

/// Tool call proposed by a mock turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderToolCall {
    /// Stable proposal id.
    pub proposal_id: String,
    /// Palyra tool name.
    pub tool_name: String,
    /// Valid JSON arguments passed to the tool.
    pub input_json: Value,
}

/// Validation issue severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaMockProviderIssueSeverity {
    Error,
}

/// One fixture validation issue with a JSONPath-style location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderFixtureIssue {
    /// Issue severity.
    pub severity: QaMockProviderIssueSeverity,
    /// Stable issue code for automation.
    pub code: String,
    /// JSONPath-style path into the YAML fixture.
    pub path: String,
    /// Human-readable issue message.
    pub message: String,
    /// Operator action that fixes the issue.
    pub recovery_hint: String,
}

/// Collection of fixture validation issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaMockProviderValidationError {
    issues: Vec<QaMockProviderFixtureIssue>,
}

impl QaMockProviderValidationError {
    /// Creates a validation error from one or more issues.
    ///
    /// # Panics
    /// Panics when called with an empty issue list. Empty issue lists represent
    /// a successful validation and should not be converted into an error.
    #[must_use]
    pub fn new(issues: Vec<QaMockProviderFixtureIssue>) -> Self {
        assert!(!issues.is_empty(), "validation errors require at least one issue");
        Self { issues }
    }

    /// Returns all collected validation issues.
    #[must_use]
    pub fn issues(&self) -> &[QaMockProviderFixtureIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for QaMockProviderValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let first =
            self.issues.first().expect("validation error is constructed with at least one issue");
        write!(formatter, "{} at {}: {}", first.code, first.path, first.message)
    }
}

impl Error for QaMockProviderValidationError {}

/// QA mock-provider fixture parse or validation failure.
#[derive(Debug)]
pub enum QaMockProviderFixtureError {
    /// YAML parsing failed before schema validation could run.
    Parse { source: yaml_serde::Error },
    /// YAML parsed successfully but failed schema validation.
    Invalid(QaMockProviderValidationError),
}

impl QaMockProviderFixtureError {
    /// Returns validation issues when the fixture parsed but failed validation.
    #[must_use]
    pub fn issues(&self) -> Option<&[QaMockProviderFixtureIssue]> {
        match self {
            Self::Parse { .. } => None,
            Self::Invalid(error) => Some(error.issues()),
        }
    }
}

impl fmt::Display for QaMockProviderFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { source } => {
                write!(formatter, "failed to parse QA mock-provider fixture YAML: {source}")
            }
            Self::Invalid(error) => write!(formatter, "invalid QA mock-provider fixture: {error}"),
        }
    }
}

impl Error for QaMockProviderFixtureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse { source } => Some(source),
            Self::Invalid(error) => Some(error),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderFixtureWire {
    schema_version: Option<u32>,
    id: Option<String>,
    turns: Option<Vec<QaMockProviderTurnWire>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderTurnWire {
    id: Option<String>,
    #[serde(default, rename = "match")]
    matcher: Option<QaMockProviderTurnMatcherWire>,
    behavior: Option<QaMockProviderBehaviorWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderTurnMatcherWire {
    #[serde(default)]
    prompt_contains: Vec<String>,
    #[serde(default)]
    tool_result_call_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderBehaviorWire {
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    deltas: Vec<String>,
    #[serde(default)]
    tool_calls: Vec<QaMockProviderToolCallWire>,
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderToolCallWire {
    proposal_id: Option<String>,
    tool_name: Option<String>,
    #[serde(default)]
    input_json: Option<Value>,
}

/// Parses and validates a QA mock-provider fixture from YAML text.
///
/// # Errors
/// Returns [`QaMockProviderFixtureError::Parse`] when YAML cannot be
/// deserialized, or [`QaMockProviderFixtureError::Invalid`] with
/// path-qualified issues when the fixture violates the schema.
pub fn parse_qa_mock_provider_fixture_yaml(
    text: &str,
) -> Result<QaMockProviderFixture, QaMockProviderFixtureError> {
    let wire = yaml_serde::from_str::<QaMockProviderFixtureWire>(text)
        .map_err(|source| QaMockProviderFixtureError::Parse { source })?;
    build_validated_fixture(wire)
}

/// Returns the versioned schema snapshot used by QA mock-provider tooling.
#[must_use]
pub fn qa_mock_provider_fixture_schema_snapshot() -> Value {
    json!({
        "schema_version": QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
        "format": QA_MOCK_PROVIDER_FIXTURE_FORMAT,
        "encoding": "yaml",
        "examples_root": "qa/fixtures",
        "required_sections": [
            "schema_version",
            "id",
            "turns"
        ],
        "behavior_kinds": BEHAVIOR_KIND_VALUES,
        "finish_reasons": FINISH_REASON_VALUES,
        "path_convention": "jsonpath"
    })
}

/// Selects the first fixture turn that matches `request`.
#[must_use]
pub fn qa_mock_provider_turn_for_request<'a>(
    fixture: &'a QaMockProviderFixture,
    request: &ProviderRequest,
) -> Option<&'a QaMockProviderTurn> {
    fixture.turns.iter().find(|turn| turn.matches_request(request))
}

/// Projects one fixture turn into provider stream events.
///
/// # Errors
/// Returns a [`ProviderError`] for negative fixture behaviors such as
/// malformed output, context overflow, or stream failure.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_stream_events_for_turn(
    turn: &QaMockProviderTurn,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
    if let Some(error) = provider_error_for_behavior(turn) {
        return Err(error);
    }

    let model_id = provider_model_id.unwrap_or_else(|| "qa-mock".to_owned());
    let prompt_tokens = turn
        .behavior
        .prompt_tokens
        .unwrap_or_else(|| estimate_fixture_tokens(request.input_text.as_str()));
    let completion_tokens = turn.behavior.completion_tokens.unwrap_or_else(|| {
        let text_tokens = text_segments_for_behavior(&turn.behavior)
            .iter()
            .map(|segment| estimate_fixture_tokens(segment.as_str()))
            .sum::<u64>();
        let tool_tokens = turn
            .behavior
            .tool_calls
            .iter()
            .map(|tool_call| estimate_fixture_tokens(tool_call.input_json.to_string().as_str()))
            .sum::<u64>();
        text_tokens.saturating_add(tool_tokens)
    });
    let mut events = vec![ProviderStreamEvent::Started {
        provider_id: "qa-mock-provider".to_owned(),
        model_id: model_id.clone(),
    }];
    for delta in text_segments_for_behavior(&turn.behavior) {
        events.push(ProviderStreamEvent::Delta { text: delta });
    }
    for tool_call in &turn.behavior.tool_calls {
        events.push(ProviderStreamEvent::ToolDelta {
            proposal_id: tool_call.proposal_id.clone(),
            tool_name: tool_call.tool_name.clone(),
            input_json: tool_call.input_json.clone(),
        });
    }
    events.push(ProviderStreamEvent::UsageDelta {
        prompt_tokens,
        completion_tokens,
        total_tokens: Some(prompt_tokens.saturating_add(completion_tokens)),
        cache_read_tokens: None,
        cache_write_tokens: None,
    });
    events.push(ProviderStreamEvent::Completed {
        finish_reason: finish_reason_for_behavior(&turn.behavior),
        raw_provider_refs: ProviderRawProviderRefs {
            provider_response_id: Some(format!("qa-mock:{}", turn.id)),
            provider_model_id: Some(model_id),
            system_fingerprint: Some(QA_MOCK_PROVIDER_FIXTURE_FORMAT.to_owned()),
            provider_trace_ref: Some(format!("qa_mock_provider:{}", turn.id)),
            stream_spill_ref: None,
        },
    });
    Ok(events)
}

/// Projects one fixture turn into a bounded provider output.
///
/// # Errors
/// Returns a [`ProviderError`] for negative fixture behaviors such as
/// malformed output, context overflow, or stream failure.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_output_for_turn(
    turn: &QaMockProviderTurn,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<ProviderTurnOutput, ProviderError> {
    let model_id = provider_model_id.clone().unwrap_or_else(|| "qa-mock".to_owned());
    let mut accumulator = ProviderStreamAccumulator::new("qa-mock-provider", model_id);
    for event in qa_mock_provider_stream_events_for_turn(turn, request, provider_model_id)? {
        accumulator.apply(event);
    }
    let mut output = accumulator.finalize();
    output.usage.source = "qa_mock_fixture".to_owned();
    Ok(output)
}

/// Projects a fixture turn into consumer-facing provider events.
///
/// # Errors
/// Returns a [`ProviderError`] when the fixture turn represents a provider
/// failure instead of a successful response.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_events_for_turn(
    turn: &QaMockProviderTurn,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<Vec<ProviderEvent>, ProviderError> {
    let output = qa_mock_provider_output_for_turn(turn, request, provider_model_id)?;
    Ok(provider_events_from_output(&output))
}

fn build_validated_fixture(
    wire: QaMockProviderFixtureWire,
) -> Result<QaMockProviderFixture, QaMockProviderFixtureError> {
    let mut issues = Vec::new();
    let schema_version = validate_schema_version(wire.schema_version, &mut issues);
    let id = validate_required_slug(wire.id, "$.id", "fixture id", &mut issues);
    let turns = validate_turns(wire.turns, &mut issues);

    if issues.is_empty() {
        Ok(QaMockProviderFixture {
            schema_version: schema_version.unwrap_or(QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION),
            id: id.unwrap_or_default(),
            turns: turns.unwrap_or_default(),
        })
    } else {
        Err(QaMockProviderFixtureError::Invalid(QaMockProviderValidationError::new(issues)))
    }
}

fn validate_schema_version(
    value: Option<u32>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<u32> {
    match value {
        Some(QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION) => {
            Some(QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION)
        }
        Some(value) => {
            push_issue(
                issues,
                "unsupported_schema_version",
                "$.schema_version",
                format!(
                    "schema_version must be {QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION}, got {value}"
                ),
                "Update the fixture to the supported QA mock-provider schema version.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_schema_version",
                "$.schema_version",
                "schema_version is required",
                "Add the supported QA mock-provider schema version.",
            );
            None
        }
    }
}

fn validate_turns(
    value: Option<Vec<QaMockProviderTurnWire>>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<Vec<QaMockProviderTurn>> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_turns",
            "$.turns",
            "turns section is required",
            "Add at least one mock-provider turn.",
        );
        return None;
    };
    if value.is_empty() {
        push_issue(
            issues,
            "empty_turns",
            "$.turns",
            "turns must contain at least one turn",
            "Add a deterministic provider turn to the fixture.",
        );
        return Some(Vec::new());
    }

    let mut seen_ids = BTreeSet::new();
    let mut turns = Vec::with_capacity(value.len());
    for (index, turn) in value.into_iter().enumerate() {
        let path = format!("$.turns[{index}]");
        let id = validate_required_slug(turn.id, format!("{path}.id").as_str(), "turn id", issues);
        if let Some(id_value) = id.as_ref() {
            if !seen_ids.insert(id_value.clone()) {
                push_issue(
                    issues,
                    "duplicate_turn_id",
                    format!("{path}.id"),
                    format!("turn id '{id_value}' is duplicated"),
                    "Use a unique id for every mock-provider turn.",
                );
            }
        }
        let matcher = validate_matcher(turn.matcher, format!("{path}.match").as_str(), issues);
        let behavior =
            validate_behavior(turn.behavior, format!("{path}.behavior").as_str(), issues);
        if let (Some(id), Some(matcher), Some(behavior)) = (id, matcher, behavior) {
            turns.push(QaMockProviderTurn { id, matcher, behavior });
        }
    }
    Some(turns)
}

fn validate_matcher(
    value: Option<QaMockProviderTurnMatcherWire>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<QaMockProviderTurnMatcher> {
    let value = value.unwrap_or(QaMockProviderTurnMatcherWire {
        prompt_contains: Vec::new(),
        tool_result_call_id: None,
    });
    validate_string_list(
        value.prompt_contains.as_slice(),
        format!("{path}.prompt_contains").as_str(),
        "prompt matcher",
        false,
        issues,
    );
    let tool_result_call_id = validate_optional_nonempty(
        value.tool_result_call_id,
        format!("{path}.tool_result_call_id").as_str(),
        issues,
    );
    Some(QaMockProviderTurnMatcher { prompt_contains: value.prompt_contains, tool_result_call_id })
}

fn validate_behavior(
    value: Option<QaMockProviderBehaviorWire>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<QaMockProviderBehavior> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_behavior",
            path,
            "behavior section is required",
            "Declare the mock-provider behavior for this turn.",
        );
        return None;
    };
    let kind = validate_behavior_kind(value.kind, format!("{path}.kind").as_str(), issues);
    let text = validate_optional_nonempty(value.text, format!("{path}.text").as_str(), issues);
    validate_string_list(
        value.deltas.as_slice(),
        format!("{path}.deltas").as_str(),
        "stream delta",
        false,
        issues,
    );
    let tool_calls =
        validate_tool_calls(value.tool_calls, format!("{path}.tool_calls").as_str(), issues);
    let finish_reason = value.finish_reason.and_then(|finish_reason| {
        validate_finish_reason(finish_reason, format!("{path}.finish_reason").as_str(), issues)
    });
    let error_message = validate_optional_nonempty(
        value.error_message,
        format!("{path}.error_message").as_str(),
        issues,
    );
    validate_behavior_shape(
        path,
        kind,
        text.as_deref(),
        value.deltas.as_slice(),
        tool_calls.as_slice(),
        error_message.as_deref(),
        issues,
    );
    kind.map(|kind| QaMockProviderBehavior {
        kind,
        text,
        deltas: value.deltas,
        tool_calls,
        finish_reason,
        error_message,
        prompt_tokens: value.prompt_tokens,
        completion_tokens: value.completion_tokens,
    })
}

fn validate_behavior_kind(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<QaMockProviderBehaviorKind> {
    let value = validate_required_string(value, path, "behavior kind", issues)?;
    if let Some(kind) = QaMockProviderBehaviorKind::parse(value.as_str()) {
        return Some(kind);
    }
    push_issue(
        issues,
        "unknown_behavior_kind",
        path,
        format!(
            "unknown behavior kind '{value}', expected one of {}",
            BEHAVIOR_KIND_VALUES.join(", ")
        ),
        "Use a supported QA mock-provider behavior kind.",
    );
    None
}

fn validate_finish_reason(
    value: String,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<ProviderFinishReason> {
    let value = validate_required_string(Some(value), path, "finish reason", issues)?;
    match value.as_str() {
        "stop" => Some(ProviderFinishReason::Stop),
        "length" => Some(ProviderFinishReason::Length),
        "tool_calls" => Some(ProviderFinishReason::ToolCalls),
        "content_filter" => Some(ProviderFinishReason::ContentFilter),
        "cancelled" => Some(ProviderFinishReason::Cancelled),
        "error" => Some(ProviderFinishReason::Error),
        "unknown" => Some(ProviderFinishReason::Unknown),
        _ => {
            push_issue(
                issues,
                "unknown_finish_reason",
                path,
                format!(
                    "unknown finish reason '{value}', expected one of {}",
                    FINISH_REASON_VALUES.join(", ")
                ),
                "Use a supported provider finish reason.",
            );
            None
        }
    }
}

fn validate_tool_calls(
    values: Vec<QaMockProviderToolCallWire>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Vec<QaMockProviderToolCall> {
    let mut tool_calls = Vec::with_capacity(values.len());
    for (index, tool_call) in values.into_iter().enumerate() {
        let call_path = format!("{path}[{index}]");
        let proposal_id = validate_required_slug(
            tool_call.proposal_id,
            format!("{call_path}.proposal_id").as_str(),
            "proposal id",
            issues,
        );
        let tool_name = validate_required_string(
            tool_call.tool_name,
            format!("{call_path}.tool_name").as_str(),
            "tool name",
            issues,
        );
        if let (Some(proposal_id), Some(tool_name)) = (proposal_id, tool_name) {
            tool_calls.push(QaMockProviderToolCall {
                proposal_id,
                tool_name,
                input_json: tool_call.input_json.unwrap_or_else(|| json!({})),
            });
        }
    }
    tool_calls
}

fn validate_behavior_shape(
    path: &str,
    kind: Option<QaMockProviderBehaviorKind>,
    text: Option<&str>,
    deltas: &[String],
    tool_calls: &[QaMockProviderToolCall],
    error_message: Option<&str>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) {
    match kind {
        Some(QaMockProviderBehaviorKind::Text) if text.is_none() && deltas.is_empty() => {
            push_issue(
                issues,
                "missing_text_output",
                path,
                "text behavior requires text or deltas",
                "Add behavior.text or behavior.deltas.",
            )
        }
        Some(
            QaMockProviderBehaviorKind::ToolCalls | QaMockProviderBehaviorKind::ApprovalRequired,
        ) if tool_calls.is_empty() => {
            push_issue(
                issues,
                "missing_tool_calls",
                format!("{path}.tool_calls"),
                "tool-call behavior requires at least one tool call",
                "Add a tool call proposal to the behavior.",
            );
        }
        Some(
            QaMockProviderBehaviorKind::ContextOverflow
            | QaMockProviderBehaviorKind::MalformedOutput
            | QaMockProviderBehaviorKind::MalformedToolArgs
            | QaMockProviderBehaviorKind::StreamError,
        ) if error_message.is_none() => push_issue(
            issues,
            "missing_error_message",
            format!("{path}.error_message"),
            "error behavior requires error_message",
            "Add a redacted provider error message for this fixture turn.",
        ),
        _ => {}
    }
}

fn finish_reason_for_behavior(behavior: &QaMockProviderBehavior) -> ProviderFinishReason {
    behavior.finish_reason.unwrap_or(match behavior.kind {
        QaMockProviderBehaviorKind::ToolCalls | QaMockProviderBehaviorKind::ApprovalRequired => {
            ProviderFinishReason::ToolCalls
        }
        QaMockProviderBehaviorKind::Empty | QaMockProviderBehaviorKind::Text => {
            ProviderFinishReason::Stop
        }
        QaMockProviderBehaviorKind::ContextOverflow => ProviderFinishReason::Length,
        QaMockProviderBehaviorKind::MalformedOutput
        | QaMockProviderBehaviorKind::MalformedToolArgs
        | QaMockProviderBehaviorKind::StreamError => ProviderFinishReason::Error,
    })
}

fn provider_error_for_behavior(turn: &QaMockProviderTurn) -> Option<ProviderError> {
    let message = turn
        .behavior
        .error_message
        .clone()
        .unwrap_or_else(|| format!("QA mock-provider turn {} failed", turn.id));
    match turn.behavior.kind {
        QaMockProviderBehaviorKind::ContextOverflow => Some(ProviderError::RequestFailed {
            message,
            retryable: false,
            retry_count: 0,
            classification: provider_failure_classification(
                ProviderFailureClass::ContextWindowExceeded,
                ProviderFailureAction::UserActionRequired,
                None,
                "qa_mock_context_overflow",
            ),
        }),
        QaMockProviderBehaviorKind::MalformedOutput => Some(ProviderError::InvalidResponse {
            message,
            retry_count: 0,
            classification: invalid_response_classification("qa_mock_malformed_output"),
        }),
        QaMockProviderBehaviorKind::MalformedToolArgs => Some(ProviderError::InvalidResponse {
            message,
            retry_count: 0,
            classification: invalid_response_classification("qa_mock_malformed_tool_args"),
        }),
        QaMockProviderBehaviorKind::StreamError => Some(ProviderError::RequestFailed {
            message,
            retryable: true,
            retry_count: 0,
            classification: retry_provider_classification("qa_mock_stream_error"),
        }),
        QaMockProviderBehaviorKind::Text
        | QaMockProviderBehaviorKind::ToolCalls
        | QaMockProviderBehaviorKind::Empty
        | QaMockProviderBehaviorKind::ApprovalRequired => None,
    }
}

fn text_segments_for_behavior(behavior: &QaMockProviderBehavior) -> Vec<String> {
    if !behavior.deltas.is_empty() {
        return behavior.deltas.clone();
    }
    behavior.text.clone().into_iter().collect()
}

fn searchable_request_text(request: &ProviderRequest) -> String {
    let mut parts = Vec::new();
    if let Some(user_visible) = request.user_visible_input_text.as_deref() {
        parts.push(user_visible.to_owned());
    }
    parts.push(request.input_text.clone());
    parts.extend(request.effective_messages().into_iter().map(|message| message.text_content()));
    parts.join("\n").to_ascii_lowercase()
}

fn request_has_tool_result(request: &ProviderRequest, tool_call_id: &str) -> bool {
    request.messages.iter().any(|message| {
        message.role == ProviderMessageRole::Tool
            && message.tool_call_id.as_deref() == Some(tool_call_id)
    })
}

fn estimate_fixture_tokens(text: &str) -> u64 {
    u64::try_from(text.split_whitespace().count()).unwrap_or(u64::MAX).max(1)
}

fn validate_required_slug(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    let value = validate_required_string(value, path, label, issues)?;
    if is_slug(value.as_str()) {
        return Some(value);
    }
    push_issue(
        issues,
        "invalid_slug",
        path,
        format!("{label} must use lowercase ASCII letters, digits, '.', '_' or '-'"),
        format!("Rename the {label} to a stable lowercase slug."),
    );
    None
}

fn validate_required_string(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    match value.map(|value| value.trim().to_owned()) {
        Some(value) if !value.is_empty() => Some(value),
        _ => {
            push_issue(
                issues,
                format!("missing_{}", label.replace(' ', "_")),
                path,
                format!("{label} is required"),
                format!("Add a non-empty {label}."),
            );
            None
        }
    }
}

fn validate_optional_nonempty(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    match value.map(|value| value.trim().to_owned()) {
        Some(value) if !value.is_empty() => Some(value),
        Some(_) => {
            push_issue(
                issues,
                "empty_string",
                path,
                "value must not be empty when present",
                "Remove the field or provide a non-empty value.",
            );
            None
        }
        None => None,
    }
}

fn validate_string_list(
    values: &[String],
    path: &str,
    label: &str,
    require_nonempty: bool,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) {
    if require_nonempty && values.is_empty() {
        push_issue(
            issues,
            "empty_list",
            path,
            format!("{path} must contain at least one {label}"),
            format!("Add at least one {label}."),
        );
    }
    for (index, value) in values.iter().enumerate() {
        if value.trim().is_empty() {
            push_issue(
                issues,
                "empty_string",
                format!("{path}[{index}]"),
                format!("{label} must not be empty"),
                format!("Remove the empty {label} or replace it with a value."),
            );
        }
    }
}

fn is_slug(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && chars.all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
}

fn push_issue(
    issues: &mut Vec<QaMockProviderFixtureIssue>,
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) {
    issues.push(QaMockProviderFixtureIssue {
        severity: QaMockProviderIssueSeverity::Error,
        code: code.into(),
        path: path.into(),
        message: message.into(),
        recovery_hint: recovery_hint.into(),
    });
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;
    use crate::{ProviderMessage, ProviderMessageContentPart};

    const EXAMPLE_FIXTURE: &str = include_str!("../../../qa/fixtures/provider_basic.yaml");
    const SCHEMA_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_mock_provider_fixture_schema.json");

    #[test]
    fn parses_example_fixture_and_projects_streamed_text() {
        let fixture = parse_qa_mock_provider_fixture_yaml(EXAMPLE_FIXTURE)
            .expect("example QA mock-provider fixture should parse");
        let request = ProviderRequest::from_input_text(
            "Say a friendly deterministic answer".to_owned(),
            false,
            Vec::new(),
            None,
        );
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("friendly prompt should select text turn");

        let stream_events = qa_mock_provider_stream_events_for_turn(
            turn,
            &request,
            Some("qa-mock-basic".to_owned()),
        )
        .expect("text turn should project stream events");
        let output =
            qa_mock_provider_output_for_turn(turn, &request, Some("qa-mock-basic".to_owned()))
                .expect("text turn should project output");

        assert!(stream_events
            .iter()
            .any(|event| matches!(event, ProviderStreamEvent::Delta { text } if text.contains("friendly"))));
        assert!(output.full_text.contains("friendly"));
        assert_eq!(output.usage.source, "qa_mock_fixture");
    }

    #[test]
    fn projects_tool_call_fixture_to_provider_event() {
        let fixture = parse_qa_mock_provider_fixture_yaml(EXAMPLE_FIXTURE)
            .expect("example QA mock-provider fixture should parse");
        let request = ProviderRequest::from_input_text(
            "Please propose the approval tool call".to_owned(),
            false,
            Vec::new(),
            None,
        );
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("approval prompt should select tool turn");

        let events =
            qa_mock_provider_events_for_turn(turn, &request, Some("qa-mock-basic".to_owned()))
                .expect("tool turn should project events");

        assert!(matches!(
            events.last(),
            Some(ProviderEvent::ToolProposal { tool_name, .. }) if tool_name == "palyra.fs.read_file"
        ));
    }

    #[test]
    fn malformed_tool_args_fixture_returns_malformed_response_error() {
        let fixture = parse_qa_mock_provider_fixture_yaml(EXAMPLE_FIXTURE)
            .expect("example QA mock-provider fixture should parse");
        let request = ProviderRequest::from_input_text(
            "Trigger malformed tool args".to_owned(),
            false,
            Vec::new(),
            None,
        );
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("malformed prompt should select negative turn");
        let error =
            qa_mock_provider_output_for_turn(turn, &request, Some("qa-mock-basic".to_owned()))
                .expect_err("malformed tool args should fail");

        assert!(matches!(error, ProviderError::InvalidResponse { .. }));
        assert_eq!(error.classification().class, ProviderFailureClass::MalformedResponse);
    }

    #[test]
    fn tool_result_matcher_selects_followup_turn() {
        let fixture = parse_qa_mock_provider_fixture_yaml(EXAMPLE_FIXTURE)
            .expect("example QA mock-provider fixture should parse");
        let mut request = ProviderRequest::from_input_text(
            "Continue after tool result".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.messages.push(ProviderMessage {
            role: ProviderMessageRole::Tool,
            content: vec![ProviderMessageContentPart::text("ok")],
            name: None,
            tool_call_id: Some("qa-approval-read".to_owned()),
            tool_calls: Vec::new(),
        });

        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("tool-result matcher should select followup");

        assert_eq!(turn.id, "after_tool_result");
    }

    #[test]
    fn schema_snapshot_matches_golden_fixture() {
        let expected: Value =
            serde_json::from_str(SCHEMA_GOLDEN).expect("schema golden should parse");

        assert_eq!(qa_mock_provider_fixture_schema_snapshot(), expected);
    }

    #[test]
    fn rejects_unknown_behavior_with_precise_path() {
        let error = parse_qa_mock_provider_fixture_yaml(
            r#"
schema_version: 1
id: qa.mock.invalid
turns:
  - id: invalid
    behavior:
      kind: mystery
"#,
        )
        .expect_err("unknown behavior should fail validation");

        let issues = error.issues().expect("validation issues should be available");
        assert!(issues.iter().any(|issue| {
            issue.path == "$.turns[0].behavior.kind" && issue.code == "unknown_behavior_kind"
        }));
    }

    #[test]
    fn empty_behavior_projects_no_text_or_tools() {
        let fixture = QaMockProviderFixture {
            schema_version: QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION,
            id: "qa.mock.empty".to_owned(),
            turns: vec![QaMockProviderTurn {
                id: "empty".to_owned(),
                matcher: QaMockProviderTurnMatcher {
                    prompt_contains: Vec::new(),
                    tool_result_call_id: None,
                },
                behavior: QaMockProviderBehavior {
                    kind: QaMockProviderBehaviorKind::Empty,
                    text: None,
                    deltas: Vec::new(),
                    tool_calls: Vec::new(),
                    finish_reason: None,
                    error_message: None,
                    prompt_tokens: Some(1),
                    completion_tokens: Some(0),
                },
            }],
        };
        let request = ProviderRequest::from_input_text("empty".to_owned(), false, Vec::new(), None);
        let turn = qa_mock_provider_turn_for_request(&fixture, &request).expect("turn exists");

        let output = qa_mock_provider_output_for_turn(turn, &request, None)
            .expect("empty output should be valid");

        assert!(output.full_text.is_empty());
        assert!(output.content_parts.is_empty());
        assert_eq!(output.finish_reason, ProviderFinishReason::Stop);
    }

    #[test]
    fn direct_fixture_construction_uses_valid_json_tool_args() {
        let tool_call = QaMockProviderToolCall {
            proposal_id: "qa-tool".to_owned(),
            tool_name: "palyra.echo".to_owned(),
            input_json: json!({"text": "hello"}),
        };

        assert_eq!(tool_call.input_json["text"], "hello");
    }
}
