//! QA Lab mock-provider fixtures and deterministic projection.
//!
//! Fixtures describe provider turns as YAML, then compile into provider-neutral
//! stream events and bounded turn outputs consumed by the daemon runtime.

use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    errors::provider_failure_classification, invalid_response_classification,
    normalize_tool_input_value, provider_events_from_output, redact_remote_secret_fragments,
    retry_provider_classification, retryable_invalid_response_classification,
    sanitize_remote_error, ProviderError, ProviderEvent, ProviderFailureAction,
    ProviderFailureClass, ProviderFinishReason, ProviderMessageRole, ProviderRawProviderRefs,
    ProviderRequest, ProviderStreamAccumulator, ProviderStreamEvent, ProviderTurnOutput,
    MAX_TOOL_ARGUMENT_BYTES,
};

/// Current QA mock-provider fixture schema version.
pub const QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION: u32 = 2;

/// Required redaction contract for record-replay provider captures.
pub const QA_MOCK_PROVIDER_REDACTION_CONTRACT: &str = "palyra-provider-replay.v1";

/// Maximum length of a provider or model provenance label.
pub const MAX_QA_MOCK_PROVIDER_PROVENANCE_LABEL_LEN: usize = 64;

/// Stable format label embedded in fixture schema snapshots.
pub const QA_MOCK_PROVIDER_FIXTURE_FORMAT: &str = "palyra-qa-mock-provider-fixture";

/// Minimum number of attempts in an explicit retry sequence.
pub const MIN_QA_MOCK_PROVIDER_ATTEMPTS: usize = 2;

/// Maximum number of provider attempts one QA fixture turn may execute.
pub const MAX_QA_MOCK_PROVIDER_ATTEMPTS: usize = 4;

/// Maximum deterministic latency assigned to one fixture attempt.
pub const MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS: u64 = 5_000;

/// Maximum cumulative deterministic latency assigned to one fixture turn.
pub const MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS: u64 = 10_000;

/// Maximum accepted serialized QA mock-provider fixture size.
pub const MAX_QA_MOCK_PROVIDER_FIXTURE_BYTES: usize = 4 * 1024 * 1024;

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
const RETRYABLE_INTERMEDIATE_BEHAVIOR_KIND_VALUES: &[&str] =
    &["malformed_output", "malformed_tool_args", "stream_error"];
const SUCCESS_CAPABLE_TERMINAL_BEHAVIOR_KIND_VALUES: &[&str] =
    &["text", "tool_calls", "empty", "approval_required"];
const SUPPORTED_FIXTURE_SCHEMA_VERSIONS: &[u32] = &[1, QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION];

/// Parsed and validated QA mock-provider fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderFixture {
    /// Version of the mock-provider fixture schema.
    pub schema_version: u32,
    /// Stable fixture identifier.
    pub id: String,
    /// Redacted capture provenance required by schema-v2 replay fixtures.
    #[serde(skip_serializing_if = "Option::is_none")]
    capture_provenance: Option<QaMockProviderCaptureProvenance>,
    /// Ordered turns; the first matching turn is selected for a request.
    pub turns: Vec<QaMockProviderTurn>,
}

impl QaMockProviderFixture {
    /// Returns validated redacted capture provenance for replay fixtures.
    #[must_use]
    pub fn capture_provenance(&self) -> Option<&QaMockProviderCaptureProvenance> {
        self.capture_provenance.as_ref()
    }
}

/// Validated, secret-free identity of a provider capture used for replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaMockProviderCaptureProvenance {
    provider_label: String,
    model_label: String,
    source_capture_sha256: String,
    redaction_contract: String,
    raw_payloads_stored: bool,
}

impl QaMockProviderCaptureProvenance {
    /// Returns the bounded provider family label.
    #[must_use]
    pub fn provider_label(&self) -> &str {
        self.provider_label.as_str()
    }

    /// Returns the bounded provider model label.
    #[must_use]
    pub fn model_label(&self) -> &str {
        self.model_label.as_str()
    }

    /// Returns the lowercase SHA-256 digest identifying the redacted source capture.
    #[must_use]
    pub fn source_capture_sha256(&self) -> &str {
        self.source_capture_sha256.as_str()
    }

    /// Returns the redaction contract applied before fixture persistence.
    #[must_use]
    pub fn redaction_contract(&self) -> &str {
        self.redaction_contract.as_str()
    }

    /// Returns whether raw provider payloads were retained in the fixture.
    #[must_use]
    pub const fn raw_payloads_stored(&self) -> bool {
        self.raw_payloads_stored
    }
}

/// One deterministic provider turn from a QA fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaMockProviderTurn {
    /// Stable turn identifier scoped to one fixture.
    pub id: String,
    /// Request selector used to choose this turn.
    pub matcher: QaMockProviderTurnMatcher,
    /// Single behavior, or the terminal behavior cached from `attempts`.
    pub behavior: QaMockProviderBehavior,
    /// Explicit validated provider-attempt sequence; empty means execute
    /// `behavior` once.
    pub attempts: Vec<QaMockProviderBehavior>,
}

// The parser requires behavior and attempts to be mutually exclusive, so the
// serialized fixture must not expose the cached terminal behavior beside an
// explicit attempt sequence.
impl Serialize for QaMockProviderTurn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct as _;

        let mut turn = serializer.serialize_struct("QaMockProviderTurn", 3)?;
        turn.serialize_field("id", &self.id)?;
        turn.serialize_field("match", &self.matcher)?;
        if self.attempts.is_empty() {
            turn.serialize_field("behavior", &self.behavior)?;
        } else {
            turn.serialize_field("attempts", &self.attempts)?;
        }
        turn.end()
    }
}

impl QaMockProviderTurn {
    /// Returns true when this turn applies to `request`.
    #[must_use]
    pub fn matches_request(&self, request: &ProviderRequest) -> bool {
        self.matcher.matches_request(request)
    }

    /// Returns the number of provider attempts executed for this turn.
    #[must_use]
    pub fn attempt_count(&self) -> usize {
        if self.attempts.is_empty() {
            1
        } else {
            self.attempts.len()
        }
    }

    /// Returns whether the turn declares an explicit retry sequence.
    ///
    /// This remains distinct from [`Self::attempt_count`] because a directly
    /// constructed, invalid one-entry sequence also reports one attempt.
    #[must_use]
    pub fn has_explicit_attempt_sequence(&self) -> bool {
        !self.attempts.is_empty()
    }

    /// Returns the configured latency for one zero-based provider attempt.
    #[must_use]
    pub fn attempt_latency_ms(&self, attempt_index: usize) -> Option<u64> {
        self.behavior_for_attempt(attempt_index).map(|behavior| behavior.latency_ms)
    }

    fn behavior_for_attempt(&self, attempt_index: usize) -> Option<&QaMockProviderBehavior> {
        if self.attempts.is_empty() {
            return (attempt_index == 0).then_some(&self.behavior);
        }
        self.attempts.get(attempt_index)
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
    /// Optional required outcome for the selected tool result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_result_success: Option<bool>,
}

impl QaMockProviderTurnMatcher {
    fn matches_request(&self, request: &ProviderRequest) -> bool {
        let text = searchable_request_text(request);
        let prompt_matches = self
            .prompt_contains
            .iter()
            .all(|needle| text.contains(needle.trim().to_ascii_lowercase().as_str()));
        let tool_result_matches = self.tool_result_call_id.as_deref().is_none_or(|tool_call_id| {
            request_has_tool_result(request, tool_call_id, self.tool_result_success)
        });
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
    /// Deterministic delay applied before this behavior resolves.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub latency_ms: u64,
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
    #[serde(default)]
    capture_provenance: Option<QaMockProviderCaptureProvenanceWire>,
    turns: Option<Vec<QaMockProviderTurnWire>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderCaptureProvenanceWire {
    provider_label: Option<String>,
    model_label: Option<String>,
    source_capture_sha256: Option<String>,
    redaction_contract: Option<String>,
    raw_payloads_stored: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderTurnWire {
    id: Option<String>,
    #[serde(default, rename = "match")]
    matcher: Option<QaMockProviderTurnMatcherWire>,
    behavior: Option<QaMockProviderBehaviorWire>,
    attempts: Option<Vec<QaMockProviderBehaviorWire>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaMockProviderTurnMatcherWire {
    #[serde(default)]
    prompt_contains: Vec<String>,
    #[serde(default)]
    tool_result_call_id: Option<String>,
    #[serde(default)]
    tool_result_success: Option<bool>,
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
    #[serde(default)]
    latency_ms: Option<u64>,
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
    if text.len() > MAX_QA_MOCK_PROVIDER_FIXTURE_BYTES {
        let mut issues = Vec::with_capacity(1);
        push_issue(
            &mut issues,
            "fixture_size_exceeded",
            "$",
            format!(
                "fixture exceeds the {MAX_QA_MOCK_PROVIDER_FIXTURE_BYTES}-byte serialized size limit"
            ),
            "Reduce the fixture before loading it.",
        );
        return Err(QaMockProviderFixtureError::Invalid(QaMockProviderValidationError::new(
            issues,
        )));
    }
    let wire = yaml_serde::from_str::<QaMockProviderFixtureWire>(text)
        .map_err(|source| QaMockProviderFixtureError::Parse { source })?;
    build_validated_fixture(wire)
}

/// Returns the versioned schema snapshot and bounded execution contract used
/// by QA mock-provider tooling.
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
            "capture_provenance",
            "turns"
        ],
        "supported_schema_versions": SUPPORTED_FIXTURE_SCHEMA_VERSIONS,
        "limits": {
            "max_fixture_bytes": MAX_QA_MOCK_PROVIDER_FIXTURE_BYTES,
            "max_tool_argument_bytes": MAX_TOOL_ARGUMENT_BYTES
        },
        "capture_provenance_contract": {
            "required_from_schema_version": 2,
            "schema_v1_serialization": "omitted",
            "fields": [
                "provider_label",
                "model_label",
                "source_capture_sha256",
                "redaction_contract",
                "raw_payloads_stored"
            ],
            "label_max_chars": MAX_QA_MOCK_PROVIDER_PROVENANCE_LABEL_LEN,
            "source_capture_sha256": {
                "encoding": "lowercase_hex",
                "length": 64
            },
            "redaction_contract": QA_MOCK_PROVIDER_REDACTION_CONTRACT,
            "raw_payloads_stored": false
        },
        "behavior_kinds": BEHAVIOR_KIND_VALUES,
        "finish_reasons": FINISH_REASON_VALUES,
        "execution_contract": {
            "forms": {
                "behavior": {
                    "mode": "single_attempt",
                    "backward_compatible": true
                },
                "attempts": {
                    "mode": "bounded_retry_sequence",
                    "min_attempts": MIN_QA_MOCK_PROVIDER_ATTEMPTS,
                    "max_attempts": MAX_QA_MOCK_PROVIDER_ATTEMPTS,
                    "retryable_intermediate_behavior_kinds":
                        RETRYABLE_INTERMEDIATE_BEHAVIOR_KIND_VALUES,
                    "success_capable_terminal_behavior_kinds":
                        SUCCESS_CAPABLE_TERMINAL_BEHAVIOR_KIND_VALUES
                }
            },
            "forms_mutually_exclusive": true,
            "latency": {
                "field": "latency_ms",
                "unit": "milliseconds",
                "default_ms": 0,
                "max_per_attempt_ms": MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS,
                "max_total_per_turn_ms": MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS
            }
        },
        "matcher_contract": {
            "tool_result": {
                "call_id_field": "tool_result_call_id",
                "success_field": "tool_result_success",
                "success_requires_call_id": true,
                "unwrapped_json_defaults_to_success": true
            }
        },
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
/// malformed output, context overflow, or stream failure. Explicit attempt
/// sequences must use [`qa_mock_provider_output_for_attempt`] so retries
/// cannot be skipped accidentally.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_stream_events_for_turn(
    turn: &QaMockProviderTurn,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
    if !turn.attempts.is_empty() {
        return Err(invalid_attempt_plan_error(
            turn.id.as_str(),
            0,
            "explicit attempt sequence requires indexed execution",
        ));
    }
    qa_mock_provider_stream_events_for_behavior(
        turn.id.as_str(),
        &turn.behavior,
        request,
        provider_model_id,
        0,
        false,
    )
}

fn qa_mock_provider_stream_events_for_behavior(
    turn_id: &str,
    behavior: &QaMockProviderBehavior,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
    retry_count: u32,
    retryable_malformed_response: bool,
) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
    if let Some(error) =
        provider_error_for_behavior(turn_id, behavior, retry_count, retryable_malformed_response)
    {
        return Err(error);
    }

    let model_id = provider_model_id.unwrap_or_else(|| "qa-mock".to_owned());
    let prompt_tokens = behavior
        .prompt_tokens
        .unwrap_or_else(|| estimate_fixture_tokens(request.input_text.as_str()));
    let completion_tokens = behavior.completion_tokens.unwrap_or_else(|| {
        let text_tokens = text_segments_for_behavior(behavior)
            .iter()
            .map(|segment| estimate_fixture_tokens(segment.as_str()))
            .sum::<u64>();
        let tool_tokens = behavior
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
    for delta in text_segments_for_behavior(behavior) {
        events.push(ProviderStreamEvent::Delta { text: delta });
    }
    for tool_call in &behavior.tool_calls {
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
        finish_reason: finish_reason_for_behavior(behavior),
        raw_provider_refs: ProviderRawProviderRefs {
            provider_response_id: Some(format!("qa-mock:{turn_id}")),
            provider_model_id: Some(model_id),
            system_fingerprint: Some(QA_MOCK_PROVIDER_FIXTURE_FORMAT.to_owned()),
            provider_trace_ref: Some(format!("qa_mock_provider:{turn_id}")),
            stream_spill_ref: None,
        },
    });
    Ok(events)
}

/// Projects one fixture turn into a bounded provider output.
///
/// # Errors
/// Returns a [`ProviderError`] for negative fixture behaviors such as
/// malformed output, context overflow, or stream failure. Explicit attempt
/// sequences must use [`qa_mock_provider_output_for_attempt`] so retries
/// cannot be skipped accidentally.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_output_for_turn(
    turn: &QaMockProviderTurn,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<ProviderTurnOutput, ProviderError> {
    if !turn.attempts.is_empty() {
        return Err(invalid_attempt_plan_error(
            turn.id.as_str(),
            0,
            "explicit attempt sequence requires indexed execution",
        ));
    }
    qa_mock_provider_output_for_behavior(
        turn.id.as_str(),
        &turn.behavior,
        request,
        provider_model_id,
        0,
        false,
    )
}

/// Projects one indexed fixture attempt into a bounded provider output.
///
/// Intermediate attempts are accepted only when they represent retryable
/// stream or malformed-response failures. The final attempt must be capable
/// of producing a successful response.
///
/// # Errors
/// Returns a [`ProviderError`] for the configured attempt failure or when the
/// attempt index/ordering violates the validated execution contract.
#[allow(clippy::result_large_err)]
pub fn qa_mock_provider_output_for_attempt(
    turn: &QaMockProviderTurn,
    attempt_index: usize,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
) -> Result<ProviderTurnOutput, ProviderError> {
    let retry_count = u32::try_from(attempt_index).unwrap_or(u32::MAX);
    let Some(behavior) = turn.behavior_for_attempt(attempt_index) else {
        return Err(invalid_attempt_plan_error(
            turn.id.as_str(),
            retry_count,
            "attempt index is outside the configured sequence",
        ));
    };
    let has_followup = attempt_index.saturating_add(1) < turn.attempt_count();
    if has_followup && !is_retryable_failure_behavior(behavior.kind) {
        return Err(invalid_attempt_plan_error(
            turn.id.as_str(),
            retry_count,
            "intermediate attempt is not a retryable failure",
        ));
    }
    if !has_followup && !is_success_capable_behavior(behavior.kind) && !turn.attempts.is_empty() {
        return Err(invalid_attempt_plan_error(
            turn.id.as_str(),
            retry_count,
            "final explicit attempt is not success-capable",
        ));
    }

    qa_mock_provider_output_for_behavior(
        turn.id.as_str(),
        behavior,
        request,
        provider_model_id,
        retry_count,
        has_followup,
    )
}

fn qa_mock_provider_output_for_behavior(
    turn_id: &str,
    behavior: &QaMockProviderBehavior,
    request: &ProviderRequest,
    provider_model_id: Option<String>,
    retry_count: u32,
    retryable_malformed_response: bool,
) -> Result<ProviderTurnOutput, ProviderError> {
    let model_id = provider_model_id.clone().unwrap_or_else(|| "qa-mock".to_owned());
    let mut accumulator = ProviderStreamAccumulator::new("qa-mock-provider", model_id);
    for event in qa_mock_provider_stream_events_for_behavior(
        turn_id,
        behavior,
        request,
        provider_model_id,
        retry_count,
        retryable_malformed_response,
    )? {
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
/// failure instead of a successful response or declares an explicit attempt
/// sequence that requires indexed execution.
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
    let capture_provenance =
        validate_capture_provenance(wire.capture_provenance, schema_version, &mut issues);
    let turns = validate_turns(wire.turns, &mut issues);

    if issues.is_empty() {
        Ok(QaMockProviderFixture {
            schema_version: schema_version.unwrap_or(QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION),
            id: id.unwrap_or_default(),
            capture_provenance,
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
        Some(version) if SUPPORTED_FIXTURE_SCHEMA_VERSIONS.contains(&version) => Some(version),
        Some(value) => {
            push_issue(
                issues,
                "unsupported_schema_version",
                "$.schema_version",
                format!(
                    "schema_version must be one of {}, got {value}",
                    SUPPORTED_FIXTURE_SCHEMA_VERSIONS
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
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

fn validate_capture_provenance(
    value: Option<QaMockProviderCaptureProvenanceWire>,
    schema_version: Option<u32>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<QaMockProviderCaptureProvenance> {
    match schema_version {
        Some(1) => {
            if value.is_some() {
                push_issue(
                    issues,
                    "capture_provenance_requires_schema_v2",
                    "$.capture_provenance",
                    "capture provenance is not supported by schema_version 1",
                    "Remove capture_provenance or migrate the fixture to schema_version 2.",
                );
            }
            None
        }
        Some(QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION) => {
            let Some(value) = value else {
                push_issue(
                    issues,
                    "missing_capture_provenance",
                    "$.capture_provenance",
                    "schema_version 2 requires redacted capture provenance",
                    "Add a typed capture_provenance section with raw_payloads_stored set to false.",
                );
                return None;
            };
            let provider_label = validate_capture_provenance_label(
                value.provider_label,
                "$.capture_provenance.provider_label",
                "provider label",
                issues,
            );
            let model_label = validate_capture_provenance_label(
                value.model_label,
                "$.capture_provenance.model_label",
                "model label",
                issues,
            );
            let source_capture_sha256 =
                validate_source_capture_sha256(value.source_capture_sha256, issues);
            let redaction_contract =
                validate_capture_redaction_contract(value.redaction_contract, issues);
            let raw_payloads_stored =
                validate_raw_payloads_not_stored(value.raw_payloads_stored, issues);

            match (
                provider_label,
                model_label,
                source_capture_sha256,
                redaction_contract,
                raw_payloads_stored,
            ) {
                (
                    Some(provider_label),
                    Some(model_label),
                    Some(source_capture_sha256),
                    Some(redaction_contract),
                    Some(raw_payloads_stored),
                ) => Some(QaMockProviderCaptureProvenance {
                    provider_label,
                    model_label,
                    source_capture_sha256,
                    redaction_contract,
                    raw_payloads_stored,
                }),
                _ => None,
            }
        }
        Some(_) | None => None,
    }
}

fn validate_capture_provenance_label(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    let value = validate_required_string(value, path, label, issues)?;
    if value.chars().count() > MAX_QA_MOCK_PROVIDER_PROVENANCE_LABEL_LEN {
        push_issue(
            issues,
            "capture_provenance_label_too_long",
            path,
            format!(
                "{label} must contain at most {MAX_QA_MOCK_PROVIDER_PROVENANCE_LABEL_LEN} characters"
            ),
            format!("Shorten the {label} to a bounded non-sensitive identifier."),
        );
        return None;
    }
    if !is_slug(value.as_str()) {
        push_issue(
            issues,
            "invalid_capture_provenance_label",
            path,
            format!("{label} must use lowercase ASCII letters, digits, '.', '_' or '-'"),
            format!("Replace the {label} with a safe lowercase slug."),
        );
        return None;
    }
    if capture_provenance_label_looks_secret_shaped(value.as_str()) {
        push_issue(
            issues,
            "secret_shaped_capture_provenance_label",
            path,
            format!("{label} must identify a provider or model without credential-shaped text"),
            format!("Replace the {label} with a non-sensitive descriptive slug."),
        );
        return None;
    }
    Some(value)
}

fn capture_provenance_label_looks_secret_shaped(value: &str) -> bool {
    const SENSITIVE_MARKERS: &[&str] = &[
        "api-key",
        "apikey",
        "secret",
        "token",
        "bearer",
        "password",
        "credential",
        "private-key",
        "access-key",
    ];
    const SENSITIVE_PREFIXES: &[&str] =
        &["sk-", "ghp_", "github_pat_", "xoxb-", "xoxp-", "ya29.", "eyj"];

    let normalized = value.replace(['.', '_'], "-");
    redact_remote_secret_fragments(value) != value
        || SENSITIVE_PREFIXES.iter().any(|prefix| value.starts_with(prefix))
        || SENSITIVE_MARKERS.iter().any(|marker| {
            normalized == *marker
                || normalized.starts_with(format!("{marker}-").as_str())
                || normalized.ends_with(format!("-{marker}").as_str())
                || normalized.contains(format!("-{marker}-").as_str())
        })
}

fn validate_source_capture_sha256(
    value: Option<String>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    let value = validate_required_string(
        value,
        "$.capture_provenance.source_capture_sha256",
        "source capture sha256",
        issues,
    )?;
    if value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Some(value);
    }
    push_issue(
        issues,
        "invalid_source_capture_sha256",
        "$.capture_provenance.source_capture_sha256",
        "source capture SHA-256 must contain exactly 64 lowercase hexadecimal characters",
        "Replace source_capture_sha256 with the lowercase digest of the redacted source capture.",
    );
    None
}

fn validate_capture_redaction_contract(
    value: Option<String>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<String> {
    let value = validate_required_string(
        value,
        "$.capture_provenance.redaction_contract",
        "redaction contract",
        issues,
    )?;
    if value == QA_MOCK_PROVIDER_REDACTION_CONTRACT {
        return Some(value);
    }
    push_issue(
        issues,
        "unsupported_capture_redaction_contract",
        "$.capture_provenance.redaction_contract",
        "capture provenance uses an unsupported redaction contract",
        format!("Set redaction_contract to {QA_MOCK_PROVIDER_REDACTION_CONTRACT}."),
    );
    None
}

fn validate_raw_payloads_not_stored(
    value: Option<bool>,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<bool> {
    match value {
        Some(false) => Some(false),
        Some(true) => {
            push_issue(
                issues,
                "raw_provider_payload_storage_forbidden",
                "$.capture_provenance.raw_payloads_stored",
                "record-replay fixtures must not retain raw provider payloads",
                "Set raw_payloads_stored to false and persist only the redacted replay projection.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_raw_payloads_stored",
                "$.capture_provenance.raw_payloads_stored",
                "capture provenance requires an explicit raw payload storage posture",
                "Set raw_payloads_stored to false.",
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
        let execution =
            validate_turn_execution(turn.behavior, turn.attempts, path.as_str(), issues);
        if let (Some(id), Some(matcher), Some((behavior, attempts))) = (id, matcher, execution) {
            turns.push(QaMockProviderTurn { id, matcher, behavior, attempts });
        }
    }
    Some(turns)
}

fn validate_turn_execution(
    behavior: Option<QaMockProviderBehaviorWire>,
    attempts: Option<Vec<QaMockProviderBehaviorWire>>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<(QaMockProviderBehavior, Vec<QaMockProviderBehavior>)> {
    match (behavior, attempts) {
        (Some(behavior), None) => {
            validate_behavior(Some(behavior), format!("{path}.behavior").as_str(), issues)
                .map(|behavior| (behavior, Vec::new()))
        }
        (None, Some(attempts)) => {
            validate_attempt_sequence(attempts, format!("{path}.attempts").as_str(), issues)
        }
        (None, None) => {
            push_issue(
                issues,
                "missing_behavior",
                format!("{path}.behavior"),
                "behavior or attempts section is required",
                "Declare one behavior or an explicit provider-attempt sequence.",
            );
            None
        }
        (Some(behavior), Some(attempts)) => {
            push_issue(
                issues,
                "conflicting_attempt_configuration",
                path,
                "behavior and attempts are mutually exclusive",
                "Keep behavior for one attempt or attempts for an explicit retry sequence.",
            );
            let _ = validate_behavior(Some(behavior), format!("{path}.behavior").as_str(), issues);
            let _ =
                validate_attempt_sequence(attempts, format!("{path}.attempts").as_str(), issues);
            None
        }
    }
}

fn validate_attempt_sequence(
    values: Vec<QaMockProviderBehaviorWire>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<(QaMockProviderBehavior, Vec<QaMockProviderBehavior>)> {
    let attempt_count = values.len();
    if attempt_count < MIN_QA_MOCK_PROVIDER_ATTEMPTS {
        push_issue(
            issues,
            "insufficient_attempts",
            path,
            format!(
                "explicit attempts must contain at least {MIN_QA_MOCK_PROVIDER_ATTEMPTS} behaviors"
            ),
            "Use behavior for one attempt or add a retryable failure before the final success.",
        );
    }
    if attempt_count > MAX_QA_MOCK_PROVIDER_ATTEMPTS {
        push_issue(
            issues,
            "too_many_attempts",
            path,
            format!(
                "attempts contains {attempt_count} behaviors, maximum is {MAX_QA_MOCK_PROVIDER_ATTEMPTS}"
            ),
            "Reduce the provider-attempt sequence to the supported bound.",
        );
    }

    let mut total_latency_ms = 0_u64;
    let mut latency_overflow = false;
    let mut parsed = Vec::with_capacity(attempt_count.min(MAX_QA_MOCK_PROVIDER_ATTEMPTS));
    let mut all_behaviors_valid = true;
    for (index, value) in values.into_iter().enumerate() {
        let attempt_path = format!("{path}[{index}]");
        let latency_ms = value.latency_ms.unwrap_or_default();
        match total_latency_ms.checked_add(latency_ms) {
            Some(total) => total_latency_ms = total,
            None => latency_overflow = true,
        }
        match validate_behavior(Some(value), attempt_path.as_str(), issues) {
            Some(behavior) => parsed.push(behavior),
            None => all_behaviors_valid = false,
        }
    }
    if latency_overflow || total_latency_ms > MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS {
        push_issue(
            issues,
            "total_latency_exceeded",
            path,
            format!(
                "attempt latency exceeds the {MAX_QA_MOCK_PROVIDER_TOTAL_LATENCY_MS}ms per-turn maximum"
            ),
            "Reduce per-attempt latency values so their sum stays within the turn budget.",
        );
    }

    if parsed.len() == attempt_count {
        for (index, behavior) in parsed.iter().enumerate() {
            let is_final = index.saturating_add(1) == attempt_count;
            if !is_final && !is_retryable_failure_behavior(behavior.kind) {
                push_issue(
                    issues,
                    "non_retryable_intermediate_attempt",
                    format!("{path}[{index}].kind"),
                    "intermediate attempts must be retryable stream or malformed-response failures",
                    "Use stream_error, malformed_output, or malformed_tool_args before the final attempt.",
                );
            }
            if is_final && !is_success_capable_behavior(behavior.kind) {
                push_issue(
                    issues,
                    "terminal_attempt_cannot_succeed",
                    format!("{path}[{index}].kind"),
                    "final attempt must be capable of producing text, tools, or an empty success",
                    "Use text, tool_calls, approval_required, or empty for the final attempt.",
                );
            }
        }
    }

    if !all_behaviors_valid || parsed.len() != attempt_count || parsed.is_empty() {
        return None;
    }
    let terminal_behavior = parsed
        .last()
        .cloned()
        .expect("non-empty validated attempt sequence has a terminal behavior");
    Some((terminal_behavior, parsed))
}

fn validate_matcher(
    value: Option<QaMockProviderTurnMatcherWire>,
    path: &str,
    issues: &mut Vec<QaMockProviderFixtureIssue>,
) -> Option<QaMockProviderTurnMatcher> {
    let value = value.unwrap_or(QaMockProviderTurnMatcherWire {
        prompt_contains: Vec::new(),
        tool_result_call_id: None,
        tool_result_success: None,
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
    if value.tool_result_success.is_some() && tool_result_call_id.is_none() {
        push_issue(
            issues,
            "tool_result_success_requires_call_id",
            format!("{path}.tool_result_success"),
            "tool_result_success requires tool_result_call_id",
            "Set tool_result_call_id or remove the outcome constraint.",
        );
    }
    Some(QaMockProviderTurnMatcher {
        prompt_contains: value.prompt_contains,
        tool_result_call_id,
        tool_result_success: value.tool_result_success,
    })
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
    let latency_ms = value.latency_ms.unwrap_or_default();
    if latency_ms > MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS {
        push_issue(
            issues,
            "attempt_latency_exceeded",
            format!("{path}.latency_ms"),
            format!(
                "latency_ms is {latency_ms}, maximum is {MAX_QA_MOCK_PROVIDER_ATTEMPT_LATENCY_MS}"
            ),
            "Reduce the deterministic attempt latency to the supported bound.",
        );
    }
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
        latency_ms,
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
        let input_json = tool_call.input_json.unwrap_or_else(|| json!({}));
        if normalize_tool_input_value(&input_json).is_err() {
            push_issue(
                issues,
                "tool_call_input_too_large",
                format!("{call_path}.input_json"),
                format!("tool input exceeds the {MAX_TOOL_ARGUMENT_BYTES}-byte serialized limit"),
                "Reduce the fixture tool input.",
            );
            continue;
        }
        if let (Some(proposal_id), Some(tool_name)) = (proposal_id, tool_name) {
            tool_calls.push(QaMockProviderToolCall { proposal_id, tool_name, input_json });
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

const fn is_retryable_failure_behavior(kind: QaMockProviderBehaviorKind) -> bool {
    matches!(
        kind,
        QaMockProviderBehaviorKind::MalformedOutput
            | QaMockProviderBehaviorKind::MalformedToolArgs
            | QaMockProviderBehaviorKind::StreamError
    )
}

const fn is_success_capable_behavior(kind: QaMockProviderBehaviorKind) -> bool {
    matches!(
        kind,
        QaMockProviderBehaviorKind::Text
            | QaMockProviderBehaviorKind::ToolCalls
            | QaMockProviderBehaviorKind::Empty
            | QaMockProviderBehaviorKind::ApprovalRequired
    )
}

fn provider_error_for_behavior(
    turn_id: &str,
    behavior: &QaMockProviderBehavior,
    retry_count: u32,
    retryable_malformed_response: bool,
) -> Option<ProviderError> {
    let message = behavior
        .error_message
        .as_deref()
        .map(sanitize_remote_error)
        .unwrap_or_else(|| format!("QA mock-provider turn {turn_id} failed"));
    match behavior.kind {
        QaMockProviderBehaviorKind::ContextOverflow => Some(ProviderError::RequestFailed {
            message,
            retryable: false,
            retry_count,
            classification: provider_failure_classification(
                ProviderFailureClass::ContextWindowExceeded,
                ProviderFailureAction::UserActionRequired,
                None,
                "qa_mock_context_overflow",
            ),
        }),
        QaMockProviderBehaviorKind::MalformedOutput => Some(ProviderError::InvalidResponse {
            message,
            retry_count,
            classification: if retryable_malformed_response {
                retryable_invalid_response_classification("qa_mock_malformed_output")
            } else {
                invalid_response_classification("qa_mock_malformed_output")
            },
        }),
        QaMockProviderBehaviorKind::MalformedToolArgs => Some(ProviderError::InvalidResponse {
            message,
            retry_count,
            classification: if retryable_malformed_response {
                retryable_invalid_response_classification("qa_mock_malformed_tool_args")
            } else {
                invalid_response_classification("qa_mock_malformed_tool_args")
            },
        }),
        QaMockProviderBehaviorKind::StreamError => Some(ProviderError::RequestFailed {
            message,
            retryable: true,
            retry_count,
            classification: retry_provider_classification("qa_mock_stream_error"),
        }),
        QaMockProviderBehaviorKind::Text
        | QaMockProviderBehaviorKind::ToolCalls
        | QaMockProviderBehaviorKind::Empty
        | QaMockProviderBehaviorKind::ApprovalRequired => None,
    }
}

fn invalid_attempt_plan_error(turn_id: &str, retry_count: u32, reason: &str) -> ProviderError {
    ProviderError::InvalidResponse {
        message: format!("QA mock-provider turn {turn_id} has an invalid attempt plan: {reason}"),
        retry_count,
        classification: invalid_response_classification("qa_mock_invalid_attempt_plan"),
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

fn request_has_tool_result(
    request: &ProviderRequest,
    tool_call_id: &str,
    expected_success: Option<bool>,
) -> bool {
    request.messages.iter().any(|message| {
        if message.role != ProviderMessageRole::Tool
            || message.tool_call_id.as_deref() != Some(tool_call_id)
        {
            return false;
        }
        expected_success.is_none_or(|expected| {
            serde_json::from_str::<Value>(message.text_content().as_str()).ok().map(|value| {
                // Successful runtime tool results are re-fed as their raw
                // JSON output; failed results carry an explicit envelope.
                value.get("success").and_then(Value::as_bool).unwrap_or(true)
            }) == Some(expected)
        })
    })
}

fn estimate_fixture_tokens(text: &str) -> u64 {
    u64::try_from(text.split_whitespace().count()).unwrap_or(u64::MAX).max(1)
}

const fn is_zero(value: &u64) -> bool {
    *value == 0
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
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{ProviderMessage, ProviderMessageContentPart};

    const EXAMPLE_FIXTURE: &str = include_str!("../../../qa/fixtures/provider_basic.yaml");
    const RETRY_FIXTURE: &str = include_str!("../../../qa/fixtures/provider_retry_recovery.yaml");
    const RECORD_REPLAY_FIXTURE: &str =
        include_str!("../../../qa/fixtures/record_replay/real_agent_runner_replay.yaml");
    const RECORD_REPLAY_SOURCE: &[u8] =
        include_bytes!("../../../qa/fixtures/real_agent_runner.yaml");
    const CAPTURE_PROVENANCE_BLOCK: &str = r#"capture_provenance:
  provider_label: palyra-qa-mock
  model_label: deterministic-agent-runner
  source_capture_sha256: 5419d3ca2450b8cb8fa2f9664261ff4e68fdd8dadf10c37f39ded05c7378f933
  redaction_contract: palyra-provider-replay.v1
  raw_payloads_stored: false
"#;
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
    fn schema_v1_round_trips_without_capture_provenance() {
        let fixture = parse_qa_mock_provider_fixture_yaml(EXAMPLE_FIXTURE)
            .expect("schema-v1 fixture should remain valid");

        assert_eq!(fixture.schema_version, 1);
        assert_eq!(fixture.capture_provenance(), None);

        let json = serde_json::to_string(&fixture).expect("schema-v1 fixture should serialize");
        let json_value: Value = serde_json::from_str(json.as_str())
            .expect("serialized schema-v1 fixture should be valid JSON");
        assert_eq!(json_value.get("capture_provenance"), None);
        assert_eq!(
            parse_qa_mock_provider_fixture_yaml(json.as_str())
                .expect("schema-v1 JSON projection should parse"),
            fixture
        );

        let yaml = yaml_serde::to_string(&fixture).expect("schema-v1 fixture should serialize");
        assert_eq!(
            parse_qa_mock_provider_fixture_yaml(yaml.as_str())
                .expect("schema-v1 YAML projection should parse"),
            fixture
        );
    }

    #[test]
    fn schema_v2_record_replay_fixture_round_trips_with_safe_provenance() {
        let fixture = parse_qa_mock_provider_fixture_yaml(RECORD_REPLAY_FIXTURE)
            .expect("schema-v2 record-replay fixture should parse");
        let provenance = fixture
            .capture_provenance()
            .expect("schema-v2 fixture should expose capture provenance");

        assert_eq!(fixture.schema_version, QA_MOCK_PROVIDER_FIXTURE_SCHEMA_VERSION);
        assert_eq!(provenance.provider_label(), "palyra-qa-mock");
        assert_eq!(provenance.model_label(), "deterministic-agent-runner");
        let source_digest = Sha256::digest(RECORD_REPLAY_SOURCE)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(provenance.source_capture_sha256(), source_digest);
        assert_eq!(provenance.redaction_contract(), QA_MOCK_PROVIDER_REDACTION_CONTRACT);
        assert!(!provenance.raw_payloads_stored());

        let json = serde_json::to_string(&fixture).expect("schema-v2 fixture should serialize");
        assert_eq!(
            parse_qa_mock_provider_fixture_yaml(json.as_str())
                .expect("schema-v2 JSON projection should parse"),
            fixture
        );
        let yaml = yaml_serde::to_string(&fixture).expect("schema-v2 fixture should serialize");
        assert_eq!(
            parse_qa_mock_provider_fixture_yaml(yaml.as_str())
                .expect("schema-v2 YAML projection should parse"),
            fixture
        );
    }

    #[test]
    fn schema_versions_enforce_capture_provenance_boundary() {
        let missing_v2 = RECORD_REPLAY_FIXTURE.replace(CAPTURE_PROVENANCE_BLOCK, "");
        assert_fixture_issue(
            missing_v2.as_str(),
            "$.capture_provenance",
            "missing_capture_provenance",
        );

        let provenance_on_v1 =
            RECORD_REPLAY_FIXTURE.replace("schema_version: 2", "schema_version: 1");
        assert_fixture_issue(
            provenance_on_v1.as_str(),
            "$.capture_provenance",
            "capture_provenance_requires_schema_v2",
        );
    }

    #[test]
    fn schema_v2_requires_every_capture_provenance_field() {
        for (line, path, code) in [
            (
                "  provider_label: palyra-qa-mock\n",
                "$.capture_provenance.provider_label",
                "missing_provider_label",
            ),
            (
                "  model_label: deterministic-agent-runner\n",
                "$.capture_provenance.model_label",
                "missing_model_label",
            ),
            (
                "  source_capture_sha256: 5419d3ca2450b8cb8fa2f9664261ff4e68fdd8dadf10c37f39ded05c7378f933\n",
                "$.capture_provenance.source_capture_sha256",
                "missing_source_capture_sha256",
            ),
            (
                "  redaction_contract: palyra-provider-replay.v1\n",
                "$.capture_provenance.redaction_contract",
                "missing_redaction_contract",
            ),
            (
                "  raw_payloads_stored: false\n",
                "$.capture_provenance.raw_payloads_stored",
                "missing_raw_payloads_stored",
            ),
        ] {
            let fixture = RECORD_REPLAY_FIXTURE.replace(line, "");
            assert_fixture_issue(fixture.as_str(), path, code);
        }
    }

    #[test]
    fn schema_v2_rejects_raw_payload_storage_and_invalid_capture_contracts() {
        let raw_payloads = RECORD_REPLAY_FIXTURE
            .replace("raw_payloads_stored: false", "raw_payloads_stored: true");
        assert_fixture_issue(
            raw_payloads.as_str(),
            "$.capture_provenance.raw_payloads_stored",
            "raw_provider_payload_storage_forbidden",
        );

        let uppercase_digest = RECORD_REPLAY_FIXTURE.replace(
            "5419d3ca2450b8cb8fa2f9664261ff4e68fdd8dadf10c37f39ded05c7378f933",
            "5419D3CA2450B8CB8FA2F9664261FF4E68FDD8DADF10C37F39DED05C7378F933",
        );
        assert_fixture_issue(
            uppercase_digest.as_str(),
            "$.capture_provenance.source_capture_sha256",
            "invalid_source_capture_sha256",
        );

        let unsupported_contract =
            RECORD_REPLAY_FIXTURE.replace("palyra-provider-replay.v1", "palyra-provider-replay.v2");
        assert_fixture_issue(
            unsupported_contract.as_str(),
            "$.capture_provenance.redaction_contract",
            "unsupported_capture_redaction_contract",
        );
    }

    #[test]
    fn schema_v2_rejects_secret_shaped_or_unsafe_capture_labels() {
        for secret_label in ["sk-secret1234567890", "ghp_1234567890abcdef"] {
            let fixture = RECORD_REPLAY_FIXTURE.replace("palyra-qa-mock", secret_label);
            assert_fixture_issue(
                fixture.as_str(),
                "$.capture_provenance.provider_label",
                "secret_shaped_capture_provenance_label",
            );
        }

        for unsafe_label in [r#""../unsafe""#, r#""provider\ncontrol""#] {
            let fixture = RECORD_REPLAY_FIXTURE.replace(
                "model_label: deterministic-agent-runner",
                format!("model_label: {unsafe_label}").as_str(),
            );
            assert_fixture_issue(
                fixture.as_str(),
                "$.capture_provenance.model_label",
                "invalid_capture_provenance_label",
            );
        }

        let oversized_label = "a".repeat(MAX_QA_MOCK_PROVIDER_PROVENANCE_LABEL_LEN + 1);
        let fixture = RECORD_REPLAY_FIXTURE.replace(
            "model_label: deterministic-agent-runner",
            format!("model_label: {oversized_label}").as_str(),
        );
        assert_fixture_issue(
            fixture.as_str(),
            "$.capture_provenance.model_label",
            "capture_provenance_label_too_long",
        );
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
    fn explicit_malformed_failure_retries_into_text_output() {
        let fixture =
            parse_qa_mock_provider_fixture_yaml(RETRY_FIXTURE).expect("retry fixture should parse");
        let request = ProviderRequest::from_input_text(
            "Recover a streamed answer".to_owned(),
            false,
            Vec::new(),
            None,
        );
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("stream recovery prompt should select a turn");

        let first_error = qa_mock_provider_output_for_attempt(turn, 0, &request, None)
            .expect_err("first attempt should fail retryably");
        let final_output = qa_mock_provider_output_for_attempt(turn, 1, &request, None)
            .expect("second attempt should produce text");
        let bypass_error = qa_mock_provider_output_for_turn(turn, &request, None)
            .expect_err("legacy projection must not bypass an explicit retry sequence");

        assert_eq!(turn.attempt_count(), 2);
        assert_eq!(turn.attempt_latency_ms(0), Some(12));
        assert_eq!(turn.attempt_latency_ms(1), Some(8));
        assert_eq!(first_error.retry_count(), 0);
        assert_eq!(first_error.classification().class, ProviderFailureClass::MalformedResponse);
        assert_eq!(first_error.classification().recommended_action, ProviderFailureAction::Retry);
        assert_eq!(
            bypass_error.classification().provider_detail.as_deref(),
            Some("qa_mock_invalid_attempt_plan")
        );
        assert_eq!(final_output.full_text, "Recovered after a retryable malformed response.");
    }

    #[test]
    fn explicit_malformed_failure_retries_into_tool_output() {
        let fixture =
            parse_qa_mock_provider_fixture_yaml(RETRY_FIXTURE).expect("retry fixture should parse");
        let request = ProviderRequest::from_input_text(
            "Recover with a tool call".to_owned(),
            false,
            Vec::new(),
            None,
        );
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("tool recovery prompt should select a turn");

        let first_error = qa_mock_provider_output_for_attempt(turn, 0, &request, None)
            .expect_err("malformed first attempt should fail retryably");
        let final_output = qa_mock_provider_output_for_attempt(turn, 1, &request, None)
            .expect("second attempt should propose a tool");

        assert_eq!(first_error.classification().class, ProviderFailureClass::MalformedResponse);
        assert_eq!(first_error.classification().recommended_action, ProviderFailureAction::Retry);
        assert!(matches!(
            final_output.content_parts.as_slice(),
            [crate::ProviderOutputContentPart::ToolCall { proposal_id, tool_name, .. }]
                if proposal_id == "qa-recovered-read" && tool_name == "palyra.fs.read_file"
        ));
    }

    #[test]
    fn serializes_explicit_attempts_without_conflicting_terminal_behavior() {
        let fixture =
            parse_qa_mock_provider_fixture_yaml(RETRY_FIXTURE).expect("retry fixture should parse");

        let serialized = serde_json::to_value(&fixture).expect("fixture should serialize");
        let turn = &serialized["turns"][0];

        assert!(turn.get("attempts").is_some());
        assert!(turn.get("behavior").is_none());
        assert!(turn.get("match").is_some());
        assert!(turn.get("matcher").is_none());
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
    fn tool_result_matcher_requires_the_configured_outcome() {
        let fixture = parse_qa_mock_provider_fixture_yaml(
            r#"
schema_version: 1
id: qa.mock.tool-result-outcome
turns:
  - id: denied
    match:
      tool_result_call_id: call-01
      tool_result_success: false
    behavior: { kind: text, text: denied }
  - id: completed
    match:
      tool_result_call_id: call-01
      tool_result_success: true
    behavior: { kind: text, text: completed }
"#,
        )
        .expect("outcome-sensitive fixture should parse");
        let mut request = ProviderRequest::from_input_text(
            "Continue after tool result".to_owned(),
            false,
            Vec::new(),
            None,
        );
        request.messages.push(ProviderMessage::tool_result(
            "call-01",
            r#"{"value":"unwrapped successful output"}"#,
        ));

        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("successful tool result should select the success followup");

        assert_eq!(turn.id, "completed");

        request.messages.pop();
        request.messages.push(ProviderMessage::tool_result(
            "call-01",
            r#"{"success":false,"error":"approval denied"}"#,
        ));
        let turn = qa_mock_provider_turn_for_request(&fixture, &request)
            .expect("failed tool result should select the denied followup");
        assert_eq!(turn.id, "denied");
    }

    #[test]
    fn rejects_tool_result_outcome_without_call_id() {
        let error = parse_qa_mock_provider_fixture_yaml(
            r#"
schema_version: 1
id: qa.mock.invalid-tool-result-outcome
turns:
  - id: invalid
    match:
      tool_result_success: true
    behavior: { kind: text, text: invalid }
"#,
        )
        .expect_err("tool-result outcome without identity should be rejected");
        let issues = error.issues().expect("validation issues should be available");

        assert!(
            issues.iter().any(|issue| issue.code == "tool_result_success_requires_call_id"),
            "missing outcome/call-id validation issue: {issues:?}"
        );
    }

    #[test]
    fn schema_snapshot_matches_golden_fixture() {
        let expected: Value =
            serde_json::from_str(SCHEMA_GOLDEN).expect("schema golden should parse");

        assert_eq!(qa_mock_provider_fixture_schema_snapshot(), expected);
    }

    #[test]
    fn rejects_oversized_fixture_documents_and_tool_inputs() {
        let oversized_document = " ".repeat(MAX_QA_MOCK_PROVIDER_FIXTURE_BYTES + 1);
        assert_fixture_issue(oversized_document.as_str(), "$", "fixture_size_exceeded");

        let oversized_tool_input = format!(
            r#"
schema_version: 1
id: qa.mock.oversized-tool-input
turns:
  - id: invalid
    behavior:
      kind: tool_calls
      tool_calls:
        - proposal_id: qa-tool
          tool_name: palyra.echo
          input_json:
            text: "{}"
"#,
            "a".repeat(MAX_TOOL_ARGUMENT_BYTES + 1)
        );
        assert_fixture_issue(
            oversized_tool_input.as_str(),
            "$.turns[0].behavior.tool_calls[0].input_json",
            "tool_call_input_too_large",
        );
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
    fn rejects_attempt_sequences_that_violate_execution_bounds() {
        let cases = [
            (
                r#"
schema_version: 1
id: qa.mock.one-attempt
turns:
  - id: invalid
    attempts:
      - { kind: text, text: done }
"#,
                "insufficient_attempts",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.too-many
turns:
  - id: invalid
    attempts:
      - { kind: stream_error, error_message: retry }
      - { kind: stream_error, error_message: retry }
      - { kind: stream_error, error_message: retry }
      - { kind: stream_error, error_message: retry }
      - { kind: text, text: done }
"#,
                "too_many_attempts",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.per-attempt-latency
turns:
  - id: invalid
    attempts:
      - { kind: stream_error, error_message: retry, latency_ms: 5001 }
      - { kind: text, text: done }
"#,
                "attempt_latency_exceeded",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.total-latency
turns:
  - id: invalid
    attempts:
      - { kind: stream_error, error_message: retry, latency_ms: 4000 }
      - { kind: malformed_output, error_message: retry, latency_ms: 4000 }
      - { kind: text, text: done, latency_ms: 4000 }
"#,
                "total_latency_exceeded",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.non-retryable-middle
turns:
  - id: invalid
    attempts:
      - { kind: context_overflow, error_message: stop }
      - { kind: text, text: done }
"#,
                "non_retryable_intermediate_attempt",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.terminal-failure
turns:
  - id: invalid
    attempts:
      - { kind: stream_error, error_message: retry }
      - { kind: malformed_output, error_message: stop }
"#,
                "terminal_attempt_cannot_succeed",
            ),
            (
                r#"
schema_version: 1
id: qa.mock.conflicting
turns:
  - id: invalid
    behavior: { kind: text, text: fallback }
    attempts:
      - { kind: stream_error, error_message: retry }
      - { kind: text, text: done }
"#,
                "conflicting_attempt_configuration",
            ),
        ];

        for (fixture, expected_code) in cases {
            let error = parse_qa_mock_provider_fixture_yaml(fixture)
                .expect_err("invalid attempt sequence should be rejected");
            let issues = error.issues().expect("validation issues should be available");
            assert!(
                issues.iter().any(|issue| issue.code == expected_code),
                "missing issue code {expected_code}: {issues:?}"
            );
        }
    }

    #[test]
    fn fixture_errors_redact_secret_shaped_fragments() {
        let fixture = parse_qa_mock_provider_fixture_yaml(
            r#"
schema_version: 1
id: qa.mock.redaction
turns:
  - id: redacted
    behavior:
      kind: stream_error
      error_message: "Bearer secret.token api_key=sk-secret123456789"
"#,
        )
        .expect("redaction fixture should parse");
        let request =
            ProviderRequest::from_input_text("redact".to_owned(), false, Vec::new(), None);
        let turn = qa_mock_provider_turn_for_request(&fixture, &request).expect("turn exists");

        let error = qa_mock_provider_output_for_turn(turn, &request, None)
            .expect_err("stream error behavior should fail");
        let message = error.failure_snapshot().message;

        assert!(message.contains("<redacted>"));
        assert!(!message.contains("secret.token"));
        assert!(!message.contains("sk-secret"));
    }

    #[test]
    fn empty_behavior_projects_no_text_or_tools() {
        let fixture = QaMockProviderFixture {
            schema_version: 1,
            id: "qa.mock.empty".to_owned(),
            capture_provenance: None,
            turns: vec![QaMockProviderTurn {
                id: "empty".to_owned(),
                matcher: QaMockProviderTurnMatcher {
                    prompt_contains: Vec::new(),
                    tool_result_call_id: None,
                    tool_result_success: None,
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
                    latency_ms: 0,
                },
                attempts: Vec::new(),
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

    fn assert_fixture_issue(fixture: &str, expected_path: &str, expected_code: &str) {
        let error = parse_qa_mock_provider_fixture_yaml(fixture)
            .expect_err("fixture should fail validation");
        let issues = error.issues().expect("validation issues should be available");

        assert!(
            issues.iter().any(|issue| issue.path == expected_path && issue.code == expected_code),
            "missing issue code={expected_code} path={expected_path}; issues={issues:?}"
        );
    }
}
