//! Provider compatibility fixture corpus for QA Lab.
//!
//! The corpus stores sanitized raw provider responses and errors together
//! with the expected failure class and recovery decision. It is deliberately
//! provider-neutral: fixtures can be replayed by the QA mock provider later,
//! while the current parser/reporting path gives CI a stable contract to
//! validate before runtime normalization is wired in.

use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use crate::{
    contains_remote_secret_fragments, sanitize_remote_error, ProviderFailureClass,
    ProviderFailureClassification, ProviderFailureClassifier, ProviderRecoveryDecisionKind,
    QaMockProviderBehaviorKind,
};

/// Current provider compatibility fixture pack schema version.
pub const PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION: u32 = 1;

/// Stable provider compatibility fixture format label.
pub const PROVIDER_COMPAT_FIXTURE_FORMAT: &str = "palyra-provider-compat-fixture-pack";

/// Stable provider compatibility report format label.
pub const PROVIDER_COMPAT_REPORT_FORMAT: &str = "palyra-provider-compat-report";

const REQUIRED_CATEGORY_VALUES: &[&str] = &[
    "truncated_tool_args",
    "invalid_json_arguments",
    "invalid_tool_name",
    "empty_final_answer",
    "context_overflow",
    "rate_limit",
    "quota",
    "auth_expired",
    "unsupported_schema",
    "malformed_sse_chunk",
    "partial_tool_call",
    "unicode_surrogate",
    "unsupported_multimodal",
    "tool_result_too_large",
    "premature_final_after_patch",
];

const RAW_PAYLOAD_KIND_VALUES: &[&str] = &["response", "error", "stream_chunk"];
const EXPECTED_VERDICT_VALUES: &[&str] = &["recover", "fail_closed"];

/// Parsed and validated provider compatibility fixture pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatFixturePack {
    /// Version of the fixture schema.
    pub schema_version: u32,
    /// Stable pack identifier.
    pub id: String,
    /// Rules that explain why raw payloads are safe to commit.
    pub anonymization: ProviderCompatAnonymizationRules,
    /// Individual provider compatibility cases.
    pub fixtures: Vec<ProviderCompatFixture>,
}

/// Redaction/anonymization rules attached to the fixture pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatAnonymizationRules {
    /// Stable redaction level label used in reports.
    pub redaction_level: String,
    /// Placeholder values allowed in raw payloads instead of real identifiers.
    pub placeholder_values: Vec<String>,
    /// Human-readable notes for maintainers.
    pub notes: Vec<String>,
}

/// One provider compatibility fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatFixture {
    /// Stable fixture id.
    pub id: String,
    /// Compatibility category covered by this fixture.
    pub category: ProviderCompatCategory,
    /// Operator-facing title.
    pub title: String,
    /// Sanitized raw provider response, error, or stream chunk.
    pub raw_payload: ProviderCompatRawPayload,
    /// Mock-provider behavior that can simulate this provider quirk.
    pub mock_behavior: ProviderCompatMockBehavior,
    /// Expected classification and recovery behavior.
    pub expected: ProviderCompatExpectedOutcome,
}

/// Compatibility category covered by a fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCompatCategory {
    TruncatedToolArgs,
    InvalidJsonArguments,
    InvalidToolName,
    EmptyFinalAnswer,
    ContextOverflow,
    RateLimit,
    Quota,
    AuthExpired,
    UnsupportedSchema,
    MalformedSseChunk,
    PartialToolCall,
    UnicodeSurrogate,
    UnsupportedMultimodal,
    ToolResultTooLarge,
    PrematureFinalAfterPatch,
}

impl ProviderCompatCategory {
    /// Returns the stable fixture string value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TruncatedToolArgs => "truncated_tool_args",
            Self::InvalidJsonArguments => "invalid_json_arguments",
            Self::InvalidToolName => "invalid_tool_name",
            Self::EmptyFinalAnswer => "empty_final_answer",
            Self::ContextOverflow => "context_overflow",
            Self::RateLimit => "rate_limit",
            Self::Quota => "quota",
            Self::AuthExpired => "auth_expired",
            Self::UnsupportedSchema => "unsupported_schema",
            Self::MalformedSseChunk => "malformed_sse_chunk",
            Self::PartialToolCall => "partial_tool_call",
            Self::UnicodeSurrogate => "unicode_surrogate",
            Self::UnsupportedMultimodal => "unsupported_multimodal",
            Self::ToolResultTooLarge => "tool_result_too_large",
            Self::PrematureFinalAfterPatch => "premature_final_after_patch",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "truncated_tool_args" => Some(Self::TruncatedToolArgs),
            "invalid_json_arguments" => Some(Self::InvalidJsonArguments),
            "invalid_tool_name" => Some(Self::InvalidToolName),
            "empty_final_answer" => Some(Self::EmptyFinalAnswer),
            "context_overflow" => Some(Self::ContextOverflow),
            "rate_limit" => Some(Self::RateLimit),
            "quota" => Some(Self::Quota),
            "auth_expired" => Some(Self::AuthExpired),
            "unsupported_schema" => Some(Self::UnsupportedSchema),
            "malformed_sse_chunk" => Some(Self::MalformedSseChunk),
            "partial_tool_call" => Some(Self::PartialToolCall),
            "unicode_surrogate" => Some(Self::UnicodeSurrogate),
            "unsupported_multimodal" => Some(Self::UnsupportedMultimodal),
            "tool_result_too_large" => Some(Self::ToolResultTooLarge),
            "premature_final_after_patch" => Some(Self::PrematureFinalAfterPatch),
            _ => None,
        }
    }
}

/// Raw upstream payload kind stored in a fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCompatRawPayloadKind {
    Response,
    Error,
    StreamChunk,
}

impl ProviderCompatRawPayloadKind {
    /// Returns the stable fixture string value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Response => "response",
            Self::Error => "error",
            Self::StreamChunk => "stream_chunk",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "response" => Some(Self::Response),
            "error" => Some(Self::Error),
            "stream_chunk" => Some(Self::StreamChunk),
            _ => None,
        }
    }
}

/// Sanitized raw provider payload for one compatibility case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatRawPayload {
    /// Payload shape.
    pub kind: ProviderCompatRawPayloadKind,
    /// HTTP content type or stream frame type.
    pub content_type: String,
    /// Optional HTTP status code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    /// Sanitized raw response body or stream chunk text.
    pub body: String,
}

/// Mock-provider behavior metadata for a compatibility fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatMockBehavior {
    /// Existing QA mock-provider behavior kind used to simulate this fixture.
    pub kind: QaMockProviderBehaviorKind,
    /// Optional finish reason expected from the provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    /// Optional safe message for negative mock-provider behaviors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Expected outcome for one provider compatibility fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatExpectedOutcome {
    /// High-level result expected from the compatibility layer.
    pub verdict: ProviderCompatExpectedVerdict,
    /// Provider failure class expected after classification.
    pub failure_class: ProviderFailureClass,
    /// Provider recovery decision expected for the fixture.
    pub recovery_decision: ProviderRecoveryDecisionKind,
    /// Whether the runtime must stop rather than guessing.
    pub fail_closed: bool,
    /// Operator-facing recovery path.
    pub recovery_path: String,
}

/// High-level expected fixture outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCompatExpectedVerdict {
    Recover,
    FailClosed,
}

impl ProviderCompatExpectedVerdict {
    /// Returns the stable fixture string value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Recover => "recover",
            Self::FailClosed => "fail_closed",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "recover" => Some(Self::Recover),
            "fail_closed" => Some(Self::FailClosed),
            _ => None,
        }
    }
}

/// Validation issue severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCompatIssueSeverity {
    Error,
}

/// One fixture validation issue with a JSONPath-style location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatFixtureIssue {
    /// Issue severity.
    pub severity: ProviderCompatIssueSeverity,
    /// Stable issue code.
    pub code: String,
    /// JSONPath-style path into the YAML fixture pack.
    pub path: String,
    /// Human-readable issue message.
    pub message: String,
    /// Operator action that fixes the issue.
    pub recovery_hint: String,
}

/// Collection of provider compatibility fixture validation issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCompatValidationError {
    issues: Vec<ProviderCompatFixtureIssue>,
}

impl ProviderCompatValidationError {
    /// Creates a validation error from one or more issues.
    ///
    /// # Panics
    /// Panics when called with an empty issue list.
    #[must_use]
    pub fn new(issues: Vec<ProviderCompatFixtureIssue>) -> Self {
        assert!(!issues.is_empty(), "validation errors require at least one issue");
        Self { issues }
    }

    /// Returns all collected validation issues.
    #[must_use]
    pub fn issues(&self) -> &[ProviderCompatFixtureIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for ProviderCompatValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let first =
            self.issues.first().expect("validation error is constructed with at least one issue");
        write!(formatter, "{} at {}: {}", first.code, first.path, first.message)
    }
}

impl Error for ProviderCompatValidationError {}

/// Provider compatibility fixture parse or validation failure.
#[derive(Debug)]
pub enum ProviderCompatFixtureError {
    /// YAML parsing failed before schema validation could run.
    Parse { source: yaml_serde::Error },
    /// YAML parsed successfully but failed schema validation.
    Invalid(ProviderCompatValidationError),
}

impl ProviderCompatFixtureError {
    /// Returns validation issues when the fixture parsed but failed validation.
    #[must_use]
    pub fn issues(&self) -> Option<&[ProviderCompatFixtureIssue]> {
        match self {
            Self::Parse { .. } => None,
            Self::Invalid(error) => Some(error.issues()),
        }
    }
}

impl fmt::Display for ProviderCompatFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { source } => {
                write!(formatter, "failed to parse provider compatibility fixture YAML: {source}")
            }
            Self::Invalid(error) => {
                write!(formatter, "invalid provider compatibility fixture: {error}")
            }
        }
    }
}

impl Error for ProviderCompatFixtureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse { source } => Some(source),
            Self::Invalid(error) => Some(error),
        }
    }
}

/// Aggregate report for a provider compatibility fixture pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatPackReport {
    pub schema_version: u32,
    pub format: &'static str,
    pub fixture_pack_id: String,
    pub fixture_count: usize,
    pub category_count: usize,
    pub missing_categories: Vec<String>,
    pub raw_payload_redaction: String,
    pub fixtures: Vec<ProviderCompatFixtureReport>,
}

/// Report row for one provider compatibility fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCompatFixtureReport {
    pub id: String,
    pub category: String,
    pub raw_payload_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    pub raw_payload_sha256: String,
    pub redacted_payload_preview: String,
    pub mock_behavior: String,
    pub expected_verdict: String,
    pub expected_failure_class: String,
    pub expected_recovery_decision: String,
    pub fail_closed: bool,
    pub recovery_path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatFixturePackWire {
    schema_version: Option<u32>,
    id: Option<String>,
    anonymization: Option<ProviderCompatAnonymizationRulesWire>,
    fixtures: Option<Vec<ProviderCompatFixtureWire>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatAnonymizationRulesWire {
    redaction_level: Option<String>,
    #[serde(default)]
    placeholder_values: Vec<String>,
    #[serde(default)]
    notes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatFixtureWire {
    id: Option<String>,
    category: Option<String>,
    title: Option<String>,
    raw_payload: Option<ProviderCompatRawPayloadWire>,
    mock_behavior: Option<ProviderCompatMockBehaviorWire>,
    expected: Option<ProviderCompatExpectedOutcomeWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatRawPayloadWire {
    kind: Option<String>,
    content_type: Option<String>,
    #[serde(default)]
    status_code: Option<u16>,
    body: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatMockBehaviorWire {
    kind: Option<String>,
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCompatExpectedOutcomeWire {
    verdict: Option<String>,
    failure_class: Option<String>,
    recovery_decision: Option<String>,
    fail_closed: Option<bool>,
    recovery_path: Option<String>,
}

/// Parses and validates a provider compatibility fixture pack from YAML text.
///
/// # Errors
/// Returns [`ProviderCompatFixtureError::Parse`] when YAML cannot be
/// deserialized, or [`ProviderCompatFixtureError::Invalid`] with path-qualified
/// issues when the pack violates the schema or redaction rules.
pub fn parse_provider_compat_fixture_pack_yaml(
    text: &str,
) -> Result<ProviderCompatFixturePack, ProviderCompatFixtureError> {
    let wire = yaml_serde::from_str::<ProviderCompatFixturePackWire>(text)
        .map_err(|source| ProviderCompatFixtureError::Parse { source })?;
    build_validated_fixture_pack(wire)
}

/// Returns the versioned schema snapshot used by provider compatibility tooling.
#[must_use]
pub fn provider_compat_fixture_schema_snapshot() -> Value {
    json!({
        "schema_version": PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION,
        "format": PROVIDER_COMPAT_FIXTURE_FORMAT,
        "encoding": "yaml",
        "examples_root": "qa/fixtures/provider_compat",
        "required_sections": [
            "schema_version",
            "id",
            "anonymization",
            "fixtures"
        ],
        "required_categories": REQUIRED_CATEGORY_VALUES,
        "raw_payload_kinds": RAW_PAYLOAD_KIND_VALUES,
        "expected_verdicts": EXPECTED_VERDICT_VALUES,
        "path_convention": "jsonpath",
        "redaction": {
            "raw_payloads_must_be_sanitized": true,
            "credential_placeholders": [
                "<redacted>",
                "<provider_response_id>",
                "<provider_request_id>",
                "<provider_account_id>"
            ]
        }
    })
}

/// Builds a report showing category coverage, failure class, and recovery path.
#[must_use]
pub fn provider_compat_fixture_pack_report(
    pack: &ProviderCompatFixturePack,
) -> ProviderCompatPackReport {
    let seen_categories =
        pack.fixtures.iter().map(|fixture| fixture.category.as_str()).collect::<BTreeSet<_>>();
    let missing_categories = REQUIRED_CATEGORY_VALUES
        .iter()
        .copied()
        .filter(|category| !seen_categories.contains(category))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let fixtures = pack.fixtures.iter().map(provider_compat_fixture_report).collect::<Vec<_>>();
    ProviderCompatPackReport {
        schema_version: PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION,
        format: PROVIDER_COMPAT_REPORT_FORMAT,
        fixture_pack_id: pack.id.clone(),
        fixture_count: pack.fixtures.len(),
        category_count: seen_categories.len(),
        missing_categories,
        raw_payload_redaction: pack.anonymization.redaction_level.clone(),
        fixtures,
    }
}

/// Classifies one compatibility fixture with the runtime provider classifier.
#[must_use]
pub fn provider_compat_fixture_classification(
    fixture: &ProviderCompatFixture,
) -> ProviderFailureClassification {
    ProviderFailureClassifier::new().classify_fixture_category(
        fixture.category.as_str(),
        fixture.raw_payload.status_code,
        fixture.id.as_str(),
    )
}

fn provider_compat_fixture_report(fixture: &ProviderCompatFixture) -> ProviderCompatFixtureReport {
    ProviderCompatFixtureReport {
        id: fixture.id.clone(),
        category: fixture.category.as_str().to_owned(),
        raw_payload_kind: fixture.raw_payload.kind.as_str().to_owned(),
        status_code: fixture.raw_payload.status_code,
        raw_payload_sha256: sha256_hex(fixture.raw_payload.body.as_bytes()),
        redacted_payload_preview: redact_provider_compat_raw_payload(
            fixture.raw_payload.body.as_str(),
        ),
        mock_behavior: fixture.mock_behavior.kind.as_str().to_owned(),
        expected_verdict: fixture.expected.verdict.as_str().to_owned(),
        expected_failure_class: fixture.expected.failure_class.as_str().to_owned(),
        expected_recovery_decision: fixture.expected.recovery_decision.as_str().to_owned(),
        fail_closed: fixture.expected.fail_closed,
        recovery_path: fixture.expected.recovery_path.clone(),
    }
}

/// Returns a bounded, secret-safe preview of a raw provider payload.
#[must_use]
pub fn redact_provider_compat_raw_payload(body: &str) -> String {
    sanitize_remote_error(body)
}

fn build_validated_fixture_pack(
    wire: ProviderCompatFixturePackWire,
) -> Result<ProviderCompatFixturePack, ProviderCompatFixtureError> {
    let mut issues = Vec::new();
    let schema_version = validate_schema_version(wire.schema_version, &mut issues);
    let id = validate_required_slug(wire.id, "$.id", "fixture pack id", &mut issues);
    let anonymization = validate_anonymization(wire.anonymization, &mut issues);
    let fixtures = validate_fixtures(wire.fixtures, &mut issues);
    validate_required_category_coverage(fixtures.as_deref(), &mut issues);

    if issues.is_empty() {
        Ok(ProviderCompatFixturePack {
            schema_version: schema_version.unwrap_or(PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION),
            id: id.unwrap_or_default(),
            anonymization: anonymization.unwrap_or(ProviderCompatAnonymizationRules {
                redaction_level: String::new(),
                placeholder_values: Vec::new(),
                notes: Vec::new(),
            }),
            fixtures: fixtures.unwrap_or_default(),
        })
    } else {
        Err(ProviderCompatFixtureError::Invalid(ProviderCompatValidationError::new(issues)))
    }
}

fn validate_schema_version(
    value: Option<u32>,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<u32> {
    match value {
        Some(PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION) => {
            Some(PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION)
        }
        Some(value) => {
            push_issue(
                issues,
                "unsupported_schema_version",
                "$.schema_version",
                format!(
                    "schema_version must be {PROVIDER_COMPAT_FIXTURE_SCHEMA_VERSION}, got {value}"
                ),
                "Update the fixture pack to the supported provider compatibility schema version.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_schema_version",
                "$.schema_version",
                "schema_version is required",
                "Add the supported provider compatibility schema version.",
            );
            None
        }
    }
}

fn validate_anonymization(
    value: Option<ProviderCompatAnonymizationRulesWire>,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatAnonymizationRules> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_anonymization",
            "$.anonymization",
            "anonymization rules are required",
            "Declare how raw provider payloads are sanitized before committing fixtures.",
        );
        return None;
    };
    let redaction_level = validate_required_string(
        value.redaction_level,
        "$.anonymization.redaction_level",
        "redaction level",
        issues,
    );
    let placeholder_values = validate_string_list(
        value.placeholder_values,
        "$.anonymization.placeholder_values",
        "placeholder value",
        true,
        issues,
    );
    let notes = validate_string_list(value.notes, "$.anonymization.notes", "note", false, issues);
    match (redaction_level, placeholder_values, notes) {
        (Some(redaction_level), Some(placeholder_values), Some(notes)) => {
            Some(ProviderCompatAnonymizationRules { redaction_level, placeholder_values, notes })
        }
        _ => None,
    }
}

fn validate_fixtures(
    value: Option<Vec<ProviderCompatFixtureWire>>,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<Vec<ProviderCompatFixture>> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_fixtures",
            "$.fixtures",
            "fixtures section is required",
            "Add at least one provider compatibility fixture.",
        );
        return None;
    };
    if value.is_empty() {
        push_issue(
            issues,
            "empty_fixtures",
            "$.fixtures",
            "fixtures must contain at least one fixture",
            "Add one fixture per required provider compatibility category.",
        );
        return Some(Vec::new());
    }

    let mut seen_ids = BTreeSet::new();
    let mut fixtures = Vec::with_capacity(value.len());
    for (index, fixture) in value.into_iter().enumerate() {
        let path = format!("$.fixtures[{index}]");
        let id =
            validate_required_slug(fixture.id, format!("{path}.id").as_str(), "fixture id", issues);
        if let Some(id_value) = id.as_ref() {
            if !seen_ids.insert(id_value.clone()) {
                push_issue(
                    issues,
                    "duplicate_fixture_id",
                    format!("{path}.id"),
                    format!("fixture id '{id_value}' is duplicated"),
                    "Use a unique id for every provider compatibility fixture.",
                );
            }
        }
        let category =
            validate_category(fixture.category, format!("{path}.category").as_str(), issues);
        let title = validate_required_string(
            fixture.title,
            format!("{path}.title").as_str(),
            "title",
            issues,
        );
        let raw_payload = validate_raw_payload(
            fixture.raw_payload,
            format!("{path}.raw_payload").as_str(),
            issues,
        );
        let mock_behavior = validate_mock_behavior(
            fixture.mock_behavior,
            format!("{path}.mock_behavior").as_str(),
            issues,
        );
        let expected =
            validate_expected(fixture.expected, format!("{path}.expected").as_str(), issues);
        if let (
            Some(id),
            Some(category),
            Some(title),
            Some(raw_payload),
            Some(mock_behavior),
            Some(expected),
        ) = (id, category, title, raw_payload, mock_behavior, expected)
        {
            fixtures.push(ProviderCompatFixture {
                id,
                category,
                title,
                raw_payload,
                mock_behavior,
                expected,
            });
        }
    }
    Some(fixtures)
}

fn validate_category(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatCategory> {
    let value = validate_required_string(value, path, "compatibility category", issues)?;
    match ProviderCompatCategory::parse(value.as_str()) {
        Some(category) => Some(category),
        None => {
            push_issue(
                issues,
                "invalid_category",
                path,
                format!("unsupported provider compatibility category '{value}'"),
                format!(
                    "Use one of the supported categories: {}.",
                    REQUIRED_CATEGORY_VALUES.join(", ")
                ),
            );
            None
        }
    }
}

fn validate_raw_payload(
    value: Option<ProviderCompatRawPayloadWire>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatRawPayload> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_raw_payload",
            path,
            "raw payload is required",
            "Add a sanitized raw provider response, error, or stream chunk.",
        );
        return None;
    };
    let kind = validate_raw_payload_kind(value.kind, format!("{path}.kind").as_str(), issues);
    let content_type = validate_required_string(
        value.content_type,
        format!("{path}.content_type").as_str(),
        "content type",
        issues,
    );
    let body =
        validate_required_string(value.body, format!("{path}.body").as_str(), "body", issues);
    if let Some(body) = body.as_ref() {
        validate_raw_payload_is_sanitized(body, format!("{path}.body").as_str(), issues);
    }
    match (kind, content_type, body) {
        (Some(kind), Some(content_type), Some(body)) => Some(ProviderCompatRawPayload {
            kind,
            content_type,
            status_code: value.status_code,
            body,
        }),
        _ => None,
    }
}

fn validate_raw_payload_kind(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatRawPayloadKind> {
    let value = validate_required_string(value, path, "raw payload kind", issues)?;
    match ProviderCompatRawPayloadKind::parse(value.as_str()) {
        Some(kind) => Some(kind),
        None => {
            push_issue(
                issues,
                "invalid_raw_payload_kind",
                path,
                format!("unsupported raw payload kind '{value}'"),
                format!("Use one of: {}.", RAW_PAYLOAD_KIND_VALUES.join(", ")),
            );
            None
        }
    }
}

fn validate_mock_behavior(
    value: Option<ProviderCompatMockBehaviorWire>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatMockBehavior> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_mock_behavior",
            path,
            "mock behavior is required",
            "Attach the QA mock-provider behavior that can simulate this compatibility fixture.",
        );
        return None;
    };
    let kind = validate_mock_behavior_kind(value.kind, format!("{path}.kind").as_str(), issues);
    let finish_reason = validate_optional_nonempty(
        value.finish_reason,
        format!("{path}.finish_reason").as_str(),
        issues,
    );
    let error_message = validate_optional_nonempty(
        value.error_message,
        format!("{path}.error_message").as_str(),
        issues,
    );
    kind.map(|kind| ProviderCompatMockBehavior { kind, finish_reason, error_message })
}

fn validate_mock_behavior_kind(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<QaMockProviderBehaviorKind> {
    let value = validate_required_string(value, path, "mock behavior kind", issues)?;
    match parse_mock_behavior_kind(value.as_str()) {
        Some(kind) => Some(kind),
        None => {
            push_issue(
                issues,
                "invalid_mock_behavior_kind",
                path,
                format!("unsupported QA mock-provider behavior kind '{value}'"),
                "Use an existing QA mock-provider behavior kind.",
            );
            None
        }
    }
}

fn validate_expected(
    value: Option<ProviderCompatExpectedOutcomeWire>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatExpectedOutcome> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_expected",
            path,
            "expected outcome is required",
            "Declare expected verdict, failure class, recovery decision, and recovery path.",
        );
        return None;
    };
    let verdict =
        validate_expected_verdict(value.verdict, format!("{path}.verdict").as_str(), issues);
    let failure_class = validate_failure_class(
        value.failure_class,
        format!("{path}.failure_class").as_str(),
        issues,
    );
    let recovery_decision = validate_recovery_decision(
        value.recovery_decision,
        format!("{path}.recovery_decision").as_str(),
        issues,
    );
    let fail_closed = validate_required_bool(
        value.fail_closed,
        format!("{path}.fail_closed").as_str(),
        "fail_closed",
        issues,
    );
    let recovery_path = validate_required_string(
        value.recovery_path,
        format!("{path}.recovery_path").as_str(),
        "recovery path",
        issues,
    );
    match (verdict, failure_class, recovery_decision, fail_closed, recovery_path) {
        (
            Some(verdict),
            Some(failure_class),
            Some(recovery_decision),
            Some(fail_closed),
            Some(recovery_path),
        ) => Some(ProviderCompatExpectedOutcome {
            verdict,
            failure_class,
            recovery_decision,
            fail_closed,
            recovery_path,
        }),
        _ => None,
    }
}

fn validate_expected_verdict(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderCompatExpectedVerdict> {
    let value = validate_required_string(value, path, "expected verdict", issues)?;
    match ProviderCompatExpectedVerdict::parse(value.as_str()) {
        Some(verdict) => Some(verdict),
        None => {
            push_issue(
                issues,
                "invalid_expected_verdict",
                path,
                format!("unsupported expected verdict '{value}'"),
                format!("Use one of: {}.", EXPECTED_VERDICT_VALUES.join(", ")),
            );
            None
        }
    }
}

fn validate_failure_class(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderFailureClass> {
    let value = validate_required_string(value, path, "provider failure class", issues)?;
    match parse_failure_class(value.as_str()) {
        Some(class) => Some(class),
        None => {
            push_issue(
                issues,
                "invalid_failure_class",
                path,
                format!("unsupported provider failure class '{value}'"),
                "Use an existing ProviderFailureClass snake_case value.",
            );
            None
        }
    }
}

fn validate_recovery_decision(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<ProviderRecoveryDecisionKind> {
    let value = validate_required_string(value, path, "provider recovery decision", issues)?;
    match parse_recovery_decision(value.as_str()) {
        Some(decision) => Some(decision),
        None => {
            push_issue(
                issues,
                "invalid_recovery_decision",
                path,
                format!("unsupported provider recovery decision '{value}'"),
                "Use an existing ProviderRecoveryDecisionKind snake_case value.",
            );
            None
        }
    }
}

fn validate_required_category_coverage(
    fixtures: Option<&[ProviderCompatFixture]>,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) {
    let seen = fixtures
        .unwrap_or_default()
        .iter()
        .map(|fixture| fixture.category.as_str())
        .collect::<BTreeSet<_>>();
    for category in REQUIRED_CATEGORY_VALUES {
        if !seen.contains(category) {
            push_issue(
                issues,
                "missing_required_category",
                "$.fixtures",
                format!("required provider compatibility category '{category}' is missing"),
                "Add at least one fixture for every required provider compatibility category.",
            );
        }
    }
}

fn validate_raw_payload_is_sanitized(
    body: &str,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) {
    if contains_remote_secret_fragments(body) {
        push_issue(
            issues,
            "raw_payload_contains_secret",
            path,
            "raw provider payload contains credential-shaped material",
            "Replace provider tokens, API keys, and secrets with <redacted> placeholders.",
        );
    }
}

fn validate_required_slug(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<String> {
    let value = validate_required_string(value, path, label, issues)?;
    if value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-' | '.'))
    {
        Some(value)
    } else {
        push_issue(
            issues,
            "invalid_slug",
            path,
            format!("{label} must use lowercase ASCII letters, digits, '.', '_' or '-'"),
            format!("Rename the {label} to a stable machine-readable slug."),
        );
        None
    }
}

fn validate_required_string(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Some(value),
        Some(_) => {
            push_issue(
                issues,
                "empty_string",
                path,
                format!("{label} must not be empty"),
                format!("Provide a non-empty {label}."),
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_required_field",
                path,
                format!("{label} is required"),
                format!("Add the required {label}."),
            );
            None
        }
    }
}

fn validate_optional_nonempty(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<String> {
    match value {
        Some(value) if value.trim().is_empty() => {
            push_issue(
                issues,
                "empty_string",
                path,
                "optional string must not be empty when present",
                "Remove the field or provide a non-empty value.",
            );
            None
        }
        value => value,
    }
}

fn validate_required_bool(
    value: Option<bool>,
    path: &str,
    label: &str,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<bool> {
    match value {
        Some(value) => Some(value),
        None => {
            push_issue(
                issues,
                "missing_required_field",
                path,
                format!("{label} is required"),
                format!("Add the required {label}."),
            );
            None
        }
    }
}

fn validate_string_list(
    values: Vec<String>,
    path: &str,
    label: &str,
    required_nonempty: bool,
    issues: &mut Vec<ProviderCompatFixtureIssue>,
) -> Option<Vec<String>> {
    if required_nonempty && values.is_empty() {
        push_issue(
            issues,
            "empty_list",
            path,
            format!("{label} list must not be empty"),
            format!("Add at least one {label}."),
        );
        return Some(Vec::new());
    }
    let mut valid = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        if value.trim().is_empty() {
            push_issue(
                issues,
                "empty_string",
                item_path,
                format!("{label} must not be empty"),
                format!("Remove the empty {label} or provide a non-empty value."),
            );
        } else {
            valid.push(value);
        }
    }
    Some(valid)
}

fn parse_mock_behavior_kind(value: &str) -> Option<QaMockProviderBehaviorKind> {
    match value {
        "text" => Some(QaMockProviderBehaviorKind::Text),
        "tool_calls" => Some(QaMockProviderBehaviorKind::ToolCalls),
        "empty" => Some(QaMockProviderBehaviorKind::Empty),
        "context_overflow" => Some(QaMockProviderBehaviorKind::ContextOverflow),
        "malformed_output" => Some(QaMockProviderBehaviorKind::MalformedOutput),
        "malformed_tool_args" => Some(QaMockProviderBehaviorKind::MalformedToolArgs),
        "stream_error" => Some(QaMockProviderBehaviorKind::StreamError),
        "approval_required" => Some(QaMockProviderBehaviorKind::ApprovalRequired),
        _ => None,
    }
}

fn parse_failure_class(value: &str) -> Option<ProviderFailureClass> {
    match value {
        "auth_invalid" => Some(ProviderFailureClass::AuthInvalid),
        "auth_expired" => Some(ProviderFailureClass::AuthExpired),
        "permission_denied" => Some(ProviderFailureClass::PermissionDenied),
        "rate_limit" => Some(ProviderFailureClass::RateLimit),
        "rate_limited" => Some(ProviderFailureClass::RateLimited),
        "quota" => Some(ProviderFailureClass::Quota),
        "quota_exceeded" => Some(ProviderFailureClass::QuotaExceeded),
        "schema_rejected" => Some(ProviderFailureClass::SchemaRejected),
        "bad_tool_arguments" => Some(ProviderFailureClass::BadToolArguments),
        "truncated_tool_arguments" => Some(ProviderFailureClass::TruncatedToolArguments),
        "context_overflow" => Some(ProviderFailureClass::ContextOverflow),
        "transient_upstream" => Some(ProviderFailureClass::TransientUpstream),
        "permanent_upstream" => Some(ProviderFailureClass::PermanentUpstream),
        "context_window_exceeded" => Some(ProviderFailureClass::ContextWindowExceeded),
        "content_policy_blocked" => Some(ProviderFailureClass::ContentPolicyBlocked),
        "malformed_response" => Some(ProviderFailureClass::MalformedResponse),
        "malformed_stream" => Some(ProviderFailureClass::MalformedStream),
        "empty_output" => Some(ProviderFailureClass::EmptyOutput),
        "premature_final" => Some(ProviderFailureClass::PrematureFinal),
        "payload_too_large" => Some(ProviderFailureClass::PayloadTooLarge),
        "provider_unavailable" => Some(ProviderFailureClass::ProviderUnavailable),
        "network_unavailable" => Some(ProviderFailureClass::NetworkUnavailable),
        "provider_timeout" => Some(ProviderFailureClass::ProviderTimeout),
        "unsupported_multimodal" => Some(ProviderFailureClass::UnsupportedMultimodal),
        _ => None,
    }
}

fn parse_recovery_decision(value: &str) -> Option<ProviderRecoveryDecisionKind> {
    match value {
        "retry_same_provider" => Some(ProviderRecoveryDecisionKind::RetrySameProvider),
        "retry_after" => Some(ProviderRecoveryDecisionKind::RetryAfter),
        "retry_transformed" => Some(ProviderRecoveryDecisionKind::RetryTransformed),
        "refresh_credential" => Some(ProviderRecoveryDecisionKind::RefreshCredential),
        "failover_provider" => Some(ProviderRecoveryDecisionKind::FailoverProvider),
        "compact_and_retry" => Some(ProviderRecoveryDecisionKind::CompactAndRetry),
        "ask_user" => Some(ProviderRecoveryDecisionKind::AskUser),
        "abort" => Some(ProviderRecoveryDecisionKind::Abort),
        "fail_closed" => Some(ProviderRecoveryDecisionKind::FailClosed),
        _ => None,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn push_issue(
    issues: &mut Vec<ProviderCompatFixtureIssue>,
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) {
    issues.push(ProviderCompatFixtureIssue {
        severity: ProviderCompatIssueSeverity::Error,
        code: code.into(),
        path: path.into(),
        message: message.into(),
        recovery_hint: recovery_hint.into(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderError, ProviderFailureAction};

    const EXAMPLE_PACK: &str =
        include_str!("../../../qa/fixtures/provider_compat/p0_provider_compat_pack.yaml");

    #[test]
    fn fixture_pack_covers_required_categories() {
        let pack = parse_provider_compat_fixture_pack_yaml(EXAMPLE_PACK)
            .expect("provider compatibility fixture pack should parse");

        let report = provider_compat_fixture_pack_report(&pack);

        assert_eq!(report.fixture_count, REQUIRED_CATEGORY_VALUES.len());
        assert_eq!(report.category_count, REQUIRED_CATEGORY_VALUES.len());
        assert!(report.missing_categories.is_empty());
    }

    #[test]
    fn report_exposes_failure_class_and_recovery_path() {
        let pack = parse_provider_compat_fixture_pack_yaml(EXAMPLE_PACK)
            .expect("provider compatibility fixture pack should parse");
        let report = provider_compat_fixture_pack_report(&pack);

        let context_overflow = report
            .fixtures
            .iter()
            .find(|fixture| fixture.category == "context_overflow")
            .expect("context overflow fixture should be present");

        assert_eq!(
            context_overflow.expected_failure_class,
            ProviderFailureClass::ContextOverflow.as_str()
        );
        assert_eq!(context_overflow.expected_recovery_decision, "compact_and_retry");
        assert!(context_overflow.recovery_path.to_ascii_lowercase().contains("compact"));
    }

    #[test]
    fn fixture_expected_outcomes_match_provider_classifier() {
        let pack = parse_provider_compat_fixture_pack_yaml(EXAMPLE_PACK)
            .expect("provider compatibility fixture pack should parse");

        for fixture in pack.fixtures.iter() {
            let classification = provider_compat_fixture_classification(fixture);
            assert_eq!(
                classification.class, fixture.expected.failure_class,
                "fixture {} should match classifier failure class",
                fixture.id
            );
            let retryable = classification.recommended_action == ProviderFailureAction::Retry;
            let error = ProviderError::RequestFailed {
                message: fixture
                    .mock_behavior
                    .error_message
                    .clone()
                    .unwrap_or_else(|| fixture.title.clone()),
                retryable,
                retry_count: 0,
                classification,
            };
            assert_eq!(
                error.envelope().recovery_decision.decision,
                fixture.expected.recovery_decision,
                "fixture {} should match classifier recovery decision",
                fixture.id
            );
        }
    }

    #[test]
    fn rejects_unredacted_secret_material_in_raw_payload() {
        let error = parse_provider_compat_fixture_pack_yaml(
            r#"
schema_version: 1
id: provider_compat.bad_secret
anonymization:
  redaction_level: sanitized_raw_payload
  placeholder_values:
    - <redacted>
fixtures:
  - id: invalid_json_arguments
    category: invalid_json_arguments
    title: Invalid JSON arguments
    raw_payload:
      kind: error
      content_type: application/json
      status_code: 400
      body: "provider leaked api_key=sk-live-secret1234567890"
    mock_behavior:
      kind: malformed_tool_args
      error_message: "invalid arguments"
    expected:
      verdict: fail_closed
      failure_class: malformed_response
      recovery_decision: fail_closed
      fail_closed: true
      recovery_path: "Reject the malformed tool arguments."
"#,
        )
        .expect_err("unredacted secret should fail validation");

        let issues = error.issues().expect("validation issue should be available");
        assert!(issues.iter().any(|issue| issue.code == "raw_payload_contains_secret"));
    }

    #[test]
    fn raw_payload_secret_validation_scans_beyond_placeholders_and_preview_limits() {
        let body = format!("<redacted> {} sk-live-secret1234567890", "visible ".repeat(80));
        let mut issues = Vec::new();

        validate_raw_payload_is_sanitized(
            body.as_str(),
            "$.fixtures[0].raw_payload.body",
            &mut issues,
        );

        assert!(issues.iter().any(|issue| issue.code == "raw_payload_contains_secret"));
        let mut placeholder_issues = Vec::new();
        validate_raw_payload_is_sanitized(
            "Bearer <redacted> token=<redacted>",
            "$.fixtures[0].raw_payload.body",
            &mut placeholder_issues,
        );
        assert!(placeholder_issues.is_empty());
    }

    #[test]
    fn invalid_tool_json_fixture_is_fail_closed_bad_tool_arguments() {
        let pack = parse_provider_compat_fixture_pack_yaml(EXAMPLE_PACK)
            .expect("provider compatibility fixture pack should parse");
        let fixture = pack
            .fixtures
            .iter()
            .find(|fixture| fixture.category == ProviderCompatCategory::InvalidJsonArguments)
            .expect("invalid JSON arguments fixture should be present");

        assert_eq!(fixture.expected.verdict, ProviderCompatExpectedVerdict::FailClosed);
        assert_eq!(fixture.expected.failure_class, ProviderFailureClass::BadToolArguments);
        assert_eq!(fixture.expected.recovery_decision, ProviderRecoveryDecisionKind::FailClosed);
        assert!(fixture.expected.fail_closed);
    }

    #[test]
    fn unsupported_multimodal_fixture_requires_transformed_retry() {
        let pack = parse_provider_compat_fixture_pack_yaml(EXAMPLE_PACK)
            .expect("provider compatibility fixture pack should parse");
        let fixture = pack
            .fixtures
            .iter()
            .find(|fixture| fixture.category == ProviderCompatCategory::UnsupportedMultimodal)
            .expect("unsupported multimodal fixture should be present");
        let classification = provider_compat_fixture_classification(fixture);
        let snapshot = classification.snapshot("unsupported multimodal".to_owned());

        assert_eq!(fixture.expected.failure_class, ProviderFailureClass::UnsupportedMultimodal);
        assert_eq!(
            fixture.expected.recovery_decision,
            ProviderRecoveryDecisionKind::RetryTransformed
        );
        assert_eq!(snapshot.recovery.action, "retry_transformed");
    }

    #[test]
    fn schema_snapshot_matches_golden_fixture() {
        let expected = json!({
            "schema_version": 1,
            "format": "palyra-provider-compat-fixture-pack",
            "encoding": "yaml",
            "examples_root": "qa/fixtures/provider_compat",
            "required_sections": [
                "schema_version",
                "id",
                "anonymization",
                "fixtures"
            ],
            "required_categories": REQUIRED_CATEGORY_VALUES,
            "raw_payload_kinds": RAW_PAYLOAD_KIND_VALUES,
            "expected_verdicts": EXPECTED_VERDICT_VALUES,
            "path_convention": "jsonpath",
            "redaction": {
                "raw_payloads_must_be_sanitized": true,
                "credential_placeholders": [
                    "<redacted>",
                    "<provider_response_id>",
                    "<provider_request_id>",
                    "<provider_account_id>"
                ]
            }
        });

        assert_eq!(provider_compat_fixture_schema_snapshot(), expected);
    }
}
