//! QA Lab scenario manifest schema and validation.
//!
//! The manifest is authored as YAML for operator readability, then converted
//! into validated Rust types before QA runners consume it.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    path::{Component, Path},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Current QA scenario manifest schema version.
pub const QA_SCENARIO_SCHEMA_VERSION: u32 = 2;

/// Stable format label embedded in the schema snapshot and reports.
pub const QA_SCENARIO_FORMAT: &str = "palyra-qa-scenario";

const MAX_TIMEOUT_MS: u64 = 3_600_000;

const AREA_VALUES: &[&str] = &[
    "text",
    "tools",
    "approvals",
    "memory",
    "browser",
    "provider",
    "workflow",
    "security",
    "replay",
];
const SUPPORTED_SCHEMA_VERSIONS: &[u32] = &[1, QA_SCENARIO_SCHEMA_VERSION];
const PROVIDER_MODE_VALUES: &[&str] = &["mock", "recorded", "live"];
const RUNNER_MODE_VALUES: &[&str] = &["fixture", "record_replay", "live"];
const APPROVAL_DECISION_VALUES: &[&str] = &["allow", "deny"];
const STEP_ACTION_VALUES: &[&str] =
    &["user_prompt", "tool_result", "approval_decision", "wait_for_event"];
const TERMINAL_STATE_VALUES: &[&str] = &["completed", "failed", "cancelled", "approval_required"];
const ARTIFACT_KIND_VALUES: &[&str] =
    &["report", "transcript", "replay_bundle", "trajectory", "evidence"];

/// Validated QA scenario manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaScenarioManifest {
    /// Version of the QA scenario schema used by this manifest.
    pub schema_version: u32,
    /// Stable scenario identifier used for reports and artifact stems.
    pub id: String,
    /// Optional human-readable scenario title.
    pub title: Option<String>,
    /// Product area the scenario exercises.
    pub area: QaScenarioArea,
    /// Provider and execution posture.
    pub mode: QaScenarioMode,
    /// Typed runner inputs for schema-v2 fixture scenarios.
    pub runner: Option<QaScenarioRunnerConfig>,
    /// Runtime capabilities and fixtures required before the scenario can run.
    pub requires: QaScenarioRequires,
    /// Ordered operator/runtime steps.
    pub steps: Vec<QaScenarioStep>,
    /// Expected terminal outcome and observable assertions.
    pub expect: QaScenarioExpect,
    /// Events, tool calls, artifacts, or answer claims that must not appear.
    pub forbidden: QaScenarioForbidden,
    /// Artifacts the runner should produce or inspect.
    pub artifacts: Vec<QaScenarioArtifact>,
    /// Maturity labels used by QA Lab scorecards.
    pub maturity: QaScenarioMaturity,
    /// Per-run and optional per-step timeout budget.
    pub timeout: QaScenarioTimeout,
}

impl Serialize for QaScenarioManifest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let is_schema_v1 = self.schema_version == 1;
        let projection = QaScenarioManifestSerialization {
            schema_version: self.schema_version,
            id: self.id.as_str(),
            title: self.title.as_deref(),
            area: self.area,
            mode: QaScenarioModeSerialization {
                provider: self.mode.provider,
                // Schema v1 has no runner field even though validation derives one internally.
                runner: (!is_schema_v1).then_some(self.mode.runner),
                deterministic: self.mode.deterministic,
            },
            runner: if is_schema_v1 { None } else { self.runner.as_ref() },
            requires: &self.requires,
            steps: self.steps.as_slice(),
            expect: &self.expect,
            forbidden: &self.forbidden,
            artifacts: self.artifacts.as_slice(),
            maturity: &self.maturity,
            timeout: &self.timeout,
        };
        projection.serialize(serializer)
    }
}

#[derive(Serialize)]
struct QaScenarioManifestSerialization<'a> {
    schema_version: u32,
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    area: QaScenarioArea,
    mode: QaScenarioModeSerialization,
    #[serde(skip_serializing_if = "Option::is_none")]
    runner: Option<&'a QaScenarioRunnerConfig>,
    requires: &'a QaScenarioRequires,
    steps: &'a [QaScenarioStep],
    expect: &'a QaScenarioExpect,
    forbidden: &'a QaScenarioForbidden,
    artifacts: &'a [QaScenarioArtifact],
    maturity: &'a QaScenarioMaturity,
    timeout: &'a QaScenarioTimeout,
}

#[derive(Serialize)]
struct QaScenarioModeSerialization {
    provider: QaScenarioProviderMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    runner: Option<QaScenarioRunnerMode>,
    deterministic: bool,
}

/// Scenario product area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioArea {
    Text,
    Tools,
    Approvals,
    Memory,
    Browser,
    Provider,
    Workflow,
    Security,
    Replay,
}

impl QaScenarioArea {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Tools => "tools",
            Self::Approvals => "approvals",
            Self::Memory => "memory",
            Self::Browser => "browser",
            Self::Provider => "provider",
            Self::Workflow => "workflow",
            Self::Security => "security",
            Self::Replay => "replay",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "tools" => Some(Self::Tools),
            "approvals" => Some(Self::Approvals),
            "memory" => Some(Self::Memory),
            "browser" => Some(Self::Browser),
            "provider" => Some(Self::Provider),
            "workflow" => Some(Self::Workflow),
            "security" => Some(Self::Security),
            "replay" => Some(Self::Replay),
            _ => None,
        }
    }
}

/// Provider mode used by a QA scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioProviderMode {
    Mock,
    Recorded,
    Live,
}

impl QaScenarioProviderMode {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Recorded => "recorded",
            Self::Live => "live",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "mock" => Some(Self::Mock),
            "recorded" => Some(Self::Recorded),
            "live" => Some(Self::Live),
            _ => None,
        }
    }
}

/// Canonical execution lane used by a QA scenario runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioRunnerMode {
    Fixture,
    RecordReplay,
    Live,
}

impl QaScenarioRunnerMode {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::RecordReplay => "record_replay",
            Self::Live => "live",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "fixture" => Some(Self::Fixture),
            "record_replay" => Some(Self::RecordReplay),
            "live" => Some(Self::Live),
            _ => None,
        }
    }
}

impl From<QaScenarioProviderMode> for QaScenarioRunnerMode {
    fn from(value: QaScenarioProviderMode) -> Self {
        match value {
            QaScenarioProviderMode::Mock => Self::Fixture,
            QaScenarioProviderMode::Recorded => Self::RecordReplay,
            QaScenarioProviderMode::Live => Self::Live,
        }
    }
}

impl From<QaScenarioRunnerMode> for QaScenarioProviderMode {
    fn from(value: QaScenarioRunnerMode) -> Self {
        match value {
            QaScenarioRunnerMode::Fixture => Self::Mock,
            QaScenarioRunnerMode::RecordReplay => Self::Recorded,
            QaScenarioRunnerMode::Live => Self::Live,
        }
    }
}

/// Execution mode for a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioMode {
    /// Legacy provider representation retained for schema-v1 and CLI compatibility.
    pub provider: QaScenarioProviderMode,
    /// Canonical runner execution lane.
    pub runner: QaScenarioRunnerMode,
    /// Whether the scenario is expected to be deterministic.
    pub deterministic: bool,
}

/// Validated fixture inputs used to prepare an isolated QA runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioRunnerConfig {
    /// Repository-relative deterministic provider fixture path.
    pub provider_fixture: String,
    /// Optional repository-relative workspace fixture path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_fixture: Option<String>,
    /// Optional non-empty policy profile identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_profile: Option<String>,
}

/// Runtime requirements declared by a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioRequires {
    /// Model capability family required by the scenario.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Runtime capabilities required before execution.
    pub capabilities: Vec<String>,
    /// Tool identifiers expected to be available.
    pub tools: Vec<String>,
    /// Fixture files required by the scenario.
    pub fixtures: Vec<String>,
}

/// Step action kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioStepAction {
    UserPrompt,
    ToolResult,
    ApprovalDecision,
    WaitForEvent,
}

impl QaScenarioStepAction {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserPrompt => "user_prompt",
            Self::ToolResult => "tool_result",
            Self::ApprovalDecision => "approval_decision",
            Self::WaitForEvent => "wait_for_event",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "user_prompt" => Some(Self::UserPrompt),
            "tool_result" => Some(Self::ToolResult),
            "approval_decision" => Some(Self::ApprovalDecision),
            "wait_for_event" => Some(Self::WaitForEvent),
            _ => None,
        }
    }
}

/// Approval action selected by a QA scenario step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QaScenarioApprovalDecision {
    Allow,
    Deny,
    /// Non-empty schema-v1 value retained without changing legacy semantics.
    Legacy(String),
}

impl QaScenarioApprovalDecision {
    /// Returns the manifest string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::Legacy(value) => value.as_str(),
        }
    }
}

impl Serialize for QaScenarioApprovalDecision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// One ordered scenario step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioStep {
    /// Stable step identifier scoped to one scenario.
    pub id: String,
    /// Action the runner must perform or await.
    pub action: QaScenarioStepAction,
    /// User prompt text for `user_prompt` steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Tool name for tool-result oriented steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Runtime event name for wait-oriented steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Approval decision for approval-oriented steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<QaScenarioApprovalDecision>,
    /// Optional proposal identifier selecting the approval request to resolve.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
}

/// Expected terminal run state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioTerminalState {
    Completed,
    Failed,
    Cancelled,
    ApprovalRequired,
}

impl QaScenarioTerminalState {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::ApprovalRequired => "approval_required",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "approval_required" => Some(Self::ApprovalRequired),
            _ => None,
        }
    }
}

/// Text assertion for a final answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioTextAssertion {
    /// Exact answer text, when the scenario requires it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equals: Option<String>,
    /// Required substrings that must appear in the answer.
    pub contains: Vec<String>,
}

/// Expected runtime event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioExpectedEvent {
    /// Runtime event type.
    pub event_type: String,
    /// Optional minimum count for the event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_count: Option<u32>,
}

/// Expected tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioExpectedToolCall {
    /// Tool identifier or glob-like token understood by the runner.
    pub name: String,
    /// Optional minimum call count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_count: Option<u32>,
    /// Required tool-result outcome; omitted manifests default to success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
}

/// Observable expectations for a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioExpect {
    /// Required terminal run state.
    pub terminal_state: QaScenarioTerminalState,
    /// Optional final-answer assertion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_answer: Option<QaScenarioTextAssertion>,
    /// Expected runtime events.
    pub events: Vec<QaScenarioExpectedEvent>,
    /// Expected tool calls.
    pub tool_calls: Vec<QaScenarioExpectedToolCall>,
}

/// Forbidden runtime observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioForbidden {
    /// Tool call names that must not appear.
    pub tool_calls: Vec<String>,
    /// Event types that must not appear.
    pub events: Vec<String>,
    /// Artifact path or kind tokens that must not appear.
    pub artifacts: Vec<String>,
    /// Final-answer substrings that must not appear.
    pub claims: Vec<String>,
}

/// Scenario artifact kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioArtifactKind {
    Report,
    Transcript,
    ReplayBundle,
    Trajectory,
    Evidence,
}

impl QaScenarioArtifactKind {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Transcript => "transcript",
            Self::ReplayBundle => "replay_bundle",
            Self::Trajectory => "trajectory",
            Self::Evidence => "evidence",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "report" => Some(Self::Report),
            "transcript" => Some(Self::Transcript),
            "replay_bundle" => Some(Self::ReplayBundle),
            "trajectory" => Some(Self::Trajectory),
            "evidence" => Some(Self::Evidence),
            _ => None,
        }
    }
}

/// Artifact expected from a scenario run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioArtifact {
    /// Repository-relative artifact path.
    pub path: String,
    /// Artifact category.
    pub kind: QaScenarioArtifactKind,
    /// Whether the artifact is required for a passing verdict.
    pub required: bool,
}

/// Scenario maturity metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioMaturity {
    /// Stable labels such as `p0`, `deterministic`, or `text_only`.
    pub labels: Vec<String>,
}

/// Timeout budget for one scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioTimeout {
    /// Whole-run timeout in milliseconds.
    pub run_ms: u64,
    /// Optional per-step timeout in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_ms: Option<u64>,
}

/// Validation issue severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioIssueSeverity {
    Error,
}

/// One manifest validation issue with a JSONPath-style location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioManifestIssue {
    /// Issue severity.
    pub severity: QaScenarioIssueSeverity,
    /// Stable issue code for automation.
    pub code: String,
    /// JSONPath-style location in the YAML manifest.
    pub path: String,
    /// Human-readable issue message.
    pub message: String,
    /// Operator action that fixes the issue.
    pub recovery_hint: String,
}

/// Collection of manifest validation issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaScenarioValidationError {
    issues: Vec<QaScenarioManifestIssue>,
}

impl QaScenarioValidationError {
    /// Creates a validation error from one or more issues.
    ///
    /// # Panics
    /// Panics when called with an empty issue list. Empty issue lists represent
    /// a successful validation and should not be converted into an error.
    #[must_use]
    pub fn new(issues: Vec<QaScenarioManifestIssue>) -> Self {
        assert!(!issues.is_empty(), "validation errors require at least one issue");
        Self { issues }
    }

    /// Returns all collected validation issues.
    #[must_use]
    pub fn issues(&self) -> &[QaScenarioManifestIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for QaScenarioValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let first =
            self.issues.first().expect("validation error is constructed with at least one issue");
        write!(formatter, "{} at {}: {}", first.code, first.path, first.message)
    }
}

impl Error for QaScenarioValidationError {}

/// Manifest parse or validation failure.
#[derive(Debug)]
pub enum QaScenarioManifestError {
    /// YAML parsing failed before schema validation could run.
    Parse { source: yaml_serde::Error },
    /// YAML parsed successfully but failed schema validation.
    Invalid(QaScenarioValidationError),
}

impl QaScenarioManifestError {
    /// Returns validation issues when the manifest parsed but failed validation.
    #[must_use]
    pub fn issues(&self) -> Option<&[QaScenarioManifestIssue]> {
        match self {
            Self::Parse { .. } => None,
            Self::Invalid(error) => Some(error.issues()),
        }
    }
}

impl fmt::Display for QaScenarioManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { source } => {
                write!(formatter, "failed to parse QA scenario YAML: {source}")
            }
            Self::Invalid(error) => write!(formatter, "invalid QA scenario manifest: {error}"),
        }
    }
}

impl Error for QaScenarioManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse { source } => Some(source),
            Self::Invalid(error) => Some(error),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioManifestWire {
    schema_version: Option<u32>,
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    area: Option<String>,
    mode: Option<QaScenarioModeWire>,
    #[serde(default)]
    runner: Option<QaScenarioRunnerConfigWire>,
    requires: Option<QaScenarioRequiresWire>,
    steps: Option<Vec<QaScenarioStepWire>>,
    expect: Option<QaScenarioExpectWire>,
    forbidden: Option<QaScenarioForbiddenWire>,
    artifacts: Option<Vec<QaScenarioArtifactWire>>,
    maturity: Option<QaScenarioMaturityWire>,
    timeout: Option<QaScenarioTimeoutWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioModeWire {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    runner: Option<String>,
    #[serde(default = "default_deterministic")]
    deterministic: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioRunnerConfigWire {
    #[serde(default)]
    provider_fixture: Option<String>,
    #[serde(default)]
    workspace_fixture: Option<String>,
    #[serde(default)]
    policy_profile: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioRequiresWire {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    tools: Vec<String>,
    #[serde(default)]
    fixtures: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioStepWire {
    id: Option<String>,
    action: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    decision: Option<String>,
    #[serde(default)]
    proposal_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioExpectWire {
    terminal_state: Option<String>,
    #[serde(default)]
    final_answer: Option<QaScenarioTextAssertionWire>,
    #[serde(default)]
    events: Vec<QaScenarioExpectedEventWire>,
    #[serde(default)]
    tool_calls: Vec<QaScenarioExpectedToolCallWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioTextAssertionWire {
    #[serde(default)]
    equals: Option<String>,
    #[serde(default)]
    contains: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioExpectedEventWire {
    event_type: Option<String>,
    #[serde(default)]
    min_count: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioExpectedToolCallWire {
    name: Option<String>,
    #[serde(default)]
    min_count: Option<u32>,
    #[serde(default)]
    success: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioForbiddenWire {
    #[serde(default)]
    tool_calls: Vec<String>,
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    artifacts: Vec<String>,
    #[serde(default)]
    claims: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioArtifactWire {
    path: Option<String>,
    kind: Option<String>,
    #[serde(default = "default_required")]
    required: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioMaturityWire {
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioTimeoutWire {
    run_ms: Option<u64>,
    #[serde(default)]
    step_ms: Option<u64>,
}

/// Parses and validates a QA scenario manifest from YAML text.
///
/// # Errors
/// Returns [`QaScenarioManifestError::Parse`] when the YAML cannot be
/// deserialized, or [`QaScenarioManifestError::Invalid`] with path-qualified
/// issues when the manifest violates the schema.
pub fn parse_qa_scenario_manifest_yaml(
    text: &str,
) -> Result<QaScenarioManifest, QaScenarioManifestError> {
    let wire = yaml_serde::from_str::<QaScenarioManifestWire>(text)
        .map_err(|source| QaScenarioManifestError::Parse { source })?;
    build_validated_manifest(wire)
}

/// Returns the versioned schema snapshot used by QA tooling and tests.
#[must_use]
pub fn qa_scenario_manifest_schema_snapshot() -> Value {
    json!({
        "schema_version": QA_SCENARIO_SCHEMA_VERSION,
        "format": QA_SCENARIO_FORMAT,
        "encoding": "yaml",
        "examples_root": "qa/scenarios",
        "required_sections": [
            "schema_version",
            "id",
            "area",
            "mode",
            "requires",
            "steps",
            "expect",
            "forbidden",
            "artifacts",
            "maturity",
            "timeout"
        ],
        "areas": AREA_VALUES,
        "supported_schema_versions": SUPPORTED_SCHEMA_VERSIONS,
        "provider_modes": PROVIDER_MODE_VALUES,
        "runner_modes": RUNNER_MODE_VALUES,
        "approval_decisions": APPROVAL_DECISION_VALUES,
        "runner_config_fields": [
            "provider_fixture",
            "workspace_fixture",
            "policy_profile"
        ],
        "expected_tool_call_fields": ["name", "min_count", "success"],
        "step_actions": STEP_ACTION_VALUES,
        "terminal_states": TERMINAL_STATE_VALUES,
        "artifact_kinds": ARTIFACT_KIND_VALUES,
        "forbidden_fields": [
            "tool_calls",
            "events",
            "artifacts",
            "claims"
        ],
        "path_convention": "jsonpath",
        "limits": {
            "max_timeout_ms": MAX_TIMEOUT_MS
        }
    })
}

fn build_validated_manifest(
    wire: QaScenarioManifestWire,
) -> Result<QaScenarioManifest, QaScenarioManifestError> {
    let mut issues = Vec::new();
    let schema_version = validate_schema_version(wire.schema_version, &mut issues);
    let id = validate_required_slug(wire.id, "$.id", "scenario id", &mut issues);
    let title = validate_optional_nonempty(wire.title, "$.title", &mut issues);
    let area = validate_area(wire.area, &mut issues);
    let mode = validate_mode(wire.mode, schema_version, &mut issues);
    let runner = validate_runner_config(
        wire.runner,
        schema_version,
        mode.as_ref().map(|mode| mode.runner),
        &mut issues,
    );
    let requires = validate_requires(wire.requires, &mut issues);
    let steps = validate_steps(wire.steps, schema_version, &mut issues);
    let expect = validate_expect(wire.expect, &mut issues);
    let forbidden = validate_forbidden(wire.forbidden, &mut issues);
    let artifacts = validate_artifacts(wire.artifacts, &mut issues);
    let maturity = validate_maturity(wire.maturity, &mut issues);
    let timeout = validate_timeout(wire.timeout, &mut issues);

    if !issues.is_empty() {
        return Err(QaScenarioManifestError::Invalid(QaScenarioValidationError::new(issues)));
    }

    Ok(QaScenarioManifest {
        schema_version: schema_version.expect("schema version validated without issues"),
        id: id.expect("id validated without issues"),
        title,
        area: area.expect("area validated without issues"),
        mode: mode.expect("mode validated without issues"),
        runner,
        requires: requires.expect("requires validated without issues"),
        steps: steps.expect("steps validated without issues"),
        expect: expect.expect("expect validated without issues"),
        forbidden: forbidden.expect("forbidden validated without issues"),
        artifacts: artifacts.expect("artifacts validated without issues"),
        maturity: maturity.expect("maturity validated without issues"),
        timeout: timeout.expect("timeout validated without issues"),
    })
}

fn validate_schema_version(
    value: Option<u32>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<u32> {
    match value {
        Some(version) if SUPPORTED_SCHEMA_VERSIONS.contains(&version) => Some(version),
        Some(other) => {
            push_issue(
                issues,
                "unsupported_schema_version",
                "$.schema_version",
                format!(
                    "expected schema_version {}, got {other}",
                    SUPPORTED_SCHEMA_VERSIONS
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(" or ")
                ),
                format!("Set schema_version to {QA_SCENARIO_SCHEMA_VERSION}."),
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_schema_version",
                "$.schema_version",
                "schema_version is required",
                format!(
                    "Add schema_version: {QA_SCENARIO_SCHEMA_VERSION} to the scenario manifest."
                ),
            );
            None
        }
    }
}

fn validate_area(
    value: Option<String>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioArea> {
    let value = validate_required_string(value, "$.area", "area", issues)?;
    if let Some(area) = QaScenarioArea::parse(value.as_str()) {
        return Some(area);
    }
    push_issue(
        issues,
        "unknown_area",
        "$.area",
        format!("unknown area '{value}', expected one of {}", AREA_VALUES.join(", ")),
        "Use a supported QA scenario area.",
    );
    None
}

fn validate_mode(
    value: Option<QaScenarioModeWire>,
    schema_version: Option<u32>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioMode> {
    let Some(value) = value else {
        let recovery_hint = if schema_version == Some(1) {
            "Add mode.provider with mock, recorded, or live."
        } else {
            "Add mode.runner with fixture, record_replay, or live."
        };
        push_issue(issues, "missing_mode", "$.mode", "mode section is required", recovery_hint);
        return None;
    };
    if schema_version == Some(1) || (schema_version.is_none() && value.runner.is_none()) {
        return validate_legacy_mode(value, issues);
    }
    validate_current_mode(value, issues)
}

fn validate_legacy_mode(
    value: QaScenarioModeWire,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioMode> {
    if value.runner.is_some() {
        push_issue(
            issues,
            "runner_mode_requires_schema_v2",
            "$.mode.runner",
            "mode.runner is not supported by schema_version 1",
            format!("Use schema_version {QA_SCENARIO_SCHEMA_VERSION} for canonical runner modes."),
        );
    }
    let provider = validate_enum_string(
        value.provider,
        "$.mode.provider",
        "provider mode",
        PROVIDER_MODE_VALUES,
        issues,
    )
    .and_then(|provider| QaScenarioProviderMode::parse(provider.as_str()))?;
    Some(QaScenarioMode { provider, runner: provider.into(), deterministic: value.deterministic })
}

fn validate_current_mode(
    value: QaScenarioModeWire,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioMode> {
    let runner = validate_enum_string(
        value.runner,
        "$.mode.runner",
        "runner mode",
        RUNNER_MODE_VALUES,
        issues,
    )
    .and_then(|runner| QaScenarioRunnerMode::parse(runner.as_str()))?;
    let provider = QaScenarioProviderMode::from(runner);
    if let Some(compatibility_provider) = value.provider {
        let compatibility_provider = validate_enum_string(
            Some(compatibility_provider),
            "$.mode.provider",
            "provider mode",
            PROVIDER_MODE_VALUES,
            issues,
        )
        .and_then(|value| QaScenarioProviderMode::parse(value.as_str()));
        if compatibility_provider.is_some_and(|compatibility| compatibility != provider) {
            push_issue(
                issues,
                "runner_provider_mode_mismatch",
                "$.mode.provider",
                format!(
                    "provider mode must be '{}' when mode.runner is '{}'",
                    provider.as_str(),
                    runner.as_str()
                ),
                "Remove mode.provider or make it match the canonical runner mode.",
            );
        }
    }
    Some(QaScenarioMode { provider, runner, deterministic: value.deterministic })
}

fn validate_runner_config(
    value: Option<QaScenarioRunnerConfigWire>,
    schema_version: Option<u32>,
    runner_mode: Option<QaScenarioRunnerMode>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioRunnerConfig> {
    let schema_version = schema_version?;
    if schema_version == 1 {
        if value.is_some() {
            push_issue(
                issues,
                "runner_config_requires_schema_v2",
                "$.runner",
                "runner configuration is not supported by schema_version 1",
                format!(
                    "Use schema_version {QA_SCENARIO_SCHEMA_VERSION} for runner configuration."
                ),
            );
        }
        return None;
    }
    match runner_mode {
        Some(QaScenarioRunnerMode::Fixture) => {
            let Some(value) = value else {
                push_issue(
                    issues,
                    "missing_runner_config",
                    "$.runner",
                    "fixture runner mode requires runner configuration",
                    "Add runner.provider_fixture with a safe repository-relative fixture path.",
                );
                return None;
            };
            let provider_fixture = validate_runner_path(
                value.provider_fixture,
                "$.runner.provider_fixture",
                "provider fixture",
                true,
                issues,
            );
            let workspace_fixture = validate_runner_path(
                value.workspace_fixture,
                "$.runner.workspace_fixture",
                "workspace fixture",
                false,
                issues,
            );
            let policy_profile =
                validate_optional_nonempty(value.policy_profile, "$.runner.policy_profile", issues);
            provider_fixture.map(|provider_fixture| QaScenarioRunnerConfig {
                provider_fixture,
                workspace_fixture,
                policy_profile,
            })
        }
        Some(QaScenarioRunnerMode::RecordReplay | QaScenarioRunnerMode::Live) => {
            if value.is_some() {
                push_issue(
                    issues,
                    "runner_config_not_supported_for_mode",
                    "$.runner",
                    "runner configuration is only supported for fixture mode",
                    "Remove the runner section for record_replay or live mode.",
                );
            }
            None
        }
        None => None,
    }
}

fn validate_requires(
    value: Option<QaScenarioRequiresWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioRequires> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_requires",
            "$.requires",
            "requires section is required",
            "Declare the capabilities, tools, and fixtures needed before running the scenario.",
        );
        return None;
    };
    let model = validate_optional_nonempty(value.model, "$.requires.model", issues);
    validate_slug_list(
        value.capabilities.as_slice(),
        "$.requires.capabilities",
        "capability",
        true,
        issues,
    );
    validate_string_list(
        value.tools.as_slice(),
        "$.requires.tools",
        "tool requirement",
        false,
        issues,
    );
    validate_string_list(
        value.fixtures.as_slice(),
        "$.requires.fixtures",
        "fixture path",
        false,
        issues,
    );
    Some(QaScenarioRequires {
        model,
        capabilities: value.capabilities,
        tools: value.tools,
        fixtures: value.fixtures,
    })
}

fn validate_steps(
    value: Option<Vec<QaScenarioStepWire>>,
    schema_version: Option<u32>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<Vec<QaScenarioStep>> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_steps",
            "$.steps",
            "steps section is required",
            "Add at least one ordered scenario step.",
        );
        return None;
    };
    if value.is_empty() {
        push_issue(
            issues,
            "empty_steps",
            "$.steps",
            "steps must contain at least one entry",
            "Add the first scenario step, for example a user_prompt step.",
        );
        return None;
    }

    let mut seen_step_ids = BTreeSet::new();
    let mut steps = Vec::with_capacity(value.len());
    for (index, step) in value.into_iter().enumerate() {
        let path = format!("$.steps[{index}]");
        let id = validate_required_slug(step.id, format!("{path}.id").as_str(), "step id", issues);
        if let Some(id) = &id {
            if !seen_step_ids.insert(id.clone()) {
                push_issue(
                    issues,
                    "duplicate_step_id",
                    format!("{path}.id"),
                    format!("step id '{id}' is declared more than once"),
                    "Use a unique id for every step in the scenario.",
                );
            }
        }
        let action = validate_step_action(step.action, format!("{path}.action").as_str(), issues);
        let prompt =
            validate_optional_nonempty(step.prompt, format!("{path}.prompt").as_str(), issues);
        let tool = validate_optional_nonempty(step.tool, format!("{path}.tool").as_str(), issues);
        let event =
            validate_optional_nonempty(step.event, format!("{path}.event").as_str(), issues);
        let decision = validate_approval_decision(
            step.decision,
            schema_version,
            format!("{path}.decision").as_str(),
            issues,
        );
        let proposal_id = validate_optional_nonempty(
            step.proposal_id,
            format!("{path}.proposal_id").as_str(),
            issues,
        );
        validate_step_shape(
            &path,
            action,
            prompt.as_deref(),
            tool.as_deref(),
            event.as_deref(),
            decision.as_ref(),
            issues,
        );
        if let (Some(id), Some(action)) = (id, action) {
            steps.push(QaScenarioStep { id, action, prompt, tool, event, decision, proposal_id });
        }
    }
    Some(steps)
}

fn validate_step_action(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioStepAction> {
    let value = validate_required_string(value, path, "step action", issues)?;
    if let Some(action) = QaScenarioStepAction::parse(value.as_str()) {
        return Some(action);
    }
    push_issue(
        issues,
        "unknown_step_action",
        path,
        format!("unknown step action '{value}', expected one of {}", STEP_ACTION_VALUES.join(", ")),
        "Use a supported QA scenario step action.",
    );
    None
}

fn validate_approval_decision(
    value: Option<String>,
    schema_version: Option<u32>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioApprovalDecision> {
    let value = validate_optional_nonempty(value, path, issues)?;
    match value.as_str() {
        "allow" => Some(QaScenarioApprovalDecision::Allow),
        "deny" => Some(QaScenarioApprovalDecision::Deny),
        _ if schema_version == Some(1) => Some(QaScenarioApprovalDecision::Legacy(value)),
        _ => {
            push_issue(
                issues,
                "unknown_approval_decision",
                path,
                format!(
                    "unknown approval decision '{value}', expected one of {}",
                    APPROVAL_DECISION_VALUES.join(", ")
                ),
                "Use allow or deny for a schema-v2 approval decision.",
            );
            None
        }
    }
}

fn validate_step_shape(
    path: &str,
    action: Option<QaScenarioStepAction>,
    prompt: Option<&str>,
    tool: Option<&str>,
    event: Option<&str>,
    decision: Option<&QaScenarioApprovalDecision>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) {
    match action {
        Some(QaScenarioStepAction::UserPrompt) if prompt.is_none() => push_issue(
            issues,
            "missing_step_prompt",
            format!("{path}.prompt"),
            "user_prompt steps require prompt",
            "Add prompt text for the user_prompt step.",
        ),
        Some(QaScenarioStepAction::ToolResult) if tool.is_none() => push_issue(
            issues,
            "missing_step_tool",
            format!("{path}.tool"),
            "tool_result steps require tool",
            "Declare which tool the step supplies a result for.",
        ),
        Some(QaScenarioStepAction::WaitForEvent) if event.is_none() => push_issue(
            issues,
            "missing_step_event",
            format!("{path}.event"),
            "wait_for_event steps require event",
            "Declare the runtime event type the runner should wait for.",
        ),
        Some(QaScenarioStepAction::ApprovalDecision) if decision.is_none() => push_issue(
            issues,
            "missing_step_decision",
            format!("{path}.decision"),
            "approval_decision steps require decision",
            "Declare the approval decision the runner should apply.",
        ),
        _ => {}
    }
}

fn validate_expect(
    value: Option<QaScenarioExpectWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioExpect> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_expect",
            "$.expect",
            "expect section is required",
            "Declare the terminal state and observable assertions.",
        );
        return None;
    };
    let terminal_state = validate_terminal_state(value.terminal_state, issues);
    let final_answer = value
        .final_answer
        .map(|answer| validate_text_assertion(answer, "$.expect.final_answer", issues));
    let events = validate_expected_events(value.events, issues);
    let tool_calls = validate_expected_tool_calls(value.tool_calls, issues);
    if final_answer.is_none() && events.is_empty() && tool_calls.is_empty() {
        push_issue(
            issues,
            "empty_expectations",
            "$.expect",
            "expect must include final_answer, events, or tool_calls assertions",
            "Add at least one observable assertion to the expect section.",
        );
    }
    Some(QaScenarioExpect {
        terminal_state: terminal_state?,
        final_answer: final_answer.flatten(),
        events,
        tool_calls,
    })
}

fn validate_terminal_state(
    value: Option<String>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioTerminalState> {
    let value =
        validate_required_string(value, "$.expect.terminal_state", "terminal state", issues)?;
    if let Some(state) = QaScenarioTerminalState::parse(value.as_str()) {
        return Some(state);
    }
    push_issue(
        issues,
        "unknown_terminal_state",
        "$.expect.terminal_state",
        format!(
            "unknown terminal state '{value}', expected one of {}",
            TERMINAL_STATE_VALUES.join(", ")
        ),
        "Use a supported terminal state.",
    );
    None
}

fn validate_text_assertion(
    value: QaScenarioTextAssertionWire,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioTextAssertion> {
    let equals =
        validate_optional_nonempty(value.equals, format!("{path}.equals").as_str(), issues);
    validate_string_list(
        value.contains.as_slice(),
        format!("{path}.contains").as_str(),
        "required final-answer substring",
        false,
        issues,
    );
    if equals.is_none() && value.contains.is_empty() {
        push_issue(
            issues,
            "empty_text_assertion",
            path,
            "final_answer must include equals or contains",
            "Add an exact answer or at least one required substring.",
        );
        return None;
    }
    Some(QaScenarioTextAssertion { equals, contains: value.contains })
}

fn validate_expected_events(
    values: Vec<QaScenarioExpectedEventWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Vec<QaScenarioExpectedEvent> {
    let mut events = Vec::with_capacity(values.len());
    for (index, event) in values.into_iter().enumerate() {
        let path = format!("$.expect.events[{index}]");
        if let Some(event_type) = validate_required_string(
            event.event_type,
            format!("{path}.event_type").as_str(),
            "event type",
            issues,
        ) {
            events.push(QaScenarioExpectedEvent { event_type, min_count: event.min_count });
        }
    }
    events
}

fn validate_expected_tool_calls(
    values: Vec<QaScenarioExpectedToolCallWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Vec<QaScenarioExpectedToolCall> {
    let mut tool_calls = Vec::with_capacity(values.len());
    for (index, tool_call) in values.into_iter().enumerate() {
        let path = format!("$.expect.tool_calls[{index}]");
        if let Some(name) = validate_required_string(
            tool_call.name,
            format!("{path}.name").as_str(),
            "tool call name",
            issues,
        ) {
            tool_calls.push(QaScenarioExpectedToolCall {
                name,
                min_count: tool_call.min_count,
                success: tool_call.success,
            });
        }
    }
    tool_calls
}

fn validate_forbidden(
    value: Option<QaScenarioForbiddenWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioForbidden> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_forbidden",
            "$.forbidden",
            "forbidden section is required",
            "Declare forbidden tool calls, events, artifacts, and claims; use empty lists if none apply.",
        );
        return None;
    };
    validate_string_list(
        value.tool_calls.as_slice(),
        "$.forbidden.tool_calls",
        "forbidden tool call",
        false,
        issues,
    );
    validate_string_list(
        value.events.as_slice(),
        "$.forbidden.events",
        "forbidden event",
        false,
        issues,
    );
    validate_string_list(
        value.artifacts.as_slice(),
        "$.forbidden.artifacts",
        "forbidden artifact",
        false,
        issues,
    );
    validate_string_list(
        value.claims.as_slice(),
        "$.forbidden.claims",
        "forbidden claim",
        false,
        issues,
    );
    Some(QaScenarioForbidden {
        tool_calls: value.tool_calls,
        events: value.events,
        artifacts: value.artifacts,
        claims: value.claims,
    })
}

fn validate_artifacts(
    value: Option<Vec<QaScenarioArtifactWire>>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<Vec<QaScenarioArtifact>> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_artifacts",
            "$.artifacts",
            "artifacts section is required",
            "Declare expected artifacts; use an empty list when the scenario produces none.",
        );
        return None;
    };
    let mut artifacts = Vec::with_capacity(value.len());
    for (index, artifact) in value.into_iter().enumerate() {
        let path = format!("$.artifacts[{index}]");
        let artifact_path =
            validate_artifact_path(artifact.path, format!("{path}.path").as_str(), issues);
        let kind = validate_artifact_kind(artifact.kind, format!("{path}.kind").as_str(), issues);
        if let (Some(path), Some(kind)) = (artifact_path, kind) {
            artifacts.push(QaScenarioArtifact { path, kind, required: artifact.required });
        }
    }
    Some(artifacts)
}

fn validate_artifact_kind(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioArtifactKind> {
    let value = validate_required_string(value, path, "artifact kind", issues)?;
    if let Some(kind) = QaScenarioArtifactKind::parse(value.as_str()) {
        return Some(kind);
    }
    push_issue(
        issues,
        "unknown_artifact_kind",
        path,
        format!(
            "unknown artifact kind '{value}', expected one of {}",
            ARTIFACT_KIND_VALUES.join(", ")
        ),
        "Use a supported scenario artifact kind.",
    );
    None
}

fn validate_artifact_path(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<String> {
    let value = validate_required_string(value, path, "artifact path", issues)?;
    let value_path = Path::new(value.as_str());
    if value_path.is_absolute()
        || value_path.components().any(|component| matches!(component, Component::ParentDir))
    {
        push_issue(
            issues,
            "unsafe_artifact_path",
            path,
            format!("artifact path '{value}' must be relative and must not contain '..'"),
            "Use a repository-relative artifact path below qa/reports or qa/fixtures.",
        );
        return None;
    }
    Some(value)
}

fn validate_runner_path(
    value: Option<String>,
    path: &str,
    label: &str,
    required: bool,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<String> {
    let value = if required {
        validate_required_string(value, path, label, issues)
    } else {
        validate_optional_nonempty(value, path, issues)
    }?;
    if runner_path_is_unsafe(value.as_str()) {
        push_issue(
            issues,
            "unsafe_runner_path",
            path,
            format!("{label} path '{value}' must be relative and must not contain '..'"),
            "Use a repository-relative path without parent traversal or a drive prefix.",
        );
        return None;
    }
    Some(value)
}

fn runner_path_is_unsafe(value: &str) -> bool {
    let path = Path::new(value);
    path.is_absolute()
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
        || value.contains('\0')
        || path.components().any(|component| matches!(component, Component::ParentDir))
        || value.split(['/', '\\']).any(|segment| segment == "..")
}

fn validate_maturity(
    value: Option<QaScenarioMaturityWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioMaturity> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_maturity",
            "$.maturity",
            "maturity section is required",
            "Add maturity.labels for scorecards and QA Lab filtering.",
        );
        return None;
    };
    validate_slug_list(
        value.labels.as_slice(),
        "$.maturity.labels",
        "maturity label",
        true,
        issues,
    );
    Some(QaScenarioMaturity { labels: value.labels })
}

fn validate_timeout(
    value: Option<QaScenarioTimeoutWire>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioTimeout> {
    let Some(value) = value else {
        push_issue(
            issues,
            "missing_timeout",
            "$.timeout",
            "timeout section is required",
            "Add a bounded timeout.run_ms value.",
        );
        return None;
    };
    let run_ms = validate_timeout_value(value.run_ms, "$.timeout.run_ms", issues)?;
    if let Some(step_ms) = value.step_ms {
        validate_timeout_value(Some(step_ms), "$.timeout.step_ms", issues)?;
        if step_ms > run_ms {
            push_issue(
                issues,
                "invalid_step_timeout",
                "$.timeout.step_ms",
                "step_ms must be less than or equal to run_ms",
                "Keep per-step timeout within the whole-run timeout.",
            );
        }
    }
    Some(QaScenarioTimeout { run_ms, step_ms: value.step_ms })
}

fn validate_timeout_value(
    value: Option<u64>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<u64> {
    match value {
        Some(value) if (1..=MAX_TIMEOUT_MS).contains(&value) => Some(value),
        Some(value) => {
            push_issue(
                issues,
                "invalid_timeout",
                path,
                format!("timeout must be in range 1..={MAX_TIMEOUT_MS}, got {value}"),
                "Use a positive bounded timeout in milliseconds.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_timeout_value",
                path,
                "timeout value is required",
                "Add a timeout value in milliseconds.",
            );
            None
        }
    }
}

fn validate_enum_string(
    value: Option<String>,
    path: &str,
    label: &str,
    known: &[&str],
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<String> {
    let value = validate_required_string(value, path, label, issues)?;
    if known.contains(&value.as_str()) {
        return Some(value);
    }
    push_issue(
        issues,
        "unknown_enum_value",
        path,
        format!("unknown {label} '{value}', expected one of {}", known.join(", ")),
        format!("Use a supported {label}."),
    );
    None
}

fn validate_required_slug(
    value: Option<String>,
    path: &str,
    label: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
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
    issues: &mut Vec<QaScenarioManifestIssue>,
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
    issues: &mut Vec<QaScenarioManifestIssue>,
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

fn validate_slug_list(
    values: &[String],
    path: &str,
    label: &str,
    require_nonempty: bool,
    issues: &mut Vec<QaScenarioManifestIssue>,
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
    validate_string_list(values, path, label, false, issues);
    for (index, value) in values.iter().enumerate() {
        if !value.trim().is_empty() && !is_slug(value.trim()) {
            push_issue(
                issues,
                "invalid_slug",
                format!("{path}[{index}]"),
                format!("{label} '{value}' must be a lowercase slug"),
                format!("Rename the {label} to a lowercase ASCII slug."),
            );
        }
    }
}

fn validate_string_list(
    values: &[String],
    path: &str,
    label: &str,
    require_nonempty: bool,
    issues: &mut Vec<QaScenarioManifestIssue>,
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
    issues: &mut Vec<QaScenarioManifestIssue>,
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) {
    issues.push(QaScenarioManifestIssue {
        severity: QaScenarioIssueSeverity::Error,
        code: code.into(),
        path: path.into(),
        message: message.into(),
        recovery_hint: recovery_hint.into(),
    });
}

const fn default_deterministic() -> bool {
    true
}

const fn default_required() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_SCENARIO: &str = include_str!("../../../qa/scenarios/text_run_basic.yaml");
    const LEGACY_APPROVAL_SCENARIO: &str =
        include_str!("../../../qa/scenarios/mcp/mcp_write_tool_approval.yaml");
    const SCHEMA_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_scenario_manifest_schema.json");
    const V2_FIXTURE_SCENARIO: &str = r#"
schema_version: 2
id: runner.fixture.basic
area: approvals
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
requires:
  model: text
  capabilities: [agent_run, qa_lab]
  tools: [palyra.fs.read_file]
  fixtures: []
steps:
  - id: prompt
    action: user_prompt
    prompt: "Inspect the fixture."
  - id: deny
    action: approval_decision
    decision: deny
    proposal_id: qa-approval-read
expect:
  terminal_state: completed
  final_answer:
    contains: ["fixture"]
  events: []
  tool_calls: []
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [p0, real_runtime]
timeout:
  run_ms: 30000
  step_ms: 10000
"#;

    #[test]
    fn parses_text_run_example_scenario() {
        let manifest =
            parse_qa_scenario_manifest_yaml(EXAMPLE_SCENARIO).expect("example scenario is valid");

        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.id, "text.run.basic");
        assert_eq!(manifest.area, QaScenarioArea::Text);
        assert_eq!(manifest.mode.provider, QaScenarioProviderMode::Mock);
        assert_eq!(manifest.mode.runner, QaScenarioRunnerMode::Fixture);
        assert_eq!(manifest.runner, None);
        assert_eq!(manifest.steps.len(), 1);
        assert_eq!(manifest.expect.terminal_state, QaScenarioTerminalState::Completed);
    }

    #[test]
    fn maps_schema_v1_provider_modes_to_canonical_runner_modes() {
        let cases = [
            ("mock", QaScenarioProviderMode::Mock, QaScenarioRunnerMode::Fixture),
            ("recorded", QaScenarioProviderMode::Recorded, QaScenarioRunnerMode::RecordReplay),
            ("live", QaScenarioProviderMode::Live, QaScenarioRunnerMode::Live),
        ];

        for (provider, expected_provider, expected_runner) in cases {
            let scenario =
                EXAMPLE_SCENARIO.replace("provider: mock", &format!("provider: {provider}"));
            let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect("schema-v1 provider mode should remain valid");

            assert_eq!(manifest.mode.provider, expected_provider);
            assert_eq!(manifest.mode.runner, expected_runner);
            assert_eq!(manifest.runner, None);
        }
    }

    #[test]
    fn preserves_schema_v1_approval_decision_strings() {
        let manifest = parse_qa_scenario_manifest_yaml(LEGACY_APPROVAL_SCENARIO)
            .expect("legacy approval scenario should remain valid");
        let approval = manifest
            .steps
            .iter()
            .find(|step| step.action == QaScenarioStepAction::ApprovalDecision)
            .expect("legacy scenario should contain an approval step");

        assert_eq!(
            approval.decision,
            Some(QaScenarioApprovalDecision::Legacy("approve_once".to_owned()))
        );
        assert_eq!(
            approval.decision.as_ref().map(QaScenarioApprovalDecision::as_str),
            Some("approve_once")
        );
        assert_eq!(approval.proposal_id, None);
    }

    #[test]
    fn schema_v1_provider_and_approval_values_round_trip_through_json_and_yaml() {
        let cases = [
            ("mock", "approve_once", QaScenarioRunnerMode::Fixture),
            ("recorded", "reject_once", QaScenarioRunnerMode::RecordReplay),
            ("live", "defer_to_operator", QaScenarioRunnerMode::Live),
        ];

        for (provider, decision, expected_runner) in cases {
            let scenario = LEGACY_APPROVAL_SCENARIO
                .replace("provider: mock", &format!("provider: {provider}"))
                .replace("decision: approve_once", &format!("decision: {decision}"));
            let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect("schema-v1 scenario should parse before serialization");
            assert_eq!(manifest.mode.runner, expected_runner);

            let json = serde_json::to_string(&manifest)
                .expect("schema-v1 manifest should serialize as JSON");
            let json_value: Value =
                serde_json::from_str(json.as_str()).expect("serialized JSON should parse");
            assert_eq!(json_value.pointer("/mode/runner"), None);
            assert_eq!(
                json_value.pointer("/steps/1/decision").and_then(Value::as_str),
                Some(decision)
            );
            let json_round_trip = parse_qa_scenario_manifest_yaml(json.as_str())
                .expect("schema-v1 JSON projection should remain valid");
            assert_eq!(json_round_trip, manifest);

            let yaml = yaml_serde::to_string(&manifest)
                .expect("schema-v1 manifest should serialize as YAML");
            let yaml_wire = yaml_serde::from_str::<QaScenarioManifestWire>(yaml.as_str())
                .expect("serialized schema-v1 YAML should match the wire schema");
            assert_eq!(yaml_wire.mode.and_then(|mode| mode.runner), None);
            let yaml_round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
                .expect("schema-v1 YAML projection should remain valid");
            assert_eq!(yaml_round_trip, manifest);
        }
    }

    #[test]
    fn parses_schema_v2_fixture_runner_and_typed_approval() {
        let manifest = parse_qa_scenario_manifest_yaml(V2_FIXTURE_SCENARIO)
            .expect("schema-v2 fixture scenario should parse");

        assert_eq!(manifest.schema_version, QA_SCENARIO_SCHEMA_VERSION);
        assert_eq!(manifest.mode.runner, QaScenarioRunnerMode::Fixture);
        assert_eq!(manifest.mode.provider, QaScenarioProviderMode::Mock);
        assert_eq!(
            manifest.runner,
            Some(QaScenarioRunnerConfig {
                provider_fixture: "qa/fixtures/provider_basic.yaml".to_owned(),
                workspace_fixture: Some("qa/fixtures/sandbox_workspaces/repo_basic".to_owned()),
                policy_profile: Some("qa_read_only".to_owned()),
            })
        );
        assert_eq!(manifest.steps[1].decision, Some(QaScenarioApprovalDecision::Deny));
        assert_eq!(manifest.steps[1].proposal_id.as_deref(), Some("qa-approval-read"));
    }

    #[test]
    fn parses_both_schema_v2_approval_decisions() {
        for (raw, expected) in [
            ("deny", QaScenarioApprovalDecision::Deny),
            ("allow", QaScenarioApprovalDecision::Allow),
        ] {
            let scenario =
                V2_FIXTURE_SCENARIO.replace("decision: deny", &format!("decision: {raw}"));
            let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect("typed schema-v2 approval decision should parse");

            assert_eq!(manifest.steps[1].decision, Some(expected));
        }
    }

    #[test]
    fn maps_schema_v2_runner_modes_to_legacy_provider_modes() {
        let runner_block = r#"runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
"#;
        for (runner, expected_provider) in [
            ("record_replay", QaScenarioProviderMode::Recorded),
            ("live", QaScenarioProviderMode::Live),
        ] {
            let scenario = V2_FIXTURE_SCENARIO
                .replace("runner: fixture", &format!("runner: {runner}"))
                .replace(runner_block, "");
            let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect("non-fixture schema-v2 runner mode should parse without fixture config");

            assert_eq!(manifest.mode.runner.as_str(), runner);
            assert_eq!(manifest.mode.provider, expected_provider);
            assert_eq!(manifest.runner, None);
        }
    }

    #[test]
    fn schema_v2_serialization_retains_canonical_runner_projection() {
        let manifest = parse_qa_scenario_manifest_yaml(V2_FIXTURE_SCENARIO)
            .expect("schema-v2 fixture scenario should parse before serialization");

        let json =
            serde_json::to_string(&manifest).expect("schema-v2 manifest should serialize as JSON");
        let json_value: Value =
            serde_json::from_str(json.as_str()).expect("serialized JSON should parse");
        assert_eq!(json_value.pointer("/mode/runner").and_then(Value::as_str), Some("fixture"));
        assert_eq!(
            json_value.pointer("/runner/provider_fixture").and_then(Value::as_str),
            Some("qa/fixtures/provider_basic.yaml")
        );
        let json_round_trip = parse_qa_scenario_manifest_yaml(json.as_str())
            .expect("schema-v2 JSON projection should remain valid");
        assert_eq!(json_round_trip, manifest);

        let yaml =
            yaml_serde::to_string(&manifest).expect("schema-v2 manifest should serialize as YAML");
        let yaml_wire = yaml_serde::from_str::<QaScenarioManifestWire>(yaml.as_str())
            .expect("serialized schema-v2 YAML should match the wire schema");
        assert_eq!(yaml_wire.mode.and_then(|mode| mode.runner).as_deref(), Some("fixture"));
        let yaml_round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
            .expect("schema-v2 YAML projection should remain valid");
        assert_eq!(yaml_round_trip, manifest);
    }

    #[test]
    fn schema_snapshot_matches_golden_fixture() {
        let expected: Value =
            serde_json::from_str(SCHEMA_GOLDEN).expect("schema golden should parse");

        assert_eq!(qa_scenario_manifest_schema_snapshot(), expected);
    }

    #[test]
    fn schema_v2_requires_canonical_runner_mode() {
        let scenario = V2_FIXTURE_SCENARIO.replace("  runner: fixture\n", "");

        assert_validation_issue(&scenario, "$.mode.runner", "missing_runner_mode");
    }

    #[test]
    fn schema_v2_fixture_mode_requires_runner_config() {
        let scenario = V2_FIXTURE_SCENARIO.replace(
            r#"runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
"#,
            "",
        );

        assert_validation_issue(&scenario, "$.runner", "missing_runner_config");
    }

    #[test]
    fn schema_v2_fixture_mode_requires_provider_fixture() {
        let scenario = V2_FIXTURE_SCENARIO
            .replace("  provider_fixture: qa/fixtures/provider_basic.yaml\n", "");

        assert_validation_issue(&scenario, "$.runner.provider_fixture", "missing_provider_fixture");
    }

    #[test]
    fn schema_v2_rejects_unsafe_runner_paths_with_precise_paths() {
        for unsafe_path in ["../secret.yaml", r"..\secret.yaml", r"C:\secret.yaml", "/secret.yaml"]
        {
            let scenario =
                V2_FIXTURE_SCENARIO.replace("qa/fixtures/provider_basic.yaml", unsafe_path);

            assert_validation_issue(&scenario, "$.runner.provider_fixture", "unsafe_runner_path");
        }
        let unsafe_workspace =
            V2_FIXTURE_SCENARIO.replace("qa/fixtures/sandbox_workspaces/repo_basic", "../outside");
        assert_validation_issue(
            &unsafe_workspace,
            "$.runner.workspace_fixture",
            "unsafe_runner_path",
        );
    }

    #[test]
    fn schema_v2_rejects_empty_runner_identifiers() {
        let empty_policy =
            V2_FIXTURE_SCENARIO.replace("policy_profile: qa_read_only", "policy_profile: '   '");
        assert_validation_issue(&empty_policy, "$.runner.policy_profile", "empty_string");

        let empty_proposal =
            V2_FIXTURE_SCENARIO.replace("proposal_id: qa-approval-read", "proposal_id: '   '");
        assert_validation_issue(&empty_proposal, "$.steps[1].proposal_id", "empty_string");
    }

    #[test]
    fn schema_v2_rejects_legacy_approval_decisions() {
        let scenario = V2_FIXTURE_SCENARIO.replace("decision: deny", "decision: approve_once");

        assert_validation_issue(&scenario, "$.steps[1].decision", "unknown_approval_decision");
    }

    #[test]
    fn schema_v2_rejects_conflicting_compatibility_provider_mode() {
        let scenario = V2_FIXTURE_SCENARIO
            .replace("  runner: fixture\n", "  runner: fixture\n  provider: live\n");

        assert_validation_issue(&scenario, "$.mode.provider", "runner_provider_mode_mismatch");
    }

    #[test]
    fn schema_v1_rejects_schema_v2_runner_fields() {
        let scenario =
            EXAMPLE_SCENARIO.replace("  provider: mock\n", "  provider: mock\n  runner: fixture\n");

        assert_validation_issue(&scenario, "$.mode.runner", "runner_mode_requires_schema_v2");
    }

    #[test]
    fn runner_config_denies_unknown_fields() {
        let scenario = V2_FIXTURE_SCENARIO.replace(
            "  policy_profile: qa_read_only\n",
            "  policy_profile: qa_read_only\n  unexpected: true\n",
        );
        let error = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect_err("unknown runner config field should fail parsing");

        assert!(matches!(error, QaScenarioManifestError::Parse { .. }));
    }

    #[test]
    fn rejects_missing_id_with_precise_path() {
        let error = parse_qa_scenario_manifest_yaml(
            r#"
schema_version: 1
area: text
mode:
  provider: mock
requires:
  capabilities: [agent_run]
steps:
  - id: prompt
    action: user_prompt
    prompt: "Say hello."
expect:
  terminal_state: completed
  final_answer:
    contains: ["hello"]
forbidden:
  tool_calls: []
  events: []
  artifacts: []
artifacts: []
maturity:
  labels: [p0]
timeout:
  run_ms: 30000
"#,
        )
        .expect_err("missing id should fail validation");

        let issues = error.issues().expect("validation issues should be available");
        assert!(issues
            .iter()
            .any(|issue| issue.path == "$.id" && issue.code == "missing_scenario_id"));
    }

    #[test]
    fn rejects_unknown_area_with_precise_path() {
        let scenario = EXAMPLE_SCENARIO.replace("area: text", "area: unknown_area");
        let error = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect_err("unknown area should fail");

        let issues = error.issues().expect("validation issues should be available");
        assert!(issues.iter().any(|issue| issue.path == "$.area" && issue.code == "unknown_area"));
    }

    fn assert_validation_issue(scenario: &str, expected_path: &str, expected_code: &str) {
        let error = parse_qa_scenario_manifest_yaml(scenario)
            .expect_err("scenario should fail schema validation");
        let issues = error.issues().expect("validation issues should be available");

        assert!(
            issues.iter().any(|issue| issue.path == expected_path && issue.code == expected_code),
            "missing issue code={expected_code} path={expected_path}; issues={issues:?}"
        );
    }
}
