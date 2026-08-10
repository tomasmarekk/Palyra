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

use crate::qa_fault_injection::{
    qa_fault_point_descriptor, QaFaultInjectionPlan, QaFaultRecoveryClass,
    QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
};
use crate::qa_runtime_path::{
    McpTransportInvocationMode, NoHiddenFallbackExpectation,
    QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
};

/// Current QA scenario manifest schema version.
pub const QA_SCENARIO_SCHEMA_VERSION: u32 = 5;

/// Stable format label embedded in the schema snapshot and reports.
pub const QA_SCENARIO_FORMAT: &str = "palyra-qa-scenario";

const MAX_TIMEOUT_MS: u64 = 3_600_000;
const MAX_EXPECTED_DAEMON_RESTARTS: u32 = 32;
const MAX_EXPECTED_MIN_COUNT: u32 = 4_096;

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
const SUPPORTED_SCHEMA_VERSIONS: &[u32] = &[1, 2, 3, 4, 5];
const FAULT_CONTRACT_MIN_SCHEMA_VERSION: u32 = 4;
const RUNTIME_PATH_EXPECTATION_MIN_SCHEMA_VERSION: u32 = 5;
const PROVIDER_MODE_VALUES: &[&str] = &["mock", "recorded", "live"];
const RUNNER_MODE_VALUES: &[&str] = &["fixture", "record_replay", "live"];
const LIVE_PROVIDER_KIND_VALUES: &[&str] = &["openai_compatible", "anthropic"];
const LIVE_SECRET_PROFILE_ENV_PREFIX: &str = "PALYRA_QA_LIVE_";
const APPROVAL_DECISION_VALUES: &[&str] = &["allow", "deny"];
const STEP_ACTION_VALUES: &[&str] =
    &["user_prompt", "tool_result", "approval_decision", "wait_for_event"];
const TERMINAL_STATE_VALUES: &[&str] = &["completed", "failed", "cancelled", "approval_required"];
const ARTIFACT_KIND_VALUES: &[&str] =
    &["report", "transcript", "replay_bundle", "trajectory", "evidence", "workspace"];

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
    /// Typed provider binding and common runner inputs when the schema supports them.
    pub runner: Option<QaScenarioRunnerConfig>,
    /// Optional deterministic fault plan supported by schema version 4 and later.
    pub fault_injection: Option<QaFaultInjectionPlan>,
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
            fault_injection: if self.schema_version >= 4 {
                self.fault_injection.as_ref()
            } else {
                None
            },
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
    #[serde(skip_serializing_if = "Option::is_none")]
    fault_injection: Option<&'a QaFaultInjectionPlan>,
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

/// Validated provider binding and common inputs used to prepare an isolated QA runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioRunnerConfig {
    #[serde(flatten)]
    binding: QaScenarioProviderBinding,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_fixture: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_profile: Option<String>,
}

impl QaScenarioRunnerConfig {
    /// Returns the lane-specific provider binding.
    #[must_use]
    pub const fn binding(&self) -> &QaScenarioProviderBinding {
        &self.binding
    }

    /// Returns the canonical runner lane represented by this configuration.
    #[must_use]
    pub const fn runner_mode(&self) -> QaScenarioRunnerMode {
        self.binding.runner_mode()
    }

    /// Returns the deterministic provider fixture path for fixture lanes.
    #[must_use]
    pub fn provider_fixture(&self) -> Option<&str> {
        self.binding.provider_fixture()
    }

    /// Returns the redacted replay fixture path for record-replay lanes.
    #[must_use]
    pub fn replay_fixture(&self) -> Option<&str> {
        self.binding.replay_fixture()
    }

    /// Returns whether a record-replay fixture was declared redacted.
    #[must_use]
    pub const fn fixture_redacted(&self) -> Option<bool> {
        self.binding.fixture_redacted()
    }

    /// Returns the live credential-profile environment variable name.
    #[must_use]
    pub fn live_secret_profile_env(&self) -> Option<&str> {
        self.binding.live_secret_profile_env()
    }

    /// Returns the typed live provider kind.
    #[must_use]
    pub const fn live_provider_kind(&self) -> Option<QaScenarioLiveProviderKind> {
        self.binding.live_provider_kind()
    }

    /// Returns the live model identifier.
    #[must_use]
    pub fn live_model(&self) -> Option<&str> {
        self.binding.live_model()
    }

    /// Returns the optional live provider base URL.
    #[must_use]
    pub fn live_base_url(&self) -> Option<&str> {
        self.binding.live_base_url()
    }

    /// Returns the explicit raw-payload storage posture for lanes that declare it.
    #[must_use]
    pub const fn raw_payload_storage(&self) -> Option<bool> {
        self.binding.raw_payload_storage()
    }

    /// Returns the optional repository-relative workspace fixture path.
    #[must_use]
    pub fn workspace_fixture(&self) -> Option<&str> {
        self.workspace_fixture.as_deref()
    }

    /// Returns the optional non-empty policy profile identifier.
    #[must_use]
    pub fn policy_profile(&self) -> Option<&str> {
        self.policy_profile.as_deref()
    }
}

/// Lane-specific provider binding for a validated QA scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum QaScenarioProviderBinding {
    /// Deterministic provider fixture binding.
    Fixture(QaScenarioFixtureProviderBinding),
    /// Redacted record-replay provider fixture binding.
    RecordReplay(QaScenarioRecordReplayProviderBinding),
    /// Explicit live-provider binding that references credentials indirectly.
    Live(QaScenarioLiveProviderBinding),
}

impl QaScenarioProviderBinding {
    /// Returns the canonical runner lane represented by this binding.
    #[must_use]
    pub const fn runner_mode(&self) -> QaScenarioRunnerMode {
        match self {
            Self::Fixture(_) => QaScenarioRunnerMode::Fixture,
            Self::RecordReplay(_) => QaScenarioRunnerMode::RecordReplay,
            Self::Live(_) => QaScenarioRunnerMode::Live,
        }
    }

    /// Returns the deterministic provider fixture path for fixture bindings.
    #[must_use]
    pub fn provider_fixture(&self) -> Option<&str> {
        match self {
            Self::Fixture(binding) => Some(binding.provider_fixture()),
            Self::RecordReplay(_) | Self::Live(_) => None,
        }
    }

    /// Returns the redacted replay fixture path for record-replay bindings.
    #[must_use]
    pub fn replay_fixture(&self) -> Option<&str> {
        match self {
            Self::RecordReplay(binding) => Some(binding.replay_fixture()),
            Self::Fixture(_) | Self::Live(_) => None,
        }
    }

    /// Returns whether a record-replay fixture was declared redacted.
    #[must_use]
    pub const fn fixture_redacted(&self) -> Option<bool> {
        match self {
            Self::RecordReplay(binding) => Some(binding.fixture_redacted()),
            Self::Fixture(_) | Self::Live(_) => None,
        }
    }

    /// Returns the live credential-profile environment variable name.
    #[must_use]
    pub fn live_secret_profile_env(&self) -> Option<&str> {
        match self {
            Self::Live(binding) => Some(binding.secret_profile_env()),
            Self::Fixture(_) | Self::RecordReplay(_) => None,
        }
    }

    /// Returns the typed live provider kind.
    #[must_use]
    pub const fn live_provider_kind(&self) -> Option<QaScenarioLiveProviderKind> {
        match self {
            Self::Live(binding) => Some(binding.provider_kind()),
            Self::Fixture(_) | Self::RecordReplay(_) => None,
        }
    }

    /// Returns the live model identifier.
    #[must_use]
    pub fn live_model(&self) -> Option<&str> {
        match self {
            Self::Live(binding) => Some(binding.model()),
            Self::Fixture(_) | Self::RecordReplay(_) => None,
        }
    }

    /// Returns the optional live provider base URL.
    #[must_use]
    pub fn live_base_url(&self) -> Option<&str> {
        match self {
            Self::Live(binding) => binding.base_url(),
            Self::Fixture(_) | Self::RecordReplay(_) => None,
        }
    }

    /// Returns the explicit raw-payload storage posture for record-replay and live bindings.
    #[must_use]
    pub const fn raw_payload_storage(&self) -> Option<bool> {
        match self {
            Self::RecordReplay(binding) => Some(binding.raw_payload_storage()),
            Self::Live(binding) => Some(binding.raw_payload_storage()),
            Self::Fixture(_) => None,
        }
    }
}

/// Validated deterministic provider fixture binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioFixtureProviderBinding {
    provider_fixture: String,
}

impl QaScenarioFixtureProviderBinding {
    /// Returns the safe repository-relative deterministic provider fixture path.
    #[must_use]
    pub fn provider_fixture(&self) -> &str {
        self.provider_fixture.as_str()
    }
}

/// Validated redacted provider replay binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioRecordReplayProviderBinding {
    replay_fixture: String,
    fixture_redacted: bool,
    raw_payload_storage: bool,
}

impl QaScenarioRecordReplayProviderBinding {
    /// Returns the safe repository-relative replay fixture path.
    #[must_use]
    pub fn replay_fixture(&self) -> &str {
        self.replay_fixture.as_str()
    }

    /// Returns whether the replay fixture was explicitly declared redacted.
    #[must_use]
    pub const fn fixture_redacted(&self) -> bool {
        self.fixture_redacted
    }

    /// Returns whether raw provider payload storage is enabled.
    #[must_use]
    pub const fn raw_payload_storage(&self) -> bool {
        self.raw_payload_storage
    }
}

/// Supported live provider implementation family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaScenarioLiveProviderKind {
    /// OpenAI-compatible chat-completions provider.
    #[serde(rename = "openai_compatible")]
    OpenAiCompatible,
    /// Anthropic Messages API provider.
    Anthropic,
}

impl QaScenarioLiveProviderKind {
    /// Returns the manifest string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "openai_compatible",
            Self::Anthropic => "anthropic",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "openai_compatible" => Some(Self::OpenAiCompatible),
            "anthropic" => Some(Self::Anthropic),
            _ => None,
        }
    }
}

/// Validated live-provider binding with indirect credential lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioLiveProviderBinding {
    secret_profile_env: String,
    provider_kind: QaScenarioLiveProviderKind,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
    raw_payload_storage: bool,
}

impl QaScenarioLiveProviderBinding {
    /// Returns the validated environment variable that names the live secret profile.
    #[must_use]
    pub fn secret_profile_env(&self) -> &str {
        self.secret_profile_env.as_str()
    }

    /// Returns the live provider implementation family.
    #[must_use]
    pub const fn provider_kind(&self) -> QaScenarioLiveProviderKind {
        self.provider_kind
    }

    /// Returns the non-empty live model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        self.model.as_str()
    }

    /// Returns the optional non-empty live provider base URL.
    #[must_use]
    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    /// Returns whether raw provider payload storage is enabled.
    #[must_use]
    pub const fn raw_payload_storage(&self) -> bool {
        self.raw_payload_storage
    }
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
    /// Optional maximum total call count, regardless of result outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_count: Option<u32>,
    /// Required tool-result outcome. Schema v4 and later treat omission as any
    /// outcome; legacy schemas retain their success-by-default contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
}

/// One expected activation and its required recovery classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioExpectedFaultActivation {
    /// Activation id declared by the scenario's fault plan.
    pub activation_id: String,
    /// Recovery class the runtime must prove in evidence.
    pub recovery_class: QaFaultRecoveryClass,
}

/// Exact fault outcomes required for a scenario verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaScenarioExpectedFaultInjection {
    /// Expected activations; validation requires one entry per planned activation.
    pub activations: Vec<QaScenarioExpectedFaultActivation>,
    /// Exact number of daemon restarts expected during the scenario.
    pub daemon_restarts: u32,
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
    /// Exact runtime path and bounded fallback posture required by schema v5.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_path: Option<NoHiddenFallbackExpectation>,
    /// Exact activation, recovery, and restart assertions for a fault plan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fault_injection: Option<QaScenarioExpectedFaultInjection>,
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
    Workspace,
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
            Self::Workspace => "workspace",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "report" => Some(Self::Report),
            "transcript" => Some(Self::Transcript),
            "replay_bundle" => Some(Self::ReplayBundle),
            "trajectory" => Some(Self::Trajectory),
            "evidence" => Some(Self::Evidence),
            "workspace" => Some(Self::Workspace),
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
    /// Optional exact lowercase SHA-256 digest of the observed content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
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
    #[serde(default)]
    fault_injection: Option<QaFaultInjectionPlan>,
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
    replay_fixture: Option<String>,
    #[serde(default)]
    fixture_redacted: Option<bool>,
    #[serde(default)]
    secret_profile_env: Option<String>,
    #[serde(default)]
    provider_kind: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    raw_payload_storage: Option<bool>,
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
    #[serde(default)]
    runtime_path: Option<NoHiddenFallbackExpectationWire>,
    #[serde(default)]
    fault_injection: Option<QaScenarioExpectedFaultInjectionWire>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NoHiddenFallbackExpectationWire {
    runtime_contract_version: Option<String>,
    provider_lane: Option<String>,
    attempt_owner: Option<String>,
    harness_id: Option<String>,
    context_engine_id: Option<String>,
    #[serde(default)]
    mcp_transport_mode: Option<McpTransportInvocationMode>,
    max_fallback_count: Option<u32>,
    #[serde(default)]
    allowed_fallback_reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioExpectedFaultInjectionWire {
    #[serde(default)]
    activations: Vec<QaScenarioExpectedFaultActivationWire>,
    daemon_restarts: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QaScenarioExpectedFaultActivationWire {
    activation_id: Option<String>,
    recovery_class: Option<String>,
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
    max_count: Option<u32>,
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
    #[serde(default)]
    sha256: Option<String>,
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
            "runner",
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
        "live_provider_kinds": LIVE_PROVIDER_KIND_VALUES,
        "approval_decisions": APPROVAL_DECISION_VALUES,
        "runner_config_fields": [
            "provider_fixture",
            "replay_fixture",
            "fixture_redacted",
            "secret_profile_env",
            "provider_kind",
            "model",
            "base_url",
            "raw_payload_storage",
            "workspace_fixture",
            "policy_profile"
        ],
        "optional_sections": ["fault_injection"],
        "fault_injection_plan_schema_version": QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        "runtime_path_evidence_schema_version": QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
        "schema_v5_required_expect_fields": ["runtime_path"],
        "runtime_path_expectation_fields": [
            "runtime_contract_version",
            "provider_lane",
            "attempt_owner",
            "harness_id",
            "context_engine_id",
            "mcp_transport_mode",
            "max_fallback_count",
            "allowed_fallback_reason_codes"
        ],
        "mcp_transport_modes": ["per_call", "persistent"],
        "expected_fault_injection_fields": ["activations", "daemon_restarts"],
        "expected_fault_activation_fields": ["activation_id", "recovery_class"],
        "expected_tool_call_fields": ["name", "min_count", "max_count", "success"],
        "step_actions": STEP_ACTION_VALUES,
        "terminal_states": TERMINAL_STATE_VALUES,
        "artifact_kinds": ARTIFACT_KIND_VALUES,
        "artifact_fields": ["path", "kind", "required", "sha256"],
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
    let fault_injection = validate_fault_injection_plan(
        wire.fault_injection,
        schema_version,
        mode.as_ref(),
        &mut issues,
    );
    let requires = validate_requires(wire.requires, &mut issues);
    let steps = validate_steps(wire.steps, schema_version, &mut issues);
    let expect = validate_expect(
        wire.expect,
        schema_version,
        mode.as_ref().map(|mode| mode.runner),
        fault_injection.as_ref(),
        &mut issues,
    );
    let forbidden = validate_forbidden(wire.forbidden, &mut issues);
    let artifacts = validate_artifacts(wire.artifacts, schema_version, &mut issues);
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
        fault_injection,
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
    if schema_version == 2 {
        return validate_schema_v2_runner_config(value, runner_mode, issues);
    }
    let runner_mode = runner_mode?;
    let Some(value) = value else {
        let recovery_hint = match runner_mode {
            QaScenarioRunnerMode::Fixture => {
                "Add runner.provider_fixture with a safe repository-relative fixture path."
            }
            QaScenarioRunnerMode::RecordReplay => {
                "Add a redacted runner.replay_fixture and explicitly disable raw payload storage."
            }
            QaScenarioRunnerMode::Live => {
                "Add an explicit live provider binding that references a QA secret profile env."
            }
        };
        push_issue(
            issues,
            "missing_runner_config",
            "$.runner",
            format!("{} runner mode requires runner configuration", runner_mode.as_str()),
            recovery_hint,
        );
        return None;
    };

    let QaScenarioRunnerConfigWire {
        provider_fixture,
        replay_fixture,
        fixture_redacted,
        secret_profile_env,
        provider_kind,
        model,
        base_url,
        raw_payload_storage,
        workspace_fixture,
        policy_profile,
    } = value;
    let workspace_fixture = validate_runner_path(
        workspace_fixture,
        "$.runner.workspace_fixture",
        "workspace fixture",
        false,
        issues,
    );
    let policy_profile =
        validate_optional_nonempty(policy_profile, "$.runner.policy_profile", issues);

    let binding = match runner_mode {
        QaScenarioRunnerMode::Fixture => {
            reject_runner_fields_for_mode(
                &[
                    (replay_fixture.is_some(), "$.runner.replay_fixture", "replay_fixture"),
                    (fixture_redacted.is_some(), "$.runner.fixture_redacted", "fixture_redacted"),
                    (
                        secret_profile_env.is_some(),
                        "$.runner.secret_profile_env",
                        "secret_profile_env",
                    ),
                    (provider_kind.is_some(), "$.runner.provider_kind", "provider_kind"),
                    (model.is_some(), "$.runner.model", "model"),
                    (base_url.is_some(), "$.runner.base_url", "base_url"),
                    (
                        raw_payload_storage.is_some(),
                        "$.runner.raw_payload_storage",
                        "raw_payload_storage",
                    ),
                ],
                runner_mode,
                issues,
            );
            validate_runner_path(
                provider_fixture,
                "$.runner.provider_fixture",
                "provider fixture",
                true,
                issues,
            )
            .map(|provider_fixture| {
                QaScenarioProviderBinding::Fixture(QaScenarioFixtureProviderBinding {
                    provider_fixture,
                })
            })
        }
        QaScenarioRunnerMode::RecordReplay => {
            reject_runner_fields_for_mode(
                &[
                    (provider_fixture.is_some(), "$.runner.provider_fixture", "provider_fixture"),
                    (
                        secret_profile_env.is_some(),
                        "$.runner.secret_profile_env",
                        "secret_profile_env",
                    ),
                    (provider_kind.is_some(), "$.runner.provider_kind", "provider_kind"),
                    (model.is_some(), "$.runner.model", "model"),
                    (base_url.is_some(), "$.runner.base_url", "base_url"),
                ],
                runner_mode,
                issues,
            );
            let replay_fixture = validate_runner_path(
                replay_fixture,
                "$.runner.replay_fixture",
                "replay fixture",
                true,
                issues,
            );
            let fixture_redacted = validate_fixture_redacted(fixture_redacted, issues);
            let raw_payload_storage =
                validate_raw_payload_storage_disabled(raw_payload_storage, runner_mode, issues);
            match (replay_fixture, fixture_redacted, raw_payload_storage) {
                (Some(replay_fixture), Some(fixture_redacted), Some(raw_payload_storage)) => {
                    Some(QaScenarioProviderBinding::RecordReplay(
                        QaScenarioRecordReplayProviderBinding {
                            replay_fixture,
                            fixture_redacted,
                            raw_payload_storage,
                        },
                    ))
                }
                _ => None,
            }
        }
        QaScenarioRunnerMode::Live => {
            reject_runner_fields_for_mode(
                &[
                    (provider_fixture.is_some(), "$.runner.provider_fixture", "provider_fixture"),
                    (replay_fixture.is_some(), "$.runner.replay_fixture", "replay_fixture"),
                    (fixture_redacted.is_some(), "$.runner.fixture_redacted", "fixture_redacted"),
                ],
                runner_mode,
                issues,
            );
            let secret_profile_env = validate_live_secret_profile_env(secret_profile_env, issues);
            let provider_kind = validate_live_provider_kind(provider_kind, issues);
            let model = validate_required_string(model, "$.runner.model", "live model", issues);
            let base_url = validate_optional_nonempty(base_url, "$.runner.base_url", issues);
            let raw_payload_storage =
                validate_raw_payload_storage_disabled(raw_payload_storage, runner_mode, issues);
            match (secret_profile_env, provider_kind, model, raw_payload_storage) {
                (
                    Some(secret_profile_env),
                    Some(provider_kind),
                    Some(model),
                    Some(raw_payload_storage),
                ) => Some(QaScenarioProviderBinding::Live(QaScenarioLiveProviderBinding {
                    secret_profile_env,
                    provider_kind,
                    model,
                    base_url,
                    raw_payload_storage,
                })),
                _ => None,
            }
        }
    };

    binding.map(|binding| QaScenarioRunnerConfig { binding, workspace_fixture, policy_profile })
}

fn validate_schema_v2_runner_config(
    value: Option<QaScenarioRunnerConfigWire>,
    runner_mode: Option<QaScenarioRunnerMode>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioRunnerConfig> {
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
            let QaScenarioRunnerConfigWire {
                provider_fixture,
                replay_fixture,
                fixture_redacted,
                secret_profile_env,
                provider_kind,
                model,
                base_url,
                raw_payload_storage,
                workspace_fixture,
                policy_profile,
            } = value;
            reject_runner_fields_for_mode(
                &[
                    (replay_fixture.is_some(), "$.runner.replay_fixture", "replay_fixture"),
                    (fixture_redacted.is_some(), "$.runner.fixture_redacted", "fixture_redacted"),
                    (
                        secret_profile_env.is_some(),
                        "$.runner.secret_profile_env",
                        "secret_profile_env",
                    ),
                    (provider_kind.is_some(), "$.runner.provider_kind", "provider_kind"),
                    (model.is_some(), "$.runner.model", "model"),
                    (base_url.is_some(), "$.runner.base_url", "base_url"),
                    (
                        raw_payload_storage.is_some(),
                        "$.runner.raw_payload_storage",
                        "raw_payload_storage",
                    ),
                ],
                QaScenarioRunnerMode::Fixture,
                issues,
            );
            let provider_fixture = validate_runner_path(
                provider_fixture,
                "$.runner.provider_fixture",
                "provider fixture",
                true,
                issues,
            );
            let workspace_fixture = validate_runner_path(
                workspace_fixture,
                "$.runner.workspace_fixture",
                "workspace fixture",
                false,
                issues,
            );
            let policy_profile =
                validate_optional_nonempty(policy_profile, "$.runner.policy_profile", issues);
            provider_fixture.map(|provider_fixture| QaScenarioRunnerConfig {
                binding: QaScenarioProviderBinding::Fixture(QaScenarioFixtureProviderBinding {
                    provider_fixture,
                }),
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
                    "schema_version 2 runner configuration is only supported for fixture mode",
                    format!(
                        "Remove the runner section or use schema_version {QA_SCENARIO_SCHEMA_VERSION} for typed provider bindings."
                    ),
                );
            }
            None
        }
        None => None,
    }
}

fn reject_runner_fields_for_mode(
    fields: &[(bool, &str, &str)],
    runner_mode: QaScenarioRunnerMode,
    issues: &mut Vec<QaScenarioManifestIssue>,
) {
    for &(present, path, field) in fields {
        if present {
            push_issue(
                issues,
                "runner_field_not_supported_for_mode",
                path,
                format!(
                    "runner.{field} is not supported when mode.runner is '{}'",
                    runner_mode.as_str()
                ),
                format!(
                    "Remove runner.{field} from the {} runner configuration.",
                    runner_mode.as_str()
                ),
            );
        }
    }
}

fn validate_fixture_redacted(
    value: Option<bool>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<bool> {
    match value {
        Some(true) => Some(true),
        Some(false) => {
            push_issue(
                issues,
                "fixture_must_be_redacted",
                "$.runner.fixture_redacted",
                "record-replay fixtures must be explicitly marked as redacted",
                "Set runner.fixture_redacted to true after verifying the replay fixture is redacted.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_fixture_redacted",
                "$.runner.fixture_redacted",
                "record-replay runner requires an explicit fixture redaction declaration",
                "Set runner.fixture_redacted to true after verifying the replay fixture is redacted.",
            );
            None
        }
    }
}

fn validate_raw_payload_storage_disabled(
    value: Option<bool>,
    runner_mode: QaScenarioRunnerMode,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<bool> {
    match value {
        Some(false) => Some(false),
        Some(true) => {
            push_issue(
                issues,
                "raw_payload_storage_must_be_disabled",
                "$.runner.raw_payload_storage",
                format!(
                    "raw provider payload storage must be disabled for {} runner mode",
                    runner_mode.as_str()
                ),
                "Set runner.raw_payload_storage to false.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_raw_payload_storage",
                "$.runner.raw_payload_storage",
                format!(
                    "{} runner mode requires an explicit raw payload storage posture",
                    runner_mode.as_str()
                ),
                "Set runner.raw_payload_storage to false.",
            );
            None
        }
    }
}

fn validate_live_secret_profile_env(
    value: Option<String>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<String> {
    let value = validate_required_string(
        value,
        "$.runner.secret_profile_env",
        "live secret profile env",
        issues,
    )?;
    if live_secret_profile_env_is_valid(value.as_str()) {
        return Some(value);
    }
    push_issue(
        issues,
        "invalid_live_secret_profile_env",
        "$.runner.secret_profile_env",
        format!(
            "live secret profile env must be an uppercase environment identifier prefixed with {LIVE_SECRET_PROFILE_ENV_PREFIX}"
        ),
        format!(
            "Use a name such as {LIVE_SECRET_PROFILE_ENV_PREFIX}OPENAI without embedding a credential value."
        ),
    );
    None
}

fn live_secret_profile_env_is_valid(value: &str) -> bool {
    value.strip_prefix(LIVE_SECRET_PROFILE_ENV_PREFIX).is_some_and(|suffix| !suffix.is_empty())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_live_provider_kind(
    value: Option<String>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioLiveProviderKind> {
    let value =
        validate_required_string(value, "$.runner.provider_kind", "live provider kind", issues)?;
    if let Some(provider_kind) = QaScenarioLiveProviderKind::parse(value.as_str()) {
        return Some(provider_kind);
    }
    push_issue(
        issues,
        "unknown_live_provider_kind",
        "$.runner.provider_kind",
        format!(
            "unknown live provider kind '{value}', expected one of {}",
            LIVE_PROVIDER_KIND_VALUES.join(", ")
        ),
        "Use openai_compatible or anthropic for runner.provider_kind.",
    );
    None
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
    for (index, fixture) in value.fixtures.iter().enumerate() {
        if !qa_path_is_below(fixture.as_str(), &["qa/fixtures"]) {
            push_issue(
                issues,
                "unsafe_fixture_path",
                format!("$.requires.fixtures[{index}]").as_str(),
                format!("fixture path '{fixture}' must stay below qa/fixtures"),
                "Use a normalized repository-relative path below qa/fixtures.",
            );
        }
    }
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

fn validate_fault_injection_plan(
    value: Option<QaFaultInjectionPlan>,
    schema_version: Option<u32>,
    mode: Option<&QaScenarioMode>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaFaultInjectionPlan> {
    let plan = value?;
    if schema_version.is_some_and(|version| version < FAULT_CONTRACT_MIN_SCHEMA_VERSION) {
        push_issue(
            issues,
            "fault_injection_requires_schema_v4",
            "$.fault_injection",
            "fault_injection is supported only by schema_version 4 or later",
            "Set schema_version to at least 4 or remove the fault_injection section.",
        );
        return None;
    }
    if mode.is_some_and(|mode| mode.runner == QaScenarioRunnerMode::Live) {
        push_issue(
            issues,
            "fault_injection_live_runner_forbidden",
            "$.fault_injection",
            "fault injection is not available for live runner mode",
            "Use fixture or record_replay mode for deterministic fault injection.",
        );
    }
    if mode.is_some_and(|mode| !mode.deterministic) {
        push_issue(
            issues,
            "fault_injection_requires_deterministic_mode",
            "$.mode.deterministic",
            "fault injection requires deterministic mode",
            "Set mode.deterministic to true.",
        );
    }
    if let Err(error) = plan.validate() {
        for issue in error.issues() {
            let suffix = issue.path.strip_prefix('$').unwrap_or(issue.path.as_str());
            push_issue(
                issues,
                issue.code.clone(),
                format!("$.fault_injection{suffix}"),
                issue.message.clone(),
                "Correct the versioned fault plan before running the scenario.",
            );
        }
        return None;
    }
    Some(plan)
}

fn validate_expect(
    value: Option<QaScenarioExpectWire>,
    schema_version: Option<u32>,
    runner_mode: Option<QaScenarioRunnerMode>,
    fault_plan: Option<&QaFaultInjectionPlan>,
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
    let tool_calls = validate_expected_tool_calls(value.tool_calls, schema_version, issues);
    let runtime_path =
        validate_runtime_path_expectation(value.runtime_path, schema_version, runner_mode, issues);
    let fault_injection = validate_expected_fault_injection(
        value.fault_injection,
        schema_version,
        fault_plan,
        issues,
    );
    if final_answer.is_none()
        && events.is_empty()
        && tool_calls.is_empty()
        && runtime_path.is_none()
        && fault_injection.is_none()
    {
        push_issue(
            issues,
            "empty_expectations",
            "$.expect",
            "expect must include final_answer, events, tool_calls, runtime_path, or fault_injection assertions",
            "Add at least one observable assertion to the expect section.",
        );
    }
    Some(QaScenarioExpect {
        terminal_state: terminal_state?,
        final_answer: final_answer.flatten(),
        events,
        tool_calls,
        runtime_path,
        fault_injection,
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
        let min_count_is_valid = validate_expected_min_count(
            event.min_count,
            format!("{path}.min_count").as_str(),
            issues,
        );
        let Some(event_type) = validate_required_string(
            event.event_type,
            format!("{path}.event_type").as_str(),
            "event type",
            issues,
        ) else {
            continue;
        };
        if min_count_is_valid {
            events.push(QaScenarioExpectedEvent { event_type, min_count: event.min_count });
        }
    }
    events
}

fn validate_expected_tool_calls(
    values: Vec<QaScenarioExpectedToolCallWire>,
    schema_version: Option<u32>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Vec<QaScenarioExpectedToolCall> {
    let mut tool_calls = Vec::with_capacity(values.len());
    for (index, tool_call) in values.into_iter().enumerate() {
        let path = format!("$.expect.tool_calls[{index}]");
        let min_count_is_valid = validate_expected_min_count(
            tool_call.min_count,
            format!("{path}.min_count").as_str(),
            issues,
        );
        let Some(name) = validate_required_string(
            tool_call.name,
            format!("{path}.name").as_str(),
            "tool call name",
            issues,
        ) else {
            continue;
        };
        if min_count_is_valid {
            let max_count = if schema_version
                .is_some_and(|version| version >= FAULT_CONTRACT_MIN_SCHEMA_VERSION)
            {
                tool_call.max_count
            } else {
                if tool_call.max_count.is_some() {
                    push_issue(
                        issues,
                        "tool_call_max_count_requires_schema_v4",
                        format!("{path}.max_count"),
                        "max_count is supported only by schema_version 4 or later",
                        "Set schema_version to at least 4 or remove max_count.",
                    );
                }
                None
            };
            let effective_min_count = tool_call.min_count.unwrap_or(1);
            if max_count.is_some_and(|max_count| max_count < effective_min_count) {
                push_issue(
                    issues,
                    "invalid_expected_tool_call_count_range",
                    format!("{path}.max_count"),
                    format!(
                        "max_count must be greater than or equal to the effective min_count {effective_min_count}"
                    ),
                    "Increase max_count or lower min_count.",
                );
                continue;
            }
            tool_calls.push(QaScenarioExpectedToolCall {
                name,
                min_count: tool_call.min_count,
                max_count,
                success: tool_call.success,
            });
        }
    }
    tool_calls
}

fn validate_expected_min_count(
    value: Option<u32>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> bool {
    if value.is_none_or(|count| count <= MAX_EXPECTED_MIN_COUNT) {
        return true;
    }
    push_issue(
        issues,
        "expected_min_count_out_of_range",
        path,
        format!("min_count must not exceed {MAX_EXPECTED_MIN_COUNT}"),
        "Lower min_count to keep QA evidence generation bounded.",
    );
    false
}

fn validate_runtime_path_expectation(
    value: Option<NoHiddenFallbackExpectationWire>,
    schema_version: Option<u32>,
    runner_mode: Option<QaScenarioRunnerMode>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<NoHiddenFallbackExpectation> {
    if schema_version.is_none_or(|version| version < RUNTIME_PATH_EXPECTATION_MIN_SCHEMA_VERSION) {
        if value.is_some() {
            push_issue(
                issues,
                "runtime_path_expectation_requires_schema_v5",
                "$.expect.runtime_path",
                "runtime_path expectations are supported only by schema_version 5",
                "Set schema_version to 5 or remove the runtime_path expectation.",
            );
        }
        return None;
    }

    let Some(value) = value else {
        push_issue(
            issues,
            "missing_runtime_path_expectation",
            "$.expect.runtime_path",
            "schema_version 5 requires an exact runtime_path expectation",
            "Declare the runtime contract, provider lane, path components, and fallback policy.",
        );
        return None;
    };

    let runtime_contract_version = validate_required_string(
        value.runtime_contract_version,
        "$.expect.runtime_path.runtime_contract_version",
        "runtime contract version",
        issues,
    );
    let provider_lane = validate_enum_string(
        value.provider_lane,
        "$.expect.runtime_path.provider_lane",
        "runtime path provider lane",
        RUNNER_MODE_VALUES,
        issues,
    );
    if let (Some(provider_lane), Some(runner_mode)) = (provider_lane.as_deref(), runner_mode) {
        if provider_lane != runner_mode.as_str() {
            push_issue(
                issues,
                "runtime_path_provider_lane_mismatch",
                "$.expect.runtime_path.provider_lane",
                format!(
                    "runtime_path provider lane '{provider_lane}' does not match mode.runner '{}'",
                    runner_mode.as_str()
                ),
                "Use the same provider lane as mode.runner.",
            );
        }
    }
    let attempt_owner = validate_required_string(
        value.attempt_owner,
        "$.expect.runtime_path.attempt_owner",
        "runtime path attempt owner",
        issues,
    );
    let harness_id = validate_required_string(
        value.harness_id,
        "$.expect.runtime_path.harness_id",
        "runtime path harness id",
        issues,
    );
    let context_engine_id = validate_required_string(
        value.context_engine_id,
        "$.expect.runtime_path.context_engine_id",
        "runtime path context engine id",
        issues,
    );
    let max_fallback_count = match value.max_fallback_count {
        Some(count) => Some(count),
        None => {
            push_issue(
                issues,
                "missing_runtime_path_max_fallback_count",
                "$.expect.runtime_path.max_fallback_count",
                "runtime path max_fallback_count is required",
                "Declare the exact maximum fallback count, including zero.",
            );
            None
        }
    };

    let expectation = NoHiddenFallbackExpectation {
        runtime_contract_version: runtime_contract_version?,
        provider_lane: provider_lane?,
        attempt_owner: attempt_owner?,
        harness_id: harness_id?,
        context_engine_id: context_engine_id?,
        mcp_transport_mode: value.mcp_transport_mode,
        max_fallback_count: max_fallback_count?,
        allowed_fallback_reason_codes: value.allowed_fallback_reason_codes,
    };
    if let Err(error) = expectation.validate_shape() {
        let suffix = error.path().strip_prefix('$').unwrap_or(error.path());
        push_issue(
            issues,
            error.code(),
            format!("$.expect.runtime_path{suffix}"),
            error.message(),
            "Correct the bounded runtime-path selectors and fallback policy.",
        );
        return None;
    }
    Some(expectation)
}

fn validate_expected_fault_injection(
    value: Option<QaScenarioExpectedFaultInjectionWire>,
    schema_version: Option<u32>,
    fault_plan: Option<&QaFaultInjectionPlan>,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<QaScenarioExpectedFaultInjection> {
    if schema_version.is_some_and(|version| version < FAULT_CONTRACT_MIN_SCHEMA_VERSION) {
        if value.is_some() {
            push_issue(
                issues,
                "fault_expectations_require_schema_v4",
                "$.expect.fault_injection",
                "fault_injection expectations are supported only by schema_version 4 or later",
                "Set schema_version to at least 4 or remove the fault_injection expectations.",
            );
        }
        return None;
    }

    let Some(value) = value else {
        if fault_plan.is_some() {
            push_issue(
                issues,
                "missing_fault_expectations",
                "$.expect.fault_injection",
                "a fault plan requires exact activation, recovery, and restart expectations",
                "Add expect.fault_injection for every planned activation.",
            );
        }
        return None;
    };
    let Some(fault_plan) = fault_plan else {
        push_issue(
            issues,
            "fault_expectations_require_plan",
            "$.expect.fault_injection",
            "fault expectations require a valid fault_injection plan",
            "Add and correct the top-level fault_injection plan.",
        );
        return None;
    };

    let daemon_restarts = match value.daemon_restarts {
        Some(restarts) if restarts <= MAX_EXPECTED_DAEMON_RESTARTS => Some(restarts),
        Some(restarts) => {
            push_issue(
                issues,
                "invalid_expected_daemon_restarts",
                "$.expect.fault_injection.daemon_restarts",
                format!(
                    "daemon_restarts must be in range 0..={MAX_EXPECTED_DAEMON_RESTARTS}, got {restarts}"
                ),
                "Use an exact bounded daemon restart count.",
            );
            None
        }
        None => {
            push_issue(
                issues,
                "missing_expected_daemon_restarts",
                "$.expect.fault_injection.daemon_restarts",
                "daemon_restarts is required for fault-injection expectations",
                "Declare the exact expected daemon restart count, including zero.",
            );
            None
        }
    };

    if value.activations.is_empty() {
        push_issue(
            issues,
            "empty_expected_fault_activations",
            "$.expect.fault_injection.activations",
            "fault-injection expectations must include every planned activation",
            "Add one expected activation for every fault plan activation.",
        );
    }
    let planned_by_id = fault_plan
        .activations
        .iter()
        .map(|activation| (activation.id.as_str(), activation))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut expected_ids = BTreeSet::new();
    let mut activations = Vec::with_capacity(value.activations.len());
    for (index, expected) in value.activations.into_iter().enumerate() {
        let path = format!("$.expect.fault_injection.activations[{index}]");
        let activation_id = validate_required_slug(
            expected.activation_id,
            format!("{path}.activation_id").as_str(),
            "fault activation id",
            issues,
        );
        let recovery_class = validate_required_string(
            expected.recovery_class,
            format!("{path}.recovery_class").as_str(),
            "fault recovery class",
            issues,
        )
        .and_then(|value| {
            let recovery_class = QaFaultRecoveryClass::parse(value.as_str());
            if recovery_class.is_none() {
                push_issue(
                    issues,
                    "unknown_fault_recovery_class",
                    format!("{path}.recovery_class"),
                    format!("unknown fault recovery class `{value}`"),
                    "Use a recovery class from the fault-injection schema snapshot.",
                );
            }
            recovery_class
        });
        let Some(activation_id) = activation_id else {
            continue;
        };
        if !expected_ids.insert(activation_id.clone()) {
            push_issue(
                issues,
                "duplicate_expected_fault_activation",
                format!("{path}.activation_id"),
                format!("fault activation `{activation_id}` is expected more than once"),
                "Keep exactly one expectation for each planned activation.",
            );
        }
        let Some(planned) = planned_by_id.get(activation_id.as_str()) else {
            push_issue(
                issues,
                "unplanned_expected_fault_activation",
                format!("{path}.activation_id"),
                format!("fault activation `{activation_id}` is not declared by the plan"),
                "Reference an activation id from the top-level fault_injection plan.",
            );
            continue;
        };
        let Some(recovery_class) = recovery_class else {
            continue;
        };
        if let Some(descriptor) = qa_fault_point_descriptor(planned.point_id.as_str()) {
            if !descriptor.supports_recovery(recovery_class) {
                push_issue(
                    issues,
                    "unsupported_fault_recovery_class",
                    format!("{path}.recovery_class"),
                    format!(
                        "fault point `{}` cannot prove recovery class `{}`",
                        planned.point_id,
                        recovery_class.as_str()
                    ),
                    "Choose a recovery class supported by the registered fault point.",
                );
            }
        }
        activations.push(QaScenarioExpectedFaultActivation { activation_id, recovery_class });
    }
    for planned in &fault_plan.activations {
        if !expected_ids.contains(planned.id.as_str()) {
            push_issue(
                issues,
                "missing_expected_fault_activation",
                "$.expect.fault_injection.activations",
                format!("planned fault activation `{}` has no expectation", planned.id),
                "Add one expected recovery class for the planned activation.",
            );
        }
    }

    daemon_restarts
        .map(|daemon_restarts| QaScenarioExpectedFaultInjection { activations, daemon_restarts })
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
    schema_version: Option<u32>,
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
        let kind = validate_artifact_kind(artifact.kind, format!("{path}.kind").as_str(), issues)
            .and_then(|kind| {
                if kind == QaScenarioArtifactKind::Workspace
                    && schema_version
                        .is_some_and(|version| version < FAULT_CONTRACT_MIN_SCHEMA_VERSION)
                {
                    push_issue(
                        issues,
                        "workspace_artifact_requires_schema_v4",
                        format!("{path}.kind"),
                        "workspace artifacts are supported only by schema_version 4 or later",
                        "Set schema_version to at least 4 or use a legacy artifact kind.",
                    );
                    None
                } else {
                    Some(kind)
                }
            });
        let sha256 =
            if schema_version.is_some_and(|version| version >= FAULT_CONTRACT_MIN_SCHEMA_VERSION) {
                validate_artifact_sha256(artifact.sha256, format!("{path}.sha256").as_str(), issues)
            } else {
                if artifact.sha256.is_some() {
                    push_issue(
                    issues,
                    "artifact_sha256_requires_schema_v4",
                    format!("{path}.sha256"),
                    "artifact sha256 assertions are supported only by schema_version 4 or later",
                    "Set schema_version to at least 4 or remove sha256.",
                );
                }
                None
            };
        if let (Some(path), Some(kind)) = (artifact_path, kind) {
            artifacts.push(QaScenarioArtifact { path, kind, required: artifact.required, sha256 });
        }
    }
    Some(artifacts)
}

fn validate_artifact_sha256(
    value: Option<String>,
    path: &str,
    issues: &mut Vec<QaScenarioManifestIssue>,
) -> Option<String> {
    let value = value?;
    let valid = value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid {
        return Some(value);
    }
    push_issue(
        issues,
        "invalid_artifact_sha256",
        path,
        "artifact sha256 must contain exactly 64 lowercase hexadecimal characters",
        "Use the canonical lowercase SHA-256 digest of the expected artifact content.",
    );
    None
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
    if !qa_path_is_below(value.as_str(), &["qa/reports", "qa/fixtures"]) {
        push_issue(
            issues,
            "unsafe_artifact_path",
            path,
            format!("artifact path '{value}' must stay below qa/reports or qa/fixtures"),
            "Use a repository-relative artifact path below qa/reports or qa/fixtures.",
        );
        return None;
    }
    Some(value)
}

fn qa_path_is_below(value: &str, allowed_roots: &[&str]) -> bool {
    if runner_path_is_unsafe(value) || value.contains('\\') || value.contains(':') {
        return false;
    }
    let normalized = value.split('/').collect::<Vec<_>>();
    if normalized.iter().any(|component| component.is_empty() || *component == ".") {
        return false;
    }
    allowed_roots.iter().any(|root| {
        value.strip_prefix(root).is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
    })
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
    const FAULT_TOOL_SCENARIO: &str =
        include_str!("../../../qa/scenarios/fault_injection/tool_effect_before_ack.yaml");
    const SCHEMA_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_scenario_manifest_schema.json");
    const FIXTURE_RUNNER_BLOCK: &str = r#"runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
"#;
    const RECORD_REPLAY_RUNNER_BLOCK: &str = r#"runner:
  replay_fixture: qa/fixtures/provider_basic.yaml
  fixture_redacted: true
  raw_payload_storage: false
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
"#;
    const LIVE_RUNNER_BLOCK: &str = r#"runner:
  secret_profile_env: PALYRA_QA_LIVE_OPENAI
  provider_kind: openai_compatible
  model: qa-live-model
  base_url: https://api.example.test/v1
  raw_payload_storage: false
  workspace_fixture: qa/fixtures/sandbox_workspaces/repo_basic
  policy_profile: qa_read_only
"#;
    const FAULT_PLAN_BLOCK: &str = r#"fault_injection:
  schema_version: 1
  format: palyra-qa-fault-injection-plan
  seed: 4242
  activations:
    - id: tool-crash-after-effect
      point_id: tool.after_effect_before_ack
      occurrence: 1
      action:
        type: terminate_process
"#;
    const FAULT_EXPECTATION_BLOCK: &str = r#"  fault_injection:
    activations:
      - activation_id: tool-crash-after-effect
        recovery_class: duplicate_suppressed
    daemon_restarts: 1
"#;
    const RUNTIME_PATH_EXPECTATION_BLOCK: &str = r#"  runtime_path:
    runtime_contract_version: runtime-contracts.v8
    provider_lane: fixture
    attempt_owner: embedded_run_stream
    harness_id: embedded_run_stream
    context_engine_id: legacy_provider_input
    max_fallback_count: 0
    allowed_fallback_reason_codes: []
"#;
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

        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.mode.runner, QaScenarioRunnerMode::Fixture);
        assert_eq!(manifest.mode.provider, QaScenarioProviderMode::Mock);
        let runner = manifest.runner.as_ref().expect("fixture runner config should be present");
        assert!(matches!(runner.binding(), QaScenarioProviderBinding::Fixture(_)));
        assert_eq!(runner.runner_mode(), QaScenarioRunnerMode::Fixture);
        assert_eq!(runner.provider_fixture(), Some("qa/fixtures/provider_basic.yaml"));
        assert_eq!(runner.replay_fixture(), None);
        assert_eq!(runner.workspace_fixture(), Some("qa/fixtures/sandbox_workspaces/repo_basic"));
        assert_eq!(runner.policy_profile(), Some("qa_read_only"));
        assert_eq!(runner.raw_payload_storage(), None);
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
        for (runner, expected_provider) in [
            ("record_replay", QaScenarioProviderMode::Recorded),
            ("live", QaScenarioProviderMode::Live),
        ] {
            let scenario = V2_FIXTURE_SCENARIO
                .replace("runner: fixture", &format!("runner: {runner}"))
                .replace(FIXTURE_RUNNER_BLOCK, "");
            let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect("schema-v2 non-fixture mode should retain its config-free projection");

            assert_eq!(manifest.mode.runner.as_str(), runner);
            assert_eq!(manifest.mode.provider, expected_provider);
            assert_eq!(manifest.runner, None);
        }
    }

    #[test]
    fn schema_v2_rejects_record_replay_and_live_runner_config() {
        for (runner_mode, runner_block) in
            [("record_replay", RECORD_REPLAY_RUNNER_BLOCK), ("live", LIVE_RUNNER_BLOCK)]
        {
            let scenario = V2_FIXTURE_SCENARIO
                .replace("runner: fixture", &format!("runner: {runner_mode}"))
                .replace(FIXTURE_RUNNER_BLOCK, runner_block);

            assert_validation_issue(&scenario, "$.runner", "runner_config_not_supported_for_mode");
        }
    }

    #[test]
    fn schema_v3_fixture_uses_typed_provider_binding() {
        let scenario = V2_FIXTURE_SCENARIO.replace("schema_version: 2", "schema_version: 3");
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v3 fixture binding should parse");
        let runner = manifest.runner.as_ref().expect("schema-v3 fixture config should be present");

        assert_eq!(runner.runner_mode(), QaScenarioRunnerMode::Fixture);
        assert_eq!(runner.provider_fixture(), Some("qa/fixtures/provider_basic.yaml"));
    }

    #[test]
    fn schema_v4_fault_plan_and_expectations_round_trip() {
        let scenario = schema_v4_fault_scenario();
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v4 fault scenario should parse");

        assert_eq!(manifest.schema_version, 4);
        let plan = manifest.fault_injection.as_ref().expect("fault plan should be retained");
        assert_eq!(plan.seed, 4242);
        assert_eq!(plan.activations.len(), 1);
        let expectations = manifest
            .expect
            .fault_injection
            .as_ref()
            .expect("fault expectations should be retained");
        assert_eq!(expectations.daemon_restarts, 1);
        assert_eq!(
            expectations.activations[0].recovery_class,
            QaFaultRecoveryClass::DuplicateSuppressed
        );

        let json = serde_json::to_string(&manifest)
            .expect("schema-v4 fault manifest should serialize as JSON");
        let json_round_trip = parse_qa_scenario_manifest_yaml(json.as_str())
            .expect("schema-v4 JSON projection should parse");
        assert_eq!(json_round_trip, manifest);
        let yaml = yaml_serde::to_string(&manifest)
            .expect("schema-v4 fault manifest should serialize as YAML");
        let yaml_round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
            .expect("schema-v4 YAML projection should parse");
        assert_eq!(yaml_round_trip, manifest);
    }

    #[test]
    fn schema_v5_runtime_path_expectation_round_trips() {
        let scenario = schema_v5_scenario();
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v5 runtime-path scenario should parse");

        assert_eq!(manifest.schema_version, 5);
        let runtime_path = manifest
            .expect
            .runtime_path
            .as_ref()
            .expect("schema-v5 runtime path should be retained");
        assert_eq!(runtime_path.runtime_contract_version, "runtime-contracts.v8");
        assert_eq!(runtime_path.provider_lane, "fixture");
        assert_eq!(runtime_path.attempt_owner, "embedded_run_stream");
        assert_eq!(runtime_path.harness_id, "embedded_run_stream");
        assert_eq!(runtime_path.context_engine_id, "legacy_provider_input");
        assert_eq!(runtime_path.mcp_transport_mode, None);
        assert_eq!(runtime_path.max_fallback_count, 0);
        assert!(runtime_path.allowed_fallback_reason_codes.is_empty());

        let json = serde_json::to_string(&manifest)
            .expect("schema-v5 runtime-path manifest should serialize as JSON");
        let json_round_trip = parse_qa_scenario_manifest_yaml(json.as_str())
            .expect("schema-v5 JSON projection should parse");
        assert_eq!(json_round_trip, manifest);
        let yaml = yaml_serde::to_string(&manifest)
            .expect("schema-v5 runtime-path manifest should serialize as YAML");
        let yaml_round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
            .expect("schema-v5 YAML projection should parse");
        assert_eq!(yaml_round_trip, manifest);
    }

    #[test]
    fn schema_v5_mcp_transport_mode_is_typed_and_round_trips() {
        let scenario = schema_v5_scenario().replace(
            "    context_engine_id: legacy_provider_input\n",
            "    context_engine_id: legacy_provider_input\n    mcp_transport_mode: per_call\n",
        );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("a canonical MCP transport mode should parse");
        let runtime_path = manifest
            .expect
            .runtime_path
            .as_ref()
            .expect("schema-v5 runtime path should be retained");

        assert_eq!(runtime_path.mcp_transport_mode, Some(McpTransportInvocationMode::PerCall));

        let yaml = yaml_serde::to_string(&manifest)
            .expect("typed MCP transport expectation should serialize as YAML");
        let round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
            .expect("typed MCP transport expectation should round-trip");
        assert_eq!(round_trip, manifest);
    }

    #[test]
    fn schema_v5_requires_runtime_path_expectation() {
        let scenario = V2_FIXTURE_SCENARIO.replace("schema_version: 2", "schema_version: 5");

        assert_validation_issue(
            scenario.as_str(),
            "$.expect.runtime_path",
            "missing_runtime_path_expectation",
        );
    }

    #[test]
    fn schemas_v1_through_v4_reject_runtime_path_expectation() {
        for schema_version in [1, 2, 3, 4] {
            let scenario = schema_v5_scenario().replacen(
                "schema_version: 5",
                format!("schema_version: {schema_version}").as_str(),
                1,
            );

            assert_validation_issue(
                scenario.as_str(),
                "$.expect.runtime_path",
                "runtime_path_expectation_requires_schema_v5",
            );
        }
    }

    #[test]
    fn schema_v5_requires_runtime_path_provider_lane_to_match_runner() {
        let scenario =
            schema_v5_scenario().replace("provider_lane: fixture", "provider_lane: live");

        assert_validation_issue(
            scenario.as_str(),
            "$.expect.runtime_path.provider_lane",
            "runtime_path_provider_lane_mismatch",
        );
    }

    #[test]
    fn schema_v5_rejects_unsafe_runtime_path_metadata() {
        let scenario = schema_v5_scenario().replace("runtime-contracts.v8", "runtime contracts v8");

        assert_validation_issue(
            scenario.as_str(),
            "$.expect.runtime_path.runtime_contract_version",
            "runtime_path_metadata_invalid",
        );
    }

    #[test]
    fn schema_v5_rejects_non_transport_mcp_modes() {
        for mode in ["not_used", "custom_session"] {
            let scenario = schema_v5_scenario().replace(
                "    context_engine_id: legacy_provider_input\n",
                format!(
                    "    context_engine_id: legacy_provider_input\n    mcp_transport_mode: {mode}\n"
                )
                .as_str(),
            );

            let error = parse_qa_scenario_manifest_yaml(scenario.as_str())
                .expect_err("an unknown MCP transport mode should fail typed decoding");

            assert!(matches!(error, QaScenarioManifestError::Parse { .. }));
            assert!(
                error.to_string().contains("unknown variant"),
                "unexpected parse error for mode={mode}: {error}"
            );
        }
    }

    #[test]
    fn schema_v5_preserves_schema_v4_fault_contracts() {
        let scenario =
            FAULT_TOOL_SCENARIO.replace("schema_version: 4", "schema_version: 5").replacen(
                "forbidden:\n",
                format!("{RUNTIME_PATH_EXPECTATION_BLOCK}forbidden:\n").as_str(),
                1,
            );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v5 should preserve schema-v4 fault contracts");

        assert!(manifest.expect.runtime_path.is_some());
        assert_eq!(manifest.fault_injection.as_ref().map(|plan| plan.seed), Some(20_260_710));
        assert_eq!(
            manifest.expect.fault_injection.as_ref().map(|expectation| expectation.daemon_restarts),
            Some(1)
        );
        assert_eq!(manifest.expect.tool_calls[0].max_count, Some(1));
        assert_eq!(
            manifest.artifacts[0].sha256.as_deref(),
            Some("78930e608b8acfc1799a5e09e2f1bdb408dde4fd2607d38b86acb16130c2c550")
        );
    }

    #[test]
    fn schema_v4_retains_exact_tool_count_and_artifact_digest_expectations() {
        let manifest = parse_qa_scenario_manifest_yaml(FAULT_TOOL_SCENARIO)
            .expect("fault tool scenario should parse");

        assert_eq!(manifest.expect.tool_calls[0].min_count, Some(1));
        assert_eq!(manifest.expect.tool_calls[0].max_count, Some(1));
        assert_eq!(manifest.expect.tool_calls[0].success, None);
        assert_eq!(
            manifest.artifacts[0].sha256.as_deref(),
            Some("78930e608b8acfc1799a5e09e2f1bdb408dde4fd2607d38b86acb16130c2c550")
        );
    }

    #[test]
    fn schema_v4_rejects_inverted_tool_count_and_noncanonical_artifact_digest() {
        let invalid_count = FAULT_TOOL_SCENARIO.replace("max_count: 1", "max_count: 0");
        assert_validation_issue(
            invalid_count.as_str(),
            "$.expect.tool_calls[0].max_count",
            "invalid_expected_tool_call_count_range",
        );

        let invalid_digest = FAULT_TOOL_SCENARIO
            .replace("78930e608b8acfc1799a5e09e2f1bdb408dde4fd2607d38b86acb16130c2c550", "ABC123");
        assert_validation_issue(
            invalid_digest.as_str(),
            "$.artifacts[0].sha256",
            "invalid_artifact_sha256",
        );
    }

    #[test]
    fn rejects_min_counts_that_would_expand_unbounded_preview_evidence() {
        let event = V2_FIXTURE_SCENARIO.replace(
            "  events: []",
            "  events:\n    - event_type: qa.event\n      min_count: 4294967295",
        );
        assert_validation_issue(
            event.as_str(),
            "$.expect.events[0].min_count",
            "expected_min_count_out_of_range",
        );

        let tool_call = V2_FIXTURE_SCENARIO.replace(
            "  tool_calls: []",
            "  tool_calls:\n    - name: palyra.fs.read_file\n      min_count: 4294967295",
        );
        assert_validation_issue(
            tool_call.as_str(),
            "$.expect.tool_calls[0].min_count",
            "expected_min_count_out_of_range",
        );
    }

    #[test]
    fn schemas_v1_through_v3_reject_fault_contracts() {
        for schema_version in [1, 2, 3] {
            let scenario = schema_v4_fault_scenario().replacen(
                "schema_version: 4",
                format!("schema_version: {schema_version}").as_str(),
                1,
            );
            assert_validation_issue(
                scenario.as_str(),
                "$.fault_injection",
                "fault_injection_requires_schema_v4",
            );
            assert_validation_issue(
                scenario.as_str(),
                "$.expect.fault_injection",
                "fault_expectations_require_schema_v4",
            );
        }
    }

    #[test]
    fn schemas_v1_through_v3_reject_v4_tool_and_artifact_contracts() {
        let scenario = V2_FIXTURE_SCENARIO
            .replacen(
                "  tool_calls: []",
                "  tool_calls:\n    - name: palyra.fs.read_file\n      max_count: 1",
                1,
            )
            .replace(
                "\nartifacts: []\n",
                "\nartifacts:\n  - path: src/app.txt\n    kind: workspace\n    required: true\n    sha256: 78930e608b8acfc1799a5e09e2f1bdb408dde4fd2607d38b86acb16130c2c550\n",
            );
        for schema_version in [1, 2, 3] {
            let versioned = scenario.replacen(
                "schema_version: 2",
                format!("schema_version: {schema_version}").as_str(),
                1,
            );
            assert_validation_issue(
                versioned.as_str(),
                "$.expect.tool_calls[0].max_count",
                "tool_call_max_count_requires_schema_v4",
            );
            assert_validation_issue(
                versioned.as_str(),
                "$.artifacts[0].kind",
                "workspace_artifact_requires_schema_v4",
            );
            assert_validation_issue(
                versioned.as_str(),
                "$.artifacts[0].sha256",
                "artifact_sha256_requires_schema_v4",
            );
        }
    }

    #[test]
    fn schema_v4_requires_plan_and_expectations_together() {
        let scenario = schema_v4_fault_scenario();
        let without_expectations = scenario.replace(FAULT_EXPECTATION_BLOCK, "");
        assert_validation_issue(
            without_expectations.as_str(),
            "$.expect.fault_injection",
            "missing_fault_expectations",
        );

        let without_plan = scenario.replace(FAULT_PLAN_BLOCK, "");
        assert_validation_issue(
            without_plan.as_str(),
            "$.expect.fault_injection",
            "fault_expectations_require_plan",
        );
    }

    #[test]
    fn schema_v4_rejects_invalid_or_unsupported_fault_recovery() {
        let unknown =
            schema_v4_fault_scenario().replace("duplicate_suppressed", "not_a_recovery_class");
        assert_validation_issue(
            unknown.as_str(),
            "$.expect.fault_injection.activations[0].recovery_class",
            "unknown_fault_recovery_class",
        );

        let unsupported =
            schema_v4_fault_scenario().replace("duplicate_suppressed", "cleanup_succeeded");
        assert_validation_issue(
            unsupported.as_str(),
            "$.expect.fault_injection.activations[0].recovery_class",
            "unsupported_fault_recovery_class",
        );
    }

    #[test]
    fn schema_v4_projects_plan_validation_paths_under_fault_injection() {
        let scenario = schema_v4_fault_scenario().replace("occurrence: 1", "occurrence: 0");

        assert_validation_issue(
            scenario.as_str(),
            "$.fault_injection.activations[0].occurrence",
            "invalid_occurrence",
        );
    }

    #[test]
    fn schema_v4_workspace_artifact_accepts_exact_relative_effect_path() {
        let scenario = V2_FIXTURE_SCENARIO
            .replace("schema_version: 2", "schema_version: 4")
            .replace(
            "\nartifacts: []\n",
            "\nartifacts:\n  - path: src/fault-once.txt\n    kind: workspace\n    required: true\n",
        );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v4 workspace artifact should parse");

        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.artifacts[0].path, "src/fault-once.txt");
        assert_eq!(manifest.artifacts[0].kind, QaScenarioArtifactKind::Workspace);
    }

    #[test]
    fn parses_and_round_trips_record_replay_binding() {
        let scenario = schema_v3_scenario_with_runner("record_replay", RECORD_REPLAY_RUNNER_BLOCK);
        let manifest = assert_schema_v3_scenario_round_trips(scenario.as_str());
        let runner = manifest.runner.as_ref().expect("record-replay config should be present");

        assert!(matches!(runner.binding(), QaScenarioProviderBinding::RecordReplay(_)));
        assert_eq!(runner.replay_fixture(), Some("qa/fixtures/provider_basic.yaml"));
        assert_eq!(runner.fixture_redacted(), Some(true));
        assert_eq!(runner.raw_payload_storage(), Some(false));
        assert_eq!(runner.provider_fixture(), None);
    }

    #[test]
    fn parses_and_round_trips_live_binding() {
        let scenario = schema_v3_scenario_with_runner("live", LIVE_RUNNER_BLOCK);
        let manifest = assert_schema_v3_scenario_round_trips(scenario.as_str());
        let runner = manifest.runner.as_ref().expect("live config should be present");

        assert!(matches!(runner.binding(), QaScenarioProviderBinding::Live(_)));
        assert_eq!(runner.live_secret_profile_env(), Some("PALYRA_QA_LIVE_OPENAI"));
        assert_eq!(runner.live_provider_kind(), Some(QaScenarioLiveProviderKind::OpenAiCompatible));
        assert_eq!(runner.live_model(), Some("qa-live-model"));
        assert_eq!(runner.live_base_url(), Some("https://api.example.test/v1"));
        assert_eq!(runner.raw_payload_storage(), Some(false));
        assert_eq!(runner.provider_fixture(), None);
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
        let scenario = V2_FIXTURE_SCENARIO.replace(FIXTURE_RUNNER_BLOCK, "");

        assert_validation_issue(&scenario, "$.runner", "missing_runner_config");
    }

    #[test]
    fn schema_v3_non_fixture_modes_require_runner_config() {
        for (runner_mode, runner_block) in
            [("record_replay", RECORD_REPLAY_RUNNER_BLOCK), ("live", LIVE_RUNNER_BLOCK)]
        {
            let scenario =
                schema_v3_scenario_with_runner(runner_mode, runner_block).replace(runner_block, "");

            assert_validation_issue(&scenario, "$.runner", "missing_runner_config");
        }
    }

    #[test]
    fn schema_v2_fixture_mode_requires_provider_fixture() {
        let scenario = V2_FIXTURE_SCENARIO
            .replace("  provider_fixture: qa/fixtures/provider_basic.yaml\n", "");

        assert_validation_issue(&scenario, "$.runner.provider_fixture", "missing_provider_fixture");
    }

    #[test]
    fn rejects_unsafe_runner_paths_with_precise_paths() {
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

        let unsafe_replay =
            schema_v3_scenario_with_runner("record_replay", RECORD_REPLAY_RUNNER_BLOCK)
                .replace("qa/fixtures/provider_basic.yaml", "../provider-secret.yaml");
        assert_validation_issue(&unsafe_replay, "$.runner.replay_fixture", "unsafe_runner_path");
    }

    #[test]
    fn requires_fixtures_stay_below_the_fixture_root() {
        for unsafe_path in [
            "../../secret.yaml",
            "/etc/passwd",
            r"C:\secret.yaml",
            r"qa\fixtures\provider.yaml",
            "fixtures/provider.yaml",
        ] {
            let scenario = V2_FIXTURE_SCENARIO
                .replace("  fixtures: []", format!("  fixtures:\n    - {unsafe_path}").as_str());

            assert_validation_issue(&scenario, "$.requires.fixtures[0]", "unsafe_fixture_path");
        }
    }

    #[test]
    fn artifact_paths_stay_below_qa_artifact_roots() {
        for unsafe_path in [
            "../../report.json",
            "/tmp/report.json",
            r"C:\report.json",
            r"qa\reports\report.json",
            "reports/report.json",
        ] {
            let scenario = EXAMPLE_SCENARIO.replace("qa/reports/text_run_basic.json", unsafe_path);

            assert_validation_issue(&scenario, "$.artifacts[0].path", "unsafe_artifact_path");
        }
    }

    #[test]
    fn record_replay_requires_explicit_redacted_fixture_posture() {
        let scenario = schema_v3_scenario_with_runner("record_replay", RECORD_REPLAY_RUNNER_BLOCK);
        let omitted = scenario.replace("  fixture_redacted: true\n", "");
        assert_validation_issue(&omitted, "$.runner.fixture_redacted", "missing_fixture_redacted");

        let false_value = scenario.replace("  fixture_redacted: true", "  fixture_redacted: false");
        assert_validation_issue(
            &false_value,
            "$.runner.fixture_redacted",
            "fixture_must_be_redacted",
        );
    }

    #[test]
    fn record_replay_and_live_require_raw_payload_storage_disabled() {
        for (runner_mode, runner_block) in
            [("record_replay", RECORD_REPLAY_RUNNER_BLOCK), ("live", LIVE_RUNNER_BLOCK)]
        {
            let scenario = schema_v3_scenario_with_runner(runner_mode, runner_block);
            let omitted = scenario.replace("  raw_payload_storage: false\n", "");
            assert_validation_issue(
                &omitted,
                "$.runner.raw_payload_storage",
                "missing_raw_payload_storage",
            );

            let enabled =
                scenario.replace("  raw_payload_storage: false", "  raw_payload_storage: true");
            assert_validation_issue(
                &enabled,
                "$.runner.raw_payload_storage",
                "raw_payload_storage_must_be_disabled",
            );
        }
    }

    #[test]
    fn live_binding_requires_valid_indirect_secret_profile_env() {
        for invalid_env in [
            "PALYRA_QA_LIVE_",
            "PALYRA_QA_LIVE_openai",
            "PALYRA_QA_LIVE_OPENAI-PROD",
            "PALYRA_LIVE_OPENAI",
        ] {
            let scenario = schema_v3_scenario_with_runner("live", LIVE_RUNNER_BLOCK)
                .replace("PALYRA_QA_LIVE_OPENAI", invalid_env);
            assert_validation_issue(
                &scenario,
                "$.runner.secret_profile_env",
                "invalid_live_secret_profile_env",
            );
        }
    }

    #[test]
    fn live_binding_requires_supported_provider_kind_and_nonempty_model() {
        let scenario = schema_v3_scenario_with_runner("live", LIVE_RUNNER_BLOCK);
        let anthropic = scenario.replace("openai_compatible", "anthropic");
        let anthropic_manifest = parse_qa_scenario_manifest_yaml(anthropic.as_str())
            .expect("anthropic live provider kind should parse");
        assert_eq!(
            anthropic_manifest.runner.as_ref().and_then(QaScenarioRunnerConfig::live_provider_kind),
            Some(QaScenarioLiveProviderKind::Anthropic)
        );

        let unknown_provider = scenario.replace("openai_compatible", "deterministic");
        assert_validation_issue(
            &unknown_provider,
            "$.runner.provider_kind",
            "unknown_live_provider_kind",
        );

        let empty_model = scenario.replace("model: qa-live-model", "model: '   '");
        assert_validation_issue(&empty_model, "$.runner.model", "missing_live_model");

        let empty_base_url =
            scenario.replace("base_url: https://api.example.test/v1", "base_url: '   '");
        assert_validation_issue(&empty_base_url, "$.runner.base_url", "empty_string");

        let without_base_url = scenario.replace("  base_url: https://api.example.test/v1\n", "");
        let manifest = parse_qa_scenario_manifest_yaml(without_base_url.as_str())
            .expect("live base URL should remain optional");
        assert_eq!(manifest.runner.as_ref().and_then(QaScenarioRunnerConfig::live_base_url), None);
    }

    #[test]
    fn schema_v3_rejects_every_lane_incompatible_runner_field() {
        let cases = [
            (
                "fixture",
                FIXTURE_RUNNER_BLOCK,
                [
                    ("replay_fixture", "qa/fixtures/replay.yaml"),
                    ("fixture_redacted", "true"),
                    ("secret_profile_env", "PALYRA_QA_LIVE_OPENAI"),
                    ("provider_kind", "openai_compatible"),
                    ("model", "qa-live-model"),
                    ("base_url", "https://api.example.test/v1"),
                    ("raw_payload_storage", "false"),
                ]
                .as_slice(),
            ),
            (
                "record_replay",
                RECORD_REPLAY_RUNNER_BLOCK,
                [
                    ("provider_fixture", "qa/fixtures/provider.yaml"),
                    ("secret_profile_env", "PALYRA_QA_LIVE_OPENAI"),
                    ("provider_kind", "openai_compatible"),
                    ("model", "qa-live-model"),
                    ("base_url", "https://api.example.test/v1"),
                ]
                .as_slice(),
            ),
            (
                "live",
                LIVE_RUNNER_BLOCK,
                [
                    ("provider_fixture", "qa/fixtures/provider.yaml"),
                    ("replay_fixture", "qa/fixtures/replay.yaml"),
                    ("fixture_redacted", "true"),
                ]
                .as_slice(),
            ),
        ];

        for (runner_mode, runner_block, incompatible_fields) in cases {
            for (field, value) in incompatible_fields {
                let scenario = schema_v3_scenario_with_extra_runner_field(
                    runner_mode,
                    runner_block,
                    field,
                    value,
                );
                assert_runner_field_not_supported(&scenario, runner_mode, field);
            }
        }
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

    fn schema_v3_scenario_with_runner(runner_mode: &str, runner_block: &str) -> String {
        V2_FIXTURE_SCENARIO
            .replace("schema_version: 2", "schema_version: 3")
            .replace("runner: fixture", &format!("runner: {runner_mode}"))
            .replace(FIXTURE_RUNNER_BLOCK, runner_block)
    }

    fn schema_v4_fault_scenario() -> String {
        V2_FIXTURE_SCENARIO
            .replace("schema_version: 2", "schema_version: 4")
            .replace(
                FIXTURE_RUNNER_BLOCK,
                format!("{FIXTURE_RUNNER_BLOCK}{FAULT_PLAN_BLOCK}").as_str(),
            )
            .replace(
                "  tool_calls: []\nforbidden:",
                format!("  tool_calls: []\n{FAULT_EXPECTATION_BLOCK}forbidden:").as_str(),
            )
    }

    fn schema_v5_scenario() -> String {
        V2_FIXTURE_SCENARIO.replace("schema_version: 2", "schema_version: 5").replace(
            "  tool_calls: []\nforbidden:",
            format!("  tool_calls: []\n{RUNTIME_PATH_EXPECTATION_BLOCK}forbidden:").as_str(),
        )
    }

    fn schema_v3_scenario_with_extra_runner_field(
        runner_mode: &str,
        runner_block: &str,
        field: &str,
        value: &str,
    ) -> String {
        schema_v3_scenario_with_runner(runner_mode, runner_block).replace(
            "  policy_profile: qa_read_only\n",
            &format!("  policy_profile: qa_read_only\n  {field}: {value}\n"),
        )
    }

    fn assert_schema_v3_scenario_round_trips(scenario: &str) -> QaScenarioManifest {
        let manifest = parse_qa_scenario_manifest_yaml(scenario)
            .expect("schema-v3 provider binding should parse");

        let json =
            serde_json::to_string(&manifest).expect("schema-v3 manifest should serialize as JSON");
        let json_round_trip = parse_qa_scenario_manifest_yaml(json.as_str())
            .expect("schema-v3 JSON projection should remain valid");
        assert_eq!(json_round_trip, manifest);

        let yaml =
            yaml_serde::to_string(&manifest).expect("schema-v3 manifest should serialize as YAML");
        let yaml_round_trip = parse_qa_scenario_manifest_yaml(yaml.as_str())
            .expect("schema-v3 YAML projection should remain valid");
        assert_eq!(yaml_round_trip, manifest);

        manifest
    }

    fn assert_runner_field_not_supported(scenario: &str, runner_mode: &str, field: &str) {
        let error = parse_qa_scenario_manifest_yaml(scenario)
            .expect_err("lane-incompatible runner field should fail validation");
        let issues = error.issues().expect("validation issues should be available");
        let expected_path = format!("$.runner.{field}");
        let issue = issues
            .iter()
            .find(|issue| {
                issue.path == expected_path && issue.code == "runner_field_not_supported_for_mode"
            })
            .unwrap_or_else(|| {
                panic!(
                    "missing incompatible-field issue for mode={runner_mode} field={field}; issues={issues:?}"
                )
            });

        assert_eq!(
            issue.message,
            format!("runner.{field} is not supported when mode.runner is '{runner_mode}'")
        );
        assert_eq!(
            issue.recovery_hint,
            format!("Remove runner.{field} from the {runner_mode} runner configuration.")
        );
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
