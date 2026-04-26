use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    replay_bundle::{ReplayArtifactRef, ReplayBundle, ReplayTapeEvent},
    runtime_contracts::RunLifecycleTransitionRecord,
};

/// Release-gate eval suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalSuiteKind {
    ProviderRuntimeConformance,
    ToolSecurityExploitCorpus,
    LongRunAgentEval,
    ContextQualityRegression,
    AcpRealtimeProtocolContracts,
    SchedulerQueueDeliveryRecovery,
    PluginConnectorContractMatrix,
    NodeCapabilityGrantTests,
    SecurityInvariantTests,
    ReplayEvalRunnerReleaseGate,
}

impl ReleaseEvalSuiteKind {
    /// Stable string identifier used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderRuntimeConformance => "provider_runtime_conformance",
            Self::ToolSecurityExploitCorpus => "tool_security_exploit_corpus",
            Self::LongRunAgentEval => "long_run_agent_eval",
            Self::ContextQualityRegression => "context_quality_regression",
            Self::AcpRealtimeProtocolContracts => "acp_realtime_protocol_contracts",
            Self::SchedulerQueueDeliveryRecovery => "scheduler_queue_delivery_recovery",
            Self::PluginConnectorContractMatrix => "plugin_connector_contract_matrix",
            Self::NodeCapabilityGrantTests => "node_capability_grant_tests",
            Self::SecurityInvariantTests => "security_invariant_tests",
            Self::ReplayEvalRunnerReleaseGate => "replay_eval_runner_release_gate",
        }
    }
}

/// Coverage dimension required by one or more release eval suites.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalDimension {
    ErrorTaxonomy,
    Retry,
    PromptAssembly,
    Streaming,
    Cancellation,
    UsageAccounting,
    FakeProvider,
    DeterministicProvider,
    RateLimitRetryMatrix,
    SandboxEscape,
    PromptToolInjection,
    SecretExfiltration,
    PathTraversal,
    Ssrf,
    ApprovalBypass,
    ExpectedDenyAudit,
    Compaction,
    Tools,
    Artifacts,
    Approvals,
    ProviderFailures,
    Completion,
    SafetyViolations,
    TokenCost,
    CompactionQuality,
    Recovery,
    ReplayBundle,
    ActiveTaskRetention,
    HallucinationRisk,
    ToolIntegrity,
    MemoryInjection,
    PreCompression,
    PostCompression,
    Restart,
    SummaryDiff,
    RealtimeWire,
    AcpWire,
    GoldenRequestResponseEvent,
    BackwardCompatibility,
    VersionBump,
    ScopeCapabilityNegative,
    StaleRunning,
    AckUncertainty,
    DeadLetter,
    PendingMerge,
    MisfireCatchUp,
    CriticalRestart,
    JournalState,
    OutwardDeliveryState,
    AckNackUnknownTrace,
    ContractKind,
    AbiVersion,
    ManifestCapabilities,
    InstallScan,
    ConnectorSimulator,
    CrashIsolation,
    InvalidCapabilityFailClosed,
    GoldenAbi,
    PresenceTtl,
    Heartbeat,
    Capabilities,
    AttestationMetadata,
    GrantRevokeJournal,
    CommandRouting,
    StaleNodeNoWork,
    MtlsIdentityMismatch,
    CommandAllowlist,
    SdkWitVersionSeparate,
    CompatibilityMatrix,
    SdkGeneratorVersionGuard,
    NoSecretInPromptLogEvent,
    NoSideEffectWithoutPolicy,
    NoToolBypass,
    NoCrossSessionEventLeak,
    PositiveControl,
    NegativeControl,
    ReleaseGate,
    AggregatedReport,
    ReplayBundles,
    GoldenProtocolInventory,
    CiReleaseGate,
    DeterminismGate,
    PolicyGate,
    SafetyScoreGate,
}

/// Assertion family evaluated for a case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalAssertionKind {
    PositiveControl,
    NegativeControl,
    FailClosed,
    AuditEvent,
    ReplayBundlePasses,
    SafetyScoreMinimum,
    GoldenProtocol,
    NoSecretLeak,
    PolicyGate,
    Deterministic,
    CompatibilityVersioned,
    CommandRouting,
    RecoveryState,
    AggregateReport,
}

/// Pass/fail status for release eval reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalStatus {
    Passed,
    Failed,
}

/// Severity for an eval gate issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalIssueSeverity {
    Error,
    Warning,
}

/// Golden manifest consumed by the release eval gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvalManifest {
    pub schema_version: u32,
    pub contract_version: String,
    pub inventory: ReleaseGoldenProtocolInventory,
    pub suites: Vec<ReleaseEvalSuite>,
}

/// Golden protocol inventory required by the release gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseGoldenProtocolInventory {
    pub schema_version: u32,
    pub inventory_version: String,
    pub updated_on: String,
    pub change_reason: String,
    pub protocols: Vec<ReleaseProtocolInventoryEntry>,
}

/// One protocol or ABI contract pinned by the release eval inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseProtocolInventoryEntry {
    pub domain: String,
    pub contract: String,
    pub version: String,
    pub compatibility_policy: String,
    pub golden_fixture: String,
    pub change_reason: String,
}

/// One suite in the release eval manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvalSuite {
    pub kind: ReleaseEvalSuiteKind,
    pub title: String,
    pub release_gate: bool,
    pub minimum_safety_score_bps: u32,
    #[serde(default)]
    pub invariants: Vec<String>,
    pub cases: Vec<ReleaseEvalCase>,
}

/// One deterministic eval case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvalCase {
    pub case_id: String,
    pub title: String,
    pub deterministic: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flaky: Option<ReleaseFlakyMark>,
    pub safety_score_bps: u32,
    pub dimensions: Vec<ReleaseEvalDimension>,
    pub assertions: Vec<ReleaseEvalAssertion>,
    pub replay: ReleaseReplayFixture,
}

/// Explicit marker for a known flaky case. Flaky cases still fail on regression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseFlakyMark {
    pub reason: String,
    pub trend_metric: String,
}

/// Expected and observed condition for a case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvalAssertion {
    pub kind: ReleaseEvalAssertionKind,
    pub target: String,
    pub expected: String,
    pub actual: String,
    pub passed: bool,
    pub evidence: String,
    pub recovery_hint: String,
}

/// Replay fixture used to generate a canonical replay bundle per eval case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReplayFixture {
    pub run_id: String,
    pub session_id: String,
    pub origin_kind: String,
    pub state: String,
    pub principal: String,
    pub device_id: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    #[serde(default)]
    pub config_snapshot: Value,
    pub tape_events: Vec<ReplayTapeEvent>,
    #[serde(default)]
    pub lifecycle_transitions: Vec<RunLifecycleTransitionRecord>,
    #[serde(default)]
    pub artifact_refs: Vec<ReplayArtifactRef>,
}

/// Result of evaluating a manifest, including generated replay bundles.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReleaseEvalOutput {
    pub report: ReleaseEvalReport,
    pub replay_bundles: Vec<ReleaseGeneratedReplayBundle>,
}

/// Aggregate report emitted by the release eval runner.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReleaseEvalReport {
    pub schema_version: u32,
    pub contract_version: String,
    pub status: ReleaseEvalStatus,
    pub summary: ReleaseEvalSummary,
    pub protocol_inventory: ReleaseGoldenProtocolInventory,
    pub issues: Vec<ReleaseEvalIssue>,
    pub suites: Vec<ReleaseEvalSuiteReport>,
}

/// Aggregate counters for a release eval run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseEvalSummary {
    pub suites_total: usize,
    pub suites_passed: usize,
    pub suites_failed: usize,
    pub cases_total: usize,
    pub cases_passed: usize,
    pub cases_failed: usize,
    pub release_gates: usize,
    pub generated_replay_bundles: usize,
    pub lowest_safety_score_bps: u32,
}

/// Suite-level report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReleaseEvalSuiteReport {
    pub kind: ReleaseEvalSuiteKind,
    pub status: ReleaseEvalStatus,
    pub release_gate: bool,
    pub missing_dimensions: Vec<ReleaseEvalDimension>,
    pub issues: Vec<ReleaseEvalIssue>,
    pub cases: Vec<ReleaseEvalCaseReport>,
}

/// Case-level report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseEvalCaseReport {
    pub case_id: String,
    pub status: ReleaseEvalStatus,
    pub safety_score_bps: u32,
    pub replay_bundle_id: Option<String>,
    pub replay_bundle_sha256: Option<String>,
    pub replay_status: ReleaseEvalStatus,
    pub issues: Vec<ReleaseEvalIssue>,
}

/// One issue emitted by the release-gate evaluator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReleaseEvalIssue {
    pub severity: ReleaseEvalIssueSeverity,
    pub code: String,
    pub path: String,
    pub message: String,
    pub recovery_hint: String,
}

/// Generated replay bundle metadata and payload for a case.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReleaseGeneratedReplayBundle {
    pub suite_kind: ReleaseEvalSuiteKind,
    pub case_id: String,
    pub bundle: ReplayBundle,
}
