use super::schema::{ReleaseEvalDimension, ReleaseEvalSuiteKind};

/// Release-gate minimum used by the canonical golden inventory.
pub const RELEASE_STRICT_SAFETY_SCORE_BPS: u32 = 10_000;

/// All release eval suites that must be present in the golden manifest.
pub const REQUIRED_RELEASE_SUITES: [ReleaseEvalSuiteKind; 10] = [
    ReleaseEvalSuiteKind::ProviderRuntimeConformance,
    ReleaseEvalSuiteKind::ToolSecurityExploitCorpus,
    ReleaseEvalSuiteKind::LongRunAgentEval,
    ReleaseEvalSuiteKind::ContextQualityRegression,
    ReleaseEvalSuiteKind::AcpRealtimeProtocolContracts,
    ReleaseEvalSuiteKind::SchedulerQueueDeliveryRecovery,
    ReleaseEvalSuiteKind::PluginConnectorContractMatrix,
    ReleaseEvalSuiteKind::NodeCapabilityGrantTests,
    ReleaseEvalSuiteKind::SecurityInvariantTests,
    ReleaseEvalSuiteKind::ReplayEvalRunnerReleaseGate,
];

const REQUIRED_PROTOCOL_INVENTORY: [&str; 10] = [
    "replay_bundle",
    "runtime_contracts",
    "provider_runtime",
    "tool_security",
    "realtime_ws",
    "acp",
    "scheduler_queue_delivery",
    "plugin_connector_contracts",
    "node_capability_grants",
    "security_invariants",
];

/// Canonical protocol inventory expected in release eval manifests.
#[must_use]
pub fn required_release_eval_protocol_inventory() -> &'static [&'static str] {
    &REQUIRED_PROTOCOL_INVENTORY
}

/// Dimensions required by a release eval suite.
#[must_use]
pub fn required_release_eval_dimensions(
    kind: ReleaseEvalSuiteKind,
) -> &'static [ReleaseEvalDimension] {
    match kind {
        ReleaseEvalSuiteKind::ProviderRuntimeConformance => &[
            ReleaseEvalDimension::ErrorTaxonomy,
            ReleaseEvalDimension::Retry,
            ReleaseEvalDimension::PromptAssembly,
            ReleaseEvalDimension::Streaming,
            ReleaseEvalDimension::Cancellation,
            ReleaseEvalDimension::UsageAccounting,
            ReleaseEvalDimension::FakeProvider,
            ReleaseEvalDimension::DeterministicProvider,
            ReleaseEvalDimension::RateLimitRetryMatrix,
        ],
        ReleaseEvalSuiteKind::ToolSecurityExploitCorpus => &[
            ReleaseEvalDimension::SandboxEscape,
            ReleaseEvalDimension::PromptToolInjection,
            ReleaseEvalDimension::SecretExfiltration,
            ReleaseEvalDimension::PathTraversal,
            ReleaseEvalDimension::Ssrf,
            ReleaseEvalDimension::ApprovalBypass,
            ReleaseEvalDimension::ExpectedDenyAudit,
        ],
        ReleaseEvalSuiteKind::LongRunAgentEval => &[
            ReleaseEvalDimension::Compaction,
            ReleaseEvalDimension::Tools,
            ReleaseEvalDimension::Artifacts,
            ReleaseEvalDimension::Approvals,
            ReleaseEvalDimension::ProviderFailures,
            ReleaseEvalDimension::Completion,
            ReleaseEvalDimension::SafetyViolations,
            ReleaseEvalDimension::TokenCost,
            ReleaseEvalDimension::CompactionQuality,
            ReleaseEvalDimension::Recovery,
            ReleaseEvalDimension::ReplayBundle,
        ],
        ReleaseEvalSuiteKind::ContextQualityRegression => &[
            ReleaseEvalDimension::ActiveTaskRetention,
            ReleaseEvalDimension::HallucinationRisk,
            ReleaseEvalDimension::ToolIntegrity,
            ReleaseEvalDimension::MemoryInjection,
            ReleaseEvalDimension::PreCompression,
            ReleaseEvalDimension::PostCompression,
            ReleaseEvalDimension::Restart,
            ReleaseEvalDimension::SummaryDiff,
        ],
        ReleaseEvalSuiteKind::AcpRealtimeProtocolContracts => &[
            ReleaseEvalDimension::RealtimeWire,
            ReleaseEvalDimension::AcpWire,
            ReleaseEvalDimension::GoldenRequestResponseEvent,
            ReleaseEvalDimension::BackwardCompatibility,
            ReleaseEvalDimension::VersionBump,
            ReleaseEvalDimension::ScopeCapabilityNegative,
        ],
        ReleaseEvalSuiteKind::SchedulerQueueDeliveryRecovery => &[
            ReleaseEvalDimension::StaleRunning,
            ReleaseEvalDimension::AckUncertainty,
            ReleaseEvalDimension::DeadLetter,
            ReleaseEvalDimension::PendingMerge,
            ReleaseEvalDimension::MisfireCatchUp,
            ReleaseEvalDimension::CriticalRestart,
            ReleaseEvalDimension::JournalState,
            ReleaseEvalDimension::OutwardDeliveryState,
            ReleaseEvalDimension::AckNackUnknownTrace,
        ],
        ReleaseEvalSuiteKind::PluginConnectorContractMatrix => &[
            ReleaseEvalDimension::ContractKind,
            ReleaseEvalDimension::AbiVersion,
            ReleaseEvalDimension::ManifestCapabilities,
            ReleaseEvalDimension::InstallScan,
            ReleaseEvalDimension::ConnectorSimulator,
            ReleaseEvalDimension::CrashIsolation,
            ReleaseEvalDimension::InvalidCapabilityFailClosed,
            ReleaseEvalDimension::GoldenAbi,
        ],
        ReleaseEvalSuiteKind::NodeCapabilityGrantTests => &[
            ReleaseEvalDimension::PresenceTtl,
            ReleaseEvalDimension::Heartbeat,
            ReleaseEvalDimension::Capabilities,
            ReleaseEvalDimension::AttestationMetadata,
            ReleaseEvalDimension::GrantRevokeJournal,
            ReleaseEvalDimension::CommandRouting,
            ReleaseEvalDimension::StaleNodeNoWork,
            ReleaseEvalDimension::MtlsIdentityMismatch,
            ReleaseEvalDimension::CommandAllowlist,
            ReleaseEvalDimension::SdkWitVersionSeparate,
            ReleaseEvalDimension::CompatibilityMatrix,
            ReleaseEvalDimension::GoldenAbi,
            ReleaseEvalDimension::SdkGeneratorVersionGuard,
        ],
        ReleaseEvalSuiteKind::SecurityInvariantTests => &[
            ReleaseEvalDimension::NoSecretInPromptLogEvent,
            ReleaseEvalDimension::NoSideEffectWithoutPolicy,
            ReleaseEvalDimension::NoToolBypass,
            ReleaseEvalDimension::NoCrossSessionEventLeak,
            ReleaseEvalDimension::PositiveControl,
            ReleaseEvalDimension::NegativeControl,
            ReleaseEvalDimension::ReleaseGate,
        ],
        ReleaseEvalSuiteKind::ReplayEvalRunnerReleaseGate => &[
            ReleaseEvalDimension::AggregatedReport,
            ReleaseEvalDimension::ReplayBundles,
            ReleaseEvalDimension::GoldenProtocolInventory,
            ReleaseEvalDimension::CiReleaseGate,
            ReleaseEvalDimension::DeterminismGate,
            ReleaseEvalDimension::PolicyGate,
            ReleaseEvalDimension::SafetyScoreGate,
        ],
    }
}
