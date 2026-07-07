//! Shared contract for agent-runtime roadmap milestones.
//!
//! This module keeps new runtime-loop work on one vocabulary: stable capability
//! ids, reason codes, journal event names, rollout gates, and redaction posture.
//! It is intentionally metadata-only; enabling behavior still belongs behind
//! the daemon's feature rollout config.

use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::feature_rollouts::{
    ACP_RUNTIME_ROLLOUT_CONFIG_PATH, ACP_RUNTIME_ROLLOUT_ENV, ADVISOR_FANOUT_ROLLOUT_CONFIG_PATH,
    ADVISOR_FANOUT_ROLLOUT_ENV, AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH,
    AGENT_HARNESS_RUNTIME_ROLLOUT_ENV, AGENT_PLAN_STATE_ROLLOUT_CONFIG_PATH,
    AGENT_PLAN_STATE_ROLLOUT_ENV, ATTACK_SURFACE_AUDIT_ROLLOUT_CONFIG_PATH,
    ATTACK_SURFACE_AUDIT_ROLLOUT_ENV, BROWSER_RESCUE_ROLLOUT_CONFIG_PATH,
    BROWSER_RESCUE_ROLLOUT_ENV, CHANNEL_TURN_KERNEL_ROLLOUT_CONFIG_PATH,
    CHANNEL_TURN_KERNEL_ROLLOUT_ENV, COMPACTION_SAFEGUARD_ROLLOUT_CONFIG_PATH,
    COMPACTION_SAFEGUARD_ROLLOUT_ENV, INLINE_RUNTIME_HOOKS_ROLLOUT_CONFIG_PATH,
    INLINE_RUNTIME_HOOKS_ROLLOUT_ENV, LSP_SERVICE_ROLLOUT_CONFIG_PATH, LSP_SERVICE_ROLLOUT_ENV,
    OBJECTIVE_JUDGE_ROLLOUT_CONFIG_PATH, OBJECTIVE_JUDGE_ROLLOUT_ENV,
    PROGRESS_DRAFTS_ROLLOUT_CONFIG_PATH, PROGRESS_DRAFTS_ROLLOUT_ENV,
    PROVIDER_RECOVERY_ROLLOUT_CONFIG_PATH, PROVIDER_RECOVERY_ROLLOUT_ENV,
    PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH, PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV,
    TERMINAL_SESSIONS_ROLLOUT_CONFIG_PATH, TERMINAL_SESSIONS_ROLLOUT_ENV,
    TOOL_REPAIR_ROLLOUT_CONFIG_PATH, TOOL_REPAIR_ROLLOUT_ENV,
    TOOL_RESULT_MIDDLEWARE_ROLLOUT_CONFIG_PATH, TOOL_RESULT_MIDDLEWARE_ROLLOUT_ENV,
    VERIFICATION_RUNTIME_ROLLOUT_CONFIG_PATH, VERIFICATION_RUNTIME_ROLLOUT_ENV,
};
use crate::redaction::{is_sensitive_key, redact_diagnostic_text};

/// Schema version stamped on runtime-roadmap journal payloads and catalog snapshots.
pub const RUNTIME_ROADMAP_SCHEMA_VERSION: u32 = 1;

macro_rules! runtime_roadmap_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $canonical:literal $(| $alias:literal )*
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            /// Returns the canonical serialized identifier.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $canonical,
                    )+
                }
            }

            /// Parses a canonical name or backward-compatible alias.
            ///
            /// Matching is case-insensitive and ignores surrounding whitespace.
            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    $(
                        $canonical $(| $alias )* => Some(Self::$variant),
                    )+
                    _ => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

runtime_roadmap_enum! {
    /// Coarse-grained capability groups used to align roadmap work with rollout gates.
    pub enum RuntimeRoadmapCapability {
        BaselineContracts => "baseline_contracts",
        ReasonCodeTaxonomy => "reason_code_taxonomy",
        JournalSchemaExtension => "journal_schema_extension",
        RuntimeLoopRolloutConfig => "runtime_loop_rollout_config",
        ReplayRegressionHarness => "replay_regression_harness",
        MilestoneImplementationStyle => "milestone_implementation_style",
        IntegrationSmokeValidation => "integration_smoke_validation",
        AgentHarnessRuntime => "agent_harness_runtime",
        InlineRuntimeHooks => "inline_runtime_hooks",
        ToolResultMiddleware => "tool_result_middleware",
        ToolRepair => "tool_repair",
        ProviderStreamNormalizer => "provider_stream_normalizer",
        ProviderRecovery => "provider_recovery",
        TerminalSessions => "terminal_sessions",
        BrowserRescue => "browser_rescue",
        LspService => "lsp_service",
        AdvisorFanout => "advisor_fanout",
        AcpRuntime => "acp_runtime",
        ChannelTurnKernel => "channel_turn_kernel",
        AgentPlanState => "agent_plan_state",
        ObjectiveJudge => "objective_judge",
        VerificationRuntime => "verification_runtime",
        ProgressDrafts => "progress_drafts",
        CompactionSafeguard => "compaction_safeguard",
        AttackSurfaceAudit => "attack_surface_audit"
    }
}

/// Every runtime-roadmap capability, in canonical display order.
pub const ALL_RUNTIME_ROADMAP_CAPABILITIES: [RuntimeRoadmapCapability; 25] = [
    RuntimeRoadmapCapability::BaselineContracts,
    RuntimeRoadmapCapability::ReasonCodeTaxonomy,
    RuntimeRoadmapCapability::JournalSchemaExtension,
    RuntimeRoadmapCapability::RuntimeLoopRolloutConfig,
    RuntimeRoadmapCapability::ReplayRegressionHarness,
    RuntimeRoadmapCapability::MilestoneImplementationStyle,
    RuntimeRoadmapCapability::IntegrationSmokeValidation,
    RuntimeRoadmapCapability::AgentHarnessRuntime,
    RuntimeRoadmapCapability::InlineRuntimeHooks,
    RuntimeRoadmapCapability::ToolResultMiddleware,
    RuntimeRoadmapCapability::ToolRepair,
    RuntimeRoadmapCapability::ProviderStreamNormalizer,
    RuntimeRoadmapCapability::ProviderRecovery,
    RuntimeRoadmapCapability::TerminalSessions,
    RuntimeRoadmapCapability::BrowserRescue,
    RuntimeRoadmapCapability::LspService,
    RuntimeRoadmapCapability::AdvisorFanout,
    RuntimeRoadmapCapability::AcpRuntime,
    RuntimeRoadmapCapability::ChannelTurnKernel,
    RuntimeRoadmapCapability::AgentPlanState,
    RuntimeRoadmapCapability::ObjectiveJudge,
    RuntimeRoadmapCapability::VerificationRuntime,
    RuntimeRoadmapCapability::ProgressDrafts,
    RuntimeRoadmapCapability::CompactionSafeguard,
    RuntimeRoadmapCapability::AttackSurfaceAudit,
];

impl RuntimeRoadmapCapability {
    /// Human-readable name for console, CLI, and diagnostics surfaces.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::BaselineContracts => "Baseline contracts",
            Self::ReasonCodeTaxonomy => "Reason code taxonomy",
            Self::JournalSchemaExtension => "Journal schema extension",
            Self::RuntimeLoopRolloutConfig => "Runtime loop rollout config",
            Self::ReplayRegressionHarness => "Replay regression harness",
            Self::MilestoneImplementationStyle => "Milestone implementation style",
            Self::IntegrationSmokeValidation => "Integration smoke validation",
            Self::AgentHarnessRuntime => "Agent harness runtime",
            Self::InlineRuntimeHooks => "Inline runtime hooks",
            Self::ToolResultMiddleware => "Tool result middleware",
            Self::ToolRepair => "Tool repair",
            Self::ProviderStreamNormalizer => "Provider stream normalizer",
            Self::ProviderRecovery => "Provider recovery",
            Self::TerminalSessions => "Terminal sessions",
            Self::BrowserRescue => "Browser rescue",
            Self::LspService => "LSP service",
            Self::AdvisorFanout => "Advisor fanout",
            Self::AcpRuntime => "ACP runtime",
            Self::ChannelTurnKernel => "Channel turn kernel",
            Self::AgentPlanState => "Agent plan state",
            Self::ObjectiveJudge => "Objective judge",
            Self::VerificationRuntime => "Verification runtime",
            Self::ProgressDrafts => "Progress drafts",
            Self::CompactionSafeguard => "Compaction safeguard",
            Self::AttackSurfaceAudit => "Attack surface audit",
        }
    }

    /// One-line operator-facing description of the capability's boundary.
    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::BaselineContracts => {
                "Defines the shared metadata contract used before roadmap runtime loops execute."
            }
            Self::ReasonCodeTaxonomy => {
                "Keeps roadmap reason codes stable across journal, replay, diagnostics, and tests."
            }
            Self::JournalSchemaExtension => {
                "Uses the existing journal append log with typed payload envelopes."
            }
            Self::RuntimeLoopRolloutConfig => {
                "Registers default-off rollout gates for new agent-runtime behavior."
            }
            Self::ReplayRegressionHarness => {
                "Defines shared replay and regression fixture contracts for roadmap work."
            }
            Self::MilestoneImplementationStyle => {
                "Documents the conservative implementation style expected for roadmap milestones."
            }
            Self::IntegrationSmokeValidation => {
                "Pins smoke targets for journal, run-stream, fixture, and diagnostics integration points."
            }
            Self::AgentHarnessRuntime => {
                "Guards agent harness selection, lifecycle, callbacks, transcripts, and authority fences."
            }
            Self::InlineRuntimeHooks => {
                "Guards inline hook invocation points before they enter the critical run loop."
            }
            Self::ToolResultMiddleware => {
                "Guards tool-result middleware before model-visible projection changes."
            }
            Self::ToolRepair => "Guards tool-call repair parsing and proposal recovery.",
            Self::ProviderStreamNormalizer => {
                "Guards provider stream normalization before tool proposal flow."
            }
            Self::ProviderRecovery => {
                "Guards bounded provider retry, repair, auth failover, and terminal recovery decisions."
            }
            Self::TerminalSessions => {
                "Guards persistent terminal process handles, cwd/env state, and cleanup evidence."
            }
            Self::BrowserRescue => {
                "Guards browser vision, dialog, CDP, and multimodal rescue boundaries."
            }
            Self::LspService => {
                "Guards code-intelligence service lifecycle, diagnostics, and workspace scoping."
            }
            Self::AdvisorFanout => {
                "Guards advisor fanout, trace attribution, budget governance, and non-authoritative reviews."
            }
            Self::AcpRuntime => {
                "Guards ACP runtime actor queues, permissions, replay, and native handoff boundaries."
            }
            Self::ChannelTurnKernel => {
                "Guards channel turn admission, debounce, history, and delivery lifecycle work."
            }
            Self::AgentPlanState => "Guards model-visible plan state and progress checkpoints.",
            Self::ObjectiveJudge => "Guards auxiliary objective completion judging.",
            Self::VerificationRuntime => {
                "Guards evidence ledger, verify-before-finish, and diagnostics status."
            }
            Self::ProgressDrafts => {
                "Guards durable progress drafts and long-running turn visibility."
            }
            Self::CompactionSafeguard => "Guards compaction checkpoints and rollback decisions.",
            Self::AttackSurfaceAudit => {
                "Guards attack-surface graphing and outbound sanitizer previews."
            }
        }
    }

    /// Returns the explicit rollout gate required before this capability can change behavior.
    #[must_use]
    pub const fn rollout_gate(self) -> Option<RuntimeRoadmapRolloutGate> {
        match self {
            Self::BaselineContracts
            | Self::ReasonCodeTaxonomy
            | Self::JournalSchemaExtension
            | Self::RuntimeLoopRolloutConfig
            | Self::ReplayRegressionHarness
            | Self::MilestoneImplementationStyle
            | Self::IntegrationSmokeValidation => None,
            Self::AgentHarnessRuntime => Some(RuntimeRoadmapRolloutGate::new(
                AGENT_HARNESS_RUNTIME_ROLLOUT_ENV,
                AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH,
            )),
            Self::InlineRuntimeHooks => Some(RuntimeRoadmapRolloutGate::new(
                INLINE_RUNTIME_HOOKS_ROLLOUT_ENV,
                INLINE_RUNTIME_HOOKS_ROLLOUT_CONFIG_PATH,
            )),
            Self::ToolResultMiddleware => Some(RuntimeRoadmapRolloutGate::new(
                TOOL_RESULT_MIDDLEWARE_ROLLOUT_ENV,
                TOOL_RESULT_MIDDLEWARE_ROLLOUT_CONFIG_PATH,
            )),
            Self::ToolRepair => Some(RuntimeRoadmapRolloutGate::new(
                TOOL_REPAIR_ROLLOUT_ENV,
                TOOL_REPAIR_ROLLOUT_CONFIG_PATH,
            )),
            Self::ProviderStreamNormalizer => Some(RuntimeRoadmapRolloutGate::new(
                PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV,
                PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH,
            )),
            Self::ProviderRecovery => Some(RuntimeRoadmapRolloutGate::new(
                PROVIDER_RECOVERY_ROLLOUT_ENV,
                PROVIDER_RECOVERY_ROLLOUT_CONFIG_PATH,
            )),
            Self::TerminalSessions => Some(RuntimeRoadmapRolloutGate::new(
                TERMINAL_SESSIONS_ROLLOUT_ENV,
                TERMINAL_SESSIONS_ROLLOUT_CONFIG_PATH,
            )),
            Self::BrowserRescue => Some(RuntimeRoadmapRolloutGate::new(
                BROWSER_RESCUE_ROLLOUT_ENV,
                BROWSER_RESCUE_ROLLOUT_CONFIG_PATH,
            )),
            Self::LspService => Some(RuntimeRoadmapRolloutGate::new(
                LSP_SERVICE_ROLLOUT_ENV,
                LSP_SERVICE_ROLLOUT_CONFIG_PATH,
            )),
            Self::AdvisorFanout => Some(RuntimeRoadmapRolloutGate::new(
                ADVISOR_FANOUT_ROLLOUT_ENV,
                ADVISOR_FANOUT_ROLLOUT_CONFIG_PATH,
            )),
            Self::AcpRuntime => Some(RuntimeRoadmapRolloutGate::new(
                ACP_RUNTIME_ROLLOUT_ENV,
                ACP_RUNTIME_ROLLOUT_CONFIG_PATH,
            )),
            Self::ChannelTurnKernel => Some(RuntimeRoadmapRolloutGate::new(
                CHANNEL_TURN_KERNEL_ROLLOUT_ENV,
                CHANNEL_TURN_KERNEL_ROLLOUT_CONFIG_PATH,
            )),
            Self::AgentPlanState => Some(RuntimeRoadmapRolloutGate::new(
                AGENT_PLAN_STATE_ROLLOUT_ENV,
                AGENT_PLAN_STATE_ROLLOUT_CONFIG_PATH,
            )),
            Self::ObjectiveJudge => Some(RuntimeRoadmapRolloutGate::new(
                OBJECTIVE_JUDGE_ROLLOUT_ENV,
                OBJECTIVE_JUDGE_ROLLOUT_CONFIG_PATH,
            )),
            Self::VerificationRuntime => Some(RuntimeRoadmapRolloutGate::new(
                VERIFICATION_RUNTIME_ROLLOUT_ENV,
                VERIFICATION_RUNTIME_ROLLOUT_CONFIG_PATH,
            )),
            Self::ProgressDrafts => Some(RuntimeRoadmapRolloutGate::new(
                PROGRESS_DRAFTS_ROLLOUT_ENV,
                PROGRESS_DRAFTS_ROLLOUT_CONFIG_PATH,
            )),
            Self::CompactionSafeguard => Some(RuntimeRoadmapRolloutGate::new(
                COMPACTION_SAFEGUARD_ROLLOUT_ENV,
                COMPACTION_SAFEGUARD_ROLLOUT_CONFIG_PATH,
            )),
            Self::AttackSurfaceAudit => Some(RuntimeRoadmapRolloutGate::new(
                ATTACK_SURFACE_AUDIT_ROLLOUT_ENV,
                ATTACK_SURFACE_AUDIT_ROLLOUT_CONFIG_PATH,
            )),
        }
    }
}

runtime_roadmap_enum! {
    /// Canonical event type persisted in runtime-roadmap journal payloads.
    pub enum RuntimeRoadmapEventType {
        ContractRegistered => "contract_registered",
        ContractCompleted => "contract_completed",
        ContractFailed => "contract_failed",
        RolloutGateRegistered => "rollout_gate_registered",
        HarnessStarted => "harness_started",
        HarnessCompleted => "harness_completed",
        HarnessFailed => "harness_failed"
    }
}

impl RuntimeRoadmapEventType {
    /// Dotted journal event name used when appending this payload to the audit log.
    #[must_use]
    pub const fn journal_event(self) -> &'static str {
        match self {
            Self::ContractRegistered => "runtime_roadmap.contract.registered",
            Self::ContractCompleted => "runtime_roadmap.contract.completed",
            Self::ContractFailed => "runtime_roadmap.contract.failed",
            Self::RolloutGateRegistered => "runtime_roadmap.rollout_gate.registered",
            Self::HarnessStarted => "runtime_roadmap.harness.started",
            Self::HarnessCompleted => "runtime_roadmap.harness.completed",
            Self::HarnessFailed => "runtime_roadmap.harness.failed",
        }
    }
}

runtime_roadmap_enum! {
    /// Decision captured by a roadmap journal payload.
    pub enum RuntimeRoadmapDecision {
        AdoptExistingSurface => "adopt_existing_surface",
        DefineContract => "define_contract",
        RequireFeatureRollout => "require_feature_rollout",
        DeferRuntimeBehavior => "defer_runtime_behavior",
        RejectInvalidContract => "reject_invalid_contract"
    }
}

impl RuntimeRoadmapDecision {
    /// Returns `true` when the decision must name a rollout env var and config path.
    #[must_use]
    pub const fn requires_rollout_gate(self) -> bool {
        matches!(self, Self::RequireFeatureRollout)
    }
}

runtime_roadmap_enum! {
    /// Stable reason codes for roadmap baseline and runtime-loop guardrails.
    pub enum RuntimeRoadmapReasonCode {
        ContractsDefined => "runtime_roadmap.baseline.contracts_defined",
        ReasonTaxonomyCatalogued => "runtime_roadmap.reason_taxonomy.catalogued",
        JournalAppendLogReused => "runtime_roadmap.journal.existing_append_log",
        RolloutDefaultOff => "runtime_roadmap.rollout.default_off",
        RuntimeBehaviorDeferred => "runtime_roadmap.behavior.deferred",
        ReplayRegressionFixtureAccepted => "runtime_roadmap.fixture.replay_regression_accepted",
        ImplementationStyleDocumented => "runtime_roadmap.implementation_style.documented",
        IntegrationSmokeTargetVerified => "runtime_roadmap.integration_smoke.target_verified",
        FixtureValidationFailed => "runtime_roadmap.fixture.validation_failed",
        GoldenTrajectoryAccepted => "runtime_roadmap.trajectory.golden_fixture_accepted",
        SecurityInvariantAccepted => "runtime_roadmap.security.invariant_fixture_accepted",
        BoundaryTaxonomyCatalogued => "runtime_roadmap.boundary_taxonomy.catalogued",
        BoundaryMetadataRedacted => "runtime_roadmap.boundary_taxonomy.metadata_redacted",
        InvalidContractRejected => "runtime_roadmap.contract.invalid"
    }
}

runtime_roadmap_enum! {
    /// Redaction boundary promised by a roadmap journal payload.
    pub enum RuntimeRoadmapRedactionBoundary {
        MetadataOnly => "metadata_only",
        SanitizedAuditPayload => "sanitized_audit_payload",
        ModelVisibleSummary => "model_visible_summary",
        RawPayloadForbidden => "raw_payload_forbidden"
    }
}

/// Static rollout gate metadata for one capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeRoadmapRolloutGate {
    pub env_var: &'static str,
    pub config_path: &'static str,
    pub default_enabled: bool,
}

impl RuntimeRoadmapRolloutGate {
    /// Creates a default-off rollout gate descriptor.
    #[must_use]
    pub const fn new(env_var: &'static str, config_path: &'static str) -> Self {
        Self { env_var, config_path, default_enabled: false }
    }
}

/// Serializable capability catalog entry for diagnostics and release fixtures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRoadmapCapabilityDescriptor {
    pub schema_version: u32,
    pub capability: RuntimeRoadmapCapability,
    pub label: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_env_var: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_config_path: Option<String>,
    pub default_enabled: bool,
}

impl RuntimeRoadmapCapabilityDescriptor {
    /// Builds a descriptor from the canonical capability metadata.
    #[must_use]
    pub fn from_capability(capability: RuntimeRoadmapCapability) -> Self {
        let rollout_gate = capability.rollout_gate();
        Self {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            capability,
            label: capability.label().to_owned(),
            summary: capability.summary().to_owned(),
            rollout_env_var: rollout_gate.map(|gate| gate.env_var.to_owned()),
            rollout_config_path: rollout_gate.map(|gate| gate.config_path.to_owned()),
            default_enabled: rollout_gate.is_some_and(|gate| gate.default_enabled),
        }
    }
}

/// Builds the runtime-roadmap capability catalog in canonical order.
#[must_use]
pub fn runtime_roadmap_capability_catalog() -> Vec<RuntimeRoadmapCapabilityDescriptor> {
    ALL_RUNTIME_ROADMAP_CAPABILITIES
        .into_iter()
        .map(RuntimeRoadmapCapabilityDescriptor::from_capability)
        .collect()
}

runtime_roadmap_enum! {
    /// Runtime boundary families that must share stable reason-code and diagnostics names.
    pub enum RuntimeBoundaryFamily {
        Harness => "harness",
        Hook => "hook",
        Middleware => "middleware",
        ProviderStream => "provider_stream",
        TurnRecovery => "turn_recovery",
        Terminal => "terminal",
        BrowserRescue => "browser_rescue",
        Lsp => "lsp",
        Acp => "acp",
        Learning => "learning"
    }
}

runtime_roadmap_enum! {
    /// Operator-facing severity attached to a boundary event family.
    pub enum RuntimeBoundarySeverity {
        Info => "info",
        Warning => "warning",
        Error => "error"
    }
}

runtime_roadmap_enum! {
    /// Whether a runtime boundary event can be retried automatically.
    pub enum RuntimeBoundaryRetryability {
        Retryable => "retryable",
        NonRetryable => "non_retryable",
        OperatorActionRequired => "operator_action_required"
    }
}

runtime_roadmap_enum! {
    /// Where a boundary event may be surfaced after redaction.
    pub enum RuntimeBoundaryVisibilityPolicy {
        InternalDiagnostics => "internal_diagnostics",
        OperatorDiagnostics => "operator_diagnostics",
        AuditAndReplay => "audit_and_replay",
        ModelVisibleSummary => "model_visible_summary"
    }
}

runtime_roadmap_enum! {
    /// Host-facing integration interfaces that must not own critical runtime authority.
    pub enum RuntimeHostAuthorityInterface {
        Harness => "harness",
        Hooks => "hooks",
        Mcp => "mcp",
        CodexAdapter => "codex_adapter",
        Terminal => "terminal",
        RemoteWorker => "remote_worker",
        AdvisorFanout => "advisor_fanout"
    }
}

runtime_roadmap_enum! {
    /// Backend runtime fixture areas used by synthetic replay and smoke tests.
    pub enum BackendRuntimeFixtureArea {
        RunLoop => "run_loop",
        ProviderStream => "provider_stream",
        ToolCall => "tool_call",
        FilePatch => "file_patch",
        Lsp => "lsp",
        Compaction => "compaction"
    }
}

/// Stable diagnostics/audit descriptor for one runtime boundary event family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBoundaryEventDescriptor {
    pub schema_version: u32,
    pub family: RuntimeBoundaryFamily,
    pub event_name: String,
    pub severity: RuntimeBoundarySeverity,
    pub retryability: RuntimeBoundaryRetryability,
    pub visibility_policy: RuntimeBoundaryVisibilityPolicy,
    pub redaction_boundary: RuntimeRoadmapRedactionBoundary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_capability: Option<RuntimeRoadmapCapability>,
    pub metadata_keys: Vec<String>,
}

const RUNTIME_BOUNDARY_METADATA_LIMIT_BYTES: usize = 2_048;

fn boundary_event_descriptor(
    family: RuntimeBoundaryFamily,
    event_name: &str,
    severity: RuntimeBoundarySeverity,
    retryability: RuntimeBoundaryRetryability,
    visibility_policy: RuntimeBoundaryVisibilityPolicy,
    rollout_capability: Option<RuntimeRoadmapCapability>,
    metadata_keys: &[&str],
) -> RuntimeBoundaryEventDescriptor {
    RuntimeBoundaryEventDescriptor {
        schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
        family,
        event_name: event_name.to_owned(),
        severity,
        retryability,
        visibility_policy,
        redaction_boundary: RuntimeRoadmapRedactionBoundary::MetadataOnly,
        rollout_capability,
        metadata_keys: metadata_keys.iter().map(|value| (*value).to_owned()).collect(),
    }
}

/// Builds the canonical boundary event taxonomy for upcoming runtime milestones.
#[must_use]
pub fn runtime_boundary_event_taxonomy() -> Vec<RuntimeBoundaryEventDescriptor> {
    vec![
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Harness,
            "harness.selection.decision",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::NonRetryable,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::AgentHarnessRuntime),
            &["harness_id", "selection_mode", "support_outcome"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Hook,
            "hook.lifecycle.observed",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::OperatorActionRequired,
            RuntimeBoundaryVisibilityPolicy::OperatorDiagnostics,
            Some(RuntimeRoadmapCapability::InlineRuntimeHooks),
            &["hook_id", "phase", "decision_kind"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Middleware,
            "tool.middleware.projected",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::NonRetryable,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::ToolResultMiddleware),
            &["tool_name", "middleware_id", "visibility"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Middleware,
            "tool.middleware.failed",
            RuntimeBoundarySeverity::Warning,
            RuntimeBoundaryRetryability::NonRetryable,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::ToolResultMiddleware),
            &["tool_name", "middleware_id", "failure_code"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::ProviderStream,
            "provider.stream.normalized",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::Retryable,
            RuntimeBoundaryVisibilityPolicy::InternalDiagnostics,
            Some(RuntimeRoadmapCapability::ProviderStreamNormalizer),
            &["provider_kind", "chunk_count", "repair_count"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::TurnRecovery,
            "turn.recovery.retry_planned",
            RuntimeBoundarySeverity::Warning,
            RuntimeBoundaryRetryability::Retryable,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::ProviderRecovery),
            &["provider_kind", "recovery_recipe", "attempt_index"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Terminal,
            "terminal.session.lifecycle",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::OperatorActionRequired,
            RuntimeBoundaryVisibilityPolicy::OperatorDiagnostics,
            Some(RuntimeRoadmapCapability::TerminalSessions),
            &["session_handle", "state", "cleanup_evidence"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::BrowserRescue,
            "browser.rescue.requested",
            RuntimeBoundarySeverity::Warning,
            RuntimeBoundaryRetryability::OperatorActionRequired,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::BrowserRescue),
            &["profile_id", "rescue_kind", "policy_decision"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Lsp,
            "lsp.lifecycle.changed",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::Retryable,
            RuntimeBoundaryVisibilityPolicy::OperatorDiagnostics,
            Some(RuntimeRoadmapCapability::LspService),
            &["workspace_root", "language_id", "state"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Acp,
            "acp.runtime.actor_queued",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::Retryable,
            RuntimeBoundaryVisibilityPolicy::AuditAndReplay,
            Some(RuntimeRoadmapCapability::AcpRuntime),
            &["session_id", "actor_id", "permission_state"],
        ),
        boundary_event_descriptor(
            RuntimeBoundaryFamily::Learning,
            "learning.candidate.reviewed",
            RuntimeBoundarySeverity::Info,
            RuntimeBoundaryRetryability::NonRetryable,
            RuntimeBoundaryVisibilityPolicy::OperatorDiagnostics,
            None,
            &["candidate_kind", "scope", "decision"],
        ),
    ]
}

/// Redacts and bounds free-form boundary metadata before diagnostics serialization.
#[must_use]
pub fn sanitize_runtime_boundary_metadata(mut metadata: Value) -> Value {
    redact_runtime_boundary_value(&mut metadata, None);
    match serde_json::to_vec(&metadata) {
        Ok(encoded) if encoded.len() <= RUNTIME_BOUNDARY_METADATA_LIMIT_BYTES => metadata,
        Ok(encoded) => serde_json::json!({
            "schema_version": RUNTIME_ROADMAP_SCHEMA_VERSION,
            "truncated": true,
            "original_bytes": encoded.len(),
            "limit_bytes": RUNTIME_BOUNDARY_METADATA_LIMIT_BYTES,
            "redaction_boundary": RuntimeRoadmapRedactionBoundary::MetadataOnly,
            "reason_code": RuntimeRoadmapReasonCode::BoundaryMetadataRedacted,
        }),
        Err(_) => serde_json::json!({
            "schema_version": RUNTIME_ROADMAP_SCHEMA_VERSION,
            "truncated": true,
            "reason_code": RuntimeRoadmapReasonCode::BoundaryMetadataRedacted,
        }),
    }
}

fn redact_runtime_boundary_value(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                redact_runtime_boundary_value(child, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for child in items {
                redact_runtime_boundary_value(child, key_context);
            }
        }
        Value::String(raw) => {
            *raw = sanitize_runtime_boundary_string(raw.as_str(), key_context);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn sanitize_runtime_boundary_string(raw: &str, key_context: Option<&str>) -> String {
    if key_context.is_some_and(is_sensitive_key) {
        return crate::redaction::REDACTED.to_owned();
    }
    let redacted = redact_diagnostic_text(raw);
    if redacted.contains("vault://") || redacted.contains("vault:") {
        "<vault_ref:redacted>".to_owned()
    } else {
        redacted
    }
}

const HOST_OWNED_RUNTIME_AUTHORITIES: &[&str] =
    &["credentials", "approvals", "transcript", "sandbox", "journal", "tool_execution"];

/// Checklist entry for one interface that delegates runtime work to the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeHostAuthorityChecklistEntry {
    pub schema_version: u32,
    pub interface: RuntimeHostAuthorityInterface,
    pub integration_boundary: String,
    pub denied_authorities: Vec<String>,
    pub host_owned_authorities: Vec<String>,
    pub audit_event_samples: Vec<String>,
}

impl RuntimeHostAuthorityChecklistEntry {
    /// Validates that the interface does not claim host-owned runtime authority.
    ///
    /// # Errors
    /// Returns [`RuntimeSecurityInvariantError`] when a critical authority is
    /// not explicitly denied and retained by the host, or when an audit event
    /// sample uses an invalid dotted event name.
    pub fn validate(&self) -> Result<(), RuntimeSecurityInvariantError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeSecurityInvariantError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }
        for authority in HOST_OWNED_RUNTIME_AUTHORITIES {
            if !self.denied_authorities.iter().any(|value| value == authority)
                || !self.host_owned_authorities.iter().any(|value| value == authority)
            {
                return Err(RuntimeSecurityInvariantError::HostAuthorityGranted {
                    interface: self.interface.as_str().to_owned(),
                    authority: (*authority).to_owned(),
                });
            }
        }
        for event_name in &self.audit_event_samples {
            validate_event_name(event_name).map_err(|_| {
                RuntimeSecurityInvariantError::InvalidAuditEventName {
                    interface: self.interface.as_str().to_owned(),
                    event_name: event_name.clone(),
                }
            })?;
        }
        Ok(())
    }
}

/// Returns the host-owned authority checklist for external runtime interfaces.
#[must_use]
pub fn runtime_host_authority_checklist() -> Vec<RuntimeHostAuthorityChecklistEntry> {
    [
        (
            RuntimeHostAuthorityInterface::Harness,
            "harness callbacks report decisions; host owns run state, transcript, gate, and journal writes",
            &["harness.selection.decision", "harness.lifecycle.completed"][..],
        ),
        (
            RuntimeHostAuthorityInterface::Hooks,
            "hooks observe or request bounded changes; host owns policy, approvals, and side effects",
            &["hook.lifecycle.observed", "hook.policy.denied"][..],
        ),
        (
            RuntimeHostAuthorityInterface::Mcp,
            "MCP tool servers expose descriptors; host owns credential binding, approval, sandbox, and tape",
            &["mcp.tool.requested", "mcp.tool.denied"][..],
        ),
        (
            RuntimeHostAuthorityInterface::CodexAdapter,
            "Codex adapter bridges protocol messages; host owns transcript persistence and execution gates",
            &["codex.adapter.requested", "codex.adapter.rejected"][..],
        ),
        (
            RuntimeHostAuthorityInterface::Terminal,
            "terminal sessions expose bounded process handles; host owns cwd/env validation, sandbox, and cleanup",
            &["terminal.session.lifecycle", "terminal.command.denied"][..],
        ),
        (
            RuntimeHostAuthorityInterface::RemoteWorker,
            "remote workers execute leased work only; host owns policy, attestation, credentials, and journal commits",
            &["worker.lease.issued", "worker.execution.denied"][..],
        ),
        (
            RuntimeHostAuthorityInterface::AdvisorFanout,
            "advisors return non-authoritative reviews; host owns approvals, final transcript, and mutation execution",
            &["advisor.fanout.requested", "advisor.finding.recorded"][..],
        ),
    ]
    .into_iter()
    .map(|(interface, integration_boundary, audit_event_samples)| {
        RuntimeHostAuthorityChecklistEntry {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            interface,
            integration_boundary: integration_boundary.to_owned(),
            denied_authorities: HOST_OWNED_RUNTIME_AUTHORITIES
                .iter()
                .map(|authority| (*authority).to_owned())
                .collect(),
            host_owned_authorities: HOST_OWNED_RUNTIME_AUTHORITIES
                .iter()
                .map(|authority| (*authority).to_owned())
                .collect(),
            audit_event_samples: audit_event_samples
                .iter()
                .map(|event_name| (*event_name).to_owned())
                .collect(),
        }
    })
    .collect()
}

/// Asserts that every host-authority interface preserves the critical host fences.
///
/// # Errors
/// Returns [`RuntimeSecurityInvariantError`] when any checklist entry is invalid.
pub fn assert_host_authority_checklist_denies_direct_runtime_authority(
    entries: &[RuntimeHostAuthorityChecklistEntry],
) -> Result<(), RuntimeSecurityInvariantError> {
    for entry in entries {
        entry.validate()?;
    }
    Ok(())
}

/// Metadata for one backend runtime smoke fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendRuntimeFixture {
    pub schema_version: u32,
    pub fixture_id: String,
    pub area: BackendRuntimeFixtureArea,
    pub source_path: String,
    pub risk_classification: String,
    pub expected_runtime_path: String,
    pub expected_terminal_state: String,
    pub expected_journal_events: Vec<String>,
    pub redaction_boundary: RuntimeRoadmapRedactionBoundary,
    pub evidence_refs: Vec<String>,
}

impl BackendRuntimeFixture {
    /// Validates a backend runtime fixture taxonomy entry.
    ///
    /// # Errors
    /// Returns [`RuntimeRoadmapHarnessValidationError`] when schema, slug,
    /// repo path, event name, runtime path, or evidence metadata is invalid.
    pub fn validate(&self) -> Result<(), RuntimeRoadmapHarnessValidationError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeRoadmapHarnessValidationError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }
        validate_slug("backend runtime fixture id", self.fixture_id.as_str())?;
        validate_repo_relative_path(
            "backend runtime fixture source path",
            self.source_path.as_str(),
        )?;
        if self.expected_runtime_path.trim().is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingRequiredSymbol {
                target_id: self.fixture_id.clone(),
            });
        }
        if self.expected_journal_events.is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingTrajectoryExpectedEvent {
                fixture_id: self.fixture_id.clone(),
            });
        }
        for event_name in &self.expected_journal_events {
            validate_event_name(event_name)?;
        }
        if self.evidence_refs.is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingEvidenceRef {
                fixture_id: self.fixture_id.clone(),
            });
        }
        for evidence_ref in &self.evidence_refs {
            validate_repo_relative_path("backend runtime fixture evidence ref", evidence_ref)?;
        }
        Ok(())
    }
}

struct BackendRuntimeFixtureInput<'a> {
    fixture_id: &'a str,
    area: BackendRuntimeFixtureArea,
    source_path: &'a str,
    risk_classification: &'a str,
    expected_runtime_path: &'a str,
    expected_terminal_state: &'a str,
    expected_journal_events: &'a [&'a str],
    evidence_refs: &'a [&'a str],
}

fn backend_runtime_fixture(input: BackendRuntimeFixtureInput<'_>) -> BackendRuntimeFixture {
    BackendRuntimeFixture {
        schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
        fixture_id: input.fixture_id.to_owned(),
        area: input.area,
        source_path: input.source_path.to_owned(),
        risk_classification: input.risk_classification.to_owned(),
        expected_runtime_path: input.expected_runtime_path.to_owned(),
        expected_terminal_state: input.expected_terminal_state.to_owned(),
        expected_journal_events: input
            .expected_journal_events
            .iter()
            .map(|event_name| (*event_name).to_owned())
            .collect(),
        redaction_boundary: RuntimeRoadmapRedactionBoundary::MetadataOnly,
        evidence_refs: input.evidence_refs.iter().map(|path| (*path).to_owned()).collect(),
    }
}

/// Returns the canonical backend runtime fixture taxonomy.
#[must_use]
pub fn backend_runtime_fixture_taxonomy() -> Vec<BackendRuntimeFixture> {
    vec![
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "run_loop_terminal_summary",
            area: BackendRuntimeFixtureArea::RunLoop,
            source_path: "fixtures/golden/runtime_roadmap_phase1_trajectories.json",
            risk_classification: "p0_host_owned_runtime",
            expected_runtime_path: "run_runtime_path_summary",
            expected_terminal_state: "done",
            expected_journal_events: &["run.runtime_path_summary", "harness.selection.decision"],
            evidence_refs: &["fixtures/golden/runtime_roadmap_phase1_trajectories.json"],
        }),
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "provider_stream_malformed_chunk",
            area: BackendRuntimeFixtureArea::ProviderStream,
            source_path: "qa/scenarios/provider/malformed_sse_chunk.yaml",
            risk_classification: "p0_provider_recovery",
            expected_runtime_path: "provider_stream_normalizer",
            expected_terminal_state: "failed",
            expected_journal_events: &["provider.stream.normalized", "turn.recovery.retry_planned"],
            evidence_refs: &["fixtures/provider_compat/p0_provider_compat_pack.yaml"],
        }),
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "tool_call_approval_followthrough",
            area: BackendRuntimeFixtureArea::ToolCall,
            source_path: "qa/scenarios/approval_turn_tool_followthrough.yaml",
            risk_classification: "p0_mutating_tool_gate",
            expected_runtime_path: "tool_gate",
            expected_terminal_state: "done",
            expected_journal_events: &["tool.gate.decision", "tool.call.completed"],
            evidence_refs: &["qa/scenarios/tool_result_redaction.yaml"],
        }),
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "file_patch_premature_final",
            area: BackendRuntimeFixtureArea::FilePatch,
            source_path: "qa/scenarios/provider/premature_final_after_patch.yaml",
            risk_classification: "p0_file_mutation_recovery",
            expected_runtime_path: "file_patch_verification",
            expected_terminal_state: "failed",
            expected_journal_events: &["file.patch.intent", "verification.evidence.recorded"],
            evidence_refs: &["qa/scenarios/process_background_verification.yaml"],
        }),
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "lsp_rust_workspace_scope",
            area: BackendRuntimeFixtureArea::Lsp,
            source_path: "fixtures/code-intel/rust/src/lib.rs",
            risk_classification: "p1_workspace_scoped_code_intel",
            expected_runtime_path: "lsp_service",
            expected_terminal_state: "done",
            expected_journal_events: &["lsp.lifecycle.changed", "lsp.diagnostics.delta"],
            evidence_refs: &["fixtures/code-intel/rust/src/lib.rs"],
        }),
        backend_runtime_fixture(BackendRuntimeFixtureInput {
            fixture_id: "compaction_retry_mutating_tool",
            area: BackendRuntimeFixtureArea::Compaction,
            source_path: "qa/scenarios/compaction_retry_mutating_tool.yaml",
            risk_classification: "p0_replay_safe_compaction",
            expected_runtime_path: "compaction_safeguard",
            expected_terminal_state: "done",
            expected_journal_events: &["compaction.safeguard.recorded", "tool.gate.decision"],
            evidence_refs: &["qa/scenarios/compaction_retry_mutating_tool.yaml"],
        }),
    ]
}

/// Projection for the backend runtime fixture taxonomy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendRuntimeFixtureTaxonomyProjection {
    pub schema_version: u32,
    pub fixtures_total: usize,
    pub areas: Vec<BackendRuntimeFixtureArea>,
    pub expected_runtime_paths: Vec<String>,
    pub expected_journal_events: Vec<String>,
}

/// Projects backend runtime fixtures into a compact taxonomy summary.
///
/// # Errors
/// Returns [`RuntimeRoadmapHarnessValidationError`] when any fixture is invalid.
pub fn project_backend_runtime_fixture_taxonomy(
    fixtures: &[BackendRuntimeFixture],
) -> Result<BackendRuntimeFixtureTaxonomyProjection, RuntimeRoadmapHarnessValidationError> {
    let mut areas = BTreeSet::new();
    let mut expected_runtime_paths = BTreeSet::new();
    let mut expected_journal_events = BTreeSet::new();

    for fixture in fixtures {
        fixture.validate()?;
        areas.insert(fixture.area);
        expected_runtime_paths.insert(fixture.expected_runtime_path.clone());
        for event_name in &fixture.expected_journal_events {
            expected_journal_events.insert(event_name.clone());
        }
    }

    Ok(BackendRuntimeFixtureTaxonomyProjection {
        schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
        fixtures_total: fixtures.len(),
        areas: areas.into_iter().collect(),
        expected_runtime_paths: expected_runtime_paths.into_iter().collect(),
        expected_journal_events: expected_journal_events.into_iter().collect(),
    })
}

runtime_roadmap_enum! {
    /// Visibility partition for expected trajectory events.
    pub enum RuntimeTrajectoryEventVisibility {
        UserVisible => "user_visible",
        ModelVisible => "model_visible",
        InternalAudit => "internal_audit"
    }
}

/// One provider stream item consumed by a golden trajectory fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTrajectoryProviderChunk {
    pub seq: u32,
    pub kind: String,
    pub body: String,
    pub malformed: bool,
}

/// One tool call declared by a golden trajectory fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTrajectoryToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub read_only: bool,
    pub requires_gate: bool,
}

/// One tool result projection declared by a golden trajectory fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTrajectoryToolResult {
    pub call_id: String,
    pub visibility: RuntimeTrajectoryEventVisibility,
    pub body: String,
}

/// One canonical event expected from a trajectory fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTrajectoryExpectedEvent {
    pub event_name: String,
    pub visibility: RuntimeTrajectoryEventVisibility,
    pub reason_code: RuntimeRoadmapReasonCode,
}

/// Golden provider/tool trajectory fixture contract for agent-loop smoke tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeGoldenTrajectoryFixture {
    pub schema_version: u32,
    pub fixture_id: String,
    pub enabled_capabilities: Vec<RuntimeRoadmapCapability>,
    pub provider_chunks: Vec<RuntimeTrajectoryProviderChunk>,
    pub assistant_turns: Vec<String>,
    pub tool_calls: Vec<RuntimeTrajectoryToolCall>,
    pub tool_results: Vec<RuntimeTrajectoryToolResult>,
    pub user_steering_events: Vec<String>,
    pub expected_events: Vec<RuntimeTrajectoryExpectedEvent>,
}

impl RuntimeGoldenTrajectoryFixture {
    /// Validates trajectory fixture invariants used by smoke and replay tests.
    ///
    /// # Errors
    /// Returns [`RuntimeRoadmapHarnessValidationError`] when the fixture uses an
    /// unsupported schema, unsafe id, or empty provider/event sequence.
    pub fn validate(&self) -> Result<(), RuntimeRoadmapHarnessValidationError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeRoadmapHarnessValidationError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }
        validate_slug("trajectory fixture id", self.fixture_id.as_str())?;
        if self.provider_chunks.is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingTrajectoryProviderChunk {
                fixture_id: self.fixture_id.clone(),
            });
        }
        if self.expected_events.is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingTrajectoryExpectedEvent {
                fixture_id: self.fixture_id.clone(),
            });
        }
        for event in &self.expected_events {
            validate_event_name(event.event_name.as_str())?;
        }
        Ok(())
    }
}

/// Compact diagnostics projection for the golden trajectory catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeGoldenTrajectoryProjection {
    pub schema_version: u32,
    pub fixtures_total: usize,
    pub provider_chunks_total: usize,
    pub tool_calls_total: usize,
    pub malformed_provider_fixtures: usize,
    pub event_names: Vec<String>,
    pub enabled_capabilities: Vec<RuntimeRoadmapCapability>,
}

/// Returns the canonical Phase 1 golden trajectory fixture catalog.
#[must_use]
pub fn runtime_roadmap_phase1_trajectory_fixtures() -> Vec<RuntimeGoldenTrajectoryFixture> {
    vec![
        RuntimeGoldenTrajectoryFixture {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            fixture_id: "phase1_smoke_no_tool".to_owned(),
            enabled_capabilities: vec![RuntimeRoadmapCapability::AgentHarnessRuntime],
            provider_chunks: vec![RuntimeTrajectoryProviderChunk {
                seq: 1,
                kind: "assistant_message".to_owned(),
                body: "ready".to_owned(),
                malformed: false,
            }],
            assistant_turns: vec!["ready".to_owned()],
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            user_steering_events: Vec::new(),
            expected_events: vec![RuntimeTrajectoryExpectedEvent {
                event_name: "harness.selection.decision".to_owned(),
                visibility: RuntimeTrajectoryEventVisibility::InternalAudit,
                reason_code: RuntimeRoadmapReasonCode::GoldenTrajectoryAccepted,
            }],
        },
        RuntimeGoldenTrajectoryFixture {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            fixture_id: "phase1_read_only_tool_call".to_owned(),
            enabled_capabilities: vec![
                RuntimeRoadmapCapability::AgentHarnessRuntime,
                RuntimeRoadmapCapability::ToolResultMiddleware,
            ],
            provider_chunks: vec![
                RuntimeTrajectoryProviderChunk {
                    seq: 1,
                    kind: "tool_call".to_owned(),
                    body: "palyra.fs.read_file".to_owned(),
                    malformed: false,
                },
                RuntimeTrajectoryProviderChunk {
                    seq: 2,
                    kind: "assistant_message".to_owned(),
                    body: "file summarized".to_owned(),
                    malformed: false,
                },
            ],
            assistant_turns: vec!["file summarized".to_owned()],
            tool_calls: vec![RuntimeTrajectoryToolCall {
                call_id: "call_read_1".to_owned(),
                tool_name: "palyra.fs.read_file".to_owned(),
                read_only: true,
                requires_gate: true,
            }],
            tool_results: vec![RuntimeTrajectoryToolResult {
                call_id: "call_read_1".to_owned(),
                visibility: RuntimeTrajectoryEventVisibility::ModelVisible,
                body: "redacted file summary".to_owned(),
            }],
            user_steering_events: Vec::new(),
            expected_events: vec![
                RuntimeTrajectoryExpectedEvent {
                    event_name: "harness.selection.decision".to_owned(),
                    visibility: RuntimeTrajectoryEventVisibility::InternalAudit,
                    reason_code: RuntimeRoadmapReasonCode::GoldenTrajectoryAccepted,
                },
                RuntimeTrajectoryExpectedEvent {
                    event_name: "tool.middleware.projected".to_owned(),
                    visibility: RuntimeTrajectoryEventVisibility::InternalAudit,
                    reason_code: RuntimeRoadmapReasonCode::GoldenTrajectoryAccepted,
                },
            ],
        },
        RuntimeGoldenTrajectoryFixture {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            fixture_id: "phase1_malformed_provider_output".to_owned(),
            enabled_capabilities: vec![
                RuntimeRoadmapCapability::ProviderStreamNormalizer,
                RuntimeRoadmapCapability::ProviderRecovery,
            ],
            provider_chunks: vec![RuntimeTrajectoryProviderChunk {
                seq: 1,
                kind: "malformed_tool_call".to_owned(),
                body: "{\"arguments\":".to_owned(),
                malformed: true,
            }],
            assistant_turns: Vec::new(),
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            user_steering_events: vec!["operator_requested_retry".to_owned()],
            expected_events: vec![
                RuntimeTrajectoryExpectedEvent {
                    event_name: "provider.stream.normalized".to_owned(),
                    visibility: RuntimeTrajectoryEventVisibility::InternalAudit,
                    reason_code: RuntimeRoadmapReasonCode::GoldenTrajectoryAccepted,
                },
                RuntimeTrajectoryExpectedEvent {
                    event_name: "turn.recovery.retry_planned".to_owned(),
                    visibility: RuntimeTrajectoryEventVisibility::InternalAudit,
                    reason_code: RuntimeRoadmapReasonCode::GoldenTrajectoryAccepted,
                },
            ],
        },
    ]
}

/// Projects golden trajectory fixtures into a diagnostics/read-model summary.
///
/// # Errors
/// Returns [`RuntimeRoadmapHarnessValidationError`] when any built-in fixture drifts.
pub fn runtime_roadmap_phase1_trajectory_projection(
) -> Result<RuntimeGoldenTrajectoryProjection, RuntimeRoadmapHarnessValidationError> {
    let fixtures = runtime_roadmap_phase1_trajectory_fixtures();
    let mut event_names = BTreeSet::new();
    let mut enabled_capabilities = BTreeSet::new();
    let mut provider_chunks_total = 0;
    let mut tool_calls_total = 0;
    let mut malformed_provider_fixtures = 0;

    for fixture in &fixtures {
        fixture.validate()?;
        provider_chunks_total += fixture.provider_chunks.len();
        tool_calls_total += fixture.tool_calls.len();
        if fixture.provider_chunks.iter().any(|chunk| chunk.malformed) {
            malformed_provider_fixtures += 1;
        }
        for capability in &fixture.enabled_capabilities {
            enabled_capabilities.insert(*capability);
        }
        for event in &fixture.expected_events {
            event_names.insert(event.event_name.clone());
        }
    }

    Ok(RuntimeGoldenTrajectoryProjection {
        schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
        fixtures_total: fixtures.len(),
        provider_chunks_total,
        tool_calls_total,
        malformed_provider_fixtures,
        event_names: event_names.into_iter().collect(),
        enabled_capabilities: enabled_capabilities.into_iter().collect(),
    })
}

/// Fixture describing the tool-gate posture required for one tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeToolGateFixture {
    pub tool_name: String,
    pub mutating: bool,
    pub gate_required: bool,
    pub reason_code: String,
}

/// Fixture describing one harness authority request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHarnessAuthorityFixture {
    pub authority: String,
    pub allowed: bool,
}

/// Fixture describing the terminal outcome of a denied mutating operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMutationApprovalFixture {
    pub tool_name: String,
    pub mutating: bool,
    pub approval_denied: bool,
    pub terminal_state: String,
    pub reason_code: String,
}

/// Validation failure for security invariant helper fixtures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeSecurityInvariantError {
    #[error(
        "runtime security invariant schema version {actual} is not supported; expected {expected}"
    )]
    UnsupportedSchemaVersion { actual: u32, expected: u32 },
    #[error("raw secret leaked into {surface}")]
    RawSecretLeaked { surface: String },
    #[error("{tool_name} must require the execution gate")]
    ToolGateMissing { tool_name: String },
    #[error("direct journal write authority must stay denied: {authority}")]
    DirectJournalWriteAuthority { authority: String },
    #[error("{interface} must not own host authority {authority}")]
    HostAuthorityGranted { interface: String, authority: String },
    #[error("{interface} audit event name must be dotted lowercase ASCII: {event_name}")]
    InvalidAuditEventName { interface: String, event_name: String },
    #[error("{tool_name} approval denial must be terminal for mutation")]
    ApprovalDenialNotTerminal { tool_name: String },
    #[error("{field} must be non-empty")]
    EmptyField { field: &'static str },
}

/// Asserts that a fake secret is absent from every serialized model/audit surface.
///
/// # Errors
/// Returns [`RuntimeSecurityInvariantError::RawSecretLeaked`] naming the first
/// surface that still contains the raw secret.
pub fn assert_no_raw_secret_in_tape(
    raw_secret: &str,
    serialized_surfaces: &[(&str, &str)],
) -> Result<(), RuntimeSecurityInvariantError> {
    if raw_secret.is_empty() {
        return Err(RuntimeSecurityInvariantError::EmptyField { field: "raw_secret" });
    }
    for (surface, serialized) in serialized_surfaces {
        if serialized.contains(raw_secret) {
            return Err(RuntimeSecurityInvariantError::RawSecretLeaked {
                surface: (*surface).to_owned(),
            });
        }
    }
    Ok(())
}

/// Asserts that the execution gate remains mandatory for a tool fixture.
///
/// # Errors
/// Returns [`RuntimeSecurityInvariantError::ToolGateMissing`] when the fixture
/// would let a tool call bypass the gate.
pub fn assert_tool_requires_gate(
    fixture: &RuntimeToolGateFixture,
) -> Result<(), RuntimeSecurityInvariantError> {
    if fixture.tool_name.trim().is_empty() {
        return Err(RuntimeSecurityInvariantError::EmptyField { field: "tool_name" });
    }
    if !fixture.gate_required {
        return Err(RuntimeSecurityInvariantError::ToolGateMissing {
            tool_name: fixture.tool_name.clone(),
        });
    }
    Ok(())
}

/// Asserts that harnesses never receive direct journal write authority.
///
/// # Errors
/// Returns [`RuntimeSecurityInvariantError::DirectJournalWriteAuthority`] when
/// any fixture grants direct journal writes.
pub fn assert_no_direct_journal_write_authority(
    authorities: &[RuntimeHarnessAuthorityFixture],
) -> Result<(), RuntimeSecurityInvariantError> {
    for authority in authorities {
        if authority.allowed && authority.authority == "journal.write.direct" {
            return Err(RuntimeSecurityInvariantError::DirectJournalWriteAuthority {
                authority: authority.authority.clone(),
            });
        }
    }
    Ok(())
}

/// Asserts that approval denial terminates a mutating operation.
///
/// # Errors
/// Returns [`RuntimeSecurityInvariantError::ApprovalDenialNotTerminal`] when a
/// denied mutating fixture can still continue.
pub fn assert_approval_denial_is_terminal_for_mutation(
    fixture: &RuntimeMutationApprovalFixture,
) -> Result<(), RuntimeSecurityInvariantError> {
    let terminal = matches!(
        fixture.terminal_state.as_str(),
        "blocked" | "denied" | "failed_closed" | "cancelled"
    );
    if fixture.mutating && fixture.approval_denied && !terminal {
        return Err(RuntimeSecurityInvariantError::ApprovalDenialNotTerminal {
            tool_name: fixture.tool_name.clone(),
        });
    }
    Ok(())
}

fn validate_event_name(value: &str) -> Result<(), RuntimeRoadmapHarnessValidationError> {
    let valid = !value.is_empty()
        && value.split('.').all(|segment| {
            !segment.is_empty()
                && segment.bytes().all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        });
    if valid {
        Ok(())
    } else {
        Err(RuntimeRoadmapHarnessValidationError::InvalidEventName { value: value.to_owned() })
    }
}

/// One source-level smoke target required by a Phase 0 roadmap fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRoadmapSmokeTarget {
    pub target_id: String,
    pub source_path: String,
    pub required_symbol: String,
    pub reason_code: RuntimeRoadmapReasonCode,
}

impl RuntimeRoadmapSmokeTarget {
    /// Builds a source-level smoke target with stable identifiers.
    #[must_use]
    pub fn new(
        target_id: impl Into<String>,
        source_path: impl Into<String>,
        required_symbol: impl Into<String>,
        reason_code: RuntimeRoadmapReasonCode,
    ) -> Self {
        Self {
            target_id: target_id.into(),
            source_path: source_path.into(),
            required_symbol: required_symbol.into(),
            reason_code,
        }
    }

    fn validate(&self) -> Result<(), RuntimeRoadmapHarnessValidationError> {
        validate_slug("smoke target id", self.target_id.as_str())?;
        validate_repo_relative_path("smoke target source path", self.source_path.as_str())?;
        if self.required_symbol.trim().is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingRequiredSymbol {
                target_id: self.target_id.clone(),
            });
        }
        Ok(())
    }
}

/// Shared replay/regression fixture contract for Phase 0 roadmap milestones.
///
/// The fixture is metadata-only: it names existing sources, expected journal
/// event names, reason codes, redaction boundaries, and evidence references so
/// later runtime milestones can reuse one audit vocabulary without creating a
/// second storage path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRoadmapHarnessFixture {
    pub schema_version: u32,
    pub fixture_id: String,
    pub capability: RuntimeRoadmapCapability,
    pub source_path: String,
    pub decision: RuntimeRoadmapDecision,
    pub reason_code: RuntimeRoadmapReasonCode,
    pub expected_journal_event: RuntimeRoadmapEventType,
    pub redaction_boundary: RuntimeRoadmapRedactionBoundary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub smoke_targets: Vec<RuntimeRoadmapSmokeTarget>,
}

impl RuntimeRoadmapHarnessFixture {
    /// Builds a Phase 0 harness fixture using the current schema version.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        fixture_id: impl Into<String>,
        capability: RuntimeRoadmapCapability,
        source_path: impl Into<String>,
        decision: RuntimeRoadmapDecision,
        reason_code: RuntimeRoadmapReasonCode,
        expected_journal_event: RuntimeRoadmapEventType,
        redaction_boundary: RuntimeRoadmapRedactionBoundary,
        evidence_refs: Vec<String>,
        smoke_targets: Vec<RuntimeRoadmapSmokeTarget>,
    ) -> Self {
        Self {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            fixture_id: fixture_id.into(),
            capability,
            source_path: source_path.into(),
            decision,
            reason_code,
            expected_journal_event,
            redaction_boundary,
            evidence_refs,
            smoke_targets,
        }
    }

    /// Validates that the fixture can be used in journal and replay-oriented tests.
    ///
    /// # Errors
    /// Returns [`RuntimeRoadmapHarnessValidationError`] when the fixture uses an
    /// unsupported schema, unsafe path, empty evidence set, invalid smoke target,
    /// or a journal event that violates the canonical rollout metadata rules.
    pub fn validate(&self) -> Result<(), RuntimeRoadmapHarnessValidationError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeRoadmapHarnessValidationError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }
        validate_slug("fixture id", self.fixture_id.as_str())?;
        validate_repo_relative_path("fixture source path", self.source_path.as_str())?;
        if self.evidence_refs.is_empty() {
            return Err(RuntimeRoadmapHarnessValidationError::MissingEvidenceRef {
                fixture_id: self.fixture_id.clone(),
            });
        }
        for evidence_ref in &self.evidence_refs {
            validate_repo_relative_path("fixture evidence ref", evidence_ref.as_str())?;
        }
        for smoke_target in &self.smoke_targets {
            smoke_target.validate()?;
        }
        self.to_journal_event()?
            .validate()
            .map_err(RuntimeRoadmapHarnessValidationError::JournalEventInvalid)?;
        Ok(())
    }

    /// Builds the journal payload associated with this fixture.
    ///
    /// # Errors
    /// Returns [`RuntimeRoadmapHarnessValidationError`] if the fixture cannot be
    /// represented safely as a journal event.
    pub fn to_journal_event(
        &self,
    ) -> Result<RuntimeRoadmapJournalEvent, RuntimeRoadmapHarnessValidationError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeRoadmapHarnessValidationError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }
        let mut event = RuntimeRoadmapJournalEvent::new(
            self.expected_journal_event,
            self.capability,
            self.decision,
            self.reason_code,
            self.redaction_boundary,
        );
        for evidence_ref in &self.evidence_refs {
            event = event.with_evidence_ref(evidence_ref);
        }
        Ok(event)
    }
}

/// Read model exposed through diagnostics for the Phase 0 harness fixtures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRoadmapHarnessProjection {
    pub schema_version: u32,
    pub fixtures_total: usize,
    pub smoke_targets_total: usize,
    pub capabilities: Vec<RuntimeRoadmapCapability>,
    pub journal_event_names: Vec<String>,
    pub reason_codes: Vec<RuntimeRoadmapReasonCode>,
    pub evidence_refs: Vec<String>,
}

/// Returns the canonical Phase 0 harness fixture catalog.
#[must_use]
pub fn runtime_roadmap_phase0_harness_fixtures() -> Vec<RuntimeRoadmapHarnessFixture> {
    vec![
        RuntimeRoadmapHarnessFixture::new(
            "phase0_replay_regression_harness",
            RuntimeRoadmapCapability::ReplayRegressionHarness,
            "crates/palyra-common/src/release_evals/evaluator.rs",
            RuntimeRoadmapDecision::AdoptExistingSurface,
            RuntimeRoadmapReasonCode::ReplayRegressionFixtureAccepted,
            RuntimeRoadmapEventType::HarnessCompleted,
            RuntimeRoadmapRedactionBoundary::SanitizedAuditPayload,
            vec![
                "fixtures/golden/release_eval_inventory.json".to_owned(),
                "crates/palyra-common/tests/release_eval_contract.rs".to_owned(),
            ],
            vec![
                RuntimeRoadmapSmokeTarget::new(
                    "release_eval_replay_bundle_builder",
                    "crates/palyra-common/src/release_evals/evaluator.rs",
                    "build_release_eval_replay_bundle",
                    RuntimeRoadmapReasonCode::ReplayRegressionFixtureAccepted,
                ),
                RuntimeRoadmapSmokeTarget::new(
                    "release_eval_contract_test",
                    "crates/palyra-common/tests/release_eval_contract.rs",
                    "release_eval_fixture_covers_all_required_suites_and_inventory",
                    RuntimeRoadmapReasonCode::ReplayRegressionFixtureAccepted,
                ),
            ],
        ),
        RuntimeRoadmapHarnessFixture::new(
            "phase0_codex_milestone_style",
            RuntimeRoadmapCapability::MilestoneImplementationStyle,
            "crates/palyra-common/src/runtime_roadmap.rs",
            RuntimeRoadmapDecision::DefineContract,
            RuntimeRoadmapReasonCode::ImplementationStyleDocumented,
            RuntimeRoadmapEventType::ContractCompleted,
            RuntimeRoadmapRedactionBoundary::MetadataOnly,
            vec!["crates/palyra-common/src/runtime_roadmap.rs".to_owned()],
            vec![RuntimeRoadmapSmokeTarget::new(
                "runtime_roadmap_metadata_only_boundary",
                "crates/palyra-common/src/runtime_roadmap.rs",
                "RuntimeRoadmapHarnessFixture",
                RuntimeRoadmapReasonCode::ImplementationStyleDocumented,
            )],
        ),
        RuntimeRoadmapHarnessFixture::new(
            "phase0_integration_smoke_validation",
            RuntimeRoadmapCapability::IntegrationSmokeValidation,
            "crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs",
            RuntimeRoadmapDecision::AdoptExistingSurface,
            RuntimeRoadmapReasonCode::IntegrationSmokeTargetVerified,
            RuntimeRoadmapEventType::HarnessCompleted,
            RuntimeRoadmapRedactionBoundary::MetadataOnly,
            vec![
                "crates/palyra-daemon/src/journal.rs".to_owned(),
                "crates/palyra-daemon/src/application/run_stream/tape.rs".to_owned(),
                "crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs"
                    .to_owned(),
            ],
            vec![
                RuntimeRoadmapSmokeTarget::new(
                    "journal_append_log",
                    "crates/palyra-daemon/src/journal.rs",
                    "JournalStore",
                    RuntimeRoadmapReasonCode::IntegrationSmokeTargetVerified,
                ),
                RuntimeRoadmapSmokeTarget::new(
                    "run_stream_tape_projection",
                    "crates/palyra-daemon/src/application/run_stream/tape.rs",
                    "append_runtime_decision_tape_event",
                    RuntimeRoadmapReasonCode::IntegrationSmokeTargetVerified,
                ),
                RuntimeRoadmapSmokeTarget::new(
                    "console_runtime_roadmap_diagnostics",
                    "crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs",
                    "collect_console_runtime_roadmap_diagnostics",
                    RuntimeRoadmapReasonCode::IntegrationSmokeTargetVerified,
                ),
            ],
        ),
    ]
}

/// Projects harness fixtures into a compact diagnostics/read-model summary.
///
/// # Errors
/// Returns [`RuntimeRoadmapHarnessValidationError`] when any fixture is invalid.
pub fn project_runtime_roadmap_harness(
    fixtures: &[RuntimeRoadmapHarnessFixture],
) -> Result<RuntimeRoadmapHarnessProjection, RuntimeRoadmapHarnessValidationError> {
    let mut capabilities = BTreeSet::new();
    let mut journal_event_names = BTreeSet::new();
    let mut reason_codes = BTreeSet::new();
    let mut evidence_refs = BTreeSet::new();
    let mut smoke_targets_total = 0;

    for fixture in fixtures {
        fixture.validate()?;
        capabilities.insert(fixture.capability);
        journal_event_names.insert(fixture.expected_journal_event.journal_event().to_owned());
        reason_codes.insert(fixture.reason_code);
        smoke_targets_total += fixture.smoke_targets.len();
        for evidence_ref in &fixture.evidence_refs {
            evidence_refs.insert(evidence_ref.clone());
        }
        for smoke_target in &fixture.smoke_targets {
            reason_codes.insert(smoke_target.reason_code);
        }
    }

    Ok(RuntimeRoadmapHarnessProjection {
        schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
        fixtures_total: fixtures.len(),
        smoke_targets_total,
        capabilities: capabilities.into_iter().collect(),
        journal_event_names: journal_event_names.into_iter().collect(),
        reason_codes: reason_codes.into_iter().collect(),
        evidence_refs: evidence_refs.into_iter().collect(),
    })
}

/// Builds the canonical diagnostics projection for Phase 0 harness fixtures.
///
/// # Errors
/// Returns [`RuntimeRoadmapHarnessValidationError`] if the built-in catalog drifts.
pub fn runtime_roadmap_phase0_harness_projection(
) -> Result<RuntimeRoadmapHarnessProjection, RuntimeRoadmapHarnessValidationError> {
    project_runtime_roadmap_harness(runtime_roadmap_phase0_harness_fixtures().as_slice())
}

/// Journal payload envelope for roadmap baseline and rollout decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRoadmapJournalEvent {
    pub schema_version: u32,
    pub event_type: RuntimeRoadmapEventType,
    pub capability: RuntimeRoadmapCapability,
    pub decision: RuntimeRoadmapDecision,
    pub reason_code: RuntimeRoadmapReasonCode,
    pub redaction_boundary: RuntimeRoadmapRedactionBoundary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_env_var: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout_config_path: Option<String>,
}

impl RuntimeRoadmapJournalEvent {
    /// Creates an event using canonical rollout metadata for `capability`.
    #[must_use]
    pub fn new(
        event_type: RuntimeRoadmapEventType,
        capability: RuntimeRoadmapCapability,
        decision: RuntimeRoadmapDecision,
        reason_code: RuntimeRoadmapReasonCode,
        redaction_boundary: RuntimeRoadmapRedactionBoundary,
    ) -> Self {
        let rollout_gate = capability.rollout_gate();
        Self {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            event_type,
            capability,
            decision,
            reason_code,
            redaction_boundary,
            evidence_refs: Vec::new(),
            rollout_env_var: rollout_gate.map(|gate| gate.env_var.to_owned()),
            rollout_config_path: rollout_gate.map(|gate| gate.config_path.to_owned()),
        }
    }

    /// Adds one evidence reference such as a fixture, test, source event id, or config path.
    #[must_use]
    pub fn with_evidence_ref(mut self, evidence_ref: impl Into<String>) -> Self {
        self.evidence_refs.push(evidence_ref.into());
        self
    }

    /// Dotted journal event name for this payload.
    #[must_use]
    pub const fn journal_event_name(&self) -> &'static str {
        self.event_type.journal_event()
    }

    /// Validates schema version and rollout metadata invariants.
    ///
    /// # Errors
    /// Returns [`RuntimeRoadmapValidationError`] when the payload uses an unsupported
    /// schema version, omits required rollout metadata, or carries rollout metadata
    /// that conflicts with the canonical capability catalog.
    pub fn validate(&self) -> Result<(), RuntimeRoadmapValidationError> {
        if self.schema_version != RUNTIME_ROADMAP_SCHEMA_VERSION {
            return Err(RuntimeRoadmapValidationError::UnsupportedSchemaVersion {
                actual: self.schema_version,
                expected: RUNTIME_ROADMAP_SCHEMA_VERSION,
            });
        }

        match self.capability.rollout_gate() {
            Some(expected) => {
                if self.decision.requires_rollout_gate()
                    && (self.rollout_env_var.is_none() || self.rollout_config_path.is_none())
                {
                    return Err(RuntimeRoadmapValidationError::MissingRolloutGate {
                        capability: self.capability,
                        decision: self.decision,
                    });
                }
                if self.rollout_env_var.as_deref().is_some_and(|value| value != expected.env_var)
                    || self
                        .rollout_config_path
                        .as_deref()
                        .is_some_and(|value| value != expected.config_path)
                {
                    return Err(RuntimeRoadmapValidationError::RolloutGateMismatch {
                        capability: self.capability,
                        expected_env_var: expected.env_var,
                        expected_config_path: expected.config_path,
                    });
                }
            }
            None => {
                if self.rollout_env_var.is_some() || self.rollout_config_path.is_some() {
                    return Err(RuntimeRoadmapValidationError::UnexpectedRolloutGate {
                        capability: self.capability,
                    });
                }
            }
        }

        Ok(())
    }
}

/// Validation failure for [`RuntimeRoadmapJournalEvent`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeRoadmapValidationError {
    #[error("runtime roadmap event schema version {actual} is not supported; expected {expected}")]
    UnsupportedSchemaVersion { actual: u32, expected: u32 },
    #[error("{capability} requires rollout metadata for decision {decision}")]
    MissingRolloutGate { capability: RuntimeRoadmapCapability, decision: RuntimeRoadmapDecision },
    #[error("{capability} does not use rollout metadata")]
    UnexpectedRolloutGate { capability: RuntimeRoadmapCapability },
    #[error(
        "{capability} rollout metadata must match {expected_config_path} and {expected_env_var}"
    )]
    RolloutGateMismatch {
        capability: RuntimeRoadmapCapability,
        expected_env_var: &'static str,
        expected_config_path: &'static str,
    },
}

/// Validation failure for Phase 0 harness fixtures and smoke targets.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeRoadmapHarnessValidationError {
    #[error(
        "runtime roadmap harness schema version {actual} is not supported; expected {expected}"
    )]
    UnsupportedSchemaVersion { actual: u32, expected: u32 },
    #[error("{field} must be a non-empty lowercase ASCII slug")]
    InvalidSlug { field: &'static str, value: String },
    #[error("{field} must be a repo-relative path without traversal or platform separators")]
    InvalidRepoRelativePath { field: &'static str, value: String },
    #[error("{fixture_id} must include at least one evidence reference")]
    MissingEvidenceRef { fixture_id: String },
    #[error("smoke target {target_id} must name a required symbol")]
    MissingRequiredSymbol { target_id: String },
    #[error("{fixture_id} must include at least one provider chunk")]
    MissingTrajectoryProviderChunk { fixture_id: String },
    #[error("{fixture_id} must include at least one expected trajectory event")]
    MissingTrajectoryExpectedEvent { fixture_id: String },
    #[error("runtime trajectory event name must be dotted lowercase ASCII: {value}")]
    InvalidEventName { value: String },
    #[error("harness fixture cannot be represented as a valid journal event: {0}")]
    JournalEventInvalid(RuntimeRoadmapValidationError),
}

fn validate_slug(
    field: &'static str,
    value: &str,
) -> Result<(), RuntimeRoadmapHarnessValidationError> {
    let valid = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(RuntimeRoadmapHarnessValidationError::InvalidSlug { field, value: value.to_owned() })
    }
}

fn validate_repo_relative_path(
    field: &'static str,
    value: &str,
) -> Result<(), RuntimeRoadmapHarnessValidationError> {
    let invalid = value.trim().is_empty()
        || value.starts_with('/')
        || value.starts_with('\\')
        || value.contains('\\')
        || value.contains(':')
        || value.split('/').any(|segment| segment.is_empty() || segment == "." || segment == "..");
    if invalid {
        Err(RuntimeRoadmapHarnessValidationError::InvalidRepoRelativePath {
            field,
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        assert_approval_denial_is_terminal_for_mutation,
        assert_host_authority_checklist_denies_direct_runtime_authority,
        assert_no_direct_journal_write_authority, assert_no_raw_secret_in_tape,
        assert_tool_requires_gate, backend_runtime_fixture_taxonomy,
        project_backend_runtime_fixture_taxonomy, project_runtime_roadmap_harness,
        runtime_boundary_event_taxonomy, runtime_host_authority_checklist,
        runtime_roadmap_capability_catalog, runtime_roadmap_phase0_harness_fixtures,
        runtime_roadmap_phase0_harness_projection, runtime_roadmap_phase1_trajectory_fixtures,
        runtime_roadmap_phase1_trajectory_projection, sanitize_runtime_boundary_metadata,
        BackendRuntimeFixture, BackendRuntimeFixtureArea, RuntimeBoundaryFamily,
        RuntimeGoldenTrajectoryFixture, RuntimeHarnessAuthorityFixture,
        RuntimeHostAuthorityChecklistEntry, RuntimeHostAuthorityInterface,
        RuntimeMutationApprovalFixture, RuntimeRoadmapCapability, RuntimeRoadmapDecision,
        RuntimeRoadmapEventType, RuntimeRoadmapHarnessFixture,
        RuntimeRoadmapHarnessValidationError, RuntimeRoadmapJournalEvent, RuntimeRoadmapReasonCode,
        RuntimeRoadmapRedactionBoundary, RuntimeRoadmapValidationError,
        RuntimeSecurityInvariantError, RuntimeToolGateFixture, ALL_RUNTIME_ROADMAP_CAPABILITIES,
        HOST_OWNED_RUNTIME_AUTHORITIES, RUNTIME_ROADMAP_SCHEMA_VERSION,
    };
    use crate::feature_rollouts::{
        AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH, AGENT_HARNESS_RUNTIME_ROLLOUT_ENV,
        TOOL_REPAIR_ROLLOUT_CONFIG_PATH, TOOL_REPAIR_ROLLOUT_ENV,
    };

    const PHASE0_HARNESS_FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/golden/runtime_roadmap_phase0_harness.json"
    );
    const PHASE1_TRAJECTORY_FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/golden/runtime_roadmap_phase1_trajectories.json"
    );
    const BACKEND_RUNTIME_FIXTURE_TAXONOMY: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/golden/backend_runtime_fixture_taxonomy.json"
    );

    #[test]
    fn roadmap_capability_catalog_exposes_rollout_boundaries() {
        let catalog = runtime_roadmap_capability_catalog();

        assert_eq!(catalog.len(), ALL_RUNTIME_ROADMAP_CAPABILITIES.len());
        let tool_repair = catalog
            .iter()
            .find(|entry| entry.capability == RuntimeRoadmapCapability::ToolRepair)
            .expect("tool repair capability should be in the catalog");
        assert_eq!(tool_repair.schema_version, RUNTIME_ROADMAP_SCHEMA_VERSION);
        assert_eq!(tool_repair.rollout_env_var.as_deref(), Some(TOOL_REPAIR_ROLLOUT_ENV));
        assert_eq!(
            tool_repair.rollout_config_path.as_deref(),
            Some(TOOL_REPAIR_ROLLOUT_CONFIG_PATH)
        );
        assert!(!tool_repair.default_enabled);

        let agent_harness = catalog
            .iter()
            .find(|entry| entry.capability == RuntimeRoadmapCapability::AgentHarnessRuntime)
            .expect("agent harness runtime capability should be in the catalog");
        assert_eq!(
            agent_harness.rollout_env_var.as_deref(),
            Some(AGENT_HARNESS_RUNTIME_ROLLOUT_ENV)
        );
        assert_eq!(
            agent_harness.rollout_config_path.as_deref(),
            Some(AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH)
        );
        assert!(!agent_harness.default_enabled);

        let baseline = catalog
            .iter()
            .find(|entry| entry.capability == RuntimeRoadmapCapability::BaselineContracts)
            .expect("baseline capability should be in the catalog");
        assert!(baseline.rollout_env_var.is_none());
        assert!(baseline.rollout_config_path.is_none());
    }

    #[test]
    fn roadmap_journal_event_serializes_stable_contract_fields() {
        let event = RuntimeRoadmapJournalEvent::new(
            RuntimeRoadmapEventType::RolloutGateRegistered,
            RuntimeRoadmapCapability::ToolRepair,
            RuntimeRoadmapDecision::RequireFeatureRollout,
            RuntimeRoadmapReasonCode::RolloutDefaultOff,
            RuntimeRoadmapRedactionBoundary::MetadataOnly,
        )
        .with_evidence_ref("feature_rollouts.tool_repair");

        event.validate().expect("canonical event should validate");
        assert_eq!(event.journal_event_name(), "runtime_roadmap.rollout_gate.registered");

        let encoded = serde_json::to_value(&event).expect("event should serialize");
        assert_eq!(encoded["schema_version"], 1);
        assert_eq!(encoded["event_type"], "rollout_gate_registered");
        assert_eq!(encoded["capability"], "tool_repair");
        assert_eq!(encoded["decision"], "require_feature_rollout");
        assert_eq!(encoded["reason_code"], "runtime_roadmap.rollout.default_off");
        assert_eq!(encoded["redaction_boundary"], "metadata_only");
        assert_eq!(encoded["rollout_env_var"], TOOL_REPAIR_ROLLOUT_ENV);
        assert_eq!(encoded["rollout_config_path"], TOOL_REPAIR_ROLLOUT_CONFIG_PATH);
    }

    #[test]
    fn roadmap_journal_event_allows_baseline_without_rollout_gate() {
        let event = RuntimeRoadmapJournalEvent::new(
            RuntimeRoadmapEventType::ContractRegistered,
            RuntimeRoadmapCapability::BaselineContracts,
            RuntimeRoadmapDecision::DefineContract,
            RuntimeRoadmapReasonCode::ContractsDefined,
            RuntimeRoadmapRedactionBoundary::SanitizedAuditPayload,
        );

        event.validate().expect("baseline metadata does not require rollout gate");
    }

    #[test]
    fn roadmap_journal_event_rejects_missing_required_rollout_gate() {
        let event: RuntimeRoadmapJournalEvent = serde_json::from_value(json!({
            "schema_version": 1,
            "event_type": "rollout_gate_registered",
            "capability": "tool_repair",
            "decision": "require_feature_rollout",
            "reason_code": "runtime_roadmap.rollout.default_off",
            "redaction_boundary": "metadata_only"
        }))
        .expect("shape should deserialize before semantic validation");

        let error = event.validate().expect_err("missing rollout gate should fail validation");
        assert_eq!(
            error,
            RuntimeRoadmapValidationError::MissingRolloutGate {
                capability: RuntimeRoadmapCapability::ToolRepair,
                decision: RuntimeRoadmapDecision::RequireFeatureRollout,
            }
        );
    }

    #[test]
    fn roadmap_journal_event_rejects_unknown_schema_version() {
        let mut event = RuntimeRoadmapJournalEvent::new(
            RuntimeRoadmapEventType::ContractCompleted,
            RuntimeRoadmapCapability::ReasonCodeTaxonomy,
            RuntimeRoadmapDecision::DefineContract,
            RuntimeRoadmapReasonCode::ReasonTaxonomyCatalogued,
            RuntimeRoadmapRedactionBoundary::MetadataOnly,
        );
        event.schema_version = 2;

        assert!(matches!(
            event.validate(),
            Err(RuntimeRoadmapValidationError::UnsupportedSchemaVersion { actual: 2, expected: 1 })
        ));
    }

    #[test]
    fn phase0_harness_fixture_serializes_stable_contract_fields() {
        let fixture = runtime_roadmap_phase0_harness_fixtures()
            .into_iter()
            .find(|fixture| fixture.fixture_id == "phase0_replay_regression_harness")
            .expect("replay regression fixture should be declared");

        fixture.validate().expect("built-in fixture should validate");
        let event = fixture.to_journal_event().expect("fixture should build journal event");
        assert_eq!(event.journal_event_name(), "runtime_roadmap.harness.completed");

        let encoded = serde_json::to_value(&fixture).expect("fixture should serialize");
        assert_eq!(encoded["schema_version"], 1);
        assert_eq!(encoded["capability"], "replay_regression_harness");
        assert_eq!(encoded["reason_code"], "runtime_roadmap.fixture.replay_regression_accepted");
        assert_eq!(encoded["expected_journal_event"], "harness_completed");
        assert_eq!(encoded["redaction_boundary"], "sanitized_audit_payload");
        assert_eq!(
            encoded["smoke_targets"][0]["required_symbol"],
            "build_release_eval_replay_bundle"
        );
    }

    #[test]
    fn phase0_harness_projection_covers_foundation_capabilities() {
        let projection =
            runtime_roadmap_phase0_harness_projection().expect("projection should validate");

        assert_eq!(projection.schema_version, RUNTIME_ROADMAP_SCHEMA_VERSION);
        assert_eq!(projection.fixtures_total, 3);
        assert_eq!(projection.smoke_targets_total, 6);
        assert_eq!(
            projection.capabilities,
            vec![
                RuntimeRoadmapCapability::ReplayRegressionHarness,
                RuntimeRoadmapCapability::MilestoneImplementationStyle,
                RuntimeRoadmapCapability::IntegrationSmokeValidation,
            ]
        );
        assert!(projection
            .journal_event_names
            .contains(&"runtime_roadmap.harness.completed".to_owned()));
        assert!(projection
            .reason_codes
            .contains(&RuntimeRoadmapReasonCode::IntegrationSmokeTargetVerified));
        assert!(projection
            .evidence_refs
            .contains(&"fixtures/golden/release_eval_inventory.json".to_owned()));
    }

    #[test]
    fn boundary_taxonomy_covers_new_runtime_families_and_redacts_metadata() {
        let taxonomy = runtime_boundary_event_taxonomy();

        assert_eq!(taxonomy.len(), 11);
        assert!(taxonomy.iter().any(|entry| {
            entry.family == RuntimeBoundaryFamily::Harness
                && entry.event_name == "harness.selection.decision"
                && entry.rollout_capability == Some(RuntimeRoadmapCapability::AgentHarnessRuntime)
        }));
        assert!(taxonomy.iter().any(|entry| entry.event_name == "tool.middleware.failed"));
        assert!(taxonomy.iter().any(|entry| entry.event_name == "turn.recovery.retry_planned"));

        let sanitized = sanitize_runtime_boundary_metadata(json!({
            "provider": "openai-compatible",
            "authorization": "Bearer raw-secret",
            "message": "callback failed token=abc mode=retry",
            "vault_ref": "vault://global/openai_api_key",
        }));

        let rendered = serde_json::to_string(&sanitized).expect("metadata should serialize");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("raw-secret"));
        assert!(!rendered.contains("token=abc"));
        assert!(!rendered.contains("vault://global/openai_api_key"));
    }

    #[test]
    fn host_authority_checklist_denies_direct_runtime_authority() {
        let checklist = runtime_host_authority_checklist();

        assert_eq!(checklist.len(), 7);
        assert_host_authority_checklist_denies_direct_runtime_authority(checklist.as_slice())
            .expect("built-in authority checklist should preserve host fences");
        let harness = checklist
            .iter()
            .find(|entry| entry.interface == RuntimeHostAuthorityInterface::Harness)
            .expect("harness checklist entry should exist");
        assert!(harness.denied_authorities.iter().any(|authority| authority == "journal"));
        assert!(harness.denied_authorities.iter().any(|authority| authority == "tool_execution"));

        let mut leaked = RuntimeHostAuthorityChecklistEntry {
            schema_version: RUNTIME_ROADMAP_SCHEMA_VERSION,
            interface: RuntimeHostAuthorityInterface::Terminal,
            integration_boundary: "test".to_owned(),
            denied_authorities: vec!["credentials".to_owned()],
            host_owned_authorities: vec!["credentials".to_owned()],
            audit_event_samples: vec!["terminal.session.lifecycle".to_owned()],
        };
        let error = leaked.validate().expect_err("missing host-owned authority fences should fail");
        assert_eq!(
            error,
            RuntimeSecurityInvariantError::HostAuthorityGranted {
                interface: "terminal".to_owned(),
                authority: "approvals".to_owned(),
            }
        );

        leaked.denied_authorities = HOST_OWNED_RUNTIME_AUTHORITIES
            .iter()
            .map(|authority| (*authority).to_owned())
            .collect();
        leaked.host_owned_authorities = leaked.denied_authorities.clone();
        leaked.audit_event_samples = vec!["Terminal Session".to_owned()];
        assert!(matches!(
            leaked.validate(),
            Err(RuntimeSecurityInvariantError::InvalidAuditEventName { .. })
        ));
    }

    #[test]
    fn backend_runtime_fixture_taxonomy_matches_golden() {
        let fixture_bytes = std::fs::read(BACKEND_RUNTIME_FIXTURE_TAXONOMY)
            .expect("backend runtime fixture taxonomy should exist");
        let from_disk: Vec<BackendRuntimeFixture> =
            serde_json::from_slice(fixture_bytes.as_slice())
                .expect("backend runtime fixture taxonomy should deserialize");
        let generated = backend_runtime_fixture_taxonomy();

        assert_eq!(from_disk, generated);
        let projection = project_backend_runtime_fixture_taxonomy(from_disk.as_slice())
            .expect("backend runtime fixture taxonomy should project");
        assert_eq!(projection.fixtures_total, 6);
        assert_eq!(
            projection.areas,
            vec![
                BackendRuntimeFixtureArea::RunLoop,
                BackendRuntimeFixtureArea::ProviderStream,
                BackendRuntimeFixtureArea::ToolCall,
                BackendRuntimeFixtureArea::FilePatch,
                BackendRuntimeFixtureArea::Lsp,
                BackendRuntimeFixtureArea::Compaction,
            ]
        );
        assert!(projection
            .expected_journal_events
            .contains(&"run.runtime_path_summary".to_owned()));
    }

    #[test]
    fn phase1_trajectory_projection_covers_smoke_tool_and_malformed_provider_paths() {
        let projection =
            runtime_roadmap_phase1_trajectory_projection().expect("projection should validate");

        assert_eq!(projection.schema_version, RUNTIME_ROADMAP_SCHEMA_VERSION);
        assert_eq!(projection.fixtures_total, 3);
        assert_eq!(projection.provider_chunks_total, 4);
        assert_eq!(projection.tool_calls_total, 1);
        assert_eq!(projection.malformed_provider_fixtures, 1);
        assert!(projection.event_names.contains(&"harness.selection.decision".to_owned()));
        assert!(projection.event_names.contains(&"turn.recovery.retry_planned".to_owned()));
        assert!(projection
            .enabled_capabilities
            .contains(&RuntimeRoadmapCapability::ProviderRecovery));
    }

    #[test]
    fn phase1_trajectory_golden_fixture_matches_generated_catalog() {
        let fixture_bytes = std::fs::read(PHASE1_TRAJECTORY_FIXTURE)
            .expect("golden trajectory fixture should exist");
        let from_disk: Vec<RuntimeGoldenTrajectoryFixture> =
            serde_json::from_slice(fixture_bytes.as_slice())
                .expect("golden trajectory fixture should deserialize");
        let generated = runtime_roadmap_phase1_trajectory_fixtures();

        assert_eq!(from_disk, generated);
        for fixture in from_disk {
            fixture.validate().expect("golden trajectory fixture should validate");
        }
    }

    #[test]
    fn phase1_trajectory_rejects_invalid_event_names() {
        let mut fixture = runtime_roadmap_phase1_trajectory_fixtures()
            .into_iter()
            .next()
            .expect("built-in fixtures should not be empty");
        fixture.expected_events[0].event_name = "Provider Stream".to_owned();

        let error = fixture.validate().expect_err("invalid event name should fail");
        assert_eq!(
            error,
            RuntimeRoadmapHarnessValidationError::InvalidEventName {
                value: "Provider Stream".to_owned(),
            }
        );
    }

    #[test]
    fn security_invariant_helpers_reject_secret_and_authority_bypasses() {
        let raw_secret = "secret_should_not_appear";
        let stdout = "tool completed with <redacted>";
        let stderr = "provider token=<redacted>";
        let tape = "{\"event\":\"tool.completed\",\"payload\":\"<redacted>\"}";
        assert_no_raw_secret_in_tape(
            raw_secret,
            &[("stdout", stdout), ("stderr", stderr), ("tape", tape)],
        )
        .expect("redacted surfaces should pass");

        let leaked = assert_no_raw_secret_in_tape(raw_secret, &[("diagnostics", raw_secret)])
            .expect_err("raw secret should fail");
        assert_eq!(
            leaked,
            super::RuntimeSecurityInvariantError::RawSecretLeaked {
                surface: "diagnostics".to_owned(),
            }
        );

        assert_tool_requires_gate(&RuntimeToolGateFixture {
            tool_name: "palyra.fs.write_file".to_owned(),
            mutating: true,
            gate_required: true,
            reason_code: "execution_gate.required".to_owned(),
        })
        .expect("mutating tool should require gate");

        assert_no_direct_journal_write_authority(&[RuntimeHarnessAuthorityFixture {
            authority: "journal.write.callback".to_owned(),
            allowed: true,
        }])
        .expect("callback-mediated journal authority should be allowed");

        let direct = assert_no_direct_journal_write_authority(&[RuntimeHarnessAuthorityFixture {
            authority: "journal.write.direct".to_owned(),
            allowed: true,
        }])
        .expect_err("direct journal authority should fail");
        assert_eq!(
            direct,
            super::RuntimeSecurityInvariantError::DirectJournalWriteAuthority {
                authority: "journal.write.direct".to_owned(),
            }
        );

        assert_approval_denial_is_terminal_for_mutation(&RuntimeMutationApprovalFixture {
            tool_name: "palyra.fs.write_file".to_owned(),
            mutating: true,
            approval_denied: true,
            terminal_state: "blocked".to_owned(),
            reason_code: "approval.denied".to_owned(),
        })
        .expect("approval denial should terminate mutation");
    }

    #[test]
    fn phase0_harness_rejects_unsafe_fixture_paths() {
        let mut fixture = runtime_roadmap_phase0_harness_fixtures()
            .into_iter()
            .next()
            .expect("built-in fixtures should not be empty");
        fixture.source_path = "../escaped.json".to_owned();

        let error = fixture.validate().expect_err("path traversal should be rejected");
        assert_eq!(
            error,
            RuntimeRoadmapHarnessValidationError::InvalidRepoRelativePath {
                field: "fixture source path",
                value: "../escaped.json".to_owned(),
            }
        );
    }

    #[test]
    fn phase0_harness_rejects_missing_evidence() {
        let mut fixture = runtime_roadmap_phase0_harness_fixtures()
            .into_iter()
            .next()
            .expect("built-in fixtures should not be empty");
        fixture.evidence_refs.clear();

        let error = fixture.validate().expect_err("evidence refs should be required");
        assert_eq!(
            error,
            RuntimeRoadmapHarnessValidationError::MissingEvidenceRef {
                fixture_id: "phase0_replay_regression_harness".to_owned(),
            }
        );
    }

    #[test]
    fn phase0_harness_golden_fixture_matches_generated_catalog() {
        let fixture_bytes =
            std::fs::read(PHASE0_HARNESS_FIXTURE).expect("golden harness fixture should exist");
        let from_disk: Vec<RuntimeRoadmapHarnessFixture> =
            serde_json::from_slice(fixture_bytes.as_slice())
                .expect("golden harness fixture should deserialize");
        let generated = runtime_roadmap_phase0_harness_fixtures();

        assert_eq!(from_disk, generated);
        project_runtime_roadmap_harness(from_disk.as_slice())
            .expect("golden harness fixture should project");
    }

    #[test]
    fn phase0_smoke_targets_reference_existing_sources_and_symbols() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

        for fixture in runtime_roadmap_phase0_harness_fixtures() {
            let source_path = repo_root.join(fixture.source_path.as_str());
            assert!(
                source_path.is_file(),
                "fixture source path should exist: {}",
                fixture.source_path
            );
            for evidence_ref in &fixture.evidence_refs {
                assert!(
                    repo_root.join(evidence_ref).is_file(),
                    "fixture evidence ref should exist: {evidence_ref}"
                );
            }
            for smoke_target in fixture.smoke_targets {
                let target_path = repo_root.join(smoke_target.source_path.as_str());
                let source =
                    std::fs::read_to_string(target_path.as_path()).unwrap_or_else(|error| {
                        panic!("failed to read smoke target {}: {error}", smoke_target.source_path)
                    });
                assert!(
                    source.contains(smoke_target.required_symbol.as_str()),
                    "smoke target {} should contain symbol {}",
                    smoke_target.target_id,
                    smoke_target.required_symbol
                );
            }
        }
    }
}
