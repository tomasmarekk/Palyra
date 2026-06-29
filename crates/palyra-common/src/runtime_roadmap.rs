//! Shared contract for agent-runtime roadmap milestones.
//!
//! This module keeps new runtime-loop work on one vocabulary: stable capability
//! ids, reason codes, journal event names, rollout gates, and redaction posture.
//! It is intentionally metadata-only; enabling behavior still belongs behind
//! the daemon's feature rollout config.

use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};

use crate::feature_rollouts::{
    AGENT_PLAN_STATE_ROLLOUT_CONFIG_PATH, AGENT_PLAN_STATE_ROLLOUT_ENV,
    ATTACK_SURFACE_AUDIT_ROLLOUT_CONFIG_PATH, ATTACK_SURFACE_AUDIT_ROLLOUT_ENV,
    CHANNEL_TURN_KERNEL_ROLLOUT_CONFIG_PATH, CHANNEL_TURN_KERNEL_ROLLOUT_ENV,
    COMPACTION_SAFEGUARD_ROLLOUT_CONFIG_PATH, COMPACTION_SAFEGUARD_ROLLOUT_ENV,
    OBJECTIVE_JUDGE_ROLLOUT_CONFIG_PATH, OBJECTIVE_JUDGE_ROLLOUT_ENV,
    PROGRESS_DRAFTS_ROLLOUT_CONFIG_PATH, PROGRESS_DRAFTS_ROLLOUT_ENV,
    PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH, PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV,
    TOOL_REPAIR_ROLLOUT_CONFIG_PATH, TOOL_REPAIR_ROLLOUT_ENV,
    VERIFICATION_RUNTIME_ROLLOUT_CONFIG_PATH, VERIFICATION_RUNTIME_ROLLOUT_ENV,
};

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
        ToolRepair => "tool_repair",
        ProviderStreamNormalizer => "provider_stream_normalizer",
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
pub const ALL_RUNTIME_ROADMAP_CAPABILITIES: [RuntimeRoadmapCapability; 16] = [
    RuntimeRoadmapCapability::BaselineContracts,
    RuntimeRoadmapCapability::ReasonCodeTaxonomy,
    RuntimeRoadmapCapability::JournalSchemaExtension,
    RuntimeRoadmapCapability::RuntimeLoopRolloutConfig,
    RuntimeRoadmapCapability::ReplayRegressionHarness,
    RuntimeRoadmapCapability::MilestoneImplementationStyle,
    RuntimeRoadmapCapability::IntegrationSmokeValidation,
    RuntimeRoadmapCapability::ToolRepair,
    RuntimeRoadmapCapability::ProviderStreamNormalizer,
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
            Self::ToolRepair => "Tool repair",
            Self::ProviderStreamNormalizer => "Provider stream normalizer",
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
            Self::ToolRepair => "Guards tool-call repair parsing and proposal recovery.",
            Self::ProviderStreamNormalizer => {
                "Guards provider stream normalization before tool proposal flow."
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
            Self::ToolRepair => Some(RuntimeRoadmapRolloutGate::new(
                TOOL_REPAIR_ROLLOUT_ENV,
                TOOL_REPAIR_ROLLOUT_CONFIG_PATH,
            )),
            Self::ProviderStreamNormalizer => Some(RuntimeRoadmapRolloutGate::new(
                PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV,
                PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH,
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
        project_runtime_roadmap_harness, runtime_roadmap_capability_catalog,
        runtime_roadmap_phase0_harness_fixtures, runtime_roadmap_phase0_harness_projection,
        RuntimeRoadmapCapability, RuntimeRoadmapDecision, RuntimeRoadmapEventType,
        RuntimeRoadmapHarnessFixture, RuntimeRoadmapHarnessValidationError,
        RuntimeRoadmapJournalEvent, RuntimeRoadmapReasonCode, RuntimeRoadmapRedactionBoundary,
        RuntimeRoadmapValidationError, ALL_RUNTIME_ROADMAP_CAPABILITIES,
        RUNTIME_ROADMAP_SCHEMA_VERSION,
    };
    use crate::feature_rollouts::{TOOL_REPAIR_ROLLOUT_CONFIG_PATH, TOOL_REPAIR_ROLLOUT_ENV};

    const PHASE0_HARNESS_FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/golden/runtime_roadmap_phase0_harness.json"
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
