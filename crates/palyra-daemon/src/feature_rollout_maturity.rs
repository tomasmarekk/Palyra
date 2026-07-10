//! Feature-rollout maturity matrix shared by daemon diagnostics and doctor.
//!
//! Rollout enablement answers whether a flag is on right now; this matrix
//! answers whether the capability is safe to promote, which owners and tests
//! gate it, and what still blocks activation.

mod manifest;

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fmt,
};

use palyra_common::feature_rollouts::{self, FeatureRolloutSetting, FeatureRolloutSource};
use serde_json::{json, Map, Value};

use crate::{
    config::FeatureRolloutsConfig,
    feature_usage::{FeatureUsageCapabilitySnapshot, FeatureUsageSnapshot},
};

pub(crate) const FEATURE_ROLLOUT_MATURITY_SCHEMA_VERSION: u32 = 2;
const FEATURE_ROLLOUT_MATURITY_LEGACY_SCHEMA_VERSION: u32 = 1;

const CORE_VISIBILITY_TESTS: &[&str] = &[
    "cargo test -p palyra-daemon --test current_state_inventory --locked",
    "cargo test -p palyra-daemon --test admin_surface --locked",
];
const RUNTIME_PREVIEW_TESTS: &[&str] = &[
    "cargo test -p palyra-daemon runtime_preview --locked",
    "cargo test -p palyra-daemon --test current_state_inventory --locked",
];
const EXECUTION_BACKEND_TESTS: &[&str] = &[
    "cargo test -p palyra-daemon execution_backend_parity --locked",
    "cargo test -p palyra-workerd remote_tool_kind_maps_backend_parity_tools --locked",
    "palyra qa validate --path qa/scenarios/execution_backends --json",
    "cargo test -p palyra-daemon --test current_state_inventory --locked",
    "bash scripts/test/run-critical-attack-scenarios.sh",
];
const MODEL_PROVIDER_TESTS: &[&str] = &[
    "cargo test -p palyra-model-providers --locked",
    "cargo test -p palyra-daemon provider_stream --locked",
    "cargo test -p palyra-daemon responses_failed_stream_event --locked",
];
const COMPACTION_TESTS: &[&str] = &[
    "cargo test -p palyra-daemon session_compaction --locked",
    "bash scripts/test/run-replay-gate.sh",
];
const SECURITY_TESTS: &[&str] =
    &["cargo test -p palyra-cli security --locked", "bash scripts/check-high-risk-patterns.sh"];

const DIAGNOSTICS_ACCEPTANCE: &[&str] = &[
    "rollout appears in /console/v1/diagnostics with maturity metadata",
    "rollout appears in palyra doctor --json when admin diagnostics are reachable",
    "current_state_inventory golden records maturity changes",
];
const RUNTIME_PREVIEW_ACCEPTANCE: &[&str] = &[
    "runtime preview controls expose activation blockers for the capability",
    "enabled mode requires the matching rollout flag",
    "golden inventory records the effective state",
];
const EXECUTION_BACKEND_ACCEPTANCE: &[&str] = &[
    "scenario pack covers process, filesystem, patch, artifact, tool-program, cancellation, cleanup, and unavailable-backend fallback cases",
    "backend advertises degraded outcome when disabled",
    "policy, attestation, and cleanup evidence are covered before production enablement",
];
const COMPACTION_ACCEPTANCE: &[&str] = &[
    "compaction path records replay-safe evidence",
    "provider-backed work degrades to local evidence when rollout is disabled",
];
const SECURITY_ACCEPTANCE: &[&str] = &[
    "security audit output remains redacted",
    "high-risk pattern scan covers new public posture fields",
];
const ROLLOUT_PROMOTION_GATE: &str =
    "default-on promotion requires owner acceptance, required tests, rollback metadata, and inventory golden update";

const NO_DEPRECATED_ALIASES: &[&str] = &[];
const NO_STABLE_DEPENDENCIES: &[FeatureRolloutFlag] = &[];

const DEFAULT_MIGRATION_NOTE: &str =
    "canonical flag only; no deprecated aliases are accepted for this rollout";

const OBSERVABILITY_EXPOSURE: &str =
    "operator diagnostics: /console/v1/diagnostics, /admin/v1/status, palyra doctor --json";
const INTERNAL_EXPOSURE: &str =
    "internal runtime flag; externally visible only through diagnostics and doctor";
const RUNTIME_PREVIEW_EXPOSURE: &str =
    "runtime preview controls plus operator diagnostics and doctor";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FeatureRolloutFlag {
    DynamicToolBuilder,
    ContextEngine,
    ExecutionBackendRemoteNode,
    ExecutionBackendNetworkedWorker,
    ExecutionBackendDocker,
    ExecutionBackendSshTunnel,
    SafetyBoundary,
    ExecutionGatePipelineV2,
    AgentHarnessRuntime,
    InlineRuntimeHooks,
    ToolResultMiddleware,
    SessionQueuePolicy,
    PruningPolicyMatrix,
    RetrievalDualPath,
    AuxiliaryExecutor,
    FlowOrchestration,
    DeliveryArbitration,
    ReplayCapture,
    NetworkedWorkers,
    ToolRepair,
    ProviderStreamNormalizer,
    ProviderRecovery,
    TerminalSessions,
    BrowserRescue,
    LspService,
    AdvisorFanout,
    AcpRuntime,
    ChannelTurnKernel,
    AgentPlanState,
    ObjectiveJudge,
    VerificationRuntime,
    ProgressDrafts,
    CompactionSafeguard,
    ProviderBackedEvidenceCompaction,
    AttackSurfaceAudit,
}

impl FeatureRolloutFlag {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DynamicToolBuilder => "dynamic_tool_builder",
            Self::ContextEngine => "context_engine",
            Self::ExecutionBackendRemoteNode => "execution_backend_remote_node",
            Self::ExecutionBackendNetworkedWorker => "execution_backend_networked_worker",
            Self::ExecutionBackendDocker => "execution_backend_docker",
            Self::ExecutionBackendSshTunnel => "execution_backend_ssh_tunnel",
            Self::SafetyBoundary => "safety_boundary",
            Self::ExecutionGatePipelineV2 => "execution_gate_pipeline_v2",
            Self::AgentHarnessRuntime => "agent_harness_runtime",
            Self::InlineRuntimeHooks => "inline_runtime_hooks",
            Self::ToolResultMiddleware => "tool_result_middleware",
            Self::SessionQueuePolicy => "session_queue_policy",
            Self::PruningPolicyMatrix => "pruning_policy_matrix",
            Self::RetrievalDualPath => "retrieval_dual_path",
            Self::AuxiliaryExecutor => "auxiliary_executor",
            Self::FlowOrchestration => "flow_orchestration",
            Self::DeliveryArbitration => "delivery_arbitration",
            Self::ReplayCapture => "replay_capture",
            Self::NetworkedWorkers => "networked_workers",
            Self::ToolRepair => "tool_repair",
            Self::ProviderStreamNormalizer => "provider_stream_normalizer",
            Self::ProviderRecovery => "provider_recovery",
            Self::TerminalSessions => "terminal_sessions",
            Self::BrowserRescue => "browser_rescue",
            Self::LspService => "lsp_service",
            Self::AdvisorFanout => "advisor_fanout",
            Self::AcpRuntime => "acp_runtime",
            Self::ChannelTurnKernel => "channel_turn_kernel",
            Self::AgentPlanState => "agent_plan_state",
            Self::ObjectiveJudge => "objective_judge",
            Self::VerificationRuntime => "verification_runtime",
            Self::ProgressDrafts => "progress_drafts",
            Self::CompactionSafeguard => "compaction_safeguard",
            Self::ProviderBackedEvidenceCompaction => "provider_backed_evidence_compaction",
            Self::AttackSurfaceAudit => "attack_surface_audit",
        }
    }

    const fn config_path(self) -> &'static str {
        match self {
            Self::DynamicToolBuilder => feature_rollouts::DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH,
            Self::ContextEngine => feature_rollouts::CONTEXT_ENGINE_ROLLOUT_CONFIG_PATH,
            Self::ExecutionBackendRemoteNode => {
                feature_rollouts::EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_CONFIG_PATH
            }
            Self::ExecutionBackendNetworkedWorker => {
                feature_rollouts::EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH
            }
            Self::ExecutionBackendDocker => {
                feature_rollouts::EXECUTION_BACKEND_DOCKER_ROLLOUT_CONFIG_PATH
            }
            Self::ExecutionBackendSshTunnel => {
                feature_rollouts::EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_CONFIG_PATH
            }
            Self::SafetyBoundary => feature_rollouts::SAFETY_BOUNDARY_ROLLOUT_CONFIG_PATH,
            Self::ExecutionGatePipelineV2 => {
                feature_rollouts::EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH
            }
            Self::AgentHarnessRuntime => {
                feature_rollouts::AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH
            }
            Self::InlineRuntimeHooks => feature_rollouts::INLINE_RUNTIME_HOOKS_ROLLOUT_CONFIG_PATH,
            Self::ToolResultMiddleware => {
                feature_rollouts::TOOL_RESULT_MIDDLEWARE_ROLLOUT_CONFIG_PATH
            }
            Self::SessionQueuePolicy => feature_rollouts::SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH,
            Self::PruningPolicyMatrix => {
                feature_rollouts::PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH
            }
            Self::RetrievalDualPath => feature_rollouts::RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH,
            Self::AuxiliaryExecutor => feature_rollouts::AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH,
            Self::FlowOrchestration => feature_rollouts::FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH,
            Self::DeliveryArbitration => feature_rollouts::DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH,
            Self::ReplayCapture => feature_rollouts::REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH,
            Self::NetworkedWorkers => feature_rollouts::NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH,
            Self::ToolRepair => feature_rollouts::TOOL_REPAIR_ROLLOUT_CONFIG_PATH,
            Self::ProviderStreamNormalizer => {
                feature_rollouts::PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH
            }
            Self::ProviderRecovery => feature_rollouts::PROVIDER_RECOVERY_ROLLOUT_CONFIG_PATH,
            Self::TerminalSessions => feature_rollouts::TERMINAL_SESSIONS_ROLLOUT_CONFIG_PATH,
            Self::BrowserRescue => feature_rollouts::BROWSER_RESCUE_ROLLOUT_CONFIG_PATH,
            Self::LspService => feature_rollouts::LSP_SERVICE_ROLLOUT_CONFIG_PATH,
            Self::AdvisorFanout => feature_rollouts::ADVISOR_FANOUT_ROLLOUT_CONFIG_PATH,
            Self::AcpRuntime => feature_rollouts::ACP_RUNTIME_ROLLOUT_CONFIG_PATH,
            Self::ChannelTurnKernel => feature_rollouts::CHANNEL_TURN_KERNEL_ROLLOUT_CONFIG_PATH,
            Self::AgentPlanState => feature_rollouts::AGENT_PLAN_STATE_ROLLOUT_CONFIG_PATH,
            Self::ObjectiveJudge => feature_rollouts::OBJECTIVE_JUDGE_ROLLOUT_CONFIG_PATH,
            Self::VerificationRuntime => feature_rollouts::VERIFICATION_RUNTIME_ROLLOUT_CONFIG_PATH,
            Self::ProgressDrafts => feature_rollouts::PROGRESS_DRAFTS_ROLLOUT_CONFIG_PATH,
            Self::CompactionSafeguard => feature_rollouts::COMPACTION_SAFEGUARD_ROLLOUT_CONFIG_PATH,
            Self::ProviderBackedEvidenceCompaction => {
                feature_rollouts::PROVIDER_BACKED_EVIDENCE_COMPACTION_ROLLOUT_CONFIG_PATH
            }
            Self::AttackSurfaceAudit => feature_rollouts::ATTACK_SURFACE_AUDIT_ROLLOUT_CONFIG_PATH,
        }
    }

    const fn env_var(self) -> &'static str {
        match self {
            Self::DynamicToolBuilder => feature_rollouts::DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            Self::ContextEngine => feature_rollouts::CONTEXT_ENGINE_ROLLOUT_ENV,
            Self::ExecutionBackendRemoteNode => {
                feature_rollouts::EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV
            }
            Self::ExecutionBackendNetworkedWorker => {
                feature_rollouts::EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV
            }
            Self::ExecutionBackendDocker => feature_rollouts::EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV,
            Self::ExecutionBackendSshTunnel => {
                feature_rollouts::EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV
            }
            Self::SafetyBoundary => feature_rollouts::SAFETY_BOUNDARY_ROLLOUT_ENV,
            Self::ExecutionGatePipelineV2 => {
                feature_rollouts::EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV
            }
            Self::AgentHarnessRuntime => feature_rollouts::AGENT_HARNESS_RUNTIME_ROLLOUT_ENV,
            Self::InlineRuntimeHooks => feature_rollouts::INLINE_RUNTIME_HOOKS_ROLLOUT_ENV,
            Self::ToolResultMiddleware => feature_rollouts::TOOL_RESULT_MIDDLEWARE_ROLLOUT_ENV,
            Self::SessionQueuePolicy => feature_rollouts::SESSION_QUEUE_POLICY_ROLLOUT_ENV,
            Self::PruningPolicyMatrix => feature_rollouts::PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
            Self::RetrievalDualPath => feature_rollouts::RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
            Self::AuxiliaryExecutor => feature_rollouts::AUXILIARY_EXECUTOR_ROLLOUT_ENV,
            Self::FlowOrchestration => feature_rollouts::FLOW_ORCHESTRATION_ROLLOUT_ENV,
            Self::DeliveryArbitration => feature_rollouts::DELIVERY_ARBITRATION_ROLLOUT_ENV,
            Self::ReplayCapture => feature_rollouts::REPLAY_CAPTURE_ROLLOUT_ENV,
            Self::NetworkedWorkers => feature_rollouts::NETWORKED_WORKERS_ROLLOUT_ENV,
            Self::ToolRepair => feature_rollouts::TOOL_REPAIR_ROLLOUT_ENV,
            Self::ProviderStreamNormalizer => {
                feature_rollouts::PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV
            }
            Self::ProviderRecovery => feature_rollouts::PROVIDER_RECOVERY_ROLLOUT_ENV,
            Self::TerminalSessions => feature_rollouts::TERMINAL_SESSIONS_ROLLOUT_ENV,
            Self::BrowserRescue => feature_rollouts::BROWSER_RESCUE_ROLLOUT_ENV,
            Self::LspService => feature_rollouts::LSP_SERVICE_ROLLOUT_ENV,
            Self::AdvisorFanout => feature_rollouts::ADVISOR_FANOUT_ROLLOUT_ENV,
            Self::AcpRuntime => feature_rollouts::ACP_RUNTIME_ROLLOUT_ENV,
            Self::ChannelTurnKernel => feature_rollouts::CHANNEL_TURN_KERNEL_ROLLOUT_ENV,
            Self::AgentPlanState => feature_rollouts::AGENT_PLAN_STATE_ROLLOUT_ENV,
            Self::ObjectiveJudge => feature_rollouts::OBJECTIVE_JUDGE_ROLLOUT_ENV,
            Self::VerificationRuntime => feature_rollouts::VERIFICATION_RUNTIME_ROLLOUT_ENV,
            Self::ProgressDrafts => feature_rollouts::PROGRESS_DRAFTS_ROLLOUT_ENV,
            Self::CompactionSafeguard => feature_rollouts::COMPACTION_SAFEGUARD_ROLLOUT_ENV,
            Self::ProviderBackedEvidenceCompaction => {
                feature_rollouts::PROVIDER_BACKED_EVIDENCE_COMPACTION_ROLLOUT_ENV
            }
            Self::AttackSurfaceAudit => feature_rollouts::ATTACK_SURFACE_AUDIT_ROLLOUT_ENV,
        }
    }

    fn setting(self, config: &FeatureRolloutsConfig) -> FeatureRolloutSetting {
        match self {
            Self::DynamicToolBuilder => config.dynamic_tool_builder,
            Self::ContextEngine => config.context_engine,
            Self::ExecutionBackendRemoteNode => config.execution_backend_remote_node,
            Self::ExecutionBackendNetworkedWorker => config.execution_backend_networked_worker,
            Self::ExecutionBackendDocker => config.execution_backend_docker,
            Self::ExecutionBackendSshTunnel => config.execution_backend_ssh_tunnel,
            Self::SafetyBoundary => config.safety_boundary,
            Self::ExecutionGatePipelineV2 => config.execution_gate_pipeline_v2,
            Self::AgentHarnessRuntime => config.agent_harness_runtime,
            Self::InlineRuntimeHooks => config.inline_runtime_hooks,
            Self::ToolResultMiddleware => config.tool_result_middleware,
            Self::SessionQueuePolicy => config.session_queue_policy,
            Self::PruningPolicyMatrix => config.pruning_policy_matrix,
            Self::RetrievalDualPath => config.retrieval_dual_path,
            Self::AuxiliaryExecutor => config.auxiliary_executor,
            Self::FlowOrchestration => config.flow_orchestration,
            Self::DeliveryArbitration => config.delivery_arbitration,
            Self::ReplayCapture => config.replay_capture,
            Self::NetworkedWorkers => config.networked_workers,
            Self::ToolRepair => config.tool_repair,
            Self::ProviderStreamNormalizer => config.provider_stream_normalizer,
            Self::ProviderRecovery => config.provider_recovery,
            Self::TerminalSessions => config.terminal_sessions,
            Self::BrowserRescue => config.browser_rescue,
            Self::LspService => config.lsp_service,
            Self::AdvisorFanout => config.advisor_fanout,
            Self::AcpRuntime => config.acp_runtime,
            Self::ChannelTurnKernel => config.channel_turn_kernel,
            Self::AgentPlanState => config.agent_plan_state,
            Self::ObjectiveJudge => config.objective_judge,
            Self::VerificationRuntime => config.verification_runtime,
            Self::ProgressDrafts => config.progress_drafts,
            Self::CompactionSafeguard => config.compaction_safeguard,
            Self::ProviderBackedEvidenceCompaction => config.provider_backed_evidence_compaction,
            Self::AttackSurfaceAudit => config.attack_surface_audit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeatureRolloutMaturity {
    Scaffold,
    PreviewOnly,
    GatedProduction,
    Stable,
    Deprecated,
    Blocked,
}

impl FeatureRolloutMaturity {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Scaffold => "scaffold",
            Self::PreviewOnly => "preview_only",
            Self::GatedProduction => "gated_production",
            Self::Stable => "stable",
            Self::Deprecated => "deprecated",
            Self::Blocked => "blocked",
        }
    }

    pub(crate) fn parse(raw: &str) -> Result<Self, FeatureRolloutMaturityParseError> {
        let normalized = raw.trim().to_ascii_lowercase().replace(['-', ' '], "_");
        match normalized.as_str() {
            "scaffold" => Ok(Self::Scaffold),
            "preview_only" => Ok(Self::PreviewOnly),
            "gated_production" => Ok(Self::GatedProduction),
            "stable" => Ok(Self::Stable),
            "deprecated" => Ok(Self::Deprecated),
            "blocked" => Ok(Self::Blocked),
            _ => Err(FeatureRolloutMaturityParseError { value: raw.trim().to_owned() }),
        }
    }

    const fn default_enable_allowed(self) -> bool {
        matches!(self, Self::Stable)
    }

    const fn transition_criteria(self) -> &'static str {
        match self {
            Self::Scaffold => {
                "keep default-off; add diagnostics, fixtures, owner, rollback, and required tests before preview"
            }
            Self::PreviewOnly => {
                "keep default-off; require explicit rollout flag, owner acceptance, and targeted acceptance tests"
            }
            Self::GatedProduction => {
                "keep explicit gate; require green release hardening evidence before stable promotion"
            }
            Self::Stable => "eligible for default-on only after release hardening and inventory drift review",
            Self::Deprecated => "keep disabled by default and document replacement or removal path",
            Self::Blocked => "cannot be enabled by default until listed blockers are cleared",
        }
    }
}

impl serde::Serialize for FeatureRolloutMaturity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

const FEATURE_ROLLOUT_MATURITY_STATES: &[FeatureRolloutMaturity] = &[
    FeatureRolloutMaturity::Scaffold,
    FeatureRolloutMaturity::PreviewOnly,
    FeatureRolloutMaturity::GatedProduction,
    FeatureRolloutMaturity::Stable,
    FeatureRolloutMaturity::Deprecated,
    FeatureRolloutMaturity::Blocked,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeatureRolloutMaturityParseError {
    value: String,
}

impl fmt::Display for FeatureRolloutMaturityParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "feature rollout maturity must be one of: scaffold, preview_only, gated_production, stable, deprecated, blocked; got '{}'",
            self.value
        )
    }
}

impl Error for FeatureRolloutMaturityParseError {}

#[derive(Debug, Clone, Copy)]
struct FeatureRolloutDescriptor {
    flag: FeatureRolloutFlag,
    owner_component: &'static str,
    maturity: FeatureRolloutMaturity,
    required_tests: &'static [&'static str],
    public_api_exposure: &'static str,
    activation_blockers: &'static [&'static str],
    acceptance_criteria: &'static [&'static str],
    deprecated_aliases: &'static [&'static str],
    migration_note: &'static str,
    stable_dependencies: &'static [FeatureRolloutFlag],
}

impl FeatureRolloutDescriptor {
    fn to_diagnostics_value(
        self,
        config: &FeatureRolloutsConfig,
        usage: &FeatureUsageSnapshot,
        promotion: manifest::ResolvedFeatureRolloutPromotion<'_>,
    ) -> Value {
        let setting = self.flag.setting(config);
        let activation_authoritative = rollout_activation_authoritative(promotion.rollout);
        let effective_enabled = activation_authoritative.then_some(setting.enabled);
        let mut activation_blockers = Vec::new();
        let inactive_reason = rollout_inactive_reason(
            self.flag,
            setting,
            promotion.rollout,
            activation_authoritative,
        );
        if let Some(blocker) = rollout_configuration_blocker(
            self.flag,
            setting,
            promotion.rollout,
            activation_authoritative,
        ) {
            activation_blockers.push(blocker);
        }
        activation_blockers
            .extend(self.activation_blockers.iter().map(|value| (*value).to_owned()));
        activation_blockers.extend(promotion.rollout.promotion_blockers.iter().cloned());
        let runtime_usage = runtime_usage_projection(
            self.flag.as_str(),
            promotion.rollout.execution_completeness,
            effective_enabled.unwrap_or(false),
            usage,
        );

        json!({
            "enabled": setting.enabled,
            "source": setting.source,
            "rollout_activation": {
                "configured_enabled": setting.enabled,
                "source": setting.source,
                "authoritative": activation_authoritative,
                "effective_enabled": effective_enabled,
                "effective_posture": effective_enabled
                    .map_or("not_authoritative", |_| rollout_default_posture(setting)),
                "reason_code": rollout_activation_reason_code(effective_enabled),
            },
            "config_path": self.flag.config_path(),
            "env_var": self.flag.env_var(),
            "default_posture": rollout_default_posture(setting),
            "rollback_knob": rollout_rollback_knob(self.flag, activation_authoritative),
            "maturity": self.maturity,
            "contract_availability": promotion.rollout.contract_availability,
            "execution_completeness": promotion.rollout.execution_completeness,
            "promotion_state": promotion.rollout.promotion_state,
            "support_maturity": promotion.rollout.support_maturity,
            "lifecycle": promotion.rollout.lifecycle,
            "runtime_usage": runtime_usage,
            "owner_component": self.owner_component,
            "required_tests": self.required_tests,
            "public_api_exposure": self.public_api_exposure,
            "activation_blockers": activation_blockers,
            "acceptance_criteria": self.acceptance_criteria,
            "promotion_gate": {
                "default_enable_allowed": promotion.rollout.promotion_state == manifest::PromotionState::Stable,
                "default_enable_blockers": self.default_enable_blockers(),
                "transition_criteria": self.maturity.transition_criteria(),
                "test_coverage_marker": if self.required_tests.is_empty() || self.acceptance_criteria.is_empty() {
                    "missing_required_test_or_acceptance_criteria"
                } else {
                    "required_tests_and_acceptance_criteria_present"
                },
                "release_gate": ROLLOUT_PROMOTION_GATE,
            },
            "promotion_manifest": {
                "schema_version": manifest::FEATURE_ROLLOUT_PROMOTION_SCHEMA_VERSION,
                "schema_id": manifest::FEATURE_ROLLOUT_PROMOTION_SCHEMA_ID,
                "evidence_profile": promotion.rollout.evidence_profile,
                "required_test_refs": promotion.evidence.required_test_refs,
                "sli": promotion.evidence.sli,
                "rollback": promotion.evidence.rollback,
                "compatibility_commitment": promotion.evidence.compatibility_commitment,
                "legacy_removal_condition": promotion.evidence.legacy_removal_condition,
                "direct_hot_path_test_ref": promotion.evidence.direct_hot_path_test_ref,
                "no_hidden_fallback_test_ref": promotion.evidence.no_hidden_fallback_test_ref,
                "replacement": promotion.rollout.replacement,
                "removal_date": promotion.rollout.removal_date,
                "removal_condition": promotion.rollout.removal_condition,
                "shadow_side_effect_posture": promotion.rollout.shadow_side_effect_posture,
            },
            "deprecated_aliases": self.deprecated_aliases,
            "migration_note": self.migration_note,
            "inactive_reason": inactive_reason,
        })
    }

    fn default_enable_blockers(self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.maturity.default_enable_allowed() {
            blockers.push("maturity state does not allow default-on production rollout");
        }
        if self.required_tests.is_empty() {
            blockers.push("required_tests must be populated before promotion");
        }
        if self.acceptance_criteria.is_empty() {
            blockers.push("acceptance_criteria must be populated before promotion");
        }
        blockers
    }
}

fn rollout_activation_authoritative(rollout: &manifest::FeatureRolloutPromotion) -> bool {
    rollout.lifecycle == manifest::RolloutLifecycle::Active
        && rollout.promotion_state != manifest::PromotionState::ContractOnly
}

fn rollout_inactive_reason(
    flag: FeatureRolloutFlag,
    setting: FeatureRolloutSetting,
    rollout: &manifest::FeatureRolloutPromotion,
    activation_authoritative: bool,
) -> Option<String> {
    match rollout.lifecycle {
        manifest::RolloutLifecycle::Deprecated => Some(format!(
            "{} is deprecated; migrate to {} instead of enabling the retained compatibility flag",
            flag.as_str(),
            rollout.replacement.as_deref().unwrap_or("the supported replacement")
        )),
        manifest::RolloutLifecycle::Retired => Some(format!(
            "{} is retired and cannot be activated; use {}",
            flag.as_str(),
            rollout.replacement.as_deref().unwrap_or("a supported capability")
        )),
        manifest::RolloutLifecycle::Active if !activation_authoritative => Some(format!(
            "{} is contract_only; its accepted rollout setting is non-authoritative until a reviewed runtime activation boundary is wired",
            flag.as_str()
        )),
        manifest::RolloutLifecycle::Active if !setting.enabled => Some(format!(
            "{} is disabled from {:?}; activation remains blocked until the listed tests and owner acceptance pass",
            flag.as_str(),
            feature_rollout_source_as_str(setting.source)
        )),
        manifest::RolloutLifecycle::Active => None,
    }
}

fn rollout_configuration_blocker(
    flag: FeatureRolloutFlag,
    setting: FeatureRolloutSetting,
    rollout: &manifest::FeatureRolloutPromotion,
    activation_authoritative: bool,
) -> Option<String> {
    match rollout.lifecycle {
        manifest::RolloutLifecycle::Deprecated => Some(format!(
            "Do not enable deprecated {}; migrate to {} and satisfy its removal condition.",
            flag.as_str(),
            rollout.replacement.as_deref().unwrap_or("the supported replacement")
        )),
        manifest::RolloutLifecycle::Retired => Some(format!(
            "Retired capability {} is unavailable; use {}.",
            flag.as_str(),
            rollout.replacement.as_deref().unwrap_or("a supported capability")
        )),
        manifest::RolloutLifecycle::Active if !activation_authoritative => Some(format!(
            "{} is contract_only; {} and {} are accepted for compatibility but cannot activate runtime behavior.",
            flag.as_str(),
            flag.config_path(),
            flag.env_var()
        )),
        manifest::RolloutLifecycle::Active if !setting.enabled => Some(format!(
            "Enable {} or {} after required tests and owner acceptance pass.",
            flag.config_path(),
            flag.env_var()
        )),
        manifest::RolloutLifecycle::Active => None,
    }
}

fn rollout_rollback_knob(flag: FeatureRolloutFlag, activation_authoritative: bool) -> Value {
    if activation_authoritative {
        return json!({
            "applicable": true,
            "env_var": flag.env_var(),
            "config_path": flag.config_path(),
            "safe_default": "disabled",
            "operator_action": format!(
                "unset {} and remove or set {} = false",
                flag.env_var(),
                flag.config_path()
            ),
            "reason_code": "feature_rollout.rollback_by_disable",
        });
    }

    json!({
        "applicable": false,
        "env_var": flag.env_var(),
        "config_path": flag.config_path(),
        "safe_default": "disabled",
        "operator_action": "the accepted rollout setting does not control runtime behavior; roll back the runtime implementation independently",
        "reason_code": "feature_rollout.rollback_not_authoritative",
    })
}

const fn rollout_activation_reason_code(effective_enabled: Option<bool>) -> &'static str {
    match effective_enabled {
        Some(true) => "feature_rollout.activation_enabled",
        Some(false) => "feature_rollout.activation_disabled",
        None => "feature_rollout.activation_not_authoritative",
    }
}

const fn rollout_default_posture(setting: FeatureRolloutSetting) -> &'static str {
    match (setting.source, setting.enabled) {
        (FeatureRolloutSource::Default, false) => "default_off",
        (FeatureRolloutSource::Default, true) => "default_on",
        (FeatureRolloutSource::Config, false) => "disabled_by_config",
        (FeatureRolloutSource::Config, true) => "enabled_by_config",
        (FeatureRolloutSource::Env, false) => "disabled_by_env",
        (FeatureRolloutSource::Env, true) => "enabled_by_env",
    }
}

const fn feature_rollout_source_as_str(source: FeatureRolloutSource) -> &'static str {
    match source {
        FeatureRolloutSource::Default => "default",
        FeatureRolloutSource::Config => "config",
        FeatureRolloutSource::Env => "env",
    }
}

fn builtin_promotion_manifest() -> &'static manifest::FeatureRolloutPromotionManifest {
    let expected = FEATURE_ROLLOUT_DESCRIPTORS
        .iter()
        .map(|descriptor| (descriptor.flag.as_str(), descriptor.owner_component))
        .collect::<Vec<_>>();
    manifest::builtin_feature_rollout_promotion_manifest(expected.as_slice())
        .expect("built-in rollout promotion manifest is validated during config loading")
}

fn runtime_usage_projection(
    capability: &str,
    execution_completeness: manifest::ExecutionCompleteness,
    rollout_enabled: bool,
    usage: &FeatureUsageSnapshot,
) -> Value {
    let capability_usage =
        usage.capabilities.iter().find(|snapshot| snapshot.capability.as_str() == capability);
    let qualified_hot_path = capability_usage.is_some_and(|snapshot| {
        rollout_enabled
            && execution_completeness == manifest::ExecutionCompleteness::Complete
            && snapshot.terminal_direct_runs > 0
            && snapshot.active_runs == 0
            && snapshot.fallback_runs == 0
            && snapshot.mixed_runs == 0
            && !snapshot.window_truncated
            && snapshot.dropped_observations == 0
    });
    let qualification_reason_code = feature_usage_qualification_reason(
        execution_completeness,
        rollout_enabled,
        capability_usage,
        qualified_hot_path,
    );

    match capability_usage {
        Some(snapshot) => json!({
            "instrumented": true,
            "observation_scope": usage.observation_scope,
            "resets_on_restart": usage.resets_on_restart,
            "retained_run_capacity": usage.capacity,
            "retained_run_count": snapshot.observed_runs,
            "active_run_count": snapshot.active_runs,
            "terminal_run_count": snapshot.terminal_observed_runs,
            "window_truncated": snapshot.window_truncated,
            "evicted_run_count": snapshot.evicted_runs,
            "dropped_observation_count": snapshot.dropped_observations,
            "dropped_observation_reason_counts": snapshot.dropped_observation_reason_counts,
            "terminal_fence_window_truncated": usage.terminal_fence_window_truncated,
            "evicted_terminal_fence_count": usage.evicted_terminal_fences,
            "observed_unique_runs": snapshot.observed_runs,
            "direct_unique_runs": snapshot.direct_runs,
            "fallback_unique_runs": snapshot.fallback_runs,
            "mixed_path_unique_runs": snapshot.mixed_runs,
            "terminal_observed_unique_runs": snapshot.terminal_observed_runs,
            "terminal_direct_unique_runs": snapshot.terminal_direct_runs,
            "terminal_fallback_unique_runs": snapshot.terminal_fallback_runs,
            "terminal_mixed_path_unique_runs": snapshot.terminal_mixed_runs,
            "fallback_reason_counts": snapshot.reason_counts,
            "qualified_hot_path": qualified_hot_path,
            "qualification_reason_code": qualification_reason_code,
        }),
        None => json!({
            "instrumented": false,
            "observation_scope": usage.observation_scope,
            "resets_on_restart": usage.resets_on_restart,
            "retained_run_capacity": usage.capacity,
            "window_truncated": false,
            "evicted_run_count": 0,
            "dropped_observation_count": 0,
            "dropped_observation_reason_counts": {},
            "terminal_fence_window_truncated": usage.terminal_fence_window_truncated,
            "evicted_terminal_fence_count": usage.evicted_terminal_fences,
            "observed_unique_runs": null,
            "direct_unique_runs": null,
            "fallback_unique_runs": null,
            "mixed_path_unique_runs": null,
            "terminal_observed_unique_runs": null,
            "terminal_direct_unique_runs": null,
            "terminal_fallback_unique_runs": null,
            "terminal_mixed_path_unique_runs": null,
            "fallback_reason_counts": null,
            "qualified_hot_path": false,
            "qualification_reason_code": qualification_reason_code,
        }),
    }
}

fn feature_usage_qualification_reason(
    execution_completeness: manifest::ExecutionCompleteness,
    rollout_enabled: bool,
    capability_usage: Option<&FeatureUsageCapabilitySnapshot>,
    qualified_hot_path: bool,
) -> &'static str {
    let Some(capability_usage) = capability_usage else {
        return "feature_usage.not_instrumented";
    };
    if execution_completeness != manifest::ExecutionCompleteness::Complete {
        return "feature_usage.execution_incomplete";
    }
    if !rollout_enabled {
        return if capability_usage.direct_runs > 0 {
            "feature_usage.activation_mismatch"
        } else {
            "feature_usage.rollout_inactive"
        };
    }
    if capability_usage.window_truncated {
        return "feature_usage.retained_window_truncated";
    }
    if capability_usage.dropped_observations > 0 {
        return "feature_usage.observation_dropped";
    }
    if capability_usage.fallback_runs > 0 || capability_usage.mixed_runs > 0 {
        return "feature_usage.fallback_observed";
    }
    if capability_usage.active_runs > 0 {
        return "feature_usage.active_run_observed";
    }
    if capability_usage.terminal_direct_runs == 0 {
        return "feature_usage.terminal_direct_run_not_observed";
    }
    if qualified_hot_path {
        "feature_usage.qualified_hot_path"
    } else {
        "feature_usage.not_qualified"
    }
}

const FEATURE_ROLLOUT_DESCRIPTORS: &[FeatureRolloutDescriptor] = &[
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::DynamicToolBuilder,
        owner_component: "skills/tool runtime",
        maturity: FeatureRolloutMaturity::Scaffold,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "builder output is not yet covered by signed skill artifact compatibility tests",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ContextEngine,
        owner_component: "application/context_engine",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "context assembly traces must stay redacted and replay-compatible before production rollout",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ExecutionBackendRemoteNode,
        owner_component: "execution backends",
        maturity: FeatureRolloutMaturity::Blocked,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "remote-node runner contract is not production-backed by attestation and cleanup evidence",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ExecutionBackendNetworkedWorker,
        owner_component: "workerd/execution backends",
        maturity: FeatureRolloutMaturity::Blocked,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &[
            "networked worker execution requires worker attestation and policy-bound remote tool subsets",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ExecutionBackendDocker,
        owner_component: "execution backends",
        maturity: FeatureRolloutMaturity::Blocked,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "real Docker execution still needs operator-enabled runtime coverage before production promotion",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ExecutionBackendSshTunnel,
        owner_component: "execution backends",
        maturity: FeatureRolloutMaturity::Blocked,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "SSH worker RPC transport trust chain still needs live tunnel coverage before production promotion",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::SafetyBoundary,
        owner_component: "safety",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: SECURITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "safety transforms must preserve prompt-injection and secret-redaction regression coverage",
        ],
        acceptance_criteria: SECURITY_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ExecutionGatePipelineV2,
        owner_component: "execution gate",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "execution gate v2 must keep denial and degraded outcomes byte-stable before production",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AgentHarnessRuntime,
        owner_component: "application/agent_harness",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "harness selection, callbacks, transcript mirroring, and lifecycle attempts must stay host-owned before production routing",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::InlineRuntimeHooks,
        owner_component: "hooks/runtime loop",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "inline hook invocation must preserve timeout, panic, policy, approval, and audit fail-closed behavior",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ToolResultMiddleware,
        owner_component: "tool runtime",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "middleware may only preserve or downgrade model-visible tool results and must retain audit artifacts under host ownership",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::SessionQueuePolicy,
        owner_component: "session lifecycle",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &["queue depth and merge-window limits must remain bounded in preview controls"],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::PruningPolicyMatrix,
        owner_component: "memory/context pruning",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &[
            "manual apply and token-savings thresholds must be visible before production pruning",
        ],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::RetrievalDualPath,
        owner_component: "memory/retrieval",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &[
            "branch timeout and prompt budget limits must stay bounded in runtime preview controls",
        ],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AuxiliaryExecutor,
        owner_component: "agent delegation",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &["auxiliary task budget and count limits must remain enforced"],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::FlowOrchestration,
        owner_component: "flow orchestration",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &["flow cancellation gates and retry budgets must be replay-visible"],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::DeliveryArbitration,
        owner_component: "channel delivery",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &[
            "delivery arbitration depends on active flow orchestration and bounded suppression",
        ],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ReplayCapture,
        owner_component: "replay",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: RUNTIME_PREVIEW_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &["runtime decision capture must respect replay redaction limits"],
        acceptance_criteria: RUNTIME_PREVIEW_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::NetworkedWorkers,
        owner_component: "workerd/execution backends",
        maturity: FeatureRolloutMaturity::Blocked,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: RUNTIME_PREVIEW_EXPOSURE,
        activation_blockers: &[
            "networked workers also require feature_rollouts.execution_backend_networked_worker",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: &[FeatureRolloutFlag::ExecutionBackendNetworkedWorker],
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ToolRepair,
        owner_component: "run stream/tool repair",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "tool repair must keep proposed fixes replay-safe and operator-auditable",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ProviderStreamNormalizer,
        owner_component: "model provider streaming",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: MODEL_PROVIDER_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "stream normalization must keep malformed chunks, idle timeouts, duplicate deltas, late usage, and public failed SSE frames covered",
        ],
        acceptance_criteria: &[
            "normalizer emits hash-only audit events for malformed or recovered provider SSE frames",
            "unrecoverable provider streams emit one terminal failed event on public SSE APIs",
            "diagnostics expose the rollout maturity and activation blockers",
        ],
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ProviderRecovery,
        owner_component: "model provider recovery",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: MODEL_PROVIDER_TESTS,
        public_api_exposure: INTERNAL_EXPOSURE,
        activation_blockers: &[
            "recovery classifiers must keep retries bounded, idempotent, and visible through replay-safe reason codes",
        ],
        acceptance_criteria: &[
            "provider recovery diagnostics expose retryability and terminal failure reasons",
            "auth failover and malformed-output recovery stay bounded by provider policy",
            "raw provider payloads are redacted before diagnostics and support bundle export",
        ],
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: &[FeatureRolloutFlag::ProviderStreamNormalizer],
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::TerminalSessions,
        owner_component: "process runtime",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: EXECUTION_BACKEND_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "persistent terminal sessions must keep process handles, cwd, env, sudo posture, and cleanup evidence bounded",
        ],
        acceptance_criteria: EXECUTION_BACKEND_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::BrowserRescue,
        owner_component: "browserd/browser rescue",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "browser rescue must stay policy-gated and must not export raw screenshots or CDP payloads without redaction",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::LspService,
        owner_component: "code intelligence",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "LSP lifecycle must keep diagnostics bounded, workspace-scoped, and restart-safe before model-visible tools consume it",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AdvisorFanout,
        owner_component: "advisors",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "advisor fanout must enforce budget governance, trace attribution, and non-authoritative finalization",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AcpRuntime,
        owner_component: "acp runtime",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "ACP actors must route permission, session, replay, and compaction handoff through host-owned control-plane boundaries",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ChannelTurnKernel,
        owner_component: "channel router",
        maturity: FeatureRolloutMaturity::Deprecated,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "the rollout flag is ignored by the always-on production path; migrate diagnostics consumers to always_on_channel_turn_runtime before removing the deprecated flag",
        ],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AgentPlanState,
        owner_component: "agent plan state",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["model-visible plan state must remain scoped to diagnostic-safe fields"],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ObjectiveJudge,
        owner_component: "objective judge",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["judge outcomes must stay advisory and replay-visible until acceptance gates land"],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::VerificationRuntime,
        owner_component: "verification runtime",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["verification evidence must remain redacted and durable before stable rollout"],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ProgressDrafts,
        owner_component: "progress drafts",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["draft projection must not expose hidden transcript or secret material"],
        acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::CompactionSafeguard,
        owner_component: "session compaction",
        maturity: FeatureRolloutMaturity::GatedProduction,
        required_tests: COMPACTION_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["compaction decisions must be replay-safe and expose bounded evidence"],
        acceptance_criteria: COMPACTION_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::ProviderBackedEvidenceCompaction,
        owner_component: "session compaction",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: COMPACTION_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "provider-backed compaction must degrade to local evidence when provider calls fail",
        ],
        acceptance_criteria: COMPACTION_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: &[FeatureRolloutFlag::CompactionSafeguard],
    },
    FeatureRolloutDescriptor {
        flag: FeatureRolloutFlag::AttackSurfaceAudit,
        owner_component: "security audit",
        maturity: FeatureRolloutMaturity::PreviewOnly,
        required_tests: SECURITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &["attack-surface audit output must remain redacted and policy-aligned"],
        acceptance_criteria: SECURITY_ACCEPTANCE,
        deprecated_aliases: NO_DEPRECATED_ALIASES,
        migration_note: DEFAULT_MIGRATION_NOTE,
        stable_dependencies: NO_STABLE_DEPENDENCIES,
    },
];

pub(crate) fn validate_builtin_feature_rollout_maturity_matrix(
) -> Result<(), FeatureRolloutMaturityValidationError> {
    validate_feature_rollout_maturity_descriptors(FEATURE_ROLLOUT_DESCRIPTORS)?;
    let expected = FEATURE_ROLLOUT_DESCRIPTORS
        .iter()
        .map(|descriptor| (descriptor.flag.as_str(), descriptor.owner_component))
        .collect::<Vec<_>>();
    let promotion_manifest = manifest::builtin_feature_rollout_promotion_manifest(&expected)
        .map_err(|error| FeatureRolloutMaturityValidationError::PromotionManifestInvalid {
            reason: error.to_string(),
        })?;
    for descriptor in FEATURE_ROLLOUT_DESCRIPTORS {
        let promotion = promotion_manifest.rollout(descriptor.flag.as_str()).ok_or(
            FeatureRolloutMaturityValidationError::MissingPromotionManifestEntry {
                flag: descriptor.flag.as_str(),
            },
        )?;
        let projected_maturity = legacy_maturity_projection(promotion);
        if descriptor.maturity != projected_maturity {
            return Err(FeatureRolloutMaturityValidationError::LegacyProjectionMismatch {
                flag: descriptor.flag.as_str(),
                descriptor_maturity: descriptor.maturity.as_str(),
                manifest_maturity: projected_maturity.as_str(),
            });
        }
    }
    Ok(())
}

fn legacy_maturity_projection(
    promotion: &manifest::FeatureRolloutPromotion,
) -> FeatureRolloutMaturity {
    if matches!(
        promotion.lifecycle,
        manifest::RolloutLifecycle::Deprecated | manifest::RolloutLifecycle::Retired
    ) {
        return FeatureRolloutMaturity::Deprecated;
    }
    if promotion.contract_availability == manifest::ContractAvailability::Blocked {
        return FeatureRolloutMaturity::Blocked;
    }
    match promotion.promotion_state {
        manifest::PromotionState::Stable => FeatureRolloutMaturity::Stable,
        manifest::PromotionState::GatedProduction => FeatureRolloutMaturity::GatedProduction,
        manifest::PromotionState::ContractOnly
            if promotion.contract_availability
                == manifest::ContractAvailability::DescriptorOnly
                && promotion.execution_completeness
                    == manifest::ExecutionCompleteness::NotImplemented
                && promotion.support_maturity == manifest::SupportMaturity::Unsupported =>
        {
            FeatureRolloutMaturity::Scaffold
        }
        manifest::PromotionState::ContractOnly
        | manifest::PromotionState::Shadow
        | manifest::PromotionState::Canary => FeatureRolloutMaturity::PreviewOnly,
    }
}

fn validate_feature_rollout_maturity_descriptors(
    descriptors: &[FeatureRolloutDescriptor],
) -> Result<(), FeatureRolloutMaturityValidationError> {
    if descriptors.is_empty() {
        return Err(FeatureRolloutMaturityValidationError::EmptyMatrix);
    }
    let mut seen = HashSet::new();
    for descriptor in descriptors {
        FeatureRolloutMaturity::parse(descriptor.maturity.as_str())
            .expect("built-in feature rollout maturity names are canonical");
        if !seen.insert(descriptor.flag) {
            return Err(FeatureRolloutMaturityValidationError::DuplicateFlag(
                descriptor.flag.as_str(),
            ));
        }
        if descriptor.required_tests.is_empty() {
            return Err(FeatureRolloutMaturityValidationError::MissingRequiredTests {
                flag: descriptor.flag.as_str(),
            });
        }
        if descriptor.activation_blockers.is_empty() {
            return Err(FeatureRolloutMaturityValidationError::MissingActivationBlockers {
                flag: descriptor.flag.as_str(),
            });
        }
        if descriptor.maturity == FeatureRolloutMaturity::Stable
            && descriptor.acceptance_criteria.is_empty()
        {
            return Err(FeatureRolloutMaturityValidationError::StableWithoutAcceptance {
                flag: descriptor.flag.as_str(),
            });
        }
        if descriptor.acceptance_criteria.is_empty() {
            return Err(FeatureRolloutMaturityValidationError::MissingAcceptanceCriteria {
                flag: descriptor.flag.as_str(),
            });
        }
        if descriptor.maturity == FeatureRolloutMaturity::Stable {
            for dependency in descriptor.stable_dependencies {
                let dependency_path = dependency.config_path();
                let blocker_mentions_dependency = descriptor
                    .activation_blockers
                    .iter()
                    .any(|blocker| blocker.contains(dependency_path));
                if !blocker_mentions_dependency {
                    return Err(
                        FeatureRolloutMaturityValidationError::StableDependencyWithoutBlocker {
                            flag: descriptor.flag.as_str(),
                            dependency: dependency.as_str(),
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FeatureRolloutMaturityValidationError {
    EmptyMatrix,
    DuplicateFlag(&'static str),
    MissingRequiredTests {
        flag: &'static str,
    },
    MissingActivationBlockers {
        flag: &'static str,
    },
    MissingAcceptanceCriteria {
        flag: &'static str,
    },
    StableWithoutAcceptance {
        flag: &'static str,
    },
    StableDependencyWithoutBlocker {
        flag: &'static str,
        dependency: &'static str,
    },
    PromotionManifestInvalid {
        reason: String,
    },
    MissingPromotionManifestEntry {
        flag: &'static str,
    },
    LegacyProjectionMismatch {
        flag: &'static str,
        descriptor_maturity: &'static str,
        manifest_maturity: &'static str,
    },
}

impl fmt::Display for FeatureRolloutMaturityValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMatrix => write!(f, "feature rollout maturity matrix must not be empty"),
            Self::DuplicateFlag(flag) => {
                write!(f, "feature rollout maturity matrix contains duplicate flag {flag}")
            }
            Self::MissingRequiredTests { flag } => {
                write!(f, "feature rollout {flag} must define required tests")
            }
            Self::MissingActivationBlockers { flag } => {
                write!(f, "feature rollout {flag} must define activation blockers")
            }
            Self::MissingAcceptanceCriteria { flag } => {
                write!(f, "feature rollout {flag} must define acceptance criteria")
            }
            Self::StableWithoutAcceptance { flag } => {
                write!(f, "feature rollout {flag} cannot be stable without acceptance criteria")
            }
            Self::StableDependencyWithoutBlocker { flag, dependency } => write!(
                f,
                "feature rollout {flag} cannot be stable while depending on {dependency} without an explicit blocker"
            ),
            Self::PromotionManifestInvalid { reason } => {
                write!(f, "feature rollout promotion manifest is invalid: {reason}")
            }
            Self::MissingPromotionManifestEntry { flag } => {
                write!(f, "feature rollout {flag} is missing promotion manifest evidence")
            }
            Self::LegacyProjectionMismatch {
                flag,
                descriptor_maturity,
                manifest_maturity,
            } => write!(
                f,
                "feature rollout {flag} projects legacy maturity {manifest_maturity} but descriptor declares {descriptor_maturity}"
            ),
        }
    }
}

impl Error for FeatureRolloutMaturityValidationError {}

pub(crate) fn build_feature_rollout_diagnostics(
    config: &FeatureRolloutsConfig,
    usage: &FeatureUsageSnapshot,
) -> Value {
    let promotion_manifest = builtin_promotion_manifest();
    let promotions = promotion_manifest
        .resolved_rollouts()
        .expect("validated promotion manifest resolves every evidence profile");
    let mut map = Map::new();
    for descriptor in FEATURE_ROLLOUT_DESCRIPTORS {
        let promotion = promotions
            .iter()
            .find(|promotion| promotion.rollout.capability == descriptor.flag.as_str())
            .copied()
            .expect("validated promotion manifest contains every runtime rollout");
        map.insert(
            descriptor.flag.as_str().to_owned(),
            descriptor.to_diagnostics_value(config, usage, promotion),
        );
    }
    Value::Object(map)
}

/// Builds the additive maturity model published under `feature_rollout_maturity_v2`.
pub(crate) fn build_feature_rollout_maturity_summary_v2(
    config: &FeatureRolloutsConfig,
    usage: &FeatureUsageSnapshot,
) -> Value {
    let mut summary = build_feature_rollout_maturity_summary_base(config, Some(usage));
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "release_dashboard_contract".to_owned(),
            build_release_dashboard_contract(config, Some(usage)),
        );
    }
    summary
}

/// Builds the frozen summary shape retained under `feature_rollout_maturity` for v1 clients.
pub(crate) fn build_feature_rollout_maturity_summary_v1(config: &FeatureRolloutsConfig) -> Value {
    let mut summary = build_feature_rollout_maturity_summary_v1_base(config);
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "release_dashboard_contract".to_owned(),
            build_release_dashboard_contract_v1(config),
        );
    }
    summary
}

fn resolved_promotion<'a>(
    promotion_manifest: &'a manifest::FeatureRolloutPromotionManifest,
    capability: &str,
) -> manifest::ResolvedFeatureRolloutPromotion<'a> {
    let rollout = promotion_manifest
        .rollout(capability)
        .expect("validated promotion manifest contains every runtime rollout");
    let evidence = promotion_manifest
        .evidence_profiles
        .get(rollout.evidence_profile.as_str())
        .expect("validated promotion manifest resolves every evidence profile");
    manifest::ResolvedFeatureRolloutPromotion { rollout, evidence }
}

fn build_feature_rollout_maturity_summary_base(
    config: &FeatureRolloutsConfig,
    usage: Option<&FeatureUsageSnapshot>,
) -> Value {
    let promotion_manifest = builtin_promotion_manifest();
    let mut maturity_counts = BTreeMap::new();
    for maturity in FEATURE_ROLLOUT_MATURITY_STATES {
        maturity_counts.insert(maturity.as_str(), 0_usize);
    }
    let mut contract_availability_counts =
        ["descriptor_only", "api_available", "runtime_available", "blocked"]
            .into_iter()
            .map(|state| (state, 0_usize))
            .collect::<BTreeMap<_, _>>();
    let mut execution_completeness_counts = ["not_implemented", "partial", "complete"]
        .into_iter()
        .map(|state| (state, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut promotion_state_counts =
        ["contract_only", "shadow", "canary", "gated_production", "stable"]
            .into_iter()
            .map(|state| (state, 0_usize))
            .collect::<BTreeMap<_, _>>();
    let mut support_maturity_counts =
        ["unsupported", "experimental", "preview", "supported", "deprecated", "retired"]
            .into_iter()
            .map(|state| (state, 0_usize))
            .collect::<BTreeMap<_, _>>();
    let mut lifecycle_counts = ["active", "deprecated", "retired"]
        .into_iter()
        .map(|state| (state, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut enabled_flags = 0_usize;
    let mut inactive_flags = 0_usize;
    let mut blocked_flags = 0_usize;
    let mut authoritative_rollout_flags = 0_usize;
    let mut non_authoritative_rollout_flags = 0_usize;
    let mut effectively_enabled_rollouts = 0_usize;
    let mut qualified_hot_path_flags = 0_usize;

    for descriptor in FEATURE_ROLLOUT_DESCRIPTORS {
        let setting = descriptor.flag.setting(config);
        let promotion = resolved_promotion(promotion_manifest, descriptor.flag.as_str());
        let activation_authoritative = rollout_activation_authoritative(promotion.rollout);
        let effective_enabled = activation_authoritative && setting.enabled;
        if setting.enabled {
            enabled_flags = enabled_flags.saturating_add(1);
        } else {
            inactive_flags = inactive_flags.saturating_add(1);
        }
        if activation_authoritative {
            authoritative_rollout_flags = authoritative_rollout_flags.saturating_add(1);
        } else {
            non_authoritative_rollout_flags = non_authoritative_rollout_flags.saturating_add(1);
        }
        if effective_enabled {
            effectively_enabled_rollouts = effectively_enabled_rollouts.saturating_add(1);
        }
        if descriptor.maturity == FeatureRolloutMaturity::Blocked {
            blocked_flags = blocked_flags.saturating_add(1);
        }
        *maturity_counts.entry(descriptor.maturity.as_str()).or_insert(0_usize) += 1;
        *contract_availability_counts
            .entry(promotion.rollout.contract_availability.as_str())
            .or_insert(0_usize) += 1;
        *execution_completeness_counts
            .entry(promotion.rollout.execution_completeness.as_str())
            .or_insert(0_usize) += 1;
        *promotion_state_counts
            .entry(promotion.rollout.promotion_state.as_str())
            .or_insert(0_usize) += 1;
        *support_maturity_counts
            .entry(promotion.rollout.support_maturity.as_str())
            .or_insert(0_usize) += 1;
        *lifecycle_counts.entry(promotion.rollout.lifecycle.as_str()).or_insert(0_usize) += 1;
        if usage.is_some_and(|usage| {
            runtime_usage_projection(
                descriptor.flag.as_str(),
                promotion.rollout.execution_completeness,
                effective_enabled,
                usage,
            )["qualified_hot_path"]
                .as_bool()
                .unwrap_or(false)
        }) {
            qualified_hot_path_flags = qualified_hot_path_flags.saturating_add(1);
        }
    }

    let usage_window = usage.map_or_else(
        || {
            json!({
                "observation_scope": "not_collected",
                "resets_on_restart": true,
                "retained_run_capacity": 0,
                "retained_run_count": 0,
                "active_run_count": 0,
                "terminal_run_count": 0,
                "window_truncated": false,
                "evicted_run_count": 0,
                "terminal_fence_capacity": 0,
                "retained_terminal_fence_count": 0,
                "terminal_fence_window_truncated": false,
                "evicted_terminal_fence_count": 0,
                "dropped_observation_count": 0,
                "dropped_observation_reason_counts": {},
            })
        },
        |usage| {
            json!({
                "observation_scope": usage.observation_scope,
                "resets_on_restart": usage.resets_on_restart,
                "retained_run_capacity": usage.capacity,
                "retained_run_count": usage.retained_runs,
                "active_run_count": usage.active_runs,
                "terminal_run_count": usage.terminal_runs,
                "window_truncated": usage.window_truncated,
                "evicted_run_count": usage.evicted_runs,
                "terminal_fence_capacity": usage.terminal_fence_capacity,
                "retained_terminal_fence_count": usage.retained_terminal_fences,
                "terminal_fence_window_truncated": usage.terminal_fence_window_truncated,
                "evicted_terminal_fence_count": usage.evicted_terminal_fences,
                "dropped_observation_count": usage.dropped_observations,
                "dropped_observation_reason_counts": usage.dropped_observation_reason_counts,
            })
        },
    );
    let v1_compatibility_projection = json!({
        "schema_version": FEATURE_ROLLOUT_MATURITY_LEGACY_SCHEMA_VERSION,
        "flag_count": FEATURE_ROLLOUT_DESCRIPTORS.len(),
        "enabled_flags": enabled_flags,
        "inactive_flags": inactive_flags,
        "blocked_flags": blocked_flags,
        "maturity_counts": maturity_counts,
        "deprecated_alias_policy": "deprecated rollout aliases are not accepted; use the listed config_path or env_var",
        "migration_note": "flag renames must add a deprecated_aliases entry here before aliases are removed from loaders",
    });

    json!({
        "schema_version": FEATURE_ROLLOUT_MATURITY_SCHEMA_VERSION,
        "legacy_schema_version": FEATURE_ROLLOUT_MATURITY_LEGACY_SCHEMA_VERSION,
        "flag_count": FEATURE_ROLLOUT_DESCRIPTORS.len(),
        "enabled_flags": enabled_flags,
        "inactive_flags": inactive_flags,
        "blocked_flags": blocked_flags,
        "authoritative_rollout_flags": authoritative_rollout_flags,
        "non_authoritative_rollout_flags": non_authoritative_rollout_flags,
        "effectively_enabled_rollouts": effectively_enabled_rollouts,
        "maturity_counts": maturity_counts,
        "contract_availability_counts": contract_availability_counts,
        "execution_completeness_counts": execution_completeness_counts,
        "promotion_state_counts": promotion_state_counts,
        "support_maturity_counts": support_maturity_counts,
        "lifecycle_counts": lifecycle_counts,
        "qualified_hot_path_flags": qualified_hot_path_flags,
        "usage_window": usage_window,
        "promotion_manifest": {
            "schema_version": promotion_manifest.schema_version,
            "schema_id": promotion_manifest.schema_id,
            "schema_sha256": promotion_manifest.schema_sha256,
            "evidence_profile_count": promotion_manifest.evidence_profiles.len(),
            "rollout_count": promotion_manifest.rollouts.len(),
        },
        "v1_compatibility_projection": v1_compatibility_projection,
        "deprecated_alias_policy": "deprecated rollout aliases are not accepted; use the listed config_path or env_var",
        "migration_note": "flag renames must add a deprecated_aliases entry here before aliases are removed from loaders",
    })
}

fn build_feature_rollout_maturity_summary_v1_base(config: &FeatureRolloutsConfig) -> Value {
    build_feature_rollout_maturity_summary_base(config, None)
        .get("v1_compatibility_projection")
        .cloned()
        .expect("version 2 maturity summary always embeds the frozen version 1 projection")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseAcceptanceGate {
    pub(crate) id: String,
    pub(crate) area: String,
    pub(crate) passed: bool,
    pub(crate) required: bool,
    pub(crate) evidence_ref: String,
    pub(crate) blocking_dependencies: Vec<String>,
    pub(crate) manual_override: Option<ReleaseManualOverride>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseManualOverride {
    pub(crate) approval_ref: String,
    pub(crate) actor_ref: String,
    pub(crate) reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseMilestoneStatus {
    pub(crate) area: String,
    pub(crate) code_complete: bool,
    pub(crate) acceptance_complete: bool,
}

const STABLE_CANDIDATE_REQUIRED_GATE_IDS: [&str; 4] =
    ["direct-hot-path", "no-hidden-fallback", "qualified-production-window", "sli-window"];

pub(crate) struct ReleaseDashboardInput<'a> {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) gates: &'a [ReleaseAcceptanceGate],
    pub(crate) milestone_statuses: &'a [ReleaseMilestoneStatus],
    pub(crate) usage: Option<&'a FeatureUsageSnapshot>,
}

pub(crate) fn build_release_acceptance_dashboard(
    config: &FeatureRolloutsConfig,
    input: ReleaseDashboardInput<'_>,
) -> Value {
    let milestone_statuses = input
        .milestone_statuses
        .iter()
        .map(|status| (status.area.as_str(), status))
        .collect::<BTreeMap<_, _>>();
    let p0_areas = [
        FeatureRolloutFlag::AgentHarnessRuntime,
        FeatureRolloutFlag::ExecutionGatePipelineV2,
        FeatureRolloutFlag::ProviderRecovery,
        FeatureRolloutFlag::ReplayCapture,
        FeatureRolloutFlag::VerificationRuntime,
        FeatureRolloutFlag::CompactionSafeguard,
        FeatureRolloutFlag::AdvisorFanout,
        FeatureRolloutFlag::LspService,
    ];
    let area_reports = p0_areas
        .iter()
        .filter_map(|flag| {
            FEATURE_ROLLOUT_DESCRIPTORS.iter().find(|descriptor| descriptor.flag == *flag).map(
                |descriptor| {
                    release_dashboard_area_report(
                        *descriptor,
                        config,
                        &milestone_statuses,
                        input.gates,
                        input.usage,
                    )
                },
            )
        })
        .collect::<Vec<_>>();
    let failing_gates = input
        .gates
        .iter()
        .filter(|gate| gate.required && !gate.passed)
        .map(release_gate_value)
        .collect::<Vec<_>>();
    let manual_overrides = input
        .gates
        .iter()
        .filter_map(|gate| {
            gate.manual_override.as_ref().map(|override_record| {
                json!({
                    "gate_id": gate.id,
                    "area": gate.area,
                    "approval_ref": override_record.approval_ref,
                    "actor_ref": override_record.actor_ref,
                    "reason_code": override_record.reason_code,
                    "audit_required": true,
                })
            })
        })
        .collect::<Vec<_>>();
    let stable_candidate_count = area_reports
        .iter()
        .filter(|report| report.get("stable_candidate").and_then(Value::as_bool).unwrap_or(false))
        .count();

    json!({
        "schema_version": 1,
        "generated_at_unix_ms": input.generated_at_unix_ms,
        "p0_area_count": area_reports.len(),
        "stable_candidate_count": stable_candidate_count,
        "areas": area_reports,
        "failing_gates": failing_gates,
        "manual_overrides": manual_overrides,
        "maturity_summary": build_feature_rollout_maturity_summary_v1_base(config),
        "roadmap_checkbox_policy": "roadmap acceptance complete does not imply stable candidate",
    })
}

fn build_release_dashboard_contract(
    config: &FeatureRolloutsConfig,
    usage: Option<&FeatureUsageSnapshot>,
) -> Value {
    let gates = [
        ReleaseAcceptanceGate {
            id: "replay-fixtures".to_owned(),
            area: "replay_capture".to_owned(),
            passed: false,
            required: true,
            evidence_ref: "qa://replay_capture/replay-fixtures".to_owned(),
            blocking_dependencies: vec!["run_trace_v1".to_owned()],
            manual_override: None,
        },
        ReleaseAcceptanceGate {
            id: "harness-conformance".to_owned(),
            area: "agent_harness_runtime".to_owned(),
            passed: false,
            required: true,
            evidence_ref: "qa://agent_harness_runtime/harness-conformance".to_owned(),
            blocking_dependencies: vec!["execution_gate_pipeline_v2".to_owned()],
            manual_override: Some(ReleaseManualOverride {
                approval_ref: "manual-review-required".to_owned(),
                actor_ref: "operator:release".to_owned(),
                reason_code: "release_dashboard.contract_sample".to_owned(),
            }),
        },
    ];
    let milestones = [
        ReleaseMilestoneStatus {
            area: "replay_capture".to_owned(),
            code_complete: true,
            acceptance_complete: false,
        },
        ReleaseMilestoneStatus {
            area: "agent_harness_runtime".to_owned(),
            code_complete: true,
            acceptance_complete: false,
        },
    ];

    let mut dashboard = build_release_acceptance_dashboard(
        config,
        ReleaseDashboardInput {
            generated_at_unix_ms: 0,
            gates: gates.as_slice(),
            milestone_statuses: milestones.as_slice(),
            usage,
        },
    );
    let markdown_preview = render_release_acceptance_dashboard_markdown(&dashboard);
    if let Some(object) = dashboard.as_object_mut() {
        object.insert("markdown_preview".to_owned(), Value::String(markdown_preview));
    }
    dashboard
}

fn build_release_dashboard_contract_v1(config: &FeatureRolloutsConfig) -> Value {
    let mut dashboard = build_release_dashboard_contract(config, None);
    if let Some(areas) = dashboard.get_mut("areas").and_then(Value::as_array_mut) {
        for area in areas {
            let area =
                area.as_object_mut().expect("release dashboard areas are authored as objects");
            for additive_field in [
                "promotion_state",
                "execution_completeness",
                "configured_enabled",
                "activation_authoritative",
                "effective_enabled",
                "promotion_evidence_ready",
                "promotion_required_test_refs",
                "qualified_hot_path",
                "stable_evidence_gates_ready",
                "required_stable_evidence_gate_ids",
            ] {
                area.remove(additive_field);
            }
        }
    }
    let markdown_preview = render_release_acceptance_dashboard_markdown_v1(&dashboard);
    dashboard
        .as_object_mut()
        .expect("release dashboard contract is authored as an object")
        .insert("markdown_preview".to_owned(), Value::String(markdown_preview));
    dashboard
}

fn render_release_acceptance_dashboard_markdown_v1(dashboard: &Value) -> String {
    let mut output = String::new();
    output.push_str("# Release Acceptance Dashboard\n\n");
    output.push_str(&format!(
        "- P0 areas: {}\n",
        dashboard.get("p0_area_count").and_then(Value::as_u64).unwrap_or_default()
    ));
    output.push_str(&format!(
        "- Stable candidates: {}\n",
        dashboard.get("stable_candidate_count").and_then(Value::as_u64).unwrap_or_default()
    ));
    output.push_str("- Roadmap checkbox policy: acceptance complete is not stable by itself\n\n");
    output.push_str("| Area | Maturity | Code complete | Tested | Stable candidate | Blockers |\n");
    output.push_str("| --- | --- | --- | --- | --- | --- |\n");
    if let Some(areas) = dashboard.get("areas").and_then(Value::as_array) {
        for area in areas {
            let blockers = area
                .get("blockers")
                .and_then(Value::as_array)
                .map(|values| {
                    values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("; ")
                })
                .unwrap_or_default();
            output.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                area.get("area").and_then(Value::as_str).unwrap_or("unknown"),
                area.get("maturity").and_then(Value::as_str).unwrap_or("unknown"),
                area.get("code_complete").and_then(Value::as_bool).unwrap_or(false),
                area.get("tested").and_then(Value::as_bool).unwrap_or(false),
                area.get("stable_candidate").and_then(Value::as_bool).unwrap_or(false),
                blockers
            ));
        }
    }
    output
}

pub(crate) fn render_release_acceptance_dashboard_markdown(dashboard: &Value) -> String {
    let mut output = String::new();
    output.push_str("# Release Acceptance Dashboard\n\n");
    output.push_str(&format!(
        "- P0 areas: {}\n",
        dashboard.get("p0_area_count").and_then(Value::as_u64).unwrap_or_default()
    ));
    output.push_str(&format!(
        "- Stable candidates: {}\n",
        dashboard.get("stable_candidate_count").and_then(Value::as_u64).unwrap_or_default()
    ));
    output.push_str("- Roadmap checkbox policy: acceptance complete is not stable by itself\n\n");
    output.push_str(
        "| Area | Legacy maturity | Promotion | Code complete | Tested | Stable candidate | Blockers |\n",
    );
    output.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
    if let Some(areas) = dashboard.get("areas").and_then(Value::as_array) {
        for area in areas {
            let blockers = area
                .get("blockers")
                .and_then(Value::as_array)
                .map(|values| {
                    values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("; ")
                })
                .unwrap_or_default();
            output.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} |\n",
                area.get("area").and_then(Value::as_str).unwrap_or("unknown"),
                area.get("maturity").and_then(Value::as_str).unwrap_or("unknown"),
                area.get("promotion_state").and_then(Value::as_str).unwrap_or("unknown"),
                area.get("code_complete").and_then(Value::as_bool).unwrap_or(false),
                area.get("tested").and_then(Value::as_bool).unwrap_or(false),
                area.get("stable_candidate").and_then(Value::as_bool).unwrap_or(false),
                blockers
            ));
        }
    }
    output
}

fn release_dashboard_area_report(
    descriptor: FeatureRolloutDescriptor,
    config: &FeatureRolloutsConfig,
    milestone_statuses: &BTreeMap<&str, &ReleaseMilestoneStatus>,
    gates: &[ReleaseAcceptanceGate],
    usage: Option<&FeatureUsageSnapshot>,
) -> Value {
    let flag = descriptor.flag.as_str();
    let status = milestone_statuses.get(flag).copied();
    let area_gates = gates.iter().filter(|gate| gate.area == flag).collect::<Vec<_>>();
    let failing_required_gates = area_gates
        .iter()
        .filter(|gate| gate.required && !gate.passed)
        .map(|gate| gate.id.clone())
        .collect::<Vec<_>>();
    let tested = !area_gates.is_empty() && failing_required_gates.is_empty();
    let code_complete = status.is_some_and(|status| status.code_complete);
    let acceptance_complete = status.is_some_and(|status| status.acceptance_complete);
    let promotion = resolved_promotion(builtin_promotion_manifest(), flag);
    let setting = descriptor.flag.setting(config);
    let activation_authoritative = rollout_activation_authoritative(promotion.rollout);
    let effective_enabled = activation_authoritative && setting.enabled;
    let promotion_evidence_ready = promotion.evidence.direct_hot_path_test_ref.is_some()
        && promotion.evidence.no_hidden_fallback_test_ref.is_some();
    let qualified_hot_path = usage.is_some_and(|usage| {
        runtime_usage_projection(
            flag,
            promotion.rollout.execution_completeness,
            effective_enabled,
            usage,
        )["qualified_hot_path"]
            .as_bool()
            .unwrap_or(false)
    });
    let missing_stable_evidence_gates = STABLE_CANDIDATE_REQUIRED_GATE_IDS
        .into_iter()
        .filter(|required_id| {
            !area_gates.iter().any(|gate| {
                gate.id == *required_id
                    && gate.required
                    && gate.passed
                    && gate.manual_override.is_none()
                    && gate.blocking_dependencies.is_empty()
            })
        })
        .collect::<Vec<_>>();
    let stable_evidence_gates_ready = missing_stable_evidence_gates.is_empty();
    let mut blockers = descriptor
        .activation_blockers
        .iter()
        .map(|blocker| (*blocker).to_owned())
        .collect::<Vec<_>>();
    blockers.extend(promotion.rollout.promotion_blockers.iter().cloned());
    if !activation_authoritative {
        blockers.push(
            "rollout setting is non-authoritative and cannot activate the capability".to_owned(),
        );
    } else if !setting.enabled {
        blockers.push("rollout is not active in the current process".to_owned());
    }
    if !failing_required_gates.is_empty() {
        blockers.push(format!("failing required gates: {}", failing_required_gates.join(", ")));
    }
    if !missing_stable_evidence_gates.is_empty() {
        blockers.push(format!(
            "missing passed stable-evidence gates: {}",
            missing_stable_evidence_gates.join(", ")
        ));
    }
    for gate in &area_gates {
        blockers.extend(gate.blocking_dependencies.iter().cloned());
    }
    let stable_candidate = code_complete
        && acceptance_complete
        && tested
        && promotion.rollout.execution_completeness == manifest::ExecutionCompleteness::Complete
        && promotion.rollout.contract_availability
            == manifest::ContractAvailability::RuntimeAvailable
        && promotion.rollout.lifecycle == manifest::RolloutLifecycle::Active
        && matches!(
            promotion.rollout.promotion_state,
            manifest::PromotionState::GatedProduction | manifest::PromotionState::Stable
        )
        && promotion_evidence_ready
        && qualified_hot_path
        && stable_evidence_gates_ready
        && blockers.is_empty();

    json!({
        "area": flag,
        "owner_component": descriptor.owner_component,
        "maturity": descriptor.maturity.as_str(),
        "promotion_state": promotion.rollout.promotion_state,
        "execution_completeness": promotion.rollout.execution_completeness,
        "enabled": setting.enabled,
        "configured_enabled": setting.enabled,
        "activation_authoritative": activation_authoritative,
        "effective_enabled": activation_authoritative.then_some(setting.enabled),
        "code_complete": code_complete,
        "acceptance_complete": acceptance_complete,
        "tested": tested,
        "gated_production": matches!(promotion.rollout.promotion_state, manifest::PromotionState::GatedProduction | manifest::PromotionState::Stable),
        "promotion_evidence_ready": promotion_evidence_ready,
        "qualified_hot_path": qualified_hot_path,
        "stable_evidence_gates_ready": stable_evidence_gates_ready,
        "required_stable_evidence_gate_ids": STABLE_CANDIDATE_REQUIRED_GATE_IDS,
        "stable_candidate": stable_candidate,
        "required_tests": descriptor.required_tests,
        "promotion_required_test_refs": promotion.evidence.required_test_refs,
        "acceptance_criteria": descriptor.acceptance_criteria,
        "dependencies": descriptor.stable_dependencies.iter().map(|flag| flag.as_str()).collect::<Vec<_>>(),
        "gates": area_gates.iter().map(|gate| release_gate_value(gate)).collect::<Vec<_>>(),
        "blockers": blockers,
    })
}

fn release_gate_value(gate: &ReleaseAcceptanceGate) -> Value {
    json!({
        "id": gate.id,
        "area": gate.area,
        "passed": gate.passed,
        "required": gate.required,
        "evidence_ref": gate.evidence_ref,
        "blocking_dependencies": gate.blocking_dependencies,
        "manual_override": gate.manual_override.as_ref().map(|override_record| {
            json!({
                "approval_ref": override_record.approval_ref,
                "actor_ref": override_record.actor_ref,
                "reason_code": override_record.reason_code,
                "audit_required": true,
            })
        }),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        build_feature_rollout_diagnostics, build_feature_rollout_maturity_summary_v1,
        build_feature_rollout_maturity_summary_v2, build_release_acceptance_dashboard,
        builtin_promotion_manifest, render_release_acceptance_dashboard_markdown,
        validate_feature_rollout_maturity_descriptors, FeatureRolloutDescriptor,
        FeatureRolloutFlag, FeatureRolloutMaturity, FeatureRolloutMaturityValidationError,
        ReleaseAcceptanceGate, ReleaseDashboardInput, ReleaseManualOverride,
        ReleaseMilestoneStatus, DIAGNOSTICS_ACCEPTANCE, FEATURE_ROLLOUT_DESCRIPTORS,
        NO_DEPRECATED_ALIASES, NO_STABLE_DEPENDENCIES, STABLE_CANDIDATE_REQUIRED_GATE_IDS,
    };
    use crate::{
        config::FeatureRolloutsConfig,
        feature_usage::{
            FeatureUsageCapability, FeatureUsagePath, FeatureUsageReason, FeatureUsageRegistry,
        },
    };
    use palyra_common::feature_rollouts::FeatureRolloutSetting;
    use serde::Deserialize;
    use serde_json::Value;

    #[test]
    fn maturity_parser_accepts_canonical_states_and_alias_separators() {
        let cases = [
            ("scaffold", FeatureRolloutMaturity::Scaffold),
            ("preview only", FeatureRolloutMaturity::PreviewOnly),
            ("gated-production", FeatureRolloutMaturity::GatedProduction),
            ("stable", FeatureRolloutMaturity::Stable),
            ("deprecated", FeatureRolloutMaturity::Deprecated),
            ("blocked", FeatureRolloutMaturity::Blocked),
        ];

        for (raw, expected) in cases {
            assert_eq!(
                FeatureRolloutMaturity::parse(raw).expect("maturity state should parse"),
                expected
            );
        }
    }

    #[test]
    fn maturity_parser_rejects_unknown_states() {
        let error =
            FeatureRolloutMaturity::parse("pilot").expect_err("unknown maturity state should fail");

        assert!(error.to_string().contains("pilot"));
    }

    #[test]
    fn builtin_maturity_matrix_has_one_descriptor_per_rollout_flag() {
        validate_feature_rollout_maturity_descriptors(FEATURE_ROLLOUT_DESCRIPTORS)
            .expect("builtin feature rollout maturity matrix should validate");
        assert_eq!(FEATURE_ROLLOUT_DESCRIPTORS.len(), 35);
    }

    #[test]
    fn non_authoritative_rollouts_do_not_claim_canary_activation() {
        let manifest = builtin_promotion_manifest();
        let cases = [
            ("safety_boundary", super::manifest::ContractAvailability::RuntimeAvailable),
            ("provider_recovery", super::manifest::ContractAvailability::RuntimeAvailable),
            ("advisor_fanout", super::manifest::ContractAvailability::ApiAvailable),
            ("acp_runtime", super::manifest::ContractAvailability::RuntimeAvailable),
            ("objective_judge", super::manifest::ContractAvailability::RuntimeAvailable),
        ];

        for (capability, expected_availability) in cases {
            let rollout = manifest.rollout(capability).expect("audited rollout must exist");
            assert_eq!(rollout.contract_availability, expected_availability, "{capability}");
            assert_eq!(
                rollout.promotion_state,
                super::manifest::PromotionState::ContractOnly,
                "{capability}"
            );
            assert_eq!(
                rollout.evidence_profile, "non_authoritative_runtime_contract",
                "{capability}"
            );
        }
    }

    #[test]
    fn contract_only_configuration_does_not_claim_runtime_activation_or_rollback() {
        let config = FeatureRolloutsConfig {
            safety_boundary: FeatureRolloutSetting::from_config(true),
            progress_drafts: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let diagnostics =
            build_feature_rollout_diagnostics(&config, &FeatureUsageRegistry::new().snapshot());
        let safety =
            diagnostics.get("safety_boundary").expect("safety rollout diagnostics must exist");

        assert_eq!(safety["enabled"], true);
        assert_eq!(safety["rollout_activation"]["configured_enabled"], true);
        assert_eq!(safety["rollout_activation"]["authoritative"], false);
        assert!(safety["rollout_activation"]["effective_enabled"].is_null());
        assert_eq!(
            safety["rollout_activation"]["reason_code"],
            "feature_rollout.activation_not_authoritative"
        );
        assert_eq!(safety["rollback_knob"]["applicable"], false);
        assert_eq!(
            safety["rollback_knob"]["reason_code"],
            "feature_rollout.rollback_not_authoritative"
        );
        assert!(safety["inactive_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("non-authoritative")));
        assert!(safety["activation_blockers"].as_array().is_some_and(|blockers| blockers
            .iter()
            .any(|blocker| {
                blocker.as_str().is_some_and(|value| value.contains("cannot activate"))
            })));

        let progress =
            diagnostics.get("progress_drafts").expect("progress rollout diagnostics must exist");
        assert_eq!(progress["rollout_activation"]["authoritative"], true);
        assert_eq!(progress["rollout_activation"]["effective_enabled"], true);
        assert_eq!(progress["rollback_knob"]["applicable"], true);
    }

    #[test]
    fn stable_rollout_without_acceptance_criteria_is_rejected() {
        let descriptors = [FeatureRolloutDescriptor {
            flag: FeatureRolloutFlag::ToolRepair,
            owner_component: "run stream/tool repair",
            maturity: FeatureRolloutMaturity::Stable,
            required_tests: &["cargo test -p palyra-daemon --locked"],
            public_api_exposure: "operator diagnostics",
            activation_blockers: &["feature_rollouts.context_engine must be enabled first"],
            acceptance_criteria: &[],
            deprecated_aliases: NO_DEPRECATED_ALIASES,
            migration_note: "test descriptor",
            stable_dependencies: NO_STABLE_DEPENDENCIES,
        }];

        let error = validate_feature_rollout_maturity_descriptors(&descriptors)
            .expect_err("stable rollout without acceptance criteria must fail");

        assert_eq!(
            error,
            FeatureRolloutMaturityValidationError::StableWithoutAcceptance { flag: "tool_repair" }
        );
    }

    #[test]
    fn preview_rollout_without_acceptance_criteria_is_rejected() {
        let descriptors = [FeatureRolloutDescriptor {
            flag: FeatureRolloutFlag::ContextEngine,
            owner_component: "application/context_engine",
            maturity: FeatureRolloutMaturity::PreviewOnly,
            required_tests: &["cargo test -p palyra-daemon --locked"],
            public_api_exposure: "operator diagnostics",
            activation_blockers: &["redacted context traces must be present"],
            acceptance_criteria: &[],
            deprecated_aliases: NO_DEPRECATED_ALIASES,
            migration_note: "test descriptor",
            stable_dependencies: NO_STABLE_DEPENDENCIES,
        }];

        let error = validate_feature_rollout_maturity_descriptors(&descriptors)
            .expect_err("preview rollout without acceptance criteria must fail");

        assert_eq!(
            error,
            FeatureRolloutMaturityValidationError::MissingAcceptanceCriteria {
                flag: "context_engine"
            }
        );
    }

    #[test]
    fn diagnostics_expose_default_posture_and_promotion_gate() {
        let usage = FeatureUsageRegistry::new().snapshot();
        let diagnostics =
            build_feature_rollout_diagnostics(&FeatureRolloutsConfig::default(), &usage);
        let context_engine =
            diagnostics.get("context_engine").expect("context engine rollout should be present");

        assert_eq!(
            context_engine.get("default_posture").and_then(Value::as_str),
            Some("default_off")
        );
        assert_eq!(
            context_engine.pointer("/rollback_knob/safe_default").and_then(Value::as_str),
            Some("disabled")
        );
        assert_eq!(
            context_engine
                .pointer("/promotion_gate/default_enable_allowed")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            context_engine.pointer("/promotion_gate/test_coverage_marker").and_then(Value::as_str),
            Some("required_tests_and_acceptance_criteria_present")
        );
        assert_eq!(
            context_engine.pointer("/runtime_usage/instrumented").and_then(Value::as_bool),
            Some(false)
        );
        assert!(
            context_engine
                .pointer("/runtime_usage/observed_unique_runs")
                .is_some_and(Value::is_null),
            "uninstrumented capability usage must remain unknown rather than claim zero runs"
        );
    }

    #[test]
    fn qualified_hot_path_requires_terminal_direct_usage_without_fallback() {
        let config = FeatureRolloutsConfig {
            compaction_safeguard: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let registry = FeatureUsageRegistry::new();
        registry.record(
            "01ARZ3NDEKTSV4RRFFQ69G5FB6",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        let active_diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        assert_eq!(
            active_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualified_hot_path")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            active_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualification_reason_code")
                .and_then(Value::as_str),
            Some("feature_usage.active_run_observed")
        );

        registry.mark_terminal("01ARZ3NDEKTSV4RRFFQ69G5FB6");
        let direct_diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        assert_eq!(
            direct_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualified_hot_path")
                .and_then(Value::as_bool),
            Some(true)
        );

        registry.record(
            "01ARZ3NDEKTSV4RRFFQ69G5FB7",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        let mixed_lifecycle_diagnostics =
            build_feature_rollout_diagnostics(&config, &registry.snapshot());
        assert_eq!(
            mixed_lifecycle_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualified_hot_path")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            mixed_lifecycle_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualification_reason_code")
                .and_then(Value::as_str),
            Some("feature_usage.active_run_observed")
        );
        registry.mark_terminal("01ARZ3NDEKTSV4RRFFQ69G5FB7");

        registry.record(
            "01ARZ3NDEKTSV4RRFFQ69G5FB8",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.mark_terminal("01ARZ3NDEKTSV4RRFFQ69G5FB8");
        let fallback_diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        assert_eq!(
            fallback_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualified_hot_path")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            fallback_diagnostics
                .pointer("/compaction_safeguard/runtime_usage/qualification_reason_code")
                .and_then(Value::as_str),
            Some("feature_usage.fallback_observed")
        );
    }

    #[test]
    fn qualified_hot_path_fails_closed_after_invalid_observation() {
        let config = FeatureRolloutsConfig {
            compaction_safeguard: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let registry = FeatureUsageRegistry::new();
        registry.record(
            "01ARZ3NDEKTSV4RRFFQ69G5FC1",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("01ARZ3NDEKTSV4RRFFQ69G5FC1");
        registry.record(
            " ",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );

        let diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        let runtime_usage = diagnostics
            .pointer("/compaction_safeguard/runtime_usage")
            .expect("compaction runtime usage should exist");
        assert_eq!(runtime_usage["qualified_hot_path"], false);
        assert_eq!(runtime_usage["qualification_reason_code"], "feature_usage.observation_dropped");
        assert_eq!(runtime_usage["dropped_observation_count"], 1);
        assert_eq!(runtime_usage["dropped_observation_reason_counts"]["empty_run_id"], 1);
    }

    #[test]
    fn qualified_hot_path_requires_effective_rollout_activation() {
        let direct_registry = FeatureUsageRegistry::new();
        direct_registry.record(
            "disabled-direct-run",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        direct_registry.mark_terminal("disabled-direct-run");
        let direct_diagnostics = build_feature_rollout_diagnostics(
            &FeatureRolloutsConfig::default(),
            &direct_registry.snapshot(),
        );
        let direct_usage = direct_diagnostics
            .pointer("/compaction_safeguard/runtime_usage")
            .expect("compaction runtime usage should exist");
        assert_eq!(direct_usage["qualified_hot_path"], false);
        assert_eq!(direct_usage["qualification_reason_code"], "feature_usage.activation_mismatch");

        let fallback_registry = FeatureUsageRegistry::new();
        fallback_registry.record(
            "disabled-fallback-run",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        fallback_registry.mark_terminal("disabled-fallback-run");
        let fallback_diagnostics = build_feature_rollout_diagnostics(
            &FeatureRolloutsConfig::default(),
            &fallback_registry.snapshot(),
        );
        let fallback_usage = fallback_diagnostics
            .pointer("/compaction_safeguard/runtime_usage")
            .expect("compaction runtime usage should exist");
        assert_eq!(fallback_usage["qualified_hot_path"], false);
        assert_eq!(fallback_usage["qualification_reason_code"], "feature_usage.rollout_inactive");
    }

    #[test]
    fn qualified_hot_path_uses_capability_local_eviction_evidence() {
        let config = FeatureRolloutsConfig {
            compaction_safeguard: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let registry = FeatureUsageRegistry::with_test_capacity(2);
        registry.record(
            "verification-oldest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "compaction-retained",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("compaction-retained");
        registry.record(
            "verification-newest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );

        let diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        let compaction = diagnostics
            .pointer("/compaction_safeguard/runtime_usage")
            .expect("compaction runtime usage should exist");
        assert_eq!(compaction["qualified_hot_path"], true);
        assert_eq!(compaction["active_run_count"], 0);
        assert_eq!(compaction["terminal_run_count"], 1);
        assert_eq!(compaction["window_truncated"], false);
        assert_eq!(compaction["evicted_run_count"], 0);

        let verification = diagnostics
            .pointer("/verification_runtime/runtime_usage")
            .expect("verification runtime usage should exist");
        assert_eq!(verification["active_run_count"], 1);
        assert_eq!(verification["window_truncated"], true);
        assert_eq!(verification["evicted_run_count"], 1);
    }

    #[test]
    fn terminal_fence_truncation_does_not_contaminate_clean_capability_evidence() {
        let config = FeatureRolloutsConfig {
            compaction_safeguard: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let registry = FeatureUsageRegistry::with_test_capacity(2);
        registry.mark_terminal("unrelated-terminal-a");
        registry.mark_terminal("unrelated-terminal-b");
        registry.mark_terminal("unrelated-terminal-c");
        registry.record(
            "compaction-terminal",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("compaction-terminal");

        let diagnostics = build_feature_rollout_diagnostics(&config, &registry.snapshot());
        let runtime_usage = diagnostics
            .pointer("/compaction_safeguard/runtime_usage")
            .expect("compaction runtime usage should exist");
        assert_eq!(runtime_usage["qualified_hot_path"], true);
        assert_eq!(runtime_usage["terminal_fence_window_truncated"], true);
        assert_eq!(runtime_usage["qualification_reason_code"], "feature_usage.qualified_hot_path");
    }

    #[test]
    fn rollout_entries_preserve_the_legacy_client_fields() {
        #[derive(Deserialize)]
        struct LegacyRolloutEntry {
            enabled: bool,
            maturity: String,
            owner_component: String,
            required_tests: Vec<String>,
            activation_blockers: Vec<String>,
        }

        let usage = FeatureUsageRegistry::new().snapshot();
        let diagnostics =
            build_feature_rollout_diagnostics(&FeatureRolloutsConfig::default(), &usage);
        let legacy: BTreeMap<String, LegacyRolloutEntry> =
            serde_json::from_value(diagnostics).expect("v1 rollout projection should deserialize");

        assert_eq!(legacy.len(), 35);
        let compaction = legacy
            .get("compaction_safeguard")
            .expect("legacy client should retain compaction rollout");
        assert!(!compaction.enabled);
        assert_eq!(compaction.maturity, "gated_production");
        assert_eq!(compaction.owner_component, "session compaction");
        assert!(!compaction.required_tests.is_empty());
        assert!(!compaction.activation_blockers.is_empty());
    }

    #[test]
    fn maturity_summary_exposes_frozen_v1_and_additive_v2_payloads() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyMaturitySummary {
            schema_version: u32,
            flag_count: usize,
            enabled_flags: usize,
            inactive_flags: usize,
            blocked_flags: usize,
            maturity_counts: BTreeMap<String, usize>,
            deprecated_alias_policy: String,
            migration_note: String,
            release_dashboard_contract: Value,
        }

        let config = FeatureRolloutsConfig::default();
        let usage = FeatureUsageRegistry::new().snapshot();
        let v1 = build_feature_rollout_maturity_summary_v1(&config);
        let legacy: LegacyMaturitySummary = serde_json::from_value(v1.clone())
            .expect("the frozen v1 summary must reject additive root fields");
        let v2 = build_feature_rollout_maturity_summary_v2(&config, &usage);

        assert_eq!(legacy.schema_version, 1);
        assert_eq!(legacy.flag_count, 35);
        assert_eq!(legacy.enabled_flags + legacy.inactive_flags, legacy.flag_count);
        assert_eq!(legacy.blocked_flags, 5);
        assert_eq!(legacy.maturity_counts.values().sum::<usize>(), legacy.flag_count);
        assert!(!legacy.deprecated_alias_policy.is_empty());
        assert!(!legacy.migration_note.is_empty());
        assert_eq!(legacy.release_dashboard_contract["schema_version"], 1);
        let legacy_area_keys = legacy.release_dashboard_contract["areas"][0]
            .as_object()
            .expect("legacy release dashboard area must be an object")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected_legacy_area_keys = [
            "acceptance_complete",
            "acceptance_criteria",
            "area",
            "blockers",
            "code_complete",
            "dependencies",
            "enabled",
            "gated_production",
            "gates",
            "maturity",
            "owner_component",
            "required_tests",
            "stable_candidate",
            "tested",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
        assert_eq!(legacy_area_keys, expected_legacy_area_keys);
        assert!(legacy.release_dashboard_contract["markdown_preview"].as_str().is_some_and(
            |markdown| {
                markdown.contains("| Area | Maturity |") && !markdown.contains("| Promotion |")
            }
        ));
        assert_eq!(v2["schema_version"], 2);
        assert_eq!(v2["legacy_schema_version"], 1);
        assert_eq!(v2["v1_compatibility_projection"], {
            let mut projection = v1;
            projection
                .as_object_mut()
                .expect("v1 summary must be an object")
                .remove("release_dashboard_contract");
            projection
        });
    }

    #[test]
    fn stable_dependency_requires_explicit_activation_blocker() {
        let descriptors = [FeatureRolloutDescriptor {
            flag: FeatureRolloutFlag::NetworkedWorkers,
            owner_component: "workerd/execution backends",
            maturity: FeatureRolloutMaturity::Stable,
            required_tests: &["cargo test -p palyra-daemon --locked"],
            public_api_exposure: "runtime preview controls",
            activation_blockers: &["attestation evidence must be present"],
            acceptance_criteria: DIAGNOSTICS_ACCEPTANCE,
            deprecated_aliases: NO_DEPRECATED_ALIASES,
            migration_note: "test descriptor",
            stable_dependencies: &[FeatureRolloutFlag::ExecutionBackendNetworkedWorker],
        }];

        let error = validate_feature_rollout_maturity_descriptors(&descriptors)
            .expect_err("stable dependency without blocker must fail");

        assert_eq!(
            error,
            FeatureRolloutMaturityValidationError::StableDependencyWithoutBlocker {
                flag: "networked_workers",
                dependency: "execution_backend_networked_worker",
            }
        );
    }

    #[test]
    fn release_dashboard_blocks_stable_candidate_on_failing_gate() {
        let gates = vec![
            release_gate("replay_capture", "replay-redaction", true, true),
            release_gate("replay_capture", "replay-fixtures", false, true),
        ];
        let milestones = vec![ReleaseMilestoneStatus {
            area: "replay_capture".to_owned(),
            code_complete: true,
            acceptance_complete: true,
        }];
        let dashboard = build_release_acceptance_dashboard(
            &FeatureRolloutsConfig::default(),
            ReleaseDashboardInput {
                generated_at_unix_ms: 1_730_000_000_000,
                gates: gates.as_slice(),
                milestone_statuses: milestones.as_slice(),
                usage: None,
            },
        );
        let replay = dashboard["areas"]
            .as_array()
            .expect("areas should be an array")
            .iter()
            .find(|area| area["area"] == "replay_capture")
            .expect("replay area should be present");

        assert_eq!(replay["code_complete"], true);
        assert_eq!(replay["tested"], false);
        assert_eq!(replay["stable_candidate"], false);
        assert_eq!(dashboard["failing_gates"].as_array().expect("failing gates").len(), 1);
    }

    #[test]
    fn release_dashboard_does_not_equate_static_proofs_with_stable_readiness() {
        let gates = vec![
            release_gate("compaction_safeguard", "direct-hot-path", true, true),
            release_gate("compaction_safeguard", "no-hidden-fallback", true, true),
        ];
        let milestones = vec![ReleaseMilestoneStatus {
            area: "compaction_safeguard".to_owned(),
            code_complete: true,
            acceptance_complete: true,
        }];
        let dashboard = build_release_acceptance_dashboard(
            &FeatureRolloutsConfig::default(),
            ReleaseDashboardInput {
                generated_at_unix_ms: 1_730_000_000_000,
                gates: gates.as_slice(),
                milestone_statuses: milestones.as_slice(),
                usage: None,
            },
        );
        let compaction = dashboard["areas"]
            .as_array()
            .expect("areas should be an array")
            .iter()
            .find(|area| area["area"] == "compaction_safeguard")
            .expect("compaction area should be present");

        assert_eq!(compaction["promotion_state"], "gated_production");
        assert_eq!(compaction["promotion_evidence_ready"], true);
        assert_eq!(compaction["qualified_hot_path"], false);
        assert_eq!(compaction["stable_evidence_gates_ready"], false);
        assert_eq!(compaction["stable_candidate"], false);
    }

    #[test]
    fn release_dashboard_keeps_authored_blockers_after_runtime_and_release_evidence() {
        let gates = STABLE_CANDIDATE_REQUIRED_GATE_IDS
            .into_iter()
            .map(|id| release_gate("compaction_safeguard", id, true, true))
            .collect::<Vec<_>>();
        let milestones = vec![ReleaseMilestoneStatus {
            area: "compaction_safeguard".to_owned(),
            code_complete: true,
            acceptance_complete: true,
        }];
        let config = FeatureRolloutsConfig {
            compaction_safeguard: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let registry = FeatureUsageRegistry::new();
        registry.record(
            "01ARZ3NDEKTSV4RRFFQ69G5FBD",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("01ARZ3NDEKTSV4RRFFQ69G5FBD");
        let usage = registry.snapshot();
        let dashboard = build_release_acceptance_dashboard(
            &config,
            ReleaseDashboardInput {
                generated_at_unix_ms: 1_730_000_000_000,
                gates: gates.as_slice(),
                milestone_statuses: milestones.as_slice(),
                usage: Some(&usage),
            },
        );
        let compaction = dashboard["areas"]
            .as_array()
            .expect("areas should be an array")
            .iter()
            .find(|area| area["area"] == "compaction_safeguard")
            .expect("compaction area should be present");

        assert_eq!(compaction["qualified_hot_path"], true);
        assert_eq!(compaction["stable_evidence_gates_ready"], true);
        assert_eq!(compaction["stable_candidate"], false);
        assert!(compaction["blockers"].as_array().is_some_and(|blockers| {
            blockers.iter().any(|blocker| {
                blocker.as_str().is_some_and(|text| {
                    text.contains("stable promotion requires qualified production-window usage")
                })
            })
        }));
    }

    #[test]
    fn release_dashboard_renders_dependencies_and_manual_override_audit() {
        let mut gate = release_gate("agent_harness_runtime", "harness-conformance", false, true);
        gate.blocking_dependencies.push("execution_gate_pipeline_v2".to_owned());
        gate.manual_override = Some(ReleaseManualOverride {
            approval_ref: "approval-123".to_owned(),
            actor_ref: "operator:release".to_owned(),
            reason_code: "release_review.exception".to_owned(),
        });
        let gates = vec![gate];
        let milestones = vec![ReleaseMilestoneStatus {
            area: "agent_harness_runtime".to_owned(),
            code_complete: true,
            acceptance_complete: true,
        }];
        let dashboard = build_release_acceptance_dashboard(
            &FeatureRolloutsConfig::default(),
            ReleaseDashboardInput {
                generated_at_unix_ms: 1_730_000_000_000,
                gates: gates.as_slice(),
                milestone_statuses: milestones.as_slice(),
                usage: None,
            },
        );
        let markdown = render_release_acceptance_dashboard_markdown(&dashboard);

        assert!(markdown.contains("Release Acceptance Dashboard"));
        assert!(markdown.contains("agent_harness_runtime"));
        assert!(markdown.contains("execution_gate_pipeline_v2"));
        assert_eq!(dashboard["manual_overrides"].as_array().expect("manual overrides").len(), 1);
        assert_eq!(dashboard["manual_overrides"][0]["audit_required"], true);
    }

    fn release_gate(area: &str, id: &str, passed: bool, required: bool) -> ReleaseAcceptanceGate {
        ReleaseAcceptanceGate {
            id: id.to_owned(),
            area: area.to_owned(),
            passed,
            required,
            evidence_ref: format!("qa://{area}/{id}"),
            blocking_dependencies: Vec::new(),
            manual_override: None,
        }
    }
}
