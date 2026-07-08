//! Feature-rollout maturity matrix shared by daemon diagnostics and doctor.
//!
//! Rollout enablement answers whether a flag is on right now; this matrix
//! answers whether the capability is safe to promote, which owners and tests
//! gate it, and what still blocks activation.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fmt,
};

use palyra_common::feature_rollouts::{self, FeatureRolloutSetting, FeatureRolloutSource};
use serde_json::{json, Map, Value};

use crate::config::FeatureRolloutsConfig;

pub(crate) const FEATURE_ROLLOUT_MATURITY_SCHEMA_VERSION: u32 = 1;

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
    fn to_diagnostics_value(self, config: &FeatureRolloutsConfig) -> Value {
        let setting = self.flag.setting(config);
        let mut activation_blockers = Vec::new();
        if !setting.enabled {
            activation_blockers.push(format!(
                "Enable {} or {} after required tests and owner acceptance pass.",
                self.flag.config_path(),
                self.flag.env_var()
            ));
        }
        activation_blockers
            .extend(self.activation_blockers.iter().map(|value| (*value).to_owned()));

        json!({
            "enabled": setting.enabled,
            "source": setting.source,
            "config_path": self.flag.config_path(),
            "env_var": self.flag.env_var(),
            "default_posture": rollout_default_posture(setting),
            "rollback_knob": {
                "env_var": self.flag.env_var(),
                "config_path": self.flag.config_path(),
                "safe_default": "disabled",
                "operator_action": format!(
                    "unset {} and remove or set {} = false",
                    self.flag.env_var(),
                    self.flag.config_path()
                ),
            },
            "maturity": self.maturity,
            "owner_component": self.owner_component,
            "required_tests": self.required_tests,
            "public_api_exposure": self.public_api_exposure,
            "activation_blockers": activation_blockers,
            "acceptance_criteria": self.acceptance_criteria,
            "promotion_gate": {
                "default_enable_allowed": self.maturity.default_enable_allowed(),
                "default_enable_blockers": self.default_enable_blockers(),
                "transition_criteria": self.maturity.transition_criteria(),
                "test_coverage_marker": if self.required_tests.is_empty() || self.acceptance_criteria.is_empty() {
                    "missing_required_test_or_acceptance_criteria"
                } else {
                    "required_tests_and_acceptance_criteria_present"
                },
                "release_gate": ROLLOUT_PROMOTION_GATE,
            },
            "deprecated_aliases": self.deprecated_aliases,
            "migration_note": self.migration_note,
            "inactive_reason": if setting.enabled {
                None::<String>
            } else {
                Some(format!(
                    "{} is disabled from {:?}; activation remains blocked until the listed tests and owner acceptance pass",
                    self.flag.as_str(),
                    feature_rollout_source_as_str(setting.source)
                ))
            },
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
        maturity: FeatureRolloutMaturity::GatedProduction,
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
        maturity: FeatureRolloutMaturity::GatedProduction,
        required_tests: CORE_VISIBILITY_TESTS,
        public_api_exposure: OBSERVABILITY_EXPOSURE,
        activation_blockers: &[
            "production path is currently always on; rollout gate is diagnostic-only until channel kernel gating lands",
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
        maturity: FeatureRolloutMaturity::GatedProduction,
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
    validate_feature_rollout_maturity_descriptors(FEATURE_ROLLOUT_DESCRIPTORS)
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
    MissingRequiredTests { flag: &'static str },
    MissingActivationBlockers { flag: &'static str },
    MissingAcceptanceCriteria { flag: &'static str },
    StableWithoutAcceptance { flag: &'static str },
    StableDependencyWithoutBlocker { flag: &'static str, dependency: &'static str },
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
        }
    }
}

impl Error for FeatureRolloutMaturityValidationError {}

pub(crate) fn build_feature_rollout_diagnostics(config: &FeatureRolloutsConfig) -> Value {
    let mut map = Map::new();
    for descriptor in FEATURE_ROLLOUT_DESCRIPTORS {
        map.insert(descriptor.flag.as_str().to_owned(), descriptor.to_diagnostics_value(config));
    }
    Value::Object(map)
}

pub(crate) fn build_feature_rollout_maturity_summary(config: &FeatureRolloutsConfig) -> Value {
    let mut summary = build_feature_rollout_maturity_summary_base(config);
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "release_dashboard_contract".to_owned(),
            build_release_dashboard_contract(config),
        );
    }
    summary
}

fn build_feature_rollout_maturity_summary_base(config: &FeatureRolloutsConfig) -> Value {
    let mut maturity_counts = BTreeMap::new();
    for maturity in FEATURE_ROLLOUT_MATURITY_STATES {
        maturity_counts.insert(maturity.as_str(), 0_usize);
    }
    let mut enabled_flags = 0_usize;
    let mut inactive_flags = 0_usize;
    let mut blocked_flags = 0_usize;

    for descriptor in FEATURE_ROLLOUT_DESCRIPTORS {
        let setting = descriptor.flag.setting(config);
        if setting.enabled {
            enabled_flags = enabled_flags.saturating_add(1);
        } else {
            inactive_flags = inactive_flags.saturating_add(1);
        }
        if descriptor.maturity == FeatureRolloutMaturity::Blocked {
            blocked_flags = blocked_flags.saturating_add(1);
        }
        *maturity_counts.entry(descriptor.maturity.as_str()).or_insert(0_usize) += 1;
    }

    json!({
        "schema_version": FEATURE_ROLLOUT_MATURITY_SCHEMA_VERSION,
        "flag_count": FEATURE_ROLLOUT_DESCRIPTORS.len(),
        "enabled_flags": enabled_flags,
        "inactive_flags": inactive_flags,
        "blocked_flags": blocked_flags,
        "maturity_counts": maturity_counts,
        "deprecated_alias_policy": "deprecated rollout aliases are not accepted; use the listed config_path or env_var",
        "migration_note": "flag renames must add a deprecated_aliases entry here before aliases are removed from loaders",
    })
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

pub(crate) struct ReleaseDashboardInput<'a> {
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) gates: &'a [ReleaseAcceptanceGate],
    pub(crate) milestone_statuses: &'a [ReleaseMilestoneStatus],
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
        "maturity_summary": build_feature_rollout_maturity_summary_base(config),
        "roadmap_checkbox_policy": "roadmap acceptance complete does not imply stable candidate",
    })
}

fn build_release_dashboard_contract(config: &FeatureRolloutsConfig) -> Value {
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
        },
    );
    let markdown_preview = render_release_acceptance_dashboard_markdown(&dashboard);
    if let Some(object) = dashboard.as_object_mut() {
        object.insert("markdown_preview".to_owned(), Value::String(markdown_preview));
    }
    dashboard
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

fn release_dashboard_area_report(
    descriptor: FeatureRolloutDescriptor,
    config: &FeatureRolloutsConfig,
    milestone_statuses: &BTreeMap<&str, &ReleaseMilestoneStatus>,
    gates: &[ReleaseAcceptanceGate],
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
    let default_enable_allowed = descriptor.maturity.default_enable_allowed();
    let stable_candidate = code_complete && acceptance_complete && tested && default_enable_allowed;
    let setting = descriptor.flag.setting(config);
    let mut blockers = descriptor
        .activation_blockers
        .iter()
        .map(|blocker| (*blocker).to_owned())
        .collect::<Vec<_>>();
    if !failing_required_gates.is_empty() {
        blockers.push(format!("failing required gates: {}", failing_required_gates.join(", ")));
    }
    for gate in &area_gates {
        blockers.extend(gate.blocking_dependencies.iter().cloned());
    }

    json!({
        "area": flag,
        "owner_component": descriptor.owner_component,
        "maturity": descriptor.maturity.as_str(),
        "enabled": setting.enabled,
        "code_complete": code_complete,
        "acceptance_complete": acceptance_complete,
        "tested": tested,
        "gated_production": matches!(descriptor.maturity, FeatureRolloutMaturity::GatedProduction | FeatureRolloutMaturity::Stable),
        "stable_candidate": stable_candidate,
        "required_tests": descriptor.required_tests,
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
    use super::{
        build_feature_rollout_diagnostics, build_release_acceptance_dashboard,
        render_release_acceptance_dashboard_markdown,
        validate_feature_rollout_maturity_descriptors, FeatureRolloutDescriptor,
        FeatureRolloutFlag, FeatureRolloutMaturity, FeatureRolloutMaturityValidationError,
        ReleaseAcceptanceGate, ReleaseDashboardInput, ReleaseManualOverride,
        ReleaseMilestoneStatus, DIAGNOSTICS_ACCEPTANCE, FEATURE_ROLLOUT_DESCRIPTORS,
        NO_DEPRECATED_ALIASES, NO_STABLE_DEPENDENCIES,
    };
    use crate::config::FeatureRolloutsConfig;
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
        let diagnostics = build_feature_rollout_diagnostics(&FeatureRolloutsConfig::default());
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
