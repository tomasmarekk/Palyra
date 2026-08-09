//! Experimental feature rollout flags: env/config key names and boolish parsing.
//!
//! Every experimental subsystem is toggled through a `PALYRA_EXPERIMENTAL_*` variable or
//! a `feature_rollouts.*` config path defined here so flag names stay consistent across
//! the daemon, CLI, and tests. All rollouts default to off.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Env toggle for the experimental dynamic tool builder.
pub const DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER";
/// Env toggle for the experimental context engine.
pub const CONTEXT_ENGINE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_CONTEXT_ENGINE";
/// Env toggle for the experimental remote-node execution backend.
pub const EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE";
/// Env toggle for the experimental networked-worker execution backend.
pub const EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER";
/// Env toggle for the experimental Docker execution backend.
pub const EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_DOCKER";
/// Env toggle for the experimental SSH-tunnel execution backend.
pub const EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL";
/// Env toggle for the experimental safety boundary.
pub const SAFETY_BOUNDARY_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_SAFETY_BOUNDARY";
/// Env toggle for the experimental execution gate pipeline v2.
pub const EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_GATE_PIPELINE_V2";
/// Env toggle for the experimental agent harness runtime boundary.
pub const AGENT_HARNESS_RUNTIME_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_AGENT_HARNESS_RUNTIME";
/// Env toggle for the experimental inline runtime hook call sites.
pub const INLINE_RUNTIME_HOOKS_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_INLINE_RUNTIME_HOOKS";
/// Env toggle for the experimental tool-result middleware pipeline.
pub const TOOL_RESULT_MIDDLEWARE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_TOOL_RESULT_MIDDLEWARE";
/// Env toggle for the experimental session queue policy.
pub const SESSION_QUEUE_POLICY_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY";
/// Env toggle for the experimental pruning policy matrix.
pub const PRUNING_POLICY_MATRIX_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_PRUNING_POLICY_MATRIX";
/// Env toggle for the experimental dual-path retrieval.
pub const RETRIEVAL_DUAL_PATH_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_RETRIEVAL_DUAL_PATH";
/// Env toggle for the experimental auxiliary executor.
pub const AUXILIARY_EXECUTOR_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_AUXILIARY_EXECUTOR";
/// Env toggle for the experimental flow orchestration.
pub const FLOW_ORCHESTRATION_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_FLOW_ORCHESTRATION";
/// Env toggle for the experimental delivery arbitration.
pub const DELIVERY_ARBITRATION_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_DELIVERY_ARBITRATION";
/// Env toggle for the experimental replay capture.
pub const REPLAY_CAPTURE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_REPLAY_CAPTURE";
/// Env toggle for experimental networked workers.
pub const NETWORKED_WORKERS_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_NETWORKED_WORKERS";
/// Env toggle for experimental tool-call repair.
pub const TOOL_REPAIR_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_TOOL_REPAIR";
/// Env toggle for experimental provider stream normalization.
pub const PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_PROVIDER_STREAM_NORMALIZER";
/// Env toggle for the experimental provider turn recovery classifier.
pub const PROVIDER_RECOVERY_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_PROVIDER_RECOVERY";
/// Env toggle for the experimental persistent terminal session model.
pub const TERMINAL_SESSIONS_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_TERMINAL_SESSIONS";
/// Env toggle for the experimental browser rescue toolchain.
pub const BROWSER_RESCUE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_BROWSER_RESCUE";
/// Env toggle for automatic browser process and target recovery.
pub const BROWSER_RESILIENCE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_BROWSER_RESILIENCE";
/// Env toggle for connector-neutral audio transcription and synthesis.
pub const AUDIO_PIPELINE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_AUDIO_PIPELINE";
/// Env toggle for isolated remote computer-use execution.
pub const COMPUTER_USE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_COMPUTER_USE";
/// Env toggle for evidence-based semantic-memory consolidation.
pub const SEMANTIC_MEMORY_CONSOLIDATION_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_SEMANTIC_MEMORY_CONSOLIDATION";
/// Env toggle for the experimental LSP service lifecycle.
pub const LSP_SERVICE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_LSP_SERVICE";
/// Env toggle for the experimental advisor fanout runtime.
pub const ADVISOR_FANOUT_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_ADVISOR_FANOUT";
/// Env toggle for the experimental ACP runtime manager.
pub const ACP_RUNTIME_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_ACP_RUNTIME";
/// Env toggle for the experimental channel turn kernel.
pub const CHANNEL_TURN_KERNEL_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_CHANNEL_TURN_KERNEL";
/// Env toggle for experimental model-visible agent plan state.
pub const AGENT_PLAN_STATE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_AGENT_PLAN_STATE";
/// Env toggle for the experimental objective judge loop.
pub const OBJECTIVE_JUDGE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_OBJECTIVE_JUDGE";
/// Env toggle for the experimental verification runtime.
pub const VERIFICATION_RUNTIME_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_VERIFICATION_RUNTIME";
/// Env toggle for experimental progress drafts.
pub const PROGRESS_DRAFTS_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_PROGRESS_DRAFTS";
/// Env toggle for the experimental compaction safeguard.
pub const COMPACTION_SAFEGUARD_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_COMPACTION_SAFEGUARD";
/// Env toggle for experimental provider-backed evidence compaction.
pub const PROVIDER_BACKED_EVIDENCE_COMPACTION_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_PROVIDER_BACKED_EVIDENCE_COMPACTION";
/// Env toggle for the experimental attack-surface audit.
pub const ATTACK_SURFACE_AUDIT_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_ATTACK_SURFACE_AUDIT";

/// Config path for [`DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV`].
pub const DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.dynamic_tool_builder";
/// Config path for [`CONTEXT_ENGINE_ROLLOUT_ENV`].
pub const CONTEXT_ENGINE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.context_engine";
/// Config path for [`EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV`].
pub const EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_remote_node";
/// Config path for [`EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV`].
pub const EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_networked_worker";
/// Config path for [`EXECUTION_BACKEND_DOCKER_ROLLOUT_ENV`].
pub const EXECUTION_BACKEND_DOCKER_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_docker";
/// Config path for [`EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV`].
pub const EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_ssh_tunnel";
/// Config path for [`SAFETY_BOUNDARY_ROLLOUT_ENV`].
pub const SAFETY_BOUNDARY_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.safety_boundary";
/// Config path for [`EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV`].
pub const EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_gate_pipeline_v2";
/// Config path for [`AGENT_HARNESS_RUNTIME_ROLLOUT_ENV`].
pub const AGENT_HARNESS_RUNTIME_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.agent_harness_runtime";
/// Config path for [`INLINE_RUNTIME_HOOKS_ROLLOUT_ENV`].
pub const INLINE_RUNTIME_HOOKS_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.inline_runtime_hooks";
/// Config path for [`TOOL_RESULT_MIDDLEWARE_ROLLOUT_ENV`].
pub const TOOL_RESULT_MIDDLEWARE_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.tool_result_middleware";
/// Config path for [`SESSION_QUEUE_POLICY_ROLLOUT_ENV`].
pub const SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.session_queue_policy";
/// Config path for [`PRUNING_POLICY_MATRIX_ROLLOUT_ENV`].
pub const PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.pruning_policy_matrix";
/// Config path for [`RETRIEVAL_DUAL_PATH_ROLLOUT_ENV`].
pub const RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.retrieval_dual_path";
/// Config path for [`AUXILIARY_EXECUTOR_ROLLOUT_ENV`].
pub const AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.auxiliary_executor";
/// Config path for [`FLOW_ORCHESTRATION_ROLLOUT_ENV`].
pub const FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.flow_orchestration";
/// Config path for [`DELIVERY_ARBITRATION_ROLLOUT_ENV`].
pub const DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.delivery_arbitration";
/// Config path for [`REPLAY_CAPTURE_ROLLOUT_ENV`].
pub const REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.replay_capture";
/// Config path for [`NETWORKED_WORKERS_ROLLOUT_ENV`].
pub const NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.networked_workers";
/// Config path for [`TOOL_REPAIR_ROLLOUT_ENV`].
pub const TOOL_REPAIR_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.tool_repair";
/// Config path for [`PROVIDER_STREAM_NORMALIZER_ROLLOUT_ENV`].
pub const PROVIDER_STREAM_NORMALIZER_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.provider_stream_normalizer";
/// Config path for [`PROVIDER_RECOVERY_ROLLOUT_ENV`].
pub const PROVIDER_RECOVERY_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.provider_recovery";
/// Config path for [`TERMINAL_SESSIONS_ROLLOUT_ENV`].
pub const TERMINAL_SESSIONS_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.terminal_sessions";
/// Config path for [`BROWSER_RESCUE_ROLLOUT_ENV`].
pub const BROWSER_RESCUE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.browser_rescue";
/// Config path for [`BROWSER_RESILIENCE_ROLLOUT_ENV`].
pub const BROWSER_RESILIENCE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.browser_resilience";
/// Config path for [`AUDIO_PIPELINE_ROLLOUT_ENV`].
pub const AUDIO_PIPELINE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.audio_pipeline";
/// Config path for [`COMPUTER_USE_ROLLOUT_ENV`].
pub const COMPUTER_USE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.computer_use";
/// Config path for [`SEMANTIC_MEMORY_CONSOLIDATION_ROLLOUT_ENV`].
pub const SEMANTIC_MEMORY_CONSOLIDATION_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.semantic_memory_consolidation";
/// Config path for [`LSP_SERVICE_ROLLOUT_ENV`].
pub const LSP_SERVICE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.lsp_service";
/// Config path for [`ADVISOR_FANOUT_ROLLOUT_ENV`].
pub const ADVISOR_FANOUT_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.advisor_fanout";
/// Config path for [`ACP_RUNTIME_ROLLOUT_ENV`].
pub const ACP_RUNTIME_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.acp_runtime";
/// Config path for [`CHANNEL_TURN_KERNEL_ROLLOUT_ENV`].
pub const CHANNEL_TURN_KERNEL_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.channel_turn_kernel";
/// Config path for [`AGENT_PLAN_STATE_ROLLOUT_ENV`].
pub const AGENT_PLAN_STATE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.agent_plan_state";
/// Config path for [`OBJECTIVE_JUDGE_ROLLOUT_ENV`].
pub const OBJECTIVE_JUDGE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.objective_judge";
/// Config path for [`VERIFICATION_RUNTIME_ROLLOUT_ENV`].
pub const VERIFICATION_RUNTIME_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.verification_runtime";
/// Config path for [`PROGRESS_DRAFTS_ROLLOUT_ENV`].
pub const PROGRESS_DRAFTS_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.progress_drafts";
/// Config path for [`COMPACTION_SAFEGUARD_ROLLOUT_ENV`].
pub const COMPACTION_SAFEGUARD_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.compaction_safeguard";
/// Config path for [`PROVIDER_BACKED_EVIDENCE_COMPACTION_ROLLOUT_ENV`].
pub const PROVIDER_BACKED_EVIDENCE_COMPACTION_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.provider_backed_evidence_compaction";
/// Config path for [`ATTACK_SURFACE_AUDIT_ROLLOUT_ENV`].
pub const ATTACK_SURFACE_AUDIT_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.attack_surface_audit";

/// Where a rollout decision came from, for diagnostics and precedence reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureRolloutSource {
    Default,
    Config,
    Env,
}

/// A resolved rollout flag value together with its provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureRolloutSetting {
    pub enabled: bool,
    pub source: FeatureRolloutSource,
}

impl FeatureRolloutSetting {
    /// Builds a setting resolved from the config file.
    #[must_use]
    pub const fn from_config(enabled: bool) -> Self {
        Self { enabled, source: FeatureRolloutSource::Config }
    }

    /// Builds a setting resolved from an environment variable.
    #[must_use]
    pub const fn from_env(enabled: bool) -> Self {
        Self { enabled, source: FeatureRolloutSource::Env }
    }
}

impl Default for FeatureRolloutSetting {
    fn default() -> Self {
        Self { enabled: false, source: FeatureRolloutSource::Default }
    }
}

/// A rollout value that does not parse as any accepted boolean alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureRolloutParseError {
    source_name: String,
    value: String,
}

impl fmt::Display for FeatureRolloutParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} must be a boolean-like value (1/0, true/false, yes/no, on/off); got '{}'",
            self.source_name, self.value
        )
    }
}

impl Error for FeatureRolloutParseError {}

/// Parses a rollout flag value, accepting 1/0, true/false, yes/no, and on/off
/// (case-insensitive, surrounding whitespace ignored).
///
/// # Errors
/// Returns [`FeatureRolloutParseError`] naming `source_name` for any other value, so an
/// operator typo fails loudly instead of silently disabling a rollout.
pub fn parse_boolish_feature_rollout(
    raw: &str,
    source_name: &str,
) -> Result<bool, FeatureRolloutParseError> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(FeatureRolloutParseError {
            source_name: source_name.to_owned(),
            value: raw.trim().to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_boolish_feature_rollout, FeatureRolloutSetting, FeatureRolloutSource};

    #[test]
    fn boolish_rollout_parser_accepts_true_aliases() {
        for raw in ["1", "true", "TRUE", " yes ", "On"] {
            let parsed = parse_boolish_feature_rollout(raw, "TEST_ROLLOUT")
                .expect("true alias should parse");
            assert!(parsed, "expected '{raw}' to parse as true");
        }
    }

    #[test]
    fn boolish_rollout_parser_accepts_false_aliases() {
        for raw in ["0", "false", "FALSE", " no ", "Off"] {
            let parsed = parse_boolish_feature_rollout(raw, "TEST_ROLLOUT")
                .expect("false alias should parse");
            assert!(!parsed, "expected '{raw}' to parse as false");
        }
    }

    #[test]
    fn boolish_rollout_parser_rejects_unknown_values() {
        let error = parse_boolish_feature_rollout("maybe", "TEST_ROLLOUT")
            .expect_err("invalid value should fail");
        assert!(error.to_string().contains("TEST_ROLLOUT"));
        assert!(error.to_string().contains("maybe"));
    }

    #[test]
    fn feature_rollout_setting_defaults_off_with_default_source() {
        let setting = FeatureRolloutSetting::default();
        assert!(!setting.enabled);
        assert_eq!(setting.source, FeatureRolloutSource::Default);
    }
}
