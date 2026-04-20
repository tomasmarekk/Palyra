use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

pub const DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER";
pub const CONTEXT_ENGINE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_CONTEXT_ENGINE";
pub const EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE";
pub const EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_NETWORKED_WORKER";
pub const EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL";
pub const SAFETY_BOUNDARY_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_SAFETY_BOUNDARY";
pub const EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_GATE_PIPELINE_V2";
pub const SESSION_QUEUE_POLICY_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY";
pub const PRUNING_POLICY_MATRIX_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_PRUNING_POLICY_MATRIX";
pub const RETRIEVAL_DUAL_PATH_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_RETRIEVAL_DUAL_PATH";
pub const AUXILIARY_EXECUTOR_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_AUXILIARY_EXECUTOR";
pub const FLOW_ORCHESTRATION_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_FLOW_ORCHESTRATION";
pub const DELIVERY_ARBITRATION_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_DELIVERY_ARBITRATION";
pub const REPLAY_CAPTURE_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_REPLAY_CAPTURE";
pub const NETWORKED_WORKERS_ROLLOUT_ENV: &str = "PALYRA_EXPERIMENTAL_NETWORKED_WORKERS";

pub const DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.dynamic_tool_builder";
pub const CONTEXT_ENGINE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.context_engine";
pub const EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_remote_node";
pub const EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_networked_worker";
pub const EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_backend_ssh_tunnel";
pub const SAFETY_BOUNDARY_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.safety_boundary";
pub const EXECUTION_GATE_PIPELINE_V2_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.execution_gate_pipeline_v2";
pub const SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.session_queue_policy";
pub const PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH: &str =
    "feature_rollouts.pruning_policy_matrix";
pub const RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.retrieval_dual_path";
pub const AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.auxiliary_executor";
pub const FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.flow_orchestration";
pub const DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.delivery_arbitration";
pub const REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.replay_capture";
pub const NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH: &str = "feature_rollouts.networked_workers";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureRolloutSource {
    Default,
    Config,
    Env,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureRolloutSetting {
    pub enabled: bool,
    pub source: FeatureRolloutSource,
}

impl FeatureRolloutSetting {
    #[must_use]
    pub const fn from_config(enabled: bool) -> Self {
        Self { enabled, source: FeatureRolloutSource::Config }
    }

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
