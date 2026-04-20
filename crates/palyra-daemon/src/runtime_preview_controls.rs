use palyra_common::{
    feature_rollouts::{
        AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH, AUXILIARY_EXECUTOR_ROLLOUT_ENV,
        DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH, DELIVERY_ARBITRATION_ROLLOUT_ENV,
        EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH,
        EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV, FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH,
        FLOW_ORCHESTRATION_ROLLOUT_ENV, NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH,
        NETWORKED_WORKERS_ROLLOUT_ENV, PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH,
        PRUNING_POLICY_MATRIX_ROLLOUT_ENV, REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH,
        REPLAY_CAPTURE_ROLLOUT_ENV, RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH,
        RETRIEVAL_DUAL_PATH_ROLLOUT_ENV, SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH,
        SESSION_QUEUE_POLICY_ROLLOUT_ENV,
    },
    runtime_preview::{
        RuntimePreviewCapability, RuntimePreviewCapabilityConfigSnapshot,
        RuntimePreviewConfigSnapshot, RuntimePreviewEffectiveState, RuntimePreviewMode,
        RuntimePreviewSummaryState, ALL_RUNTIME_PREVIEW_CAPABILITIES,
        RUNTIME_PREVIEW_SCHEMA_VERSION,
    },
};
use serde_json::{json, Value};

use crate::config::{
    AuxiliaryExecutorConfig, DeliveryArbitrationConfig, FlowOrchestrationConfig, LoadedConfig,
    NetworkedWorkersConfig, PruningPolicyMatrixConfig, ReplayCaptureConfig,
    RetrievalDualPathConfig, SessionQueuePolicyConfig,
};
use crate::gateway::GatewayRuntimeConfigSnapshot;

#[must_use]
pub(crate) fn build_runtime_preview_config_snapshot<C: RuntimePreviewConfigView>(
    config: &C,
) -> RuntimePreviewConfigSnapshot {
    let capabilities = ALL_RUNTIME_PREVIEW_CAPABILITIES
        .into_iter()
        .map(|capability| capability_snapshot(config, capability))
        .collect::<Vec<_>>();
    let preview_capabilities = capabilities
        .iter()
        .filter(|entry| matches!(entry.effective_state, RuntimePreviewEffectiveState::PreviewOnly))
        .count();
    let enabled_capabilities = capabilities
        .iter()
        .filter(|entry| matches!(entry.effective_state, RuntimePreviewEffectiveState::Enabled))
        .count();
    let blocked_capabilities = capabilities
        .iter()
        .filter(|entry| matches!(entry.effective_state, RuntimePreviewEffectiveState::Blocked))
        .count();
    let disabled_capabilities = capabilities
        .iter()
        .filter(|entry| matches!(entry.effective_state, RuntimePreviewEffectiveState::Disabled))
        .count();
    let state = if disabled_capabilities == capabilities.len() {
        RuntimePreviewSummaryState::Disabled
    } else if enabled_capabilities == capabilities.len() {
        RuntimePreviewSummaryState::Enabled
    } else if preview_capabilities > 0 && enabled_capabilities == 0 && blocked_capabilities == 0 {
        RuntimePreviewSummaryState::PreviewOnly
    } else if blocked_capabilities > 0 && preview_capabilities == 0 && enabled_capabilities == 0 {
        RuntimePreviewSummaryState::Blocked
    } else {
        RuntimePreviewSummaryState::Mixed
    };
    RuntimePreviewConfigSnapshot {
        schema_version: RUNTIME_PREVIEW_SCHEMA_VERSION,
        state,
        preview_capabilities,
        enabled_capabilities,
        blocked_capabilities,
        disabled_capabilities,
        capabilities,
    }
}

#[must_use]
pub(crate) fn capability_snapshot<C: RuntimePreviewConfigView>(
    config: &C,
    capability: RuntimePreviewCapability,
) -> RuntimePreviewCapabilityConfigSnapshot {
    let (mode, rollout_enabled, rollout_source, rollout_env_var, rollout_config_path, settings) =
        capability_config(config, capability);
    let activation_blockers = capability_blockers(config, capability, mode, rollout_enabled);
    let effective_state = match mode {
        RuntimePreviewMode::Disabled => RuntimePreviewEffectiveState::Disabled,
        RuntimePreviewMode::PreviewOnly if activation_blockers.is_empty() => {
            RuntimePreviewEffectiveState::PreviewOnly
        }
        RuntimePreviewMode::Enabled if activation_blockers.is_empty() => {
            RuntimePreviewEffectiveState::Enabled
        }
        RuntimePreviewMode::PreviewOnly | RuntimePreviewMode::Enabled => {
            RuntimePreviewEffectiveState::Blocked
        }
    };
    RuntimePreviewCapabilityConfigSnapshot {
        capability,
        label: capability.label().to_owned(),
        summary: capability.summary().to_owned(),
        mode,
        effective_state,
        rollout_enabled,
        rollout_source,
        rollout_env_var: rollout_env_var.to_owned(),
        rollout_config_path: rollout_config_path.to_owned(),
        config_section: capability.as_str().to_owned(),
        activation_blockers,
        settings,
    }
}

#[must_use]
pub(crate) fn capability_active<C: RuntimePreviewConfigView>(
    config: &C,
    capability: RuntimePreviewCapability,
) -> bool {
    matches!(
        capability_snapshot(config, capability).effective_state,
        RuntimePreviewEffectiveState::PreviewOnly | RuntimePreviewEffectiveState::Enabled
    )
}

#[must_use]
pub(crate) fn capability_blocker_message<C: RuntimePreviewConfigView>(
    config: &C,
    capability: RuntimePreviewCapability,
) -> Option<String> {
    let snapshot = capability_snapshot(config, capability);
    if matches!(
        snapshot.effective_state,
        RuntimePreviewEffectiveState::Disabled | RuntimePreviewEffectiveState::Blocked
    ) {
        let blocker_summary = if snapshot.activation_blockers.is_empty() {
            "no activation blockers were published".to_owned()
        } else {
            snapshot.activation_blockers.join("; ")
        };
        return Some(format!(
            "{} is {}. {}",
            snapshot.label,
            snapshot.effective_state.as_str(),
            blocker_summary
        ));
    }
    None
}

fn capability_config<C: RuntimePreviewConfigView>(
    config: &C,
    capability: RuntimePreviewCapability,
) -> (
    RuntimePreviewMode,
    bool,
    palyra_common::feature_rollouts::FeatureRolloutSource,
    &'static str,
    &'static str,
    Value,
) {
    match capability {
        RuntimePreviewCapability::SessionQueuePolicy => runtime_preview_section(
            config.session_queue_policy(),
            config.feature_rollouts().session_queue_policy.enabled,
            config.feature_rollouts().session_queue_policy.source,
            SESSION_QUEUE_POLICY_ROLLOUT_ENV,
            SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::PruningPolicyMatrix => runtime_preview_section(
            config.pruning_policy_matrix(),
            config.feature_rollouts().pruning_policy_matrix.enabled,
            config.feature_rollouts().pruning_policy_matrix.source,
            PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
            PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::RetrievalDualPath => runtime_preview_section(
            config.retrieval_dual_path(),
            config.feature_rollouts().retrieval_dual_path.enabled,
            config.feature_rollouts().retrieval_dual_path.source,
            RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
            RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::AuxiliaryExecutor => runtime_preview_section(
            config.auxiliary_executor(),
            config.feature_rollouts().auxiliary_executor.enabled,
            config.feature_rollouts().auxiliary_executor.source,
            AUXILIARY_EXECUTOR_ROLLOUT_ENV,
            AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::FlowOrchestration => runtime_preview_section(
            config.flow_orchestration(),
            config.feature_rollouts().flow_orchestration.enabled,
            config.feature_rollouts().flow_orchestration.source,
            FLOW_ORCHESTRATION_ROLLOUT_ENV,
            FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::DeliveryArbitration => runtime_preview_section(
            config.delivery_arbitration(),
            config.feature_rollouts().delivery_arbitration.enabled,
            config.feature_rollouts().delivery_arbitration.source,
            DELIVERY_ARBITRATION_ROLLOUT_ENV,
            DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::ReplayCapture => runtime_preview_section(
            config.replay_capture(),
            config.feature_rollouts().replay_capture.enabled,
            config.feature_rollouts().replay_capture.source,
            REPLAY_CAPTURE_ROLLOUT_ENV,
            REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH,
        ),
        RuntimePreviewCapability::NetworkedWorkers => runtime_preview_section(
            config.networked_workers(),
            config.feature_rollouts().networked_workers.enabled,
            config.feature_rollouts().networked_workers.source,
            NETWORKED_WORKERS_ROLLOUT_ENV,
            NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH,
        ),
    }
}

fn capability_blockers<C: RuntimePreviewConfigView>(
    config: &C,
    capability: RuntimePreviewCapability,
    mode: RuntimePreviewMode,
    rollout_enabled: bool,
) -> Vec<String> {
    if matches!(mode, RuntimePreviewMode::Disabled) {
        return Vec::new();
    }
    let mut blockers = Vec::new();
    if matches!(mode, RuntimePreviewMode::Enabled) && !rollout_enabled {
        blockers.push(format!(
            "Set {}=true or {}=1 before promoting {} past preview_only.",
            rollout_config_path(capability),
            rollout_env_var(capability),
            capability.as_str()
        ));
    }
    match capability {
        RuntimePreviewCapability::DeliveryArbitration
            if matches!(config.flow_orchestration().mode, RuntimePreviewMode::Disabled) =>
        {
            blockers.push(
                "flow_orchestration.mode is disabled, so descendant delivery cannot arbitrate any active flow."
                    .to_owned(),
            );
        }
        RuntimePreviewCapability::NetworkedWorkers
            if !config.feature_rollouts().execution_backend_networked_worker.enabled =>
        {
            blockers.push(format!(
                "Set {}=true or {}=1 before networked worker runtime can advertise execution readiness.",
                EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_CONFIG_PATH,
                EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV
            ));
        }
        _ => {}
    }
    blockers
}

fn rollout_env_var(capability: RuntimePreviewCapability) -> &'static str {
    match capability {
        RuntimePreviewCapability::SessionQueuePolicy => SESSION_QUEUE_POLICY_ROLLOUT_ENV,
        RuntimePreviewCapability::PruningPolicyMatrix => PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
        RuntimePreviewCapability::RetrievalDualPath => RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
        RuntimePreviewCapability::AuxiliaryExecutor => AUXILIARY_EXECUTOR_ROLLOUT_ENV,
        RuntimePreviewCapability::FlowOrchestration => FLOW_ORCHESTRATION_ROLLOUT_ENV,
        RuntimePreviewCapability::DeliveryArbitration => DELIVERY_ARBITRATION_ROLLOUT_ENV,
        RuntimePreviewCapability::ReplayCapture => REPLAY_CAPTURE_ROLLOUT_ENV,
        RuntimePreviewCapability::NetworkedWorkers => NETWORKED_WORKERS_ROLLOUT_ENV,
    }
}

fn rollout_config_path(capability: RuntimePreviewCapability) -> &'static str {
    match capability {
        RuntimePreviewCapability::SessionQueuePolicy => SESSION_QUEUE_POLICY_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::PruningPolicyMatrix => PRUNING_POLICY_MATRIX_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::RetrievalDualPath => RETRIEVAL_DUAL_PATH_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::AuxiliaryExecutor => AUXILIARY_EXECUTOR_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::FlowOrchestration => FLOW_ORCHESTRATION_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::DeliveryArbitration => DELIVERY_ARBITRATION_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::ReplayCapture => REPLAY_CAPTURE_ROLLOUT_CONFIG_PATH,
        RuntimePreviewCapability::NetworkedWorkers => NETWORKED_WORKERS_ROLLOUT_CONFIG_PATH,
    }
}

fn runtime_preview_section(
    section: &impl RuntimePreviewSection,
    rollout_enabled: bool,
    rollout_source: palyra_common::feature_rollouts::FeatureRolloutSource,
    rollout_env_var: &'static str,
    rollout_config_path: &'static str,
) -> (
    RuntimePreviewMode,
    bool,
    palyra_common::feature_rollouts::FeatureRolloutSource,
    &'static str,
    &'static str,
    Value,
) {
    (
        section.mode(),
        rollout_enabled,
        rollout_source,
        rollout_env_var,
        rollout_config_path,
        section.settings(),
    )
}

pub(crate) trait RuntimePreviewConfigView {
    fn feature_rollouts(&self) -> &crate::config::FeatureRolloutsConfig;
    fn session_queue_policy(&self) -> &SessionQueuePolicyConfig;
    fn pruning_policy_matrix(&self) -> &PruningPolicyMatrixConfig;
    fn retrieval_dual_path(&self) -> &RetrievalDualPathConfig;
    fn auxiliary_executor(&self) -> &AuxiliaryExecutorConfig;
    fn flow_orchestration(&self) -> &FlowOrchestrationConfig;
    fn delivery_arbitration(&self) -> &DeliveryArbitrationConfig;
    fn replay_capture(&self) -> &ReplayCaptureConfig;
    fn networked_workers(&self) -> &NetworkedWorkersConfig;
}

impl RuntimePreviewConfigView for LoadedConfig {
    fn feature_rollouts(&self) -> &crate::config::FeatureRolloutsConfig {
        &self.feature_rollouts
    }

    fn session_queue_policy(&self) -> &SessionQueuePolicyConfig {
        &self.session_queue_policy
    }

    fn pruning_policy_matrix(&self) -> &PruningPolicyMatrixConfig {
        &self.pruning_policy_matrix
    }

    fn retrieval_dual_path(&self) -> &RetrievalDualPathConfig {
        &self.retrieval_dual_path
    }

    fn auxiliary_executor(&self) -> &AuxiliaryExecutorConfig {
        &self.auxiliary_executor
    }

    fn flow_orchestration(&self) -> &FlowOrchestrationConfig {
        &self.flow_orchestration
    }

    fn delivery_arbitration(&self) -> &DeliveryArbitrationConfig {
        &self.delivery_arbitration
    }

    fn replay_capture(&self) -> &ReplayCaptureConfig {
        &self.replay_capture
    }

    fn networked_workers(&self) -> &NetworkedWorkersConfig {
        &self.networked_workers
    }
}

impl RuntimePreviewConfigView for GatewayRuntimeConfigSnapshot {
    fn feature_rollouts(&self) -> &crate::config::FeatureRolloutsConfig {
        &self.feature_rollouts
    }

    fn session_queue_policy(&self) -> &SessionQueuePolicyConfig {
        &self.session_queue_policy
    }

    fn pruning_policy_matrix(&self) -> &PruningPolicyMatrixConfig {
        &self.pruning_policy_matrix
    }

    fn retrieval_dual_path(&self) -> &RetrievalDualPathConfig {
        &self.retrieval_dual_path
    }

    fn auxiliary_executor(&self) -> &AuxiliaryExecutorConfig {
        &self.auxiliary_executor
    }

    fn flow_orchestration(&self) -> &FlowOrchestrationConfig {
        &self.flow_orchestration
    }

    fn delivery_arbitration(&self) -> &DeliveryArbitrationConfig {
        &self.delivery_arbitration
    }

    fn replay_capture(&self) -> &ReplayCaptureConfig {
        &self.replay_capture
    }

    fn networked_workers(&self) -> &NetworkedWorkersConfig {
        &self.networked_workers
    }
}

trait RuntimePreviewSection {
    fn mode(&self) -> RuntimePreviewMode;
    fn settings(&self) -> Value;
}

impl RuntimePreviewSection for SessionQueuePolicyConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "max_depth": self.max_depth,
            "merge_window_ms": self.merge_window_ms,
        })
    }
}

impl RuntimePreviewSection for PruningPolicyMatrixConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "manual_apply_enabled": self.manual_apply_enabled,
            "min_token_savings": self.min_token_savings,
        })
    }
}

impl RuntimePreviewSection for RetrievalDualPathConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "branch_timeout_ms": self.branch_timeout_ms,
            "prompt_budget_tokens": self.prompt_budget_tokens,
        })
    }
}

impl RuntimePreviewSection for AuxiliaryExecutorConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "max_tasks_per_session": self.max_tasks_per_session,
            "default_budget_tokens": self.default_budget_tokens,
        })
    }
}

impl RuntimePreviewSection for FlowOrchestrationConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "cancellation_gate_enabled": self.cancellation_gate_enabled,
            "max_retry_count": self.max_retry_count,
        })
    }
}

impl RuntimePreviewSection for DeliveryArbitrationConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "descendant_preference": self.descendant_preference,
            "suppression_limit": self.suppression_limit,
        })
    }
}

impl RuntimePreviewSection for ReplayCaptureConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "capture_runtime_decisions": self.capture_runtime_decisions,
            "max_events_per_run": self.max_events_per_run,
        })
    }
}

impl RuntimePreviewSection for NetworkedWorkersConfig {
    fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    fn settings(&self) -> Value {
        json!({
            "lease_ttl_ms": self.lease_ttl_ms,
            "require_attestation": self.require_attestation,
        })
    }
}

#[cfg(test)]
mod tests {
    use palyra_common::{
        feature_rollouts::FeatureRolloutSetting,
        runtime_preview::{
            RuntimePreviewCapability, RuntimePreviewEffectiveState, RuntimePreviewMode,
            RuntimePreviewSummaryState,
        },
    };

    use super::{build_runtime_preview_config_snapshot, capability_snapshot};

    #[test]
    fn runtime_preview_snapshot_defaults_to_preview_only_and_disabled_mix() {
        let config = crate::config::LoadedConfig {
            source: "defaults".to_owned(),
            config_version: 1,
            migrated_from_version: None,
            deployment: crate::config::DeploymentConfig::default(),
            daemon: crate::config::DaemonConfig::default(),
            gateway: crate::config::GatewayConfig::default(),
            feature_rollouts: crate::config::FeatureRolloutsConfig::default(),
            session_queue_policy: crate::config::SessionQueuePolicyConfig::default(),
            pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig::default(),
            retrieval_dual_path: crate::config::RetrievalDualPathConfig::default(),
            auxiliary_executor: crate::config::AuxiliaryExecutorConfig::default(),
            flow_orchestration: crate::config::FlowOrchestrationConfig::default(),
            delivery_arbitration: crate::config::DeliveryArbitrationConfig::default(),
            replay_capture: crate::config::ReplayCaptureConfig::default(),
            networked_workers: crate::config::NetworkedWorkersConfig::default(),
            cron: crate::config::CronConfig::default(),
            orchestrator: crate::config::OrchestratorConfig::default(),
            memory: crate::config::MemoryConfig::default(),
            media: crate::media::MediaRuntimeConfig::default(),
            model_provider: crate::model_provider::ModelProviderConfig::default(),
            tool_call: crate::config::ToolCallConfig::default(),
            channel_router: crate::channel_router::ChannelRouterConfig::default(),
            canvas_host: crate::config::CanvasHostConfig::default(),
            admin: crate::config::AdminConfig::default(),
            identity: crate::config::IdentityConfig::default(),
            storage: crate::config::StorageConfig::default(),
        };

        let snapshot = build_runtime_preview_config_snapshot(&config);
        assert_eq!(snapshot.state, RuntimePreviewSummaryState::Mixed);
        assert!(snapshot.preview_capabilities > 0);
        assert!(snapshot.disabled_capabilities > 0);
    }

    #[test]
    fn enabled_mode_without_rollout_reports_blocker() {
        let mut config = crate::config::LoadedConfig {
            source: "defaults".to_owned(),
            config_version: 1,
            migrated_from_version: None,
            deployment: crate::config::DeploymentConfig::default(),
            daemon: crate::config::DaemonConfig::default(),
            gateway: crate::config::GatewayConfig::default(),
            feature_rollouts: crate::config::FeatureRolloutsConfig::default(),
            session_queue_policy: crate::config::SessionQueuePolicyConfig::default(),
            pruning_policy_matrix: crate::config::PruningPolicyMatrixConfig::default(),
            retrieval_dual_path: crate::config::RetrievalDualPathConfig::default(),
            auxiliary_executor: crate::config::AuxiliaryExecutorConfig::default(),
            flow_orchestration: crate::config::FlowOrchestrationConfig::default(),
            delivery_arbitration: crate::config::DeliveryArbitrationConfig::default(),
            replay_capture: crate::config::ReplayCaptureConfig::default(),
            networked_workers: crate::config::NetworkedWorkersConfig::default(),
            cron: crate::config::CronConfig::default(),
            orchestrator: crate::config::OrchestratorConfig::default(),
            memory: crate::config::MemoryConfig::default(),
            media: crate::media::MediaRuntimeConfig::default(),
            model_provider: crate::model_provider::ModelProviderConfig::default(),
            tool_call: crate::config::ToolCallConfig::default(),
            channel_router: crate::channel_router::ChannelRouterConfig::default(),
            canvas_host: crate::config::CanvasHostConfig::default(),
            admin: crate::config::AdminConfig::default(),
            identity: crate::config::IdentityConfig::default(),
            storage: crate::config::StorageConfig::default(),
        };
        config.session_queue_policy.mode = RuntimePreviewMode::Enabled;
        config.feature_rollouts.session_queue_policy = FeatureRolloutSetting::default();

        let snapshot = capability_snapshot(&config, RuntimePreviewCapability::SessionQueuePolicy);
        assert_eq!(snapshot.effective_state, RuntimePreviewEffectiveState::Blocked);
        assert!(!snapshot.activation_blockers.is_empty());
    }
}
